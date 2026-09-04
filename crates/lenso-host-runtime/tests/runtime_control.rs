use std::{
    fs,
    io::{Read, Write},
    path::Path,
    process::{Command, Stdio},
};

use lenso_app_plan::ResolvedAppPlan;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn digest(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn target() -> (&'static str, &'static str, &'static str) {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => ("aarch64-apple-darwin", "darwin", "arm64"),
        ("linux", "x86_64") => ("x86_64-unknown-linux-gnu", "linux", "x64"),
        values => panic!("unsupported test platform: {values:?}"),
    }
}

fn executable(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
}

fn write_frame(writer: &mut impl Write, value: &Value) {
    let bytes = serde_json::to_vec(value).unwrap();
    writer
        .write_all(&u32::try_from(bytes.len()).unwrap().to_be_bytes())
        .unwrap();
    writer.write_all(&bytes).unwrap();
    writer.flush().unwrap();
}

fn read_frame(reader: &mut impl Read) -> Value {
    let mut length = [0_u8; 4];
    reader.read_exact(&mut length).unwrap();
    let mut bytes = vec![0; u32::from_be_bytes(length) as usize];
    reader.read_exact(&mut bytes).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn run_session(runtime: &Path, lock: &Path, app: &Path, identity: &str) {
    let mut child = Command::new(runtime)
        .args([
            "--distribution",
            lock.to_str().unwrap(),
            "--root",
            app.to_str().unwrap(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = child.stdout.take().unwrap();
    write_frame(
        &mut input,
        &json!({"op":"start","version":1,"id":1,"distribution":identity}),
    );
    assert_eq!(read_frame(&mut output)["kind"], "ready");
    assert_eq!(read_frame(&mut output)["kind"], "started");
    write_frame(
        &mut input,
        &json!({"op":"inspect","version":1,"id":2,"revision":null,"offset":0,"limit":16}),
    );
    let inspected = read_frame(&mut output);
    assert_eq!(inspected["kind"], "inspected");
    assert_eq!(inspected["instances"], json!([]));
    write_frame(&mut input, &json!({"op":"stop","version":1,"id":3}));
    let stopped = read_frame(&mut output);
    assert_eq!(stopped["kind"], "terminal");
    assert_eq!(stopped["shutdown"], "suspended");
    drop(input);
    assert!(child.wait().unwrap().success());
}

#[test]
fn prepared_runtime_reaches_ready_inspects_and_suspends() {
    let temporary = tempfile::tempdir().unwrap();
    let distribution = temporary.path().join("distribution");
    let app = temporary.path().join("app");
    fs::create_dir_all(distribution.join(".lenso")).unwrap();
    fs::create_dir(distribution.join("runtime")).unwrap();
    fs::create_dir(&app).unwrap();
    let host_build = b"{\"host\":\"fixture\"}\n";
    fs::write(distribution.join(".lenso/host-build.json"), host_build).unwrap();
    fs::write(distribution.join("bundles.json"), b"[]\n").unwrap();
    fs::write(
        distribution.join("THIRD_PARTY_NOTICES.txt"),
        b"fixture notices\n",
    )
    .unwrap();
    executable(distribution.join("host.js").as_path(), b"entrypoint\n");
    executable(
        distribution.join("runtime/lenso-process-owner").as_path(),
        b"owner\n",
    );
    fs::copy(
        env!("CARGO_BIN_EXE_lenso-host-runtime"),
        distribution.join("runtime/lenso-host-runtime"),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(
            distribution.join("runtime/lenso-host-runtime"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
    }
    let resolution = serde_json::to_string(&json!({
        "schema": "lenso.runtime-app-resolution.v1",
        "app_id": "company.app",
        "authority_digest": digest(b"authority"),
        "host_build_digest": digest(host_build),
        "plugin_root_revision": digest(b"root"),
        "plan": ResolvedAppPlan::empty(),
    }))
    .unwrap();
    executable(
        distribution.join("runtime/lenso-resolver").as_path(),
        format!("#!/bin/sh\nprintf '%s\\n' '{resolution}'\n").as_bytes(),
    );
    let files = [
        (".lenso/host-build.json", "host_authority", false),
        ("bundles.json", "bundle_inventory", false),
        ("runtime/lenso-host-runtime", "host_runtime", true),
        ("runtime/lenso-resolver", "runtime_resolver", true),
        ("runtime/lenso-process-owner", "process_owner", true),
        ("host.js", "entrypoint", true),
        ("THIRD_PARTY_NOTICES.txt", "notices", false),
    ]
    .into_iter()
    .map(|(path, role, is_executable)| {
        let bytes = fs::read(distribution.join(path)).unwrap();
        json!({
            "path": path,
            "role": role,
            "sha256": digest(&bytes),
            "size": bytes.len(),
            "executable": is_executable,
        })
    })
    .collect::<Vec<_>>();
    let (target, platform, arch) = target();
    let lock = serde_json::to_vec_pretty(&json!({
        "schema": "lenso.host-distribution.v1",
        "app_id": "company.app",
        "target": target,
        "platform": platform,
        "arch": arch,
        "files": files,
    }))
    .unwrap();
    let identity = digest(&lock);
    let lock_path = distribution.join(".lenso/distribution.lock.json");
    fs::write(&lock_path, lock).unwrap();

    let runtime = distribution.join("runtime/lenso-host-runtime");
    run_session(&runtime, &lock_path, &app, &identity);
    run_session(&runtime, &lock_path, &app, &identity);
}
