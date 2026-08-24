import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const lockPath = resolve(packageRoot, "capabilities.lock.json");
const mode = process.argv[2] ?? "check";

if (mode !== "check" && mode !== "sync") {
  throw new Error("usage: capabilities.mjs <check|sync>");
}

const lock = JSON.parse(await readFile(lockPath, "utf8"));
if (lock.schema_version !== 2) {
  throw new Error(`unsupported capabilities lock schema: ${lock.schema_version}`);
}

const capabilityIds = Object.keys(lock.capabilities);
if (capabilityIds.join("\n") !== [...capabilityIds].sort().join("\n")) {
  throw new Error("capabilities.lock.json entries must be sorted by Capability id");
}

function packagePath(relativePath) {
  const path = resolve(packageRoot, relativePath);
  if (!path.startsWith(`${packageRoot}/`)) {
    throw new Error(`path escapes @lenso/bun: ${relativePath}`);
  }
  return path;
}

function schemaPaths(value, found = new Set()) {
  if (Array.isArray(value)) {
    for (const item of value) schemaPaths(item, found);
  } else if (value && typeof value === "object") {
    for (const [key, item] of Object.entries(value)) {
      if (key.endsWith("_schema") && typeof item === "string") found.add(item);
      schemaPaths(item, found);
    }
  }
  return found;
}

function githubRawBase(source) {
  const match = /^https:\/\/github\.com\/([^/]+)\/([^/]+)$/.exec(source.source_repository);
  if (!match || !/^[0-9a-f]{40}$/.test(source.source_revision)) {
    throw new Error(`Capability source must use a GitHub repository and full commit: ${source.source_repository}`);
  }
  return `https://raw.githubusercontent.com/${match[1]}/${match[2]}/${source.source_revision}`;
}

async function fetchSource(source, relativePath) {
  const response = await fetch(`${githubRawBase(source)}/${relativePath}`);
  if (!response.ok) {
    throw new Error(`failed to fetch ${relativePath}: ${response.status} ${response.statusText}`);
  }
  return response.text();
}

async function syncCapability(capabilityId, source) {
  const descriptorText = await fetchSource(source, source.source_descriptor);
  const descriptor = JSON.parse(descriptorText);
  const descriptorPath = packagePath(source.snapshot_descriptor);
  await mkdir(dirname(descriptorPath), { recursive: true });
  await writeFile(descriptorPath, `${JSON.stringify(descriptor, null, 2)}\n`);

  const sourceRoot = dirname(source.source_descriptor);
  const snapshotRoot = dirname(descriptorPath);
  for (const schemaPath of schemaPaths(descriptor)) {
    const schemaText = await fetchSource(source, `${sourceRoot}/${schemaPath}`);
    const target = resolve(snapshotRoot, schemaPath);
    if (!target.startsWith(`${snapshotRoot}/`)) {
      throw new Error(`Schema path escapes contract snapshot: ${schemaPath}`);
    }
    await mkdir(dirname(target), { recursive: true });
    await writeFile(target, `${JSON.stringify(JSON.parse(schemaText), null, 2)}\n`);
  }
  generate(capabilityId, source, "generate");
}

function generate(capabilityId, source, command) {
  const executable = process.env.LENSO_CONTRACT_CODEGEN ?? "lenso-contract-codegen";
  const result = spawnSync(executable, [
    command,
    packagePath(source.snapshot_descriptor),
    "--typescript",
    packagePath(source.typescript_projection),
  ], { stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${command} failed for ${capabilityId}`);
  }
}

for (const capabilityId of capabilityIds) {
  const source = lock.capabilities[capabilityId];
  if (mode === "sync") await syncCapability(capabilityId, source);
  const descriptor = JSON.parse(await readFile(packagePath(source.snapshot_descriptor), "utf8"));
  if (descriptor.id !== capabilityId || descriptor.version !== source.descriptor_version) {
    throw new Error(`locked identity does not match snapshot for ${capabilityId}`);
  }
  generate(capabilityId, source, "check");
}

console.log(`${mode}ed ${capabilityIds.length} Capability projections`);
