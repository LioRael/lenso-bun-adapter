export async function lensoInvoke(invocation) {
  await Promise.resolve();

  if (invocation.capability !== "example.greeting@1" || invocation.operation !== "greet") {
    throw new Error("unexpected embedded invocation");
  }

  if (invocation.request.name.length === 0) {
    return { kind: "domain_error", value: "empty_name" };
  }

  return {
    kind: "ok",
    value: {
      message: `${invocation.configuration.prefix}, ${invocation.request.name}`,
    },
  };
}
