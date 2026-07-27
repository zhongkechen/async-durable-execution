import hashlib
import re


HANDLER_PACKAGE_PREFIX = "examples."
# Preserve identifiers created before the examples package moved to the repository root.
LEGACY_HANDLER_PACKAGE_PREFIX = "async_durable_execution_examples."
LAMBDA_FUNCTION_NAME_MAX_LENGTH = 64
DEFAULT_FUNCTION_NAME_PREFIX_HEADROOM = 16
DEFAULT_FUNCTION_NAME_SUFFIX_MAX_LENGTH = (
    LAMBDA_FUNCTION_NAME_MAX_LENGTH - DEFAULT_FUNCTION_NAME_PREFIX_HEADROOM
)
HASH_LENGTH = 8


def to_legacy_relative_name(relative_name: str) -> str:
    """Map an organized example path to its pre-organization path."""
    parts = relative_name.split(".")
    if not parts:
        return relative_name

    category = parts[0]
    if category == "core":
        return ".".join(parts[1:])

    if category == "extension":
        operation_parts = parts[1:]
        if operation_parts[:1] == ["recurse"]:
            return ".".join(["invoke", *operation_parts[1:]])
        return ".".join(operation_parts)

    if category == "primitive":
        operation_parts = parts[1:]
        if operation_parts[:1] != ["child"]:
            return ".".join(operation_parts)

        child_parts = operation_parts[1:]
        if child_parts[:1] == ["block_example"]:
            return ".".join(["block_example", *child_parts])
        return ".".join(["run_in_child_context", *child_parts])

    return relative_name


def to_legacy_handler_name(handler_name: str) -> str:
    """Return the stable handler name used before examples were reorganized."""
    if not handler_name.startswith(HANDLER_PACKAGE_PREFIX):
        return handler_name

    relative_name = handler_name.removeprefix(HANDLER_PACKAGE_PREFIX)
    legacy_relative_name = to_legacy_relative_name(relative_name)
    return f"{LEGACY_HANDLER_PACKAGE_PREFIX}{legacy_relative_name}"


def to_logical_id(handler_name: str) -> str:
    """Convert a handler module name to a CloudFormation logical id."""
    handler_base = handler_name.replace(".handler", "")
    words = [word for word in re.split(r"[^A-Za-z0-9]+", handler_base) if word]
    return "".join(word[:1].upper() + word[1:] for word in words)


def to_function_name_suffix(
    handler_name: str, *, max_length: int = DEFAULT_FUNCTION_NAME_SUFFIX_MAX_LENGTH
) -> str:
    """Convert a handler module name to a Lambda-safe deploy name suffix.

    The suffix reserves some room for the runtime prefix used in CI/CD while still
    keeping the mapping deterministic for integration tests.
    """
    if handler_name.startswith(HANDLER_PACKAGE_PREFIX):
        stable_handler_name = to_legacy_handler_name(handler_name)
        shortened_handler_name = stable_handler_name.removeprefix(
            LEGACY_HANDLER_PACKAGE_PREFIX
        )
    else:
        shortened_handler_name = handler_name
        stable_handler_name = handler_name
    logical_id = to_logical_id(shortened_handler_name)
    if len(logical_id) <= max_length:
        return logical_id

    truncated_length = max_length - HASH_LENGTH - 1
    if truncated_length < 1:
        msg = f"Function name suffix max_length must be at least {HASH_LENGTH + 2}"
        raise ValueError(msg)

    digest = hashlib.sha1(stable_handler_name.encode("utf-8")).hexdigest()[:HASH_LENGTH]
    return f"{logical_id[:truncated_length]}-{digest}"
