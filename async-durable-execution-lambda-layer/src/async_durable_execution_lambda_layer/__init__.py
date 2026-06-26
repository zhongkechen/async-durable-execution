"""Build Lambda layer archives for the async durable execution SDK."""

__all__ = [
    "LayerBuildResult",
    "build_layer",
    "create_layer_archive",
    "default_sdk_spec",
]


def __getattr__(name: str):
    if name in __all__:
        from async_durable_execution_lambda_layer import builder

        return getattr(builder, name)
    raise AttributeError(name)
