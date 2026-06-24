"""Tests for DurableExecutionsPythonLanguageSDK module."""


def test_version_is_accessible():
    """Test __version__ is accessible from package root."""
    import async_durable_execution  # noqa: PLC0415

    assert hasattr(async_durable_execution, "__version__")
    assert isinstance(async_durable_execution.__version__, str)
    assert len(async_durable_execution.__version__) > 0
