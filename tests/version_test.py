"""Tests for DurableExecutionsPythonLanguageSDK module."""


def test_version_is_accessible() -> None:
    """Test __version__ is accessible from package root."""
    import async_durable_execution  # noqa: PLC0415
    from async_durable_execution import __about__  # noqa: PLC0415

    assert hasattr(async_durable_execution, "__version__")
    assert isinstance(async_durable_execution.__version__, str)
    assert len(async_durable_execution.__version__) > 0
    assert async_durable_execution.__version__ == __about__.__version__
