# Custom Operation SPI

Import these public symbols from `async_durable_execution`.

Use these contracts in independently maintained packages that implement reusable
durable operations. See [Custom Durable Operations](../../custom-operations.md)
for the extension-author guide and replay compatibility rules.

::: async_durable_execution
    options:
      members:
        - ExtensionContext
        - get_extension_context
        - ExtensionOperation
        - ExtensionStepResult
        - ExtensionStepFunction
        - ExtensionStepRetryStrategy
