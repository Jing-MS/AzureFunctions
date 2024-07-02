This Azure Function involves the following steps:
- trigger every day at 1 AM UTC
- query the Azure Log Analytics instance behind the task monitoring log
- write the aggregated output to a Kusto table

In order to make it work, few things have to be done separately:
- create a function app via the Azure portal
- grant the function app the master reader role to the log analytics instance: https://ms.portal.azure.com/#@microsoft.onmicrosoft.com/resource/subscriptions/80d2c6c6-fa64-4ab1-8aa5-4e118c6b16ce/resourceGroups/defaultresourcegroup-westus2/providers/Microsoft.OperationalInsights/workspaces/DefaultWorkspace-westus2/users
- grant the function app the ingestor role to the Kusto database: .add database <database-name> ingestors ('<app-id>')