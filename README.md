# .Net SqlDependencyListener
Simple System.Data.SqlClient.SqlDependency listener implementation

[![.NET](https://github.com/adeotek/DotNet.SqlDependencyListener/actions/workflows/dotnet.yml/badge.svg?branch=main)](https://github.com/adeotek/DotNet.SqlDependencyListener/actions/workflows/dotnet.yml)


## Installation

The Adeotek.SqlDependencyListener NuGet [package can be found here](https://www.nuget.org/packages/Adeotek.SqlDependencyListener/). Alternatively you can install it via one of the following commands below:

NuGet command:
```powershell
Install-Package Adeotek.SqlDependencyListener
```
.NET Core CLI:
```powershell
dotnet add package Adeotek.SqlDependencyListener
```


## Usage

### With automatic database setup
```c#
var options = new SqlDependencyListenerOptions
{
    CommandTimeout = 60000, // Optional (default 60000)
    ManualConfiguration = false, // Required (false)
    AutoEnableServiceBroker = true, // Optional (default false)
    ListenerType = NotificationTypes.Insert | NotificationTypes.Update | NotificationTypes.Delete,  // Optional
    Identity = 1  // Optional (default 1)
    //// Database objects naming options
    QueueSchemaName = "dbo", // Optional (default "dbo")
    QueueName = "TEST_NOTIFICATIONS_QUEUE",
    ServiceName = "TEST_NOTIFICATIONS_SERVICE",
    TriggerName = "TR_TEST_NOTIFICATIONS",
    InstallProcedureName = "SP_INSTALL_TEST_NOTIFICATIONS",
    UninstallProcedureName = "SP_UNINSTALL_TEST_NOTIFICATIONS"
};

var listener = new SqlDependencyListener<EventData<DataModel>>(
    connectionString,
    databaseName,
    tableName,
    databaseSchema,
    options);

listener.OnTableChanged += OnTableChangedHandler;
listener.Start();

// ...

listener.Stop();
```
OnTableChanged example
```c#
void OnTableChangedHandler(object sender, TableChangedEventArgs<EventData<DataModel>> e)
{
    Console.WriteLine("Event raw message (JSON): {0}", e.Message);
    EventData<DataModel> data = e.Data;
}
```

### With manual database setup
```c#
var options = new SqlDependencyListenerOptions
{
    CommandTimeout = 60000, // Optional (default 60000)
    ManualConfiguration = true, // Required (true)
    QueueSchemaName = "dbo", // Optional (default "dbo")
    QueueName = "TEST_NOTIFICATIONS_QUEUE" // Required
};

var listener = new SqlDependencyListener<CustomDataModel>(
    connectionString,
    databaseName,
    tableName,
    databaseSchema,
    options);

listener.OnTableChanged += OnTableChangedHandler;
listener.Start();

// ...

listener.Stop();
```
OnTableChanged example
```c#
void OnTableChangedHandler(object sender, TableChangedEventArgs<CustomDataModel> e)
{
    Console.WriteLine("Event raw message (JSON): {0}", e.Message);
    CustomDataModel data = e.Data;
}
```

---
_For complete usage example, please check the sample project._