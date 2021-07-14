using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using System.Threading;

namespace Adeotek.SqlDependencyListener.Sample
{
    class Program
    {
        const string ConnectionString = @"Server=LOCALHOST;Database=TEST_DB;User Id=sa;Password=<password>";
        const string Database = "TEST_DB";
        const string Schema = "dbo";
        const string Table = "TEST_TABLE";

        static void Main(string[] args)
        {
            Console.WriteLine("Starting...");

            try
            {
                // Automatic database setup
                AutomatedSqlDependencyListener();

                // Manual database setup
                // ManualSqlDependencyListener();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        // Automatic database setup
        static void AutomatedSqlDependencyListener()
        {
            var options = new SqlDependencyListenerOptions
            {
                CommandTimeout = 60000, // Optional (default 60000)
                ManualConfiguration = false, // Required (false)
                AutoEnableServiceBroker = true, // Optional (default false)
                ListenerType = NotificationTypes.Insert | NotificationTypes.Update | NotificationTypes.Delete,  // Optional
                Identity = 1  // Optional (default 1)
                //// Database objects naming options
                // QueueSchemaName = "dbo", // Optional (default "dbo")
                // QueueName = "TEST_NOTIFICATIONS_QUEUE",
                // ServiceName = "TEST_NOTIFICATIONS_SERVICE",
                // TriggerName = "TR_TEST_NOTIFICATIONS",
                // InstallProcedureName = "SP_INSTALL_TEST_NOTIFICATIONS",
                // UninstallProcedureName = "SP_UNINSTALL_TEST_NOTIFICATIONS"
            };
            var listener = new SqlDependencyListener<EventData<Data>>(
                ConnectionString,
                Database,
                Table,
                Schema,
                options);

            listener.OnTableChanged += AutomatedSqlDependencyListenerOnTableChanged;
            listener.Start();
            Console.WriteLine("Listening to database changes");
            Console.WriteLine("Press [x] to exit");

            do
            {
                Thread.Sleep(100);
            } while (Console.ReadKey().KeyChar != 'x');

            listener.Stop();
        }

        static void AutomatedSqlDependencyListenerOnTableChanged(object sender, TableChangedEventArgs<EventData<Data>> e)
        {
            Console.WriteLine("Event type: {0}", e.Data?.EventType);
            Console.WriteLine("Event message: {0}", e.Message);
        }

        // Manual database setup
        static void ManualSqlDependencyListener()
        {
            var options = new SqlDependencyListenerOptions
            {
                CommandTimeout = 60000, // Optional (default 60000)
                ManualConfiguration = true, // Required (true)
                QueueSchemaName = "dbo", // Optional (default "dbo")
                QueueName = "TEST_NOTIFICATIONS_QUEUE" // Required
            };
            var listener = new SqlDependencyListener<List<Data>>(
                ConnectionString,
                Database,
                Table,
                Schema,
                options);

            listener.OnTableChanged += ManualSqlDependencyListenerOnTableChanged;
            listener.Start();
            Console.WriteLine("Listening to database changes");
            Console.WriteLine("Press [x] to exit");

            do
            {
                Thread.Sleep(100);
            } while (Console.ReadKey().KeyChar != 'x');

            listener.Stop();
        }

        static void ManualSqlDependencyListenerOnTableChanged(object sender, TableChangedEventArgs<List<Data>> e)
        {
            Console.WriteLine("Event message: {0}", e.Message);
            if ((e.Data?.Count ?? 0) > 0)
            {
                Console.WriteLine("Data.Uid [{0}] -> Action [{1}]", e.Data[0]?.Uid.ToString() ?? "null", e.Data[0]?.Action ?? "unknown");
            }
        }

        class Data
        {
            [JsonPropertyName("ACTION")]
            public string Action { get; set; }
            [JsonPropertyName("UID")]
            public Guid Uid { get; set; }
            [JsonPropertyName("ID")]
            public int Id { get; set; }
            [JsonPropertyName("TITLE")]
            public string Title { get; set; }
            [JsonPropertyName("STATE")]
            public int State { get; set; }
            [JsonPropertyName("CREATED_AT")]
            public DateTime CreatedAt { get; set; }
        }
    }
}
