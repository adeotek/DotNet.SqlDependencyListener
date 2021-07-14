namespace Adeotek.SqlDependencyListener
{
    public class SqlDependencyListenerOptions
    {
        // Global
        public int CommandTimeout { get; set; } = 60000;
        public bool ManualConfiguration { get; set; }
        // Manual & automatic
        public string QueueSchemaName { get; set; } = "dbo";
        public string QueueName { get; set; }
        // Automatic only
        public NotificationTypes ListenerType { get; set; } = NotificationTypes.Insert | NotificationTypes.Update | NotificationTypes.Delete;
        public int Identity { get; set; } = 1;
        public bool AutoEnableServiceBroker { get; set; }
        public string ServiceName { get; set; }
        public string TriggerName { get; set; }
        public string InstallProcedureName { get; set; }
        public string UninstallProcedureName { get; set; }
    }
}