using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Text;
using System.Threading;

namespace Adeotek.SqlDependencyListener
{
    [Flags]
    public enum NotificationTypes
    {
        Insert = 1 << 1,
        Update = 1 << 2,
        Delete = 1 << 3
    }

    public class SqlDependencyListener : SqlDependencyListener<string>
    {
        public SqlDependencyListener(
            string connectionString,
            string databaseName,
            string tableName,
            string schemaName = "dbo",
            SqlDependencyListenerOptions options = default)
            : base(connectionString, databaseName, tableName, schemaName, options)
        {
        }
    }

    public class SqlDependencyListener<T> : IDisposable
    {
        private static readonly List<int> ActiveEntities = new List<int>();
        private readonly SqlDependencyListenerOptions _options;
        private CancellationTokenSource _threadSource;

        public event EventHandler<TableChangedEventArgs<T>> OnTableChanged;
        public event EventHandler OnListenerProcessStopped;
        public bool Active { get; private set; }
        public string ConnectionString { get; }
        public string DatabaseName { get; }
        public string TableName { get; }
        public string SchemaName { get; }

        public SqlDependencyListener (
            string connectionString,
            string databaseName,
            string tableName,
            string schemaName = "dbo",
            SqlDependencyListenerOptions options = default)
        {
            ConnectionString = connectionString;
            DatabaseName = databaseName;
            TableName = tableName;
            SchemaName = schemaName;
            _options = options ?? new SqlDependencyListenerOptions();
            Active = false;
        }

        public int Identity => _options.Identity;
        public int CommandTimeout => _options.CommandTimeout;
        public NotificationTypes ListenerType => _options.ListenerType;
        public string QueueSchemaName => _options.QueueSchemaName;
        public string QueueName
            => string.IsNullOrEmpty(_options.QueueName) ? $"LISTENER_QUEUE_{Identity.ToString()}" : _options.QueueName;
        public string ServiceName
            => string.IsNullOrEmpty(_options.ServiceName) ? $"LISTENER_SERVICE_{Identity.ToString()}" : _options.ServiceName;
        public string TriggerName
            => string.IsNullOrEmpty(_options.TriggerName) ? $"TR_LISTENER_{Identity.ToString()}" : _options.TriggerName;
        public string InstallProcedureName
            => string.IsNullOrEmpty(_options.InstallProcedureName) ? $"SP_INSTALL_LISTENER_{Identity.ToString()}" : _options.InstallProcedureName;
        public string UninstallProcedureName
            => string.IsNullOrEmpty(_options.UninstallProcedureName) ? $"SP_UNINSTALL_LISTENER_{Identity.ToString()}" : _options.UninstallProcedureName;

        public void Start()
        {
            if (!_options.ManualConfiguration)
            {
                lock (ActiveEntities)
                {
                    if (ActiveEntities.Contains(Identity))
                    {
                        throw new InvalidOperationException("An object with the same identity has already been started.");
                    }
                    ActiveEntities.Add(Identity);
                }
            }

            // ASP.NET fix
            // IIS is not usually restarted when a new website version is deployed
            // This situation leads to notification absence in some cases
            Stop();

            if (!_options.ManualConfiguration)
            {
                DatabasePrepare();
            }

            _threadSource = new CancellationTokenSource();
            // Pass the token to the cancelable operation.
            ThreadPool.QueueUserWorkItem(ListenerLoop, _threadSource.Token);
        }

        public void Stop()
        {
            if (!_options.ManualConfiguration)
            {
                DatabaseUnPrepare();

                lock (ActiveEntities)
                {
                    if (ActiveEntities.Contains(Identity))
                    {
                        ActiveEntities.Remove(Identity);
                    }
                }
            }

            if (_threadSource == null || _threadSource.Token.IsCancellationRequested)
            {
                return;
            }

            if (!_threadSource.Token.CanBeCanceled)
            {
                return;
            }

            _threadSource.Cancel();
            _threadSource.Dispose();
        }

        public void Dispose()
        {
            Stop();
        }

        protected virtual void ListenerLoop(object input)
        {
            try
            {
                while (true)
                {
                    var message = ReceiveEvent();
                    Active = true;
                    if (!string.IsNullOrWhiteSpace(message))
                    {
                        TableChangedHandler(message);
                    }
                }
            }
            catch
            {
                // ignored
            }
            finally
            {
                Active = false;
                ListenerProcessStoppedHandler();
            }
        }

        protected virtual void TableChangedHandler(string message)
        {
            OnTableChanged?.Invoke(this, new TableChangedEventArgs<T>(message));
        }

        protected virtual void ListenerProcessStoppedHandler()
        {
            OnListenerProcessStopped?.BeginInvoke(this, EventArgs.Empty, null, null);
        }

        protected virtual string ReceiveEvent()
        {
            var commandText = string.Format(
                SqlHelpers.ReceiveEvent,
                DatabaseName,
                QueueName,
                (CommandTimeout / 2).ToString(),
                QueueSchemaName);

            using (var conn = new SqlConnection(ConnectionString))
            {
                using (var command = new SqlCommand(commandText, conn))
                {
                    conn.Open();
                    command.CommandType = CommandType.Text;
                    command.CommandTimeout = CommandTimeout;
                    using (var reader = command.ExecuteReader())
                    {
                        if (!reader.Read() || reader.IsDBNull(0))
                        {
                            return string.Empty;
                        }

                        return reader.GetString(0);
                    }
                }
            }
        }

        #region Automatic database setup

        private void DatabasePrepare()
        {
            var installProcedureScript = GetInstallProcedureScript();
            SqlHelpers.ExecuteNonQuery(installProcedureScript, ConnectionString);
            var uninstallProcedureScript = GetUninstallProcedureScript();
            SqlHelpers.ExecuteNonQuery(uninstallProcedureScript, ConnectionString);

            var execInstallationProcedureScript = string.Format(
                SqlHelpers.ExecuteProcedure,
                DatabaseName,
                InstallProcedureName,
                SchemaName);
            SqlHelpers.ExecuteNonQuery(execInstallationProcedureScript, ConnectionString);
        }

        private void DatabaseUnPrepare()
        {
            var execUninstallationProcedureScript = string.Format(
                SqlHelpers.ExecuteProcedure,
                DatabaseName,
                UninstallProcedureName,
                SchemaName);
            SqlHelpers.ExecuteNonQuery(execUninstallationProcedureScript, ConnectionString);
        }

        private string GetInstallProcedureScript()
        {
            var enableServiceBrokerScript = _options.AutoEnableServiceBroker
                ? string.Format(SqlHelpers.EnableServiceBroker, DatabaseName)
                : string.Empty;
            var createConversationObjectsScript = string.Format(
                SqlHelpers.CreateConversationObjects,
                DatabaseName,
                QueueName,
                ServiceName,
                QueueSchemaName);
            var createTriggerScript = string.Format(
                SqlHelpers.CreateTrigger,
                TableName,
                TriggerName,
                GetTriggerTypeByListenerType(),
                ServiceName,
                SchemaName);
            var checkTriggerScript = string.Format(SqlHelpers.CheckTrigger, TriggerName, SchemaName);
            var installationProcedureScript = string.Format(
                SqlHelpers.CreateInstallationProcedure,
                DatabaseName,
                InstallProcedureName,
                enableServiceBrokerScript.Replace("'", "''"),
                createConversationObjectsScript.Replace("'", "''"),
                checkTriggerScript.Replace("'", "''"),
                createTriggerScript.Replace("'", "''''"),
                SchemaName);
            return installationProcedureScript;
        }

        private string GetUninstallProcedureScript()
        {
            var dropConversationObjectsScript = string.Format(
                SqlHelpers.DropConversationObjects,
                QueueName,
                ServiceName,
                QueueSchemaName);
            var dropTriggerScript = string.Format(SqlHelpers.DropTrigger, TriggerName, SchemaName);
            var uninstallationProcedureScript = string.Format(
                SqlHelpers.CreateUninstallationProcedure,
                DatabaseName,
                UninstallProcedureName,
                dropConversationObjectsScript.Replace("'", "''"),
                dropTriggerScript.Replace("'", "''"),
                SchemaName,
                InstallProcedureName);
            return uninstallationProcedureScript;
        }

        private string GetTriggerTypeByListenerType()
        {
            var result = new StringBuilder();

            if (ListenerType.HasFlag(NotificationTypes.Insert))
            {
                result.Append("INSERT");
            }

            if (ListenerType.HasFlag(NotificationTypes.Update))
            {
                result.Append(result.Length == 0 ? "UPDATE" : ", UPDATE");
            }

            if (ListenerType.HasFlag(NotificationTypes.Delete))
            {
                result.Append(result.Length == 0 ? "DELETE" : ", DELETE");
            }

            if (result.Length == 0)
            {
                result.Append("INSERT");
            }

            return result.ToString();
        }

        #endregion
    }
}