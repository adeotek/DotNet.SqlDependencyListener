using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;

namespace Adeotek.SqlDependencyListener;

public static class SqlHelpers
{
    #region Static methods
    public static void ExecuteNonQuery(string commandText, string connectionString)
    {
        using var conn = new SqlConnection(connectionString);
        using var command = new SqlCommand(commandText, conn);
        conn.Open();
        command.CommandType = CommandType.Text;
        command.ExecuteNonQuery();
    }

    public static int[] GetDependencyDbIdentities(string connectionString, string database, string databaseObjectsPrefix)
    {
        if (connectionString == null)
        {
            throw new ArgumentNullException(nameof(connectionString));
        }

        if (database == null)
        {
            throw new ArgumentNullException(nameof(database));
        }

        var result = new List<string>();

        using (var connection = new SqlConnection(connectionString))
        using (var command = connection.CreateCommand())
        {
            connection.Open();
            command.CommandText = string.Format(GetDependencyIdentities, database, databaseObjectsPrefix);
            command.CommandType = CommandType.Text;
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    result.Add(reader.GetString(0));
                }
            }
        }

        return result.Select(p => int.TryParse(p, out var temp) ? temp : -1)
            .Where(p => p != -1)
            .ToArray();
    }

    public static void CleanDatabase(string connectionString, string database, string installProcedureName, string uninstallProcedureName)
    {
        var cleanupScript = string.Format(
            ForcedDatabaseCleaning,
            database,
            installProcedureName,
            uninstallProcedureName);
        ExecuteNonQuery(cleanupScript, connectionString);
    }
    #endregion

    #region Listener
    /// <summary>
    /// T-SQL script-template which helps to receive changed data in monitored table.
    /// {0} - database name.
    /// {1} - conversation queue name.
    /// {2} - timeout.
    /// {3} - schema name.
    /// </summary>
    public const string ReceiveEvent = @"DECLARE @conversationHandle UNIQUEIDENTIFIER
DECLARE @message VARBINARY(MAX)
USE [{0}]
WAITFOR (
    RECEIVE TOP(1) @conversationHandle=conversation_handle, @message=message_body FROM [{3}].[{1}]
), TIMEOUT {2};
BEGIN TRY
    END CONVERSATION @conversationHandle;
END TRY
BEGIN CATCH
END CATCH
SELECT CAST(@message AS NVARCHAR(MAX))
";
    #endregion

    #region Conversation
    /// <summary>
    /// T-SQL script-template which enables ServiceBroker, if not enabled.
    /// {0} - database name;
    /// </summary>
    public const string EnableServiceBroker = @"IF EXISTS (SELECT * FROM sys.databases WHERE name = '{0}' AND is_broker_enabled = 0)
    BEGIN
        -- Setup Service Broker
        ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
        ALTER DATABASE [{0}] SET ENABLE_BROKER;
        ALTER DATABASE [{0}] SET MULTI_USER WITH ROLLBACK IMMEDIATE
        /* FOR SQL Express */
        ALTER AUTHORIZATION ON DATABASE::[{0}] TO [sa]
    END
";

    /// <summary>
    /// T-SQL script-template which prepares database for ServiceBroker notification.
    /// {0} - database name;
    /// {1} - conversation queue name.
    /// {2} - conversation service name.
    /// {3} - schema name.
    /// </summary>
    public const string CreateConversationObjects = @"IF (EXISTS (SELECT * FROM sys.databases WHERE name = '{0}' AND is_broker_enabled = 0))
    BEGIN
        RAISERROR ('ServiceBroker is disabled for database [{0}]', 16, 1)
    END
    -- Create a queue which will hold the tracked information
    IF (NOT EXISTS (SELECT * FROM sys.service_queues WHERE name = '{1}'))
        CREATE QUEUE [{3}].[{1}]
    -- Create a service on which tracked information will be sent
    IF (NOT EXISTS (SELECT * FROM sys.services WHERE name = '{2}'))
        CREATE SERVICE [{2}] ON QUEUE [{3}].[{1}] ([DEFAULT])
";

    /// <summary>
    /// T-SQL script-template which removes database notification.
    /// {0} - conversation queue name.
    /// {1} - conversation service name.
    /// {2} - schema name.
    /// </summary>
    public const string DropConversationObjects = @"DECLARE @serviceId INT
    SELECT @serviceId = service_id FROM sys.services WHERE sys.services.name = '{1}'
    DECLARE @ConversationHandle uniqueidentifier
    DECLARE Conversations CURSOR FOR
        SELECT CEP.conversation_handle
            FROM sys.conversation_endpoints CEP
            WHERE CEP.service_id = @serviceId AND ([state] != 'CD' OR [lifetime] > GETDATE() + 1)
    OPEN Conversations;
    FETCH NEXT FROM Conversations INTO @ConversationHandle;
    WHILE (@@FETCH_STATUS = 0)
    BEGIN
        END CONVERSATION @ConversationHandle WITH CLEANUP;
        FETCH NEXT FROM Conversations INTO @ConversationHandle;
    END
    CLOSE Conversations;
    DEALLOCATE Conversations;
    -- Droping service and queue.
    IF (@serviceId IS NOT NULL)
        DROP SERVICE [{1}];
    IF (OBJECT_ID ('{2}.{0}', 'SQ') IS NOT NULL)
	    DROP QUEUE [{2}].[{0}];
";
    #endregion

    #region Trigger
    /// <summary>
    /// T-SQL script-template which creates notification trigger.
    /// {0} - trigger name.
    /// {1} - schema name.
    /// </summary>
    public const string CheckTrigger = @"IF (OBJECT_ID ('{1}.{0}', 'TR') IS NOT NULL) RETURN;";

    /// <summary>
    /// T-SQL script-template which creates notification trigger.
    /// {0} - monitored table name.
    /// {1} - trigger name.
    /// {2} - trigger type (INSERT, DELETE, UPDATE...).
    /// {3} - conversation service name.
    /// {4} - schema name.
    /// </summary>
    public const string CreateTrigger = @"CREATE OR ALTER TRIGGER [{4}].[{1}]
    ON [{4}].[{0}]
    FOR {2}
AS
BEGIN
    SET NOCOUNT ON;

    IF (EXISTS (SELECT * FROM sys.services WHERE name = '{3}'))
    BEGIN
        DECLARE @message NVARCHAR(MAX)
        DECLARE @EVENT_TYPE VARCHAR(50)

        IF (EXISTS (SELECT * FROM inserted))
        BEGIN
            IF (EXISTS (SELECT * FROM deleted))
            BEGIN
                SET @MESSAGE = (
                    SELECT
                    'update' AS [event_type],
                    (SELECT * FROM inserted FOR JSON PATH) AS [inserted],
                    (SELECT * FROM deleted FOR JSON PATH) AS [deleted]
                    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                );
            END
            ELSE BEGIN
                SET @MESSAGE = (
                    SELECT
                    'insert' AS [event_type],
                    (SELECT * FROM inserted FOR JSON PATH) AS [inserted],
                    NULL AS [deleted]
                    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                );
            END
        END
        ELSE IF (EXISTS (SELECT * FROM deleted))
        BEGIN
            SET @MESSAGE = (
                SELECT
                'delete' AS [event_type],
                NULL AS [inserted],
                (SELECT * FROM deleted FOR JSON PATH) AS [deleted]
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            );
        END

        --Sending a Request Message to the Target
	    DECLARE @conversationHandle UNIQUEIDENTIFIER;
	    --Determine the Initiator Service, Target Service and the Contract
	    BEGIN DIALOG @conversationHandle
		    FROM SERVICE [{3}] TO SERVICE '{3}' ON CONTRACT [DEFAULT]
		    WITH ENCRYPTION=OFF, LIFETIME = 60;
	    --Send the Message
	    SEND ON CONVERSATION @conversationHandle MESSAGE TYPE [DEFAULT] (@message);
        --End conversation
        END CONVERSATION @conversationHandle;
    END
END
";

    /// <summary>
    /// T-SQL script-template which creates notification trigger.
    /// {0} - notification trigger name.
    /// {1} - schema name.
    /// </summary>
    public const string DropTrigger = @"IF (OBJECT_ID ('{1}.{0}', 'TR') IS NOT NULL) DROP TRIGGER [{1}].[{0}];";
    #endregion

    #region Stored procedures
    /// <summary>
    /// T-SQL script-template which creates notification setup procedure.
    /// {0} - database name.
    /// {1} - setup procedure name.
    /// {2} - service broker enable statement.
    /// {3} - service broker configuration statement.
    /// {4} - notification trigger check statement.
    /// {5} - notification trigger configuration statement.
    /// {6} - schema name.
    /// </summary>
    public const string CreateInstallationProcedure = @"USE [{0}]
" + PermissionsInfo + @"
IF (OBJECT_ID ('{6}.{1}', 'P') IS NULL)
BEGIN
    EXEC ('CREATE OR ALTER PROCEDURE [{6}].[{1}]
AS
BEGIN
    -- Service Broker configuration statement.
    {2}
    {3}
    -- Trigger check statement.
    {4}
    -- Trigger configuration statement.
    EXEC (''{5}'')
END')
END
";

    /// <summary>
    /// T-SQL script-template which creates notification uninstall procedure.
    /// {0} - database name.
    /// {1} - uninstall procedure name.
    /// {2} - notification trigger drop statement.
    /// {3} - service broker uninstall statement.
    /// {4} - schema name.
    /// {5} - install procedure name.
    /// </summary>
    public const string CreateUninstallationProcedure = @"USE [{0}]
" + PermissionsInfo + @"
IF (OBJECT_ID ('{4}.{1}', 'P') IS NULL)
BEGIN
    EXEC ('CREATE OR ALTER PROCEDURE [{4}].[{1}]
AS
BEGIN
    -- Notification Trigger drop statement.
    {3}
    -- Service Broker uninstall statement.
    {2}
    IF (OBJECT_ID (''{4}.{5}'', ''P'') IS NOT NULL)
        DROP PROCEDURE [{4}].[{5}]

    DROP PROCEDURE [{4}].[{1}]
END')
END
";
    #endregion

    #region Helpers
    /// <summary>
    /// T-SQL script-template which executes stored procedure.
    /// {0} - database name.
    /// {1} - procedure name.
    /// {2} - schema name.
    /// </summary>
    public const string ExecuteProcedure = @"USE [{0}]
IF (OBJECT_ID ('{2}.{1}', 'P') IS NOT NULL)
    EXEC [{2}].[{1}]
";

    public const string PermissionsInfo = @"DECLARE @msg NVARCHAR(MAX)
DECLARE @crlf NCHAR(1)
SET @crlf = NCHAR(10)
SET @msg = 'Current user must have following permissions: '
SET @msg = @msg + '[CREATE PROCEDURE, CREATE SERVICE, CREATE QUEUE, SUBSCRIBE QUERY NOTIFICATIONS, CONTROL, REFERENCES] '
SET @msg = @msg + 'that are required to start query notifications. '
SET @msg = @msg + 'Grant described permissions with following script: ' + @crlf
SET @msg = @msg + 'GRANT CREATE PROCEDURE TO [<username>];' + @crlf
SET @msg = @msg + 'GRANT CREATE SERVICE TO [<username>];' + @crlf
SET @msg = @msg + 'GRANT CREATE QUEUE  TO [<username>];' + @crlf
SET @msg = @msg + 'GRANT REFERENCES ON CONTRACT::[DEFAULT] TO [<username>];' + @crlf
SET @msg = @msg + 'GRANT SUBSCRIBE QUERY NOTIFICATIONS TO [<username>];' + @crlf
SET @msg = @msg + 'GRANT CONTROL ON SCHEMA::[<schemaname>] TO [<username>];'
PRINT @msg
";

    /// <summary>
    /// T-SQL script-template which returns all dependency identities in the database.
    /// {0} - database name.
    /// {1} - database objects prefix
    /// </summary>
    public const string GetDependencyIdentities = @"USE [{0}]
SELECT REPLACE(name, '{1}' , '')
    FROM sys.services
    WHERE [name] like '{1}%';
";

    /// <summary>
    /// T-SQL script-template which cleans database from notifications.
    /// {0} - database name.
    /// {1} - uninstall procedure prefix
    /// {2} - install procedure prefix
    /// </summary>
    public const string ForcedDatabaseCleaning = @"USE [{0}]
DECLARE @DB_NAME VARCHAR(255)
SET @DB_NAME = '{0}'
DECLARE @PROC_NAME VARCHAR(255)

DECLARE procedures CURSOR FOR
    SELECT sys.schemas.name + '.' + sys.objects.name
        FROM sys.objects
            INNER JOIN sys.schemas ON sys.objects.schema_id = sys.schemas.schema_id
        WHERE sys.objects.[type] = 'P' AND sys.objects.[name] like '{1}%'
OPEN procedures;
FETCH NEXT FROM procedures INTO @PROC_NAME
WHILE (@@FETCH_STATUS = 0)
BEGIN
    EXEC ('USE [' + @DB_NAME + '] EXEC ' + @PROC_NAME + ' IF (OBJECT_ID (''' + @PROC_NAME + ''', ''P'') IS NOT NULL) DROP PROCEDURE ' + @PROC_NAME)
    FETCH NEXT FROM procedures INTO @PROC_NAME
END
CLOSE procedures;
DEALLOCATE procedures;

DECLARE procedures CURSOR FOR
    SELECT sys.schemas.name + '.' + sys.objects.name
        FROM sys.objects
            INNER JOIN sys.schemas ON sys.objects.schema_id = sys.schemas.schema_id
        WHERE sys.objects.[type] = 'P' AND sys.objects.[name] like '{2}%'
OPEN procedures;
FETCH NEXT FROM procedures INTO @PROC_NAME
WHILE (@@FETCH_STATUS = 0)
BEGIN
    EXEC ('USE [' + @DB_NAME + '] DROP PROCEDURE ' + @PROC_NAME)
    FETCH NEXT FROM procedures INTO @PROC_NAME
END
CLOSE procedures;
DEALLOCATE procedures;
";
    #endregion
}