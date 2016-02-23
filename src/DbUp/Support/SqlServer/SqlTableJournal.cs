using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using DbUp.Engine;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;

namespace DbUp.Support.SqlServer
{
    /// <summary>
    /// An implementation of the <see cref="IJournal"/> interface which tracks version numbers for a 
    /// SQL Server database using a table called dbo.SchemaVersions.
    /// </summary>
    public class SqlTableJournal : IJournal
    {
        private readonly Func<IConnectionManager> connectionManager;
        private readonly Func<IUpgradeLog> log;
        private readonly string schema;
        private readonly string table;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlTableJournal"/> class.
        /// </summary>
        /// <param name="connectionManager">The connection manager.</param>
        /// <param name="logger">The log.</param>
        /// <param name="schema">The schema that contains the table.</param>
        /// <param name="table">The table name.</param>
        /// <example>
        /// var journal = new TableJournal("Server=server;Database=database;Trusted_Connection=True", "dbo", "MyVersionTable");
        /// </example>
        public SqlTableJournal(Func<IConnectionManager> connectionManager, Func<IUpgradeLog> logger, string schema, string table)
        {
            this.schema = schema;
            this.table = table;

            this.connectionManager = connectionManager;

            log = logger;
        }

        /// <summary>
        /// Recalls the version number of the database.
        /// </summary>
        /// <returns>All executed scripts.</returns>
        public string[] GetExecutedScripts()
        {
            return ExecutedScripts();
        }

        private string[] ExecutedScripts(int? batchNumber=null)
        {
            log().WriteInformation("Fetching list of already executed scripts.");
            var exists = DoesTableExist();
            if (!exists)
            {
                log().WriteInformation(string.Format("The {0} table could not be found. The database is assumed to be at version 0.", CreateTableName(schema, table)));
                return new string[0];
            }

            var scripts = new List<string>();

            connectionManager().ExecuteCommandsWithManagedConnection(dbCommandFactory =>
            {
                using (var command = dbCommandFactory())
                {
                    var script = batchNumber.HasValue ? GetExecutedScriptsByBatchNumberSql(schema, table, batchNumber.Value) : GetExecutedScriptsSql(schema, table);
                    command.CommandText = script;
                    command.CommandType = CommandType.Text;

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                            scripts.Add((string) reader[0]);
                    }
                }
            });

            return scripts.ToArray();
        }

        /// <summary>
        /// Gets the executed scripts with the passed in BatchNumber
        /// </summary>
        /// <param name="batchNumber"></param>
        /// <returns></returns>
        public string[] GetExecutedScriptsOnBatchNumber(int batchNumber)
        {
            return ExecutedScripts(batchNumber);
        }

        /// <summary>
        /// Gets the current batch number
        /// </summary>
        /// <returns></returns>
        public int GetCurrentBatchNumber()
        {
            int currnetVersion = 0;
            connectionManager().ExecuteCommandsWithManagedConnection(dbCommandFactory =>
            {
                using (var command = dbCommandFactory())
                {
                    command.CommandText = GetCurrentBatchNumberSql(schema, table);
                    command.CommandType = CommandType.Text;

                    var result = command.ExecuteScalar() as int?;
                    currnetVersion = result ?? 1;
                }
            });
            return currnetVersion;
        }


        protected string GetCurrentBatchNumberSql(string schema, string table)
        {
            return string.Format("SELECT MAX(BatchNumber) FROM {0}", CreateTableName(schema, table));

        }

        /// <summary>
        /// Create an SQL statement which will retrieve all executed scripts in order.
        /// </summary>
        protected virtual string GetExecutedScriptsSql(string schema, string table)
        {
            return string.Format("select [ScriptName] from {0} order by [ScriptName]", CreateTableName(schema, table));
        }

        ///
        protected virtual string GetExecutedScriptsByBatchNumberSql(string schema, string table , int batchNumber)
        {
            return string.Format("select [ScriptName] from {0} where BatchNumber={1} order by [ScriptName] ", CreateTableName(schema, table), batchNumber);
        }

        /// <summary>
        /// Records a database upgrade for a database specified in a given connection string.
        /// </summary>
        /// <param name="script">The script.</param>
        public void StoreExecutedScript(SqlScript script)
        {
            var exists = DoesTableExist();
            if (!exists)
            {
                log().WriteInformation(string.Format("Creating the {0} table", CreateTableName(schema, table)));

                connectionManager().ExecuteCommandsWithManagedConnection(dbCommandFactory =>
                {
                    using (var command = dbCommandFactory())
                    {
                        command.CommandText = CreateTableSql(schema, table);

                        command.CommandType = CommandType.Text;
                        command.ExecuteNonQuery();
                    }

                    log().WriteInformation(string.Format("The {0} table has been created", CreateTableName(schema, table)));
                });

                CheckBatchNumberColumnExistsAndAddIfNot();
            }


            var currentBatchNumber = GetCurrentBatchNumber();
            connectionManager().ExecuteCommandsWithManagedConnection(dbCommandFactory =>
            {
                var nextBatchNumber = currentBatchNumber+1;
                using (var command = dbCommandFactory())
                {
                    command.CommandText = string.Format("insert into {0} (ScriptName, Applied, BatchNumber) values (@scriptName, @applied, @batchNumber)", CreateTableName(schema, table));

                    var scriptNameParam = command.CreateParameter();
                    scriptNameParam.ParameterName = "scriptName";
                    scriptNameParam.Value = script.Name;
                    command.Parameters.Add(scriptNameParam);

                    var appliedParam = command.CreateParameter();
                    appliedParam.ParameterName = "applied";
                    appliedParam.Value = DateTime.Now;
                    command.Parameters.Add(appliedParam);

                    var batchNumber = command.CreateParameter();
                    batchNumber.ParameterName = "batchNumber";
                    batchNumber.Value = nextBatchNumber;
                    command.Parameters.Add(batchNumber);

                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                }
            });
        }

        public void UpdateScriptEntry(string scriptName)
        {
            connectionManager().ExecuteCommandsWithManagedConnection(dbCommandFactory =>
            {
                var newScriptName = string.Format("{0}_{1}", "rolledback", scriptName);
                using (var command = dbCommandFactory())
                {
                    command.CommandText = string.Format("update {0} set ScriptName = @newScriptName where ScriptName = @scriptName", CreateTableName(schema, table));

                    var scriptNameParam = command.CreateParameter();
                    scriptNameParam.ParameterName = "scriptName";
                    scriptNameParam.Value = scriptName;
                    command.Parameters.Add(scriptNameParam);

                    var newScriptNameParam = command.CreateParameter();
                    newScriptNameParam.ParameterName = "newScriptName";
                    newScriptNameParam.Value = newScriptName;
                    command.Parameters.Add(newScriptNameParam);

                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                }
            });
        }
        private void CheckBatchNumberColumnExistsAndAddIfNot()
        {
            var batchNumberColumnExists = DoesBatchNumberColumnExists();
            if (!batchNumberColumnExists)
            {
                log().WriteInformation(string.Format("Adding BatchNumber column"));

                connectionManager().ExecuteCommandsWithManagedConnection(dbCommandFactory =>
                {
                    using (var command = dbCommandFactory())
                    {
                        command.CommandText = AddBatchNumberColumn(schema, table);

                        command.CommandType = CommandType.Text;
                        command.ExecuteNonQuery();
                    }

                    log().WriteInformation(string.Format("Added BatchNumber column"));
                });
            }
        }

        /// <summary>Generates an SQL statement that, when exectuted, will create the journal database table.</summary>
        /// <param name="schema">Desired schema name supplied by configuration or <c>NULL</c></param>
        /// <param name="table">Desired table name</param>
        /// <returns>A <c>CREATE TABLE</c> SQL statement</returns>
        protected virtual string CreateTableSql(string schema, string table)
        {
            var tableName = CreateTableName(schema, table);
            var primaryKeyConstraintName = CreatePrimaryKeyName(table);

            return string.Format(@"create table {0} (
	[Id] int identity(1,1) not null constraint {1} primary key,
	[ScriptName] nvarchar(255) not null,
	[Applied] datetime not null,
    [BatchNumber] int not null
)", tableName, primaryKeyConstraintName);
        }

        protected virtual string AddBatchNumberColumn(string schema, string table)
        {
            var tableName = CreateTableName(schema, table);

            return string.Format(@"ALTER TABLE {0}  ADD [BatchNumber] INT NOT NULL DEFAULT(0) ", tableName);

        }

        /// <summary>Combine the <c>schema</c> and <c>table</c> values into an appropriately-quoted identifier for the journal table.</summary>
        /// <param name="schema">Desired schema name supplied by configuration or <c>NULL</c></param>
        /// <param name="table">Desired table name</param>
        /// <returns>Quoted journal table identifier</returns>
        protected virtual string CreateTableName(string schema, string table)
        {
            return string.IsNullOrEmpty(schema)
                ? SqlObjectParser.QuoteSqlObjectName(table)
                : SqlObjectParser.QuoteSqlObjectName(schema) + "." + SqlObjectParser.QuoteSqlObjectName(table);
        }

        /// <summary>Convert the <c>table</c> value into an appropriately-quoted identifier for the journal table's unique primary key.</summary>
        /// <param name="table">Desired table name</param>
        /// <returns>Quoted journal table primary key identifier</returns>
        protected virtual string CreatePrimaryKeyName(string table)
        {
            return SqlObjectParser.QuoteSqlObjectName("PK_" + table + "_Id");
        }

        private bool DoesTableExist()
        {
            return connectionManager().ExecuteCommandsWithManagedConnection(dbCommandFactory =>
            {
                try
                {
                    using (var command = dbCommandFactory())
                    {
                        return VerifyTableExistsCommand(command, table, schema);
                    }
                }
                catch (SqlException)
                {
                    return false;
                }
                catch (DbException)
                {
                    return false;
                }
            });
        }

        /// <summary>
        /// Check if the batchNumber column exists
        /// </summary>
        /// <returns></returns>
        private bool DoesBatchNumberColumnExists()
        {
            return connectionManager().ExecuteCommandsWithManagedConnection(dbCommandFactory =>
            {
                try
                {
                    using (var command = dbCommandFactory())
                    {
                        return VerifyBatchNumberColumnExistsCommand(command, table, schema);
                    }
                }
                catch (SqlException)
                {
                    return false;
                }
                catch (DbException)
                {
                    return false;
                }
            });
        }

        /// <summary>Verify, using database-specific queries, if the table exists in the database.</summary>
        /// <param name="command">The <c>IDbCommand</c> to be used for the query</param>
        /// <param name="tableName">The name of the table</param>
        /// <param name="schemaName">The schema for the table</param>
        /// <returns>True if table exists, false otherwise</returns>
        protected virtual bool VerifyTableExistsCommand(IDbCommand command, string tableName, string schemaName)
        {
            command.CommandText = string.IsNullOrEmpty(schema)
                            ? string.Format("select 1 from information_schema.tables where TABLE_NAME = '{0}'", tableName)
                            : string.Format("select 1 from information_schema.tables where TABLE_NAME = '{0}' and TABLE_SCHEMA = '{1}'", tableName, schemaName);
            command.CommandType = CommandType.Text;
            var result = command.ExecuteScalar() as int?;
            return result == 1;
        }

        protected virtual bool VerifyBatchNumberColumnExistsCommand(IDbCommand command, string tableName, string schemaName)
        {
            command.CommandText = string.IsNullOrEmpty(schema)
                            ? string.Format("select 1 from information_schema.COLUMNS where COLUMN_NAME ='BatchNumber' and TABLE_NAME='{0}'", tableName)
                            : string.Format("select 1 from information_schema.COLUMNS where COLUMN_NAME ='BatchNumber' and TABLE_NAME='{0}' and TABLE_SCHEMA = '{1}'", tableName, schemaName);
            command.CommandType = CommandType.Text;
            var result = command.ExecuteScalar() as int?;
            return result == 1;
        }

    }
}