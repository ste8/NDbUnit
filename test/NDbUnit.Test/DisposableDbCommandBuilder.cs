using System.Data;
using System.IO;

using NDbUnit.Core;

namespace NDbUnit.Test
{
    using System.Data.Common;

    public class DisposableDbCommandBuilder<TDbConnection> : IDisposableDbCommandBuilder where TDbConnection : DbConnection, new()
    {
        private readonly DbConnectionManager<TDbConnection> _connectionManager;

        private readonly DbCommandBuilder<TDbConnection> _commandBuilder;

        public DisposableDbCommandBuilder(DbConnectionManager<TDbConnection> connectionManager, DbCommandBuilder<TDbConnection> commandBuilder)
        {
            _connectionManager = connectionManager;
            _commandBuilder = commandBuilder;
        }

        public int CommandTimeOutSeconds
        {
            get { return _commandBuilder.CommandTimeOutSeconds; }
            set { _commandBuilder.CommandTimeOutSeconds = value; }
        }

        public string QuotePrefix
        {
            get { return _commandBuilder.QuotePrefix; }
        }

        public string QuoteSuffix
        {
            get { return _commandBuilder.QuoteSuffix; }
        }

        public DbConnection Connection
        {
            get { return _commandBuilder.Connection; }
        }

        public void ReleaseConnection()
        {
            _connectionManager.ReleaseConnection();
        }

        public DataSet GetSchema()
        {
            return _commandBuilder.GetSchema();
        }

        public void BuildCommands(string xmlSchemaFile)
        {
            _commandBuilder.BuildCommands(xmlSchemaFile);
        }

        public void BuildCommands(Stream xmlSchema)
        {
            _commandBuilder.BuildCommands(xmlSchema);
        }

        public DbCommand GetSelectCommand(DbTransaction transaction, string tableName)
        {
            return _commandBuilder.GetSelectCommand(transaction, tableName);
        }

        public DbCommand GetInsertCommand(DbTransaction transaction, string tableName)
        {
            return _commandBuilder.GetInsertCommand(transaction, tableName);
        }

        public DbCommand GetInsertIdentityCommand(DbTransaction transaction, string tableName)
        {
            return _commandBuilder.GetInsertIdentityCommand(transaction, tableName);
        }

        public DbCommand GetDeleteCommand(DbTransaction transaction, string tableName)
        {
            return _commandBuilder.GetDeleteCommand(transaction, tableName);
        }

        public DbCommand GetDeleteAllCommand(DbTransaction transaction, string tableName)
        {
            return _commandBuilder.GetDeleteAllCommand(transaction, tableName);
        }

        public DbCommand GetUpdateCommand(DbTransaction transaction, string tableName)
        {
            return _commandBuilder.GetUpdateCommand(transaction, tableName);
        }

        /// <summary>
        /// Führt anwendungsspezifische Aufgaben durch, die mit der Freigabe, der Zurückgabe oder dem Zurücksetzen von nicht verwalteten Ressourcen zusammenhängen.
        /// </summary>
        public void Dispose()
        {
            _connectionManager.ReleaseConnection();
        }
    }
}
