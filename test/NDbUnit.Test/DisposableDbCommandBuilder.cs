using System.Data;
using System.IO;

using NDbUnit.Core;

namespace NDbUnit.Test
{
    public class DisposableDbCommandBuilder<TDbConnection> : IDisposableDbCommandBuilder where TDbConnection : class, IDbConnection, new()
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

        public IDbConnection Connection
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

        public IDbCommand GetSelectCommand(string tableName)
        {
            return _commandBuilder.GetSelectCommand(tableName);
        }

        public IDbCommand GetInsertCommand(string tableName)
        {
            return _commandBuilder.GetInsertCommand(tableName);
        }

        public IDbCommand GetInsertIdentityCommand(string tableName)
        {
            return _commandBuilder.GetInsertIdentityCommand(tableName);
        }

        public IDbCommand GetDeleteCommand(string tableName)
        {
            return _commandBuilder.GetDeleteCommand(tableName);
        }

        public IDbCommand GetDeleteAllCommand(string tableName)
        {
            return _commandBuilder.GetDeleteAllCommand(tableName);
        }

        public IDbCommand GetUpdateCommand(string tableName)
        {
            return _commandBuilder.GetUpdateCommand(tableName);
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
