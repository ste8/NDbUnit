using System;
using System.Data;
using System.Data.Common;

namespace NDbUnit.Core
{
    public class SqlServerInsertIdentity : IDisposable
    {
        private DbCommand _identityInsertOn;

        private DbCommand _identityInsertOff;

        private bool _disposedValue;

        public SqlServerInsertIdentity(string tableName, string quotePrefix, string quoteSuffix, Func<string, DbCommand> createDbCommand, DbTransaction transaction)
        {
            _identityInsertOn =
                createDbCommand(
                    "SET IDENTITY_INSERT " +
                    TableNameHelper.FormatTableName(tableName, quotePrefix, quoteSuffix) +
                    " ON");
            _identityInsertOn.Connection = transaction.Connection;
            _identityInsertOn.Transaction = transaction;

            _identityInsertOff =
                createDbCommand(
                    "SET IDENTITY_INSERT " +
                    TableNameHelper.FormatTableName(tableName, quotePrefix, quoteSuffix) +
                    " OFF");
            _identityInsertOff.Connection = transaction.Connection;
            _identityInsertOff.Transaction = transaction;

            EnableIdentityInsert();
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    DisableIdentityInsert();

                    _identityInsertOn.Dispose();
                    _identityInsertOff.Dispose();
                }

                _identityInsertOn = null;
                _identityInsertOff = null;

                _disposedValue = true;
            }
        }

        private void EnableIdentityInsert()
        {
            _identityInsertOn.ExecuteNonQuery();
        }

        private void DisableIdentityInsert()
        {
            _identityInsertOff.ExecuteNonQuery();
        }
    }
}
