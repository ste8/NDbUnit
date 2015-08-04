/*
 *
 * NDbUnit
 * Copyright (C) 2005 - 2015
 * https://github.com/fubar-coder/NDbUnit
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

using System;
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
