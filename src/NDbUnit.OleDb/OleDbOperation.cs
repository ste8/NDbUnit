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
using System.Data;
using System.Data.Common;
using System.Data.OleDb;

namespace NDbUnit.Core.OleDb
{
    public class OleDbOperation : DbOperation
    {
        private OleDbType _oleOleDbType = OleDbType.NoDb;

        private string _oleDbTypeConnectionString;

        public override string QuotePrefix
        {
            get { return "["; }
        }

        public override string QuoteSuffix
        {
            get { return "]"; }
        }

        /// <summary>
        /// Creates an object to activate or deactivate identity insert
        /// </summary>
        /// <param name="tableName">The table name to activate the identity insert for</param>
        /// <param name="dbTransaction">The current transaction</param>
        /// <returns>The new object that - when disposed - deactivates the identity insert</returns>
        public override IDisposable ActivateInsertIdentity(string tableName, DbTransaction dbTransaction)
        {
            if (GetOleOleDbType(dbTransaction.Connection) == OleDbType.SqlServer)
                return new SqlServerInsertIdentity(tableName, QuotePrefix, QuoteSuffix, CreateDbCommand, dbTransaction);
            return null;
        }

        public OleDbType GetOleOleDbType(DbConnection connection)
        {
            if (_oleDbTypeConnectionString == null || connection.ConnectionString != _oleDbTypeConnectionString)
            {
                var csb = new OleDbConnectionStringBuilder(connection.ConnectionString);
                ParseConnectionStringBuilder(csb);
            }
            return _oleOleDbType;
        }

        protected override DbDataAdapter CreateDbDataAdapter()
        {
            return new OleDbDataAdapter();
        }

        protected override DbCommand CreateDbCommand(string cmdText)
        {
            return new OleDbCommand(cmdText);
        }

        protected override void OnInsertIdentity(DataTable dataTable, DbCommand dbCommand, DbTransaction dbTransaction)
        {
            if (GetOleOleDbType(dbTransaction.Connection) == OleDbType.SqlServer)
            {
                base.OnInsertIdentity(dataTable, dbCommand, dbTransaction);
            }
        }

        protected override void EnableTableConstraints(DataTable dataTable, DbTransaction dbTransaction)
        {
            if (GetOleOleDbType(dbTransaction.Connection) != OleDbType.SqlServer)
                return;

            using (DbCommand sqlCommand =
                CreateDbCommand(
                    "ALTER TABLE " +
                    TableNameHelper.FormatTableName(dataTable.TableName, QuotePrefix, QuoteSuffix) +
                    " CHECK CONSTRAINT ALL"))
            {
                sqlCommand.Connection = dbTransaction.Connection;
                sqlCommand.Transaction = dbTransaction;
                sqlCommand.ExecuteNonQuery();
            }
        }

        protected override void DisableTableConstraints(DataTable dataTable, DbTransaction dbTransaction)
        {
            if (GetOleOleDbType(dbTransaction.Connection) != OleDbType.SqlServer)
                return;

            using (DbCommand sqlCommand =
                CreateDbCommand(
                    "ALTER TABLE " +
                    TableNameHelper.FormatTableName(dataTable.TableName, QuotePrefix, QuoteSuffix) +
                    " NOCHECK CONSTRAINT ALL"))
            {
                sqlCommand.Connection = dbTransaction.Connection;
                sqlCommand.Transaction = dbTransaction;
                sqlCommand.ExecuteNonQuery();
            }
        }

        private void ParseConnectionStringBuilder(OleDbConnectionStringBuilder csb)
        {
            var provider = csb.Provider.ToLowerInvariant();
            if (string.Equals(provider, "SQLOLEDB", StringComparison.OrdinalIgnoreCase))
            {
                _oleOleDbType = OleDbType.SqlServer;
            }
            else if (provider.StartsWith("SQLNCLI", StringComparison.OrdinalIgnoreCase))
            {
                _oleOleDbType = OleDbType.SqlServer;
            }
        }
    }
}
