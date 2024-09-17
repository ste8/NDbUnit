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
using Microsoft.Data.Sqlite;
using NDbUnit.SqliteMicrosoftData;

namespace NDbUnit.Core.SqliteMicrosoftData
{
    public class SqliteMsDataDbOperation : DbOperation
    {
        protected override DbDataAdapter CreateDbDataAdapter()
        {
            return new CustomSqliteDataAdapter();
        }

        protected override DbCommand CreateDbCommand(string cmdText)
        {
            return new SqliteCommand(cmdText);
        }

        /// <summary>
        /// SQLite doesn't need any changes to insert PK values.
        /// </summary>
        /// <param name="dataTable"></param>
        /// <param name="dbCommand"></param>
        /// <param name="dbTransaction"></param>
        protected override void OnInsertIdentity(DataTable dataTable, DbCommand dbCommand, DbTransaction dbTransaction)
        {
            OnInsert(dataTable, dbCommand, dbTransaction);
        }

        /// <summary>
        /// Creates an object to activate or deactivate identity insert
        /// </summary>
        /// <param name="tableName">The table name to activate the identity insert for</param>
        /// <param name="dbTransaction">The current transaction</param>
        /// <returns>The new object that - when disposed - deactivates the identity insert</returns>
        public override IDisposable ActivateInsertIdentity(string tableName, DbTransaction dbTransaction)
        {
            return null;
        }

        /// <summary>
        /// Disable the constraints for the whole database
        /// </summary>
        /// <param name="dataSet">The <see cref="DataSet"/> containing all the tables where the constraints must be disabled</param>
        /// <param name="transaction">The transaction used while processing data with disabled constraints</param>
        protected override void DisableAllTableConstraints(DataSet dataSet, DbTransaction transaction)
        {
            using (var sqlCommand = (SqliteCommand)CreateDbCommand("PRAGMA ignore_check_constraints = true"))
            {
                sqlCommand.Connection = (SqliteConnection)transaction.Connection;
                sqlCommand.Transaction = (SqliteTransaction)transaction;
                sqlCommand.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Enable the constraints for the whole database
        /// </summary>
        /// <param name="dataSet">The <see cref="DataSet"/> containing all the tables where the constraints must be enabled</param>
        /// <param name="transaction">The transaction used while processing data with enabled constraints</param>
        protected override void EnableAllTableConstraints(DataSet dataSet, DbTransaction transaction)
        {
            using (var sqlCommand = (SqliteCommand)CreateDbCommand("PRAGMA ignore_check_constraints = false"))
            {
                sqlCommand.Connection = (SqliteConnection)transaction.Connection;
                sqlCommand.Transaction = (SqliteTransaction)transaction;
                sqlCommand.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Disable a single tables constraints
        /// </summary>
        /// <param name="dataTable">The table for which the constraints must be disabled</param>
        /// <param name="dbTransaction">The transaction used while processing data with disabled constraints</param>
        /// <exception cref="NotSupportedException">This method isn't supported for SQLite</exception>
        protected override void DisableTableConstraints(DataTable dataTable, DbTransaction dbTransaction)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Enable a single tables constraints
        /// </summary>
        /// <param name="dataTable">The table for which the constraints must be enabled</param>
        /// <param name="dbTransaction">The transaction used while processing data with enabled constraints</param>
        /// <exception cref="NotSupportedException">This method isn't supported for SQLite</exception>
        protected override void EnableTableConstraints(DataTable dataTable, DbTransaction dbTransaction)
        {
            throw new NotSupportedException();
        }
    }
}
