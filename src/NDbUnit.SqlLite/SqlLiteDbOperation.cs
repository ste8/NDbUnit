/*
 *
 * NDbUnit
 * Copyright (C)2005 - 2011
 * http://code.google.com/p/ndbunit
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
using System.Data.SQLite;

namespace NDbUnit.Core.SqlLite
{
    public class SqlLiteDbOperation : DbOperation
    {
        protected override DbDataAdapter CreateDbDataAdapter()
        {
            return new SQLiteDataAdapter();
        }

        protected override DbCommand CreateDbCommand(string cmdText)
        {
            return new SQLiteCommand(cmdText);
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
        /// Disable the constraints for the whole database
        /// </summary>
        /// <param name="dataSet">The <see cref="DataSet"/> containing all the tables where the constraints must be disabled</param>
        /// <param name="transaction">The transaction used while processing data with disabled constraints</param>
        protected override void DisableAllTableConstraints(DataSet dataSet, DbTransaction transaction)
        {
            using (var sqlCommand = (SQLiteCommand)CreateDbCommand("PRAGMA ignore_check_constraints = true"))
            {
                sqlCommand.Connection = (SQLiteConnection)transaction.Connection;
                sqlCommand.Transaction = (SQLiteTransaction)transaction;
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
            using (var sqlCommand = (SQLiteCommand)CreateDbCommand("PRAGMA ignore_check_constraints = false"))
            {
                sqlCommand.Connection = (SQLiteConnection)transaction.Connection;
                sqlCommand.Transaction = (SQLiteTransaction)transaction;
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
