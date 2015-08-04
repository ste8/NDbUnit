/*
 *
 * NDbUnit
 * Copyright (C) 2005 - 2015
 * https://github.com/fubar-coder/NDbUnit
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

using System;
using System.Collections.Generic;
using NDbUnit.Core;
using Npgsql;
using System.Data;
using System.Data.Common;

namespace NDbUnit.Postgresql
{
    public class PostgresqlDbOperation : DbOperation
    {
        public override string QuotePrefix
        {
            get { return "\""; }
        }

        public override string QuoteSuffix
        {
            get { return QuotePrefix; }
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

        protected override DbCommand CreateDbCommand(string cmdText)
        {
            return new NpgsqlCommand(cmdText);
        }

        protected override DbDataAdapter CreateDbDataAdapter()
        {
            return new NpgsqlDataAdapter();
        }

        protected override void DisableTableConstraints(DataTable dataTable, DbTransaction dbTransaction)
        {
            enableDisableTableConstraints(false, dataTable.TableName, dbTransaction);
        }

        protected override void EnableTableConstraints(DataTable dataTable, DbTransaction dbTransaction)
        {
            enableDisableTableConstraints(true, dataTable.TableName, dbTransaction);
        }

        protected override void OnInsertIdentity(DataTable dataTable, DbCommand dbCommand, DbTransaction dbTransaction)
        {
            //throw new NotSupportedException("OnInsertIdentity not supported!");

            DbTransaction sqlTransaction = dbTransaction;

            //DisableTableConstraints(dataTable, dbTransaction);

            try
            {
                DbDataAdapter sqlDataAdapter = CreateDbDataAdapter();
                try
                {
                    sqlDataAdapter.InsertCommand = dbCommand;
                    sqlDataAdapter.InsertCommand.Connection = sqlTransaction.Connection;
                    sqlDataAdapter.InsertCommand.Transaction = sqlTransaction;

                    ((DbDataAdapter)sqlDataAdapter).Update(dataTable);
                }
                finally
                {
                    var disposable = sqlDataAdapter as IDisposable;
                    if (disposable != null)
                        disposable.Dispose();
                }
            }

            finally
            {
                foreach (DataColumn column in dataTable.Columns)
                {
                    if (column.AutoIncrement)
                    {
                        using (
                            DbCommand selectMaxCommand =
                                CreateDbCommand(
                                    string.Format(
                                        "SELECT MAX({0}{2}{1}) FROM {3}",
                                        QuotePrefix,
                                        QuoteSuffix,
                                        column.ColumnName,
                                        TableNameHelper.FormatTableName(dataTable.TableName, QuotePrefix, QuoteSuffix))))
                        {
                            selectMaxCommand.Connection = sqlTransaction.Connection;
                            selectMaxCommand.Transaction = sqlTransaction;
                            int count = (int)selectMaxCommand.ExecuteScalar();

                            using (DbCommand sqlCommand =
                                CreateDbCommand(
                                    string.Format(
                                        "ALTER SEQUENCE \"{0}_{1}_seq\" RESTART WITH {2}",
                                        dataTable.TableName,
                                        column.ColumnName,
                                        count)))
                            {
                                sqlCommand.Connection = sqlTransaction.Connection;
                                sqlCommand.Transaction = sqlTransaction;
                                sqlCommand.ExecuteNonQuery();
                            }
                        }
                        break;
                    }
                }

                //EnableTableConstraints(dataTable, dbTransaction);
            }



        }

        private void enableDisableTableConstraints(bool enableConstraint, string tableName, DbTransaction dbTransaction)
        {
            var queryEnables =
                string.Format("ALTER TABLE \"{0}\" {1} TRIGGER ALL",
                              tableName,
                              enableConstraint ? "ENABLE" : "DISABLE");
            using (var dbCommand = new NpgsqlCommand
            {
                CommandText = queryEnables,
                Connection = (NpgsqlConnection)dbTransaction.Connection,
                Transaction = (NpgsqlTransaction)dbTransaction
            })
            {
                dbCommand.ExecuteNonQuery();
            }
        }
    }
}
