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
using System.Collections;
using System.Data.Common;
using System.Linq;

namespace NDbUnit.Core
{
    public abstract class DbOperation : IDbOperation
    {
        public virtual string QuotePrefix { get { return ""; } }

        public virtual string QuoteSuffix { get { return ""; } }

        public void Delete(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction)
        {
            DisableAllTableConstraints(ds, dbTransaction);
            deleteCommon(ds, dbCommandBuilder, dbTransaction, false);
            EnableAllTableConstraints(ds, dbTransaction);
        }

        public void DeleteAll(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction)
        {
            DisableAllTableConstraints(ds, dbTransaction);
            deleteCommon(ds, dbCommandBuilder, dbTransaction, true);
            EnableAllTableConstraints(ds, dbTransaction);
        }

        /// <summary>
        /// Creates an object to activate or deactivate identity insert
        /// </summary>
        /// <param name="tableName">The table name to activate the identity insert for</param>
        /// <param name="dbTransaction">The current transaction</param>
        /// <returns>The new object that - when disposed - deactivates the identity insert</returns>
        public abstract IDisposable ActivateInsertIdentity(string tableName, DbTransaction dbTransaction);

        public void Insert(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction)
        {
            DisableAllTableConstraints(ds, dbTransaction);
            insertCommon(ds, dbCommandBuilder, dbTransaction, false);
            EnableAllTableConstraints(ds, dbTransaction);
        }

        public void InsertIdentity(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction)
        {
            DisableAllTableConstraints(ds, dbTransaction);
            insertCommon(ds, dbCommandBuilder, dbTransaction, true);
            EnableAllTableConstraints(ds, dbTransaction);
        }

        public void Refresh(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction)
        {

            DataSetTableIterator iterator = new DataSetTableIterator(ds, false);

            DisableAllTableConstraints(ds, dbTransaction);

            foreach (DataTable dataTable in iterator)
            {
                OnRefresh(ds, dbCommandBuilder, dbTransaction, dataTable.TableName, true);
            }

            EnableAllTableConstraints(ds, dbTransaction);
        }

        public void Update(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction)
        {
            DataSet dsCopy = ds.Copy();
            dsCopy.AcceptChanges();

            DataSetTableIterator iterator = new DataSetTableIterator(dsCopy, true);

            DisableAllTableConstraints(ds, dbTransaction);

            foreach (DataTable dataTable in iterator)
            {
                foreach (DataRow dataRow in dataTable.Rows)
                {
                    // Modify every table row.
                    dataRow.BeginEdit();
                    dataRow.EndEdit();
                }

                OnUpdate(dsCopy, dbCommandBuilder, dbTransaction, dataTable.TableName);
            }

            EnableAllTableConstraints(ds, dbTransaction);
        }

        protected DataRow CloneDataRow(DataTable dataTable, DataRow dataRow)
        {
            DataRow dataRowClone = dataTable.NewRow();
            for (int i = 0; i < dataRow.ItemArray.Length; ++i)
            {
                dataRowClone[i] = dataRow[i];
            }

            return dataRowClone;
        }

        protected abstract DbCommand CreateDbCommand(string cmdText);

        protected abstract DbDataAdapter CreateDbDataAdapter();

        /// <summary>
        /// Disable the constraints for the whole database
        /// </summary>
        /// <param name="dataSet">The <see cref="DataSet"/> containing all the tables where the constraints must be disabled</param>
        /// <param name="transaction">The transaction used while processing data with disabled constraints</param>
        protected virtual void DisableAllTableConstraints(DataSet dataSet, DbTransaction transaction)
        {
            foreach (DataTable table in dataSet.Tables)
            {
                DisableTableConstraints(table, transaction);
            }
        }

        /// <summary>
        /// Enable the constraints for the whole database
        /// </summary>
        /// <param name="dataSet">The <see cref="DataSet"/> containing all the tables where the constraints must be enabled</param>
        /// <param name="transaction">The transaction used while processing data with enabled constraints</param>
        protected virtual void EnableAllTableConstraints(DataSet dataSet, DbTransaction transaction)
        {
            foreach (DataTable table in dataSet.Tables)
            {
                EnableTableConstraints(table, transaction);
            }
        }

        /// <summary>
        /// Disable a single tables constraints
        /// </summary>
        /// <param name="dataTable">The table for which the constraints must be disabled</param>
        /// <param name="dbTransaction">The transaction used while processing data with disabled constraints</param>
        protected virtual void DisableTableConstraints(DataTable dataTable, DbTransaction dbTransaction)
        {
            //base class implementation does NOTHING in this method, derived classes must override as needed
        }

        /// <summary>
        /// Enable a single tables constraints
        /// </summary>
        /// <param name="dataTable">The table for which the constraints must be enabled</param>
        /// <param name="dbTransaction">The transaction used while processing data with enabled constraints</param>
        protected virtual void EnableTableConstraints(DataTable dataTable, DbTransaction dbTransaction)
        {
            //base class implementation does NOTHING in this method, derived classes must override as needed
        }

        protected bool IsPrimaryKeyValueEqual(DataRow dataRow1, DataRow dataRow2, DataColumn[] primaryKey)
        {
            if (primaryKey.Length == 0)
            {
                return false;
            }

            for (int i = 0; i < primaryKey.Length; ++i)
            {
                DataColumn dataColumn = primaryKey[i];
                // Primary key column value is not equal.
                if (!dataRow1[dataColumn.ColumnName].Equals(dataRow2[dataColumn.ColumnName]))
                {
                    return false;
                }
            }

            return true;
        }

        protected virtual void OnDelete(DataTable dataTable, DbCommand dbCommand, DbTransaction dbTransaction)
        {
            DbTransaction sqlTransaction = dbTransaction;

            DbDataAdapter sqlDataAdapter = CreateDbDataAdapter();
            try
            {
                sqlDataAdapter.DeleteCommand = dbCommand;
                sqlDataAdapter.DeleteCommand.Connection = sqlTransaction.Connection;
                sqlDataAdapter.DeleteCommand.Transaction = sqlTransaction;

                ((DbDataAdapter)sqlDataAdapter).Update(dataTable);
            }
            finally
            {
                var disposable = sqlDataAdapter as IDisposable;
                if (disposable != null)
                    disposable.Dispose();
            }
        }

        protected virtual void OnDeleteAll(DbCommand dbCommand, DbTransaction dbTransaction)
        {
            DbTransaction sqlTransaction = dbTransaction;

            DbCommand sqlCommand = dbCommand;
            sqlCommand.Connection = sqlTransaction.Connection;
            sqlCommand.Transaction = sqlTransaction;

            sqlCommand.ExecuteNonQuery();
        }

        protected virtual void OnInsert(DataTable dataTable, DbCommand dbCommand, DbTransaction dbTransaction)
        {
            DbTransaction sqlTransaction = dbTransaction;

            //DisableTableConstraints(dataTable, dbTransaction);

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

            //EnableTableConstraints(dataTable, dbTransaction);
        }

        protected virtual void OnInsertIdentity(DataTable dataTable, DbCommand dbCommand, DbTransaction dbTransaction)
        {
            DbTransaction sqlTransaction = dbTransaction;

            var hasAutoIncColumn = dataTable.Columns.Cast<DataColumn>().Any(x => x.AutoIncrement);
            var identityInsertGuard = hasAutoIncColumn ? ActivateInsertIdentity(dataTable.TableName, dbTransaction) : null;

            using (identityInsertGuard)
            {
                using (DbDataAdapter sqlDataAdapter = CreateDbDataAdapter())
                {
                    sqlDataAdapter.InsertCommand = dbCommand;
                    sqlDataAdapter.InsertCommand.Connection = sqlTransaction.Connection;
                    sqlDataAdapter.InsertCommand.Transaction = sqlTransaction;

                    sqlDataAdapter.Update(dataTable);
                }
            }
        }

        protected virtual void OnRefresh(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction, string tableName, bool insertIdentity)
        {
            DbTransaction sqlTransaction = dbTransaction;

            
            using (DbDataAdapter sqlDataAdapter = CreateDbDataAdapter())
            {
                using (var selectCommand = dbCommandBuilder.GetSelectCommand(dbTransaction, tableName))
                {
                    selectCommand.Connection = sqlTransaction.Connection;
                    selectCommand.Transaction = sqlTransaction;
                    sqlDataAdapter.SelectCommand = selectCommand;

                    DataSet dsDb = new DataSet();
                    // Query all records in the database table.
                    ((DbDataAdapter)sqlDataAdapter).Fill(dsDb, tableName);

                    DataSet dsUpdate = dbCommandBuilder.GetSchema().Clone();
                    dsUpdate.EnforceConstraints = false;

                    DataTable dataTable = ds.Tables[tableName];
                    DataTable dataTableDb = dsDb.Tables[tableName];
                    var schemaTable = dsUpdate.Tables[tableName];

                    if (dataTableDb.PrimaryKey.Length == 0)
                        dataTableDb.PrimaryKey = schemaTable
                            .PrimaryKey
                            .Select(c => dataTableDb.Columns[c.ColumnName])
                            .OrderBy(c => c.Ordinal)
                            .ToArray();

                    // Iterate all rows in the table.
                    foreach (DataRow dataRow in dataTable.Rows)
                    {
                        var pkValues = dataTableDb.PrimaryKey
                            .Select(c => dataRow[c.ColumnName])
                            .ToArray();
                        var dataRowDb = dataTableDb.Rows.Find(pkValues);
                        bool rowExists = dataRowDb != null;

                        DataRow dataRowNew = CloneDataRow(dsUpdate.Tables[tableName], dataRow);
                        dsUpdate.Tables[tableName].Rows.Add(dataRowNew);

                        // The row does not exist in the database.
                        if (rowExists)
                        {
                            dataRowNew.AcceptChanges();
                            if (dataTableDb.PrimaryKey.Length != dataTableDb.Columns.Count)
                                MarkRowAsModified(dataRowNew);
                        }
                    }

                    var hasAutoIncColumn = dataTable.Columns.Cast<DataColumn>().Any(x => x.AutoIncrement);
                    var identityInsertGuard = insertIdentity && hasAutoIncColumn ? ActivateInsertIdentity(dataTable.TableName, dbTransaction) : null;

                    using (identityInsertGuard)
                    {
                        DbCommand insertCommand =
                            insertIdentity && hasAutoIncColumn
                                ? dbCommandBuilder.GetInsertIdentityCommand(dbTransaction, tableName)
                                : dbCommandBuilder.GetInsertCommand(dbTransaction, tableName);
                        using (insertCommand)
                        {
                            insertCommand.Connection = sqlTransaction.Connection;
                            insertCommand.Transaction = sqlTransaction;
                            sqlDataAdapter.InsertCommand = insertCommand;

                            using (var updateCommand = dbCommandBuilder.GetUpdateCommand(dbTransaction, tableName))
                            {
                                updateCommand.Connection = sqlTransaction.Connection;
                                updateCommand.Transaction = sqlTransaction;
                                sqlDataAdapter.UpdateCommand = updateCommand;

                                //DisableTableConstraints(dsUpdate.Tables[tableName], dbTransaction);

                                sqlDataAdapter.Update(dsUpdate, tableName);

                                //EnableTableConstraints(dsUpdate.Tables[tableName], dbTransaction);
                            }
                        }
                    }
                }
            }
        }

        protected virtual void OnUpdate(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction, string tableName)
        {
            DbTransaction sqlTransaction = dbTransaction;

            //DisableTableConstraints(ds.Tables[tableName], dbTransaction);

            DbDataAdapter sqlDataAdapter = CreateDbDataAdapter();
            try
            {
                using (var updateCommand = dbCommandBuilder.GetUpdateCommand(dbTransaction, tableName))
                {
                    updateCommand.Connection = sqlTransaction.Connection;
                    updateCommand.Transaction = sqlTransaction;
                    sqlDataAdapter.UpdateCommand = updateCommand;

                    ((DbDataAdapter)sqlDataAdapter).Update(ds, tableName);
                }
            }
            finally
            {
                var disposable = sqlDataAdapter as IDisposable;
                if (disposable != null)
                    disposable.Dispose();
            }

            //EnableTableConstraints(ds.Tables[tableName], dbTransaction);
        }

        private void deleteCommon(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction,
                                          bool deleteAll)
        {
            Hashtable deletedTableColl = new Hashtable();

            DataSet dsSchema = dbCommandBuilder.GetSchema();

            DataSetTableIterator iterator = new DataSetTableIterator(dsSchema, true);

            foreach (DataTable dataTable in iterator)
            {
                deleteRecursive(ds, dataTable, dbCommandBuilder, dbTransaction, deletedTableColl, deleteAll);
            }
        }

        private void deleteRecursive(DataSet ds, DataTable dataTableSchema, IDbCommandBuilder dbCommandBuilder,
                                             DbTransaction dbTransaction, Hashtable deletedTableColl, bool deleteAll)
        {
            // Table has already been deleted from.
            if (deletedTableColl.ContainsKey(dataTableSchema.TableName))
            {
                return;
            }

            // [20060724 - sdh] Move here (from end of method) to avoid infinite-loop when package has relation to itself
            // Table was deleted from in the database.
            deletedTableColl[dataTableSchema.TableName] = null;

            DataRelationCollection childRelations = dataTableSchema.ChildRelations;
            // The table has children.
            if (null != childRelations)
            {
                foreach (DataRelation childRelation in childRelations)
                {
                    // Must delete the child table first.
                    deleteRecursive(ds, childRelation.ChildTable, dbCommandBuilder, dbTransaction, deletedTableColl,
                                    deleteAll);
                }
            }

            if (deleteAll)
            {
                using (DbCommand dbCommand = dbCommandBuilder.GetDeleteAllCommand(dbTransaction, dataTableSchema.TableName))
                {
                    try
                    {
                        OnDeleteAll(dbCommand, dbTransaction);
                    }
                    catch (DBConcurrencyException)
                    {
                        // Swallow deletion of zero records.
                    }
                }
            }
            else
            {
                DataTable dataTable = ds.Tables[dataTableSchema.TableName];
                DataTable dataTableCopy = dataTable.Copy();
                dataTableCopy.AcceptChanges();

                foreach (DataRow dataRow in dataTableCopy.Rows)
                {
                    // Delete the row.
                    dataRow.Delete();
                }

                using (DbCommand dbCommand = dbCommandBuilder.GetDeleteCommand(dbTransaction, dataTableSchema.TableName))
                {
                    try
                    {
                        OnDelete(dataTableCopy, dbCommand, dbTransaction);
                    }
                    catch (DBConcurrencyException)
                    {
                        // Swallow deletion of zero records.
                    }
                }
            }
        }

        private void insertCommon(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction,
                                          bool insertIdentity)
        {
            Hashtable insertedTableColl = new Hashtable();

            DataSet dsSchema = dbCommandBuilder.GetSchema();

            DataSetTableIterator iterator = new DataSetTableIterator(dsSchema, true);

            foreach (DataTable dataTable in iterator)
            {
                insertRecursive(ds, dataTable, dbCommandBuilder, dbTransaction, insertedTableColl, insertIdentity);
            }
        }

        private void insertRecursive(DataSet ds, DataTable dataTableSchema, IDbCommandBuilder dbCommandBuilder,
                                             DbTransaction dbTransaction, Hashtable insertedTableColl, bool insertIdentity)
        {
            // Table has already been inserted into.
            if (insertedTableColl.ContainsKey(dataTableSchema.TableName))
            {
                return;
            }
            // [20060724 - sdh] Move here (from end of method) to avoid infinite-loop when package has relation to itself
            // Table was inserted into in the database.
            insertedTableColl[dataTableSchema.TableName] = null;

            ConstraintCollection constraints = dataTableSchema.Constraints;
            if (null != constraints)
            {
                foreach (Constraint constraint in constraints)
                {
                    // The table has a foreign key constraint.
                    if (constraint.GetType() == typeof(ForeignKeyConstraint))
                    {
                        ForeignKeyConstraint fkConstraint = (ForeignKeyConstraint)constraint;
                        // Must insert parent table first.
                        insertRecursive(ds, fkConstraint.RelatedTable, dbCommandBuilder, dbTransaction,
                                        insertedTableColl, insertIdentity);
                    }
                }
            }
            // process parent tables first!
            DataRelationCollection parentRelations = dataTableSchema.ParentRelations;
            if (null != parentRelations)
            {
                foreach (DataRelation parentRelation in parentRelations)
                {
                    // Must insert parent table first.
                    insertRecursive(ds, parentRelation.ParentTable, dbCommandBuilder, dbTransaction, insertedTableColl,
                                    insertIdentity);
                }
            }

            DataRow dataRowClone = null;
            DataTable dataTable = ds.Tables[dataTableSchema.TableName];
            DataTable dataTableClone = dataTableSchema.Clone();
            foreach (DataRow dataRow in dataTable.Rows)
            {
                // Insert as a new row.
                dataRowClone = CloneDataRow(dataTableClone, dataRow);
                dataTableClone.Rows.Add(dataRowClone);
            }

            if (insertIdentity)
            {
                using (DbCommand dbCommand = dbCommandBuilder.GetInsertIdentityCommand(dbTransaction, dataTableSchema.TableName))
                {
                    OnInsertIdentity(dataTableClone, dbCommand, dbTransaction);
                }
            }
            else
            {
                using (DbCommand dbCommand = dbCommandBuilder.GetInsertCommand(dbTransaction, dataTableSchema.TableName))
                {
                    OnInsert(dataTableClone, dbCommand, dbTransaction);
                }
            }
        }

        private void MarkRowAsModified(DataRow dataRowNew)
        {
            dataRowNew.BeginEdit();
            dataRowNew.EndEdit();
        }




    }
}
