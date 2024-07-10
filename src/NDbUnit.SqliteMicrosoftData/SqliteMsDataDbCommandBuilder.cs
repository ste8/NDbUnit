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
using System.Text;
using Microsoft.Data.Sqlite;

namespace NDbUnit.Core.SqliteMicrosoftData
{
    public class SqliteMsDataDbCommandBuilder : DbCommandBuilder<SqliteConnection>
    {
        private new DataTable _dataTableSchema;

        public SqliteMsDataDbCommandBuilder(DbConnectionManager<SqliteConnection> connectionManager )
            : base(connectionManager)
        {
        }

        //public SqliteMsDataDbCommandBuilder(DbConnection connection)
        //    : base(connection)
        //{
        //}

        public override string QuotePrefix
        {
            get { return "["; }
        }

        public override string QuoteSuffix
        {
            get { return "]"; }
        }

        protected override DbCommand CreateDbCommand()
        {
            var command = new SqliteCommand();

            if (CommandTimeOutSeconds != 0)
                command.CommandTimeout = CommandTimeOutSeconds;

            return command;
        }

        protected override DbCommand CreateDeleteAllCommand(DbTransaction transaction, string tableName)
        {
            return
                new SqliteCommand("DELETE FROM " + TableNameHelper.FormatTableName(tableName, QuotePrefix, QuoteSuffix));
        }

        protected override DbCommand CreateDeleteCommand(DbTransaction transaction, DbCommand selectCommand, string tableName)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("DELETE FROM " + TableNameHelper.FormatTableName(tableName, QuotePrefix, QuoteSuffix) + " WHERE ");

            SqliteCommand sqlDeleteCommand = CreateDbCommand() as SqliteCommand;

            int count = 1;
            DbParameter sqlParameter;
            foreach (DataRow dataRow in _dataTableSchema.Rows)
            {
                if (ColumnOKToInclude(dataRow))
                {
                    // A key column.
                    if ((bool)dataRow[SchemaColumns.IsKey])
                    {
                        if (count != 1)
                        {
                            sb.Append(" AND ");
                        }

                        sb.Append(QuotePrefix + dataRow[SchemaColumns.ColumnName] + QuoteSuffix);
                        sb.Append("=@p" + count);

                        sqlParameter = (SqliteParameter)CreateNewSqlParameter(count, dataRow);
                        sqlDeleteCommand.Parameters.Add(sqlParameter);

                        ++count;
                    }
                }
            }

            sqlDeleteCommand.CommandText = sb.ToString();

            return sqlDeleteCommand;
        }

        protected override DbCommand CreateInsertCommand(DbTransaction transaction, DbCommand selectCommand, string tableName)
        {
            int count = 1;
            bool notFirstColumn = false;
            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO " + TableNameHelper.FormatTableName(tableName, QuotePrefix, QuoteSuffix) + "(");
            StringBuilder sbParam = new StringBuilder();
            DbParameter sqlParameter = null;
            SqliteCommand sqlInsertCommand = CreateDbCommand() as SqliteCommand;
            foreach (DataRow dataRow in _dataTableSchema.Rows)
            {
                if (ColumnOKToInclude(dataRow))
                {
                    // Not an identity column.
                    if (!IsAutoIncrementing(dataRow))
                    {
                        if (notFirstColumn)
                        {
                            sb.Append(", ");
                            sbParam.Append(", ");
                        }

                        notFirstColumn = true;

                        sb.Append(QuotePrefix + dataRow[SchemaColumns.ColumnName] + QuoteSuffix);
                        sbParam.Append("@p" + count);

                        sqlParameter = (SqliteParameter)CreateNewSqlParameter(count, dataRow);
                        sqlInsertCommand.Parameters.Add(sqlParameter);

                        ++count;
                    }
                }
            }

            sb.Append(") VALUES(" + sbParam + ")");

            sqlInsertCommand.CommandText = sb.ToString();

            return sqlInsertCommand;
        }

        protected override DbCommand CreateInsertIdentityCommand(DbTransaction transaction, DbCommand selectCommand, string tableName)
        {
            int count = 1;
            bool notFirstColumn = false;
            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO " + TableNameHelper.FormatTableName(tableName, QuotePrefix, QuoteSuffix) + "(");
            StringBuilder sbParam = new StringBuilder();
            DbParameter sqlParameter = null;
            SqliteCommand sqlInsertIdentityCommand = CreateDbCommand() as SqliteCommand;
            foreach (DataRow dataRow in _dataTableSchema.Rows)
            {
                if (ColumnOKToInclude(dataRow))
                {
                    if (notFirstColumn)
                    {
                        sb.Append(", ");
                        sbParam.Append(", ");
                    }

                    notFirstColumn = true;

                    sb.Append(QuotePrefix + dataRow[SchemaColumns.ColumnName] + QuoteSuffix);
                    sbParam.Append("@p" + count);

                    sqlParameter = (SqliteParameter)CreateNewSqlParameter(count, dataRow);
                    sqlInsertIdentityCommand.Parameters.Add(sqlParameter);

                    ++count;
                }
            }

            sb.Append(") VALUES(" + sbParam + ")");

            sqlInsertIdentityCommand.CommandText = sb.ToString();

            return sqlInsertIdentityCommand;
        }


        protected override IDataParameter CreateNewSqlParameter(int index, DataRow dataRow)
        {
            var dataType = (Type)dataRow[SchemaColumns.DataType];
            var columnSize = (int)dataRow[SchemaColumns.ColumnSize];
            var columnName = (string)dataRow[SchemaColumns.ColumnName];
            var dbType = GetDbType(dataRow, dataType, columnSize);

            return new SqliteParameter("@p" + index, dbType, columnSize, columnName);
        }

        private static SqliteType GetDbType(DataRow dataRow, Type dataType, int columnSize)
        {
            //DbType dbType;
            //if (dataType == typeof(byte[]) && columnSize == 16)
            //{
            //    dbType = DbType.Guid;
            //}
            //else
            //{
            //    dbType = (DbType)dataRow[SchemaColumns.ProviderType];
            //}

            if (dataType == typeof(byte)) return SqliteType.Integer;
            if (dataType == typeof(Int16)) return SqliteType.Integer;
            if (dataType == typeof(Int32)) return SqliteType.Integer;
            if (dataType == typeof(Int64)) return SqliteType.Integer;
            if (dataType == typeof(string)) return SqliteType.Text;

            throw new Exception("DataType not handled: " + dataType.FullName);
        }

        protected override DbCommand CreateSelectCommand(DbTransaction transaction, DataSet ds, string tableName)
        {
            SqliteCommand sqlSelectCommand = CreateDbCommand() as SqliteCommand;

            bool notFirstColumn = false;
            StringBuilder sb = new StringBuilder("SELECT ");
            DataTable dataTable = ds.Tables[tableName];
            foreach (DataColumn dataColumn in dataTable.Columns)
            {
                if (notFirstColumn)
                {
                    sb.Append(", ");
                }

                notFirstColumn = true;

                sb.Append(QuotePrefix + dataColumn.ColumnName + QuoteSuffix);
            }

            sb.Append(" FROM ");
            sb.Append(TableNameHelper.FormatTableName(tableName, QuotePrefix, QuoteSuffix));

            sqlSelectCommand.CommandText = sb.ToString();
            sqlSelectCommand.Connection = (SqliteConnection)ConnectionManager.GetConnection();

            try
            {
                _dataTableSchema = GetSchemaTable(sqlSelectCommand);
            }
            catch (Exception e)
            {
                string message =
                    String.Format(
                        "SqlDbCommandBuilder.CreateSelectCommand(DataSet, string) failed for tableName = '{0}'",
                        tableName);
                throw new NDbUnitException(message, e);
            }

            return sqlSelectCommand;
        }

        protected override DbCommand CreateUpdateCommand(DbTransaction transaction, DbCommand selectCommand, string tableName)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("UPDATE " + TableNameHelper.FormatTableName(tableName, QuotePrefix, QuoteSuffix) + " SET ");

            SqliteCommand sqlUpdateCommand = CreateDbCommand() as SqliteCommand;

            int count = 1;
            bool notFirstKey = false;
            bool notFirstColumn = false;
            DbParameter sqlParameter = null;
            StringBuilder sbPrimaryKey = new StringBuilder();

            bool containsAllPrimaryKeys = true;
            foreach (DataRow dataRow in _dataTableSchema.Rows)
            {
                if (!(bool)dataRow[SchemaColumns.IsKey])
                {
                    containsAllPrimaryKeys = false;
                    break;
                }
            }

            foreach (DataRow dataRow in _dataTableSchema.Rows)
            {
                if (ColumnOKToInclude(dataRow))
                {
                    // A key column.
                    if ((bool)dataRow[SchemaColumns.IsKey])
                    {
                        if (notFirstKey)
                        {
                            sbPrimaryKey.Append(" AND ");
                        }

                        notFirstKey = true;

                        sbPrimaryKey.Append(QuotePrefix + dataRow[SchemaColumns.ColumnName] + QuoteSuffix);
                        sbPrimaryKey.Append("=@p" + count);

                        sqlParameter = (SqliteParameter)CreateNewSqlParameter(count, dataRow);
                        sqlUpdateCommand.Parameters.Add(sqlParameter);

                        ++count;
                    }


                    if (containsAllPrimaryKeys || !(bool)dataRow[SchemaColumns.IsKey])
                    {
                        if (notFirstColumn)
                        {
                            sb.Append(", ");
                        }

                        notFirstColumn = true;

                        sb.Append(QuotePrefix + dataRow[SchemaColumns.ColumnName] + QuoteSuffix);
                        sb.Append("=@p" + count);

                        sqlParameter = (SqliteParameter)CreateNewSqlParameter(count, dataRow);
                        sqlUpdateCommand.Parameters.Add(sqlParameter);

                        ++count;
                    }
                }
            }

            sb.Append(" WHERE " + sbPrimaryKey);

            sqlUpdateCommand.CommandText = sb.ToString();

            return sqlUpdateCommand;
        }

        protected override DbConnection GetConnection(string connectionString)
        {
            return new SqliteConnection(connectionString);
        }

        /// <summary>
        /// Since SQLite keys are auto incremented by default we need to check to see if the column
        /// is a key as well, since not all columns will be marked with AUTOINCREMENT
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        private bool IsAutoIncrementing(DataRow row)
        {
            return (bool)row[SchemaColumns.IsAutoIncrement];
        }

        private class SchemaColumns
        {
            public const string ColumnName = "ColumnName";
            public const string ColumnOrdinal = "ColumnOrdinal";
            public const string ColumnSize = "ColumnSize";
            public const string NumericalPrecision = "NumericalPrecision";
            public const string NumericalScale = "NumericalScale";
            public const string IsUnique = "IsUnique";
            public const string IsKey = "IsKey";
            public const string BaseServerName = "BaseServerName";
            public const string BaseCatalogName = "BaseCatalogName";
            public const string BaseColumnName = "BaseColumnName";
            public const string BaseSchemaName = "";
            public const string IsAutoIncrement = "IsAutoIncrement";
            public const string ProviderType = "ProviderType";
            public const string DataType = "DataType";
        }

    }
}
