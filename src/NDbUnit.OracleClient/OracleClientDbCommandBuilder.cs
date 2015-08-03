/*
 *
 * NDbUnit
 * Copyright (C) 2005 - 2015
 * https://github.com/NDbUnit/NDbUnit
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
using NDbUnit.Core;
using Oracle.DataAccess.Client;

namespace NDbUnit.OracleClient
{
    public class OracleClientDbCommandBuilder : DbCommandBuilder<OracleConnection>
    {
        public OracleClientDbCommandBuilder(DbConnectionManager<OracleConnection> connectionManager)
            : base(connectionManager)
        {
        }

        public override string QuotePrefix
        {
            get { return "\""; }
        }

        public override string QuoteSuffix
        {
            get { return QuotePrefix; }
        }

        protected override DbCommand CreateDbCommand()
        {
            OracleCommand command = new OracleCommand();
            return command;
        }

        protected override DbCommand CreateInsertCommand(DbTransaction transaction, DbCommand selectCommand, string tableName)
        {
            int count = 1;
            bool notFirstColumn = false;
            StringBuilder sb = new StringBuilder();
            sb.Append(String.Format("INSERT INTO {0}(", TableNameHelper.FormatTableName(tableName, QuotePrefix, QuoteSuffix)));
            StringBuilder sbParam = new StringBuilder();
            IDataParameter sqlParameter;
            DbCommand sqlInsertCommand = CreateDbCommand();
            foreach (DataRow dataRow in _dataTableSchema.Rows)
            {
                if (notFirstColumn)
                {
                    sb.Append(", ");
                    sbParam.Append(", ");
                }

                notFirstColumn = true;

                sb.Append(QuotePrefix + dataRow["ColumnName"] + QuoteSuffix);
                sbParam.Append(GetParameterDesignator(count));

                sqlParameter = CreateNewSqlParameter(count, dataRow);
                sqlInsertCommand.Parameters.Add(sqlParameter);

                ++count;
            }

            sb.Append(String.Format(") VALUES({0})", sbParam));

            sqlInsertCommand.CommandText = sb.ToString();
            ((OracleCommand)sqlInsertCommand).BindByName = true;

            return sqlInsertCommand;
        }

        protected override DbCommand CreateUpdateCommand(DbTransaction transaction, DbCommand selectCommand, string tableName)
        {
            var command = base.CreateUpdateCommand(transaction, selectCommand, tableName);
            ((OracleCommand) command).BindByName = true;
            return command;
        }

        protected override DbCommand CreateInsertIdentityCommand(DbTransaction transaction, DbCommand selectCommand, string tableName)
        {
            var command = base.CreateInsertIdentityCommand(transaction, selectCommand, tableName);
            ((OracleCommand)command).BindByName = true;
            return command;
        }

        protected override DbCommand CreateDeleteCommand(DbTransaction transaction, DbCommand selectCommand, string tableName)
        {
            var command = base.CreateDeleteCommand(transaction, selectCommand, tableName);
            ((OracleCommand)command).BindByName = true;
            return command;
        }

        protected override DbCommand CreateSelectCommand(DbTransaction transaction, DataSet ds, string tableName)
        {
            var command = base.CreateSelectCommand(transaction, ds, tableName);
            ((OracleCommand)command).BindByName = true;
            return command;
        }

        protected override IDataParameter CreateNewSqlParameter(int index, DataRow dataRow)
        {
            return new OracleParameter("p" + index, (Oracle.DataAccess.Client.OracleDbType)dataRow["ProviderType"],
                                      (int)dataRow["ColumnSize"], (string)dataRow["ColumnName"]);
        }

        protected override DbConnection GetConnection(string connectionString)
        {
            return new OracleConnection(connectionString);
        }

        protected override string GetIdentityColumnDesignator()
        {
            return "IsAutoIncrement";
        }

        protected override string GetParameterDesignator(int count)
        {
            return ":p" + count;
        }

    }
}
