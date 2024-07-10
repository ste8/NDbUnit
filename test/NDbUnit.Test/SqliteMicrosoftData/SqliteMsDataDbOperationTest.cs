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
using NDbUnit.Core.SqlLite;
using System.Data;
using System.Data.SQLite;
using NDbUnit.Core;
using NUnit.Framework;
using System.Data.Common;

namespace NDbUnit.Test.SqliteMicrosoftData
{
    [Category(TestCategories.SqliteTests)]
    [TestFixture]
    public class SqliteMsDataDbOperationTest : NDbUnit.Test.Common.DbOperationTestBase
    {
        protected override IDbCommandBuilder GetCommandBuilder()
        {
            return new SqlLiteDbCommandBuilder(new DbConnectionManager<SQLiteConnection>(DbConnections.SqliteMsDataConnectionString));
        }

        protected override NDbUnit.Core.IDbOperation GetDbOperation()
        {
            return new SqlLiteDbOperation();
        }

        protected override DbCommand GetResetIdentityColumnsDbCommand(DataTable table, DataColumn column)
        {
            String sql = String.Format("delete from sqlite_sequence where name = '{0}'", table.TableName);
            return new SQLiteCommand(sql, (SQLiteConnection)_commandBuilder.Connection);
        }

        protected override string GetXmlFilename()
        {
            return XmlTestFiles.Sqlite.XmlFile;
        }

        protected override string GetXmlModifyFilename()
        {
            return XmlTestFiles.Sqlite.XmlModFile;
        }

        protected override string GetXmlRefeshFilename()
        {
            return XmlTestFiles.Sqlite.XmlRefreshFile;
        }

        protected override string GetXmlSchemaFilename()
        {
            return XmlTestFiles.Sqlite.XmlSchemaFile;
        }

    }
}
