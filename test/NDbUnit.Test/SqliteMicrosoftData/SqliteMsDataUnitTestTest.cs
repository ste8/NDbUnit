﻿/*
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

using System.Collections.Generic;
using NDbUnit.Core;
using System.Data;
using System.Data.SQLite;
using System.IO;
using NDbUnit.Core.SqlLite;
using NUnit.Framework;
using System.Data.Common;

namespace NDbUnit.Test.SqliteMicrosoftData
{
    [Category(TestCategories.SqliteTests)]
    [TestFixture]
    public class SqliteMsDataUnitTestTest : NDbUnit.Test.Common.DbUnitTestTestBase
    {
        public override IList<string> ExpectedDataSetTableNames
        {
            get
            {
                return new List<string>()
                {
                    "UserRole", "Role", "User"
                };
            }
        }

        protected override IUnitTestStub GetUnitTestStub()
        {
            return new SqliteDbUnitTestStub(DbConnections.SqliteMsDataConnectionString);
        }

        protected override string GetXmlFilename()
        {
            return XmlTestFiles.Sqlite.XmlFile;
        }

        protected override string GetXmlSchemaFilename()
        {
            return XmlTestFiles.Sqlite.XmlSchemaFile;
        }

        protected class SqliteDbUnitTestStub : SqlLiteDbUnitTest, IUnitTestStub
        {
            public SqliteDbUnitTestStub(string connectionString)
                : base(connectionString)
            {
            }

            protected override IDbCommandBuilder CreateDbCommandBuilder(DbConnectionManager<SQLiteConnection> connectionManager )
            {
                return _mockDbCommandBuilder;
            }

            protected override IDbOperation CreateDbOperation()
            {
                return _mockDbOperation;
            }

            protected override DbDataAdapter CreateDataAdapter(DbCommand command)
            {
                return base.CreateDataAdapter(command);
            }

            protected override FileStream GetXmlSchemaFileStream(string xmlSchemaFile)
            {
                return _mockSchemaFileStream;
            }

            protected override FileStream GetXmlDataFileStream(string xmlFile)
            {
                return _mockDataFileStream;
            }

            protected override DataSet DS
            {
                get { return base.DS; }
            }

            public DataSet TestDataSet
            {
                get { return DS; }
            }
        }
    }

}

