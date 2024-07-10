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
using System.Data;
using Microsoft.Data.Sqlite;

namespace NDbUnit.Core.SqliteMicrosoftData
{
    public class SqliteMsDataDbUnitTest : NDbUnitTest<SqliteConnection>
    {
        public SqliteMsDataDbUnitTest(string connectionString)
            : base(connectionString)
        {
        }

        public SqliteMsDataDbUnitTest(SqliteConnection connection)
            : base(connection)
        {
        }

        protected override DbDataAdapter CreateDataAdapter(DbCommand command)
        {
            throw new NotImplementedException();
            //return new SQLiteDataAdapter((SqliteConnection)command);
        }

        protected override IDbCommandBuilder CreateDbCommandBuilder(DbConnectionManager<SqliteConnection> connectionManager)
        {
            return new SqliteMsDataDbCommandBuilder(connectionManager);
        }
        
        //protected override IDbCommandBuilder CreateDbCommandBuilder(DbConnection connection)
        //{
        //    return new SqliteMsDataDbCommandBuilder(connection);
        //}

        //protected override IDbCommandBuilder CreateDbCommandBuilder(string connectionString)
        //{
        //    return new SqliteMsDataDbCommandBuilder(connectionString);
        //}

        protected override IDbOperation CreateDbOperation()
        {
            return new SqliteMsDataDbOperation();
        }

    }

    [Obsolete("Use SqliteMsDataDbUnitTest class in place of this.")]
    public class SqlLiteUnitTest : SqliteMsDataDbUnitTest
    {
        public SqlLiteUnitTest(string connectionString) : base(connectionString)
        {
        }

        public SqlLiteUnitTest(SqliteConnection connection) : base(connection)
        {
        }
    }
}
