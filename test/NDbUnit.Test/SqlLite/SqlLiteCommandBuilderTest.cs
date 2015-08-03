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

using System.Collections.Generic;
using System.Data.SQLite;
using NDbUnit.Core.SqlLite;
using NDbUnit.Core;
using NUnit.Framework;

namespace NDbUnit.Test.SqlClient
{
    [Category(TestCategories.SqliteTests)]
    [TestFixture]
    class SqlLiteCommandBuilderTest : NDbUnit.Test.Common.DbCommandBuilderTestBase
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

        public override IList<string> ExpectedDeleteAllCommands
        {
            get
            {
                return new List<string>()
                {
                    "DELETE FROM [UserRole]",
                    "DELETE FROM [Role]",
                    "DELETE FROM [User]",
                };
            }
        }

        public override IList<string> ExpectedDeleteCommands
        {
            get
            {
                return new List<string>()
                {
                    "DELETE FROM [UserRole] WHERE [UserID]=@p1 AND [RoleID]=@p2",
                    "DELETE FROM [Role] WHERE [ID]=@p1",
                    "DELETE FROM [User] WHERE [ID]=@p1",
                };
            }
        }

        public override IList<string> ExpectedInsertCommands
        {
            get
            {
                return new List<string>()
                {
                    "INSERT INTO [UserRole]([UserID], [RoleID]) VALUES(@p1, @p2)",
                    "INSERT INTO [Role]([Name], [Description]) VALUES(@p1, @p2)",
                    "INSERT INTO [User]([FirstName], [LastName], [Age], [SupervisorID]) VALUES(@p1, @p2, @p3, @p4)",
                };

            }
        }

        public override IList<string> ExpectedInsertIdentityCommands
        {
            get
            {
                return new List<string>()
                {
                    "INSERT INTO [Role]([ID], [Name], [Description]) VALUES(@p1, @p2, @p3)",
                    "INSERT INTO [User]([ID], [FirstName], [LastName], [Age], [SupervisorID]) VALUES(@p1, @p2, @p3, @p4, @p5)",
                    "INSERT INTO [UserRole]([UserID], [RoleID]) VALUES(@p1, @p2)"
                };
            }
        }

        public override IList<string> ExpectedSelectCommands
        {
            get
            {
                return new List<string>()
                {
                    "SELECT [UserID], [RoleID] FROM [UserRole]",
                    "SELECT [ID], [Name], [Description] FROM [Role]",
                    "SELECT [ID], [FirstName], [LastName], [Age], [SupervisorID] FROM [User]",
                };
            }
        }

        public override IList<string> ExpectedUpdateCommands
        {
            get
            {
                return new List<string>()
                {
                    "UPDATE [UserRole] SET [UserID]=@p2, [RoleID]=@p4 WHERE [UserID]=@p1 AND [RoleID]=@p3",
                    "UPDATE [Role] SET [Name]=@p2, [Description]=@p3 WHERE [ID]=@p1",
                    "UPDATE [User] SET [FirstName]=@p2, [LastName]=@p3, [Age]=@p4, [SupervisorID]=@p5 WHERE [ID]=@p1",
                };
            }
        }

        protected override IDisposableDbCommandBuilder GetDbCommandBuilder()
        {
            var connectionManager = new DbConnectionManager<SQLiteConnection>(DbConnections.SqlLiteConnectionString);
            return new DisposableDbCommandBuilder<SQLiteConnection>(connectionManager, new SqlLiteDbCommandBuilder(connectionManager));
        }

        protected override string GetXmlSchemaFilename()
        {
            return XmlTestFiles.Sqlite.XmlSchemaFile;
        }

    }
}
