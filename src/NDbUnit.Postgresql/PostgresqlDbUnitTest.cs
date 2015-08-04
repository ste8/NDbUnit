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

using NDbUnit.Core;
using Npgsql;
using System.Data;
using System.Data.Common;

namespace NDbUnit.Postgresql
{
    public class PostgresqlDbUnitTest : NDbUnitTest<NpgsqlConnection>
    {
        public PostgresqlDbUnitTest(NpgsqlConnection connection)
            : base(connection)
        {
        }

        public PostgresqlDbUnitTest(string connectionString)
            : base(connectionString)
        {
        }

        protected override DbDataAdapter CreateDataAdapter(DbCommand command)
        {
            var oda = new NpgsqlDataAdapter();
            oda.SelectCommand = (NpgsqlCommand)command;
            return oda;
        }

        protected override IDbCommandBuilder CreateDbCommandBuilder(DbConnectionManager<NpgsqlConnection> connectionManager )
        {
            var commandBuilder = new PostgresqlDbCommandBuilder(connectionManager);
            commandBuilder.CommandTimeOutSeconds = this.CommandTimeOut;
            return commandBuilder;
        }

        protected override IDbOperation CreateDbOperation()
        {
            return new PostgresqlDbOperation();
        }
    }
}
