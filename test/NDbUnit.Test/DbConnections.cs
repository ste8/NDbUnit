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

using System.Configuration;

namespace NDbUnit.Test
{
    public static class DbConnections
    {
        public static string PostgresqlConnectionString
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["PostgresqlConnectionString"].ConnectionString;
            }
        }
        public static string MySqlConnectionString
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["MysqlConnectionString"].ConnectionString;
            }
        }

        public static string OleDbConnectionString
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["OleDbConnectionString"].ConnectionString;
            }
        }

        public static string SqlCeConnectionString
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["SqlCeConnectionString"].ConnectionString;
            }
        }

        public static string SqlConnectionString
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["SqlConnectionString"].ConnectionString;
            }
        }

        public static string SqlScriptTestsConnectionString
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["SqlScriptTestsConnectionString"].ConnectionString;
            }
        }

        public static string SqlLiteConnectionString
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["SqlLiteConnectionString"].ConnectionString;
            }
        }

        public static string SqliteMsDataConnectionString
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["SqliteMsDataConnectionString"].ConnectionString;
            }
        }

        public static string SqlLiteInMemConnectionString
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["SqlLiteInMemConnectionString"].ConnectionString;
            }
        }

        public static string OracleClientConnectionString
        {
            get
            {
                return ConfigurationManager.ConnectionStrings["OracleClientConnectionString"].ConnectionString;
            }
        }
    }
}
