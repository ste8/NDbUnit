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
using System.Collections.Generic;
using System.Text;

namespace NDbUnit.Core
{
    public static class TableNameHelper
    {
        private static readonly HashSet<string> _reservedForPostgres = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);

        public static string FormatTableName(string declaredTableName, string quotePrefix, string quoteSuffix)
        {
            if (IsPostgres(quotePrefix, quoteSuffix)) {
                return EscapeTableNameForPostgres(declaredTableName);
            }

            StringBuilder result = new StringBuilder();

            var tableNameElements = declaredTableName.Split(".".ToCharArray());

            bool firstElement = true;

            foreach (string s in tableNameElements)
            {
                StringBuilder temp = new StringBuilder();
                if (s != string.Empty)
                {
                    if (!s.StartsWith(quotePrefix))
                        temp.Append(quotePrefix);

                    temp.Append(s);

                    if (!s.EndsWith(quoteSuffix))
                        temp.Append(quoteSuffix);

                    if (firstElement)
                        result.Append(temp.ToString());
                    else
                        result.Append("." + temp);

                    firstElement = false;
                }
            }

            return result.ToString();
        }

        private static bool IsPostgres(string quotePrefix, string quoteSuffix)
        {
            bool result = quotePrefix == "\"" && quoteSuffix == "\"";
            return result;
        }

        public static string EscapeTableNameForPostgres(string name)
        {
            var resultName = Unescape(name);
            if (ShouldEscapeForPostgres(resultName)) {
                resultName = $"\"{resultName}\"";
            }
            return resultName;
        }

        public static string EscapeColumnNameForPostgres(string name)
        {
            var resultName = Unescape(name);
            if (ShouldEscapeForPostgres(resultName)) {
                resultName = $"\"{resultName}\"";
            }
            return resultName;
        }

        public static void SetReservedKeywordsForPostgres(IEnumerable<string> keywords)
        {
            _reservedForPostgres.Clear();
            foreach (var keyword in keywords) {
                _reservedForPostgres.Add(keyword);
            }
        }

        private static bool ShouldEscapeForPostgres(string tableName)
        {
            return _reservedForPostgres.Contains(tableName);
        }

        public static string Unescape(string name)
        {
            if (!IsEscaped(name)) return name;
            name = name.Substring(1, name.Length - 2);
            return name;
        }

        private static bool IsEscaped(string name)
        {
            bool isEscaped = (name.StartsWith("[") && name.EndsWith("]"))
                || (name.StartsWith("\"") && name.EndsWith("\""));
            return isEscaped;
        }

        public static string EscapeColumnName(string columnName, string quotePrefix, string quoteSuffix)
        {
            if (IsPostgres(quotePrefix, quoteSuffix)) {
                return EscapeColumnNameForPostgres(columnName);
            }
            return $"{quotePrefix}{columnName}{quoteSuffix}";
        }
    }
}
