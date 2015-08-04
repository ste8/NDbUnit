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
using System.Data.Common;
using System.Linq;
using System.Text;

namespace NDbUnit.Core
{
    public class OpenConnectionGuard : IDisposable
    {
        private readonly bool _connectionWasOpen;

        private readonly bool _isExternalConnection;

        private DbConnection _connection;

        public OpenConnectionGuard(DbConnection connection)
            : this(connection, true)
        {
        }

        public OpenConnectionGuard(DbConnection connection, bool isExternalConnection)
        {
            _isExternalConnection = isExternalConnection;
            _connection = connection;
            _connectionWasOpen = _connection.State == System.Data.ConnectionState.Open;
            if (!_connectionWasOpen)
                _connection.Open();
        }

        public OpenConnectionGuard(IDbConnectionManager connectionManager)
            : this(connectionManager.Connection, connectionManager.HasExternallyManagedConnection)
        {
        }

        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (!_connectionWasOpen && !_isExternalConnection)
                        _connection.Close();
                }

                _connection = null;

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
