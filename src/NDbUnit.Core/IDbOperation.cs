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

using System.Data;
using System.Data.Common;

namespace NDbUnit.Core
{
    public interface IDbOperation
    {
        void Insert(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction);
        void InsertIdentity(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction);
        void Delete(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction);
        void DeleteAll(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction);
        void Update(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction);
        void Refresh(DataSet ds, IDbCommandBuilder dbCommandBuilder, DbTransaction dbTransaction);
    }
}