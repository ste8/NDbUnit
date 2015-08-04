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
using NDbUnit.Core;
using System.IO;
using NUnit.Framework;
using System.Data.Common;

namespace NDbUnit.Test.Common
{
    public abstract class DbOperationTestBase
    {
        protected IDbCommandBuilder _commandBuilder;

        protected IDbOperation _dbOperation;

        protected DataSet _dsData;

        protected string _xmlFile;

        [TestFixtureSetUp]
        public void _FixtureSetUp()
        {
            _commandBuilder = GetCommandBuilder();

            string xmlSchemaFile = GetXmlSchemaFilename();
            _xmlFile = GetXmlFilename();

            _commandBuilder.BuildCommands(xmlSchemaFile);

            DataSet dsSchema = _commandBuilder.GetSchema();
            _dsData = dsSchema.Clone();
            _dsData.ReadXml(ReadOnlyStreamFromFilename(_xmlFile));

            _dbOperation = GetDbOperation();
        }

        [SetUp]
        public void _SetUp()
        {
            _commandBuilder.Connection.Open();
        }

        [TearDown]
        public void _TearDown()
        {
            _commandBuilder.ReleaseConnection();
        }

        [Test]
        public void All_Test_Xml_Files_Comply_With_Test_Xsd_Schema()
        {
            DataSet ds = new DataSet();
            ds.ReadXmlSchema(ReadOnlyStreamFromFilename(GetXmlSchemaFilename()));
            ds.ReadXml(ReadOnlyStreamFromFilename(GetXmlFilename()));

            DataSet dsRefresh = new DataSet();
            dsRefresh.ReadXmlSchema(ReadOnlyStreamFromFilename(GetXmlSchemaFilename()));
            dsRefresh.ReadXml(ReadOnlyStreamFromFilename(GetXmlRefeshFilename()));

            DataSet dsModify = new DataSet();
            dsModify.ReadXmlSchema(ReadOnlyStreamFromFilename(GetXmlSchemaFilename()));
            dsModify.ReadXml(ReadOnlyStreamFromFilename(GetXmlRefeshFilename()));

        }

        [Test]
        public void Delete_Executes_Without_Exception()
        {
            using (var sqlTransaction = _commandBuilder.Connection.BeginTransaction())
            {
                try
                {
                    _dbOperation.Delete(_dsData, _commandBuilder, sqlTransaction);
                    sqlTransaction.Commit();
                }
                catch (Exception)
                {
                    if (sqlTransaction != null)
                    {
                        sqlTransaction.Rollback();
                    }

                    throw;
                }
            }
            Assert.IsTrue(true);
        }

        [Test]
        public void DeleteAll_Executes_Without_Exception()
        {
            using (var sqlTransaction = _commandBuilder.Connection.BeginTransaction())
            {
                try
                {

                    _dbOperation.DeleteAll(_dsData, _commandBuilder, sqlTransaction);
                    sqlTransaction.Commit();
                }
                catch (Exception)
                {
                    if (sqlTransaction != null)
                    {
                        sqlTransaction.Rollback();
                    }

                    throw;
                }
            }
            Assert.IsTrue(true);
        }

        [Test]
        public void Insert_Executes_Without_Exception()
        {
            ResetIdentityColumns();

            _commandBuilder.ReleaseConnection();
            _commandBuilder.Connection.Open();

            DeleteAll_Executes_Without_Exception();

            _commandBuilder.ReleaseConnection();
            _commandBuilder.Connection.Open();

            using (var sqlTransaction = _commandBuilder.Connection.BeginTransaction())
            {
                try
                {
                    _dbOperation.Insert(_dsData, _commandBuilder, sqlTransaction);
                    sqlTransaction.Commit();
                }
                catch (Exception)
                {
                    if (sqlTransaction != null)
                    {
                        sqlTransaction.Rollback();
                    }

                    throw;
                }
            }
            Assert.IsTrue(true);
        }

        [Test]
        public virtual void InsertIdentity_Executes_Without_Exception()
        {
            DeleteAll_Executes_Without_Exception();

            using (var sqlTransaction = _commandBuilder.Connection.BeginTransaction())
            {
                try
                {
                    _dbOperation.InsertIdentity(_dsData, _commandBuilder, sqlTransaction);
                    sqlTransaction.Commit();
                }
                catch (Exception)
                {
                    if (sqlTransaction != null)
                    {
                        sqlTransaction.Rollback();
                    }

                    throw;
                }
            }
            Assert.IsTrue(true);
        }

        [Test]
        public void Refresh_Executes_Without_Exception()
        {
            DeleteAll_Executes_Without_Exception();
            Insert_Executes_Without_Exception();

            DataSet dsSchema = _commandBuilder.GetSchema();
            DataSet ds = dsSchema.Clone();
            ds.ReadXml(ReadOnlyStreamFromFilename(GetXmlRefeshFilename()));

            using (var sqlTransaction = _commandBuilder.Connection.BeginTransaction())
            {
                try
                {
                    _dbOperation.Refresh(ds, _commandBuilder, sqlTransaction);
                    sqlTransaction.Commit();
                }
                catch
                {
                    sqlTransaction.Rollback();
                    throw;
                }
            }
            Assert.IsTrue(true);
        }

        [Test]
        public void Update_Executes_Without_Exception()
        {
            DeleteAll_Executes_Without_Exception();
            Insert_Executes_Without_Exception();

            DataSet dsSchema = _commandBuilder.GetSchema();
            DataSet ds = dsSchema.Clone();
            ds.ReadXml(ReadOnlyStreamFromFilename(GetXmlFilename()));

            using (var sqlTransaction = _commandBuilder.Connection.BeginTransaction())
            {
                try
                {
                    _dbOperation.Update(ds, _commandBuilder, sqlTransaction);
                    sqlTransaction.Commit();
                }
                catch (Exception)
                {
                    if (sqlTransaction != null)
                    {
                        sqlTransaction.Rollback();
                    }

                    throw;
                }
            }
            Assert.IsTrue(true);
        }

        protected abstract IDbCommandBuilder GetCommandBuilder();

        protected abstract IDbOperation GetDbOperation();

        protected abstract DbCommand GetResetIdentityColumnsDbCommand(DataTable table, DataColumn column);

        protected abstract string GetXmlFilename();

        protected abstract string GetXmlModifyFilename();

        protected abstract string GetXmlRefeshFilename();

        protected abstract string GetXmlSchemaFilename();

        protected void ResetIdentityColumns()
        {
            DbTransaction sqlTransaction = null;
            try
            {
                DataSet dsSchema = _commandBuilder.GetSchema();
                sqlTransaction = _commandBuilder.Connection.BeginTransaction();
                foreach (DataTable table in dsSchema.Tables)
                {
                    foreach (DataColumn column in table.Columns)
                    {
                        if (column.AutoIncrement)
                        {
                            using (DbCommand sqlCommand = GetResetIdentityColumnsDbCommand(table, column))
                            {
                                if (sqlCommand != null)
                                {
                                    sqlCommand.Transaction = sqlTransaction;
                                    sqlCommand.ExecuteNonQuery();
                                }
                            }

                            break;
                        }
                    }
                }
                sqlTransaction.Commit();
            }
            catch (Exception)
            {
                if (sqlTransaction != null)
                {
                    sqlTransaction.Rollback();
                }

                throw;
            }
            finally
            {
                if (sqlTransaction != null)
                    sqlTransaction.Dispose();
            }
        }

        private FileStream ReadOnlyStreamFromFilename(string filename)
        {
            return new FileStream(filename, FileMode.Open, FileAccess.Read);
        }

    }
}