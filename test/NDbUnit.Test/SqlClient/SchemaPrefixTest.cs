using System;
using NUnit.Framework;

namespace NDbUnit.Test.SqlClient
{
    [Category(TestCategories.SqlServerTests)]
    [TestFixture]
    public class SchemaPrefixTest
    {
        [Test]
        public void Can_Perform_CleanInsertUpdate_Operation_Without_Exception_When_Schema_Has_Prefix()
        {
            var db = new NDbUnit.Core.SqlClient.SqlDbUnitTest(DbConnections.SqlConnectionString);
            db.ReadXmlSchema(XmlTestFiles.SqlServer.XmlSchemaFileForSchemaPrefixTests);
            db.ReadXml(XmlTestFiles.SqlServer.XmlFileForSchemaPrefixTests);

            db.PerformDbOperation(NDbUnit.Core.DbOperationFlag.CleanInsertIdentity);
        }
    }
}