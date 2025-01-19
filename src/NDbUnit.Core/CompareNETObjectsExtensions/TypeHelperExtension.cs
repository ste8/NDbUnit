using System;
using System.Data;

namespace NDbUnit.Core.CompareNETObjectsExtensions
{
    public class TypeHelperExtension
    {
        public static bool IsDataset(Type type)
        {
            if (type == null)
                return false;

            return type == typeof(DataSet);
        }

        public static bool IsDataTable(Type type)
        {
            if (type == null)
                return false;

            return type == typeof(DataTable);
        }
    }
}
