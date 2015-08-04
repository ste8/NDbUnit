using System;
using System.Collections.Generic;
using System.IO;


namespace NDbUnit.Core
{
    public interface IFileSystemService
    {
        IEnumerable<IScript> GetFilesInCurrentDirectory(string fileSpec);
        IEnumerable<IScript> GetFilesInSpecificDirectory(string pathSpec, string fileSpec);
        IScript GetSpecificFile(string fileSpec);
    }
}
