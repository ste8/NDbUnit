using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace NDbUnit.Core
{
    public class FileSystemService : IFileSystemService
    {
        public IEnumerable<IScript> GetFilesInCurrentDirectory(string fileSpec)
        {
            return GetFilesInSpecificDirectory(".", fileSpec);
        }

        public IEnumerable<IScript> GetFilesInSpecificDirectory(string pathSpec, string fileSpec)
        {
            DirectoryInfo dir = new DirectoryInfo(pathSpec);
            return dir.EnumerateFiles(fileSpec).Select(x => new ScriptFile(x));
        }

        public IScript GetSpecificFile(string fileSpec)
        {
            return new ScriptFile(new FileInfo(fileSpec));
        }
    }
}
