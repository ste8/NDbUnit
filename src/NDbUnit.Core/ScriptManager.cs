using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace NDbUnit.Core
{
    public class ScriptManager
    {
        private IFileSystemService _fileSystem;

        private List<IScript> _scripts = new List<IScript>();

        public ScriptManager(IFileSystemService fileSystem)
        {
            _fileSystem = fileSystem;
        }

        public IEnumerable<string> ScriptContents
        {
            get { return GetScriptContents(); }
        }

        public IEnumerable<IScript> Scripts
        {
            get { return _scripts; }
        }

        public void Add(IScript script)
        {
            if (script == null)
                return;
            _scripts.Add(script);
        }

        public void AddRange(IEnumerable<IScript> scripts)
        {
            if (scripts == null)
                return;
            _scripts.AddRange(scripts.OrderBy(x => x.Name));
        }

        public void AddWithWildcard(string pathSpec, string fileSpec)
        {
            AddRange(_fileSystem.GetFilesInSpecificDirectory(pathSpec, fileSpec));
        }

        public void AddSingle(string fileSpec)
        {
            Add(_fileSystem.GetSpecificFile(fileSpec));
        }

        public void ClearAll()
        {
            _scripts.Clear();
        }

        public int Count()
        {
            return _scripts.Count;
        }

        private IEnumerable<string> GetScriptContents()
        {
            IList<string> contents = new List<string>();

            foreach (var script in Scripts)
            {
                contents.Add(script.GetContents());
            }

            return contents;
        }

    }
}
