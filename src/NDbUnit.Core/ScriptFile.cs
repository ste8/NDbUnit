using System.IO;

namespace NDbUnit.Core
{
    public class ScriptFile : IScript
    {
        public ScriptFile(FileInfo info)
        {
            Info = info;
        }

        public FileInfo Info { get; private set; }

        public string Name
        {
            get { return Info.Name; }
        }

        public string GetContents()
        {
            return File.ReadAllText(Info.FullName);
        }
    }
}
