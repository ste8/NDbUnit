namespace NDbUnit.Core
{
    public class Script : IScript
    {
        public Script(string name, string script)
        {
            Name = name;
            Contents = script;
        }

        public string Name { get; private set; }

        public string Contents { get; private set; }

        public string GetContents()
        {
            return Contents;
        }
    }
}
