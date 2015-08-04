namespace NDbUnit.Core
{
    public interface IScript
    {
        string Name { get; }

        string GetContents();
    }
}
