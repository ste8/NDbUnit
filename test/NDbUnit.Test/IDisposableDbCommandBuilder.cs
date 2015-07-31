using System;

using NDbUnit.Core;

namespace NDbUnit.Test
{
    public interface IDisposableDbCommandBuilder : IDbCommandBuilder, IDisposable
    {
    }
}