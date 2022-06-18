using System;
using System.IO;

namespace SimpleQueue.InMemory.Tests
{
    public sealed class DefaultFixture : IDisposable
    {
        internal readonly TextWriter LogWriter;

        public DefaultFixture()
        {
            var logFileName = typeof(DefaultFixture).Assembly.GetName().Name + ".log";
            var logFilePath = Path.Combine(AppContext.BaseDirectory, logFileName);
            LogWriter = TextWriter.Synchronized(File.CreateText(logFilePath));
            Console.SetOut(LogWriter);
        }

        public void Dispose()
        {
            LogWriter.Flush();
            LogWriter.Close();
            LogWriter.Dispose();
        }
    }
}
