using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;

namespace Miner
{
    public class Mine
    {
        private readonly string pathOfLogFile;
        private readonly ConcurrentQueue<object?> readQueue;

        public Mine(string pathOfLogFile, ConcurrentQueue<object?> readQueue)
        {
            this.pathOfLogFile = pathOfLogFile;
            this.readQueue = readQueue;
            Console.WriteLine("Miner is ready for action");
        }

        public void Begin()
        {
            Console.WriteLine($"Mining LogFile: {pathOfLogFile}...");

            foreach (string line in File.ReadLines(pathOfLogFile))
            {
                try
                {
                    var jObject = JObject.Parse(line);

                    if (jObject == null)
                    {
                        var errMsg = $"JObject Parse returned null object while parsing, {line}";
                        Console.WriteLine(errMsg);
                        throw new Exception(errMsg);
                    }
                    else
                    {
                        readQueue.Enqueue(jObject);
                    }
                }
                catch (JsonReaderException err)
                {
                    Console.WriteLine(err.Message);
                    throw err;
                }
            }

            Console.WriteLine($"Mining Completed for LogFile: {pathOfLogFile}...");
            readQueue.Enqueue(null);
            Console.WriteLine($"Signaled end of readQueue by enqueing NULL");
        }
    }
}