using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;
using System.Text;

namespace Miner
{
    public class Mine
    {
        private readonly ulong bufferSize = 16777216; // Must be optional argument
        private readonly string pathOfLogFile;
        private readonly ClosableConcurrentQueue<string> readQueue;

        public Mine(string pathOfLogFile, ClosableConcurrentQueue<string> readQueue)
        {
            this.pathOfLogFile = pathOfLogFile;
            this.readQueue = readQueue;
            Console.WriteLine("Miner is ready for action");
        }

        public void Begin()
        {
            Console.WriteLine($"Mining LogFile: {pathOfLogFile}...");
            var currentChunk = new List<object>();

            ulong readBytes = 0;

            foreach (string line in File.ReadLines(pathOfLogFile))
            {
                readBytes += (ulong)Encoding.Unicode.GetByteCount(line);

                if (readBytes <= bufferSize)
                {
                    ProcessLine(line, currentChunk);
                }
                else
                {
                    EnqueueChunk(currentChunk);
                    Console.WriteLine($"Enqueued chunk of size: {readBytes} Bytes into {nameof(readQueue)}"); // Auxiliary log

                    currentChunk.Clear();

                    readBytes = (ulong)Encoding.Unicode.GetByteCount(line);

                    ProcessLine(line, currentChunk);
                }
            }

            Console.WriteLine($"Mining Completed for LogFile: {pathOfLogFile}...");

            EnqueueChunk(currentChunk);
            readQueue.Close();

            Console.WriteLine($"Signaled end of Mining by closing {nameof(readQueue)}");
        }

        private static void ProcessLine(string line, List<object> chunk)
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
                chunk.Add(jObject);
            }
            catch (JsonReaderException err)
            {
                Console.WriteLine(err.Message);
                throw err;
            }
        }

        private void EnqueueChunk(List<object> chunk)
        {
            if (chunk.Count > 0)
            {
                readQueue.Enqueue(JsonConvert.SerializeObject(chunk));
            }
            else
            {
                Console.WriteLine($"Empty chunk requested to be Enqueued into {nameof(readQueue)}");
            }
        }
    }
}