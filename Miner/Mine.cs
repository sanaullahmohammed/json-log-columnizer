using CommonUtilities;
using System.Diagnostics;
using System.Text;

namespace Miner
{
    public class Mine
    {
        private readonly ulong bufferSize = 33554432; // Must be optional argument
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
            Console.WriteLine($"Begin {this.GetType().Name} on LogFile: {pathOfLogFile} at {DateTime.Now}...");

            // Trying to create an array-of-objects using primitive the data-type "string"
            var currentChunk = new StringBuilder().StartJsonArray();

            var stopwatch = Stopwatch.StartNew(); // Start measuring time

            ulong readBytes = 0;

            foreach (string line in File.ReadLines(pathOfLogFile))
            {
                readBytes += (ulong)Encoding.Unicode.GetByteCount(line);

                if (readBytes <= bufferSize)
                {
                    currentChunk.AppendJson(line);
                }
                else
                {
                    Console.WriteLine($"Time elapsed to create a usable chunk for {nameof(readQueue)} is {stopwatch.ElapsedMilliseconds}ms"); // Auxilary log

                    currentChunk.AppendJson(line).EndJsonArray();
                    readQueue.Enqueue($"{currentChunk}");

                    Console.WriteLine($"Enqueued chunk of size: {readBytes} Bytes into {nameof(readQueue)}"); // Auxiliary log

                    currentChunk.Clear();
                    currentChunk.StartJsonArray();

                    readBytes = (ulong)Encoding.Unicode.GetByteCount(line);

                    currentChunk.AppendJson(line);
                }
            }

            currentChunk.EndJsonArray();
            readQueue.Enqueue($"{currentChunk}");

            Console.WriteLine($"Time elapsed to complete {this.GetType().Name} is {stopwatch.ElapsedMilliseconds}ms"); // Auxiliary log
            stopwatch.Stop();

            Console.WriteLine($"{this.GetType().Name} Completed for LogFile: {pathOfLogFile} at {DateTime.Now}...");

            readQueue.Close();

            Console.WriteLine($"Signaled end of {this.GetType().Name} by closing {nameof(readQueue)}");
        }
    }
}