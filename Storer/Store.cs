using System.Collections.Concurrent;
using System.Diagnostics;

namespace Storer
{
    public class Store
    {
        private readonly ClosableConcurrentQueue<ColumnarFile> writeQueue;
        private readonly string directoryName;

        public Store(string directoryName, ClosableConcurrentQueue<ColumnarFile> writeQueue)
        {
            this.writeQueue = writeQueue;
            this.directoryName= directoryName;
            Console.WriteLine("Store is ready for action");
        }

        public void Begin()
        {
            Console.WriteLine($"Begin {this.GetType().Name} at {DateTime.Now}...");

            var stopwatch = new Stopwatch(); // Start measuring time

            while (true)
            {
                if (!writeQueue.IsEmpty)
                {
                    stopwatch.Start();

                    Console.WriteLine($"Size of {nameof(writeQueue)} is: {writeQueue.Count}"); // Auxiliary log

                    var writeQueueEntry = writeQueue.First();

                    var pathName = Path.Combine(directoryName, writeQueueEntry.FileName);

                    using (StreamWriter outputFile = new StreamWriter(pathName, File.Exists(pathName)))
                    {
                        outputFile.Write(writeQueueEntry.Content);
                        Console.WriteLine($"Successfully created an entry in {writeQueueEntry.FileName}"); // Auxiliary log

                        Console.WriteLine($"Time elapsed to create an entry in file is {stopwatch.ElapsedMilliseconds}ms"); // Auxilary log

                        var isWriteQueueDequeueSuccessful = writeQueue.TryDequeue(out _);
                        Console.WriteLine($"Dequeuing of entry from {nameof(writeQueue)} was successful? {isWriteQueueDequeueSuccessful}"); // Auxiliary log
                    }
                }
                else if (writeQueue.IsClosed)
                {
                    Console.WriteLine($"Time elapsed to complete {this.GetType().Name} is {stopwatch.ElapsedMilliseconds}ms"); // Auxiliary log
                    stopwatch.Stop();

                    Console.WriteLine($"{this.GetType().Name} Completed at {DateTime.Now}");
                    break;
                }
            }
        }
    }
}