using System.Collections.Concurrent;

namespace Storer
{
    public class Store
    {
        private readonly ConcurrentQueue<ColumnarFile?> writeQueue;
        private readonly string directoryName;

        public Store(string directoryName, ConcurrentQueue<ColumnarFile?> writeQueue)
        {
            this.writeQueue = writeQueue;
            this.directoryName= directoryName;
            Console.WriteLine("Store is ready for action");
        }

        public void Begin()
        {
            while (true)
            {
                var writeQueueSize = writeQueue.Count;

                Console.WriteLine($"Size of Write-Queue is: {writeQueueSize}");

                if (writeQueueSize > 0)
                {
                    var writeQueueEntry = writeQueue.First();

                    if (writeQueueEntry == null)
                    {
                        Console.WriteLine($"Found NULL entry in writeQueue. Halting the storing process...");
                        break;
                    }

                    var pathName = Path.Combine(directoryName, writeQueueEntry.FileName);

                    using (StreamWriter outputFile = new StreamWriter(pathName, File.Exists(pathName)))
                    {
                        outputFile.WriteLine(writeQueueEntry.Content);
                        Console.WriteLine($"Successfully created an entry in {writeQueueEntry.FileName}");

                        var isWriteQueueDequeueSuccessful = writeQueue.TryDequeue(out _);
                        Console.WriteLine($"Dequeuing of entry from writeQueue was successful? {isWriteQueueDequeueSuccessful}");
                    }
                }
                else
                {
                    Console.WriteLine($"Write-Queue is empty...");
                }
            }

            Console.WriteLine($"Storing has ended...");
        }
    }
}