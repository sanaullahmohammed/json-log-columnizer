using System.Collections.Concurrent;

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
            while (true)
            {
                if (!writeQueue.IsEmpty)
                {
                    Console.WriteLine($"Size of Write-Queue is: {writeQueue.Count}"); // Auxiliary log

                    var writeQueueEntry = writeQueue.First();

                    var pathName = Path.Combine(directoryName, writeQueueEntry.FileName);

                    using (StreamWriter outputFile = new StreamWriter(pathName, File.Exists(pathName)))
                    {
                        outputFile.WriteLine(writeQueueEntry.Content);
                        Console.WriteLine($"Successfully created an entry in {writeQueueEntry.FileName}"); // Auxiliary log

                        var isWriteQueueDequeueSuccessful = writeQueue.TryDequeue(out _);
                        Console.WriteLine($"Dequeuing of entry from writeQueue was successful? {isWriteQueueDequeueSuccessful}"); // Auxiliary log
                    }
                }
                else if (writeQueue.IsClosed)
                {
                    Console.WriteLine($"Storing has ended");
                    break;
                }
            }
        }
    }
}