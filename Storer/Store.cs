using System.Collections.Concurrent;

namespace Storer
{
    public class Store
    {
        private readonly ConcurrentQueue<ColumnarFile?> writeQueue;
        private string directoryName = string.Empty;

        public Store(ConcurrentQueue<ColumnarFile?> writeQueue)
        {
            this.writeQueue = writeQueue;
            Console.WriteLine("Store is ready for action");
        }

        public void Begin()
        {
            if (!GenerateResultsDirectory() || string.IsNullOrEmpty(directoryName))
            {
                var errMsg = $"Couldn't generate results directory";
                Console.WriteLine(errMsg);
                throw new Exception(errMsg);
            }

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

        private bool GenerateResultsDirectory()
        {
            try
            {
                this.directoryName = @$"E:\C#_DEVELOPMENT\JsonLogColumnizer\results_{DateTime.Now.Ticks}";
                Directory.CreateDirectory(directoryName);
                return true;
            }
            catch (Exception err)
            {
                Console.WriteLine(err.Message);
                return false;
            }
        }
    }
}