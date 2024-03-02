using JsonFlatten;
using Newtonsoft.Json.Linq;
using Storer;
using System.Collections.Concurrent;

namespace Refiner
{
    public class Refine
    {
        private readonly ConcurrentQueue<object?> readQueue;
        private readonly ConcurrentQueue<ColumnarFile?> writeQueue;

        public Refine(ConcurrentQueue<object?> readQueue, ConcurrentQueue<ColumnarFile?> writeQueue)
        {
            this.readQueue = readQueue;
            this.writeQueue = writeQueue;
            Console.WriteLine("Refiner is ready for action");
        }

        public void Begin()
        {
            Console.WriteLine($"Refining has begun...");

            while (true)
            {
                var readQueueSize = readQueue.Count;

                Console.WriteLine($"Size of Read-Queue is: {readQueueSize}");

                if (readQueueSize > 0)
                {
                    var readQueueEntry = readQueue.First();

                    if (readQueueEntry == null)
                    {
                        Console.WriteLine($"Found NULL entry in readQueue. Halting the refining process...");
                        break;
                    }

                    var flattened = ((JObject)readQueueEntry).Flatten();

                    foreach (var entity in flattened)
                    {
                        var columnarFile = new ColumnarFile
                        {
                            FileName = entity.Key + ".column",
                            Content = Convert.ToString(entity.Value)!
                        };

                        writeQueue.Enqueue(columnarFile);
                        Console.WriteLine($"Successfully Enqueued columnarFile entity into writeQueue");

                        var isReadQueueDequeueSuccessful = readQueue.TryDequeue(out _);
                        Console.WriteLine($"Dequeuing of entry from readQueue was successful? {isReadQueueDequeueSuccessful}");
                    }
                }
            }

            Console.WriteLine($"Refining Completed");
            writeQueue.Enqueue(null);
            Console.WriteLine($"Signaled end of writeQueue by enqueing NULL");
        }
    }
}