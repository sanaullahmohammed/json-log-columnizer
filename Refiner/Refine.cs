using JsonFlatten;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Storer;
using System.Collections.Concurrent;
using System.Text;

namespace Refiner
{
    public class Refine
    {
        private readonly ClosableConcurrentQueue<string> readQueue;
        private readonly ClosableConcurrentQueue<ColumnarFile> writeQueue;

        public Refine(ClosableConcurrentQueue<string> readQueue, ClosableConcurrentQueue<ColumnarFile> writeQueue)
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
                if (!readQueue.IsEmpty)
                {
                    Console.WriteLine($"Size of Read-Queue is: {readQueue.Count}"); // Auxiliary log

                    var readQueueEntry = readQueue.First();

                    var sourceObjects = JsonConvert.DeserializeObject<List<object>>(readQueueEntry);

                    if (sourceObjects == null)
                    {
                        var errMsg = $"Deserialized {nameof(readQueue)} entry yields null";
                        Console.WriteLine(errMsg);
                        throw new Exception(errMsg);
                    }

                    var flattened = FlattenAndConsolidateObjects(sourceObjects);

                    foreach (var entity in flattened)
                    {
                        var columnarFile = new ColumnarFile
                        {
                            FileName = entity.Key + ".column",
                            Content = Convert.ToString(entity.Value)
                        };

                        writeQueue.Enqueue(columnarFile);
                        Console.WriteLine($"Successfully Enqueued columnarFile entity into writeQueue"); // Auxiliary log

                        var isReadQueueDequeueSuccessful = readQueue.TryDequeue(out _);
                        Console.WriteLine($"Dequeuing of entry from readQueue was successful? {isReadQueueDequeueSuccessful}"); // Auxiliary log
                    }
                }
                else if (readQueue.IsClosed)
                {
                    Console.WriteLine($"Refining Completed");
                    writeQueue.Close();
                    Console.WriteLine($"Signaled end of Mining by closing {nameof(writeQueue)}");
                    break;
                }
            }
        }

        private static Dictionary<string, string> FlattenAndConsolidateObjects(List<object> sourceEntries)
        {
            var consolidatedObject = new Dictionary<string, StringBuilder>();

            foreach (var entry in sourceEntries)
            {
                var flattened = ((JObject)entry).Flatten();

                foreach (var entity in flattened)
                {
                    if (consolidatedObject.TryGetValue(entity.Key, out var existingValue))
                    {
                        existingValue.AppendLine(Convert.ToString(entity.Value));
                    }
                    else
                    {
                        consolidatedObject[entity.Key] = new StringBuilder($"{Convert.ToString(entity.Value)}\n");
                    }
                }
            }

            return consolidatedObject.ToDictionary(kv => kv.Key, kv => kv.Value.ToString());
        }
    }
}