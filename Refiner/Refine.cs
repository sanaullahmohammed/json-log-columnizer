using CommonUtilities;
using JsonFlatten;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Storer;
using System.Collections.Concurrent;
using System.Diagnostics;
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
            Console.WriteLine($"Begin {this.GetType().Name} at {DateTime.Now}...");

            var stopwatch = new Stopwatch(); // Start measuring time

            while (true)
            {
                if (!readQueue.IsEmpty)
                {
                    stopwatch.Start();

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
                            Content = entity.Value
                        };

                        Console.WriteLine($"Time elapsed to create a usable chunk for {nameof(writeQueue)} is {stopwatch.ElapsedMilliseconds}ms"); // Auxilary log

                        writeQueue.Enqueue(columnarFile);
                        Console.WriteLine($"Successfully Enqueued columnarFile entity into writeQueue"); // Auxiliary log

                        var isReadQueueDequeueSuccessful = readQueue.TryDequeue(out _);
                        Console.WriteLine($"Dequeuing of entry from readQueue was successful? {isReadQueueDequeueSuccessful}"); // Auxiliary log
                    }
                }
                else if (readQueue.IsClosed)
                {
                    Console.WriteLine($"Time elapsed to complete {this.GetType().Name} is {stopwatch.ElapsedMilliseconds}ms"); // Auxiliary log
                    stopwatch.Stop();

                    Console.WriteLine($"{this.GetType().Name} Completed at {DateTime.Now}");
                    writeQueue.Close();
                    Console.WriteLine($"Signaled end of {this.GetType().Name} by closing {nameof(writeQueue)}");
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
                        existingValue.AppendLine($"{entity.Value.ToJsonValue()}");
                    }
                    else
                    {
                        consolidatedObject[entity.Key] = new StringBuilder().AppendLine($"{entity.Value.ToJsonValue()}");
                    }
                }
            }

            return consolidatedObject.ToDictionary(kv => kv.Key, kv => kv.Value.ToString());
        }
    }
}