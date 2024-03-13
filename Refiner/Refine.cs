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

            var e2eStopwatch = new Stopwatch(); // Start measuring time
            var deserializeStopwatch = new Stopwatch();
            var flattenConsolidateStopwatch = new Stopwatch();

            while (true)
            {
                if (!readQueue.IsEmpty)
                {
                    e2eStopwatch.Start();

                    Console.WriteLine($"Size of Read-Queue is: {readQueue.Count}"); // Auxiliary log

                    var readQueueEntry = readQueue.First();

                    deserializeStopwatch.Restart();
                    var sourceObjects = JsonConvert.DeserializeObject<List<object>>(readQueueEntry);
                    Console.WriteLine($"Time elapsed to complete DeserializeObject is {deserializeStopwatch.ElapsedMilliseconds}ms"); // Auxiliary log
                    deserializeStopwatch.Stop();

                    if (sourceObjects == null)
                    {
                        var errMsg = $"Deserialized {nameof(readQueue)} entry yields null";
                        Console.WriteLine(errMsg);
                        throw new Exception(errMsg);
                    }

                    flattenConsolidateStopwatch.Restart();
                    var flattened = FlattenAndConsolidateObjects(sourceObjects);
                    Console.WriteLine($"Time elapsed to complete {nameof(FlattenAndConsolidateObjects)} is {flattenConsolidateStopwatch.ElapsedMilliseconds}ms"); // Auxiliary log
                    flattenConsolidateStopwatch.Stop();

                    foreach (var entity in flattened)
                    {
                        var columnarFile = new ColumnarFile
                        {
                            FileName = entity.Key + ".column",
                            Content = entity.Value
                        };

                        Console.WriteLine($"Time elapsed to create a usable chunk for {nameof(writeQueue)} is {e2eStopwatch.ElapsedMilliseconds}ms"); // Auxilary log

                        writeQueue.Enqueue(columnarFile);
                        Console.WriteLine($"Successfully Enqueued columnarFile entity into writeQueue"); // Auxiliary log
                    }

                    var isReadQueueDequeueSuccessful = readQueue.TryDequeue(out _);
                    Console.WriteLine($"Dequeuing of entry from readQueue was successful? {isReadQueueDequeueSuccessful}"); // Auxiliary log
                }
                else if (readQueue.IsClosed)
                {
                    Console.WriteLine($"Time elapsed to complete {this.GetType().Name} is {e2eStopwatch.ElapsedMilliseconds}ms"); // Auxiliary log
                    e2eStopwatch.Stop();

                    Console.WriteLine($"{this.GetType().Name} Completed at {DateTime.Now}");
                    writeQueue.Close();
                    Console.WriteLine($"Signaled end of {this.GetType().Name} by closing {nameof(writeQueue)}");
                    break;
                }
                else
                {
                    e2eStopwatch.Stop();
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