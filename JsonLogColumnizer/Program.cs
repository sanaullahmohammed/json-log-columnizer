using Miner;
using Refiner;
using Storer;
using System.Collections.Concurrent;

namespace JsonLogColumnizer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Hello, from JsonLogColumnizer");

            if (args.Length < 2)
            {
                Console.WriteLine("Needs the path of the logfile and the results directory");
                return;
            }

            var pathOfLogFile = args[0];
            var pathOfResultsDirectory = args[1];

            if (!File.Exists(pathOfLogFile))
            {
                Console.WriteLine($"Path of logfile doesn't exist, {pathOfLogFile}");
                return;
            }

            if (!Directory.Exists(pathOfResultsDirectory))
            {
                Console.WriteLine($"Path of Results Directory doesn't exist, {pathOfResultsDirectory}");
                return;
            }

            var readQueue = new ClosableConcurrentQueue<string>();
            var writeQueue = new ClosableConcurrentQueue<ColumnarFile>();

            var miner = new Mine(pathOfLogFile, readQueue);
            var refiner = new Refine(readQueue, writeQueue);
            var storer = new Store(pathOfResultsDirectory, writeQueue);

            var mineThread = new Thread(miner.Begin);
            var refineThread = new Thread(refiner.Begin);
            var storeThread = new Thread(storer.Begin);

            Console.WriteLine($"Processing Has Started at {DateTime.Now}...");

            mineThread.Start();
            refineThread.Start();
            storeThread.Start();

            mineThread.Join();
            refineThread.Join();
            storeThread.Join();

            Console.WriteLine($"Processing Finished at {DateTime.Now}");
        }
    }
}