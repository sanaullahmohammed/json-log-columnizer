using Miner;
using Refiner;
using Storer;
using System.Collections.Concurrent;

namespace JsonLogColumnizer
{
    public class Program
    {
        public static bool CheckFileExists(string path)
        {
            return File.Exists(path);
        }

        public static void Main(string[] args)
        {
            Console.WriteLine("Hello, from JsonLogColumnizer");

            if (args.Length == 0)
            {
                Console.WriteLine("Needs the path of the logfile");
                return;
            }

            var pathOfLogFile = args[0];

            if (!CheckFileExists(pathOfLogFile))
            {
                Console.WriteLine($"Path of logfile doesn't exist, {pathOfLogFile}");
                return;
            }

            var readQueue = new ConcurrentQueue<object?>();
            var writeQueue = new ConcurrentQueue<ColumnarFile?>();

            var miner = new Mine(pathOfLogFile, readQueue);
            var refiner = new Refine(readQueue, writeQueue);
            var storer = new Store(writeQueue);

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