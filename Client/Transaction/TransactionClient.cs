﻿using System.Diagnostics;

namespace Client.Transaction
{
    internal class TransactionClient
    {

        private int numCustomerActor = 10;
        private int numProductActor = 100;

        private int numEpochs = 10;
        private int numWarmupEpochs = 2;

        // for experiment setting
        private int numCustomerThread = 8;
        private TimeSpan runTime = TimeSpan.FromSeconds(10);    // use this time to control how long time the experiment will run

        private CountdownEvent allThreadsStart;
        private CountdownEvent allThreadsAreDone;

        public async Task RunClient()
        {
            // Start + end ts per transaction per thread per epoch.
            var allTimestamps = new List<List<List<Tuple<long, long>>>>();
            // numCustomerActor + 1 for the extra sample actor that we use to check end-to-end latency
            var workload = new WorkloadGenerator(numCustomerActor + 1, numProductActor);
            for (int epoch = 0; epoch < numEpochs + numWarmupEpochs; epoch++)
            {
                Console.WriteLine($"\n\n============================== Epoch {epoch} ==============================");
                // ================================================================================================================
                // STEP 1: init all actors
                await workload.InitAllActors();
                Console.WriteLine("\n ***********************************************************************");
                Console.WriteLine($"#customer = {numCustomerActor}, #product = {numProductActor}");

                // ================================================================================================================
                // STEP 2: get initial inventory of all products
                var beforeTotalAmount = (await workload.GetAllInventory()).Item1.Sum();
                var beforeTotalBalance = (await workload.GetAllBalance()).Item1.Sum();

                // ================================================================================================================
                // STEP 3: spawn multiple threads to submit transactions
                allThreadsStart = new CountdownEvent(numCustomerThread);
                allThreadsAreDone = new CountdownEvent(numCustomerThread);
                Console.WriteLine("\n ***********************************************************************");
                Console.WriteLine($"Spawning {numCustomerThread} threads to check-out order");
                for (int i = 0; i < numCustomerThread; i++)
                {
                    var thread = new Thread(CustomerWorkAsync);
                    thread.Start(i);
                }

                allThreadsAreDone.Wait();   // wait until all threads are done

                // Hack. Allow the analytics to finish processing first.
                Thread.Sleep(5000);

                if (epoch >= numWarmupEpochs)
                {
                    //allTimestamps.Add(threadTimestamps);
                }

                // ================================================================================================================
                // STEP 4: check inventory of all products again
                var invRes = await workload.GetAllInventory();
                var afterTotalAmount = invRes.Item1.Sum();
                Console.WriteLine("\n ***********************************************************************");
                Console.WriteLine($"Before total product quantity {beforeTotalAmount}");
                Console.WriteLine($"After total product quantity  {afterTotalAmount}");
                Console.WriteLine("\n ***********************************************************************");
                if (invRes.Item2)
                {
                    Console.WriteLine($"The inventory has once become negative!!!");
                    Console.WriteLine("\n ***********************************************************************");
                }

                var balRes = await workload.GetAllBalance();
                var afterTotalBalance = balRes.Item1.Sum();
                var totalSpent = beforeTotalBalance - afterTotalBalance;
                Console.WriteLine($"Before total customer balance {beforeTotalBalance}");
                Console.WriteLine($"After total customer balance  {afterTotalBalance}");
                Console.WriteLine($"Total customer balance spent  {totalSpent}");
                Console.WriteLine("\n ***********************************************************************");
                if (balRes.Item2)
                {
                    Console.WriteLine($"A customers balance has once become negative!!!");
                    Console.WriteLine("\n ***********************************************************************");
                }

                // the top-10 customers
                Console.WriteLine($"The top-10 customers are: ");
                var top10 = await workload.GetTopTen();
                Console.WriteLine(top10);
                Console.WriteLine("\n ***********************************************************************");

                // Assert that the amount returned by top10 matches the total
                // (in the case that the top10 are all customers)
                if (numCustomerActor <= 10)
                {
                    var sum = 0;
                    foreach (var line in top10.Split('\n'))
                    {
                        try
                        {
                            sum += int.Parse(line.Split(':')[1].Trim());
                        }
                        catch { }
                    }

                    if (sum != totalSpent)
                    {
                        Console.WriteLine($"Total spent amongst top 10, {sum}, does not match total spent all together {totalSpent}.");
                        Console.WriteLine("\n ***********************************************************************");
                    }
                }
                Console.WriteLine("The experiment is done. ");
                await workload.StopClient();
            }

            await workload.StopClient();
            await WriteCSV(allTimestamps);
            Console.WriteLine("The experiment is done. ");
        }

        private async Task WriteCSV(List<List<List<Tuple<long, long>>>> latencies)
        {
            var maxLen = latencies.Select(l => l.Select(l => l.Count).Max()).Max();

            var header = "epoch,thread";
            for (int i = 0; i < maxLen; i++)
            {
                header += $",start{i}_ns,end{i}_ns";
            }

            var lines = new List<string>() { header };
            for (int epoch = 0; epoch < latencies.Count; epoch++)
            {
                for (int thread = 0; thread < latencies[epoch].Count; thread++)
                {
                    var row = new List<string> { epoch.ToString(), thread.ToString() };
                    for (int i = 0; i < latencies[epoch][thread].Count; i++)
                    {
                        row.Add(latencies[epoch][thread][i].Item1.ToString());
                        row.Add(latencies[epoch][thread][i].Item2.ToString());
                    }
                    lines.Add(string.Join(",", row));
                }
            }
            File.WriteAllLines($"{numCustomerActor}-{numProductActor}_latencies.csv", lines);
        }

        // ================================================================================================================
        private async void CustomerWorkAsync(object obj)
        {
            var thread = (int)obj;
            var numEmitTransaction = 0;
            var workload = new WorkloadGenerator(numCustomerActor, numProductActor);
            var watch = new Stopwatch();

            allThreadsStart.Signal();
            allThreadsStart.Wait();      // make sure all threads start at the same time

            watch.Start();
            while (watch.Elapsed < runTime)
            {
                numEmitTransaction++;
                await workload.NewCheckOutOrder();   // submit one transaction a time
            }
            var totalTime = watch.Elapsed.TotalMilliseconds;

            Console.WriteLine($"Thread {thread}: " +
                              $"Number of transactions emitted = {numEmitTransaction} " +
                              $"Total time elapsed = {totalTime}");
            allThreadsAreDone.Signal();
            await workload.StopClient();
        }
    }
}