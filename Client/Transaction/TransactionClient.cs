using System.Diagnostics;

namespace Client.Transaction
{
    internal class TransactionClient
    {
        private int numCustomerActor = 10;
        private int numProductActor = 100;

        // for experiment setting
        private int numCustomerThread = 8;
        private TimeSpan runTime = TimeSpan.FromSeconds(10);    // use this time to control how long time the experiment will run

        private CountdownEvent allThreadsStart;
        private CountdownEvent allThreadsAreDone;

        public async Task RunClient()
        {
            // ================================================================================================================
            // STEP 1: init all actors
            var workload = new WorkloadGenerator(numCustomerActor, numProductActor);

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
            var totalBalanceAnalytics = await workload.GetTotalAnalyticsBalance();
            Console.WriteLine($"Sum of analytics (Should match 'Total customer balance spent'): {totalBalanceAnalytics}");
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
                    Console.WriteLine("This is probably because the analytics hasn't finished processing.");
                    Console.WriteLine("\n ***********************************************************************");
                }
            }
            Console.WriteLine("The experiment is done. ");
            await workload.StopClient();
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
