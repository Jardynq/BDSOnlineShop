using ECommerce.Kafka;
using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;
using MathNet.Numerics.Distributions;
using System.Text;

namespace Client.Transaction
{
    internal class WorkloadGenerator
    {
        private readonly int numCustomerActor;
        private readonly int numProductActor;
        private OrleansClientManager clientManager;
        private IClusterClient client;
        private bool isClientConnected = false;

        // Added Kafka producer for task 2
        private KafkaProducer<Checkout> producer;

        private IDiscreteDistribution customerDistribution;       // which customer send the request
        private IDiscreteDistribution productDistribution;        // which product to buy
        private IDiscreteDistribution productQtyDistribution;     // the number of items available for each product
        private IDiscreteDistribution productPriceDistribution;   // the price of items
        private IDiscreteDistribution customerBalanceDistribution;// the customer balance
        private IDiscreteDistribution customerQtyDistribution;    // max qty a customer can buy for a product

        public WorkloadGenerator(int numCustomerActor, int numProductActor)
        {

            this.numCustomerActor = numCustomerActor;
            this.numProductActor = numProductActor;
            // it will generate samples within range [a, b]
            customerDistribution = new DiscreteUniform(0, numCustomerActor - 1, new Random());
            productDistribution = new DiscreteUniform(0, numProductActor - 1, new Random());
            productQtyDistribution = new DiscreteUniform(1, 100, new Random());
            productPriceDistribution = new DiscreteUniform(1, 1000, new Random());
            customerBalanceDistribution = new DiscreteUniform(1, 10000, new Random());
            customerQtyDistribution = new DiscreteUniform(1, 10, new Random());

            // wait until the client is created and connected
            InitiateClient();
            while (isClientConnected == false) Thread.Sleep(TimeSpan.FromMilliseconds(100));

            // Added Kafka producer for task 2
            producer = new KafkaProducer<Checkout>(Utilities.Constants.CheckoutNamespace);
        }

        private async void InitiateClient()
        {
            this.clientManager = new OrleansClientManager();
            this.client = await this.clientManager.StartClient();
            this.isClientConnected = true;
        }
        public async Task StopClient()
        {
            await this.clientManager.StopClient();
        }

        public async Task InitAllActors()
        {
            var analyticsActor = client.GetGrain<IAnalyticsActor>(0);
            await analyticsActor.Init();

            var tasks = new List<Task>();
            for (int i = 0; i < numCustomerActor; i++)
            {
                var balance = customerBalanceDistribution.Sample();
                var customerActor = client.GetGrain<ICustomerActor>(i);
                tasks.Add(customerActor.Init(balance));
            }

            for (int i = 0; i < numProductActor; i++)
            {
                var price = productPriceDistribution.Sample();
                var amount = productQtyDistribution.Sample();
                var productActor = client.GetGrain<IProductActor>(i);
                tasks.Add(productActor.Init(price, amount));
            }

            await Task.WhenAll(tasks);
        }

        public async Task<Tuple<List<long>, bool>> GetAllInventory()
        {
            var tasks = new List<Task<int>>();
            for (int i = 0; i < numProductActor; i++)
            {
                var productActor = client.GetGrain<IProductActor>(i);
                tasks.Add(productActor.GetInventory());
            }
            await Task.WhenAll(tasks);

            var hasEverGotNegativeInventory = false;
            var inventory = new List<long>();
            foreach (var task in tasks)
            {
                inventory.Add(task.Result);
                if (task.Result < 0) hasEverGotNegativeInventory = true;
            }
            return new Tuple<List<long>, bool>(inventory, hasEverGotNegativeInventory);
        }
        public async Task<Tuple<List<double>, bool>> GetAllBalance()
        {
            var tasks = new List<Task<double>>();
            for (int i = 0; i < numCustomerActor; i++)
            {
                var customerActor = client.GetGrain<ICustomerActor>(i);
                tasks.Add(customerActor.GetBalance());
            }
            await Task.WhenAll(tasks);

            var hasEverGotNegativeBalance = false;
            var balances = new List<double>();
            foreach (var task in tasks)
            {
                balances.Add(task.Result);
                if (task.Result < 0) hasEverGotNegativeBalance = true;
            }
            return new Tuple<List<double>, bool>(balances, hasEverGotNegativeBalance);
        }

        public async Task NewCheckOutOrder()
        {

            var customerID = customerDistribution.Sample();
            var productID = productDistribution.Sample();
            var qty = customerQtyDistribution.Sample();

            var price = await client.GetGrain<IProductActor>(productID).GetPrice();

            // Old code
            //IStreamProvider streamProvider = client.GetStreamProvider(Constants.DefaultStreamProvider);
            //IAsyncStream<Checkout> checkoutStream = streamProvider.GetStream<Checkout>(Constants.CheckoutNamespace, customerID.ToString());
            //await checkoutStream.OnNextAsync(new Checkout(customerID, price, qty));

            // Task 2 implemented here
            // Usin Kafka producer instead of stream
            var checkout = new Checkout(productID, price, qty);
            //await producer.Append(customerID, checkout);
            _ = producer.Append(customerID, checkout);
            // Do not await, just fire as many as possible.
            return;
        }

        public async Task<string> GetTopTen()
        {
            List<KeyValuePair<long, double>> res = await client.GetGrain<IAnalyticsActor>(0).Top10();
            StringBuilder sb = new StringBuilder();
            foreach (KeyValuePair<long, double> kv in res)
            {
                if (kv.Value > customerBalanceDistribution.Maximum)
                {
                    sb.AppendLine("Total amount spent larger that starting balance!");
                    sb.AppendLine("Either due to a bug, or the server has not been restarted.");
                    break;
                }
            }

            foreach (KeyValuePair<long, double> kv in res)
            {
                sb.Append(kv.Key);
                sb.Append(" : ");
                sb.Append(kv.Value);
                sb.AppendLine();
            }
            return sb.ToString();
        }

    }
}
