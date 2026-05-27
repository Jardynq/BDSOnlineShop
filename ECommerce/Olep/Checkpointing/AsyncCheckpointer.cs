using MessagePack;

namespace ECommerce.Olep.Checkpointing
{
    public class AsyncCheckpointer<TState>
        where TState : class, new()
    {
        private int currentTick;
        private int tickTriggerAmount;
        private string name;
        private long id;
        private Func<TState> snapshotter;

        public AsyncCheckpointer(string name, long id, int triggerAmount, Func<TState> snapshotter)
        {
            this.currentTick = 0;
            this.tickTriggerAmount = triggerAmount;
            this.name = name;
            this.id = id;
            this.snapshotter = snapshotter;
        }

        public void Tick()
        {
            if (this.currentTick >= this.tickTriggerAmount)
            {
                this.Trigger();
            }
            this.currentTick++;
        }

        public void Trigger()
        {
            this.currentTick = 0;
            var snapshot = snapshotter();
            _ = WriteStaticAsync(this.name, this.id, snapshot);
        }

        public static async Task WriteStaticAsync(String name, long id, TState snapshot)
        {
            // Timestamp to microsecond precision to avoid collisions, and use UTC to avo½id timezone issues
            var timestamp = DateTime.UtcNow.ToString("yyyy_MM_dd-HH_mm_ss.ffffff");
            var root = $"Checkpoints/{name}/{id}/";
            var file = $"{root}{timestamp}.bin";

            Directory.CreateDirectory(root);
            using (FileStream outputFile = new FileStream(file, FileMode.Create))
            {
                byte[] bytes = MessagePackSerializer.Serialize<TState>(snapshot);
                await outputFile.WriteAsync(bytes);
            }
        }

        public async Task<TState> LoadMostRecent()
        {
            var root = $"Checkpoints/{this.name}/{this.id}/";
            if (!Directory.Exists(root))
            {
                return new TState();
            }

            string mostRecentFile = Directory.EnumerateFiles(root)
                .OrderDescending()
                .FirstOrDefault();

            if (mostRecentFile == null)
            {
                return new TState();
            }

            Console.WriteLine("Loading checkpoint from file: " + mostRecentFile);
            var bytes = await File.ReadAllBytesAsync(mostRecentFile);
            var state = MessagePackSerializer.Deserialize<TState>(bytes);
            return state;
        }
    }
}
