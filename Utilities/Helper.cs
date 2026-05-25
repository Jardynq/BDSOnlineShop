namespace Utilities
{
    public static class Helper
    {
        public static string ChangePrintFormat(double n, int num)
            => Math.Round(n, num).ToString().Replace(',', '.');
    }
}
