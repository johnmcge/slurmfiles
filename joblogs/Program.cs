using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using CommandLine;

namespace joblogs
{
    // as a .net core console application, this will run on Linux, Windows and Mac
    public class Configurator
    {
        public string ActionOption { get; set; }
        public string InputLocation { get; set; }
        public string InputFileNameMask { get; set; }
        public string OutputLocation { get; set; }
        public string OutputFile { get; set; }
        public string MemEffFile { get; set; }
        public int BatchSize { get; set; } 
        public string Delimiter { get; set; }
        public List<string> ColsOfInterest { get; set; }
        public Dictionary<string, int> PartitionMaxReqMem { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var cfg = new Configurator();

            Parser.Default.ParseArguments<Options>(args).WithParsed<Options>(o =>
            {
                // default values are set in Options.cs
                cfg.ActionOption = o.ActionOption;
                cfg.InputLocation = o.InputLocation;
                cfg.InputFileNameMask = o.FileMask;
                cfg.OutputLocation = o.OutLocation;
                cfg.MemEffFile = o.MemEffInputFile;
                cfg.BatchSize = o.BatchSize;
                cfg.Delimiter = o.Delimiter;
            });
            cfg.ColsOfInterest = Util.LoadColumnsOfInterest(cfg.ActionOption);
            cfg.PartitionMaxReqMem = Util.LoadPartitionInfo();

            switch (cfg.ActionOption)
            {
                case "memeff":
                    if (!Directory.Exists(cfg.InputLocation))
                    {
                        Console.WriteLine($"Input location for log files not found: {cfg.InputLocation}");
                        return;
                    }
                    SetOutputFile(cfg, "memEff");
                    MemEff.GenerateFile(cfg);
                    break;

                case "summarystats":
                    if (!File.Exists(cfg.MemEffFile))
                    {
                        Console.WriteLine($"Input memeff file not found: {cfg.MemEffFile}");
                        return;
                    }
                    SetOutputFile(cfg, "summaryStats4MemEff");
                    SummaryStats.GenerateFile(cfg);
                    break;

                case "pend":
                    if (!Directory.Exists(cfg.InputLocation))
                    {
                        Console.WriteLine($"Input location for log files not found: {cfg.InputLocation}");
                        return;
                    }
                    SetOutputFile(cfg, "pendTimes");
                    PendTimes.GenerateFile(cfg);
                    break;

                case "test":
                    Console.WriteLine("testing code path");
                    break;

                default:
                    Console.WriteLine("nothing was specified to do ... ");
                    break;
            }
        }

        private static void SetOutputFile(Configurator cfg, string prefix)
        {
            string dts = DateTime.Now.ToString("yyyy-MM-dd-mm");
            string newFileName = $"{prefix}-{dts}.txt";

            if (File.Exists(Path.Combine(cfg.OutputLocation, newFileName)))
            {
                Random r = new Random();
                int rando = r.Next(1, 10000);
                newFileName = $"{prefix}-{dts}-{rando}.txt";
            }

            cfg.OutputFile = Path.Combine(cfg.OutputLocation, newFileName);
        }


    }
}
