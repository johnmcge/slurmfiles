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
                // default values set in Options.cs
                cfg.ActionOption = o.ActionOption;
                cfg.InputLocation = o.InputLocation;
                cfg.InputFileNameMask = o.FileMask;
                cfg.OutputLocation = o.OutLocation;
                cfg.MemEffFile = o.MemEffInputFile;
                cfg.BatchSize = o.BatchSize;
                cfg.Delimiter = o.Delimiter;
            });
            cfg.ColsOfInterest = LoadColumnsOfInterest();
            cfg.PartitionMaxReqMem = LoadPartitionInfo();

            switch (cfg.ActionOption)
            {
                case "memeff":
                    SetOutputFile(cfg, "memEff");
                    MemEff.GenerateMemEffFile(cfg);
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

                case "test":
                    Console.WriteLine("testing code path");
                    break;

                default:
                    Console.WriteLine("nothing was specified to do ... ");
                    break;
            }
        }

        public static List<string> LoadColumnsOfInterest()
        {
            // this list contributes the format of the output file
            List<string> lst = new List<string>();

            lst.Add("User");
            lst.Add("Account");
            lst.Add("JobID");
            lst.Add("Partition");
            lst.Add("State");

            lst.Add("MaxRSS");
            lst.Add("ReqMem");

            lst.Add("ReqCPUS");
            lst.Add("NCPUS");

            lst.Add("TotalCPU");
            lst.Add("Elapsed");

            lst.Add("Timelimit");
            //lst.Add("Submit");
            //lst.Add("Start");

            return lst;
        }


        public static Dictionary<string, int> LoadPartitionInfo()
        {
            // we neeed max allowed memory per partition to normalize ReqMem as a percentage of what is available to be requested
            // if a partition is Not listed here, all jobs from that parition will be skipped

            Dictionary<string, int> d = new Dictionary<string, int>();

            d.Add("general", 80000);        //     80 gb
            d.Add("general_big", 750000);   //    750 gb
            d.Add("hov", 750000);           //    750 gb
            d.Add("snp", 750000);           //    750 gb
            d.Add("spill", 241000);         //    241 gb
            d.Add("bigmem", 3041000);       //  3,041 gb
            d.Add("interact", 128000);      //    128 gb

            return d;
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
