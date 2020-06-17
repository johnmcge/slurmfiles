using System;
using System.Runtime.InteropServices;
using CommandLine;

namespace joblogs
{
    class Options
    {
        [Option("action",
            Default = "",
            Required = false,
            HelpText = "available actions: memeff, summarystats, pend")]
        public string ActionOption { get; set; }

        [Option("inlocation",
            // Default = "/proj/its/slurmdata/longleaf_data/jobs",
            // Default = @"E:\code\LongleafData\testData",
            Required = false,
            HelpText = "path for the input files for memeff action; the RCOps pre-processed slurm log files")]
        public string InputLocation { get; set; }

        [Option("outlocation",
            // Default = "/proj/its/johnmcge",
            // Default = @"E:\Code\LongleafData",
            Required = false,
            HelpText = "path for the MemEff or SummaryStats output file")]
        public string OutLocation { get; set; }

        [Option("fmask",
            Default = "2020-*.csv",
            Required = false,
            HelpText = "mask for the input files to be processed, such as: 2020-*.csv ")]
        public string FileMask { get; set; }

        [Option("batchsize",
            Default = 1000,
            // Default = 10,
            Required = false,
            HelpText = "number of rows to read into memory and processs in parallel")]
        public int BatchSize { get; set; }

        [Option("memefffile",
            //Default = @"E:\code\LongleafData\memEff-2020-06-16-55.txt",
            //Default = "/proj/its/johnmcge/memEff-2020-06-16-55.txt",
            Required = false,
            HelpText = "for summarystats action, path+filename for the memEff file that has already been generated")]
        public string MemEffInputFile { get; set; }

        [Option("delimiter",
            Default = "|",
            Required = false,
            HelpText = "delimiter used to split each row")]
        public string Delimiter { get; set; }


        public Options()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                InputLocation = @"E:\code\LongleafData\testData";
                OutLocation = @"E:\Code\LongleafData";
                MemEffInputFile = @"E:\code\LongleafData\memEff-2020-06-16-55.txt";
            }
            else
            {
                InputLocation = "/proj/its/slurmdata/longleaf_data/jobs";
                OutLocation = "/proj/its/johnmcge/";
                MemEffInputFile = "/proj/its/johnmcge/memEff-2020-06-16-55.txt";
            }
        }


    }
}
