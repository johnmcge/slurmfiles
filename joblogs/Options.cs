using System;
using CommandLine;

namespace joblogs
{
    class Options
    {
        [Option("action",
            Default = "",
            Required = false,
            HelpText = "available actions: memeff, summarystats")]
        public string ActionOption { get; set; }

        [Option("inlocation",
            Default = "/proj/its/slurmdata/longleaf_data/jobs",
            Required = false,
            HelpText = "path for the input files for memeff action; the RCOps pre-processed slurm log files")]
        public string InputLocation { get; set; }

        [Option("outlocation",
            // Default = "/proj/its/johnmcge",
            Default = @"C:\Code\Longleaf",
            Required = false,
            HelpText = "path for the MemEff or SummaryStats output file")]
        public string OutLocation { get; set; }

        [Option("fmask",
            Default = "2020-*.csv",
            Required = false,
            HelpText = "mask for the memeff input file names, such as: 2020-*.csv ")]
        public string FileMask { get; set; }

        [Option("batchsize",
            Default = 1000,
            Required = false,
            HelpText = "for memeff action, number of rows to read into memory and processs in parallel")]
        public int BatchSize { get; set; }

        [Option("memefffile",
            Default = @"C:\Code\Longleaf\memEff-2020-05-23-43.txt",
            Required = false,
            HelpText = "for summarystats action, path+filename for the memEff file that has already been generated")]
        public string MemEffInputFile { get; set; }

        [Option("delimiter",
            Default = "|",
            Required = false,
            HelpText = "delimiter used to split each row")]
        public string Delimiter { get; set; }

    }
}
