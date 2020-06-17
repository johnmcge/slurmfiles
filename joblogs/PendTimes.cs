using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace joblogs
{
    class PendTimes
    {


        public static void GenerateFile(Configurator cfg)
        {
            // var jobLogFiles = Directory.EnumerateFiles(cfg.InputLocation, cfg.InputFileNameMask, SearchOption.TopDirectoryOnly);

            // WriteHeaderRow(cfg);

            //foreach (var jobLogFile in jobLogFiles)
            //    ProcessFile(jobLogFile, cfg);

            TestProcessFile(@"C:\Code\Longleaf\testData\2020-04-20_jobs.csv", cfg);
        }

        private static void ProcessFile(string fileName, Configurator cfg)
        {
            // get header from file, build dictionary of index locations for 
            // columns of interest for this specific log file
            Dictionary<string, int> ColumnReference = Util.GetColumnReferences(cfg, fileName);

            string line = "";
            int filteredRecords = 0;
            int counter = 0;
            List<string> WorkSet = new List<string>();

            using (StreamReader sr = new StreamReader(fileName))
            {
                line = sr.ReadLine(); // skip the header Row

                while ((line = sr.ReadLine()) != null)
                {
                    if (!FilterOutThisRecord(line, ColumnReference, cfg))
                    {
                        WorkSet.Add(line);
                        if ((counter % cfg.BatchSize) == 0)
                        {
                            ProcessWorkSet(WorkSet, ColumnReference, cfg);
                            WorkSet.Clear();
                        }
                        counter++;
                    }
                    else
                        filteredRecords++;
                }
            }

            // catch remaining records in WorkSet
            ProcessWorkSet(WorkSet, ColumnReference, cfg);
            WorkSet.Clear();

            Console.WriteLine($"{filteredRecords} records filtered from {fileName}");
        }

        private static void TestProcessFile(string fileName, Configurator cfg)
        {
            // get header from file, build dictionary of index locations for 
            // columns of interest for this specific log file
            Dictionary<string, int> ColumnReference = Util.GetColumnReferences(cfg, fileName);

            string line = "";
            int filteredRecords = 0;
            int counter = 0;
            List<string> WorkSet = new List<string>();

            int tempMaxRecs = 20;
            int spelunk = 735;

            using (StreamReader sr = new StreamReader(fileName))
            {
                line = sr.ReadLine(); // skip the header Row

                while ((line = sr.ReadLine()) != null && counter < tempMaxRecs)
                {
                    WorkSet.Add(line);
                    if ((counter % cfg.BatchSize) == 0)
                    {
                        ProcessWorkSet(WorkSet, ColumnReference, cfg);
                        WorkSet.Clear();
                    }
                    counter++;
                }
            }

            // catch remaining records in WorkSet
            ProcessWorkSet(WorkSet, ColumnReference, cfg);
            WorkSet.Clear();

            Console.WriteLine($"{filteredRecords} records filtered from {fileName}");
        }


        private static void ProcessWorkSet(List<string> workset, Dictionary<string, int> colRef, Configurator cfg)
        {
            ConcurrentBag<string> cb = new ConcurrentBag<string>();

            Parallel.ForEach((workset), (line) =>
            {
                StringBuilder sb = new StringBuilder();
                string[] thisRec = line.Split(cfg.Delimiter);

                // straight copy for the fieldsOfInterest
                foreach (var item in cfg.ColsOfInterest)
                {
                    sb.Append($"{thisRec[colRef[item]]}");
                    sb.Append($"{cfg.Delimiter}");
                }

                // CPUEffeciency: Compare "TotalCPU" with computed field core-walltime; core-walltime = (NCPUS * "Elapsed")
                int ncpus = int.Parse(thisRec[colRef["NCPUS"]]);
                double hrsElapsed = Util.ConvertTimeStringToHours(thisRec[colRef["Elapsed"]]);

                double hrsCoresElapsed = hrsElapsed * ncpus;

                string corewalltime = Util.ConvertHoursToTimeString(hrsCoresElapsed);
                double CpuEff = 0; // ComputeCPUEffeciency(thisRec[colRef["TotalCPU"]], corewalltime);
                sb.Append($"{Math.Round(CpuEff, 2)}");
                sb.Append($"{cfg.Delimiter}");

                //// MemEffeciency:  (MaxRSS / ReqMem) * 100;  ReqMem field inlcudes unit, must be stripped and possibly converted
                string str_ReqMem = thisRec[colRef["ReqMem"]];
                int ReqMem = int.Parse(str_ReqMem.Substring(0, (str_ReqMem.Length - 2)));
                string ReqMemUnits = str_ReqMem.Substring(str_ReqMem.Length - 2);
                if (ReqMemUnits == "Mc")
                    ReqMem *= int.Parse(thisRec[colRef["NCPUS"]]);

                float MaxRSS = float.Parse(thisRec[colRef["MaxRSS"]]);
                double MemEff = (double)Math.Round(((MaxRSS / ReqMem) * 100), 2);
                sb.Append($"{MemEff}");
                sb.Append($"{cfg.Delimiter}");

                // compute GBHours Requested = (ReqMem / 1000) * Elapsed-Hrs
                double GBHoursReq = (ReqMem / 1000.0) * hrsElapsed;
                sb.Append($"{(int)(GBHoursReq)}");
                sb.Append($"{cfg.Delimiter}");

                JobSize js = Util.JobSizeBin(ncpus, ReqMem, (int)hrsElapsed);

                var hrsPending = GetPendTime(thisRec[colRef["Submit"]], thisRec[colRef["Start"]]);
   
                cb.Add(sb.ToString());
                sb.Clear();
            });

            File.AppendAllLines(cfg.OutputFile, cb);
        }


        public static double GetPendTime(string strSubmit, string strStart)
        {
            DateTime submit = DateTime.Parse(strSubmit);
            DateTime start = DateTime.Parse(strStart);

            Console.WriteLine($"sub = [{strSubmit}], start = [{strStart}]");
            Console.WriteLine($"{submit.ToString("g")}; {start.ToString("g")}");
            Console.WriteLine($"{(start - submit).ToString("g")}{Environment.NewLine}");

            return 0.0;
        }


        private static bool FilterOutThisRecord(string line, Dictionary<string, int> colRef, Configurator cfg)
        {
            // All logic that would cause one to ignore a particular line from the job log data

            var thisRec = line.Split(cfg.Delimiter);

            // filter unwanted job states
            if (thisRec[colRef["State"]] == "RUNNING" ||
                thisRec[colRef["State"]] == "SUSPENDED")
                return true;

            return false;
        }


        private static void WriteHeaderRow(Configurator cfg)
        {
            //  ProcessWorkSet() defines the structure of the output data
            //  this header row needs to mirror the structure defined in ProcessWorkSet()

            StringBuilder sb = new StringBuilder();

            foreach (var item in cfg.ColsOfInterest)
            {
                sb.Append($"{item}");
                sb.Append($"{cfg.Delimiter}");
            }

            sb.Append($"CPUEffeciency");
            sb.Append($"{cfg.Delimiter}");

            sb.Append($"MemEffeciency");
            sb.Append($"{cfg.Delimiter}");

            sb.Append($"WeightedMemEffeciency");
            sb.Append($"{cfg.Delimiter}");

            sb.Append($"GBHoursRequested");
            sb.Append($"{cfg.Delimiter}");

            sb.Append($"GBHoursUsed");
            sb.Append($"{Environment.NewLine}");

            File.AppendAllText(cfg.OutputFile, sb.ToString());
        }



    }
}
