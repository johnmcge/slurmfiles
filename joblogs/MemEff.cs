using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace joblogs
{
    class MemEff
    {
        public static void GenerateFile(Configurator cfg)
        {
            var jobLogFiles = Directory.EnumerateFiles(cfg.InputLocation, cfg.InputFileNameMask, SearchOption.TopDirectoryOnly);

            WriteHeaderRow(cfg);

            foreach (var jobLogFile in jobLogFiles)
                ProcessFile(jobLogFile, cfg);
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
                double CpuEff = ComputeCPUEffeciency(thisRec[colRef["TotalCPU"]], corewalltime);
                sb.Append($"{Math.Round(CpuEff, 2)}");
                sb.Append($"{cfg.Delimiter}");

                // MemEffeciency:  (MaxRSS / ReqMem) * 100;  ReqMem field inlcudes unit, must be stripped and possibly converted
                string str_ReqMem = thisRec[colRef["ReqMem"]];
                int ReqMem = int.Parse(str_ReqMem.Substring(0, (str_ReqMem.Length - 2)));
                string ReqMemUnits = str_ReqMem.Substring(str_ReqMem.Length - 2);
                if (ReqMemUnits == "Mc")
                    ReqMem *= int.Parse(thisRec[colRef["NCPUS"]]);

                float MaxRSS = float.Parse(thisRec[colRef["MaxRSS"]]);
                double MemEff = (double)Math.Round(((MaxRSS / ReqMem) * 100), 2);
                sb.Append($"{MemEff}");
                sb.Append($"{cfg.Delimiter}");

                // WeightedMemEffeciency
                // (double)PartitionMaxReqMem[partition]
                double MaxReqMem4Partition = (double)cfg.PartitionMaxReqMem[thisRec[colRef["Partition"]]];
                double weightedMemEff = ComputeWeightedMemEfficiency(MemEff, CpuEff, hrsElapsed, ReqMem, ncpus, MaxReqMem4Partition);
                sb.Append($"{Math.Round(weightedMemEff, 2)}");
                sb.Append($"{cfg.Delimiter}");

                // compute GBHours Requested = (ReqMem / 1000) * Elapsed-Hrs
                double GBHoursReq = (ReqMem / 1000.0) * hrsElapsed;
                sb.Append($"{(int)(GBHoursReq)}");
                sb.Append($"{cfg.Delimiter}");

                // compute GBHours actually used
                double GBHoursUsed = (MaxRSS / 1000.0) * hrsElapsed;
                sb.Append($"{(int)(GBHoursUsed)}");

                cb.Add(sb.ToString());
                sb.Clear();
            });

            File.AppendAllLines(cfg.OutputFile, cb);
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


        private static bool FilterOutThisRecord(string line, Dictionary<string, int> colRef, Configurator cfg)
        {
            // All logic that would cause one to ignore a particular line from the job log data

            var thisRec = line.Split(cfg.Delimiter);
            int ReqMemThreshold = 3096;

            // filter unwanted job states
            if (thisRec[colRef["State"]] == "RUNNING" ||
                thisRec[colRef["State"]] == "SUSPENDED" ||
                thisRec[colRef["State"]] == "OUT_OF_MEMORY")
                return true;

            // filter out partitions for which we do not have a max ReqMem configured
            if (!cfg.PartitionMaxReqMem.ContainsKey(thisRec[colRef["Partition"]]))
                return true;

            // filter out records where elapsed time is < 5 minutes
            string elapsed = thisRec[colRef["Elapsed"]];
            if (elapsed.IndexOf("-") == -1)
            {
                // if no "-" found within string, then elapsed time was less than one day
                string[] hhmmss = elapsed.Split(":");
                int hh = int.Parse(hhmmss[0]);
                int mm = int.Parse(hhmmss[1]);

                if (hh == 0)
                    if (mm < 5)
                        return true;
            }

            // filter out records where requested memory < ReqMemThreshold
            string str_ReqMem = thisRec[colRef["ReqMem"]];
            if (str_ReqMem.Length < 3)  // some cancelled jobs show as "0n" for ReqMem
                return true;

            try
            {
                int ReqMem = int.Parse(str_ReqMem.Substring(0, (str_ReqMem.Length - 2)));
                string ReqMemUnits = str_ReqMem.Substring(str_ReqMem.Length - 2);

                if (ReqMemUnits == "Mn")
                {
                    // ReqMem is expressed as memory per node. #nodes always = 1 on longleaf
                    if (ReqMem < ReqMemThreshold)
                        return true;
                }
                else if (ReqMemUnits == "Mc")
                {
                    // ReqMem is expressed as memory per core; must multiply by number of cores
                    int NCPUS = int.Parse(thisRec[colRef["NCPUS"]]);
                    if ((ReqMem * NCPUS) < ReqMemThreshold)
                        return true;
                }
                else
                {
                    Console.WriteLine($"*** unrecognized units for ReqMem. str_ReqMem = {str_ReqMem}");
                    return true;
                }
            }
            catch (Exception)
            {
                Console.WriteLine($"*** error parsing ReqMem. str_ReqMem = {str_ReqMem}");
                return true;
            }

            return false;
        }


        public static double ComputeWeightedMemEfficiency(double memeff, double cpueff, double hrselapsed, int reqmem, int ncpus, double MaxReqMem4Partition)
        {
            // any job for which memEffeciency is > x% does not need to be weighted, job is considered sufficiently effecient
            if (memeff > 80.0)
                return memeff;

            double requestedButUnusedMem = ((100.0 - memeff) / 100.0) * reqmem;
            double requestedButUnusedMemN = requestedButUnusedMem / MaxReqMem4Partition;   // normalized by max allowable 

            double elapsedN = hrselapsed / 264.0;    // 264 = 11*24 => 11 days max runtime
            double ncpusN = ncpus / 256;             // 256 = max requestable cpus

            double w1 = 0.5; // reqmem
            double w2 = 0.3; // elapsed
            double w3 = 0.2; // ncpus

            // double weightedMemEff = memeff * (1 - (w1 * requestedButUnusedMemN) + (w2 * elapsedN) + (w3 * ncpusN));

            double multiplier = (w1 * requestedButUnusedMemN) + (w2 * elapsedN) + (w3 * ncpusN);

            // extra penalty zone for jobs with eff < 0.60
            if (memeff < 60.0)
            {
                double pctPenalty = 0.0;
                if (reqmem > 30000)
                    pctPenalty += 0.20;

                if (reqmem > 100000)
                    pctPenalty += 0.40;

                if (ncpus > 12)
                    pctPenalty += 0.15;

                if (ncpus > 40)
                    pctPenalty += 0.15;

                multiplier += (1 - multiplier) * pctPenalty;
            }

            return (memeff * (1 - multiplier));
        }


        public static double ComputeCPUEffeciency(string totalcpu, string corewalltime)
        {
            // TotalCPU can be of the form: n-nn:nn:nn  nn:nn:nn  OR  nn:nn.nnn
            // possibly more, since the nn:nn.nnn form does not match the slurm documenation
            // https://slurm.schedmd.com/sacct.html  states: [days-]hours:minutes:seconds[.microseconds]
            // however the only examples I've seen where microseconds are present, both days *and* hours are omitted

            // if microseconds are present, drop them, and add hours back in as "00"
            if (totalcpu.IndexOf(".") != -1)
                totalcpu = $"00:{totalcpu.Substring(0, totalcpu.IndexOf("."))}";

            double hrsTotalCPU = Util.ConvertTimeStringToHours(totalcpu);
            double hrsCoreWallTime = Util.ConvertTimeStringToHours(corewalltime);
            return ((hrsTotalCPU / hrsCoreWallTime) * 100);
        }



    }
}
