using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace joblogs
{
    // as a .net core console application, this code will run on Windows, Mac, and Linux
    class Program
    {
        public const string InputFileLocation = "/proj/its/slurmdata/longleaf_data/jobs/";
        public const string InputFileNameMask = "2020-*.csv";
        public const string OutputFile = "/proj/its/johnmcge/out3.txt";

        public const string Delimiter = "|";
        public const int BatchSize = 1000;    // number of rows to hold in memory and process in parallel

        public static Dictionary<string, int> PartitionMaxReqMem = new Dictionary<string, int>();


        //public const string InputFileLocation = @"E:\slurmdata\longleaf_data\jobs";
        //public const string OutputFile = @"C:\Code\Longleaf\out1.txt";


        static void Main(string[] args)
        {
            var jobLogFiles = Directory.EnumerateFiles(InputFileLocation, InputFileNameMask, SearchOption.TopDirectoryOnly);

            SetupPartitionInfo();
            List<string> ColsOfInterest = LoadColumnsOfInterest();
            WriteHeaderRow(ColsOfInterest);

            foreach (var jobLogFile in jobLogFiles)
                ProcessFile(jobLogFile, ColsOfInterest);
        }


        public static void ProcessFile(string fileName, List<string> ColsOfInterest)
        {
            // get header from file, build dictionary of index locations for 
            // columns of interest for this specific log file
            Dictionary<string, int> ColumnReference = GetColumnReferences(ColsOfInterest, fileName);

            string line = "";
            int filteredRecords = 0;
            int counter = 0;
            List<string> WorkSet = new List<string>();

            using (StreamReader sr = new StreamReader(fileName))
            {
                line = sr.ReadLine(); // skip the header Row

                while ((line = sr.ReadLine()) != null)
                {
                    if (!FilterOutThisRecord(line, ColumnReference))
                    {
                        WorkSet.Add(line);
                        if ((counter % BatchSize) == 0)
                        {
                            ProcessWorkSet(WorkSet, ColumnReference, ColsOfInterest);
                            WorkSet.Clear();
                        }
                        counter++;
                    }
                    else
                        filteredRecords++;
                }
            }

            // catch remaining records in WorkSet
            ProcessWorkSet(WorkSet, ColumnReference, ColsOfInterest);
            WorkSet.Clear();

            Console.WriteLine($"{filteredRecords} records filtered from {fileName}");
        }


        public static void ProcessWorkSet(List<string> workset, Dictionary<string, int> colRef, List<string> colsOfInterest)
        {
            ConcurrentBag<string> cb = new ConcurrentBag<string>();

            Parallel.ForEach((workset), (line) =>
            {
                StringBuilder sb = new StringBuilder();
                string[] thisRec = line.Split(Delimiter);

                // straight copy for the fieldsOfInterest
                foreach (var item in colsOfInterest)
                {
                    sb.Append($"{thisRec[colRef[item]]}");
                    sb.Append($"{Delimiter}");
                }

                // CPUEffeciency: Compare "TotalCPU" with computed field core-walltime; core-walltime = (NCPUS * "Elapsed")
                int ncpus = int.Parse(thisRec[colRef["NCPUS"]]);
                double hrsElapsed = ConvertTimeStringToHours(thisRec[colRef["Elapsed"]]);
                double hrsCoresElapsed = hrsElapsed * ncpus;

                string corewalltime = ConvertHoursToTimeString(hrsCoresElapsed);
                double CpuEff = ComputeCPUEffeciency(thisRec[colRef["TotalCPU"]], corewalltime);
                sb.Append($"{Math.Round(CpuEff, 2)}");
                sb.Append($"{Delimiter}");

                // MemEffeciency:  (MaxRSS / ReqMem) * 100;  ReqMem field inlcudes unit, must be stripped and possibly converted
                string str_ReqMem = thisRec[colRef["ReqMem"]];
                int ReqMem = int.Parse(str_ReqMem.Substring(0, (str_ReqMem.Length - 2)));
                string ReqMemUnits = str_ReqMem.Substring(str_ReqMem.Length - 2);
                if (ReqMemUnits == "Mc")
                    ReqMem *= int.Parse(thisRec[colRef["NCPUS"]]);

                float MaxRSS = float.Parse(thisRec[colRef["MaxRSS"]]);
                double MemEff = (double)Math.Round(((MaxRSS / ReqMem) * 100), 2);
                sb.Append($"{MemEff}");
                sb.Append($"{Delimiter}");

                // WeightedMemEffeciency
                double weightedMemEff = ComputeWeightedMemEfficiency(MemEff, CpuEff, hrsElapsed, ReqMem, ncpus, thisRec[colRef["Partition"]]);
                sb.Append($"{Math.Round(weightedMemEff, 2)}");
                sb.Append($"{Delimiter}");

                // compute GBHours Requested = (ReqMem / 1000) * Elapsed-Hrs
                double GBHoursReq = (ReqMem / 1000.0) * hrsElapsed;
                sb.Append($"{(int)(GBHoursReq)}");
                sb.Append($"{Delimiter}");

                // compute GBHours actually used
                double GBHoursUsed = (MaxRSS / 1000.0) * hrsElapsed;
                sb.Append($"{(int)(GBHoursUsed)}");

                cb.Add(sb.ToString());
                sb.Clear();
            });

            File.AppendAllLines(OutputFile, cb);
        }

        public static void WriteHeaderRow(List<string> colsOfInterest)
        {
            //  ProcessWorkSet() defines the structure of the output data
            //  this header row needs to mirror the structure defined in ProcessWorkSet()

            StringBuilder sb = new StringBuilder();

            foreach (var item in colsOfInterest)
            {
                sb.Append($"{item}");
                sb.Append($"{Delimiter}");
            }

            sb.Append($"CPUEffeciency");
            sb.Append($"{Delimiter}");

            sb.Append($"MemEffeciency");
            sb.Append($"{Delimiter}");

            sb.Append($"WeightedMemEffeciency");
            sb.Append($"{Delimiter}");

            sb.Append($"GBHoursRequested");
            sb.Append($"{Delimiter}");

            sb.Append($"GBHoursUsed");
            sb.Append($"{Environment.NewLine}");

            File.AppendAllText(OutputFile, sb.ToString());
        }


        public static bool FilterOutThisRecord(string line, Dictionary<string, int> colRef)
        {
            // All logic that would cause one to ignore a particular line from the job log data

            var thisRec = line.Split(Delimiter);
            int ReqMemThreshold = 3096;

            // filter unwanted job states
            if (thisRec[colRef["State"]] == "RUNNING" || 
                thisRec[colRef["State"]] == "SUSPENDED" ||
                thisRec[colRef["State"]] == "OUT_OF_MEMORY")
                return true;

            // filter out partitions for which we do not have a max ReqMem configured
            if (!PartitionMaxReqMem.ContainsKey(thisRec[colRef["Partition"]]))
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

        
        public static Dictionary<string, int> GetColumnReferences(List<string> colsOfInterest, string fileName)
        {
            // create a dictionary of just the ColumnsOfInterest, including the index of each of those
            // columns from the header of the current file; so we can directly reference and 
            // iterate through just the fields of interest

            Dictionary<string, int> dict = new Dictionary<string, int>();

            string line = "";
            try
            {
                using (StreamReader sr = new StreamReader(fileName))
                {
                    line = sr.ReadLine();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to read header row from file: {fileName}");
                Console.WriteLine($"{e}");
                return dict;  // returning null dictionary
            }

            var columnsInFile = line.Split(Delimiter);

            foreach (var item in colsOfInterest)
            {
                int ndx = Array.IndexOf(columnsInFile, item);
                if (ndx > -1)
                    dict.Add(item, ndx);
            }

            int diff = colsOfInterest.Count() - dict.Count();
            if (diff > 0)
            {
                // there is at least one ColumnOfInterest not found in the file; unexpected 
                Console.WriteLine($"{diff} columns of interest were not found in {fileName}. The following were found:");
                foreach (var item in dict)
                    Console.WriteLine($" {item.Key}:{item.Value}");
            }

            return dict;
        }


        public static double ComputeWeightedMemEfficiency(double memeff, double cpueff, double hrselapsed, int reqmem, int ncpus, string partition)
        {
            // any job for which memEffeciency is > 89% does not need to be weighted
            if (memeff > 89.0)
                return memeff;

            double requestedButUnusedMem = ((100.0 - memeff) / 100.0) * reqmem;
            double requestedButUnusedMemN = requestedButUnusedMem / (double)PartitionMaxReqMem[partition];   // normalized by max allowable 

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

            double hrsTotalCPU = ConvertTimeStringToHours(totalcpu);
            double hrsCoreWallTime = ConvertTimeStringToHours(corewalltime);
            return ((hrsTotalCPU / hrsCoreWallTime) * 100);
        }


        public static double ConvertTimeStringToHours(string timeString)
        {
            double result = 0.0;

            int dashLocation = timeString.IndexOf("-");
            if (dashLocation != -1)
            {
                string days = timeString.Substring(0, dashLocation);
                result += int.Parse(days) * 24;
                timeString = timeString.Substring(dashLocation + 1);
            }

            string[] parts = timeString.Split(":");
            if (parts.Length > 2)
                result += (double)int.Parse(parts[0]) + ((double)int.Parse(parts[1]) / 60) + ((double)int.Parse(parts[2]) / 3600);

            return result;
        }
        public static string ConvertHoursToTimeString(double hrs)
        {
            string result = "";

            if (hrs > 24)
            {
                int days = (int)(hrs / 24);
                hrs = hrs - (days * 24);
                result += $"{days}-";
            }

            var ts = TimeSpan.FromHours(hrs);
            result += $"{ts.Hours:D2}:{ts.Minutes:D2}:{ts.Seconds:D2}";

            return result;
        }


        public static List<string> LoadColumnsOfInterest()
        {
            // this list also defines the format of the output file. 
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

        public static void SetupPartitionInfo()
        {
            // we neeed max allowed memory per partition to normalize ReqMem as a percentage of what is available to be requested
            // if a partition is Not listed here, all jobs from that parition will be skipped

            PartitionMaxReqMem.Add("general", 80000);        //     80 gb
            PartitionMaxReqMem.Add("general_big", 750000);   //    750 gb
            PartitionMaxReqMem.Add("hov", 750000);           //    750 gb
            PartitionMaxReqMem.Add("snp", 750000);           //    750 gb
            PartitionMaxReqMem.Add("spill", 241000);         //    241 gb
            PartitionMaxReqMem.Add("bigmem", 3041000);       //  3,041 gb
            PartitionMaxReqMem.Add("interact", 128000);      //    128 gb
        }
    }
}
