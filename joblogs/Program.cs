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
        public const string InputFileLocation = @"E:\slurmdata\longleaf_data\jobs\";
        public const string InputFileExtension = "*.csv";
        public const string OutputFile = "results.txt";
        public const string Delimiter = "|";
        public const int BatchSize = 1000;          // number of rows to hold in memory and process in parallel

        //public static int TempCounter = 0;
        //public const string InputFile = @"E:\slurmdata\longleaf_data\jobs\2020-03-01_jobs.csv";

        static void Main(string[] args)
        {
            var jobLogFiles = Directory.EnumerateFiles(InputFileLocation, InputFileExtension, SearchOption.TopDirectoryOnly);

            List<string> ColsOfInterest = LoadColumnsOfInterest();

            foreach (var jobLogFile in jobLogFiles)
                ProcessFile(jobLogFile, ColsOfInterest);

        }


        public static void ProcessFile(string fileName, List<string> ColsOfInterest)
        {
            int counter = 0;

            Dictionary<string, int> ColumnReference = GetColumnReferences(ColsOfInterest, fileName);

            List<string> WorkSet = new List<string>();

            string line = "";
            int filteredRecords = 0;

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
                            ProcessWorkSet(WorkSet, ColumnReference);
                            WorkSet.Clear();
                        }
                        counter++;
                    }
                    else
                        filteredRecords++;
                }
            }

            // catch remaining records in WorkSet
            ProcessWorkSet(WorkSet, ColumnReference);
            WorkSet.Clear();

            Console.WriteLine($"{filteredRecords} records filtered from {fileName}");
        }


        public static void ProcessWorkSet(List<string> workset, Dictionary<string, int> colref)
        {
            ConcurrentBag<string> cb = new ConcurrentBag<string>();
            List<string> colsOfInterest = LoadColumnsOfInterest();

            Parallel.ForEach((workset), (line) =>
            {
                StringBuilder sb = new StringBuilder();
                string[] parts = line.Split(Delimiter);

                // straight copy for the fieldsOfInterest
                foreach (var item in colsOfInterest)
                {
                    sb.Append($"{parts[colref[item]]}");
                    sb.Append($"{Delimiter}");
                }

                // memory effeciency = (MaxRSS / ReqMem) * 100
                // ReqMem fields inlcude "Mn" as the unit, must be stripped
                string str_ReqMem = parts[colref["ReqMem"]];
                int ReqMem = int.Parse(str_ReqMem.Substring(0, (str_ReqMem.Length - 2)));
                float MaxRSS = float.Parse(parts[colref["MaxRSS"]]);
                float MemEff = (float)Math.Round(((MaxRSS / ReqMem) * 100), 2);
                sb.Append($"{MemEff}");
                sb.Append($"{Delimiter}");

                // TBD: compute cpu effeciency
                //    Compare "TotalCPU" with computed field core-walltime
                //    core-walltime = (NCPUS * "Elapsed")
                //


                cb.Add(sb.ToString());
                sb.Clear();
            });

            File.AppendAllLines(OutputFile, cb);
        }



        public static bool FilterOutThisRecord(string line, Dictionary<string, int> colReference)
        {
            // This contains all logic that would cause one to ignore a particular line from the job log data

            var thisRec = line.Split(Delimiter);


            // filter unwanted job states
            if (thisRec[colReference["State"]] == "RUNNING" || thisRec[colReference["State"]] == "SUSPENDED")
                return true;

            // filter out records where elapsed time is < 5 minutes
            string elapsed = thisRec[colReference["Elapsed"]];
            if (elapsed.IndexOf("-") == -1)
            {
                // elapsed time was less than one day
                string[] hhmmss = elapsed.Split(":");
                int hh = int.Parse(hhmmss[0]);
                int mm = int.Parse(hhmmss[1]);

                if (hh == 0)
                    if (mm < 5)
                        return true;
            }


            // filter out records where requested memory < 3 GB
            string str_ReqMem = thisRec[colReference["ReqMem"]];
            if (str_ReqMem.Length < 3)  // some cancelled jobs show as "0n" for ReqMem
                return true;

            try
            {
                int ReqMem = int.Parse(str_ReqMem.Substring(0, (str_ReqMem.Length - 2)));
                if (ReqMem < 3096)
                    return true;
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
                {
                    Console.WriteLine($"   {item.Key}:{item.Value}");
                }
            }

            return dict;
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

        // InputFile = @"E:\slurmdata\longleaf_data\jobs\2020-03-08_jobs.csv";
        // line number 302 in 2020-03-08_jobs.csv
        // 52100077  (skip 301)  MaxRSS 18028.07, ReqMem = 98304 Mn   seff: 18.34% of 96.00 GB
        //
        // slurm log:
        //   TotalCPU: 20:29:26
        //   Timelimit: 7-00:00:00
        //   End:    2020-03-19T02:21:35
        //   Start:  2020-03-19T01:15:51
        //           2020-03-19T01:05:44   compute(End - Start)
        //   Elapsed:           01:05:44
        //
        //
        // seff:
        //   CPU Utilized: 20:29:26                  ---> TotalCPU
        //   Eff 93.52% of 21:54:40 core-walltime    ---> core-walltime = (NCPUS * elapsed) = 20 * 01:05:44
    }
}
