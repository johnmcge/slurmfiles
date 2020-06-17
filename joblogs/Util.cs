using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace joblogs
{
    public enum JobSize
    {
        Tiny = 0,
        Small = 1,
        Medium = 2,
        Large = 3,
        Huge = 4
    }

    class Util
    {

        public static Dictionary<string, int> GetColumnReferences(Configurator cfg, string fileName)
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

            var columnsInFile = line.Split(cfg.Delimiter);

            foreach (var item in cfg.ColsOfInterest)
            {
                int ndx = Array.IndexOf(columnsInFile, item);
                if (ndx > -1)
                    dict.Add(item, ndx);
            }

            int diff = cfg.ColsOfInterest.Count() - dict.Count();
            if (diff > 0)
            {
                // there is at least one ColumnOfInterest not found in the file; unexpected 
                Console.WriteLine($"{diff} columns of interest were not found in {fileName}. The following were found:");
                foreach (var item in dict)
                    Console.WriteLine($" {item.Key}:{item.Value}");
            }

            return dict;
        }


        public static Dictionary<string, int> GetAllColumnReferences(Configurator cfg, string fileName)
        {
            // create a dictionary of ALL columns, including the index of each of those
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

            var columnsInFile = line.Split(cfg.Delimiter);

            foreach (var item in columnsInFile)
            {
                int ndx = Array.IndexOf(columnsInFile, item);
                dict.Add(item, ndx);
            }

            return dict;
        }


        public static JobSize JobSizeBin(int ncpus, int reqmem, int hrsElapsed)
        {
            // logic to bin jobs according to the enum defined in Util.cs

            // JUST A WAG - NEED REAL LOGIC HERE
            if (ncpus > 100 && hrsElapsed > 36)
                return JobSize.Huge;

            if (ncpus > 40 && hrsElapsed > 24)
                return JobSize.Large;

            if (ncpus > 12)
                return JobSize.Medium;

            return JobSize.Small;
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

        public static List<string> LoadColumnsOfInterest(string ActionOption)
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

            if (ActionOption == "pend")
            {
                lst.Add("Submit");
                lst.Add("Start");
            }

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


    }
}
