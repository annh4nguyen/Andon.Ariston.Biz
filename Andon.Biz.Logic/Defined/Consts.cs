using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace iAndon.Biz.Logic
{
    public class Consts
    {

        public static string LOG_CATEGORY = "Biz";


  

        public static DateTime DEFAULT_TIME = new DateTime(2024, 1, 1, 6, 0, 0); //Bổ sung cái này cho đỡ lỗi
        public static DateTime MAX_TIME = new DateTime(2050, 1, 1, 6, 0, 0); //Bổ sung cái này cho đỡ lỗi

        public static int ON_STATUS = 1;
        public static int OFF_STATUS = 0;

        public static int DEFAULT_QUANTITY = 1260;
        public static int DEFAULT_CYCLE_TIME = 20;


        public static string Production_Good = "Good";
        public static string Production_Fast = "Fast";
        public static string Production_Delayed = "Delayed";
        public static string Production_Stop = "Stopped";

        //====================================================
        //EVENT_DEF
        public static string EVENTDEF_RUNNING = "1";
        public static string EVENTDEF_STOP = "2";
        public static string EVENTDEF_NOPLAN = "101";
        public static string EVENTDEF_BREAK = "100";
        public static string EVENTDEF_DISCONNECT = "11";
        public static string EVENTDEF_DEFAULT = "0";
        //====================================================


        public static int CUSTOMER_ID = 3;
        public static int FACTORY_ID = 1;

        // Date 15-02-2022 cập nhật lại format ngày từ dd/MM/yyyy -> yyyy-MM-dd
        public static string MIN_DATE = "2022-01-01";
        //public static string MAX_DATE = "31/12/3000";
        // Date 15-02-2022 cập nhật lại format ngày từ dd/MM/yyyy -> yyyy-MM-dd
        public static string MAX_DATE = "2222-12-31";

        public static int START_TIME_SLOT = 65;

        public static int HOUR_FOR_NEW_DAY = 6;

        public static int VERIFY_EVENT = 30; //30 giây

        public static int BUFFER_TIME_IN_MINUTE = 1;
        public static int BUFFER_TIME_IN_SECOND = 0;

        public const int MinuteArchive = 1;
        public const int HourArchive = 2;
        public const int DayArchive = 3;
        public const int MonthArchive = 4;
        public const int YearArchive = 5;

        public long Time2Num(DateTime time, int type)
        {
            if (type == YearArchive) return long.Parse($"{time:yyyy}");
            if (type == MonthArchive) return long.Parse($"{time:yyyyMM}");
            if (type == DayArchive) return long.Parse($"{time:yyyyMMdd}");
            if (type == HourArchive) return long.Parse($"{time:yyyyMMddHH}");
            if (type == MinuteArchive) return long.Parse($"{time:yyyyMMddHHmm}");
            return 0;
        }
        public DateTime Num2Time(long num, int type)
        {
            if (type == MinuteArchive) return new DateTime(int.Parse($"{num:0}".Substring(0, 4)), int.Parse($"{num:0}".Substring(4, 2)), int.Parse($"{num:0}".Substring(6, 2)), int.Parse($"{num:0}".Substring(8, 2)), int.Parse($"{num:0}".Substring(10, 2)), 0);
            if (type == HourArchive) return new DateTime(int.Parse($"{num:0}".Substring(0, 4)), int.Parse($"{num:0}".Substring(4, 2)), int.Parse($"{num:0}".Substring(6, 2)), int.Parse($"{num:0}".Substring(8, 2)), 0, 0);
            if (type == DayArchive) return new DateTime(int.Parse($"{num:0}".Substring(0, 4)), int.Parse($"{num:0}".Substring(4, 2)), int.Parse($"{num:0}".Substring(6, 2)), 0, 0, 0);
            if (type == MonthArchive) return new DateTime(int.Parse($"{num:0}".Substring(0, 4)), int.Parse($"{num:0}".Substring(4, 2)), 1, 0, 0, 0);
            return new DateTime(int.Parse($"{num:0}".Substring(0, 4)), 1, 1, 0, 0, 0);
        }

    }
}
