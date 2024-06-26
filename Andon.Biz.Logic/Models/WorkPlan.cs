using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace iAndon.Biz.Logic.Models
{
    public class WorkPlan : MES_WORK_PLAN
    {
        //public List<BreakTime> BreakTimes { get; set; } = new List<BreakTime>();
        public DateTime PlanStart { get; set; }
        public DateTime PlanFinish { get; set; }
        public List<MES_WORK_PLAN_DETAIL> WorkPlanDetails { get; set; } = new List<MES_WORK_PLAN_DETAIL>();

        public int Priority { get; set; }
        public WorkPlan Cast(MES_WORK_PLAN _tblWorkPlan)
        {
            WorkPlan workPlan = new WorkPlan()
            {
                WORK_PLAN_ID = _tblWorkPlan.WORK_PLAN_ID,
                DAY = _tblWorkPlan.DAY,
                LINE_ID = _tblWorkPlan.LINE_ID,
                SHIFT_ID = _tblWorkPlan.SHIFT_ID,
                STATUS = _tblWorkPlan.STATUS,
            };

            return workPlan;
        }
        public MES_WORK_PLAN Cast()
        {
            MES_WORK_PLAN tblWorkPlan = new MES_WORK_PLAN()
            {
                WORK_PLAN_ID = this.WORK_PLAN_ID,
                DAY = this.DAY,
                LINE_ID = this.LINE_ID,
                SHIFT_ID = this.SHIFT_ID,
                STATUS = this.STATUS,
            };
            return tblWorkPlan;
        }
    }

    public class Shift : DG_DM_SHIFT
    {
        public DateTime Start { get; set; }
        public DateTime Finish { get; set; }
        public decimal Duration { get; set; }

        public List<BreakTime> BreakTimes = new List<BreakTime>();
        
        public void Cast(DG_DM_SHIFT _shift, decimal _fullday = 0)
        {
            this.SHIFT_ID = _shift.SHIFT_ID;
            this.HOUR_START = _shift.HOUR_START;
            this.MINUTE_START = _shift.MINUTE_START;
            this.HOUR_END = _shift.HOUR_END;
            this.MINUTE_END = _shift.MINUTE_END;

            if (_fullday == 0)
            {
                _fullday = decimal.Parse($"{DateTime.Now:yyyyMMdd}");
            }
            decimal _year = _fullday / 10000;
            decimal _month = (_fullday % 10000) / 100;
            decimal _day = _fullday % 100;
            this.Start = new DateTime((int)_year, (int)_month, (int)_day, this.HOUR_START, this.MINUTE_START, 0);
            this.Finish = new DateTime((int)_year, (int)_month, (int)_day, this.HOUR_END, this.MINUTE_END, 0);
            this.Finish = this.Finish.AddMinutes(0 - Consts.BUFFER_TIME_IN_MINUTE);

            if (this.HOUR_START < Consts.HOUR_FOR_NEW_DAY)
            {
                this.Start = this.Start.AddDays(1);
            }

            if (this.HOUR_END < Consts.HOUR_FOR_NEW_DAY)
            {
                this.Finish = this.Finish.AddDays(1);
            }

            if (this.Finish < this.Start)
            {
                this.Finish = this.Finish.AddDays(1);
            }
            this.Duration = (decimal)(this.Finish - this.Start).TotalSeconds;
        }
    }

    public class BreakTime : DM_MES_BREAK_TIME
    {
        public DateTime StartTime { get; set; }
        public DateTime FinishTime { get; set; }
        public decimal Duration { get; set; }

        public void Cast(DM_MES_BREAK_TIME breakTime, decimal _fullday = 0)
        {
            this.BREAK_ID = breakTime.BREAK_ID;
            this.SHIFT_ID = breakTime.SHIFT_ID;
            this.BREAK_NAME = breakTime.BREAK_NAME;
            this.START_HOUR = breakTime.START_HOUR;
            this.START_MINUTE = breakTime.START_MINUTE;
            this.FINISH_HOUR = breakTime.FINISH_HOUR;
            this.FINISH_MINUTE = breakTime.FINISH_MINUTE;

            if (_fullday == 0)
            {
                _fullday = decimal.Parse($"{DateTime.Now:yyyyMMdd}");
            }
            decimal _year = _fullday / 10000;
            decimal _month = (_fullday % 10000) / 100;
            decimal _day = _fullday % 100;

            this.StartTime = new DateTime((int)_year, (int)_month, (int)_day, this.START_HOUR, this.START_MINUTE, 0);
            if (this.START_HOUR < Consts.HOUR_FOR_NEW_DAY)
            {
                this.StartTime = this.StartTime.AddDays(1);
            }
            this.FinishTime = new DateTime((int)_year, (int)_month, (int)_day, this.FINISH_HOUR, this.FINISH_MINUTE, 0);

            if (this.FINISH_HOUR < Consts.HOUR_FOR_NEW_DAY)
            {
                this.FinishTime = this.FinishTime.AddDays(1);
            }

            if (this.FinishTime < this.StartTime)
            {
                this.FinishTime = this.FinishTime.AddDays(1);
            }
        }
    }

    public class TimeData
    {
        public string TimeName { get; set; }
        public DateTime Start { get; set; }
        public DateTime Finish { get; set; }
        public decimal Duration { get; set; }
    }

}
