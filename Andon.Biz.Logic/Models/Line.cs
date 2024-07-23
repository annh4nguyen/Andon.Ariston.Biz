using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace iAndon.Biz.Logic.Models
{
    public class Line : DM_MES_LINE
    {
        public string Factory_Name { get; set; }
        public List<Node> Nodes { get; set; }
        public WorkPlan WorkPlan { get; set; }
        public List<BreakTime> BreakTimes { get; set; }
        public List<MES_LINE_EVENT> LineEvents { get; set; }
        public List<MES_LINE_WORKING> LineWorkings { get; set; }
        public List<MES_LINE_STOP> LineStops { get; set; }
        public MES_REPORT_LINE ReportLine { get; set; }
        public List<MES_REPORT_LINE_DETAIL> ReportLineDetails { get; set; }
        public List<TimeData> TimeDatas { get; set; }

        public string EventDefId { get; set; }
        public string EventDefName_VN { get; set; }
        public string EventDefName_EN { get; set; }
        public string EventDefColor { get; set; }
        public string LastEventDefId { get; set; }
        public int CurrentDetail { get; set; }
        public DateTime Changed { get; set; }

        public Line Cast(DM_MES_LINE tblLine)
        {
            return new Line()
            {
                //Properties
                LINE_ID = tblLine.LINE_ID,
                LINE_CODE = tblLine.LINE_CODE,
                LINE_NAME = tblLine.LINE_NAME,
                DESCRIPTION = tblLine.DESCRIPTION,
                FACTORY_ID = tblLine.FACTORY_ID,
                IS_AUTO = tblLine.IS_AUTO,
                NUMBER_ORDER = tblLine.NUMBER_ORDER,
                GATEWAY_ID = tblLine.GATEWAY_ID,
                ACTIVE = tblLine.ACTIVE,
                USER_ID = tblLine.USER_ID,

                //Business
                Nodes = new List<Node>(),
                BreakTimes = new List<BreakTime>(),
                TimeDatas = new List<TimeData>(),
                WorkPlan = null,
                LineEvents = new List<MES_LINE_EVENT>(),
                LineWorkings = new List<MES_LINE_WORKING>(),
                LineStops = new List<MES_LINE_STOP>(),
                ReportLine = null,
                ReportLineDetails = new List<MES_REPORT_LINE_DETAIL>(),
                CurrentDetail = 0,
                Changed = DateTime.Now,
            };
        }
    }

}
