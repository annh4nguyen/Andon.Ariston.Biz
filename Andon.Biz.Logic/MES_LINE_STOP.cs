//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated from a template.
//
//     Manual changes to this file may cause unexpected behavior in your application.
//     Manual changes to this file will be overwritten if the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace iAndon.Biz.Logic
{
    using System;
    using System.Collections.Generic;
    
    public partial class MES_LINE_STOP
    {
        public string LINE_STOP_ID { get; set; }
        public string LINE_ID { get; set; }
        public string LINE_CODE { get; set; }
        public string LINE_NAME { get; set; }
        public string WORK_PLAN_ID { get; set; }
        public decimal DAY { get; set; }
        public string SHIFT_ID { get; set; }
        public System.DateTime START { get; set; }
        public System.DateTime FINISH { get; set; }
        public string EVENTDEF_ID { get; set; }
        public string EVENTDEF_NAME_VN { get; set; }
        public string EVENTDEF_NAME_EN { get; set; }
        public string EVENTDEF_COLOR { get; set; }
        public string REASON_ID { get; set; }
        public string REASON_NAME_VN { get; set; }
        public string REASON_NAME_EN { get; set; }
        public decimal DURATION { get; set; }
    }
}
