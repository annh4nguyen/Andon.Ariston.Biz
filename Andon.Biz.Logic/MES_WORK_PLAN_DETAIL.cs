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
    
    public partial class MES_WORK_PLAN_DETAIL
    {
        public string WORK_PLAN_DETAIL_ID { get; set; }
        public string WORK_PLAN_ID { get; set; }
        public string LINE_ID { get; set; }
        public decimal DAY { get; set; }
        public string SHIFT_ID { get; set; }
        public System.DateTime PLAN_START { get; set; }
        public System.DateTime PLAN_FINISH { get; set; }
        public string WORK_ORDER_CODE { get; set; }
        public string WORK_ORDER_PLAN_CODE { get; set; }
        public string PO_CODE { get; set; }
        public string PRODUCT_ID { get; set; }
        public string PRODUCT_CODE { get; set; }
        public string CONFIG_ID { get; set; }
        public decimal TAKT_TIME { get; set; }
        public int STATION_QUANTITY { get; set; }
        public int BATCH { get; set; }
        public int HEAD_COUNT { get; set; }
        public decimal PLAN_QUANTITY { get; set; }
        public decimal START_AT { get; set; }
        public decimal FINISH_AT { get; set; }
        public string DESCRIPTION { get; set; }
        public int STATUS { get; set; }
    }
}
