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
    
    public partial class MES_MSG_LINE_DETAIL
    {
        public string REPORT_LINE_DETAIL_ID { get; set; }
        public string LINE_ID { get; set; }
        public string TIME_NAME { get; set; }
        public System.DateTime STARTED { get; set; }
        public System.DateTime FINISHED { get; set; }
        public string PRODUCT_ID { get; set; }
        public string PRODUCT_CODE { get; set; }
        public string PRODUCT_NAME { get; set; }
        public decimal PLAN_QUANTITY { get; set; }
        public int HEAD_COUNT { get; set; }
        public decimal TAKT_TIME { get; set; }
        public decimal TARGET_QUANTITY { get; set; }
        public decimal ACTUAL_QUANTITY { get; set; }
        public decimal ACTUAL_NG_QUANTITY { get; set; }
        public decimal PLAN_RATE { get; set; }
        public decimal TARGET_RATE { get; set; }
        public decimal TIME_RATE { get; set; }
        public decimal QUALITY_RATE { get; set; }
        public decimal OEE { get; set; }
        public int INDEX_DETAIL { get; set; }
        public byte STATUS { get; set; }
        public string STATUS_NAME { get; set; }
        public System.DateTime TIME_UPDATED { get; set; }
    }
}
