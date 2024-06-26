using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace iAndon.Biz.Logic.Models
{
    public class PMSData
    {
        public int productlineid { get; set; }
        public double planid { get; set; }
        public double ponumber { get; set; }
        public string productcode { get; set; }
        public string productname { get; set; }
        public string model { get; set; }
        public int planquantity { get; set; }
        public int actualquantity { get; set; }
        public string status { get; set; }
        public string lastproductiontime { get; set; }
    }
}
