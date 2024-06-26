using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace iAndon.Biz.Logic.Models
{
    public class Node : DM_MES_NODE
    {
        public List<MES_NODE_EVENT> NodeEvents { get; set; }
        public Node Cast(DM_MES_NODE tblNode)
        {
            return new Node()
            {
                //Properties
                NODE_ID = tblNode.NODE_ID,
                NODE_NAME = tblNode.NODE_NAME,
                NODE_TYPE_ID = tblNode.NODE_TYPE_ID,
                DEVICE_ID = tblNode.DEVICE_ID,
                ZONE_ID = tblNode.ZONE_ID,
                LINE_ID = tblNode.LINE_ID,
                DESCRIPTION = tblNode.DESCRIPTION,
                NUMBER_ORDER = tblNode.NUMBER_ORDER,
                ACTIVE = tblNode.ACTIVE,
                USER_ID = tblNode.USER_ID,
                NodeEvents = new List<MES_NODE_EVENT>(),
            };
        }
    }

}
