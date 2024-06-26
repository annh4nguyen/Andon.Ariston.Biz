using System;
using System.Xml.Serialization;

namespace iAndon.MSG
{
    public class Header
    {
        [XmlElement(ElementName = "f")]
        public string From { get; set; }
        [XmlElement(ElementName = "i")]
        public DateTime Time { get; set; }
        [XmlElement(ElementName = "t")]
        public MessageType Type { get; set; }
        public Header()
        {

        }
    }

    public enum MessageType
    {
        Andon = 1,
        TempHumd = 2,
        Electricity = 3,
    }
}

