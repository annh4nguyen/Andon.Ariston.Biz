using EasyNetQ;
using Avani.Helper;
using System;
using System.Xml.Serialization;
using System.IO;
using Newtonsoft.Json;

namespace iAndon.MSG
{
    [Queue("iAndon.Andon.MSG", ExchangeName = "Andon")]
    public class Andon_MSG
    {
        [XmlElement(ElementName = "h")]
        public Header Header { get; set; }
        [XmlElement(ElementName = "b")]
        public Andon_BodyMessage Body { get; set; }
        public Andon_MSG()
        {
            this.Header = new Header();
            this.Body = new Andon_BodyMessage();
        }
        public Andon_MSG(string from, DateTime time, MessageType type)
        {
            this.Header = new Header()
            {
                From = from,
                Time = time,
                Type = type
            };
            this.Body = new Andon_BodyMessage()
            {

            };
        }
        public string Serialize()
        {
            return Utils.Serialize(this);
        }

        public static Andon_MSG Deserialize(string xml)
        {
            return Utils.Deserialize<Andon_MSG>(xml);
        }

        public void StoreRawData(Log _Logger, string _LogPath, string _LogCategory)
        {
            try
            {
                string strMessage = JsonConvert.SerializeObject(this);
                DateTime receivedTime = this.Header.Time;
                string clientIP = this.Header.From;
                strMessage = $"{strMessage}#";
                string path = Path.Combine(_LogPath, "Raws", receivedTime.Year.ToString(), receivedTime.Month.ToString("00"), receivedTime.Day.ToString("00"));
                if (!Directory.Exists(path)) Directory.CreateDirectory(path);
                string fileName = Path.Combine(path, $"{clientIP}.txt");
                if (File.Exists(fileName))
                {
                    using (StreamWriter writer = File.AppendText(fileName))
                    {
                        writer.WriteLine(strMessage);
                    }
                }
                else
                {
                    using (StreamWriter writer = File.CreateText(fileName))
                    {
                        writer.Write(strMessage);
                    }
                }
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"Error when store message: {ex} ", LogType.Error);
            }
        }


    }

    public class Andon_BodyMessage
    {
        [XmlElement(ElementName = "m")]
        public string DeviceId { get; set; }
        public int In01 { get; set; }
        public int In02 { get; set; }
        public int In03 { get; set; }
        public int In04 { get; set; }
        public int In05 { get; set; }
        public int In06 { get; set; }
    }


}

