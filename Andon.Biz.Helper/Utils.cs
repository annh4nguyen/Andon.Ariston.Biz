using Avani.Helper;
using System.Configuration;

namespace iAndon.Biz.Helper
{
    public class Utils
    {
        public static Log GetLog()
        {
            string _LogPath = ConfigurationManager.AppSettings["log_path"];
            LogType logLevel = (LogType)int.Parse(ConfigurationManager.AppSettings["log_level"].ToString());
            return new Log(System.IO.Path.Combine(_LogPath, "Logs")) { Level = logLevel };
        }
    }
}
