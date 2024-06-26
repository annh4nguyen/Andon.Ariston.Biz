using iAndon.Biz.Logic;
using Avani.Helper;
using System;
using System.ServiceProcess;

namespace iAndon.Biz.Service
{
    public partial class Service : ServiceBase
    {
        private Log _Logger;
        private readonly string _LogCategory = "Biz";
        private MainApp _AppLogic { get; set; }
        public Service()
        {
            _Logger = iAndon.Biz.Logic.Utils.GetLog();
            _AppLogic = new MainApp();
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            try
            {
                _Logger.Write(_LogCategory, "iAndon Biz Service Start", LogType.Info);
                _AppLogic.Start();
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"iAndon Biz Service Start Error: {ex}", LogType.Error);
                this.Stop();
            }
        }

        protected override void OnStop()
        {
            try
            {
                _Logger.Write(_LogCategory, "iAndon Biz Service Stop", LogType.Info);
                _AppLogic.Stop();
            }
            catch (Exception ex)
            {
                _Logger.Write(_LogCategory, $"iAndon Biz Service Stop Error: {ex}", LogType.Error);
            }
        }
    }
}
