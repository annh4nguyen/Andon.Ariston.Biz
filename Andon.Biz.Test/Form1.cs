using iAndon.Biz.Logic;
using System;
using System.Configuration;
using System.Windows.Forms;

namespace iAndon.Biz.Test
{
    public partial class Form1 : Form
    {
        MainApp Service = new MainApp();
        public Form1()
        {
            InitializeComponent();
        }
        private void Form1_Load(object sender, EventArgs e)
        {
            txtCustomerID.Text = ConfigurationManager.AppSettings["CustomerId"];
            txtWebSocketURL.Text = ConfigurationManager.AppSettings["Websocket_Url"];
            txtRabbitHost.Text = ConfigurationManager.AppSettings["RabbitMQ.Host"];
            txtRabbitVHost.Text = ConfigurationManager.AppSettings["RabbitMQ.VirtualHost"];
            txtRabbitUser.Text = ConfigurationManager.AppSettings["RabbitMQ.User"];
            txtRabbitPassword.Text = ConfigurationManager.AppSettings["RabbitMQ.Password"];
            txtRabbitInterval.Text = ConfigurationManager.AppSettings["queue_interval"];
            txtArchiveInterval.Text = ConfigurationManager.AppSettings["archive_interval"];
            txtLiveInterval.Text = ConfigurationManager.AppSettings["data_live_interval"];
            txtLiveTime.Text = ConfigurationManager.AppSettings["data_live_time"];
            txtLLogPath.Text = ConfigurationManager.AppSettings["log_path"];
            txtLogLevel.Text = ConfigurationManager.AppSettings["log_level"];
        }
        private void btnStart_Click(object sender, EventArgs e)
        {
            try
            {
                btnStart.Visible = false;
                btnStop.Visible = true;
                Service.Start();
            }
            catch(Exception ex)
            {
                MessageBox.Show($"Lỗi: {ex.Message}");
                btnStart.Visible = true;
                btnStop.Visible = false;
            }
        }

        private void btnStop_Click(object sender, EventArgs e)
        {
            try
            {
                Service.Stop();
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Lỗi: {ex.Message}");
            }
            finally
            {
                btnStart.Visible = true;
                btnStop.Visible = false;
            }
        }

    }
}
