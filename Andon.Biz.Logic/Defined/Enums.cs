namespace iAndon.Biz.Logic
{
    public enum PLAN_STATUS
    {
        Draft = 0,
        NotStart = 1,
        Proccessing = 2,
        Done = 3,
        Timeout = 4,
        Ready2Done = 5,
        UpdateOnline = 6,
        Cancel = 7,
        Ready2Cancel = 8,
    }
   
    public enum PRODUCT_STATUS
    {
        Draft = -1,
        InActive = 0,
        Active = 1,
    }
}
