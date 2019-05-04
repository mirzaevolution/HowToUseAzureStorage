namespace QueueAADAuthentication
{
    public class AzureAADObject
    {
        public string Authority { get; set; }
        public string TenantId { get; set; }
        public string ResourceId { get; set; }
        public string ClientId { get; set; }
        public string ClientRedirectionURI { get; set; }
    }
}
