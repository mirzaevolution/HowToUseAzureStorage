using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage;

namespace QueueAADAuthentication
{
    class Program
    {
        
        private static CloudQueueClient _cloudQueueClient;
        
        private static AzureAADObject GetConfiguration()
        {
            AzureAADObject azureAADObject = new AzureAADObject();
            azureAADObject.Authority = string.Format(ConfigurationManager.AppSettings[nameof(azureAADObject.Authority)], ConfigurationManager.AppSettings[nameof(azureAADObject.TenantId)]);
            azureAADObject.ClientId = ConfigurationManager.AppSettings[nameof(azureAADObject.ClientId)];
            azureAADObject.ClientRedirectionURI = ConfigurationManager.AppSettings[nameof(azureAADObject.ClientRedirectionURI)];
            azureAADObject.ResourceId = ConfigurationManager.AppSettings[nameof(azureAADObject.ResourceId)];
            return azureAADObject;
        }
        private static async Task AuthenticateUser()
        {
            try
            {
                AzureAADObject azureAADObject = GetConfiguration();
                AuthenticationContext authenticationContext = new AuthenticationContext(azureAADObject.Authority);
                AuthenticationResult authenticationResult = await authenticationContext.AcquireTokenAsync(azureAADObject.ResourceId, azureAADObject.ClientId, new Uri(azureAADObject.ClientRedirectionURI), new PlatformParameters(PromptBehavior.Auto));
                TokenCredential tokenCredential = new TokenCredential(authenticationResult.AccessToken);
                StorageCredentials storageCredentials = new StorageCredentials(tokenCredential);
                CloudStorageAccount cloudStorageAccount = new CloudStorageAccount(storageCredentials, "azurestoragetrial", "core.windows.net", true);
                _cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        private static async void GetMessages()
        {
            try
            {
                await AuthenticateUser();
                CloudQueue cloudQueue = _cloudQueueClient.GetQueueReference("trial-queue");
                if(await cloudQueue.ExistsAsync())
                {
                    await cloudQueue.FetchAttributesAsync();
                    foreach(var message in await cloudQueue.GetMessagesAsync(cloudQueue.ApproximateMessageCount.HasValue ? cloudQueue.ApproximateMessageCount.Value : 0))
                    {
                        Console.WriteLine(message.AsString);
                    }
                }
                else
                {
                    Console.WriteLine("Queue does not exit");
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        static void Main(string[] args)
        {
            GetMessages();
            Console.ReadLine();
        }
    }
}
