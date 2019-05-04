using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using System.Configuration;
namespace AppendBlob1
{
    class Program
    {
        private static CloudBlobClient _cloudBlobClient;
        static Program()
        {
            AuthenticateUser();
        }
        private static AzureAADObject GetAzureAADObject()
        {
            AzureAADObject azureAADObject = new AzureAADObject();
            azureAADObject.TenantId = ConfigurationManager.AppSettings[nameof(azureAADObject.TenantId)];
            azureAADObject.ClientId = ConfigurationManager.AppSettings[nameof(azureAADObject.ClientId)];
            azureAADObject.ResourceId = ConfigurationManager.AppSettings[nameof(azureAADObject.ResourceId)];
            azureAADObject.RedirectUri = ConfigurationManager.AppSettings[nameof(azureAADObject.RedirectUri)];
            azureAADObject.Authority = string.Format(ConfigurationManager.AppSettings[nameof(azureAADObject.Authority)], azureAADObject.TenantId);
            azureAADObject.StorageAccountName = ConfigurationManager.AppSettings[nameof(azureAADObject.StorageAccountName)];
            azureAADObject.EndPointSuffix = ConfigurationManager.AppSettings[nameof(azureAADObject.EndPointSuffix)];
            return azureAADObject;
        }
        private static void AuthenticateUser()
        {
            try
            {
                AzureAADObject azureAADObject = GetAzureAADObject();
                AuthenticationContext authenticationContext =
                    new AuthenticationContext(azureAADObject.Authority);
                AuthenticationResult authenticationResult =
                    authenticationContext.AcquireTokenAsync(azureAADObject.ResourceId, azureAADObject.ClientId, new Uri(azureAADObject.RedirectUri), new PlatformParameters(PromptBehavior.Auto)).Result;
                StorageCredentials storageCredentials = new StorageCredentials(new TokenCredential(authenticationResult.AccessToken));
                CloudStorageAccount cloudStorageAccount = new CloudStorageAccount(storageCredentials, azureAADObject.StorageAccountName, azureAADObject.EndPointSuffix, true);
                _cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        private async static void UploadText()
        {
            try
            {
                CloudBlobContainer textFilesContainer =   _cloudBlobClient.GetContainerReference("text-files");
                
                if(await textFilesContainer.ExistsAsync())
                {
                    CloudAppendBlob myTextBlob = textFilesContainer.GetAppendBlobReference($"text-{Guid.NewGuid().ToString("N")}.txt");
                    await myTextBlob.CreateOrReplaceAsync();

                    Console.WriteLine("Appending data to the file..");

                    for (int i = 1; i <= 100; i++)
                    {
                        string text = $"Line #{i} -> {Guid.NewGuid().ToString("N")}, @ {DateTime.UtcNow.ToLongTimeString()}{Environment.NewLine}";
                        await myTextBlob.AppendTextAsync(text);
                    }
                    Console.WriteLine("Finished. Now, adding some metadata");


                    myTextBlob.Metadata.Add("author", "Mirza Ghulam Rasyid");
                    myTextBlob.Metadata.Add("fileName", myTextBlob.Name);
                    myTextBlob.Metadata.Add("creationDateTime", DateTime.UtcNow.ToString());
                    await myTextBlob.SetMetadataAsync();
                    Console.WriteLine("Finished");
                }
                else
                {
                    Console.WriteLine("Container doesn't exist");
                }

            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        static void Main(string[] args)
        {
            UploadText();
            Console.ReadLine();
        }
    }
}
