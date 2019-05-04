using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.File;

namespace ConnectToAzureFileStorage
{
    class Program
    {
        private static AzureAccessObject GetAzureAccessObject()
        {
            AzureAccessObject azureAccessObject = new AzureAccessObject();
            azureAccessObject.StorageAccountName = ConfigurationManager.AppSettings["AzureStorageAccount"];
            azureAccessObject.StorageAccessKey = ConfigurationManager.AppSettings["AzureStorageKey"];
            return azureAccessObject;
        }
        private static AzureAccessObject azureAccessObject;
        private static StorageCredentials credentials;
        private static CloudStorageAccount cloudStorageAccount;
        static Program()
        {
            azureAccessObject = GetAzureAccessObject();
            credentials = new StorageCredentials(azureAccessObject.StorageAccountName, azureAccessObject.StorageAccessKey);
            cloudStorageAccount = new CloudStorageAccount(credentials, true);
        }
        private static async void ConnectToAzureFileStorage()
        {
            try
            {

                CloudFileClient cloudFileClient = cloudStorageAccount.CreateCloudFileClient();
                foreach (CloudFileShare fileShare in cloudFileClient.ListShares())
                {

                    Console.WriteLine($"{fileShare.Name} - {fileShare.StorageUri}");
                    CloudFileDirectory cloudFileDirectory = fileShare.GetRootDirectoryReference();
                    if(cloudFileDirectory!=null && await cloudFileDirectory.ExistsAsync())
                    {
                        foreach (CloudFile fileItem in cloudFileDirectory.ListFilesAndDirectories())
                        {
                            Console.WriteLine($"\t[{fileItem.Name}] - {fileItem?.Properties.Length} bytes");
                        }
                    }
                   
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        private static async void GetListOfFilesSegmented()
        {
            var client = cloudStorageAccount.CreateCloudFileClient();
            CloudFileShare cloudFileShare = client.GetShareReference("trial");
            if(cloudFileShare!=null && await cloudFileShare.ExistsAsync())
            {
                CloudFileDirectory cloudFileDirectory = cloudFileShare.GetRootDirectoryReference();
                if(cloudFileDirectory!=null && await cloudFileDirectory.ExistsAsync())
                {
                    FileContinuationToken fileContinuationToken = new FileContinuationToken();
                    
                    do
                    {
                        FileResultSegment fileResultSegment = await cloudFileDirectory.ListFilesAndDirectoriesSegmentedAsync(fileContinuationToken);
                        foreach(CloudFile result in fileResultSegment.Results)
                        {
                            Console.WriteLine($"{result.Name} - {result.Properties.Length} bytes");
                        }
                        fileContinuationToken = fileResultSegment.ContinuationToken;

                    } while (fileContinuationToken != null);
                }
                else
                {
                    Console.WriteLine("Cloud file directory is null");
                }
            }
            else
            {
                Console.WriteLine("Cloud file share is null");
            }
        }
        private static void GetConnectionLimit()
        {
            Console.WriteLine(System.Net.ServicePointManager.DefaultConnectionLimit);
        }
        static void Main(string[] args)
        {
            //ConnectToAzureFileStorage();
            GetConnectionLimit();
            //GetListOfFilesSegmented();
            Console.ReadLine();
        }
    }
}
