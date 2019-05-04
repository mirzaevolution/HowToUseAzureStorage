using System;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.File;
using Microsoft.WindowsAzure.Storage;

namespace ConnectToAzureFSUsingConnectionString
{
    class Program
    {
        private static CloudFileClient _cloudFileClient;

        private static string LoadConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["AzureStorageConnStr"].ConnectionString;
        }
        static Program()
        {
            try
            {
                CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(LoadConnectionString());
                _cloudFileClient = cloudStorageAccount.CreateCloudFileClient();
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        private async static void DownloadContent()
        {
            CloudFileShare trialFileShare = _cloudFileClient.GetShareReference("trial");
            if(await trialFileShare.ExistsAsync())
            {
                CloudFileDirectory rootDirectory = trialFileShare.GetRootDirectoryReference();
                CloudFileDirectory textFoldersDirectory = rootDirectory.GetDirectoryReference("text-folders");
                if(await textFoldersDirectory.ExistsAsync())
                {
                    CloudFile myTargetFile = textFoldersDirectory.GetFileReference("my targets.txt");
                    if(await myTargetFile.ExistsAsync())
                    {
                        string content = await myTargetFile.DownloadTextAsync();
                        Console.WriteLine(content);
                    }
                }
            }
            else
            {
                Console.WriteLine("File share doesn't exist");
            }
        }
        static void Main(string[] args)
        {
            DownloadContent();
            Console.ReadLine();
        }
    }
}
