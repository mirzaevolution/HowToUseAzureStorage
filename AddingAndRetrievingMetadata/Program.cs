using System;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Queue;

namespace AddingAndRetrievingMetadata
{
    class Program
    {
        private static CloudQueueClient _cloudQueueClient;
        
        static Program()
        {
            RegisterClient();
        }

        private static string GetConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["AzureStorageConnectionString"].ConnectionString;
        }
        private static void RegisterClient()
        {
            try
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(GetConnectionString());
                _cloudQueueClient = storageAccount.CreateCloudQueueClient();
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        private async static void SetMetadata()
        {
            try
            {
                CloudQueue cloudQueue = _cloudQueueClient.GetQueueReference("trial-queue");
                if(await cloudQueue.ExistsAsync())
                {
                    cloudQueue.Metadata.Add(new KeyValuePair<string, string>("author", "Mirza Ghulam Rasyid"));
                    cloudQueue.Metadata.Add(new KeyValuePair<string, string>("version", "1.0"));
                    await cloudQueue.SetMetadataAsync();
                    Console.WriteLine("Done");
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        private async static void GetMetadata()
        {
            try
            {
                CloudQueue cloudQueue = _cloudQueueClient.GetQueueReference("trial-queue");
                if (await cloudQueue.ExistsAsync())
                {
                    await cloudQueue.FetchAttributesAsync();
                    if(cloudQueue.Metadata!=null && cloudQueue.Metadata.Count > 0)
                    {
                        foreach(KeyValuePair<string,string> kvp in cloudQueue.Metadata)
                        {
                            Console.WriteLine($"Key:{kvp.Key}, Value:{kvp.Value}");
                        }
                    }
                    else
                    {
                        Console.WriteLine("No metadata available");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        static void Main(string[] args)
        {
            //SetMetadata();
            GetMetadata();
            Console.ReadLine();
        }
    }
}
