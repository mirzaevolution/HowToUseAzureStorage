using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BlobLeasing
{
    class Program
    {
        private static string _connectionString = @"DefaultEndpointsProtocol=https;AccountName=azurestoragetrial;AccountKey=jok0//DFUzAtGeLFJ6F7nHiqJmlSxEVUfzFmsuJmnYHRklvlY+XgC2+g0DUVXnuQMKIXoySjtfS/3GYV0GYFyw==;EndpointSuffix=core.windows.net";
        private static CloudBlobClient _cloudBlobClient;
        private static void InitializeClient()
        {
            try
            {
                CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(_connectionString);
                _cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
                System.Net.ServicePointManager.DefaultConnectionLimit = 10;


            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        static Program()
        {
            InitializeClient();
        }
        private async static void AcquireLeaseDemo()
        {
            try
            {
                string name = "LeaseDemo/text-0221e06e5e254fb3bb0bf0e37d9d0e32.txt";
                CloudBlobContainer container = _cloudBlobClient.GetContainerReference("text-files");
                await container.CreateIfNotExistsAsync();
                CloudAppendBlob cloudAppendBlob = container.GetAppendBlobReference(name);
                bool shouldContinue = true;
                if(await cloudAppendBlob.ExistsAsync())
                {
                    if(cloudAppendBlob.Properties.LeaseStatus == LeaseStatus.Locked)
                    {
                        shouldContinue = false;
                        Console.WriteLine("Blob is currently in lease");
                    }
                    else
                    {
                        await cloudAppendBlob.CreateOrReplaceAsync();
                    }
                }
                if (shouldContinue)
                {
                    string proposedLeaseId = Guid.NewGuid().ToString("n");
                    AccessCondition accessCondition = new AccessCondition();
                    OperationContext operationContext = new OperationContext();
                    Console.WriteLine("Acquiring lock...");

                    string leaseId = await cloudAppendBlob.AcquireLeaseAsync(null, proposedLeaseId, accessCondition, null, operationContext);

                    accessCondition.LeaseId = leaseId;
                    try
                    {
                        Console.WriteLine("Uploading text...");
                        for (int i = 0; i <= 50; i++)
                        {
                            await cloudAppendBlob.AppendTextAsync($"{Guid.NewGuid().ToString("n")}{Environment.NewLine}", UTF8Encoding.UTF8, accessCondition, null, operationContext);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
                    finally
                    {
                        await cloudAppendBlob.ReleaseLeaseAsync(accessCondition, null, operationContext);
                        Console.WriteLine("Process finished. Lock has been released");
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
            AcquireLeaseDemo();
            Console.ReadLine();
        }
    }
}
