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

namespace BlockBlob1
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
                _cloudBlobClient= cloudStorageAccount.CreateCloudBlobClient();
                System.Net.ServicePointManager.DefaultConnectionLimit = 10;


            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        static Program()
        {
            InitializeClient();
        }
        private async static void QueryAllBlobs(string containerName)
        {
            try
            {
                CloudBlobContainer container = _cloudBlobClient.GetContainerReference(containerName);
                if (await container.ExistsAsync())
                {
                    foreach(CloudBlob blob in container.ListBlobs(blobListingDetails: BlobListingDetails.All,useFlatBlobListing:true))
                    {
                        blob.FetchAttributes();
                        Console.WriteLine($"{blob.Name}");
                        Console.WriteLine($"\tStorage Url Primary: {blob.StorageUri.PrimaryUri}");
                        Console.WriteLine($"\tStorage Url Secondary: {blob.StorageUri.SecondaryUri}");
                        Console.WriteLine($"\tBlob Type: `{blob.Properties.BlobType.ToString()}`");
                        Console.WriteLine($"\tLength: `{blob.Properties.Length}`");
                        Console.WriteLine($"\tLease Status: `{blob.Properties.LeaseStatus.ToString()}`");
                        Console.WriteLine($"\tContent Type: `{blob.Properties.ContentType}`");
                        Console.WriteLine($"\tETag: `{blob.Properties.ETag}`");
                        Console.WriteLine($"\tLast Modified: `{blob.Properties.LastModified}`");
                        if(blob.Metadata!= null && blob.Metadata.Count > 0)
                        {
                            Console.WriteLine("\tMetadata:");
                            foreach(KeyValuePair<string,string> kvp in blob.Metadata)
                            {
                                Console.WriteLine($"\t\tKey: {kvp.Key}, Value: {kvp.Value}");
                            }
                        }



                    }
                }
                else
                {
                    Console.WriteLine("Container is not found");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        private async static void UploadSingleFile(string containerName, string path)
        {
            try
            {
                if (File.Exists(path))
                {
                    CloudBlobContainer container = _cloudBlobClient.GetContainerReference(containerName);
                    if(await container.ExistsAsync())
                    {
                        string name = Path.GetFileName(path);
                        CloudBlockBlob cloudBlockBlob =  container.GetBlockBlobReference(name);
                        await cloudBlockBlob.DeleteIfExistsAsync();
                        Stopwatch stopwatch = Stopwatch.StartNew();
                        await cloudBlockBlob.UploadFromFileAsync(path);
                        stopwatch.Stop();
                        Console.WriteLine("File has been uploaded successfully. Adding some metadata...");
                        cloudBlockBlob.Metadata.Add(new KeyValuePair<string, string>("author", "Mirza Ghulam Rasyid"));
                        cloudBlockBlob.Metadata.Add(new KeyValuePair<string, string>("filename", cloudBlockBlob.Name));
                        cloudBlockBlob.Metadata.Add(new KeyValuePair<string, string>("createdDate", DateTime.UtcNow.ToString()));
                        cloudBlockBlob.Metadata.Add(new KeyValuePair<string, string>("originalFilePath", path));
                        await cloudBlockBlob.SetMetadataAsync();
                        Console.WriteLine("Metadata tags have been added.");

                        Console.WriteLine($"Upload time: {stopwatch.Elapsed}");
                    }
                    else
                    {
                        Console.WriteLine("Container is not found");
                    }
                }
                else
                {
                    Console.WriteLine("File not found");
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        private async static void DemoPutBlock()
        {
            try
            {

                CloudBlobContainer container = _cloudBlobClient.GetContainerReference("text-blob");
                await container.CreateIfNotExistsAsync();
                CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference($"{Guid.NewGuid().ToString("n")}.txt");
                
                await cloudBlockBlob.DeleteIfExistsAsync();
                cloudBlockBlob.StreamMinimumReadSizeInBytes = 16384;

                List<BlobUploadObject> contents = new List<BlobUploadObject>();
                
                for(int i = 1; i <= 10; i++)
                {
                    contents.Add(new BlobUploadObject(new string(Guid.NewGuid().ToString().Take(6).ToArray()), $"Iterator => {i}# - {Guid.NewGuid().ToString("N")}{Environment.NewLine}", true));
                }
                Stopwatch stopwatch = Stopwatch.StartNew();

                foreach (var item in contents)
                {
                    try
                    {
                        await cloudBlockBlob.PutBlockAsync(item.IdBase64, 
                            new MemoryStream(UTF8Encoding.UTF8.GetBytes(item.Content)),
                            null, null,
                            new BlobRequestOptions
                            {
                                ParallelOperationThreadCount = 10,
                                DisableContentMD5Validation = true,
                                StoreBlobContentMD5 = false,
                            
                            },null);

                    }
                    catch(Exception ex)
                    {
                        item.Success = false;   
                        Console.WriteLine(ex);
                    }
                }
                await cloudBlockBlob.PutBlockListAsync(contents.Where(x=>x.Success).Select(x => x.IdBase64));
                stopwatch.Stop();
                Console.WriteLine("File has been uploaded successfully. Adding some metadata...");
                cloudBlockBlob.Metadata.Add(new KeyValuePair<string, string>("author", "Mirza Ghulam Rasyid"));
                cloudBlockBlob.Metadata.Add(new KeyValuePair<string, string>("filename", cloudBlockBlob.Name));
                cloudBlockBlob.Metadata.Add(new KeyValuePair<string, string>("createdDate", DateTime.UtcNow.ToString()));
                await cloudBlockBlob.SetMetadataAsync();
                Console.WriteLine("Metadata tags have been added.");
                Console.WriteLine($"Upload time: {stopwatch.Elapsed}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        private async static void Download(string containerName, string blobName, Action<CloudBlockBlob> action)
        {
            try
            {

                CloudBlobContainer container = _cloudBlobClient.GetContainerReference(containerName);
                if (await container.ExistsAsync())
                {
                    
                    CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(blobName);

                    if(await cloudBlockBlob.ExistsAsync())
                    {
                        action?.Invoke(cloudBlockBlob);
                    }
                    else
                    {
                        Console.WriteLine("Blob doesn't exist");
                    }
                }
                else
                {
                    Console.WriteLine("Container is not found");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        private async static void GetLinks(string containerName)
        {
            try
            {

                CloudBlobContainer container = _cloudBlobClient.GetContainerReference(containerName);
                if (await container.ExistsAsync())
                {

                    string sasKey = container.GetSharedAccessSignature(new SharedAccessBlobPolicy
                    {
                        SharedAccessStartTime = DateTime.UtcNow.AddMinutes(-5),
                        SharedAccessExpiryTime = DateTime.UtcNow.AddHours(24),
                        Permissions = SharedAccessBlobPermissions.Read
                    });
                    foreach(CloudBlob blob in container.ListBlobs(useFlatBlobListing:true))
                    {
                        Console.WriteLine($"{blob.Uri}{sasKey}");
                    }
                }
                else
                {
                    Console.WriteLine("Container is not found");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        
        private static List<BlobUploadObjectBytes> GetUploadBlobSegments(string path)
        {
            List<BlobUploadObjectBytes> blobList = new List<BlobUploadObjectBytes>();
            byte[] data = File.ReadAllBytes(path);
            int dataLength = data.Length;
            int dataReadCounter = 0;
            int chunkSize = 250 * 1024; //250KiB
            int blockIndex = 0;
            string id = string.Empty;
            do
            {
                byte[] buffer = new byte[chunkSize];
                int limit = blockIndex + chunkSize;
                for (int chunkIndex = 0; blockIndex < limit; blockIndex++, chunkIndex++)
                {
                    buffer[chunkIndex] = data[blockIndex];
                }
                dataReadCounter = blockIndex;
                blobList.Add(new BlobUploadObjectBytes(new string(Guid.NewGuid().ToString().Take(6).ToArray()), buffer, true));
            }
            while (dataLength - dataReadCounter > chunkSize);

            int finalChunkSize = dataLength - dataReadCounter;
            byte[] finalBuffer = new byte[finalChunkSize];
            for (int chunkIndex = 0; blockIndex < dataLength; blockIndex++, chunkIndex++)
            {
                finalBuffer[chunkIndex] = data[blockIndex];
            }
            
            blobList.Add(new BlobUploadObjectBytes(new string(Guid.NewGuid().ToString().Take(6).ToArray()), finalBuffer, true));
            return blobList;
        }

        private async static void UploadBlockBlobs(string containerName, string path)
        {
            try
            {
                if (File.Exists(path))
                {
                    CloudBlobContainer container = _cloudBlobClient.GetContainerReference(containerName);
                    if (await container.ExistsAsync())
                    {
                        string name = Path.GetFileName(path);
                        CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(name);
                        await cloudBlockBlob.DeleteIfExistsAsync();


                        List<BlobUploadObjectBytes> list = GetUploadBlobSegments(path);
                        Stopwatch stopwatch = Stopwatch.StartNew();

                        foreach (var blob in list)
                        {
                            try
                            {
                                Console.WriteLine($"[%]............Uploading {blob.Id}");
                                await cloudBlockBlob.PutBlockAsync(blob.IdBase64, new MemoryStream(blob.Contents, true), null);
                            }
                            catch
                            {
                                blob.Success = false;
                                Console.WriteLine($"[!]............{blob.IdBase64} failed to upload");

                            }
                        }

                        Console.WriteLine($"[#]............Commiting data");

                        await cloudBlockBlob.PutBlockListAsync(list.Where(x => x.Success).Select(x => x.Id));
                        
                        stopwatch.Stop();
                        Console.WriteLine("File has been uploaded successfully. Adding some metadata...");
                        cloudBlockBlob.Metadata.Add(new KeyValuePair<string, string>("author", "Mirza Ghulam Rasyid"));
                        cloudBlockBlob.Metadata.Add(new KeyValuePair<string, string>("filename", cloudBlockBlob.Name));
                        cloudBlockBlob.Metadata.Add(new KeyValuePair<string, string>("createdDate", DateTime.UtcNow.ToString()));
                        cloudBlockBlob.Metadata.Add(new KeyValuePair<string, string>("originalFilePath", path));
                        await cloudBlockBlob.SetMetadataAsync();
                        Console.WriteLine("Metadata tags have been added.");

                        Console.WriteLine($"Upload time: {stopwatch.Elapsed}");
                    }
                    else
                    {
                        Console.WriteLine("Container is not found");
                    }
                }
                else
                {
                    Console.WriteLine("File not found");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }


        #region Demo Update Certain Block List
        private async static void UploadSample()
        {
            try
            {
                CloudBlobContainer container = _cloudBlobClient.GetContainerReference("text-files");
                await container.CreateIfNotExistsAsync();
                List<BlobUploadObject> list = new List<BlobUploadObject>
                {
                    new BlobUploadObject("0x001", "*********" + Environment.NewLine, true),
                    new BlobUploadObject("0x002", "+++++++++" + Environment.NewLine, true),
                    new BlobUploadObject("0x003", "---------" + Environment.NewLine, true),
                    new BlobUploadObject("0x004", "=========" + Environment.NewLine, true)
                };
                CloudBlockBlob fileBlob = container.GetBlockBlobReference("sample.txt");
                foreach(BlobUploadObject obj in list)
                {
                    try
                    {
                        Console.WriteLine($"[%].................Uploading {obj.IdBase64}");
                        await fileBlob.PutBlockAsync(obj.IdBase64, new MemoryStream(UTF8Encoding.UTF8.GetBytes(obj.Content), true), null);
                        Console.WriteLine($"[@].................{obj.IdBase64} has been uploaded");
                    }
                    catch
                    {
                        Console.WriteLine($"[!].................{obj.IdBase64} failed to upload");

                        obj.Success = false;
                    }
                }
                await fileBlob.PutBlockListAsync(list.Where(x => x.Success).Select(x => x.IdBase64));
                Console.WriteLine("File has been uploaded");

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        private async static void UpdateCertainBlock()
        {
            try
            {
                CloudBlobContainer container = _cloudBlobClient.GetContainerReference("text-files");

                if(await container.ExistsAsync())
                {
                    CloudBlockBlob fileBlob = container.GetBlockBlobReference("sample.txt");
                    if(await fileBlob.ExistsAsync())
                    {
                        List<ListBlockItem> ids = new List<ListBlockItem>(await fileBlob.DownloadBlockListAsync());
                        if (ids.Count > 0)
                        {

                            string blockId = "0x002";
                            string blockIdBase64 = Convert.ToBase64String(UTF8Encoding.UTF8.GetBytes(blockId));
                            string newContent = "000000000" + Environment.NewLine;
                            List<string> idList = new List<string>();
                            if (ids.Any(x => x.Name.Equals(blockIdBase64, StringComparison.InvariantCultureIgnoreCase)))
                            {

                                await fileBlob.PutBlockAsync(blockIdBase64, new MemoryStream(UTF8Encoding.UTF8.GetBytes(newContent), true), null);
                                idList.AddRange(ids.Select(x => x.Name));
                            }
                            else
                            {
                                await fileBlob.PutBlockAsync(blockIdBase64, new MemoryStream(UTF8Encoding.UTF8.GetBytes(newContent), true),null);
                                idList.AddRange(ids.Select(x => x.Name));
                                idList.Add(blockIdBase64);
                                Console.WriteLine("Target block is not found. Additional Block has been added");

                            }
                            await fileBlob.PutBlockListAsync(idList);
                            Console.WriteLine("Done");
                        }
                        else
                        {
                            Console.WriteLine("No available blocks in the file");
                        }
                    }
                    else
                    {
                        Console.WriteLine("File doesn't exist");
                    }
                }
                else
                {
                    Console.WriteLine("Container doesn't exist");
                }
                
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

        }
        #endregion

        static void Main(string[] args)
        {
            //UploadSingleFile(@"D:\buaya.jpg");
            //UploadSingleFile(@"D:\pictures\lambo.jpg");
            //QueryAllBlobs("images");

            //DemoPutBlock();
            //Download("text-blob", "d8a05bcb03b44840bdfca6d715a8d0dc.txt", async (blob) =>
            //{
            //    string content = await blob.DownloadTextAsync();
            //    Console.WriteLine(content);
            //});
            //GetLinks("text-blob");

            //UploadBlockBlobs("etc", "video.mp4");
            //uploadsinglefile("etc", "video2.mp4");

            //UploadSample();
            UpdateCertainBlock();

            Console.ReadLine();
        }
    }
}
