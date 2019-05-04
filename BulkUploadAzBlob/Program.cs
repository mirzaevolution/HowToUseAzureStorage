using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using BufferCopyCore;
namespace BulkUploadAzBlob
{
    public class RestrictionSizeDefault
    {
        public static readonly int Tiny = 3145728;         //3MB
        public static readonly int Small = 7340032;        //7MB
        public static readonly int Medium = 20971520;      //20MB
        public static readonly int Big = 52428800;         //50MB
    }
    public enum BufferSizeDefault
    {
        Tiny        = 512000,                       //500KB
        Small       = 2097152,                      //2MB
        Medium      = 5242880,                      //5MB
        Big         = 10485760,                     //10MB
        Extra       = 26214400,                     //25MB
    }
    class Program
    {
        private static string _connectionString = @"DefaultEndpointsProtocol=https;AccountName=azurestoragetrial;AccountKey=jok0//DFUzAtGeLFJ6F7nHiqJmlSxEVUfzFmsuJmnYHRklvlY+XgC2+g0DUVXnuQMKIXoySjtfS/3GYV0GYFyw==;EndpointSuffix=core.windows.net";
        private static CloudBlobClient _cloudBlobClient;
        static Program()
        {
            InitBlobClient();
        }
        private static void InitBlobClient()
        {
            try
            {
                _cloudBlobClient = CloudStorageAccount.Parse(_connectionString).CreateCloudBlobClient();
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        private static void UploadFile(string filename)
        {
            try
            {
                FileInfo fileInfo = new FileInfo(filename);
                long size = fileInfo.Length;
                long bufferSize = 0;
                if (size <= RestrictionSizeDefault.Tiny)
                {
                    bufferSize = (long)BufferSizeDefault.Tiny;
                }
                else if (size > RestrictionSizeDefault.Tiny && size <= RestrictionSizeDefault.Small)
                {
                    bufferSize = (long)BufferSizeDefault.Small;

                }
                else if (size > RestrictionSizeDefault.Small && size <= RestrictionSizeDefault.Medium)
                {
                    bufferSize = (long)BufferSizeDefault.Medium;

                }
                else if(size > RestrictionSizeDefault.Medium && size <= RestrictionSizeDefault.Big)
                {
                    bufferSize = (long)BufferSizeDefault.Big;
                }
                else if(size>RestrictionSizeDefault.Big)
                {
                    bufferSize = (long)BufferSizeDefault.Extra;
                }
                GeneralBufferCopy generalBufferCopy = new GeneralBufferCopy();
                byte[] fileContents = new byte[size];
                using (FileStream fs = fileInfo.Open(FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    fs.Read(fileContents, 0, fileContents.Length);
                }
                CloudBlobContainer container =  _cloudBlobClient.GetContainerReference("etc");
                container.CreateIfNotExists();
                string saveFileName = Path.GetFileName(filename);
                CloudBlockBlob blob = container.GetBlockBlobReference(saveFileName);
                
                List<string> ids = new List<string>();
                Console.WriteLine($"[#] Start uploading {saveFileName} with {bufferSize.ToString("n")} bytes buffer ............");
                void saveCallback(BufferItem bufferItem)
                {
                    try
                    {
                        string id = Convert.ToBase64String(UTF8Encoding.UTF8.GetBytes(bufferItem.Id));
                        MemoryStream ms = new MemoryStream(bufferItem.Buffer);
                        Console.Write($"[*] Uploading blob item: {id} ............");
                        blob.PutBlock(id, ms, null, null, new BlobRequestOptions
                        {
                            ParallelOperationThreadCount = 10,
                            DisableContentMD5Validation = true,
                            StoreBlobContentMD5 = false
                        }, null);
                        ids.Add(id);
                        Console.WriteLine("[OK]");

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("[ERROR]");
                        Console.WriteLine("#SaveCallback Error");
                        Console.WriteLine(ex.Message);
                    }
                }
                generalBufferCopy.CopyFileWithBuffer(fileContents, bufferSize, saveCallback, false);
                if (ids.Count > 0)
                {
                    blob.PutBlockList(ids);
                    Console.WriteLine("Blob uploaded successfully!");
                    blob.Metadata.Add(new KeyValuePair<string, string>("author", Environment.UserName));
                    blob.Metadata.Add(new KeyValuePair<string,string>("creationTimeUtc", DateTime.UtcNow.ToShortDateString()));
                    try
                    {
                        blob.SetMetadataAsync().Wait();
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine("\nError while setting up metadata");
                        Console.WriteLine(ex);
                    }
                }
                
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        static void Run()
        {
            Console.WriteLine("Press [ENTER] to select file and upload...");
            Console.ReadLine();
            OpenFileDialog openFileDialog = new OpenFileDialog
            {
                FileName = "",
                CheckPathExists = true,
                CheckFileExists = true
            };
            if(openFileDialog.ShowDialog()==DialogResult.OK)
            {
                UploadFile(openFileDialog.FileName);
            }
        }
        [STAThread]
        static void Main(string[] args)
        {
            Run();
        }
    }
}
