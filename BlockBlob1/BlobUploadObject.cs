using System;
using System.Text;

namespace BlockBlob1
{
    public class BlobUploadObject
    {
        
        public BlobUploadObject(string id, string content, bool success)
        {
            this.Id = id;
            this.Content = content;
            this.Success = success;
            this.IdBase64 = Convert.ToBase64String(UTF8Encoding.UTF8.GetBytes(id));
        }
        public string Id { get; set; }
        public string Content { get; set; }
        public bool Success { get; set; }
        public string IdBase64 { get; private set; }
    }
    public class BlobUploadObjectBytes
    {

        public BlobUploadObjectBytes(string id, byte[] contents, bool success)
        {
            this.Id = id;
            this.Contents = contents;
            this.Success = success;
            this.IdBase64 = Convert.ToBase64String(UTF8Encoding.UTF8.GetBytes(id));
        }
        public string Id { get; set; }
        public byte[] Contents { get; set; }
        public bool Success { get; set; }
        public string IdBase64 { get; private set; }
    }
}
