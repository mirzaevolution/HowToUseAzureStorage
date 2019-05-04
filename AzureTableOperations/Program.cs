using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableOperations
{
    public class Customer : TableEntity
    {
        public Customer()
        {
            if (string.IsNullOrEmpty(this.PartitionKey))
            {
                this.PartitionKey = "Customers";
            }
            if (string.IsNullOrEmpty(this.RowKey))
            {
                this.RowKey = Guid.NewGuid().ToString("n");
            }
        }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }
        public string Phone { get; set; }
    }
    public class Program
    {
        private static string _connectionString { get; set; } = @"DefaultEndpointsProtocol=https;AccountName=azurestoragetrial;AccountKey=jok0//DFUzAtGeLFJ6F7nHiqJmlSxEVUfzFmsuJmnYHRklvlY+XgC2+g0DUVXnuQMKIXoySjtfS/3GYV0GYFyw==;EndpointSuffix=core.windows.net";
        private static CloudTableClient _cloudTableClient;

        static Program()
        {
            InitializeConnection();
        }

        private static void InitializeConnection()
        {
            try
            {
                CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(_connectionString);
                _cloudTableClient = cloudStorageAccount.CreateCloudTableClient();
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        private static List<Customer> GetCustomers()
        {
            List<Customer> list = new List<Customer>();
            Random random = new Random();
            for(int i = 401; i <= 500; i++)
            {
                list.Add(new Customer
                {
                    FirstName = $"{i} FirstName",
                    LastName = $"{i} LastName",
                    Email = $"my{i}-email@gmail.com",
                    Phone = $"{random.Next(1000, 10000)}-{random.Next(10000, 100000)}"
                });
            }
            return list;
        }
        private async static void InsertBatch()
        {
            try
            {
                CloudTable customersTable = _cloudTableClient.GetTableReference("customers");
                await customersTable.CreateIfNotExistsAsync();
                List<Customer> list = GetCustomers();
                TableBatchOperation tableBatchOperation = new TableBatchOperation();
                foreach(Customer item in list)
                {
                    TableOperation tableOperation = TableOperation.Insert(item);
                    tableBatchOperation.Add(tableOperation);
                }
                var result = await customersTable.ExecuteBatchAsync(tableBatchOperation);
                Console.WriteLine($"{result.Count} rows processed successfully");
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        private async static void QueryData(string propertyName="", string value="")
        {
            try
            {
                CloudTable customersTable = _cloudTableClient.GetTableReference("customers");
                if (await customersTable.ExistsAsync())
                {

                        TableQuery<Customer> tableQuery = string.IsNullOrEmpty(propertyName) ?
                            new TableQuery<Customer>().Where(
                                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Customers")
                            ) : new TableQuery<Customer>().Where(
                                    TableQuery.CombineFilters(
                                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Customers"),
                                        TableOperators.And,
                                        TableQuery.GenerateFilterCondition(propertyName, QueryComparisons.Equal, value)));
                        TableContinuationToken token = new TableContinuationToken();
                        Stopwatch stp = Stopwatch.StartNew();
                        int rowIndex = 1;
                        do
                        {
                            TableQuerySegment<Customer> result = await customersTable.ExecuteQuerySegmentedAsync<Customer>(tableQuery, token);
                            foreach(Customer customer in result.Results)
                            {
                                Console.WriteLine($"[{rowIndex++}] {customer.RowKey} - {customer.FirstName} - {customer.LastName} - {customer.Email} - {customer.Phone}");
                            }
                            Console.WriteLine("-------------------------------------------------------------");
                            token = result.ContinuationToken;
                        } while (token != null);
                        stp.Stop();
                        Console.WriteLine($"\nScan time: {stp.Elapsed}");
                }
                else
                {
                    Console.WriteLine("Table doesn't exist");
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        private async static void QueryDataNew(string propertyName = "", string value = "")
        {
            try
            {
                CloudTable customersTable = _cloudTableClient.GetTableReference("customers");
                if (await customersTable.ExistsAsync())
                {

                    TableQuery<Customer> tableQuery = string.IsNullOrEmpty(propertyName) ?
                           new TableQuery<Customer>().Where(
                               TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Customers")
                           ) : new TableQuery<Customer>().Where(
                                   TableQuery.CombineFilters(
                                       TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Customers"),
                                       TableOperators.And,
                                       TableQuery.GenerateFilterCondition(propertyName, QueryComparisons.Equal, value)));
                    var rowIndex = 1;
                    Stopwatch stp = Stopwatch.StartNew();

                    var result = customersTable.ExecuteQuery<Customer>(tableQuery);
                    stp.Stop();
                    foreach (Customer customer in result)
                    {
                        Console.WriteLine($"[{rowIndex++}] {customer.RowKey} - {customer.FirstName} - {customer.LastName} - {customer.Email} - {customer.Phone}");
                    }
                    Console.WriteLine("-------------------------------------------------------------");
                    Console.WriteLine($"\nScan time: {stp.Elapsed}");

                }
                else
                {
                    Console.WriteLine("Table doesn't exist");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        private async static void QueryDataAdvanced(string[] selectedProperties, string propertyName = "", string value = "")
        {
            try
            {
                CloudTable customersTable = _cloudTableClient.GetTableReference("customers");
                if (await customersTable.ExistsAsync())
                {

                    TableQuery tableQuery = string.IsNullOrEmpty(propertyName) ?
                           new TableQuery().Where(
                               TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Customers")
                           ) : new TableQuery().Where(
                                   TableQuery.CombineFilters(
                                       TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Customers"),
                                       TableOperators.And,
                                       TableQuery.GenerateFilterCondition(propertyName, QueryComparisons.Equal, value)))
                    .Select(selectedProperties);
                    var rowIndex = 1;
                    Stopwatch stp = Stopwatch.StartNew();

                    var result = customersTable.ExecuteQuery(tableQuery);
                    stp.Stop();
                    foreach (DynamicTableEntity customer in result)
                    {
                        Console.Write($"[{rowIndex++}] ");
                        foreach(var property in customer.Properties)
                        {
                            Console.Write($"{property.Key}:`{property.Value.StringValue}`; ");
                        }
                        Console.WriteLine();
                    }
                    Console.WriteLine("-------------------------------------------------------------");
                    Console.WriteLine($"\nScan time: {stp.Elapsed}");

                }
                else
                {
                    Console.WriteLine("Table doesn't exist");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        static void Main(string[] args)
        {
            //InsertBatch();
            //QueryData();
            //QueryDataNew();
            //QueryData("Email", "my74-email@email.com");
            //QueryDataNew("Email", "my74-email@email.com");
            QueryDataAdvanced(new string[] { "FirstName", "LastName", "Email" });

            Console.ReadLine();
        }
    }
}
