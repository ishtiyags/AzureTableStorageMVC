using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace YourApp.Models
{
    public class TableManager
    {
        private readonly CloudTable table;

        public TableManager(string tableName)
        {
            var conn = ConfigurationManager.ConnectionStrings["AzureStorage"]?.ConnectionString;
            if (string.IsNullOrEmpty(conn))
                throw new InvalidOperationException("AzureStorage connection string missing in Web.config");

            var account = CloudStorageAccount.Parse(conn);
            var client = account.CreateCloudTableClient();
            table = client.GetTableReference(tableName);

            // Synchronous create to keep constructor simple. In high-scale apps call EnsureTableExistsAsync() on startup.
            table.CreateIfNotExists();
        }

        // Optional: call this from startup if you prefer async creation
        public Task EnsureTableExistsAsync() => table.CreateIfNotExistsAsync();

        // -------------------------
        // INSERT / UPSERT / REPLACE
        // -------------------------

        // Insert or merge (upsert-like): merges provided properties with existing entity if present
        public async Task InsertOrMergeEntityAsync<T>(T entity) where T : TableEntity
        {
            var op = TableOperation.InsertOrMerge(entity);
            await table.ExecuteAsync(op);
        }

        // Insert only - will fail if the entity exists (409 Conflict)
        public async Task InsertEntityAsync<T>(T entity) where T : TableEntity
        {
            var op = TableOperation.Insert(entity);
            await table.ExecuteAsync(op);
        }

        // Insert or replace - inserts if not exists, or replaces the whole entity if exists
        public async Task InsertOrReplaceEntityAsync<T>(T entity) where T : TableEntity
        {
            var op = TableOperation.InsertOrReplace(entity);
            await table.ExecuteAsync(op);
        }

        // Strict replace - will only succeed if ETag matches (optimistic concurrency)
        public async Task ReplaceEntityAsync<T>(T entity) where T : TableEntity
        {
            var op = TableOperation.Replace(entity);
            await table.ExecuteAsync(op);
        }

        // -------------------------
        // RETRIEVE
        // -------------------------

        // Retrieve single entity by partitionKey + rowKey
        public async Task<T> RetrieveByKeysAsync<T>(string partitionKey, string rowKey) where T : TableEntity
        {
            var op = TableOperation.Retrieve<T>(partitionKey, rowKey);
            var result = await table.ExecuteAsync(op);
            return (T)result.Result;
        }

        // Retrieve by query (handles continuation tokens / paging)
        public async Task<List<T>> RetrieveEntitiesAsync<T>(TableQuery<T> query = null) where T : TableEntity, new()
        {
            var results = new List<T>();
            TableContinuationToken token = null;
            query = query ?? new TableQuery<T>();

            do
            {
                var seg = await table.ExecuteQuerySegmentedAsync(query, token);
                results.AddRange(seg.Results);
                token = seg.ContinuationToken;
            } while (token != null);

            return results;
        }

        // Convenience: retrieve all entities in a partition with optional property filter
        public async Task<List<T>> RetrieveByPartitionAndFilterAsync<T>(string partitionKey, string propertyName = null, string propertyValue = null) where T : TableEntity, new()
        {
            var pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
            TableQuery<T> query;

            if (!string.IsNullOrEmpty(propertyName))
            {
                var propFilter = TableQuery.GenerateFilterCondition(propertyName, QueryComparisons.Equal, propertyValue);
                var combined = TableQuery.CombineFilters(pkFilter, TableOperators.And, propFilter);
                query = new TableQuery<T>().Where(combined);
            }
            else
            {
                query = new TableQuery<T>().Where(pkFilter);
            }

            return await RetrieveEntitiesAsync(query);
        }

        // -------------------------
        // DELETE
        // -------------------------

        // Delete entity; if you want unconditional delete set entity.ETag = "*";
        public async Task DeleteEntityAsync<T>(T entity) where T : TableEntity
        {
            var op = TableOperation.Delete(entity);
            await table.ExecuteAsync(op);
        }

        // -------------------------
        // BATCH (same partition only)
        // -------------------------

        // Insert many entities in batches (100 ops per batch max). All entities must share the same PartitionKey per batch.
        public async Task InsertBatchAsync<T>(IEnumerable<T> entities) where T : TableEntity
        {
            var batch = new TableBatchOperation();
            string currentPartition = null;

            foreach (var e in entities)
            {
                if (currentPartition == null) currentPartition = e.PartitionKey;

                // If partition changes or batch size hits 100 -> flush
                if (!string.Equals(currentPartition, e.PartitionKey, StringComparison.Ordinal) || batch.Count == 100)
                {
                    if (batch.Count > 0) await table.ExecuteBatchAsync(batch);
                    batch = new TableBatchOperation();
                    currentPartition = e.PartitionKey;
                }

                batch.Insert(e);
            }

            if (batch.Count > 0) await table.ExecuteBatchAsync(batch);
        }
    }
}
