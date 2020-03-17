// Copyright (c) ServiceStack, Inc. All Rights Reserved.
// License: https://raw.github.com/ServiceStack/ServiceStack/master/license.txt

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace ServiceStack.Aws.DynamoDb
{
    public partial class PocoDynamo
    {
        public Task ExecAsync(Func<Task> fn, Type[] rethrowExceptions = null, HashSet<string> retryOnErrorCodes = null)
            => ExecAsync(async () =>
                         {
                             await fn();

                             return true;
                         },
                         rethrowExceptions, retryOnErrorCodes);

        public async Task<T> ExecAsync<T>(Func<Task<T>> fn, Type[] rethrowExceptions = null, HashSet<string> retryOnErrorCodes = null)
        {
            var i = 0;
            Exception originalEx = null;
            DateTime? firstAttempt = null;

            if (retryOnErrorCodes == null)
            {
                retryOnErrorCodes = RetryOnErrorCodes;
            }

            do
            {
                try
                {
                    var result = await fn().ConfigureAwait(false);

                    return result;
                }
                catch(Exception outerEx)
                {
                    i++;

                    if (!firstAttempt.HasValue)
                    {
                        firstAttempt = DateTime.UtcNow;
                    }

                    var ex = outerEx.UnwrapIfSingleException();

                    ExceptionFilter?.Invoke(ex);

                    if (rethrowExceptions != null)
                    {
                        foreach (var rethrowEx in rethrowExceptions)
                        {
                            if (!ex.GetType().IsAssignableFrom(rethrowEx))
                            {
                                continue;
                            }

                            if (ex != outerEx)
                            {
                                throw ex;
                            }

                            throw;
                        }
                    }

                    if (originalEx == null)
                    {
                        originalEx = ex;
                    }

                    var amazonEx = ex as AmazonDynamoDBException;

                    if (amazonEx?.StatusCode == HttpStatusCode.BadRequest &&
                        !retryOnErrorCodes.Contains(amazonEx.ErrorCode))
                    {
                        throw;
                    }

                    await i.SleepBackOffMultiplierAsync().ConfigureAwait(false);
                }
            } while ((DateTime.UtcNow - firstAttempt.Value) < MaxRetryOnExceptionTimeout);

            throw new TimeoutException($"Exceeded timeout of {MaxRetryOnExceptionTimeout}", originalEx);
        }

        private async Task CreateTableAsync(DynamoMetadataType table)
        {
            var request = ToCreateTableRequest(table);

            try
            {
                await ExecAsync(() => DynamoDb.CreateTableAsync(request)).ConfigureAwait(false);
            }
            catch(AmazonDynamoDBException ex)
            {
                if (ex.ErrorCode == DynamoErrors.AlreadyExists)
                {
                    return;
                }

                throw;
            }
        }

        public async Task CreateMissingTablesAsync(IEnumerable<DynamoMetadataType> tables, CancellationToken token = default)
        {
            var tablesList = tables.Safe().ToList();

            if (tablesList.Count == 0)
            {
                return;
            }

            var existingTableNames = GetTableNames().ToList();

            foreach (var table in tablesList)
            {
                if (existingTableNames.Contains(table.Name))
                {
                    continue;
                }

                if (Log.IsDebugEnabled)
                {
                    Log.Debug("Creating Table: " + table.Name);
                }

                await CreateTableAsync(table).ConfigureAwait(false);
            }

            await WaitForTablesToBeReadyAsync(tablesList.Map(x => x.Name), token).ConfigureAwait(false);
        }

        public async Task WaitForTablesToBeReadyAsync(IEnumerable<string> tableNames, CancellationToken token = default)
        {
            var pendingTables = new List<string>(tableNames);

            if (pendingTables.Count == 0)
            {
                return;
            }

            do
            {
                try
                {
                    var responses = await Task.WhenAll(pendingTables.Map(x =>
                                                                             ExecAsync(() => DynamoDb.DescribeTableAsync(x, token))).ToArray());

                    foreach (var response in responses)
                    {
                        if (response.Table.TableStatus == DynamoStatus.Active)
                        {
                            pendingTables.Remove(response.Table.TableName);
                        }
                    }

                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug($"Tables Pending: {pendingTables.ToJsv()}");
                    }

                    if (pendingTables.Count == 0)
                    {
                        return;
                    }

                    if (token.IsCancellationRequested)
                    {
                        return;
                    }

                    Thread.Sleep(PollTableStatus);
                }
                catch(ResourceNotFoundException)
                {
                    // DescribeTable is eventually consistent. So you might
                    // get resource not found. So we handle the potential exception.
                }
            } while (true);
        }

        public Task InitSchemaAsync()
            => CreateMissingTablesAsync(DynamoMetadata.GetTables());

        public Task<T> GetItemAsync<T>(DynamoId id, bool? consistentRead = null)
            => id.Range != null
                   ? GetItemAsync<T>(id.Hash, id.Range, consistentRead)
                   : GetItemAsync<T>(id.Hash, consistentRead);

        public async Task<T> GetItemAsync<T>(object hash, bool? consistentRead = null)
        {
            var table = DynamoMetadata.GetTable<T>();

            var request = new GetItemRequest
                          {
                              TableName = table.Name,
                              Key = Converters.ToAttributeKeyValue(this, table.HashKey, hash),
                              ConsistentRead = consistentRead ?? ConsistentRead
                          };

            var result = await ConvertGetItemResponseAsync<T>(request, table).ConfigureAwait(false);

            return result;
        }

        public async Task<T> GetItemAsync<T>(object hash, object range, bool? consistentRead = null)
        {
            var table = DynamoMetadata.GetTable<T>();

            var request = new GetItemRequest
                          {
                              TableName = table.Name,
                              Key = Converters.ToAttributeKeyValue(this, table, hash, range),
                              ConsistentRead = consistentRead ?? ConsistentRead
                          };

            var result = await ConvertGetItemResponseAsync<T>(request, table).ConfigureAwait(false);

            return result;
        }

        public async IAsyncEnumerable<T> GetItemsAsync<T>(IEnumerable<object> hashes, bool? consistentRead = null)
        {
            var table = DynamoMetadata.GetTable<T>();

            foreach (var hashesBatch in ToLazyBatchesOf(hashes, MaxReadBatchSize))
            {
                var getItems = new KeysAndAttributes
                               {
                                   ConsistentRead = consistentRead ?? ConsistentRead
                               };

                hashesBatch.Each(id => getItems.Keys.Add(Converters.ToAttributeKeyValue(this, table.HashKey, id)));

                await foreach (var result in ConvertBatchGetItemResponseAsync<T>(table, getItems).ConfigureAwait(false))
                {
                    yield return result;
                }
            }
        }

        public async IAsyncEnumerable<T> GetItemsAsync<T>(IEnumerable<DynamoId> ids, bool? consistentRead = null)
        {
            var table = DynamoMetadata.GetTable<T>();

            foreach (var hashesBatch in ToLazyBatchesOf(ids, MaxReadBatchSize))
            {
                var getItems = new KeysAndAttributes
                               {
                                   ConsistentRead = consistentRead ?? ConsistentRead
                               };

                hashesBatch.Each(id => getItems.Keys.Add(Converters.ToAttributeKeyValue(this, table, id)));

                await foreach (var result in ConvertBatchGetItemResponseAsync<T>(table, getItems).ConfigureAwait(false))
                {
                    yield return result;
                }
            }
        }

        public async Task<T> PutItemAsync<T>(T value, bool returnOld = false)
        {
            var table = DynamoMetadata.GetTable<T>();

            var request = new PutItemRequest
                          {
                              TableName = table.Name,
                              Item = Converters.ToAttributeValues(this, value, table),
                              ReturnValues = returnOld
                                                 ? ReturnValue.ALL_OLD
                                                 : ReturnValue.NONE
                          };

            var response = await ExecAsync(() => DynamoDb.PutItemAsync(request)).ConfigureAwait(false);

            if (response.Attributes.IsEmpty())
            {
                return default;
            }

            return Converters.FromAttributeValues<T>(table, response.Attributes);
        }

        public async Task<bool> UpdateItemAsync<T>(UpdateExpression<T> update)
        {
            try
            {
                await ExecAsync(() => DynamoDb.UpdateItemAsync(update)).ConfigureAwait(false);

                return true;
            }
            catch(ConditionalCheckFailedException)
            {
                return false;
            }
        }

        public async Task UpdateItemAsync<T>(DynamoUpdateItem update)
        {
            var table = DynamoMetadata.GetTable<T>();

            var request = new UpdateItemRequest
                          {
                              TableName = table.Name,
                              Key = Converters.ToAttributeKeyValue(this, table, update.Hash, update.Range),
                              AttributeUpdates = new Dictionary<string, AttributeValueUpdate>(),
                              ReturnValues = ReturnValue.NONE
                          };

            if (update.Put != null)
            {
                foreach (var entry in update.Put)
                {
                    var field = table.GetField(entry.Key);

                    if (field == null)
                    {
                        continue;
                    }

                    request.AttributeUpdates[field.Name] = new AttributeValueUpdate(Converters.ToAttributeValue(this, field.Type, field.DbType, entry.Value), DynamoAttributeAction.Put);
                }
            }

            if (update.Add != null)
            {
                foreach (var entry in update.Add)
                {
                    var field = table.GetField(entry.Key);

                    if (field == null)
                    {
                        continue;
                    }

                    request.AttributeUpdates[field.Name] = new AttributeValueUpdate(Converters.ToAttributeValue(this, field.Type, field.DbType, entry.Value), DynamoAttributeAction.Add);
                }
            }

            if (update.Delete != null)
            {
                foreach (var key in update.Delete)
                {
                    var field = table.GetField(key);

                    if (field == null)
                    {
                        continue;
                    }

                    request.AttributeUpdates[field.Name] = new AttributeValueUpdate(null, DynamoAttributeAction.Delete);
                }
            }

            await ExecAsync(() => DynamoDb.UpdateItemAsync(request)).ConfigureAwait(false);
        }

        public async Task<T> UpdateItemNonDefaultsAsync<T>(T value, bool returnOld = false)
        {
            var table = DynamoMetadata.GetTable<T>();

            var request = new UpdateItemRequest
                          {
                              TableName = table.Name,
                              Key = Converters.ToAttributeKey(this, table, value),
                              AttributeUpdates = Converters.ToNonDefaultAttributeValueUpdates(this, value, table),
                              ReturnValues = returnOld
                                                 ? ReturnValue.ALL_OLD
                                                 : ReturnValue.NONE
                          };

            var response = await ExecAsync(() => DynamoDb.UpdateItemAsync(request)).ConfigureAwait(false);

            if (response.Attributes.IsEmpty())
            {
                return default;
            }

            return Converters.FromAttributeValues<T>(table, response.Attributes);
        }

        public async Task PutItemsAsync<T>(IEnumerable<T> items)
        {
            var table = DynamoMetadata.GetTable<T>();

            foreach (var itemsBatch in ToLazyBatchesOf(items, MaxWriteBatchSize))
            {
                var putItems = itemsBatch.Map(x => new WriteRequest(new PutRequest(Converters.ToAttributeValues(this, x, table))));

                await ExecBatchWriteItemResponseAsync<T>(table, putItems).ConfigureAwait(false);
            }
        }

        public async Task<T> DeleteItemAsync<T>(object hash, ReturnItem returnItem = ReturnItem.None)
        {
            var table = DynamoMetadata.GetTable<T>();

            var request = new DeleteItemRequest
                          {
                              TableName = table.Name,
                              Key = Converters.ToAttributeKeyValue(this, table.HashKey, hash),
                              ReturnValues = returnItem.ToReturnValue()
                          };

            var response = await ExecAsync(() => DynamoDb.DeleteItemAsync(request)).ConfigureAwait(false);

            if (response.Attributes.IsEmpty())
            {
                return default;
            }

            return Converters.FromAttributeValues<T>(table, response.Attributes);
        }

        public Task<T> DeleteItemAsync<T>(DynamoId id, ReturnItem returnItem = ReturnItem.None)
            => id.Range != null
                   ? DeleteItemAsync<T>(id.Hash, id.Range, returnItem)
                   : DeleteItemAsync<T>(id.Hash, returnItem);

        public async Task<T> DeleteItemAsync<T>(object hash, object range, ReturnItem returnItem = ReturnItem.None)
        {
            var table = DynamoMetadata.GetTable<T>();

            var request = new DeleteItemRequest
                          {
                              TableName = table.Name,
                              Key = Converters.ToAttributeKeyValue(this, table, hash, range),
                              ReturnValues = returnItem.ToReturnValue()
                          };

            var response = await ExecAsync(() => DynamoDb.DeleteItemAsync(request)).ConfigureAwait(false);

            if (response.Attributes.IsEmpty())
            {
                return default;
            }

            return Converters.FromAttributeValues<T>(table, response.Attributes);
        }

        public async Task DeleteItemsAsync<T>(IEnumerable<object> hashes)
        {
            var table = DynamoMetadata.GetTable<T>();

            foreach (var hashBatch in ToLazyBatchesOf(hashes, MaxWriteBatchSize))
            {
                var deleteItems = hashBatch.Map(id => new WriteRequest(new DeleteRequest(Converters.ToAttributeKeyValue(this, table.HashKey, id))));

                await ExecBatchWriteItemResponseAsync<T>(table, deleteItems).ConfigureAwait(false);
            }
        }

        public async Task DeleteItemsAsync<T>(IEnumerable<DynamoId> ids)
        {
            var table = DynamoMetadata.GetTable<T>();

            foreach (var idBatch in ToLazyBatchesOf(ids, MaxWriteBatchSize))
            {
                var deleteItems = idBatch.Map(id => new WriteRequest(new DeleteRequest(Converters.ToAttributeKeyValue(this, table, id))));

                await ExecBatchWriteItemResponseAsync<T>(table, deleteItems).ConfigureAwait(false);
            }
        }

        public async IAsyncEnumerable<T> ScanAsync<T>(ScanRequest request, Func<ScanResponse, IEnumerable<T>> converter)
        {
            ScanResponse response = null;

            do
            {
                if (response != null)
                {
                    request.ExclusiveStartKey = response.LastEvaluatedKey;
                }

                response = await ExecAsync(() => DynamoDb.ScanAsync(request)).ConfigureAwait(false);

                var results = converter(response);

                foreach (var result in results)
                {
                    yield return result;
                }
            } while (!response.LastEvaluatedKey.IsEmpty());
        }

        public async IAsyncEnumerable<T> ScanAsync<T>(ScanExpression<T> request, int limit)
        {
            if (request.Limit == default)
            {
                request.Limit = limit;
            }

            ScanResponse response = null;
            var count = 0;

            do
            {
                if (response != null)
                {
                    request.ExclusiveStartKey = response.LastEvaluatedKey;
                }

                response = await ExecAsync(() => DynamoDb.ScanAsync(request)).ConfigureAwait(false);

                var results = response.ConvertAll<T>();

                foreach (var result in results)
                {
                    yield return result;

                    count++;

                    if (count >= limit)
                    {
                        break;
                    }
                }
            } while (!response.LastEvaluatedKey.IsEmpty() && count < limit);
        }

        public IAsyncEnumerable<T> ScanAsync<T>(ScanExpression<T> request)
            => ScanAsync(request, r => r.ConvertAll<T>());

        public async IAsyncEnumerable<T> ScanAsync<T>(ScanRequest request, int limit)
        {
            if (request.Limit == default)
            {
                request.Limit = limit;
            }

            ScanResponse response = null;
            var count = 0;

            do
            {
                if (response != null)
                {
                    request.ExclusiveStartKey = response.LastEvaluatedKey;
                }

                response = await ExecAsync(() => DynamoDb.ScanAsync(request)).ConfigureAwait(false);

                var results = response.ConvertAll<T>();

                foreach (var result in results)
                {
                    yield return result;

                    count++;

                    if (count >= limit)
                    {
                        break;
                    }
                }
            } while (!response.LastEvaluatedKey.IsEmpty() && count < limit);
        }

        public IAsyncEnumerable<T> ScanAsync<T>(ScanRequest request)
            => ScanAsync(request, r => r.ConvertAll<T>());

        public IAsyncEnumerable<T> QueryAsync<T>(QueryExpression<T> request)
            => QueryAsync(request, r => r.ConvertAll<T>());

        public IAsyncEnumerable<T> QueryAsync<T>(QueryExpression<T> request, int limit)
            => QueryAsync<T>((QueryRequest)request, limit);

        public IAsyncEnumerable<T> QueryAsync<T>(QueryRequest request)
            => QueryAsync(request, r => r.ConvertAll<T>());

        public async IAsyncEnumerable<T> QueryAsync<T>(QueryRequest request, int limit)
        {
            if (request.Limit == default)
            {
                request.Limit = limit;
            }

            QueryResponse response = null;
            var count = 0;

            do
            {
                if (response != null)
                {
                    request.ExclusiveStartKey = response.LastEvaluatedKey;
                }

                response = await ExecAsync(() => DynamoDb.QueryAsync(request)).ConfigureAwait(false);

                var results = response.ConvertAll<T>();

                foreach (var result in results)
                {
                    yield return result;

                    count++;

                    if (count >= limit)
                    {
                        break;
                    }
                }
            } while (!response.LastEvaluatedKey.IsEmpty() && count < limit);
        }

        public async IAsyncEnumerable<T> QueryAsync<T>(QueryRequest request, Func<QueryResponse, IEnumerable<T>> converter)
        {
            QueryResponse response = null;

            do
            {
                if (response != null)
                {
                    request.ExclusiveStartKey = response.LastEvaluatedKey;
                }

                response = await ExecAsync(() => DynamoDb.QueryAsync(request)).ConfigureAwait(false);

                var results = converter(response);

                foreach (var result in results)
                {
                    yield return result;
                }
            } while (!response.LastEvaluatedKey.IsEmpty());
        }
    }
}
