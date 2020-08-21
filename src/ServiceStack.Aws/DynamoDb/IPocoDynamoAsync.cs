// Copyright (c) ServiceStack, Inc. All Rights Reserved.
// License: https://raw.github.com/ServiceStack/ServiceStack/master/license.txt

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;

namespace ServiceStack.Aws.DynamoDb
{
    /// <summary>
    /// Available API's with Async equivalents
    /// </summary>
    public interface IPocoDynamoAsync
    {
        Task CreateMissingTablesAsync(IEnumerable<DynamoMetadataType> tables, 
            CancellationToken token = default(CancellationToken));

        Task WaitForTablesToBeReadyAsync(IEnumerable<string> tableNames, 
            CancellationToken token = default(CancellationToken));

        Task InitSchemaAsync();

        Task<T> GetItemAsync<T>(object hash, bool? consistentRead = null);
        Task<T> GetItemAsync<T>(DynamoId id, bool? consistentRead = null);
        Task<T> GetItemAsync<T>(object hash, object range, bool? consistentRead = null);
        IAsyncEnumerable<T> GetItemsAsync<T>(IEnumerable<object> hashes, bool? consistentRead = null);
        IAsyncEnumerable<T> GetItemsAsync<T>(IEnumerable<DynamoId> ids, bool? consistentRead = null);

        Task<T> PutItemAsync<T>(T value, bool returnOld = false);
        Task PutItemsAsync<T>(IEnumerable<T> items);
        Task PutItemsAsync<T>(IAsyncEnumerable<T> items);

        Task<bool> UpdateItemAsync<T>(UpdateExpression<T> update);
        Task UpdateItemAsync<T>(DynamoUpdateItem update);

        Task<T> DeleteItemAsync<T>(object hash, ReturnItem returnItem = ReturnItem.None);
        Task<T> DeleteItemAsync<T>(DynamoId id, ReturnItem returnItem = ReturnItem.None);
        Task<T> DeleteItemAsync<T>(object hash, object range, ReturnItem returnItem = ReturnItem.None);

        Task DeleteItemsAsync<T>(IEnumerable<object> hashes);
        Task DeleteItemsAsync<T>(IEnumerable<DynamoId> ids);
        Task DeleteItemsAsync<T>(IAsyncEnumerable<object> hashes);
        Task DeleteItemsAsync<T>(IAsyncEnumerable<DynamoId> ids);

        IAsyncEnumerable<T> ScanAsync<T>(ScanRequest request, Func<ScanResponse, IEnumerable<T>> converter);
        IAsyncEnumerable<T> ScanAsync<T>(ScanExpression<T> request, int limit);
        IAsyncEnumerable<T> ScanAsync<T>(ScanExpression<T> request);
        IAsyncEnumerable<T> ScanAsync<T>(ScanRequest request, int limit);
        IAsyncEnumerable<T> ScanAsync<T>(ScanRequest request);

        IAsyncEnumerable<T> QueryAsync<T>(QueryExpression<T> request, int limit);
        IAsyncEnumerable<T> QueryAsync<T>(QueryRequest request, int limit);
        IAsyncEnumerable<T> QueryAsync<T>(QueryRequest request);
        IAsyncEnumerable<T> QueryAsync<T>(QueryRequest request, Func<QueryResponse, IEnumerable<T>> converter);

        Task<long> IncrementAsync<T>(object hash, string fieldName, long amount = 1);
    }
}