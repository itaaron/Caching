// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Threading.Tasks;
using Microsoft.Framework.Caching.Distributed;

namespace Microsoft.Framework.Caching.SqlServer
{
    internal interface ISqlOperations
    {
        void GetTableSchema();

        Task GetTableSchemaAsync();

        CacheItem GetCacheItem(string key);

        Task<CacheItem> GetCacheItemAsync(string key);

        void UpdateCacheItemExpiration(string key, DateTime newExpirationTimeUTC);

        Task UpdateCacheItemExpirationAsync(string key, DateTime newExpirationTimeUTC);

        void DeleteCacheItem(string key);

        Task DeleteCacheItemAsync(string key);

        void SetCacheItem(string key, byte[] value, DistributedCacheEntryOptions options);

        Task SetCacheItemAsync(string key, byte[] value, DistributedCacheEntryOptions options);

        void DeleteExpiredCacheItems();
    }
}