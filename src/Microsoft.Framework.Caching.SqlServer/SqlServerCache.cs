// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Framework.Caching.Distributed;
using Microsoft.Framework.Internal;
using Microsoft.Framework.Logging;
using Microsoft.Framework.OptionsModel;

namespace Microsoft.Framework.Caching.SqlServer
{
    /// <summary>
    /// Distributed cache implementation using Microsoft SQL Server database.
    /// </summary>
    public class SqlServerCache : IDistributedCache
    {
        private readonly SqlServerCacheOptions _options;
        private readonly ILogger _logger;
        private readonly ISqlOperations _sqlOperations;
        private DateTime _lastExpirationScanUTC;

        public SqlServerCache(IOptions<SqlServerCacheOptions> options, ILoggerFactory loggerFactory)
        {
            _options = options.Options;
            _logger = loggerFactory.CreateLogger<SqlServerCache>();

            // SqlClient library on Mono doesn't have support for DateTimeOffset and also
            // it doesn't have support for apis like GetFieldValue, GetFieldValueAsync etc.
            // So we detect the platform to perform things differently for Mono vs. non-Mono platforms.
            if (PlatformHelper.IsMono)
            {
                _sqlOperations = new SqlOperationsForMono(options.Options, loggerFactory);
            }
            else
            {
                _sqlOperations = new SqlOperations(options.Options, loggerFactory);
            }
        }

        public void Connect()
        {
            // Try connecting to the database and check if its available.
            _sqlOperations.GetTableSchema();
        }

        public async Task ConnectAsync()
        {
            // Try connecting to the database and check if its available.
            await _sqlOperations.GetTableSchemaAsync();
        }

        public byte[] Get([NotNull] string key)
        {
            var cacheItem = _sqlOperations.GetCacheItem(key);

            ScanForExpiredItemsIfRequired();

            return cacheItem?.Value;
        }

        public async Task<byte[]> GetAsync([NotNull] string key)
        {
            var cacheItem = await _sqlOperations.GetCacheItemAsync(key);

            ScanForExpiredItemsIfRequired();

            return cacheItem?.Value;
        }

        public void Refresh([NotNull] string key)
        {
            Get(key);
        }

        public async Task RefreshAsync([NotNull] string key)
        {
            await GetAsync(key);
        }

        public void Remove([NotNull] string key)
        {
            _sqlOperations.DeleteCacheItem(key);

            ScanForExpiredItemsIfRequired();
        }

        public async Task RemoveAsync([NotNull] string key)
        {
            await _sqlOperations.DeleteCacheItemAsync(key);

            ScanForExpiredItemsIfRequired();
        }

        public void Set([NotNull] string key, [NotNull] byte[] value, [NotNull] DistributedCacheEntryOptions options)
        {
            _sqlOperations.SetCacheItem(key, value, options);

            ScanForExpiredItemsIfRequired();
        }

        public async Task SetAsync(
            [NotNull] string key,
            [NotNull] byte[] value,
            [NotNull] DistributedCacheEntryOptions options)
        {
            await _sqlOperations.SetCacheItemAsync(key, value, options);

            ScanForExpiredItemsIfRequired();
        }

        // Called by multiple actions to see how long it's been since we last checked for expired items.
        // If sufficient time has elapsed then a scan is initiated on a background task.
        private void ScanForExpiredItemsIfRequired()
        {
            var utcNow = _options.SystemClock.UtcNow.UtcDateTime;
            if ((utcNow - _lastExpirationScanUTC) > _options.ExpiredItemsDeletionInterval)
            {
                _lastExpirationScanUTC = utcNow;
                ThreadPool.QueueUserWorkItem(DeleteExpiredCacheItems, state: null);
            }
        }

        private void DeleteExpiredCacheItems(object state)
        {
            _sqlOperations.DeleteExpiredCacheItems();
        }
    }
}