// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.Framework.Logging;

namespace Microsoft.Framework.Caching.SqlServer
{
    internal class SqlOperationsForMono : SqlOperations
    {
        private readonly ILogger _logger;
        private readonly SqlServerCacheOptions _options;

        public SqlOperationsForMono(SqlServerCacheOptions options, ILoggerFactory loggerFactory)
            : base(options, loggerFactory)
        {
            _options = options;
            _logger = loggerFactory.CreateLogger<SqlOperationsForMono>();
        }

        public override CacheItem GetCacheItem(string key)
        {
            var utcNowDateTime = _options.SystemClock.UtcNow.UtcDateTime;

            CacheItem item = null;
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.GetCacheItem, connection);
                command.Parameters
                    .AddCacheItemId(key)
                    .AddWithValue("UtcNow", SqlDbType.DateTime, utcNowDateTime);

                connection.Open();

                var reader = command.ExecuteReader(CommandBehavior.SingleRow);

                if (reader.Read())
                {
                    var id = reader.GetString(Columns.Indexes.CacheItemIdIndex);
                    var value = (byte[])reader[Columns.Indexes.CacheItemValueIndex];
                    var expirationTime = DateTime.Parse(reader[Columns.Indexes.ExpiresAtTimeUTCIndex].ToString());

                    TimeSpan? slidingExpiration = null;
                    if (!reader.IsDBNull(Columns.Indexes.SlidingExpirationInTicksIndex))
                    {
                        slidingExpiration = TimeSpan.FromTicks(
                            reader.GetInt64(Columns.Indexes.SlidingExpirationInTicksIndex));
                    }

                    DateTime? absoluteExpiration = null;
                    if (!reader.IsDBNull(Columns.Indexes.AbsoluteExpirationUTCIndex))
                    {
                        absoluteExpiration = DateTime.Parse(
                            reader[Columns.Indexes.AbsoluteExpirationUTCIndex].ToString());
                    }

                    item = new CacheItem()
                    {
                        Id = id,
                        Value = value,
                        ExpirationTimeUTC = expirationTime,
                        SlidingExpiration = slidingExpiration,
                        AbsoluteExpirationUTC = absoluteExpiration
                    };
                }
                else
                {
                    return null;
                }

                // Check if the current item has a sliding expiration and is going to expire within the
                // next timeout period(ex:20 minutes), then extend the expiration time.
                if (item.SlidingExpiration.HasValue &&
                    utcNowDateTime >= (item.ExpirationTimeUTC - item.SlidingExpiration.Value))
                {
                    var newExpirationTime = CacheItemExpiration.GetExpirationTimeUTC(
                        item.SlidingExpiration,
                        item.AbsoluteExpirationUTC,
                        utcNowDateTime);

                    if (item.ExpirationTimeUTC != newExpirationTime)
                    {
                        UpdateCacheItemExpiration(key, newExpirationTime);
                    }
                }
            }

            return item;
        }

        public override async Task<CacheItem> GetCacheItemAsync(string key)
        {
            var utcNowDateTime = _options.SystemClock.UtcNow.UtcDateTime;

            CacheItem item = null;
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.GetCacheItem, connection);
                command.Parameters
                    .AddCacheItemId(key)
                    .AddWithValue("UtcNow", SqlDbType.DateTime, utcNowDateTime);

                await connection.OpenAsync();

                var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow);

                if (await reader.ReadAsync())
                {
                    var id = reader.GetString(Columns.Indexes.CacheItemIdIndex);
                    var value = (byte[])reader[Columns.Indexes.CacheItemValueIndex];
                    var expirationTime = DateTime.Parse(reader[Columns.Indexes.ExpiresAtTimeUTCIndex].ToString());

                    TimeSpan? slidingExpiration = null;
                    if (!await reader.IsDBNullAsync(Columns.Indexes.SlidingExpirationInTicksIndex))
                    {
                        slidingExpiration = TimeSpan.FromTicks(
                            Convert.ToInt64(reader[Columns.Indexes.SlidingExpirationInTicksIndex].ToString()));
                    }

                    DateTime? absoluteExpiration = null;
                    if (!await reader.IsDBNullAsync(Columns.Indexes.AbsoluteExpirationUTCIndex))
                    {
                        absoluteExpiration = DateTime.Parse(
                            reader[Columns.Indexes.AbsoluteExpirationUTCIndex].ToString());
                    }

                    item = new CacheItem()
                    {
                        Id = id,
                        Value = value,
                        ExpirationTimeUTC = expirationTime,
                        SlidingExpiration = slidingExpiration,
                        AbsoluteExpirationUTC = absoluteExpiration
                    };
                }
                else
                {
                    return null;
                }

                // Check if the current item has a sliding expiration and is going to expire within the
                // next timeout period(ex:20 minutes), then extend the expiration time.
                if (item.SlidingExpiration.HasValue &&
                    utcNowDateTime >= (item.ExpirationTimeUTC - item.SlidingExpiration.Value))
                {
                    var newExpirationTime = CacheItemExpiration.GetExpirationTimeUTC(
                        item.SlidingExpiration,
                        item.AbsoluteExpirationUTC,
                        utcNowDateTime);

                    if (item.ExpirationTimeUTC != newExpirationTime)
                    {
                        await UpdateCacheItemExpirationAsync(key, newExpirationTime);
                    }
                }
            }

            return item;
        }
    }
}