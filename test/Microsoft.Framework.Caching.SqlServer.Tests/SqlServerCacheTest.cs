// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Framework.Caching.Distributed;
using Microsoft.Framework.Configuration;
using Microsoft.Framework.Internal;
using Microsoft.Framework.Logging;
using Microsoft.Framework.OptionsModel;
using Xunit;

namespace Microsoft.Framework.Caching.SqlServer
{
    // This requires SQL Server database to be setup
    // public
    public class SqlServerCacheTest
    {
        private const string ConnectionStringKey = "ConnectionString";
        private const string SchemaNameKey = "SchemaName";
        private const string TableNameKey = "TableName";

        private readonly string _tableName;
        private readonly string _schemaName;
        private readonly string _connectionString;

        public SqlServerCacheTest()
        {
            // TODO: Figure how to use config.json which requires resolving IApplicationEnvironment which currently
            // fails.
            var memoryConfigurationSource = new MemoryConfigurationSource();
            memoryConfigurationSource.Add(
                ConnectionStringKey,
                "Server=localhost;Database=CacheTestDb;Trusted_Connection=True;");
            memoryConfigurationSource.Add(SchemaNameKey, "dbo");
            memoryConfigurationSource.Add(TableNameKey, "CacheTest");

            var configurationBuilder = new ConfigurationBuilder();
            configurationBuilder
                .Add(memoryConfigurationSource)
                .AddEnvironmentVariables();

            var configuration = configurationBuilder.Build();
            _tableName = configuration.Get(TableNameKey);
            _schemaName = configuration.Get(SchemaNameKey);
            _connectionString = configuration.Get(ConnectionStringKey);
        }

        [Fact]
        public async Task ReturnsNullValue_ForNonExistingCacheItem()
        {
            // Arrange
            var sqlServerCache = await GetCacheAndConnectAsync();

            // Act
            var value = await sqlServerCache.GetAsync("NonExisting");

            // Assert
            Assert.Null(value);
        }

        [Fact]
        public async Task SetWithAbsoluteExpirationSetInThePast_Throws()
        {
            // Arrange
            var testClock = new TestClock();
            var key = Guid.NewGuid().ToString();
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");
            var sqlServerCache = await GetCacheAndConnectAsync(testClock);

            // Act & Assert

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            {
                return sqlServerCache.SetAsync(
                    key,
                    expectedValue,
                    new DistributedCacheEntryOptions().SetAbsoluteExpiration(testClock.UtcNow.AddHours(-1)));
            });
            Assert.Equal("The absolute expiration value must be in the future.", exception.Message);
        }

        [Fact]
        public async Task SetWithSlidingExpiration_ReturnsNullValue_ForExpiredCacheItem()
        {
            // Arrange
            var testClock = new TestClock();
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = await GetCacheAndConnectAsync(testClock);
            await sqlServerCache.SetAsync(
                key,
                Encoding.UTF8.GetBytes("Hello, World!"),
                new DistributedCacheEntryOptions().SetSlidingExpiration(TimeSpan.FromSeconds(10)));

            // set the clock's UtcNow far in future
            testClock.Add(TimeSpan.FromHours(10));

            // Act
            var value = await sqlServerCache.GetAsync(key);

            // Assert
            Assert.Null(value);
        }

        [Fact]
        public async Task SetWithAbsoluteExpirationRelativeToNow_ReturnsNullValue_ForExpiredCacheItem()
        {
            // Arrange
            var testClock = new TestClock();
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = await GetCacheAndConnectAsync(testClock);
            await sqlServerCache.SetAsync(
                key,
                Encoding.UTF8.GetBytes("Hello, World!"),
                new DistributedCacheEntryOptions().SetAbsoluteExpiration(relative: TimeSpan.FromSeconds(10)));

            // set the clock's UtcNow far in future
            testClock.Add(TimeSpan.FromHours(10));

            // Act
            var value = await sqlServerCache.GetAsync(key);

            // Assert
            Assert.Null(value);
        }

        [Fact]
        public async Task SetWithAbsoluteExpiration_ReturnsNullValue_ForExpiredCacheItem()
        {
            // Arrange
            var testClock = new TestClock();
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = await GetCacheAndConnectAsync(testClock);
            await sqlServerCache.SetAsync(
                key,
                Encoding.UTF8.GetBytes("Hello, World!"),
                new DistributedCacheEntryOptions()
                .SetAbsoluteExpiration(absolute: testClock.UtcNow.Add(TimeSpan.FromSeconds(30))));

            // set the clock's UtcNow far in future
            testClock.Add(TimeSpan.FromHours(10));

            // Act
            var value = await sqlServerCache.GetAsync(key);

            // Assert
            Assert.Null(value);
        }

        [Fact]
        public async Task ThrowsException_OnNoSlidingOrAbsoluteExpirationOptions()
        {
            // Arrange
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = await GetCacheAndConnectAsync();
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");

            // Act & Assert
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            {
                return sqlServerCache.SetAsync(
                    key,
                    expectedValue,
                    new DistributedCacheEntryOptions());
            });
            Assert.Equal("Either absolute or sliding expiration needs to be provided.", exception.Message);
        }

        [Fact]
        public async Task DoesNotThrowException_WhenOnlyAbsoluteExpirationSupplied_AbsoluteExpirationRelativeToNow()
        {
            // Arrange
            var testClock = new TestClock();
            var absoluteExpirationRelativeToUtcNow = TimeSpan.FromSeconds(10);
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = await GetCacheAndConnectAsync();
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");

            // Act
            await sqlServerCache.SetAsync(
                key,
                expectedValue,
                new DistributedCacheEntryOptions()
                .SetAbsoluteExpiration(relative: absoluteExpirationRelativeToUtcNow));

            // Assert
            await AssertGetCacheItemFromDatabaseAsync(
                sqlServerCache,
                key,
                expectedValue,
                testClock.Add(absoluteExpirationRelativeToUtcNow).UtcDateTime);
        }

        [Fact]
        public async Task DoesNotThrowException_WhenOnlyAbsoluteExpirationSupplied_AbsoluteExpiration()
        {
            // Arrange
            var testClock = new TestClock();
            var absoluteExpiration = new DateTimeOffset(2025, 1, 1, 1, 0, 0, TimeSpan.Zero);
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = await GetCacheAndConnectAsync();
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");

            // Act
            await sqlServerCache.SetAsync(
                key,
                expectedValue,
                new DistributedCacheEntryOptions()
                .SetAbsoluteExpiration(absolute: absoluteExpiration));

            // Assert
            await AssertGetCacheItemFromDatabaseAsync(
                sqlServerCache,
                key,
                expectedValue,
                absoluteExpiration.UtcDateTime);
        }

        [Fact]
        public async Task SetCacheItem_UpdatesAbsoluteExpirationTime()
        {
            // Arrange
            var testClock = new TestClock();
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = await GetCacheAndConnectAsync();
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");
            var absoluteExpiration = testClock.UtcNow.Add(TimeSpan.FromSeconds(10));

            // Act & Assert
            // Creates a new item
            await sqlServerCache.SetAsync(
                key,
                expectedValue,
                new DistributedCacheEntryOptions().SetAbsoluteExpiration(absoluteExpiration));
            await AssertGetCacheItemFromDatabaseAsync(
                sqlServerCache, key, expectedValue, absoluteExpiration.UtcDateTime);

            // Updates an existing item with new absolute expiration time
            absoluteExpiration = testClock.UtcNow.Add(TimeSpan.FromMinutes(30));
            await sqlServerCache.SetAsync(
                key,
                expectedValue,
                new DistributedCacheEntryOptions().SetAbsoluteExpiration(absoluteExpiration));
            await AssertGetCacheItemFromDatabaseAsync(
                sqlServerCache, key, expectedValue, absoluteExpiration.UtcDateTime);
        }

        [Fact]
        public async Task ExtendsExpirationTime_ForSlidingExpiration()
        {
            // Arrange
            var testClock = new TestClock();
            var slidingExpiration = TimeSpan.FromSeconds(10);
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = await GetCacheAndConnectAsync(testClock);
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");
            await sqlServerCache.SetAsync(
                key,
                expectedValue,
                new DistributedCacheEntryOptions().SetSlidingExpiration(slidingExpiration));
            // modify the 'UtcNow' to fall in the window which
            // causes the expiration time to be extended
            var utcNow = testClock.Add(TimeSpan.FromSeconds(15)).UtcNow;

            // Act
            var value = await sqlServerCache.GetAsync(key);

            // Assert
            Assert.NotNull(value);
            Assert.Equal(expectedValue, value);

            // verify if the expiration time in database is set as expected
            await AssertGetCacheItemFromDatabaseAsync(sqlServerCache, key, expectedValue, utcNow.Add(
                    TimeSpan.FromTicks(
                        CacheItemExpiration.ExpirationTimeMultiplier * slidingExpiration.Ticks)).UtcDateTime);
        }

        [Fact]
        public async Task GetItem_SlidingExpirationDoesNot_ExceedAbsoluteExpirationIfSet()
        {
            // Arrange
            var testClock = new TestClock();
            var slidingExpiration = TimeSpan.FromMinutes(5);
            var absoluteExpiration = testClock.UtcNow.Add(TimeSpan.FromMinutes(30));
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = await GetCacheAndConnectAsync(testClock);
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");
            await sqlServerCache.SetAsync(
                key,
                expectedValue,
                // Set both sliding and absolute expiration
                new DistributedCacheEntryOptions()
                .SetSlidingExpiration(slidingExpiration)
                .SetAbsoluteExpiration(absoluteExpiration));

            // Act && Assert
            var utcNow = testClock.UtcNow;
            var cacheItemInfo = await GetCacheItemFromDatabaseAsync(key);
            Assert.NotNull(cacheItemInfo);
            Assert.Equal(utcNow.AddMinutes(10).UtcDateTime, cacheItemInfo.ExpiresAtTime);

            // trigger extension of expiration - succeeds
            utcNow = testClock.Add(TimeSpan.FromMinutes(8)).UtcNow;
            await AssertGetCacheItemFromDatabaseAsync(
                sqlServerCache, key, expectedValue, utcNow.AddMinutes(10).UtcDateTime);

            // trigger extension of expiration - succeeds
            utcNow = testClock.Add(TimeSpan.FromMinutes(8)).UtcNow;
            await AssertGetCacheItemFromDatabaseAsync(
                sqlServerCache, key, expectedValue, utcNow.AddMinutes(10).UtcDateTime);

            // trigger extension of expiration - fails
            utcNow = testClock.Add(TimeSpan.FromMinutes(8)).UtcNow;
            // The expiration extension must not exceed the absolute expiration
            await AssertGetCacheItemFromDatabaseAsync(
                sqlServerCache, key, expectedValue, absoluteExpiration.UtcDateTime);
        }

        [Fact]
        public async Task DoestNotExtendsExpirationTime_ForAbsoluteExpiration()
        {
            // Arrange
            var testClock = new TestClock();
            var absoluteExpirationRelativeToNow = TimeSpan.FromSeconds(30);
            var expectedExpiresAtTime = testClock.UtcNow.Add(absoluteExpirationRelativeToNow);
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = await GetCacheAndConnectAsync();
            var expectedValue = Encoding.UTF8.GetBytes("Hello, World!");
            await sqlServerCache.SetAsync(
                key,
                expectedValue,
                new DistributedCacheEntryOptions().SetAbsoluteExpiration(absoluteExpirationRelativeToNow));
            testClock.Add(TimeSpan.FromSeconds(25));

            // Act
            var value = await sqlServerCache.GetAsync(key);

            // Assert
            Assert.NotNull(value);
            Assert.Equal(expectedValue, value);

            // verify if the expiration time in database is set as expected
            var cacheItemInfo = await GetCacheItemFromDatabaseAsync(key);
            Assert.NotNull(cacheItemInfo);
            Assert.Equal(expectedExpiresAtTime.UtcDateTime, cacheItemInfo.ExpiresAtTime);
        }

        [Fact]
        public async Task DeletesCacheItem_OnExplicitlyCalled()
        {
            // Arrange
            var testClock = new TestClock();
            var key = Guid.NewGuid().ToString();
            var sqlServerCache = await GetCacheAndConnectAsync();
            await sqlServerCache.SetAsync(
                key,
                Encoding.UTF8.GetBytes("Hello, World!"),
                new DistributedCacheEntryOptions().SetSlidingExpiration(TimeSpan.FromSeconds(10)));

            // Act
            await sqlServerCache.RemoveAsync(key);

            // Assert
            var cacheItemInfo = await GetCacheItemFromDatabaseAsync(key);
            Assert.Null(cacheItemInfo);
        }

        private async Task<SqlServerCache> GetCacheAndConnectAsync(ISystemClock testClock = null)
        {
            var options = new SqlServerCacheOptions()
            {
                ConnectionString = _connectionString,
                SchemaName = _schemaName,
                TableName = _tableName,
                SystemClock = testClock ?? new TestClock(),
                ExpiredItemsDeletionInterval = TimeSpan.FromHours(2)
            };

            var cache = new SqlServerCache(new TestSqlServerCacheOptions(options), new LoggerFactory().AddConsole());

            await cache.ConnectAsync();

            return cache;
        }

        private async Task AssertGetCacheItemFromDatabaseAsync(
            SqlServerCache cache,
            string key,
            byte[] expectedValue,
            DateTime expectedExpirationTime)
        {
            var value = await cache.GetAsync(key);
            Assert.NotNull(value);
            Assert.Equal(expectedValue, value);
            var cacheItemInfo = await GetCacheItemFromDatabaseAsync(key);
            Assert.NotNull(cacheItemInfo);
            Assert.Equal(expectedExpirationTime, cacheItemInfo.ExpiresAtTime);
        }

        private async Task<CacheItemInfo> GetCacheItemFromDatabaseAsync(string key)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                var command = new SqlCommand(
                    $"SELECT Id, Value, ExpiresAtTimeUTC, SlidingExpirationInTicks, AbsoluteExpirationUTC " +
                    $"FROM {_tableName} WHERE Id = @Id",
                    connection);
                command.Parameters.AddWithValue("Id", key);

                await connection.OpenAsync();

                var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow);

                // NOTE: The following code is made to run on Mono as well because of which
                // we cannot use GetFieldValueAsync etc.
                if (await reader.ReadAsync())
                {
                    var cacheItemInfo = new CacheItemInfo();
                    cacheItemInfo.Id = key;
                    cacheItemInfo.Value = (byte[])reader[1];
                    cacheItemInfo.ExpiresAtTime = DateTime.Parse(reader[2].ToString());

                    if (!await reader.IsDBNullAsync(3))
                    {
                        cacheItemInfo.SlidingExpirationInTicks = reader.GetInt64(3);
                    }

                    if (!await reader.IsDBNullAsync(4))
                    {
                        cacheItemInfo.AbsoluteExpiration = DateTime.Parse(reader[4].ToString());
                    }

                    return cacheItemInfo;
                }
                else
                {
                    return null;
                }
            }
        }

        private class TestSqlServerCacheOptions : IOptions<SqlServerCacheOptions>
        {
            private readonly SqlServerCacheOptions _innerOptions;

            public TestSqlServerCacheOptions(SqlServerCacheOptions innerOptions)
            {
                _innerOptions = innerOptions;
            }

            public SqlServerCacheOptions Options
            {
                get
                {
                    return _innerOptions;
                }
            }

            public SqlServerCacheOptions GetNamedOptions(string name)
            {
                return _innerOptions;
            }
        }
    }
}
