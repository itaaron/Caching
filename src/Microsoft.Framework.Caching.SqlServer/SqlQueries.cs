// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

namespace Microsoft.Framework.Caching.SqlServer
{
    internal class SqlQueries
    {
        private const string CreateTableFormat = "CREATE TABLE {0}(" +
            "Id nvarchar(100)  NOT NULL PRIMARY KEY, " +
            "Value varbinary(MAX) NOT NULL, " +
            "ExpiresAtTimeUTC datetime NOT NULL, " +
            "SlidingExpirationInTicks bigint NULL," +
            "AbsoluteExpirationUTC datetime NULL)";

        private const string CreateNonClusteredIndexOnExpirationTimeFormat
            = "CREATE NONCLUSTERED INDEX Index_ExpiresAtTimeUTC ON {0}(ExpiresAtTimeUTC)";

        private const string TableInfoFormat =
            "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE " +
            "FROM INFORMATION_SCHEMA.TABLES " +
            "WHERE TABLE_SCHEMA = '{0}' " +
            "AND TABLE_NAME = '{1}'";

        private const string GetCacheItemFormat =
            "SELECT Id, Value, ExpiresAtTimeUTC, SlidingExpirationInTicks, AbsoluteExpirationUTC " +
            "FROM {0} WHERE Id = @Id AND @UtcNow <= ExpiresAtTimeUTC";

        private const string SetCacheItemFormat =
            "BEGIN " +
                "IF NOT EXISTS(SELECT Id FROM {0} WHERE Id = @Id) " +
                "BEGIN " +
                    "INSERT INTO {0} " +
                        "(Id, Value, ExpiresAtTimeUTC, SlidingExpirationInTicks, AbsoluteExpirationUTC) " +
                        "VALUES (@Id, @Value, @ExpiresAtTimeUTC, @SlidingExpirationInTicks, @AbsoluteExpirationUTC) " +
                "END " +
                "ELSE " +
                "BEGIN " +
                    "UPDATE {0} SET Value = @Value, ExpiresAtTimeUTC = @ExpiresAtTimeUTC " +
                    "WHERE Id = @Id " +
                "END " +
            "END";

        private const string DeleteCacheItemFormat = "DELETE FROM {0} WHERE Id = @Id";

        private const string UpdateCacheItemExpirationFormat = "UPDATE {0} SET ExpiresAtTimeUTC = @ExpiresAtTimeUTC " +
            "WHERE Id = @Id";

        public const string DeleteExpiredCacheItemsFormat = "DELETE FROM {0} WHERE @UtcNow > ExpiresAtTimeUTC";

        public SqlQueries(string schemaName, string tableName)
        {
            //TODO: sanitize schema and table name

            var tableNameWithSchema = string.Format("[{0}].[{1}]", schemaName, tableName);
            CreateTable = string.Format(CreateTableFormat, tableNameWithSchema);
            CreateNonClusteredIndexOnExpirationTime = string.Format(
                CreateNonClusteredIndexOnExpirationTimeFormat,
                tableNameWithSchema);
            TableInfo = string.Format(TableInfoFormat, schemaName, tableName);
            GetCacheItem = string.Format(GetCacheItemFormat, tableNameWithSchema);
            DeleteCacheItem = string.Format(DeleteCacheItemFormat, tableNameWithSchema);
            UpdateCacheItemExpiration = string.Format(UpdateCacheItemExpirationFormat, tableNameWithSchema);
            DeleteExpiredCacheItems = string.Format(DeleteExpiredCacheItemsFormat, tableNameWithSchema);
            SetCacheItem = string.Format(SetCacheItemFormat, tableNameWithSchema);
        }

        public virtual string CreateTable { get; }

        public virtual string CreateNonClusteredIndexOnExpirationTime { get; }

        public virtual string GetTableSchema { get; }

        public virtual string TableInfo { get; }

        public virtual string GetCacheItem { get; }

        public virtual string SetCacheItem { get; }

        public virtual string DeleteCacheItem { get; }

        public virtual string UpdateCacheItemExpiration { get; }

        public virtual string DeleteExpiredCacheItems { get; }
    }
}
