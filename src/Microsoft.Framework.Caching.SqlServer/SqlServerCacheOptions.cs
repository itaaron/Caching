// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using Microsoft.Framework.Internal;

namespace Microsoft.Framework.Caching.SqlServer
{
    public class SqlServerCacheOptions
    {
        private readonly TimeSpan MinimumExpiredItemsDeletionInterval;
        private TimeSpan _expiredItemsDeletionInterval;

        public SqlServerCacheOptions()
        {
            MinimumExpiredItemsDeletionInterval = TimeSpan.FromHours(1);
            _expiredItemsDeletionInterval = MinimumExpiredItemsDeletionInterval;
        }

        private ISystemClock _systemClock = new SystemClock();
        public ISystemClock SystemClock
        {
            get
            {
                return _systemClock;
            }
            [param: NotNull]
            set
            {
                _systemClock = value;
            }
        }

        public TimeSpan ExpiredItemsDeletionInterval
        {
            get
            {
                return _expiredItemsDeletionInterval;
            }
            set
            {
                if (value < MinimumExpiredItemsDeletionInterval)
                {
                    throw new ArgumentException(
                        $"{ExpiredItemsDeletionInterval} cannot be less the minimum value of " +
                        $"{MinimumExpiredItemsDeletionInterval.TotalMinutes} minutes.");
                }

                _expiredItemsDeletionInterval = value;
            }
        }

        private string _connectionString;
        public string ConnectionString
        {
            get
            {
                return _connectionString;
            }
            set
            {
                if (string.IsNullOrEmpty(value))
                {
                    throw new ArgumentException($"{nameof(ConnectionString)} cannot be empty or null.");
                }

                _connectionString = value;
            }
        }

        private string _schemaName;
        public string SchemaName
        {
            get
            {
                return _schemaName;
            }
            set
            {
                if (string.IsNullOrEmpty(value))
                {
                    throw new ArgumentException($"{nameof(SchemaName)} cannot be empty or null.");
                }

                _schemaName = value;
            }
        }

        private string _tableName;
        public string TableName
        {
            get
            {
                return _tableName;
            }
            set
            {
                if (string.IsNullOrEmpty(value))
                {
                    throw new ArgumentException($"{nameof(TableName)} cannot be empty or null.");
                }

                _tableName = value;
            }
        }
    }
}