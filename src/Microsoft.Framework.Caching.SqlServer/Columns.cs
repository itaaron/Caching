// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

namespace Microsoft.Framework.Caching.SqlServer
{
    public static class Columns
    {
        public static class Names
        {
            public const string CacheItemId = "Id";
            public const string CacheItemValue = "Value";
            public const string ExpiresAtTimeUTC = "ExpiresAtTimeUTC";
            public const string SlidingExpirationInTicks = "SlidingExpirationInTicks";
            public const string AbsoluteExpirationUTC = "AbsoluteExpirationUTC";
        }

        public static class Indexes
        {
            // The value of the following index positions is dependent on how the SQL queries
            // are selecting the columns.
            public const int CacheItemIdIndex = 0;
            public const int CacheItemValueIndex = 1;
            public const int ExpiresAtTimeUTCIndex = 2;
            public const int SlidingExpirationInTicksIndex = 3;
            public const int AbsoluteExpirationUTCIndex = 4;
        }
    }
}
