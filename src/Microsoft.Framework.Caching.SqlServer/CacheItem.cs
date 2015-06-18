// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;

namespace Microsoft.Framework.Caching.SqlServer
{
    internal class CacheItem
    {
        public string Id { get; set; }

        public byte[] Value { get; set; }

        public DateTime ExpirationTimeUTC { get; set; }

        public DateTime? AbsoluteExpirationUTC { get; set; }

        public TimeSpan? SlidingExpiration { get; set; }
    }
}