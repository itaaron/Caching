// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;

namespace Microsoft.Framework.Caching.SqlServer
{
    internal class CacheItemExpirationInfo
    {
        // This should never be null. This value is calculated based on Sliding or Absolute expiration
        public DateTime ExpiresAtTimeUTC { get; set; }

        public TimeSpan? SlidingExpiration { get; set; }

        public DateTime? AbsoluteExpirationUTC { get; set; }
    }
}
