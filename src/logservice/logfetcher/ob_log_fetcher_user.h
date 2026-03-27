/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_FETCHER_USER_H_
#define OCEANBASE_LOG_FETCHER_USER_H_

namespace oceanbase
{
namespace logfetcher
{
enum LogFetcherUser
{
  UNKNOWN,
  STANDBY,  // Physical standby
  CDC,      // OBCDC
};

static bool is_standby(const LogFetcherUser &user) { return LogFetcherUser::STANDBY == user; }
static bool is_cdc(const LogFetcherUser &user) { return LogFetcherUser::CDC == user; }

} // namespace logfetcher
} // namespace oceanbase

#endif
