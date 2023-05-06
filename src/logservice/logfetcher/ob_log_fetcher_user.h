/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
