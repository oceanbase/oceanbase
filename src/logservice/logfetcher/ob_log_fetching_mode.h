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
 *
 */
#ifndef OCEANBASE_LOG_FETCHER_FETCHING_MODE_H_
#define OCEANBASE_LOG_FETCHER_FETCHING_MODE_H_

namespace oceanbase
{
namespace logfetcher
{
enum class ClientFetchingMode
{
  FETCHING_MODE_UNKNOWN = 0,

  FETCHING_MODE_INTEGRATED,
  FETCHING_MODE_DIRECT,

  FETCHING_MODE_MAX
};

const char *print_fetching_mode(const ClientFetchingMode mode);
ClientFetchingMode get_fetching_mode(const char *fetching_mode_str);

bool is_fetching_mode_valid(const ClientFetchingMode mode);
bool is_integrated_fetching_mode(const ClientFetchingMode mode);
bool is_direct_fetching_mode(const ClientFetchingMode mode);

} // logservice
} // oceanbase

#endif
