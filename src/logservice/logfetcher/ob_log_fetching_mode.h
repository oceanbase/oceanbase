/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
