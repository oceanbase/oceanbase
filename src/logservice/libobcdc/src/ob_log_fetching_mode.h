/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 */
#ifndef OCEANBASE_LIBOCDC_OB_LOG_FETCHING_MODE_H_
#define OCEANBASE_LIBOCDC_OB_LOG_FETCHING_MODE_H_

namespace oceanbase
{
namespace libobcdc
{

enum class ClientFetchingMode {
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

} // cdc
} // oceanbase

#endif
