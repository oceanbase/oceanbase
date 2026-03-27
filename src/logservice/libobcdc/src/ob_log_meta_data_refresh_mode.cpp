/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_log_meta_data_refresh_mode.h"

namespace oceanbase
{
namespace libobcdc
{
using namespace oceanbase::common;

const char *print_refresh_mode(const RefreshMode mode)
{
  const char *mode_str = "INVALID";

  switch (mode) {
    case DATA_DICT: {
      mode_str = "DataDict RefreshMode";
      break;
    }
    case ONLINE: {
      mode_str = "Online RefreshMode";
      break;
    }
    default: {
      mode_str = "INVALID";
      break;
    }
  }

  return mode_str;
}

RefreshMode parse_refresh_mode(const char *refresh_mode_str)
{
  RefreshMode ret_mode = UNKNOWN_REFRSH_MODE;

  if (OB_ISNULL(refresh_mode_str)) {
  } else {
    if (0 == strcmp("data_dict", refresh_mode_str)) {
      ret_mode = DATA_DICT;
    } else if (0 == strcmp("online", refresh_mode_str)) {
      ret_mode = ONLINE;
    } else {
    }
  }

  return ret_mode;
}

bool is_refresh_mode_valid(RefreshMode mode)
{
  bool bool_ret = false;

  bool_ret = (mode > RefreshMode::UNKNOWN_REFRSH_MODE)
    && (mode < RefreshMode::MAX_REFRESH_MODE);

  return bool_ret;
}

bool is_data_dict_refresh_mode(const RefreshMode mode)
{
  return RefreshMode::DATA_DICT == mode;
}

bool is_online_refresh_mode(const RefreshMode mode)
{
  return RefreshMode::ONLINE == mode;
}

}
}
