/**
 * Copyright (c) 2022 OceanBase
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

RefreshMode get_refresh_mode(const char *refresh_mode_str)
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
