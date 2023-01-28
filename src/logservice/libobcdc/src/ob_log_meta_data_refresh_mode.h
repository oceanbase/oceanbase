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

#ifndef OCEANBASE_LOG_META_DATA_REFRESH_MODE_H_
#define OCEANBASE_LOG_META_DATA_REFRESH_MODE_H_

#include "share/ob_define.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace libobcdc
{
enum RefreshMode
{
  UNKNOWN_REFRSH_MODE = 0,

  DATA_DICT = 1,
  ONLINE = 2,

  MAX_REFRESH_MODE
};
const char *print_refresh_mode(const RefreshMode mode);
RefreshMode get_refresh_mode(const char *refresh_mode_str);

bool is_refresh_mode_valid(RefreshMode mode);
bool is_data_dict_refresh_mode(const RefreshMode mode);
bool is_online_refresh_mode(const RefreshMode mode);

}
}

#endif
