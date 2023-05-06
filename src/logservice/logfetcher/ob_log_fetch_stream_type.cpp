/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 *  Fetch log stream type
 */

#include "ob_log_fetch_stream_type.h"

namespace oceanbase
{
namespace logfetcher
{
bool is_fetch_stream_type_valid(const FetchStreamType type)
{
  return type > FETCH_STREAM_TYPE_UNKNOWN && type < FETCH_STREAM_TYPE_MAX;
}

const char *print_fetch_stream_type(FetchStreamType type)
{
  const char *str = nullptr;

  switch (type) {
    case FETCH_STREAM_TYPE_UNKNOWN:
      str = "UNKNOWN";
      break;
    case FETCH_STREAM_TYPE_HOT:
      str = "HOT";
      break;
    case FETCH_STREAM_TYPE_COLD:
      str = "COLD";
      break;
    case FETCH_STREAM_TYPE_SYS_LS:
      str = "SYS_LS";
      break;
    default:
      str = "INVALID";
      break;
  }

  return str;
}

}
}
