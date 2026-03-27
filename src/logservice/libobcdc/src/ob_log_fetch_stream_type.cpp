/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_log_fetch_stream_type.h"

namespace oceanbase
{
namespace libobcdc
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
