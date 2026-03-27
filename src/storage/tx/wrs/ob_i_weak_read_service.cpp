/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_i_weak_read_service.h"


namespace oceanbase
{
namespace transaction
{

const char *wrs_level_to_str(const int level)
{
  const char *str = "INVALID";
  switch (level) {
    case WRS_LEVEL_CLUSTER:
      str = "CLUSTER";
      break;
    case WRS_LEVEL_REGION:
      str = "REGION";
      break;
    case WRS_LEVEL_ZONE:
      str = "ZONE";
      break;
    case WRS_LEVEL_SERVER:
      str = "SERVER";
      break;
    default:
      str = "UNKONWN";
  }
  return str;
}

}
}
