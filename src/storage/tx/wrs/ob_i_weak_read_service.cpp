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
