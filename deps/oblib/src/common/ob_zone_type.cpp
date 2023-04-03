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

#define USING_LOG_PREFIX COMMON

#include "common/ob_zone_type.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{

static const char *zone_type_strs[] = { "ReadWrite", "ReadOnly", "Encryption", "Invalid" };
const char *zone_type_to_str(ObZoneType zone_type)
{
  const char *zone_type_str = NULL;
  if (zone_type < ObZoneType::ZONE_TYPE_READWRITE || zone_type > ObZoneType::ZONE_TYPE_INVALID) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown zone_type", K(zone_type));
  } else {
    int index = static_cast<int>(zone_type);
    zone_type_str = zone_type_strs[index];
  }
  return zone_type_str;
}

ObZoneType str_to_zone_type(const char *zone_type_str)
{
  ObZoneType zone_type = ObZoneType::ZONE_TYPE_INVALID;
  if (NULL == zone_type_str) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "empty zone_type_str", KP(zone_type_str));
  } else {
    for (int64_t i = 0; i <= ObZoneType::ZONE_TYPE_INVALID; ++i) {
      if (0 == strncasecmp(zone_type_strs[i], zone_type_str, strlen(zone_type_strs[i]))) {
        zone_type = static_cast<ObZoneType>(i);
      }
    }
  }
  return zone_type;
}

}//end namespace common
}//end namespace oceanbase

