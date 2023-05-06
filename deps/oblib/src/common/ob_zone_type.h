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

#ifndef OCEANBASE_COMMON_OB_ZONE_TYPE_H_
#define OCEANBASE_COMMON_OB_ZONE_TYPE_H_
#include <stdint.h>

namespace oceanbase
{
namespace common
{
const int64_t MAX_ZONE_TYPE_LENGTH = 128;
enum ObZoneType
{
  ZONE_TYPE_READWRITE = 0,
  ZONE_TYPE_READONLY = 1,
  ZONE_TYPE_ENCRYPTION = 2,
  ZONE_TYPE_INVALID = 3,
};

const char *zone_type_to_str(ObZoneType zone_type);
ObZoneType str_to_zone_type(const char *zone_type_str);

}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_ZONE_TYPE_H_
