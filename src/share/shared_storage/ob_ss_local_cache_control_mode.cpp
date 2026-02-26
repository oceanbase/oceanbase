//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX SHARE

#include "share/shared_storage/ob_ss_local_cache_control_mode.h"

namespace oceanbase
{
namespace common
{

ObSSLocalCacheControlMode::ObSSLocalCacheControlMode(const uint8_t *values)
{
  if (OB_UNLIKELY(values == NULL)) {
    value_ = 0;
  } else {
    value_ = (static_cast<uint16_t>(values[1]) << 8) | static_cast<uint16_t>(values[0]);
  }
}

void ObSSLocalCacheControlMode::assign(const ObSSLocalCacheControlMode &other)
{
  if (this != &other) {
    value_ = other.get_value();
  }
}

int ObSSLocalCacheControlMode::set_value(const ObConfigModeItem &mode_item)
{
  int ret = OB_SUCCESS;
  const uint8_t *values = mode_item.get_value();
  if (OB_ISNULL(values)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "mode item's value_ is null ptr", KR(ret));
  } else {
    STATIC_ASSERT(((sizeof(value_)/sizeof(uint8_t) <= ObConfigModeItem::MAX_MODE_BYTES)),
                  "value_ size overflow");
    value_ = 0;
    for (uint64_t i = 0; i < 2; ++i) {
      value_ = (value_ | static_cast<uint16_t>(values[i]) << (8 * i));
    }
  }
  return ret;
}

void ObSSLocalCacheControlMode::set_micro_cache_mode(uint16_t mode)
{
  micro_cache_mode_ = mode;
}

void ObSSLocalCacheControlMode::set_macro_read_cache_mode(uint16_t mode)
{
  macro_read_cache_mode_ = mode;
}

void ObSSLocalCacheControlMode::set_macro_write_cache_mode(uint16_t mode)
{
  macro_write_cache_mode_ = mode;
}

} // namespace common
} // namespace oceanbase
