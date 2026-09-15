/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of the License at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * either express or implied, including but not limited to non-infringement,
 * merchantability or fit for a particular purpose.
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_INSTANCE_HASH_MODE_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_INSTANCE_HASH_MODE_H_

#include <stdint.h>
#include <strings.h>
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace libobcdc
{

enum InstanceHashMode
{
  INVALID_INSTANCE_HASH_MODE = 0,
  INSTANCE_HASH_BY_LS,
  INSTANCE_HASH_BY_TABLE,
  INSTANCE_HASH_BY_TABLET,
  MAX_INSTANCE_HASH_MODE
};

OB_INLINE const char *print_instance_hash_mode(const InstanceHashMode mode)
{
  const char *mode_str = "INVALID";

  switch (mode) {
    case INSTANCE_HASH_BY_LS:
      mode_str = "LS";
      break;
    case INSTANCE_HASH_BY_TABLE:
      mode_str = "TABLE";
      break;
    case INSTANCE_HASH_BY_TABLET:
      mode_str = "TABLET";
      break;
    default:
      break;
  }

  return mode_str;
}

OB_INLINE InstanceHashMode parse_instance_hash_mode(const char *mode_str)
{
  InstanceHashMode mode = INVALID_INSTANCE_HASH_MODE;

  if (NULL != mode_str) {
    if (0 == strcasecmp("LS", mode_str)) {
      mode = INSTANCE_HASH_BY_LS;
    } else if (0 == strcasecmp("TABLE", mode_str)) {
      mode = INSTANCE_HASH_BY_TABLE;
    } else if (0 == strcasecmp("TABLET", mode_str)) {
      mode = INSTANCE_HASH_BY_TABLET;
    }
  }

  return mode;
}

OB_INLINE bool is_instance_hash_mode_valid(const InstanceHashMode mode)
{
  return mode > INVALID_INSTANCE_HASH_MODE && mode < MAX_INSTANCE_HASH_MODE;
}

OB_INLINE bool is_table_or_tablet_hash_mode(const InstanceHashMode mode)
{
  return INSTANCE_HASH_BY_TABLE == mode || INSTANCE_HASH_BY_TABLET == mode;
}

OB_INLINE bool is_hash_value_served_by_instance(
    const uint64_t hash_value,
    const int64_t instance_num,
    const int64_t instance_index)
{
  const uint64_t bucket_count = static_cast<uint64_t>(instance_num);
  // Use a bit mask for power-of-two bucket counts; otherwise fall back to modulo.
  const uint64_t bucket = 0 == (bucket_count & (bucket_count - 1))
      ? (hash_value & (bucket_count - 1))
      : hash_value % bucket_count;
  return bucket == static_cast<uint64_t>(instance_index);
}

OB_INLINE bool is_table_served_by_instance(
    const uint64_t table_id,
    const int64_t instance_num,
    const int64_t instance_index)
{
  return is_hash_value_served_by_instance(table_id, instance_num, instance_index);
}

OB_INLINE bool is_tablet_served_by_instance(
    const uint64_t tablet_id,
    const int64_t instance_num,
    const int64_t instance_index)
{
  return is_hash_value_served_by_instance(tablet_id, instance_num, instance_index);
}

} // namespace libobcdc
} // namespace oceanbase

#endif
