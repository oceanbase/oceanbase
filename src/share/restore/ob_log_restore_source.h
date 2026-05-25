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

#ifndef OCEANBASE_SHARE_OB_LOG_RESTORE_SOURCE_H_
#define OCEANBASE_SHARE_OB_LOG_RESTORE_SOURCE_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/worker.h"
#include "lib/allocator/page_arena.h"
#include "share/scn.h"
#include <cstdint>

namespace oceanbase
{
namespace share
{
enum class ObLogRestoreSourceType
{
  INVALID = 0,
  SERVICE = 1,
  LOCATION = 2,
  RAWPATH = 3,
  MAX = 4,
};

OB_INLINE bool is_valid_log_source_type(const ObLogRestoreSourceType &type)
{
  return type > ObLogRestoreSourceType::INVALID
    && type < ObLogRestoreSourceType::MAX;
}

OB_INLINE bool is_service_log_source_type(const ObLogRestoreSourceType &type)
{
  return type == ObLogRestoreSourceType::SERVICE;
}

OB_INLINE bool is_location_log_source_type(const ObLogRestoreSourceType &type)
{
  return type == ObLogRestoreSourceType::LOCATION;
}

OB_INLINE bool is_raw_path_log_source_type(const ObLogRestoreSourceType &type)
{
  return type == ObLogRestoreSourceType::RAWPATH;
}

struct ObLogRestoreSourceItem
{
  uint64_t tenant_id_;
  int64_t id_;
  ObLogRestoreSourceType type_;
  common::ObString value_;
  SCN until_scn_;
  int64_t recover_delay_us_;
  common::ObArenaAllocator allocator_;
  ObLogRestoreSourceItem() :
    tenant_id_(),
    id_(),
    type_(ObLogRestoreSourceType::INVALID),
    until_scn_(),
    recover_delay_us_(0),
    allocator_() {}
  ObLogRestoreSourceItem(const uint64_t tenant_id,
      const int64_t id,
      const SCN &until_scn) :
    tenant_id_(tenant_id),
    id_(id),
    type_(ObLogRestoreSourceType::INVALID),
    until_scn_(until_scn),
    recover_delay_us_(0),
    allocator_() {}
  ObLogRestoreSourceItem(const uint64_t tenant_id,
      const int64_t id,
      const ObLogRestoreSourceType &type,
      const ObString &value,
      const SCN &until_scn,
      const int64_t recover_delay_us = 0) :
    tenant_id_(tenant_id),
    id_(id),
    type_(type),
    value_(value),
    until_scn_(until_scn),
    recover_delay_us_(recover_delay_us),
    allocator_() {}
    ~ObLogRestoreSourceItem() {}
  void reset()
  {
    tenant_id_ = 0;
    id_ = 0;
    type_ = ObLogRestoreSourceType::INVALID;
    value_.reset();
    until_scn_.reset();
    recover_delay_us_ = 0;
    allocator_.reset();
  }
  bool is_valid() const;
  int deep_copy(ObLogRestoreSourceItem &other);
  static ObLogRestoreSourceType get_source_type(const ObString &type);
  static const char *get_source_type_str(const ObLogRestoreSourceType &type);
  // Parse delay value string (e.g. "30s", "1h") into microseconds
  static int parse_delay_value(const char *str, int64_t &delay_us);

  // Check if meta tenant data_version supports delay feature; sets enabled=true if version >= 4.4.2.2
  static int check_delay_data_version(const uint64_t tenant_id, bool &enabled);
  TO_STRING_KV(K_(tenant_id), K_(id), K_(until_scn), K_(type), K_(value), K_(recover_delay_us));
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreSourceItem);
};

} // namespace share
} // namespace oceanbase
#endif /* OCEANBASE_SHARE_OB_LOG_RESTORE_SOURCE_H_ */
