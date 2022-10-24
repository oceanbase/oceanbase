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

#ifndef OCEANBASE_SHARE_OB_LOG_ARCHIVE_SOURCE_H_
#define OCEANBASE_SHARE_OB_LOG_ARCHIVE_SOURCE_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/worker.h"
#include <cstdint>
namespace oceanbase
{
namespace share
{
enum class ObLogArchiveSourceType
{
  INVALID = 0,
  SERVICE = 1,
  LOCATION = 2,
  RAWPATH = 3,
  MAX = 4,
};

OB_INLINE bool is_valid_log_source_type(const ObLogArchiveSourceType &type)
{
  return type > ObLogArchiveSourceType::INVALID
    && type < ObLogArchiveSourceType::MAX;
}

OB_INLINE bool is_service_log_source_type(const ObLogArchiveSourceType &type)
{
  return type == ObLogArchiveSourceType::SERVICE;
}

OB_INLINE bool is_location_log_source_type(const ObLogArchiveSourceType &type)
{
  return type == ObLogArchiveSourceType::LOCATION;
}

OB_INLINE bool is_raw_path_log_source_type(const ObLogArchiveSourceType &type)
{
  return type == ObLogArchiveSourceType::RAWPATH;
}

struct ObLogArchiveSourceItem
{
  uint64_t tenant_id_;
  int64_t id_;
  ObLogArchiveSourceType type_;
  common::ObString value_;
  int64_t until_ts_;
  lib::ObArenaAllocator allocator_;
  ObLogArchiveSourceItem() :
    tenant_id_(),
    id_(),
    type_(ObLogArchiveSourceType::INVALID),
    until_ts_(OB_INVALID_TIMESTAMP),
    allocator_() {}
  ObLogArchiveSourceItem(const uint64_t tenant_id,
      const int64_t id,
      const int64_t until_ts) :
    tenant_id_(tenant_id),
    id_(id),
    type_(ObLogArchiveSourceType::INVALID),
    until_ts_(until_ts),
    allocator_() {}
  ObLogArchiveSourceItem(const uint64_t tenant_id,
      const int64_t id,
      const ObLogArchiveSourceType &type,
      const ObString &value,
      const int64_t until_ts) :
    tenant_id_(tenant_id),
    id_(id),
    type_(type),
    value_(value),
    until_ts_(until_ts),
    allocator_() {}
    ~ObLogArchiveSourceItem() {}
  bool is_valid() const;
  int deep_copy(ObLogArchiveSourceItem &other);
  static ObLogArchiveSourceType get_source_type(const ObString &type);
  static const char *get_source_type_str(const ObLogArchiveSourceType &type);
  TO_STRING_KV(K_(tenant_id), K_(id), K_(until_ts), K_(type), K_(value));
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogArchiveSourceItem);
};

} // namespace share
} // namespace oceanbase
#endif /* OCEANBASE_SHARE_OB_LOG_ARCHIVE_SOURCE_H_ */
