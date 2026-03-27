/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_MACRO_CACHE_OB_EXT_TABLE_DISK_CACHE_COMMON_META_H_
#define OCEANBASE_STORAGE_MACRO_CACHE_OB_EXT_TABLE_DISK_CACHE_COMMON_META_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace storage
{

/**
 * File version information for external table file.
 * Version is valid if either content_digest_ or modify_time_ is valid.
 */
class ObExtFileVersion
{
public:
  ObExtFileVersion();
  ~ObExtFileVersion();
  ObExtFileVersion(const common::ObString &content_digest, const int64_t modify_time);
  void reset();
  bool is_valid() const;
  bool operator==(const ObExtFileVersion &other) const;
  uint64_t hash() const;

  const common::ObString &content_digest() const { return content_digest_; }
  int64_t modify_time() const { return modify_time_; }

  TO_STRING_KV(K(content_digest_), K(modify_time_));

private:
  common::ObString content_digest_;
  int64_t modify_time_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MACRO_CACHE_OB_EXT_TABLE_DISK_CACHE_COMMON_META_H_
