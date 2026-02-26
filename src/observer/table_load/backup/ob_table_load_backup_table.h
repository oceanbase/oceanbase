/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once
#include "common/row/ob_row_iterator.h"
#include "share/schema/ob_table_schema.h"
#include "share/backup/ob_backup_struct.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace observer
{
enum class ObTableLoadBackupVersion
{
  INVALID = 0,
  V_1_4,
  V_3_X,      // FARM COMPAT WHITELIST, 重命名，加白名单
  V_2_X_LOG,
  V_2_X_PHY,
  MAX_VERSION
};

static bool is_logical_backup_version(const ObTableLoadBackupVersion &backup_version)
{
  return backup_version == ObTableLoadBackupVersion::V_1_4 || backup_version == ObTableLoadBackupVersion::V_2_X_LOG;
}

class ObTableLoadBackupTable
{
public:
  static int get_table(const ObTableLoadBackupVersion &backup_version,
                       ObTableLoadBackupTable *&table,
                       ObIAllocator &allocator);
  ObTableLoadBackupTable() {}
  virtual ~ObTableLoadBackupTable() {}
  virtual int init(const ObTableLoadBackupVersion &backup_version,
                   const share::ObBackupStorageInfo *storage_info,
                   const ObString &path,
                   const share::schema::ObTableSchema *table_schema) = 0;
  virtual int scan(int64_t part_idx, common::ObNewRowIterator *&iter, common::ObIAllocator &allocator,
                   int64_t subpart_count = 1, int64_t subpart_idx = 0) = 0;
  virtual int64_t get_partition_count() const = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

} // namespace observer
} // namespace oceanbase
