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
  MAX_VERSION
};

class ObTableLoadBackupTable
{
public:
  static int get_table(ObTableLoadBackupVersion version,
                       ObTableLoadBackupTable *&table,
                       ObIAllocator &allocator);
  ObTableLoadBackupTable() {}
  virtual ~ObTableLoadBackupTable() {}
  virtual int init(const share::ObBackupStorageInfo *storage_info, const ObString &path) = 0;
  virtual int scan(int64_t part_idx, common::ObNewRowIterator *&iter, common::ObIAllocator &allocator,
                   int64_t subpart_count = 1, int64_t subpart_idx = 0) = 0;
  virtual bool is_valid() const = 0;
  virtual int64_t get_column_count() const = 0;
  virtual int64_t get_partition_count() const = 0;
};

} // namespace observer
} // namespace oceanbase
