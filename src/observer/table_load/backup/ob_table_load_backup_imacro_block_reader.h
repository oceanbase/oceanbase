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
#include "observer/table_load/backup/ob_table_load_backup_table.h"
#include "observer/table_load/backup/ob_table_load_backup_icolumn_map.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
class ObTableLoadBackupIMacroBlockReader
{
public:
  ObTableLoadBackupIMacroBlockReader() = default;
  virtual ~ObTableLoadBackupIMacroBlockReader() = default;
  virtual int init(const char *buf, int64_t buf_size, const ObTableLoadBackupVersion &backup_version) = 0;
  virtual void reset() = 0;
  virtual int get_next_micro_block(const ObMicroBlockData *&micro_block_data) = 0;
  virtual const ObIColumnMap* get_column_map() = 0;
  virtual bool is_lob_block() = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadBackupIMacroBlockReader);
};

} // table_load_backup
} // namespace observer
} // namespace oceanbase
