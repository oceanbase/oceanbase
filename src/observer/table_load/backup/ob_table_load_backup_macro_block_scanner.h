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
#include "observer/table_load/backup/ob_table_load_backup_micro_block_scanner.h"
#include "observer/table_load/backup/ob_table_load_backup_macro_block_reader.h"
#include "observer/table_load/backup/ob_table_load_backup_sstable_block_reader.h"
#include "common/row/ob_row_iterator.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

class ObTableLoadBackupMacroBlockScanner : public ObNewRowIterator
{
public:
  ObTableLoadBackupMacroBlockScanner();
  virtual ~ObTableLoadBackupMacroBlockScanner() = default;
  int init(const char *buf,
           int64_t buf_size,
           const ObTableLoadBackupVersion &backup_version,
           const ObSchemaInfo *schema_info,
           const ObIArray<int64_t> *column_ids);
  void reset() override;
  int get_next_row(common::ObNewRow *&row) override;
  bool is_lob_block() { return macro_block_reader_ != nullptr && macro_block_reader_->is_lob_block(); }
private:
  int init_macro_block_reader(const char *buf, int64_t buf_size);
  int init_column_map_ids(const ObSchemaInfo *schema_info, const ObIArray<int64_t> *column_ids);
  int init_row();
  int switch_next_micro_block();
  int adjust_column_idx(common::ObNewRow *&row);
protected:
  ObArenaAllocator allocator_;
  ObTableLoadBackupVersion backup_version_;
  ObTableLoadBackupMicroBlockScanner micro_block_scanner_;
  ObArray<int64_t> column_map_ids_;
  common::ObNewRow row_;
  ObTableLoadBackupIMacroBlockReader *macro_block_reader_;
  bool is_inited_;
};

} // table_load_backup
} // namespace observer
} // namespace oceanbase
