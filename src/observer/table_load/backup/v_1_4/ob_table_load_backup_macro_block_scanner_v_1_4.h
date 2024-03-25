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
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_macro_block_reader_v_1_4.h"
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_micro_block_scanner_v_1_4.h"
#include "common/row/ob_row_iterator.h"

namespace oceanbase
{
namespace observer
{

class ObTableLoadBackupMacroBlockScanner_V_1_4 : public ObNewRowIterator
{
public:
  ObTableLoadBackupMacroBlockScanner_V_1_4()
    : allocator_("TLD_BMBS_V_1_4"),
      block_idx_(0),
      row_alloced_(false),
      is_inited_(false)
  {
    allocator_.set_tenant_id(MTL_ID());
    column_map_ids_.set_tenant_id(MTL_ID());
  }
  virtual ~ObTableLoadBackupMacroBlockScanner_V_1_4() {}
  int init(const char *buf, int64_t buf_size, const ObIArray<int64_t> *column_ids);
  void reset() override;
  int get_next_row(common::ObNewRow *&row) override;
private:
  int init_column_map(const ObIArray<int64_t> *column_ids);
  int init_micro_block_scanner();
private:
  ObArenaAllocator allocator_;
  ObTableLoadBackupMacroBlockReader_V_1_4 macro_reader_;
  ObArray<int64_t> column_map_ids_;
  ObTableLoadBackupMicroBlockScanner_V_1_4 micro_scanner_;
  common::ObNewRow row_;
  int32_t block_idx_;
  bool row_alloced_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
