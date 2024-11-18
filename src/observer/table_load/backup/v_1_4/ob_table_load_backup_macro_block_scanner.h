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
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_macro_block_reader.h"
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_micro_block_scanner.h"
#include "common/row/ob_row_iterator.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_1_4
{

class ObTableLoadBackupMacroBlockScanner : public ObNewRowIterator
{
public:
  ObTableLoadBackupMacroBlockScanner()
    : allocator_("TLD_BMaBSR_V14"),
      block_idx_(0),
      is_inited_(false)
  {
    allocator_.set_tenant_id(MTL_ID());
    column_map_ids_.set_tenant_id(MTL_ID());
  }
  virtual ~ObTableLoadBackupMacroBlockScanner() {}
  int init(
      const char *buf,
      int64_t buf_size,
      const ObSchemaInfo *schema_info,
      const ObIArray<int64_t> *column_ids);
  void reset() override;
  int get_next_row(common::ObNewRow *&row) override;
private:
  int init_column_map_ids(const ObSchemaInfo *schema_info, const ObIArray<int64_t> *column_ids);
  int init_row();
  int init_micro_block_scanner();
  int adjust_column_idx(common::ObNewRow *&row);
private:
  ObArenaAllocator allocator_;
  ObTableLoadBackupMacroBlockReader macro_reader_;
  ObArray<int64_t> column_map_ids_;
  ObTableLoadBackupMicroBlockScanner micro_scanner_;
  common::ObNewRow row_;
  int32_t block_idx_;
  bool is_inited_;
};

} // table_load_backup_v_1_4
} // namespace observer
} // namespace oceanbase
