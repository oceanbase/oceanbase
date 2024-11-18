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
#include "observer/table_load/backup/v_3_x/ob_table_load_backup_sstable_block_reader.h"
#include "observer/table_load/backup/v_3_x/ob_table_load_backup_micro_block_scanner.h"
#include "common/row/ob_row_iterator.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_3_x
{

class ObTableLoadBackupSSTableBlockScanner : public ObNewRowIterator
{
public:
  ObTableLoadBackupSSTableBlockScanner();
  virtual ~ObTableLoadBackupSSTableBlockScanner() {}
  int init(const char *buf, int64_t buf_size, const ObSchemaInfo &schema_info);
  void reset() override;
  int get_next_row(common::ObNewRow *&row) override;
  bool is_lob_block() { return sstable_block_reader_.get_macro_block_common_header().is_lob_data_block(); }
private:
  int init_column_map_ids(const ObSchemaInfo &schema_info);
  int init_row();
  int adjust_column_idx(common::ObNewRow *&row);
  int switch_next_micro_block();
private:
  ObArenaAllocator allocator_;
  ObTableLoadBackupSSTableBlockReader sstable_block_reader_;
  ObTableLoadBackupMicroBlockScanner micro_block_scanner_;
  ObArray<int64_t> column_map_ids_;
  common::ObNewRow row_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadBackupSSTableBlockScanner);
};

} // table_load_backup_v_3_x
} // namespace observer
} // namespace oceanbase
