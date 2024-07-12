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
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_macro_block_scanner_v_1_4.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace observer
{

class ObTableLoadBackupPartScanner_V_1_4 : public ObNewRowIterator
{
  static const int64_t MIN_SUBPART_MACRO_BLOCK_COUNT = 16;
  static const int64_t MACRO_BLOCK_BUF_SIZE = 4 * 1024 * 1024;
public:
  ObTableLoadBackupPartScanner_V_1_4()
    : allocator_("TLD_BPS_V_1_4"),
      buf_(nullptr),
      block_idx_(-1),
      block_start_idx_(-1),
      block_end_idx_(-1),
      is_inited_(false)
  {
    allocator_.set_tenant_id(MTL_ID());
    column_ids_.set_tenant_id(MTL_ID());
    macro_block_list_.set_tenant_id(MTL_ID());
  }
  virtual ~ObTableLoadBackupPartScanner_V_1_4() {}
  int init(const share::ObBackupStorageInfo &storage_info, const ObIArray<int64_t> &column_ids,
           const ObString &data_path, const ObString &meta_path,
           int64_t subpart_count, int64_t subpart_idx);
  void reset() override;
  int get_next_row(ObNewRow *&row) override;
  TO_STRING_KV(K(storage_info_), K(column_ids_), K(data_path_), K(macro_block_list_),
               K(block_idx_), K(block_start_idx_), K(block_end_idx_));
private:
  int init_macro_block_list(const ObString &meta_path);
  int locate_subpart_macro_block(int64_t subpart_count, int64_t subpart_idx);
  int init_macro_block_scanner();
  int switch_next_macro_block();
private:
  ObArenaAllocator allocator_;
  share::ObBackupStorageInfo storage_info_;
  ObArray<int64_t> column_ids_;
  ObString data_path_;
  char *buf_;
  ObArray<ObString> macro_block_list_;
  int64_t block_idx_;
  // [start_idx, end_idx)
  int64_t block_start_idx_;
  int64_t block_end_idx_;
  ObTableLoadBackupMacroBlockScanner_V_1_4 scanner_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
