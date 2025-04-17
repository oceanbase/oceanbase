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
#include "observer/table_load/backup/ob_table_load_backup_macro_block_scanner.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

class ObTableLoadBackupPartScanner : public ObNewRowIterator
{
public:
  ObTableLoadBackupPartScanner();
  virtual ~ObTableLoadBackupPartScanner();
  void reset() override;
  int get_next_row(ObNewRow *&row) override;
protected:
  static const int64_t MIN_SUBPART_MACRO_BLOCK_COUNT = 16;
  static const int64_t SSTABLE_BLOCK_BUF_SIZE = 2 * 1024 * 1024 + 256 * 1024;
  int inner_init(const ObTableLoadBackupVersion &backup_version,
                 const share::ObBackupStorageInfo &storage_info,
                 const ObSchemaInfo &schema_info,
                 int64_t subpart_count,
                 int64_t subpart_idx);
  int init_macro_block_index(const ObString &meta_path);
  int locate_subpart_macro_block(const int64_t total_macro_block_count,
                                 const int64_t subpart_count,
                                 const int64_t subpart_idx);
  int init_lob_col_buf();
  int switch_next_macro_block();
  int read_lob_data(common::ObNewRow *&row);
  int fill_lob_buf(const ObBackupLobIndex &lob_index,
                   char *&buf,
                   const int64_t buf_size,
                   int64_t &pos);
  bool is_lob_block(const int64_t macro_block_id) const;
  virtual int init_macro_block_index(const int64_t subpart_count,
                                     const int64_t subpart_idx) = 0;
  virtual int read_macro_block_data(const int64_t block_idx,
                                    const bool is_lob_block,
                                    char *&data_buf,
                                    int64_t &read_size) = 0;
protected:
  ObArenaAllocator allocator_;
  ObArenaAllocator lob_header_allocator_;
  ObTableLoadBackupVersion backup_version_;
  share::ObBackupStorageInfo storage_info_;
  ObSchemaInfo schema_info_;
  ObArray<int64_t> column_ids_;
  common::hash::ObHashMap<int64_t, int64_t> lob_macro_block_idx_map_;
  char *buf_;
  char *lob_buf_;
  ObArray<char *> lob_col_buf_;
  ObArray<int64_t> lob_col_buf_size_;
  int64_t block_idx_;
  // [start_idx, end_idx)
  int64_t block_start_idx_;
  int64_t block_end_idx_;
  ObTableLoadBackupMacroBlockScanner macro_block_scanner_;
  bool is_inited_;
};

} // namespace table_load_backup
} // namespace observer
} // namespace oceanbase
