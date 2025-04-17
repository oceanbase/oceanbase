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
#include "observer/table_load/backup/ob_table_load_backup_column_map_v1.h"
#include "observer/table_load/backup/ob_table_load_backup_imacro_block_reader.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
class ObTableLoadBackupMacroBlockReader : public ObTableLoadBackupIMacroBlockReader
{
public:
  ObTableLoadBackupMacroBlockReader();
  virtual ~ObTableLoadBackupMacroBlockReader() = default;
  int init(const char *buf, int64_t buf_size, const ObTableLoadBackupVersion &backup_version) override;
  void reset() override;
  const ObIColumnMap* get_column_map() override { return &col_map_; }
  bool is_lob_block() override { return false; }
  const ObMacroBlockMeta* get_macro_block_meta() { return &meta_; }
  int get_next_micro_block(const ObMicroBlockData *&micro_block_data) override;
private:
  int inner_init();
  int alloc_buf(const int64_t size);
private:
  ObArenaAllocator allocator_;
  ObMacroBlockMeta meta_;
  ObColumnMapV1 col_map_;
  const ObMicroBlockIndex *micro_index_;
  common::ObCompressor *compressor_;
  const char *buf_;
  int64_t buf_size_;
  char *decomp_buf_;
  int64_t decomp_buf_size_;
  int64_t pos_;
  ObMicroBlockData micro_block_data_;
  int64_t micro_offset_;
  int64_t cur_block_idx_;
  ObTableLoadBackupVersion backup_version_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadBackupMacroBlockReader);
};

} // table_load_backup
} // namespace observer
} // namespace oceanbase
