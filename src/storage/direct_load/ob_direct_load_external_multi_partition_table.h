/**
 * Copyright (c) 2021 OceanBase
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

#include "lib/allocator/page_arena.h"
#include "storage/direct_load/ob_direct_load_external_block_writer.h"
#include "storage/direct_load/ob_direct_load_external_multi_partition_row.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"
#include "storage/direct_load/ob_direct_load_external_fragment.h"

namespace oceanbase
{
namespace storage
{
struct ObDirectLoadExternalMultiPartitionTableBuildParam
{
public:
  ObDirectLoadExternalMultiPartitionTableBuildParam();
  ~ObDirectLoadExternalMultiPartitionTableBuildParam();
  bool is_valid() const;
  TO_STRING_KV(K_(table_data_desc), KP_(datum_utils), KP_(file_mgr), KP_(extra_buf),
               K_(extra_buf_size));
public:
  ObDirectLoadTableDataDesc table_data_desc_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  ObDirectLoadTmpFileManager *file_mgr_;
  char *extra_buf_;
  int64_t extra_buf_size_;
};

class ObDirectLoadExternalMultiPartitionTableBuilder : public ObIDirectLoadPartitionTableBuilder
{
  typedef ObDirectLoadExternalMultiPartitionRow RowType;
  typedef ObDirectLoadExternalBlockWriter<RowType> ExternalWriter;
  static const int64_t MAX_TMP_FILE_SIZE = 1LL * 1024 * 1024 * 1024; // 1GiB
public:
  ObDirectLoadExternalMultiPartitionTableBuilder();
  virtual ~ObDirectLoadExternalMultiPartitionTableBuilder();
  int init(const ObDirectLoadExternalMultiPartitionTableBuildParam &param);
  int append_row(const common::ObTabletID &tablet_id,
                 const table::ObTableLoadSequenceNo &seq_no,
                 const blocksstable::ObDatumRow &datum_row) override;
  int close() override;
  int64_t get_row_count() const override { return total_row_count_; }
  int get_tables(common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                 common::ObIAllocator &allocator) override;
private:
  int alloc_tmp_file();
  int generate_fragment();
  int switch_fragment();
private:
  ObDirectLoadExternalMultiPartitionTableBuildParam param_;
  common::ObArenaAllocator allocator_;
  ObDirectLoadTmpFileHandle file_handle_;
  ExternalWriter external_writer_;
  RowType row_;
  ObDirectLoadExternalFragmentArray fragment_array_;
  int64_t total_row_count_;
  int64_t fragment_row_count_;
  int64_t max_data_block_size_;
  bool is_closed_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadExternalMultiPartitionTableBuilder);
};

} // namespace storage
} // namespace oceanbase
