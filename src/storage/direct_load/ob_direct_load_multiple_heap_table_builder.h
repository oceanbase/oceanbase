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

#include "common/ob_tablet_id.h"
#include "storage/direct_load/ob_direct_load_data_block.h"
#include "storage/direct_load/ob_direct_load_data_block_writer.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_multiple_external_row.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_index_block_writer.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTmpFileManager;

struct ObDirectLoadMultipleHeapTableBuildParam
{
public:
  ObDirectLoadMultipleHeapTableBuildParam();
  ~ObDirectLoadMultipleHeapTableBuildParam();
  bool is_valid() const;
  TO_STRING_KV(K_(table_data_desc), KP_(file_mgr), KP_(extra_buf), K_(extra_buf_size),
               K_(index_dir_id), K_(data_dir_id));
public:
  ObDirectLoadTableDataDesc table_data_desc_;
  ObDirectLoadTmpFileManager *file_mgr_;
  char *extra_buf_;
  int64_t extra_buf_size_;
  int64_t index_dir_id_;
  int64_t data_dir_id_;
};

class ObDirectLoadMultipleHeapTableBuilder : public ObIDirectLoadPartitionTableBuilder
{
  typedef ObDirectLoadMultipleExternalRow RowType;
  typedef ObDirectLoadMultipleHeapTableIndexBlockWriter IndexBlockWriter;
  typedef ObDirectLoadDataBlockWriter<ObDirectLoadDataBlock::Header, RowType> DataBlockWriter;
public:
  ObDirectLoadMultipleHeapTableBuilder();
  virtual ~ObDirectLoadMultipleHeapTableBuilder();
  int init(const ObDirectLoadMultipleHeapTableBuildParam &param);
  int append_row(const common::ObTabletID &tablet_id,
                 const table::ObTableLoadSequenceNo &seq_no,
                 const blocksstable::ObDatumRow &datum_row) override;
  int append_row(const RowType &row);
  int close() override;
  int64_t get_row_count() const override { return row_count_; }
  int get_tables(common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                 common::ObIAllocator &allocator) override;
private:
  class DataBlockFlushCallback : public ObIDirectLoadDataBlockFlushCallback
  {
  public:
    DataBlockFlushCallback() : data_block_offset_(0) {}
    virtual ~DataBlockFlushCallback() = default;
    int write(char *buf, int64_t buf_size, int64_t offset) override
    {
      data_block_offset_ = offset + buf_size;
      return common::OB_SUCCESS;
    }
    int64_t get_data_block_offset() const { return data_block_offset_; }
  private:
    int64_t data_block_offset_;
  };
private:
  ObDirectLoadMultipleHeapTableBuildParam param_;
  RowType row_;
  ObDirectLoadTmpFileHandle index_file_handle_;
  ObDirectLoadTmpFileHandle data_file_handle_;
  IndexBlockWriter index_block_writer_;
  DataBlockWriter data_block_writer_;
  DataBlockFlushCallback callback_;
  ObDirectLoadMultipleHeapTableTabletIndex last_tablet_index_;
  int64_t index_entry_count_;
  int64_t row_count_;
  bool is_closed_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
