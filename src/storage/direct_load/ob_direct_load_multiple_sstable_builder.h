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
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_row.h"
#include "storage/direct_load/ob_direct_load_sstable_data_block_writer.h"
#include "storage/direct_load/ob_direct_load_sstable_index_block_writer.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTmpFileManager;

struct ObDirectLoadMultipleSSTableBuildParam
{
public:
  ObDirectLoadMultipleSSTableBuildParam();
  ~ObDirectLoadMultipleSSTableBuildParam();
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(table_data_desc), KP_(datum_utils), KP_(file_mgr));
public:
  common::ObTabletID tablet_id_;
  ObDirectLoadTableDataDesc table_data_desc_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  ObDirectLoadTmpFileManager *file_mgr_;
  char *extra_buf_;
  int64_t extra_buf_size_;
};

class ObDirectLoadMultipleSSTableBuilder : public ObIDirectLoadPartitionTableBuilder
{
  typedef ObDirectLoadMultipleDatumRowkey RowkeyType;
  typedef ObDirectLoadMultipleDatumRow RowType;
public:
  ObDirectLoadMultipleSSTableBuilder();
  virtual ~ObDirectLoadMultipleSSTableBuilder();
  int init(const ObDirectLoadMultipleSSTableBuildParam &param);
  int append_row(const common::ObTabletID &tablet_id,
                 const table::ObTableLoadSequenceNo &seq_no,
                 const blocksstable::ObDatumRow &datum_row) override;
  int append_row(const RowType &row);
  int close() override;
  int64_t get_row_count() const override { return row_count_; }
  int get_tables(common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                 common::ObIAllocator &allocator) override;
private:
  int check_rowkey_order(const RowkeyType &rowkey) const;
  int save_last_rowkey(const RowkeyType &rowkey);
private:
  class DataBlockFlushCallback : public ObIDirectLoadDataBlockFlushCallback
  {
  public:
    DataBlockFlushCallback();
    virtual ~DataBlockFlushCallback();
    int init(ObDirectLoadSSTableIndexBlockWriter *index_block_writer);
    int write(char *buf, int64_t buf_size, int64_t offset) override;
  private:
    ObDirectLoadSSTableIndexBlockWriter *index_block_writer_;
    bool is_inited_;
  };
private:
  common::ObArenaAllocator allocator_;
  ObDirectLoadMultipleSSTableBuildParam param_;
  RowType row_;
  RowkeyType first_rowkey_;
  RowkeyType last_rowkey_;
  common::ObArenaAllocator last_rowkey_allocator_;
  ObDirectLoadTmpFileHandle index_file_handle_;
  ObDirectLoadTmpFileHandle data_file_handle_;
  ObDirectLoadSSTableIndexBlockWriter index_block_writer_;
  ObDirectLoadSSTableDataBlockWriter<RowType> data_block_writer_;
  DataBlockFlushCallback callback_;
  int64_t row_count_;
  bool is_closed_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
