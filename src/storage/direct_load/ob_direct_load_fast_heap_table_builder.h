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
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/direct_load/ob_direct_load_insert_table_row_iterator.h"
#include "storage/direct_load/ob_direct_load_lob_builder.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace table
{
class ObTableLoadSqlStatistics;
} // namespace table
namespace storage
{
class ObDirectLoadInsertTableContext;
class ObDirectLoadDMLRowHandler;
struct ObDirectLoadFastHeapTableBuildParam
{
public:
  ObDirectLoadFastHeapTableBuildParam();
  ~ObDirectLoadFastHeapTableBuildParam();
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id),
               K_(table_data_desc),
               KP_(insert_table_ctx),
               KP_(dml_row_handler));
public:
  common::ObTabletID tablet_id_;
  ObDirectLoadTableDataDesc table_data_desc_;
  ObDirectLoadInsertTableContext *insert_table_ctx_;
  ObDirectLoadDMLRowHandler *dml_row_handler_;
};

class ObDirectLoadFastHeapTableBuilder : public ObIDirectLoadPartitionTableBuilder
{
  static const int64_t HIDDEN_ROWKEY_COLUMN_NUM = 1;
public:
  ObDirectLoadFastHeapTableBuilder();
  virtual ~ObDirectLoadFastHeapTableBuilder();
  int init(const ObDirectLoadFastHeapTableBuildParam &param);
  int append_row(const common::ObTabletID &tablet_id, const table::ObTableLoadSequenceNo &seq_no,
                 const blocksstable::ObDatumRow &datum_row) override;
  int close() override;
  int64_t get_row_count() const override { return row_count_; }
  int get_tables(common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                 common::ObIAllocator &allocator) override;
private:
  int init_sstable_slice_ctx();
  int switch_sstable_slice();
private:
  class RowIterator : public ObDirectLoadInsertTableRowIterator
  {
  public:
    RowIterator();
    virtual ~RowIterator();
    int init(ObDirectLoadInsertTabletContext *insert_tablet_ctx,
             table::ObTableLoadSqlStatistics *sql_statistics,
             ObDirectLoadLobBuilder &lob_builder);
    int set_row(blocksstable::ObDatumRow &row);
  protected:
    int inner_get_next_row(blocksstable::ObDatumRow *&row) override;
  private:
    blocksstable::ObDatumRow *datum_row_;
  };
private:
  ObDirectLoadFastHeapTableBuildParam param_;
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  table::ObTableLoadSqlStatistics *sql_statistics_;
  ObDirectLoadLobBuilder lob_builder_;
  ObDirectLoadInsertTabletWriteCtx write_ctx_;
  blocksstable::ObDatumRow datum_row_;
  RowIterator row_iter_;
  int64_t current_slice_id_;
  int64_t row_count_;
  bool has_lob_;
  bool is_closed_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadFastHeapTableBuilder);
};

} // namespace storage
} // namespace oceanbase
