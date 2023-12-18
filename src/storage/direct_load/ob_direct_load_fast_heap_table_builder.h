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
#include "storage/direct_load/ob_direct_load_table_data_desc.h"
#include "storage/direct_load/ob_direct_load_insert_table_row_iterator.h"
#include "storage/direct_load/ob_direct_load_lob_builder.h"
#include "sql/engine/expr/ob_expr_sys_op_opnsize.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"

namespace oceanbase
{
namespace common
{
class ObOptOSGColumnStat;
} // namespace common
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
  TO_STRING_KV(K_(tablet_id), K_(snapshot_version), K_(table_data_desc), KP_(datum_utils),
               KP_(col_descs), KP_(lob_column_cnt), KP_(cmp_funcs), KP_(dml_row_handler),
               K_(online_opt_stat_gather), K_(px_mode));
public:
  common::ObTabletID tablet_id_;
  int64_t snapshot_version_;
  int64_t lob_column_cnt_;
  ObDirectLoadTableDataDesc table_data_desc_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  const blocksstable::ObStoreCmpFuncs *cmp_funcs_;
  ObDirectLoadInsertTableContext *insert_table_ctx_;
  ObDirectLoadDMLRowHandler *dml_row_handler_;
  bool online_opt_stat_gather_;
  bool px_mode_;
};

class ObDirectLoadFastHeapTableBuilder : public ObIDirectLoadPartitionTableBuilder
{
  static const int64_t HIDDEN_ROWKEY_COLUMN_NUM = 1;
public:
  ObDirectLoadFastHeapTableBuilder();
  virtual ~ObDirectLoadFastHeapTableBuilder();
  int init(const ObDirectLoadFastHeapTableBuildParam &param);
  int append_row(const common::ObTabletID &tablet_id,
                 const table::ObTableLoadSequenceNo &seq_no,
                 const blocksstable::ObDatumRow &datum_row) override;
  int close() override;
  int64_t get_row_count() const override { return row_count_; }
  int get_tables(common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                 common::ObIAllocator &allocator) override;
private:
  int init_sql_statistics();
  int init_lob_builder();
  int init_sstable_slice_ctx();
  int switch_sstable_slice();
private:
  class RowIterator : public ObDirectLoadInsertTableRowIterator
    {
    public:
      RowIterator();
      virtual ~RowIterator();
      void reuse();
      void reset();
      int init(const ObDirectLoadFastHeapTableBuildParam &param, blocksstable::ObDatumRow &row,
              common::ObIArray<ObOptOSGColumnStat*> &column_stat_array, ObDirectLoadLobBuilder &lob_builder);
    protected:
      int inner_get_next_row(blocksstable::ObDatumRow *&row) override;
    private:
      blocksstable::ObDatumRow *datum_row_;
      bool iter_end_;
      bool is_inited_;
    };
private:
  ObDirectLoadFastHeapTableBuildParam param_;
  common::ObArenaAllocator allocator_;
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  ObDirectLoadInsertTabletWriteCtx write_ctx_;
  blocksstable::ObDatumRow datum_row_;
  common::ObArray<ObOptOSGColumnStat *> column_stat_array_;
  ObDirectLoadLobBuilder lob_builder_;
  RowIterator row_iter_;
  int64_t current_slice_id_;
  int64_t row_count_;
  bool is_closed_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadFastHeapTableBuilder);
};

} // namespace storage
} // namespace oceanbase
