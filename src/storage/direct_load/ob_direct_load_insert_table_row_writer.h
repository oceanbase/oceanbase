/**
 * Copyright (c) 2024 OceanBase
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

#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "storage/direct_load/ob_direct_load_batch_row_buffer.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/direct_load/ob_direct_load_insert_table_row_handler.h"
#include "storage/direct_load/ob_direct_load_row_iterator.h"

namespace oceanbase
{
namespace sql
{
class ObLoadDataStat;
} // namespace sql
namespace storage
{
class ObDirectLoadDatumRow;
class ObDirectLoadDMLRowHandler;

class ObDirectLoadInsertTableBatchRowBufferWriter
{
public:
  ObDirectLoadInsertTableBatchRowBufferWriter();
  virtual ~ObDirectLoadInsertTableBatchRowBufferWriter();
  virtual int close();
  void cancel() { is_canceled_ = true; }
  int64_t get_row_count() const { return row_count_; }
  VIRTUAL_TO_STRING_KV(KP_(insert_tablet_ctx), K_(slice_id), K_(row_count), K_(is_canceled),
                       K_(is_inited));

protected:
  int inner_init(ObDirectLoadInsertTabletContext *insert_tablet_ctx,
                 const ObDirectLoadInsertTableRowInfo &row_info,
                 common::ObIAllocator *lob_allocator = nullptr);
  int flush_buffer();
  int flush_batch(blocksstable::ObBatchDatumRows &datum_rows);
  virtual int before_flush_batch(blocksstable::ObBatchDatumRows &datum_rows) { return OB_SUCCESS; }
  virtual int after_flush_batch(blocksstable::ObBatchDatumRows &datum_rows) { return OB_SUCCESS; }

protected:
  ObArenaAllocator allocator_;
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  ObDirectLoadInsertTableRowHandler row_handler_;
  ObDirectLoadBatchRowBuffer buffer_;
  blocksstable::ObBatchDatumRows datum_rows_;
  ObTabletID tablet_id_;
  int64_t slice_id_;
  int64_t row_count_;
  bool is_canceled_;
  bool is_inited_;
};

// only for hidden table
class ObDirectLoadInsertTableBatchRowDirectWriter
  : public ObDirectLoadInsertTableBatchRowBufferWriter
{
public:
  ObDirectLoadInsertTableBatchRowDirectWriter()
    : ObDirectLoadInsertTableBatchRowBufferWriter(),
      dml_row_handler_(nullptr),
      job_stat_(nullptr),
      write_ctx_(),
      direct_datum_rows_(),
      expect_column_count_(0),
      row_flag_()
  {
  }
  virtual ~ObDirectLoadInsertTableBatchRowDirectWriter() = default;
  int init(ObDirectLoadInsertTabletContext *insert_tablet_ctx,
           const ObDirectLoadInsertTableRowInfo &row_info,
           ObDirectLoadDMLRowHandler *dml_row_handler,
           common::ObIAllocator *lob_allocator,
           sql::ObLoadDataStat *job_stat = nullptr);
  int append_batch(const IVectorPtrs &vectors, const int64_t batch_size);
  int append_row(const IVectorPtrs &vectors, const int64_t row_idx);
  int append_row(const ObDirectLoadDatumRow &datum_row);
  int close() override;

private:
  int init_sstable_slice();
  int close_sstable_slice();
  int switch_sstable_slice();
  int before_flush_batch(blocksstable::ObBatchDatumRows &datum_rows) override;
  int after_flush_batch(blocksstable::ObBatchDatumRows &datum_rows) override;

private:
  ObDirectLoadDMLRowHandler *dml_row_handler_;
  sql::ObLoadDataStat *job_stat_;
  ObDirectLoadInsertTabletWriteCtx write_ctx_;
  blocksstable::ObBatchDatumRows direct_datum_rows_;
  int64_t expect_column_count_;
  ObDirectLoadRowFlag row_flag_;
};

class ObDirectLoadInsertTableBatchRowStoreWriter final
  : public ObDirectLoadInsertTableBatchRowBufferWriter
{
public:
  ObDirectLoadInsertTableBatchRowStoreWriter()
    : ObDirectLoadInsertTableBatchRowBufferWriter(), dml_row_handler_(nullptr), job_stat_(nullptr)
  {
  }
  virtual ~ObDirectLoadInsertTableBatchRowStoreWriter() = default;
  int init(ObDirectLoadInsertTabletContext *insert_tablet_ctx,
           const ObDirectLoadInsertTableRowInfo &row_info,
           const int64_t slice_id,
           ObDirectLoadDMLRowHandler *dml_row_handler,
           sql::ObLoadDataStat *job_stat);
  int write(ObDirectLoadIStoreRowIterator *row_iter);

private:
  int after_flush_batch(blocksstable::ObBatchDatumRows &datum_rows) override;

private:
  ObDirectLoadDMLRowHandler *dml_row_handler_;
  sql::ObLoadDataStat *job_stat_;
};

} // namespace storage
} // namespace oceanbase
