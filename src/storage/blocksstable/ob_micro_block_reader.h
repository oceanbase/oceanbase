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

#ifndef OB_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_READER_H_
#define OB_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_READER_H_

#include "ob_imicro_block_reader.h"
#include "ob_micro_block_hash_index.h"
#include "ob_row_reader.h"
#include "sql/engine/basic/ob_pushdown_filter.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace storage {
struct PushdownFilterInfo;
}
namespace blocksstable
{

class ObIMicroBlockFlatReader
{
public:
  ObIMicroBlockFlatReader();
  virtual ~ObIMicroBlockFlatReader();
  void reset();
protected:
  int find_bound_(const ObDatumRowkey &key,
                         const bool lower_bound,
                         const int64_t begin_idx,
                         const int64_t end_idx,
                         const ObStorageDatumUtils &datum_utils,
                         int64_t &row_idx,
                         bool &equal);
  OB_INLINE int init(const ObMicroBlockData &block_data);
protected:
  const ObMicroBlockHeader *header_;
  const char *data_begin_;
  const char *data_end_;
  const int32_t *index_data_;
  // TODO: remove allocator
  ObBlockReaderAllocator allocator_;
  ObRowReader flat_row_reader_;
};

class ObMicroBlockReader : public ObIMicroBlockFlatReader, public ObIMicroBlockReader
{
public:
  ObMicroBlockReader()
    : ObIMicroBlockFlatReader(),
      ObIMicroBlockReader()
  {}
  virtual ~ObMicroBlockReader()
  { reset(); }
  virtual ObReaderType get_type() override { return Reader; }
  virtual void reset();
  virtual int init(
      const ObMicroBlockData &block_data,
      const ObITableReadInfo &read_info) override;
  //when there is not read_info in input parameters, it indicates reading all columns from all rows
  //when the incoming datum_utils is nullptr, it indicates not calling locate_range or find_bound
  virtual int init(
      const ObMicroBlockData &block_data,
	  const ObStorageDatumUtils *datum_utils) override;
  virtual int get_row(const int64_t index, ObDatumRow &row) override;
  virtual int get_row_header(
      const int64_t row_idx,
      const ObRowHeader *&row_header) override;
  virtual int get_row_count(int64_t &row_count) override;
  int get_multi_version_info(
      const int64_t row_idx,
      const int64_t schema_rowkey_cnt,
      const ObRowHeader *&row_header,
      int64_t &trans_version,
      int64_t &sql_sequence);
  // Filter interface for filter pushdown
  int filter_pushdown_filter(
      const sql::ObPushdownFilterExecutor *parent,
      sql::ObPushdownFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      common::ObBitmap &result_bitmap);
  int get_rows(
      const common::ObIArray<int32_t> &cols_projector,
      const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
      const blocksstable::ObDatumRow *default_row,
      const int32_t *row_ids,
      const int64_t row_cap,
      ObDatumRow &row_buf,
      common::ObIArray<ObSqlDatumInfo> &datum_infos,
      const int64_t datum_offset,
      sql::ObExprPtrIArray &exprs,
      sql::ObEvalCtx &eval_ctx);
  virtual int get_row_count(
      int32_t col,
      const int32_t *row_ids,
      const int64_t row_cap,
      const bool contains_null,
      const share::schema::ObColumnParam *col_param,
      int64_t &count) override final;
  virtual int64_t get_column_count() const override
  {
    OB_ASSERT(nullptr != header_);
    return header_->column_count_;
  }
  virtual int get_aggregate_result(
      const ObTableIterParam &iter_param,
      const ObTableAccessContext &context,
      const int32_t col_offset,
      const share::schema::ObColumnParam &col_param,
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObAggDatumBuf &agg_datum_buf,
      storage::ObAggCell &agg_cell) override;
  int get_aggregate_result(
      const ObTableIterParam &iter_param,
      const ObTableAccessContext &context,
      const int32_t *row_ids,
      const int64_t row_cap,
      ObDatumRow &row_buf,
      common::ObIArray<storage::ObAggCell*> &agg_cells);
  virtual int get_column_datum(
      const ObTableIterParam &iter_param,
      const ObTableAccessContext &context,
      const share::schema::ObColumnParam &col_param,
      const int32_t col_offset,
      const int64_t row_index,
      ObStorageDatum &datum) override;
  virtual int compare_rowkey(
      const ObDatumRowkey &rowkey,
      const int64_t index,
      int32_t &compare_result) override;
  virtual int find_bound(
      const ObDatumRowkey &key,
      const bool lower_bound,
      const int64_t begin_idx,
      int64_t &row_idx,
      bool &equal) override;
  virtual int find_bound_through_linear_search(
      const ObDatumRowkey &rowkey,
      const int64_t begin_idx,
      int64_t &row_idx) override;
  OB_INLINE bool single_version_rows() { return nullptr != header_ && header_->single_version_rows_; }

  // For column store
  virtual int find_bound(
      const ObDatumRowkey &key,
      const bool lower_bound,
      const int64_t begin_idx,
      const int64_t end_idx,
      int64_t &row_idx,
      bool &equal) override;
  virtual void reserve_reader_memory(bool reserve) override
  { allocator_.set_reserve_memory(reserve); }
  int get_rows(
      const common::ObIArray<int32_t> &cols_projector,
      const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
      const blocksstable::ObDatumRow *default_row,
      const int32_t *row_ids,
      const int64_t vector_offset,
      const int64_t row_cap,
      ObDatumRow &row_buf,
      sql::ObExprPtrIArray &exprs,
      sql::ObEvalCtx &eval_ctx);
  virtual bool has_lob_out_row() const override final
  { return nullptr != header_ && header_->has_lob_out_row(); }

protected:
  virtual int find_bound(
      const ObDatumRange &range,
      const int64_t begin_idx,
      int64_t &row_idx,
      bool &equal,
      int64_t &end_key_begin_idx,
      int64_t &end_key_end_idx) override;
};

class ObMicroBlockGetReader : public ObIMicroBlockFlatReader, public ObIMicroBlockGetReader
{
public:
  ObMicroBlockGetReader()
      : ObIMicroBlockFlatReader(),
        ObIMicroBlockGetReader(),
        hash_index_()
  {}
  virtual ~ObMicroBlockGetReader() {}
  virtual int get_row(
      const ObMicroBlockData &block_data,
      const ObDatumRowkey &rowkey,
      const storage::ObITableReadInfo &read_info,
      ObDatumRow &row) final;
  virtual int exist_row(
      const ObMicroBlockData &block_data,
      const ObDatumRowkey &rowkey,
      const storage::ObITableReadInfo &read_info,
      bool &exist,
      bool &found) final;
  int locate_rowkey(const ObDatumRowkey &rowkey, int64_t &row_idx);
  virtual int get_row(
      const ObMicroBlockData &block_data,
      const ObITableReadInfo &read_info,
      const uint32_t row_idx,
      ObDatumRow &row) final;
  int get_row_id(
      const ObMicroBlockData &block_data,
      const ObDatumRowkey &rowkey,
      const ObITableReadInfo &read_info,
      int64_t &row_id) final;
protected:
  int inner_init(const ObMicroBlockData &block_data,
                 const ObITableReadInfo &read_info,
                 const ObDatumRowkey &rowkey);
private:
  int locate_rowkey_fast_path(const ObDatumRowkey &rowkey,
                              int64_t &row_idx,
                              bool &need_binary_search,
                              bool &found);
private:
  ObMicroBlockHashIndex hash_index_;
};

} //end namespace blocksstable
} //end namespace oceanbase
#endif
