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

#ifndef OCEANBASE_STORAGE_OB_PUSHDOWN_AGGREGATE_H_
#define OCEANBASE_STORAGE_OB_PUSHDOWN_AGGREGATE_H_

#include "lib/allocator/ob_allocator.h"
#include "sql/engine/expr/ob_expr.h"
#include "lib/utility/ob_hyperloglog.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "ob_aggregate_base.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObStorageDatum;
}
namespace storage
{

#define USE_GROUP_BY_MAX_DISTINCT_CNT 16384
#define USE_GROUP_BY_BUF_BLOCK_SIZE 256
#define USE_GROUP_BY_BUF_MAX_BLOCK_CNT USE_GROUP_BY_MAX_DISTINCT_CNT / USE_GROUP_BY_BUF_BLOCK_SIZE

struct ObAggCellBasicInfo
{
  ObAggCellBasicInfo(
      const int32_t col_offset,
      const int32_t col_index,
      const share::schema::ObColumnParam *col_param,
      sql::ObExpr *agg_expr,
      const int64_t batch_size,
      const bool is_padding_mode)
    : col_offset_(col_offset),
      col_index_(col_index),
      col_param_(col_param),
      agg_expr_(agg_expr),
      batch_size_(batch_size),
      is_padding_mode_(is_padding_mode)
  {}
  ~ObAggCellBasicInfo() { reset(); }
  void reset()
  {
    col_offset_ = -1;
    col_index_ = -1;
    col_param_ = nullptr;
    agg_expr_ = nullptr;
    batch_size_ = 0;
    is_padding_mode_ = false;
  }
  OB_INLINE bool is_valid() const
  {
    return col_offset_ >= 0 && nullptr != agg_expr_ && batch_size_ >= 0;
  }
  OB_INLINE bool is_padding_mode() const
  {
    return is_padding_mode_;
  }
  OB_INLINE bool need_padding() const
  {
    return is_padding_mode_ && nullptr != col_param_ && col_param_->get_meta_type().is_fixed_len_char_type();
  }
  TO_STRING_KV(K_(col_offset), K_(col_index), KPC_(col_param), K_(agg_expr), K_(batch_size), K_(is_padding_mode));
  int32_t col_offset_; // offset in projector
  int32_t col_index_; // column index
  const share::schema::ObColumnParam *col_param_;
  sql::ObExpr *agg_expr_;
  int64_t batch_size_;
  bool is_padding_mode_;
};

class ObAggCell : public ObAggCellBase
{
public:
  ObAggCell(const ObAggCellBasicInfo &basic_info, common::ObIAllocator &allocator);
  virtual ~ObAggCell() { reset(); };
  virtual void reset();
  virtual void reuse();
  virtual int init(const bool is_group_by, sql::ObEvalCtx *eval_ctx);
  // need to fill default value
  virtual int eval(
      blocksstable::ObStorageDatum &datum,
      const int64_t row_count = 1,
      const int64_t agg_row_idx = 0) = 0;
  // no need to fill default value
  virtual int eval_batch(const common::ObDatum *datums, const int64_t count) = 0;
  virtual int eval_micro_block(
      const ObTableIterParam &iter_param,
      const ObTableAccessContext &context,
      const int32_t col_offset,
      blocksstable::ObIMicroBlockReader *reader,
      const int32_t *row_ids,
      const int64_t row_count);
  virtual int eval_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg = false);
  // For group by pushdown
  virtual int eval_batch_in_group_by(
      const common::ObDatum *datums,
      const int64_t count,
      const uint32_t *refs,
      const int64_t distinct_cnt,
      const bool is_group_by_col = false,
      const bool is_default_datum = false) = 0;
  virtual int copy_output_rows(const int32_t start_offset, const int32_t end_offset);
  virtual int copy_single_output_row(sql::ObEvalCtx &ctx);
  virtual int collect_result(sql::ObEvalCtx &ctx);
  virtual int collect_batch_result_in_group_by(const int64_t distinct_cnt);
  virtual int can_use_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg, bool &can_agg);
  virtual bool need_access_data() const { return true; }
  virtual bool finished() const { return false; }
  virtual int reserve_group_by_buf(const int64_t size);
  virtual int output_extra_group_by_result(const int64_t start, const int64_t count);
  virtual int pad_column_in_group_by(const int64_t row_cap, common::ObIAllocator &allocator);
  OB_INLINE bool is_aggregated() const { return aggregated_; }
  OB_INLINE int32_t get_col_offset() const { return basic_info_.col_offset_; }
  OB_INLINE common::ObDatum *get_col_datums() const { return col_datums_; }
  OB_INLINE const sql::ObExpr *get_agg_expr() const { return basic_info_.agg_expr_; }
  OB_INLINE bool is_lob_col() const { return is_lob_col_; }
  OB_INLINE ObObjType get_obj_type() const { return basic_info_.agg_expr_->obj_meta_.get_type(); }
  OB_INLINE common::ObObjDatumMapType get_datum_map_type() const { return basic_info_.agg_expr_->obj_datum_map_; }
  OB_INLINE void set_group_by_result_cnt(const int64_t group_by_result_cnt) { group_by_result_cnt_ = group_by_result_cnt; }
  OB_INLINE bool is_padding_mode() const { return basic_info_.is_padding_mode(); }
  OB_INLINE bool need_padding() const { return basic_info_.need_padding(); }
  INHERIT_TO_STRING_KV("ObAggCellBase", ObAggCellBase, K_(basic_info), K_(def_datum), K_(is_lob_col), K_(aggregated), KP_(agg_datum_buf));
protected:
  static const int64_t DEFAULT_DATUM_OFFSET = -1;
  int fill_default_if_need(blocksstable::ObStorageDatum &datum);
  int pad_column_if_need(blocksstable::ObStorageDatum &datum, common::ObIAllocator &padding_allocator, bool alloc_need_reuse = true);
  int deep_copy_datum(const blocksstable::ObStorageDatum &src, common::ObIAllocator &tmp_alloc);
  int read_agg_datum(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg);
  void clear_group_by_info();
  OB_INLINE common::ObDatum &get_group_by_result_datum(const int32_t datum_offset)
  {
    return DEFAULT_DATUM_OFFSET == datum_offset ? result_datum_ : group_by_result_datum_buf_->at(datum_offset);
  }
  int prepare_def_datum();
  ObAggCellBasicInfo basic_info_;
  // for scalar group by pushdown
  blocksstable::ObStorageDatum def_datum_;
  bool is_lob_col_;
  bool aggregated_;
  ObAggDatumBuf *agg_datum_buf_;
  // for normal group by pushdown
  // agg col expr's datums
  common::ObDatum *col_datums_;
  // store the aggregated result
  ObGroupByExtendableBuf<ObDatum> *group_by_result_datum_buf_;
  int64_t group_by_result_cnt_;
  common::ObArenaAllocator padding_allocator_;
private:
  virtual bool can_use_index_info() const { return true; }
  DISALLOW_COPY_AND_ASSIGN(ObAggCell);
};

class ObCountAggCell : public ObAggCell
{
public:
  ObCountAggCell(
      const ObAggCellBasicInfo &basic_info,
      common::ObIAllocator &allocator,
      const bool exclude_null);
  virtual ~ObCountAggCell() { reset(); };
  virtual void reset() override;
  virtual void reuse() override;
  virtual int init(const bool is_group_by, sql::ObEvalCtx *eval_ctx) override;
  virtual int eval(
      blocksstable::ObStorageDatum &datum,
      const int64_t row_count = 1,
      const int64_t agg_row_idx = 0) override;
  virtual int eval_batch(const common::ObDatum *datums, const int64_t count) override;
  virtual int eval_micro_block(
      const ObTableIterParam &iter_param,
      const ObTableAccessContext &context,
      const int32_t col_offset,
      blocksstable::ObIMicroBlockReader *reader,
      const int32_t *row_ids,
      const int64_t row_count) override;
  virtual int eval_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg = false) override;
  virtual int eval_batch_in_group_by(
      const common::ObDatum *datums,
      const int64_t count,
      const uint32_t *refs,
      const int64_t distinct_cnt,
      const bool is_group_by_col = false,
      const bool is_default_datum = false) override;
  virtual int copy_output_rows(const int32_t start_offset, const int32_t end_offset) override;
  virtual int copy_single_output_row(sql::ObEvalCtx &ctx) override;
  virtual int collect_result(sql::ObEvalCtx &ctx) override;
  virtual int collect_batch_result_in_group_by(const int64_t distinct_cnt) override;
  virtual bool need_access_data() const override { return exclude_null_; }
  INHERIT_TO_STRING_KV("ObAggCell", ObAggCell, K_(exclude_null), K_(row_count));
private:
  bool exclude_null_;
  int64_t row_count_;
};

class ObMinAggCell : public ObAggCell
{
public:
  ObMinAggCell(const ObAggCellBasicInfo &basic_info, common::ObIAllocator &allocator);
  virtual ~ObMinAggCell() { reset(); };
  virtual void reset() override;
  virtual void reuse() override;
  virtual int init(const bool is_group_by, sql::ObEvalCtx *eval_ctx) override;
  virtual int eval(
      blocksstable::ObStorageDatum &datum,
      const int64_t row_count = 1,
      const int64_t agg_row_idx = 0) override;
  virtual int eval_batch(const common::ObDatum *datums, const int64_t count) override;
  virtual int eval_batch_in_group_by(
      const common::ObDatum *datums,
      const int64_t count,
      const uint32_t *refs,
      const int64_t distinct_cnt,
      const bool is_group_by_col = false,
      const bool is_default_datum = false) override;
  virtual int pad_column_in_group_by(const int64_t row_cap, common::ObIAllocator &allocator) override;
  INHERIT_TO_STRING_KV("ObAggCell", ObAggCell, K_(cmp_fun));
private:
  virtual bool can_use_index_info() const override
  {
    return nullptr != basic_info_.col_param_ &&
           (basic_info_.col_param_->get_meta_type().is_numeric_type() || basic_info_.col_param_->get_meta_type().is_temporal_type());
  }
  ObDatumCmpFuncType cmp_fun_;
  uint32_t *group_by_ref_array_;
  common::ObArenaAllocator datum_allocator_;
};

class ObMaxAggCell : public ObAggCell
{
public:
  ObMaxAggCell(const ObAggCellBasicInfo &basic_info, common::ObIAllocator &allocator);
  virtual ~ObMaxAggCell() { reset(); };
  virtual void reset() override;
  virtual void reuse() override;
  virtual int init(const bool is_group_by, sql::ObEvalCtx *eval_ctx) override;
  virtual int eval(
      blocksstable::ObStorageDatum &datum,
      const int64_t row_count = 1,
      const int64_t agg_row_idx = 0) override;
  virtual int eval_batch(const common::ObDatum *datums, const int64_t count) override;
  virtual int eval_batch_in_group_by(
      const common::ObDatum *datums,
      const int64_t count,
      const uint32_t *refs,
      const int64_t distinct_cnt,
      const bool is_group_by_col = false,
      const bool is_default_datum = false) override;
  virtual int pad_column_in_group_by(const int64_t row_cap, common::ObIAllocator &allocator) override;
  INHERIT_TO_STRING_KV("ObAggCell", ObAggCell, K_(cmp_fun));
private:
  virtual bool can_use_index_info() const override
  {
    return nullptr != basic_info_.col_param_ &&
           (basic_info_.col_param_->get_meta_type().is_numeric_type() || basic_info_.col_param_->get_meta_type().is_temporal_type());
  }
  ObDatumCmpFuncType cmp_fun_;
  uint32_t *group_by_ref_array_;
  common::ObArenaAllocator datum_allocator_;
};

// For statistical information aggregation pushdown.
// Not support cross-partition aggregate, not support group by.
class ObHyperLogLogAggCell : public ObAggCell
{
public:
  ObHyperLogLogAggCell(const ObAggCellBasicInfo &basic_info, common::ObIAllocator &allocator);
  virtual ~ObHyperLogLogAggCell() { reset(); }
  virtual void reset() override;
  virtual void reuse() override;
  virtual int init(const bool is_group_by, sql::ObEvalCtx *eval_ctx) override;
  virtual int eval(
      blocksstable::ObStorageDatum &datum,
      const int64_t row_count = 1,
      const int64_t agg_row_idx = 0) override;
  virtual int eval_batch(const common::ObDatum *datums, const int64_t count) override;
  virtual int eval_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg = false) override
  { return OB_NOT_SUPPORTED; }
  virtual int eval_batch_in_group_by(
      const common::ObDatum *datums,
      const int64_t count,
      const uint32_t *refs,
      const int64_t distinct_cnt,
      const bool is_group_by_col = false,
      const bool is_default_datum = false) override
  { return OB_NOT_SUPPORTED; }
  virtual int copy_output_rows(const int32_t start_offset, const int32_t end_offset) override { return OB_NOT_SUPPORTED; }
  virtual int collect_result(sql::ObEvalCtx &ctx) override;
  virtual int collect_batch_result_in_group_by(const int64_t distinct_cnt) override { return OB_NOT_SUPPORTED; }
  virtual int reserve_group_by_buf(const int64_t size) override { return OB_NOT_SUPPORTED; }
  virtual int output_extra_group_by_result(const int64_t start, const int64_t count) override { return OB_NOT_SUPPORTED; }
  INHERIT_TO_STRING_KV("ObAggCell", ObAggCell, K_(hash_func), K_(def_hash_value), KPC_(ndv_calculator));
  static const int64_t LLC_BUCKET_BITS = 10; // same as ObAggregateProcessor llc bucket bits.
private:
  virtual bool can_use_index_info() const override { return false; } // can not use now.
  sql::ObExprHashFuncType hash_func_;
  ObHyperLogLogCalculator *ndv_calculator_;
  uint64_t def_hash_value_;
};

// For statistical information aggregation pushdown.
// Not support cross-partition aggregate, not support group by.
class ObSumOpSizeAggCell : public ObAggCell
{
public:
  ObSumOpSizeAggCell(
      const ObAggCellBasicInfo &basic_info,
      common::ObIAllocator &allocator,
      const bool exclude_null);
  virtual ~ObSumOpSizeAggCell() { reset(); }
  virtual void reset() override;
  virtual void reuse() override;
  virtual int init(const bool is_group_by, sql::ObEvalCtx *eval_ctx) override;
  virtual int eval(
      blocksstable::ObStorageDatum &datum,
      const int64_t row_count = 1,
      const int64_t agg_row_idx = 0) override;
  virtual int eval_batch(const common::ObDatum *datums, const int64_t count) override;
  virtual int eval_micro_block(
      const ObTableIterParam &iter_param,
      const ObTableAccessContext &context,
      const int32_t col_offset,
      blocksstable::ObIMicroBlockReader *reader,
      const int32_t *row_ids,
      const int64_t row_count) override;
  virtual int eval_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg = false);
  virtual int eval_batch_in_group_by(
      const common::ObDatum *datums,
      const int64_t count,
      const uint32_t *refs,
      const int64_t distinct_cnt,
      const bool is_group_by_col = false,
      const bool is_default_datum = false) override
  { return OB_NOT_SUPPORTED; }
  virtual int copy_output_rows(const int32_t start_offset, const int32_t end_offset) override { return OB_NOT_SUPPORTED; }
  virtual int collect_result(sql::ObEvalCtx &ctx) override;
  virtual int collect_batch_result_in_group_by(const int64_t distinct_cnt) override { return OB_NOT_SUPPORTED; }
  virtual bool need_access_data() const override
  {
    ObObjDatumMapType type = basic_info_.agg_expr_->args_[0]->obj_datum_map_;
    return type == OBJ_DATUM_STRING || type == OBJ_DATUM_NUMBER || type == OBJ_DATUM_DECIMALINT || exclude_null_;
  }
  virtual int reserve_group_by_buf(const int64_t size) override { return OB_NOT_SUPPORTED; }
  virtual int output_extra_group_by_result(const int64_t start, const int64_t count) override { return OB_NOT_SUPPORTED; }
  INHERIT_TO_STRING_KV("ObAggCell", ObAggCell, K_(total_size), K_(op_size), K_(def_op_size), K_(exclude_null));
private:
  int set_op_size();
  int get_datum_op_size(const ObDatum &datum, int64_t &length);
  virtual bool can_use_index_info() const override { return is_fixed_length_type(); }
  OB_INLINE bool is_valid_op_size() const { return op_size_ >= 0; }
  OB_INLINE bool is_fixed_length_type() const
  {
    ObObjDatumMapType type = basic_info_.agg_expr_->args_[0]->obj_datum_map_;
    return type != OBJ_DATUM_STRING && type != OBJ_DATUM_NUMBER && type != OBJ_DATUM_DECIMALINT;
  }

  int64_t op_size_;
  int64_t def_op_size_;
  uint64_t total_size_;
  bool exclude_null_;
};

class ObSumAggCell : public ObAggCell
{
  typedef int (ObSumAggCell::*ObSumEvalAggFuncType)(const common::ObDatum &datum, const int32_t datum_offset);
  typedef int (ObSumAggCell::*ObSumEvalBatchAggFuncType)(const common::ObDatum *datums, const int64_t count);
  typedef int (ObSumAggCell::*ObSumCopyDatumFuncType)(const ObDatum &datum, ObDatum &result_datum);
public:
  ObSumAggCell(
    const ObAggCellBasicInfo &basic_info,
    common::ObIAllocator &allocator);
  virtual ~ObSumAggCell() { reset(); }
  virtual void reset() override;
  virtual void reuse() override;
  virtual int init(const bool is_group_by, sql::ObEvalCtx *eval_ctx) override;
  virtual int eval(
      blocksstable::ObStorageDatum &datum,
      const int64_t row_count = 1,
      const int64_t agg_row_idx = 0) override;
  virtual int eval_batch(const common::ObDatum *datums, const int64_t count) override;
  virtual int eval_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg = false) override;
  virtual int eval_batch_in_group_by(
      const common::ObDatum *datums,
      const int64_t count,
      const uint32_t *refs,
      const int64_t distinct_cnt,
      const bool is_group_by_col = false,
      const bool is_default_datum = false) override;
  virtual int copy_output_rows(const int32_t start_offset, const int32_t end_offset) override;
  virtual int copy_single_output_row(sql::ObEvalCtx &ctx) override;
  virtual int collect_result(sql::ObEvalCtx &ctx) override;
  virtual int collect_batch_result_in_group_by(const int64_t distinct_cnt) override;
  virtual int reserve_group_by_buf(const int64_t size) override;
  virtual int output_extra_group_by_result(const int64_t start, const int64_t count) override;
  OB_INLINE bool is_sum_use_int() const { return sum_use_int_flag_; }
  INHERIT_TO_STRING_KV("ObAggCell", ObAggCell, K_(obj_tc), K_(sum_use_int_flag), K_(num_int));
private:
  virtual bool can_use_index_info() const override;
  int init_decimal_int_func();
  OB_INLINE int16_t child_scale() const { return basic_info_.agg_expr_->args_[0]->datum_meta_.scale_; }
  template<typename RES_T>
  int eval_int(const common::ObDatum &datum, const int32_t datum_offset);
  template<typename RES_T>
  int eval_uint(const common::ObDatum &datum, const int32_t datum_offset);
  int eval_float(const common::ObDatum &datum, const int32_t datum_offset);
  int eval_double(const common::ObDatum &datum, const int32_t datum_offset);
  int eval_number(const common::ObDatum &datum, const int32_t datum_offset);
  int eval_vector(const common::ObDatum &datum, const int32_t datum_offset);
  template<typename RES_T>
  int eval_number_decimal_int(const common::ObDatum &datum, const int32_t datum_offset);
  int init_eval_skip_index_func_for_decimal();
  template<typename RES_T, typename ARG_T>
  int eval_decimal_int(const common::ObDatum &datum, const int32_t datum_offset);
  template<typename ARG_T>
  int eval_decimal_int_number(const common::ObDatum &datum, const int32_t datum_offset);
  template<typename RES_T>
  int eval_int_batch(const common::ObDatum *datums, const int64_t count);
  template<typename RES_T>
  int eval_uint_batch(const common::ObDatum *datums, const int64_t count);
  int eval_float_batch(const common::ObDatum *datums, const int64_t count);
  int eval_double_batch(const common::ObDatum *datums, const int64_t count);
  int eval_number_batch(const common::ObDatum *datums, const int64_t count);
  int eval_vector_batch(const common::ObDatum *datums, const int64_t count);
  template<typename RES_T, typename CALC_T, typename ARG_T>
  int eval_decimal_int_batch(const common::ObDatum *datums, const int64_t count);
  template<typename CALC_T, typename ARG_T>
  int eval_decimal_int_number_batch(const common::ObDatum *datums, const int64_t count);
  template<typename RES_T>
  OB_INLINE int eval_int_inner(const common::ObDatum &datum, ObDataBuffer &alloc, const int32_t datum_offset = -1);
  template<>
  OB_INLINE int eval_int_inner<number::ObNumber>(const common::ObDatum &datum, ObDataBuffer &alloc, const int32_t datum_offset);
  template<typename RES_T>
  OB_INLINE int eval_uint_inner(const common::ObDatum &datum, ObDataBuffer &alloc, const int32_t datum_offset = -1);
  template<>
  OB_INLINE int eval_uint_inner<number::ObNumber>(const common::ObDatum &datum, ObDataBuffer &alloc, const int32_t datum_offset);
  OB_INLINE int eval_float_inner(const common::ObDatum &datum, const int32_t datum_offset = -1);
  OB_INLINE int eval_double_inner(const common::ObDatum &datum, const int32_t datum_offset = -1);
  int copy_int_to_number(const ObDatum &datum, ObDatum &result_datum);
  template<typename RES_T>
  int copy_int_to_decimal_int(const ObDatum &datum, ObDatum &result_datum);
  int copy_uint_to_number(const ObDatum &datum, ObDatum &result_datum);
  template<typename RES_T>
  int copy_uint_to_decimal_int(const ObDatum &datum, ObDatum &result_datum);
  int copy_float(const ObDatum &datum, ObDatum &result_datum);
  int copy_double(const ObDatum &datum, ObDatum &result_datum);
  int copy_number(const ObDatum &datum, ObDatum &result_datum);
  int copy_vector(const ObDatum &datum, ObDatum &result_datum);
  template<typename RES_T, typename ARG_T>
  int copy_decimal_int(const ObDatum &datum, ObDatum &result_datum);
  template<typename ARG_T>
  int copy_decimal_int_to_number(const ObDatum &datum, ObDatum &result_datum);
  int collect_result_in_group_by(const int64_t datum_offset);
  int collect_result_to_decimal_int(
      const int128_t &right_nmb,
      const common::ObDatum &datum,
      common::ObDatum &result);
  void clear_group_by_info();
  void reset_aggregate_info();
  ObObjTypeClass obj_tc_;
  bool sum_use_int_flag_;
  bool is_sum_use_temp_buf_;
  union {
    int64_t num_int_;
    uint64_t num_uint_;
  };
  ObGroupByExtendableBuf<bool> *sum_use_int_flag_buf_;
  union {
    ObGroupByExtendableBuf<int64_t> *num_int_buf_;
    ObGroupByExtendableBuf<uint64_t> *num_uint_buf_;
    void *num_buf_;
  };
  ObSumEvalAggFuncType eval_func_;
  ObSumEvalBatchAggFuncType eval_batch_func_;
  ObSumCopyDatumFuncType copy_datum_func_;
  ObSumEvalAggFuncType eval_skip_index_func_;
  blocksstable::ObStorageDatum cast_datum_;
  char *sum_temp_buffer_;
  char *cast_temp_buffer_;
  common::ObArenaAllocator datum_allocator_;
};

// mysql compatibility, select a,count(a), output first value of a
class ObFirstRowAggCell : public ObAggCell
{
public:
  ObFirstRowAggCell(const ObAggCellBasicInfo &basic_info, common::ObIAllocator &allocator);
  virtual ~ObFirstRowAggCell() { reset(); };
  virtual void reset();
  virtual void reuse();
  virtual int init(const bool is_group_by, sql::ObEvalCtx *eval_ctx) override;
  virtual int eval(
      blocksstable::ObStorageDatum &datum,
      const int64_t row_count = 1,
      const int64_t agg_row_idx = 0) override;
  virtual int eval_batch(const common::ObDatum *datums, const int64_t count) override
  {
    UNUSEDx(datums, count);
    return OB_NOT_SUPPORTED;
  }
  virtual int eval_micro_block(
      const ObTableIterParam &iter_param,
      const ObTableAccessContext &context,
      const int32_t col_offset,
      blocksstable::ObIMicroBlockReader *reader,
      const int32_t *row_ids,
      const int64_t row_count) override;
  virtual int eval_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg = false) override;
  virtual int eval_batch_in_group_by(
      const common::ObDatum *datums,
      const int64_t count,
      const uint32_t *refs,
      const int64_t distinct_cnt,
      const bool is_group_by_col = false,
      const bool is_default_datum = false) override;
  virtual int copy_output_rows(const int32_t start_offset, const int32_t end_offset) override
  {
    UNUSEDx(start_offset, end_offset);
    return OB_SUCCESS;
  }
  virtual int copy_single_output_row(sql::ObEvalCtx &ctx) override
  {
    UNUSED(ctx);
    return OB_SUCCESS;
  }
  virtual int collect_result(sql::ObEvalCtx &ctx) override;
  virtual int collect_batch_result_in_group_by(const int64_t distinct_cnt) override;
  virtual bool need_access_data() const override { return !finished(); }
  virtual bool finished() const override { return aggregated_; }
  virtual int reserve_group_by_buf(const int64_t size) override;
  virtual int output_extra_group_by_result(const int64_t start, const int64_t count) override;
  virtual int can_use_index_info(const blocksstable::ObMicroIndexInfo &index_info,
    const bool is_cg, bool &can_agg) override;
  OB_INLINE void set_determined_value()
  {
    is_determined_value_ = true;
    result_datum_.reuse();
    result_datum_.set_null();
    aggregated_ = true;
  }
  INHERIT_TO_STRING_KV("ObAggCell", ObAggCell, K_(is_determined_value), K_(aggregated_flag_cnt));
private:
  virtual bool can_use_index_info() const override { return finished(); }
  void clear_group_by_info();
  bool is_determined_value_;
  int64_t aggregated_flag_cnt_;
  ObGroupByExtendableBuf<bool> *aggregated_flag_buf_;
  common::ObArenaAllocator datum_allocator_;
};

class ObPDAggFactory
{
public:
  ObPDAggFactory(common::ObIAllocator &allocator) : allocator_ (allocator) {}
  ~ObPDAggFactory() {}

  int alloc_cell(
      const ObAggCellBasicInfo &basic_info,
      common::ObIArray<ObAggCell*> &agg_cells,
      const bool exclude_null = false,
      const bool is_group_by = false,
      sql::ObEvalCtx *eval_ctx = nullptr);
  void release(common::ObIArray<ObAggCell*> &agg_cells);
private:
  DISALLOW_COPY_AND_ASSIGN(ObPDAggFactory);
  common::ObIAllocator &allocator_;
};

class ObGroupByCell : public ObGroupByCellBase
{
public:
  ObGroupByCell(const int64_t batch_size, common::ObIAllocator &allocator);
  ~ObGroupByCell() { reset(); }
  void reset() override;
  void reuse() override;
  int init(const ObTableAccessParam &param, const ObTableAccessContext &context, sql::ObEvalCtx &eval_ctx) override;
  int init_for_single_row(const ObTableAccessParam &param, const ObTableAccessContext &context, sql::ObEvalCtx &eval_ctx);
  // do group by for aggregate cell indicated by 'agg_idx'
  // datums: batch of datums of this column
  // count: batch size
  // agg_idx: aggregate index in 'agg_cells_'
  // is_group_by_col: true if current column is group by column
  // is_default_datum: true if current column is new added column
  // ref_offset: for column store, may do 'eval_batch' multiple times for one batch
  int eval_batch(
      common::ObDatum *datums,
      const int64_t count,
      const int32_t agg_idx,
      const bool is_group_by_col = false,
      const bool is_default_datum = false,
      const uint32_t ref_offset = 0) override;
  // copy row/rows from output to aggregate
  // in the case where can not do batch scan or can not do group by pushdown
  int copy_output_row(const int64_t batch_idx, const ObTableIterParam &iter_param) override;
  int copy_output_rows(const int64_t batch_idx, const ObTableIterParam &iter_param) override;
  int copy_single_output_row(sql::ObEvalCtx &ctx) override;
  int pad_column_in_group_by(const int64_t row_cap);
  int collect_result() override;
  int add_distinct_null_value() override;
  // for micro with bitmap, should extract distinct values according bitmap
  int extract_distinct() override;
  int output_extra_group_by_result(int64_t &count, const ObTableIterParam &iter_param) override;
  // for column store, assign aggregate cells to column group scanner(ObCGGroupByScanner)
  int assign_agg_cells(const sql::ObExpr *col_expr, common::ObIArray<int32_t> &agg_idxs) override;
  OB_INLINE bool is_exceed_sql_batch() const override { return group_by_col_datum_buf_->is_use_extra_buf(); }
  OB_INLINE common::ObDatum *get_group_by_col_datums_to_fill() override
  { return need_extract_distinct_ ? tmp_group_by_datum_buf_->get_group_by_datums() : group_by_col_datum_buf_->get_group_by_datums(); }
  OB_INLINE const char **get_cell_datas() override
  { return need_extract_distinct_ ? tmp_group_by_datum_buf_->get_group_by_cell_datas() : group_by_col_datum_buf_->get_group_by_cell_datas(); }
  OB_INLINE common::ObDatum *get_group_by_col_datums() const { return group_by_col_datum_buf_->get_group_by_datums(); }
  OB_INLINE common::ObIArray<ObAggCell*> &get_agg_cells() { return agg_cells_; }
  OB_INLINE bool need_read_reference() const { return need_extract_distinct_ || agg_cells_.count() > 0; }
  OB_INLINE bool need_do_aggregate() const { return agg_cells_.count() > 0; }

  DECLARE_TO_STRING;
protected:
  int prepare_tmp_group_by_buf(const int64_t size) override;
  // for case the count of distinct values exceed sql batch size
  int reserve_group_by_buf(const int64_t size) override;
private:
  int init_agg_cells(const ObTableAccessParam &param, const ObTableAccessContext &context, sql::ObEvalCtx &eval_ctx, const bool is_for_single_row);
  ObAggGroupByDatumBuf *group_by_col_datum_buf_;
  // for micro with bitmap
  // first read all the distinct values into this buffer
  // then extract actual distinct values according bitmap
  ObAggGroupByDatumBuf *tmp_group_by_datum_buf_;
  // aggregate cells
  common::ObSEArray<ObAggCell*, DEFAULT_AGG_CELL_CNT> agg_cells_;
  ObAggDatumBuf *agg_datum_buf_;
  ObPDAggFactory agg_cell_factory_;
  DISALLOW_COPY_AND_ASSIGN(ObGroupByCell);
};

class ObAggSelector
{
public:
  ObAggSelector(const uint16_t count) : count_(count) {}
  ~ObAggSelector() {}

  bool is_valid() const { return count_ != 0; }
  uint16_t begin() const { return 0; }
  uint16_t end() const { return count_; }
  uint16_t next(uint16_t &i) const { return ++i; }
  uint16_t get_batch_index(uint16_t i) const { return i; }
private:
  uint16_t count_;
};

class ObAggSource
{
public:
  ObAggSource(const ObDatum *datums) : datums_(datums) {}
  ~ObAggSource() {}
  const common::ObDatum *at(const int64_t i) const { return &datums_[i]; }
private:
  const ObDatum *datums_;
};

#define DATUM_TO_DECIMAL_INT(datum, type) \
  *const_cast<type *>(reinterpret_cast<const type*>(datum.get_decimal_int()))

#define DATUM_TO_CONST_DECIMAL_INT(datum, type) \
  *reinterpret_cast<const type*>(datum.get_decimal_int())

template<typename RES_T>
OB_INLINE int ObSumAggCell::eval_int_inner(const common::ObDatum &datum, ObDataBuffer &alloc, const int32_t datum_offset)
{
  UNUSED(alloc);
  int ret = OB_SUCCESS;
  if (datum.is_null()) {
  } else {
    common::ObDatum &result_datum = get_group_by_result_datum(datum_offset);
    int64_t &datum_int = DEFAULT_DATUM_OFFSET == datum_offset ? num_int_ : num_int_buf_->at(datum_offset);
    bool &sum_use_int_flag =  DEFAULT_DATUM_OFFSET == datum_offset ? sum_use_int_flag_ : sum_use_int_flag_buf_->at(datum_offset);

    int64_t new_int = datum.get_int();
    int64_t sum_int = datum_int + new_int;
    if (sql::ObExprAdd::is_int_int_out_of_range(datum_int, new_int, sum_int)) {
      LOG_DEBUG("int64_t add overflow, will use decimal int", K(datum_int), K(new_int), K(sum_int));
      if (OB_UNLIKELY(result_datum.is_null())) {
        DATUM_TO_DECIMAL_INT(result_datum, RES_T) = datum_int;
        DATUM_TO_DECIMAL_INT(result_datum, RES_T) += new_int;
        result_datum.pack_ = sizeof(RES_T);
      } else {
        DATUM_TO_DECIMAL_INT(result_datum, RES_T) =
          DATUM_TO_CONST_DECIMAL_INT(result_datum, RES_T) + datum_int + new_int;
      }
      datum_int = 0;
    } else {
      LOG_DEBUG("int64_t add does not overflow", K(datum_int), K(new_int), K(sum_int));
      datum_int = sum_int;
    }
    sum_use_int_flag = true;
  }
  return ret;
}

template<>
OB_INLINE int ObSumAggCell::eval_int_inner<number::ObNumber>(const common::ObDatum &datum,
                                                             ObDataBuffer &alloc,
                                                             const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  if (datum.is_null()) {
  } else {
    common::ObDatum &result_datum = get_group_by_result_datum(datum_offset);
    int64_t &datum_int = DEFAULT_DATUM_OFFSET == datum_offset ? num_int_ : num_int_buf_->at(datum_offset);
    bool &sum_use_int_flag =  DEFAULT_DATUM_OFFSET == datum_offset ? sum_use_int_flag_ : sum_use_int_flag_buf_->at(datum_offset);

    int64_t new_int = datum.get_int();
    int64_t sum_int = datum_int + new_int;
    if (sql::ObExprAdd::is_int_int_out_of_range(datum_int, new_int, sum_int)) {
      LOG_DEBUG("int64_t add overflow, will use number", K(datum_int), K(new_int), K(sum_int));
      common::number::ObNumber result_nmb;
      if (!result_datum.is_null()) {
        common::number::ObCompactNumber &cnum = const_cast<common::number::ObCompactNumber &>(result_datum.get_number());
        result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
      }
      if (OB_FAIL(result_nmb.add(datum_int, new_int, result_nmb, alloc))) {
        LOG_WARN("number add failed", K(ret));
      } else {
        result_datum.set_number(result_nmb);
        datum_int = 0;
        alloc.free();
      }
    } else {
      LOG_DEBUG("int64_t add does not overflow", K(datum_int), K(new_int), K(sum_int), K(datum_offset));
      datum_int = sum_int;
    }
    sum_use_int_flag = true;
  }
  return ret;
}

template<typename RES_T>
OB_INLINE int ObSumAggCell::eval_uint_inner(const common::ObDatum &datum, ObDataBuffer &alloc, const int32_t datum_offset)
{
  UNUSED(alloc);
  int ret = OB_SUCCESS;
  if (datum.is_null()) {
  } else {
    common::ObDatum &result_datum = get_group_by_result_datum(datum_offset);
    uint64_t &datum_uint = DEFAULT_DATUM_OFFSET == datum_offset ? num_uint_ : num_uint_buf_->at(datum_offset);
    bool &sum_use_int_flag =  DEFAULT_DATUM_OFFSET == datum_offset ? sum_use_int_flag_ : sum_use_int_flag_buf_->at(datum_offset);

    uint64_t new_uint = datum.get_uint();
    uint64_t sum_uint = datum_uint + new_uint;
    if (sql::ObExprAdd::is_uint_uint_out_of_range(datum_uint, new_uint, sum_uint)) {
      LOG_DEBUG("uint64_t add overflow, will use number", K(datum_uint), K(new_uint), K(sum_uint));
      if (OB_UNLIKELY(result_datum_.is_null())) {
        DATUM_TO_DECIMAL_INT(result_datum_, RES_T) = datum_uint;
        DATUM_TO_DECIMAL_INT(result_datum_, RES_T) += new_uint;
        result_datum_.pack_ = sizeof(RES_T);
      } else {
        DATUM_TO_DECIMAL_INT(result_datum_, RES_T) =
          DATUM_TO_CONST_DECIMAL_INT(result_datum_, RES_T) + datum_uint + new_uint;
      }
      datum_uint = 0;
    } else {
      LOG_DEBUG("uint64_t add does not overflow", K(datum_uint), K(new_uint), K(sum_uint), K(datum_offset));
      datum_uint = sum_uint;
    }
    sum_use_int_flag = true;
  }
  return ret;
}

template<>
OB_INLINE int ObSumAggCell::eval_uint_inner<number::ObNumber>(const common::ObDatum &datum,
                                                              ObDataBuffer &alloc,
                                                              const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  if (datum.is_null()) {
  } else {
    common::ObDatum &result_datum = get_group_by_result_datum(datum_offset);
    uint64_t &datum_uint = DEFAULT_DATUM_OFFSET == datum_offset ? num_uint_ : num_uint_buf_->at(datum_offset);
    bool &sum_use_int_flag =  DEFAULT_DATUM_OFFSET == datum_offset ? sum_use_int_flag_ : sum_use_int_flag_buf_->at(datum_offset);

    uint64_t new_uint = datum.get_uint();
    uint64_t sum_uint = datum_uint + new_uint;
    if (sql::ObExprAdd::is_uint_uint_out_of_range(datum_uint, new_uint, sum_uint)) {
      LOG_DEBUG("uint64_t add overflow, will use number", K(datum_uint), K(new_uint), K(sum_uint));
      common::number::ObNumber result_nmb;
      if (!result_datum.is_null()) {
        common::number::ObCompactNumber &cnum = const_cast<common::number::ObCompactNumber &>(result_datum.get_number());
        result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
      }
      if (OB_FAIL(result_nmb.add(datum_uint, new_uint, result_nmb, alloc))) {
        LOG_WARN("number add failed", K(ret));
      } else {
        result_datum.set_number(result_nmb);
        datum_uint = 0;
        alloc.free();
      }
    } else {
      LOG_DEBUG("uint64_t add does not overflow", K(datum_uint), K(new_uint), K(sum_uint));
      datum_uint = sum_uint;
    }
    sum_use_int_flag = true;
  }
  return ret;
}

OB_INLINE int ObSumAggCell::eval_float_inner(const common::ObDatum &datum, const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  common::ObDatum &result_datum = get_group_by_result_datum(datum_offset);
  if (datum.is_null()) {
  } else if (result_datum.is_null()) {
    result_datum.set_float(datum.get_float());
  } else {
    float left_f = result_datum.get_float();
    float right_f = datum.get_float();
    if (OB_UNLIKELY(sql::ObArithExprOperator::is_float_out_of_range(left_f + right_f))
        && !lib::is_oracle_mode()) {
      ret = OB_OPERATE_OVERFLOW;
      char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
      int64_t pos = 0;
      databuff_printf(expr_str,
                      OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                      pos,
                      "'(%e + %e)'", left_f, right_f);
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "BINARY_FLOAT", expr_str);
      LOG_WARN("float out of range", K(left_f), K(right_f));
    } else {
      result_datum.set_float(left_f + right_f);
    }
  }
  return ret;
}

OB_INLINE int ObSumAggCell::eval_double_inner(const common::ObDatum &datum, const int32_t datum_offset)
{
  int ret = OB_SUCCESS;
  common::ObDatum &result_datum = get_group_by_result_datum(datum_offset);
  if (datum.is_null()) {
  } else if (result_datum.is_null()) {
    result_datum.set_double(datum.get_double());
  } else {
    double left_d = result_datum.get_double();
    double right_d = datum.get_double();
    result_datum.set_double(left_d + right_d);
  }
  return ret;
}

}
}

#endif // OCEANBASE_STORAGE_OB_PUSHDOWN_AGGREGATE_H_
