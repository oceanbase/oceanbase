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

#ifndef OCEANBASE_STORAGE_OB_PUSHDOWN_AGGREGATE_VEC_H_
#define OCEANBASE_STORAGE_OB_PUSHDOWN_AGGREGATE_VEC_H_

#include "share/aggregate/agg_ctx.h"
#include "ob_aggregate_base.h"

namespace oceanbase
{
namespace storage
{
struct ObAggCellVecBasicInfo
{
  ObAggCellVecBasicInfo(
      share::aggregate::RuntimeContext &agg_ctx,
      ObCompactRow **&rows,
      const RowMeta &row_meta,
      const ObBatchRows &brs,
      const int32_t col_offset,
      const share::schema::ObColumnParam *col_param,
      const bool is_padding_mode)
        : agg_ctx_(agg_ctx),
          rows_(rows),
          row_meta_(row_meta),
          brs_(brs),
          col_param_(col_param),
          col_offset_(col_offset),
          is_padding_mode_(is_padding_mode)
  {}
  void reset()
  {
    col_param_ = nullptr;
    rows_ = nullptr;
    is_padding_mode_ = false;
  }
  OB_INLINE bool is_padding_mode() const
  {
    return is_padding_mode_;
  }
  OB_INLINE bool need_padding() const
  {
    return is_padding_mode_ && nullptr != col_param_ && col_param_->get_meta_type().is_fixed_len_char_type();
  }
  TO_STRING_KV(KPC_(col_param), K_(rows), K_(row_meta), K_(brs), K_(col_offset), K_(is_padding_mode));
  share::aggregate::RuntimeContext &agg_ctx_;
  ObCompactRow **&rows_;
  const RowMeta &row_meta_;
  const ObBatchRows &brs_;
  const share::schema::ObColumnParam *col_param_;
  const int32_t col_offset_;
  bool is_padding_mode_;
};

class ObAggCellVec : public ObAggCellBase
{
public:
  ObAggCellVec(const int64_t agg_idx,
               const ObAggCellVecBasicInfo &basic_info,
               common::ObIAllocator &allocator);
  virtual ~ObAggCellVec();
  virtual void reset();
  virtual void reuse();
  virtual int init();
  virtual int eval(blocksstable::ObStorageDatum &datum,
                   const int64_t row_count = 1,
                   int64_t agg_row_idx = 0);
  virtual int eval_batch(blocksstable::ObIMicroBlockReader *reader,
                         const int32_t col_offset,
                         const int32_t *row_ids,
                         const int64_t row_count,
                         const int64_t row_offset = 0,
                         const int64_t agg_row_idx = 0);
  virtual int eval_index_info(const blocksstable::ObMicroIndexInfo &index_info,
                              const bool is_cg,
                              const int64_t agg_row_idx = 0);
  virtual int eval_batch_in_group_by(common::ObDatum *datums,
                                     const int64_t count,
                                     const uint32_t *refs,
                                     const int64_t distinct_cnt,
                                     const bool is_group_by_col = false,
                                     const bool is_default_datum = false);
  virtual int collect_result(const bool fill_output,
                             const sql::ObExpr *group_by_col_expr = nullptr,
                             const int32_t start_ouput_idx = 0,
                             const int32_t start_row_idx = 0,
                             const int32_t batch_size = 1);
  virtual int copy_output_rows(const int32_t start_offset, const int32_t end_offset);
  virtual int can_use_index_info(const blocksstable::ObMicroIndexInfo &index_info,
                                 const int32_t col_index, bool &can_agg);
  OB_INLINE virtual bool can_pushdown_decoder(blocksstable::ObIMicroBlockReader *reader,
                                              const int32_t col_offset,
                                              const int32_t *row_ids,
                                              const int64_t row_count) const
  { return reader->can_pushdown_decoder(*basic_info_.col_param_, col_offset, row_ids, row_count, *this); }
  OB_INLINE virtual bool need_access_data() const { return true; }
  OB_INLINE virtual bool need_get_row_ids() const { return true; }
  OB_INLINE int64_t get_agg_idx() const { return agg_idx_; }
  OB_INLINE bool need_padding() const { return basic_info_.need_padding(); }
  OB_INLINE bool is_padding_mode() const { return basic_info_.is_padding_mode(); }
  OB_INLINE const share::schema::ObColumnParam *get_col_param() const { return basic_info_.col_param_; }
  OB_INLINE int32_t get_col_offset() const { return basic_info_.col_offset_; }
  OB_INLINE sql::ObExpr *get_agg_expr() const
  {
    const sql::ObAggrInfo &agg_info = basic_info_.agg_ctx_.aggr_infos_.at(agg_idx_);
    return agg_info.expr_;
  }
  OB_INLINE sql::ObExpr *get_project_expr() const
  {
    const sql::ObAggrInfo &agg_info = basic_info_.agg_ctx_.aggr_infos_.at(agg_idx_);
    return agg_info.expr_->args_[0];
  }
  OB_INLINE ObObjType get_obj_type() const
  {
    const sql::ObAggrInfo &agg_info = basic_info_.agg_ctx_.aggr_infos_.at(agg_idx_);
    return agg_info.expr_->obj_meta_.get_type();
  }
  int get_def_datum(const blocksstable::ObStorageDatum *&default_datum);

  INHERIT_TO_STRING_KV("ObAggCellBase", ObAggCellBase, K_(agg_idx), K_(basic_info), KP_(aggregate));
protected:
  int read_agg_datum(const blocksstable::ObMicroIndexInfo &index_info, const int32_t col_index);
  int pad_column_if_need(blocksstable::ObStorageDatum &datum);
  int fill_output_expr_if_need(sql::ObExpr *output_expr,
                               const sql::ObExpr *group_by_col_expr,
                               sql::ObEvalCtx &eval_ctx,
                               const int32_t batch_size);
  OB_INLINE virtual bool can_use_index_info() const { return true; }
  OB_INLINE bool is_lob_col()
  {
    bool bret = false;
    // "nullptr == col_param_" means COUNT(*), COUNT(*) ignore log judgement, return false.
    if (nullptr != basic_info_.col_param_) {
      bret = basic_info_.col_param_->get_meta_type().is_lob_storage();
    }
    return bret;
  }

  int64_t agg_idx_;
  ObAggCellVecBasicInfo basic_info_;
  share::aggregate::IAggregate* aggregate_;
  common::ObArenaAllocator padding_allocator_;
  blocksstable::ObStorageDatum default_datum_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAggCellVec);
};

class ObCountAggCellVec final : public ObAggCellVec
{
public:
  ObCountAggCellVec(const int64_t agg_idx,
                    const ObAggCellVecBasicInfo &basic_info,
                    common::ObIAllocator &allocator,
                    const bool exclude_null);
  int eval(blocksstable::ObStorageDatum &datum,
           const int64_t row_count = 1,
           const int64_t agg_row_idx = 0) override;
  int eval_batch(blocksstable::ObIMicroBlockReader *reader,
                 const int32_t col_offset,
                 const int32_t *row_ids,
                 const int64_t row_count,
                 const int64_t row_offset = 0,
                 const int64_t agg_row_idx = 0) override;
  int eval_index_info(const blocksstable::ObMicroIndexInfo &index_info,
                      const bool is_cg,
                      const int64_t agg_row_idx = 0) override;
  int eval_batch_in_group_by(common::ObDatum *datums,
                             const int64_t count,
                             const uint32_t *refs,
                             const int64_t distinct_cnt,
                             const bool is_group_by_col = false,
                             const bool is_default_datum = false) override;
  int can_use_index_info(const blocksstable::ObMicroIndexInfo &index_info,
                                 const int32_t col_index, bool &can_agg) override;
  int copy_output_rows(const int32_t start_offset, const int32_t end_offset) override;
  bool can_pushdown_decoder(blocksstable::ObIMicroBlockReader *reader,
                            const int32_t col_offset,
                            const int32_t *row_ids,
                            const int64_t row_count) const override
  {
    UNUSEDx(reader, col_offset, row_ids, row_count);
    return true;
  }
  OB_INLINE bool need_access_data() const override { return false; }
  OB_INLINE bool need_get_row_ids() const override { return exclude_null_; }
  INHERIT_TO_STRING_KV("ObAggCellVec", ObAggCellVec, K_(exclude_null));
private:
  bool exclude_null_;
};

class ObMaxAggCellVec final : public ObAggCellVec
{
public:
  ObMaxAggCellVec(const int64_t agg_idx,
                  const ObAggCellVecBasicInfo &basic_info,
                  common::ObIAllocator &allocator);
};

class ObMinAggCellVec final : public ObAggCellVec
{
public:
  ObMinAggCellVec(const int64_t agg_idx,
                  const ObAggCellVecBasicInfo &basic_info,
                  common::ObIAllocator &allocator);
};

class ObSumAggCellVec final : public ObAggCellVec
{
public:
  ObSumAggCellVec(const int64_t agg_idx,
                  const ObAggCellVecBasicInfo &basic_info,
                  common::ObIAllocator &allocator);
  void reuse() override;
  int eval(blocksstable::ObStorageDatum &datum,
           const int64_t row_count = 1,
           const int64_t agg_row_idx = 0) override;
  int eval_index_info(const blocksstable::ObMicroIndexInfo &index_info,
                      const bool is_cg,
                      const int64_t agg_row_idx = 0) override;
  bool can_pushdown_decoder(blocksstable::ObIMicroBlockReader *reader,
                            const int32_t col_offset,
                            const int32_t *row_ids,
                            const int64_t row_count) const override
  {
    UNUSEDx(reader, col_offset, row_ids, row_count);
    return false;
  }
  INHERIT_TO_STRING_KV("ObAggCellVec", ObAggCellVec, K_(cast_datum));
protected:
  OB_INLINE bool can_use_index_info() const override
  { return nullptr != basic_info_.col_param_ && basic_info_.col_param_->get_meta_type().is_numeric_type(); }
private:
  blocksstable::ObStorageDatum cast_datum_;
};

class ObHyperLogLogAggCellVec final : public ObAggCellVec
{
public:
  ObHyperLogLogAggCellVec(const int64_t agg_idx,
                          const ObAggCellVecBasicInfo &basic_info,
                          common::ObIAllocator &allocator);
protected:
  OB_INLINE bool can_use_index_info() const override { return false; }
};

class ObSumOpNSizeAggCellVec final : public ObAggCellVec
{
public:
  ObSumOpNSizeAggCellVec(const int64_t agg_idx,
                        const ObAggCellVecBasicInfo &basic_info,
                        common::ObIAllocator &allocator,
                        const bool exclude_null);
  int init() override;
  int eval(blocksstable::ObStorageDatum &datum,
           const int64_t row_count = 1,
           const int64_t agg_row_idx = 0) override;
  int eval_batch(blocksstable::ObIMicroBlockReader *reader,
                 const int32_t col_offset,
                 const int32_t *row_ids,
                 const int64_t row_count,
                 const int64_t row_offset = 0,
                 const int64_t agg_row_idx = 0) override;
  int eval_index_info(const blocksstable::ObMicroIndexInfo &index_info,
                      const bool is_cg,
                      const int64_t agg_row_idx = 0) override;
  int can_use_index_info(const blocksstable::ObMicroIndexInfo &index_info,
                         const int32_t col_index, bool &can_agg) override;
  OB_INLINE bool need_access_data() const override
  {
    return !is_fixed_length_type();
  }
  OB_INLINE bool need_get_row_ids() const override
  {
    return exclude_null_ || !is_fixed_length_type();
  }
  INHERIT_TO_STRING_KV("ObAggCellVec", ObAggCellVec, K_(op_nsize), K_(exclude_null));
protected:
  int set_op_nsize();
  int get_datum_op_nsize(blocksstable::ObStorageDatum &datum, int64_t &length);
  OB_INLINE bool can_use_index_info() const override { return is_fixed_length_type(); }
  OB_INLINE bool is_fixed_length_type() const
  {
    const sql::ObExpr *proj_expr = get_project_expr();
    ObObjDatumMapType type = proj_expr->obj_datum_map_;
    return type != OBJ_DATUM_STRING && type != OBJ_DATUM_NUMBER && type != OBJ_DATUM_DECIMALINT;
  }
private:
  int64_t op_nsize_;
  bool exclude_null_;
};

class ObRbBuildAggCellVec final : public ObAggCellVec
{
public:
  ObRbBuildAggCellVec(const int64_t agg_idx,
                  const ObAggCellVecBasicInfo &basic_info,
                  common::ObIAllocator &allocator);
protected:
  OB_INLINE bool can_use_index_info() const override { return false; }
};

class ObPDAggVecFactory
{
public:
  ObPDAggVecFactory(common::ObIAllocator &allocator) : allocator_(allocator) {}
  ~ObPDAggVecFactory() {}
  int alloc_cell(
      const ObAggCellVecBasicInfo &basic_info,
      ObAggCellVec *&agg_cell,
      const int64_t agg_idx,
      const bool exclude_null);
  void release(common::ObIArray<ObAggCellVec *> &agg_cells);
private:
  common::ObIAllocator &allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObPDAggVecFactory);
};

class ObGroupByCellVec : public ObGroupByCellBase
{
public:
  ObGroupByCellVec(
      const int64_t batch_size,
      sql::ObEvalCtx &eval_ctx,
      sql::ObBitVector *skip_bit,
      common::ObIAllocator &allocator);
  virtual ~ObGroupByCellVec();
  void reset() override;
  void reuse() override;
  int init(const ObTableAccessParam &param, const ObTableAccessContext &context, sql::ObEvalCtx &eval_ctx) override;
  int eval_batch(
      common::ObDatum *datums,
      const int64_t count,
      const int32_t agg_idx,
      const bool is_group_by_col = false,
      const bool is_default_datum = false,
      const uint32_t ref_offset = 0) override;
  int copy_output_row(const int64_t batch_idx, const ObTableIterParam &iter_param) override;
  int copy_output_rows(const int64_t batch_idx, const ObTableIterParam &iter_param) override;
  int copy_single_output_row(sql::ObEvalCtx &ctx) override
  { return OB_NOT_SUPPORTED; }
  int collect_result() override
  { return OB_SUCCESS; }
  int add_distinct_null_value() override;
  int extract_distinct() override;
  int output_extra_group_by_result(int64_t &count, const ObTableIterParam &iter_param) override;
  int pad_column_in_group_by(const int64_t row_cap);
  int assign_agg_cells(const sql::ObExpr *col_expr, common::ObIArray<int32_t> &agg_idxs) override;
  OB_INLINE common::ObIArray<ObAggCellVec *> &get_agg_cells() { return agg_cells_; }
  OB_INLINE common::ObDatum *get_group_by_col_datums_to_fill() override
  { return need_extract_distinct_ ? tmp_group_by_datum_buf_->get_datums() : group_by_col_datum_buf_->get_datums(); }
  OB_INLINE const char **get_cell_datas() override
  { return need_extract_distinct_ ? tmp_group_by_datum_buf_->get_cell_datas() : group_by_col_datum_buf_->get_cell_datas(); }
  OB_INLINE common::ObDatum *get_group_by_col_datums() const override
  { return group_by_col_datum_buf_->get_datums(); }
  OB_INLINE bool need_read_reference() const { return need_extract_distinct_ || agg_cells_.count() > 0; }
  OB_INLINE bool need_do_aggregate() const { return agg_cells_.count() > 0; }
  OB_INLINE ObAggCellVec *get_sorted_cell(const int64_t idx)
  {
    const int64_t agg_idx = pd_agg_ctx_.cols_offset_map_.at(idx).agg_idx_;
    return agg_cells_.at(agg_idx);
  }
  int init_vector_header(const sql::ObExprPtrIArray *agg_exprs, const bool init_group_by_col);

  INHERIT_TO_STRING_KV("ObGroupByCellBase", ObGroupByCellBase,
                       K_(pd_agg_ctx),
                       K_(group_by_col_datum_buf),
                       K_(tmp_group_by_datum_buf),
                       K_(agg_cells));
protected:
  int prepare_tmp_group_by_buf(const int64_t size) override;
  int reserve_group_by_buf(const int64_t size) override;
private:
  int init_agg_cells(const ObTableAccessParam &param,
                     const ObTableAccessContext &context,
                     sql::ObEvalCtx &eval_ctx,
                     const bool is_for_single_row);
  ObPushdownAggContext pd_agg_ctx_;
  ObAggDatumBuf *group_by_col_datum_buf_;
  ObAggDatumBuf *tmp_group_by_datum_buf_;
  common::ObSEArray<ObAggCellVec*, DEFAULT_AGG_CELL_CNT> agg_cells_;
  ObPDAggVecFactory agg_cell_factory_vec_;
  sql::ObEvalCtx &eval_ctx_;
  common::ObArenaAllocator tmp_datum_allocator_;
  common::ObArenaAllocator group_by_datum_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObGroupByCellVec);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_OB_PUSHDOWN_AGGREGATE_VEC_H_ */
