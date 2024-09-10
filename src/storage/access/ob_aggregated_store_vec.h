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

#ifndef OCEANBASE_STORAGE_OB_AGGREGATED_STORE_VEC_H_
#define OCEANBASE_STORAGE_OB_AGGREGATED_STORE_VEC_H_

#include "share/aggregate/processor.h"
#include "ob_vector_store.h"
#include "ob_pushdown_aggregate_vec.h"

namespace oceanbase
{
namespace storage
{
struct ObAggTypeFlag
{
public:
  ObAggTypeFlag() : t_flag_(0) {}
  ObAggTypeFlag(const int16_t flag) : t_flag_(flag) {}
  ~ObAggTypeFlag() = default;
  const ObAggTypeFlag &operator =(const ObAggTypeFlag &other) {
    if (this != &other) {
      this->t_flag_ = other.t_flag_;
    }
    return *this;
  }
  OB_INLINE void set_count_flag(const bool t_count) { t_count_ = t_count; }
  OB_INLINE void set_minmax_flag(const bool t_minmax) { t_minmax_ = t_minmax; }
  OB_INLINE void set_sum_flag(const bool t_sum) { t_sum_ = t_sum; }
  OB_INLINE bool has_count() const { return t_count_; }
  OB_INLINE bool has_minmax() const { return t_minmax_; }
  OB_INLINE bool has_sum() const { return t_sum_; }
  OB_INLINE bool only_count() const { return t_flag_ == 1; }
  TO_STRING_KV(K_(t_flag));

  union {
    struct {
      int16_t t_count_  : 1;
      int16_t t_minmax_ : 1;
      int16_t t_sum_    : 1;
      int16_t reserved_ : 13;
    };
    int16_t t_flag_;
  };
};

struct ObAggGroupVec : public ObAggGroupBase
{
public:
  ObAggGroupVec();
  ObAggGroupVec(ObColumnParam* col_param, sql::ObExpr* project_expr,
                 const int32_t col_offset, const int32_t col_index);
  virtual ~ObAggGroupVec();
  void reuse();
  int eval(blocksstable::ObStorageDatum &datum, const int64_t row_count) override;
  int eval_batch(
      const ObTableIterParam *iter_param,
      const ObTableAccessContext *context,
      const int32_t col_idx,
      blocksstable::ObIMicroBlockReader *reader,
      const int32_t *row_ids,
      const int64_t row_count,
      const bool projected) override;
  int can_use_index_info(const blocksstable::ObMicroIndexInfo &index_info, bool &can_agg) override;
  int fill_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg) override;
  int collect_result();
  OB_INLINE int set_agg_type_flag(const ObPDAggType agg_type);
  OB_INLINE bool check_need_project(
      blocksstable::ObIMicroBlockReader *reader,
      const int32_t *row_ids,
      const int64_t row_count)
  {
    bool bret = false;
    if (agg_type_flag_.has_sum()) {
      bret = true;
    } else if (agg_type_flag_.has_minmax()) {
      for (int64_t i = 0; !bret && i < agg_cells_.count(); ++i) {
        const ObAggCellVec *agg_cell = agg_cells_.at(i);
        bret |= !agg_cell->can_pushdown_decoder(reader, col_offset_, row_ids, row_count);
      }
    }
    return bret;
  }
  OB_INLINE ObAggCellVec* at(const int64_t idx) { return agg_cells_.at(idx); }
  OB_INLINE int64_t get_agg_count() const { return agg_cells_.count(); }
  OB_INLINE void set_cg_offset_and_index()
  {
    col_offset_ = 0;
    col_index_ = 0;
  }
  OB_INLINE bool is_vec() const override { return true; }
  OB_INLINE bool check_finished() const override { return false; }
  TO_STRING_KV(K_(col_offset), K_(col_index), K_(need_access_data),
               K_(need_get_row_ids), K_(agg_type_flag),
               K_(agg_cells), KPC_(col_param), KPC_(project_expr));
public:
  ObSEArray<ObAggCellVec*, 1> agg_cells_;
  ObColumnParam* col_param_;
  sql::ObExpr* project_expr_;
  int32_t col_offset_;
  int32_t col_index_;
  ObAggTypeFlag agg_type_flag_;
  bool need_access_data_;
  bool need_get_row_ids_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAggGroupVec);
};

class ObAggregatedStoreVec : public ObAggStoreBase, public ObVectorStore
{
public:
  ObAggregatedStoreVec(
      const int64_t batch_size,
      sql::ObEvalCtx &eval_ctx,
      ObTableAccessContext &context,
      sql::ObBitVector *skip_bit);
  virtual ~ObAggregatedStoreVec();
  virtual void reset() override;
  virtual void reuse() override;
  virtual int reuse_capacity(const int64_t capacity) override;
  virtual int init(const ObTableAccessParam &param, common::hash::ObHashSet<int32_t> *agg_col_mask = nullptr) override;
  virtual int fill_row(blocksstable::ObDatumRow &out_row) override;
  virtual int fill_rows(
      const int64_t group_idx,
      blocksstable::ObIMicroBlockRowScanner &scanner,
      int64_t &begin_index,
      const int64_t end_index,
      const ObFilterResult &res) override;
  virtual int fill_rows(const int64_t group_idx, const int64_t row_count);
  int can_use_index_info(const blocksstable::ObMicroIndexInfo &index_info, bool &can_agg) override;
  int fill_index_info(const blocksstable::ObMicroIndexInfo &index_info, const bool is_cg) override;
  int collect_aggregated_result() override;
  int get_agg_group(const sql::ObExpr *expr, ObAggGroupVec *&agg_group);
  INHERIT_TO_STRING_KV("ObVectorStore", ObVectorStore, K_(pd_agg_ctx), K_(agg_groups),
                        K_(need_access_data), K_(need_get_row_ids));
private:
  void release_agg_group();
  int init_agg_groups(const ObTableAccessParam &param);
  int check_agg_store_valid();
  int do_aggregate(blocksstable::ObIMicroBlockReader *reader = nullptr);
  OB_INLINE void reset_after_aggregate()
  {
    count_ = 0;
    eval_ctx_.set_batch_idx(0);
  }
  ObPushdownAggContext pd_agg_ctx_;
  common::ObSEArray<ObAggGroupVec*, 4> agg_groups_;
  common::hash::ObHashSet<int32_t> col_mask_set_;
  ObPDAggVecFactory pd_agg_factory_;
  common::ObIAllocator &allocator_;
  // need_access_data is true => need_get_row_ids_ must be true.
  // need_access_data is false => need_get_row_ids_ may be true/false.
  bool need_access_data_;
  bool need_get_row_ids_;
  DISALLOW_COPY_AND_ASSIGN(ObAggregatedStoreVec);
};

} /* namespace stroage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_OB_AGGREGATED_STORE_VEC_H_ */
