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
#include "lib/ob_define.h"
#include "lib/container/ob_array.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_msg.h"

namespace oceanbase
{
namespace sql
{

struct ObCompactRow;
class ObExprTopNFilterContext;
struct ObSortFieldCollation;

struct ObTopNFilterCmpMeta final
{
  OB_UNIS_VERSION_V(1);
public:
  union
  {
    NullSafeRowCmpFunc cmp_func_;
    sql::serializable_function ser_cmp_func_;
  };
  ObObjMeta obj_meta_;
  TO_STRING_KV(K(obj_meta_), K(ser_cmp_func_));
};

struct ObTopNFilterCompare final
{
  OB_UNIS_VERSION_V(1);
public:
  inline int compare_for_build(const ObDatum &l, const ObDatum &r, int &cmp_res) const {
    int ret = build_meta_.cmp_func_(build_meta_.obj_meta_, build_meta_.obj_meta_,
                                    l.ptr_, l.len_, l.is_null(),
                                    r.ptr_, r.len_, r.is_null(),
                                    cmp_res);
    // when compare new comming data with origin data, we always want maintan the smaller one.
    if (!is_ascending_) {
      cmp_res = -cmp_res;
    }
    return ret;
  }
  inline int compare_for_filter(const ObDatum &l, const ObDatum &r, int &cmp_res) const {
    int ret = filter_meta_.cmp_func_(filter_meta_.obj_meta_, build_meta_.obj_meta_,
                                    l.ptr_, l.len_, l.is_null(),
                                    r.ptr_, r.len_, r.is_null(),
                                    cmp_res);
    if (!is_ascending_) {
      cmp_res = -cmp_res;
    }
    return ret;
  }
public:
  // in join scene, the join cond is T1.a=T2.b, sometimes a and b are not same type but not need to
  // cast if the sql with order by T1.a, the topn filter can pushdown to T2.b, but the compare info
  // is differnet in build stage and filter stage
  ObTopNFilterCmpMeta build_meta_;
  ObTopNFilterCmpMeta filter_meta_;
  bool is_ascending_;
  common::ObCmpNullPos null_pos_;
  TO_STRING_KV(K(build_meta_), K(filter_meta_), K(is_ascending_), K(null_pos_));
};
typedef common::ObFixedArray<ObTopNFilterCompare, common::ObIAllocator> ObTopNFilterCompares;

struct ObPushDownTopNFilterInfo
{
  OB_UNIS_VERSION(1);

public:
  explicit ObPushDownTopNFilterInfo(common::ObIAllocator &alloc)
      : enabled_(false), p2p_dh_id_(OB_INVALID), effective_sk_cnt_(0), total_sk_cnt_(0),
        cmp_metas_(alloc), dh_msg_type_(ObP2PDatahubMsgBase::ObP2PDatahubMsgType::NOT_INIT),
        expr_ctx_id_(UINT32_MAX /*INVALID_EXP_CTX_ID*/), is_shared_(false), is_shuffle_(false),
        max_batch_size_(0), adaptive_filter_ratio_(0.5)
  {}
  int init(int64_t p2p_dh_id, int64_t effective_sk_cnt, int64_t total_sk_cnt,
           const ObIArray<ObTopNFilterCmpMeta> &cmp_metas,
           ObP2PDatahubMsgBase::ObP2PDatahubMsgType dh_msg_type, uint32_t expr_ctx_id,
           bool is_shared, bool is_shuffle, int64_t max_batch_size, double adaptive_filter_ratio);
  int assign(const ObPushDownTopNFilterInfo &src);

public:
  bool enabled_;
  int64_t p2p_dh_id_;
  int64_t effective_sk_cnt_;
  int64_t total_sk_cnt_;
  ObFixedArray<ObTopNFilterCmpMeta, common::ObIAllocator> cmp_metas_;
  ObP2PDatahubMsgBase::ObP2PDatahubMsgType dh_msg_type_;
  uint32_t expr_ctx_id_;
  bool is_shared_; // whether the filter is shared in sql level
  bool is_shuffle_; // whether need shuffle topn msg between differnet dfos
  int64_t max_batch_size_;
  double adaptive_filter_ratio_;
  TO_STRING_KV(K(enabled_), K(p2p_dh_id_), K(dh_msg_type_), K(expr_ctx_id_), K(is_shared_),
               K(is_shuffle_), K(max_batch_size_), K(adaptive_filter_ratio_));
};

class ObPushDownTopNFilterMsg final : public ObP2PDatahubMsgBase
{
  OB_UNIS_VERSION_V(1);

public:
  ObPushDownTopNFilterMsg();
  ~ObPushDownTopNFilterMsg()
  {
    destroy();
  }
  int init(const ObPushDownTopNFilterInfo *pd_topn_filter_info, uint64_t tenant_id,
           const ObIArray<ObSortFieldCollation> *sort_collations, ObExecContext *exec_ctx,
           int64_t px_seq_id);
  int destroy();
  int assign(const ObP2PDatahubMsgBase &src_msg) override;
  int deep_copy_msg(ObP2PDatahubMsgBase *&dest_msg) override;
  int merge(ObP2PDatahubMsgBase &) override;

  int filter_out_data(const ObExpr &expr, ObEvalCtx &ctx, ObExprTopNFilterContext &filter_ctx,
                      ObDatum &res);
  int filter_out_data_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                          const int64_t batch_size,
                          ObExprTopNFilterContext &filter_ctx);
  int filter_out_data_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                           const EvalBound &bound,
                           ObExprTopNFilterContext &filter_ctx);
  int update_filter_data(ObCompactRow *compact_row, const RowMeta *row_meta_, bool &is_updated);
  int update_filter_data(ObChunkDatumStore::StoredRow *store_row, bool &is_updated);

  int prepare_storage_white_filter_data(ObDynamicFilterExecutor &dynamic_filter,
                                ObEvalCtx &eval_ctx,
                                ObRuntimeFilterParams &params,
                                bool &is_data_prepared) override final;

  int update_storage_white_filter_data(ObDynamicFilterExecutor &dynamic_filter,
                                       ObRuntimeFilterParams &params, bool &is_update);

  inline int64_t get_current_data_version() { return ATOMIC_LOAD_ACQ(&data_version_); }
  inline bool is_null_first(int64_t col_idx) {
    return (ObCmpNullPos::NULL_FIRST == compares_.at(col_idx).null_pos_);
  }

private:
  bool check_has_null(ObCompactRow *compact_row);
  bool check_has_null(ObChunkDatumStore::StoredRow *store_row);
  // for merge p2p msg in consumer
  int merge_heap_top_datums(ObIArray<ObDatum> &incomming_datums);
  int copy_heap_top_datums_from(ObIArray<ObDatum> &incomming_datums);

  // for update from local thread in producer
  int copy_heap_top_datums_from(ObCompactRow *compact_row, const RowMeta *row_meta_);
  int copy_heap_top_datums_from(ObChunkDatumStore::StoredRow *store_row);

  // for filter out data
  inline int compare(int64_t col_idx, ObDatum &prob_datum, int &cmp_res)
  {
    return compares_.at(col_idx).compare_for_filter(prob_datum, heap_top_datums_.at(col_idx),
                                                    cmp_res);
  }

  inline int get_compare_result(int64_t col_idx, ObDatum &datum, int &cmp_res) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(compare(col_idx, datum, cmp_res))) {
      SQL_LOG(WARN, "fail to compare", K(ret));
    } else if (cmp_res == 0) {
      if (col_idx == total_sk_cnt_ - 1) {
        // this arg is the last one of sort key, we can directly filter
        cmp_res = 1;
      } else if (col_idx == heap_top_datums_.count() - 1) {
        // last sort key pushdown, we need to output the data
        cmp_res = -1;
      }
    }
    return ret;
  }

  int dynamic_copy_cell(const ObDatum &src, ObDatum &target, int64_t &cell_size);
  int adjust_cell_size();

  int do_filter_out_data_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                             const int64_t batch_size,
                             ObExprTopNFilterContext &filter_ctx);
  int do_filter_out_data_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                                const EvalBound &bound, ObExprTopNFilterContext &filter_ctx);

  template <typename ArgVec, typename ResVec>
  int process_multi_columns(int64_t arg_idx, const ObExpr &expr, ObEvalCtx &ctx, uint16_t *selector,
                            int64_t old_row_selector_cnt, int64_t &new_row_selector_cnt,
                            int64_t &filter_count);

private:
  // total sort key count in topn sort operator, total_sk_cnt_ >= heap_top_datums_.count()
  int64_t total_sk_cnt_;
  ObTopNFilterCompares compares_;
  ObFixedArray<ObDatum, common::ObIAllocator> heap_top_datums_;
  ObFixedArray<int64_t, common::ObIAllocator> cells_size_;
  int64_t data_version_;
};

} // end namespace sql
} // end namespace oceanbase
