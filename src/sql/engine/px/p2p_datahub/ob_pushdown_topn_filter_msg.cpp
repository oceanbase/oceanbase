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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/px/p2p_datahub/ob_pushdown_topn_filter_msg.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"
#include "sql/engine/sort/ob_sort_vec_op.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "sql/engine/sort/ob_sort_vec_op_context.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_topn_filter.h"
#include "sql/engine/px/ob_px_sqc_handler.h"


namespace oceanbase
{
namespace sql
{

class InitSelecterOP
{
public:
  InitSelecterOP(uint16_t *selector, int64_t &selector_cnt)
      : selector_(selector), selector_cnt_(selector_cnt)
  {}
  OB_INLINE int operator() (int64_t batch_i) {
    selector_[selector_cnt_++] = batch_i;
    return OB_SUCCESS;
  }
private:
  uint16_t *selector_;
  int64_t &selector_cnt_;
};


OB_SERIALIZE_MEMBER(ObTopNFilterCmpMeta, ser_cmp_func_, obj_meta_);
OB_SERIALIZE_MEMBER(ObTopNFilterCompare, build_meta_, filter_meta_, is_ascending_, null_pos_);
OB_SERIALIZE_MEMBER(ObPushDownTopNFilterInfo, enabled_, p2p_dh_id_, effective_sk_cnt_,
                    total_sk_cnt_, cmp_metas_, dh_msg_type_, expr_ctx_id_, is_shared_, is_shuffle_,
                    max_batch_size_, adaptive_filter_ratio_);

int ObPushDownTopNFilterInfo::init(int64_t p2p_dh_id, int64_t effective_sk_cnt,
                                   int64_t total_sk_cnt,
                                   const ObIArray<ObTopNFilterCmpMeta> &cmp_metas,
                                   ObP2PDatahubMsgBase::ObP2PDatahubMsgType dh_msg_type,
                                   uint32_t expr_ctx_id, bool is_shared, bool is_shuffle,
                                   int64_t max_batch_size, double adaptive_filter_ratio)
{
  int ret = OB_SUCCESS;
  p2p_dh_id_ = p2p_dh_id;
  effective_sk_cnt_ = effective_sk_cnt;
  total_sk_cnt_ = total_sk_cnt;
  dh_msg_type_ = dh_msg_type;
  expr_ctx_id_ = expr_ctx_id;
  is_shared_ = is_shared;
  is_shuffle_ = is_shuffle;
  max_batch_size_ = max_batch_size;
  adaptive_filter_ratio_ = adaptive_filter_ratio;
  if (OB_FAIL(cmp_metas_.assign(cmp_metas))) {
    LOG_WARN("failed to assign cmp_metas");
  } else {
    enabled_ = true;
  }
  return ret;
}

int ObPushDownTopNFilterInfo::assign(const ObPushDownTopNFilterInfo &src)
{
  int ret = OB_SUCCESS;
  p2p_dh_id_ = src.p2p_dh_id_;
  effective_sk_cnt_ = src.effective_sk_cnt_;
  total_sk_cnt_ = src.total_sk_cnt_;
  dh_msg_type_ = src.dh_msg_type_;
  expr_ctx_id_ = src.expr_ctx_id_;
  is_shared_ = src.is_shared_;
  is_shuffle_ = src.is_shuffle_;
  max_batch_size_ = src.max_batch_size_;
  adaptive_filter_ratio_ = src.adaptive_filter_ratio_;
  enabled_ = src.enabled_;
  if (OB_FAIL(cmp_metas_.assign(src.cmp_metas_))) {
    LOG_WARN("failed to assign cmp_metas");
  }
  return ret;
}

OB_DEF_SERIALIZE(ObPushDownTopNFilterMsg)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObPushDownTopNFilterMsg, ObP2PDatahubMsgBase));
  LST_DO_CODE(OB_UNIS_ENCODE, total_sk_cnt_, compares_, heap_top_datums_, cells_size_,
              data_version_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPushDownTopNFilterMsg)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObPushDownTopNFilterMsg, ObP2PDatahubMsgBase));
  LST_DO_CODE(OB_UNIS_DECODE, total_sk_cnt_, compares_, heap_top_datums_, cells_size_,
              data_version_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(adjust_cell_size())) {
    LOG_WARN("fail do adjust cell size", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPushDownTopNFilterMsg)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObPushDownTopNFilterMsg, ObP2PDatahubMsgBase));
  LST_DO_CODE(OB_UNIS_ADD_LEN, total_sk_cnt_, compares_, heap_top_datums_, cells_size_,
              data_version_);
  return len;
}
ObPushDownTopNFilterMsg::ObPushDownTopNFilterMsg()
    : ObP2PDatahubMsgBase(), total_sk_cnt_(0), compares_(allocator_), heap_top_datums_(allocator_),
      cells_size_(allocator_), data_version_(0)
{}

int ObPushDownTopNFilterMsg::init(const ObPushDownTopNFilterInfo *pd_topn_filter_info,
                                  uint64_t tenant_id,
                                  const ObIArray<ObSortFieldCollation> *sort_collations,
                                  ObExecContext *exec_ctx,
                                  int64_t px_seq_id)
{
  int ret = OB_SUCCESS;
  int64_t timeout_ts = GET_PHY_PLAN_CTX(*exec_ctx)->get_timeout_timestamp();
  int64_t effective_sk_cnt = pd_topn_filter_info->effective_sk_cnt_;
  ObPxSqcHandler *sqc_handler = exec_ctx->get_sqc_handler();
  int64_t task_id = 0;
  common::ObRegisterDmInfo register_dm_info;
  if (nullptr == sqc_handler) {
    // none px plan
  } else {
    task_id = exec_ctx->get_px_task_id();
    ObPxSqcMeta &sqc = sqc_handler->get_sqc_init_arg().sqc_;
    register_dm_info.detectable_id_ = sqc.get_px_detectable_ids().qc_detectable_id_;
    register_dm_info.addr_ = sqc.get_qc_addr();
  }

  if (OB_FAIL(ObP2PDatahubMsgBase::init(
          pd_topn_filter_info->p2p_dh_id_, px_seq_id, task_id,
          tenant_id, timeout_ts, register_dm_info))) {
    LOG_WARN("fail to init basic p2p msg", K(ret));
  } else if (FALSE_IT(total_sk_cnt_ = pd_topn_filter_info->total_sk_cnt_)) {
  } else if (OB_FAIL(heap_top_datums_.prepare_allocate(effective_sk_cnt))) {
    LOG_WARN("fail to prepare allocate heap_top_datums_", K(ret));
  } else if (OB_FAIL(cells_size_.prepare_allocate(effective_sk_cnt))) {
    LOG_WARN("fail to prepare allocate cells_size_", K(ret));
  } else if (OB_FAIL(compares_.prepare_allocate(effective_sk_cnt))) {
    LOG_WARN("fail to prepare allocate compares_", K(ret));
  } else {
    for (int64_t i = 0; i < effective_sk_cnt && OB_SUCC(ret); ++i) {
      // TODO XUNSI: in join scene, if the sort key is the join key of the right table
      // the build_meta_ and filter_meta_ may different, preprae it
      compares_.at(i).build_meta_.cmp_func_ = pd_topn_filter_info->cmp_metas_.at(i).cmp_func_;
      compares_.at(i).build_meta_.obj_meta_.set_meta(pd_topn_filter_info->cmp_metas_.at(i).obj_meta_);
      compares_.at(i).filter_meta_.cmp_func_ = pd_topn_filter_info->cmp_metas_.at(i).cmp_func_;
      compares_.at(i).filter_meta_.obj_meta_.set_meta(pd_topn_filter_info->cmp_metas_.at(i).obj_meta_);
      cells_size_.at(i) = 0;
      compares_.at(i).is_ascending_ = sort_collations->at(i).is_ascending_;
      compares_.at(i).null_pos_ = sort_collations->at(i).null_pos_;
    }
    set_msg_expect_cnt(1); // TODO fix me in shared msg
    set_msg_cur_cnt(1);
  }
  LOG_TRACE("[TopN Filter] init ObPushDownTopNFilterMsg", K(ret), K(effective_sk_cnt));
  return ret;
}

int ObPushDownTopNFilterMsg::destroy()
{
  int ret = OB_SUCCESS;
  compares_.reset();
  heap_top_datums_.reset();
  cells_size_.reset();
  allocator_.reset();
  LOG_DEBUG("[TopN Filter] destroy ObPushDownTopNFilterMsg", K(common::lbt()));
  return OB_SUCCESS;
}

int ObPushDownTopNFilterMsg::assign(const ObP2PDatahubMsgBase &src_msg)
{
  int ret = OB_SUCCESS;
  const ObPushDownTopNFilterMsg &src_topn_msg =
      static_cast<const ObPushDownTopNFilterMsg &>(src_msg);
  if (OB_FAIL(ObP2PDatahubMsgBase::assign(src_msg))) {
    LOG_WARN("failed to assign base data", K(ret));
  } else if (FALSE_IT(total_sk_cnt_ = src_topn_msg.total_sk_cnt_)) {
  } else if (OB_FAIL(compares_.assign(src_topn_msg.compares_))) {
    LOG_WARN("fail to assign compares_", K(ret));
  } else if (OB_FAIL(heap_top_datums_.assign(src_topn_msg.heap_top_datums_))) {
    LOG_WARN("fail to assign heap top datums", K(ret));
  } else if (OB_FAIL(cells_size_.assign(src_topn_msg.cells_size_))) {
    LOG_WARN("failed to assign cell size", K(ret));
  } else if (OB_FAIL(adjust_cell_size())) {
    LOG_WARN("fail to adjust cell size", K(ret));
  } else if (FALSE_IT(data_version_ = src_topn_msg.data_version_)) {
  } else {
    // deep copy datum memory
    for (int i = 0; i < src_topn_msg.heap_top_datums_.count() && OB_SUCC(ret); ++i) {
      const ObDatum &src_datum = src_topn_msg.heap_top_datums_.at(i);
      if (OB_FAIL(heap_top_datums_.at(i).deep_copy(src_datum, allocator_))) {
        LOG_WARN("fail to deep copy heap top datum", K(ret));
      }
    }
  }
  return ret;
}

int ObPushDownTopNFilterMsg::deep_copy_msg(ObP2PDatahubMsgBase *&dest_msg)
{
  int ret = OB_SUCCESS;
  ObPushDownTopNFilterMsg *new_topn_msg = nullptr;
  ObMemAttr attr(tenant_id_, "TOPNVECMSG");
  if (OB_FAIL(PX_P2P_DH.alloc_msg<ObPushDownTopNFilterMsg>(attr, new_topn_msg))) {
    LOG_WARN("fail to alloc msg", K(ret));
  } else if (OB_FAIL(new_topn_msg->assign(*this))) {
    LOG_WARN("fail to assign msg", K(ret));
  } else {
    dest_msg = new_topn_msg;
  }
  return ret;
}

int ObPushDownTopNFilterMsg::merge(ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  ObPushDownTopNFilterMsg &incomming_topn_msg = static_cast<ObPushDownTopNFilterMsg &>(msg);
  if (incomming_topn_msg.heap_top_datums_.count() != heap_top_datums_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected heap top datum count", K(incomming_topn_msg.heap_top_datums_.count()),
             K(heap_top_datums_.count()));
  } else if (incomming_topn_msg.is_empty_) {
    /*do nothing*/
  } else {
    ObSpinLockGuard guard(lock_);
    if (OB_FAIL(merge_heap_top_datums(incomming_topn_msg.heap_top_datums_))) {
      LOG_WARN("fail to merge heap top datums", K(ret));
    } else if (is_empty_) {
      is_empty_ = false;
    }
  }
  return ret;
}

int ObPushDownTopNFilterMsg::filter_out_data(const ObExpr &expr, ObEvalCtx &ctx,
                                             ObExprTopNFilterContext &filter_ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  int cmp_res = 0;
  bool is_filtered = false;
  filter_ctx.total_count_++;
  if (OB_UNLIKELY(is_empty_)) {
    res.set_int(0);
    filter_ctx.filter_count_++;
    filter_ctx.check_count_++;
  } else {
    filter_ctx.check_count_++;
    for (int i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
      if (OB_FAIL(expr.args_[i]->eval(ctx, datum))) {
        LOG_WARN("failed to eval datum", K(ret));
      } else {
        if (OB_FAIL(get_compare_result(i, *datum, cmp_res))) {
          LOG_WARN("fail to compare", K(ret));
        } else if (cmp_res > 0) {
          is_filtered = true;
          break;
        } else if (cmp_res < 0) {
          // the data less than head top data is selected
          break;
        } else {
          // only if the data of the previous column is equal, we need compare the next column
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!is_filtered) {
        res.set_int(1);
      } else {
        filter_ctx.filter_count_++;
        res.set_int(0);
      }
      filter_ctx.collect_sample_info(is_filtered, 1);
    }
  }
  return ret;
}

int ObPushDownTopNFilterMsg::filter_out_data_batch(
    const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size,
    ObExprTopNFilterContext &filter_ctx)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  if (OB_UNLIKELY(is_empty_)) {
    ObDatum *results = expr.locate_batch_datums(ctx);
    for (int64_t i = 0; i < batch_size; i++) {
      results[i].set_int(0);
    }
    filter_ctx.total_count_ += batch_size;
    filter_ctx.check_count_ += batch_size;
    filter_ctx.filter_count_ += batch_size;
  } else if (OB_FAIL(do_filter_out_data_batch(expr, ctx, skip, batch_size, filter_ctx))) {
    LOG_WARN("failed to do_filter_out_data_batch");
  }
  if (OB_SUCC(ret)) {
    eval_flags.set_all(batch_size);
  }
  return ret;
}

int ObPushDownTopNFilterMsg::filter_out_data_vector(
    const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound,
    ObExprTopNFilterContext &filter_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_empty_)) {
    int64_t total_count = 0;
    int64_t filter_count = 0;
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_UNIFORM == res_format) {
      IntegerUniVec *res_vec = static_cast<IntegerUniVec *>(expr.get_vector(ctx));
      ret = proc_filter_empty(res_vec, skip, bound, total_count, filter_count);
    } else if (VEC_FIXED == res_format) {
      IntegerFixedVec *res_vec = static_cast<IntegerFixedVec *>(expr.get_vector(ctx));
      ret = proc_filter_empty(res_vec, skip, bound, total_count, filter_count);
    }
    if (OB_SUCC(ret)) {
      eval_flags.set_all(true);
      filter_ctx.filter_count_ += filter_count;
      filter_ctx.check_count_ += total_count;
      filter_ctx.total_count_ += total_count;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
      ObExpr *e = expr.args_[i];
      if (OB_FAIL(e->eval_vector(ctx, skip, bound))) {
        LOG_WARN("evaluate vector failed", K(ret), K(*e));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_filter_out_data_vector(expr, ctx, skip, bound, filter_ctx))) {
      LOG_WARN("fail to do filter out data vector");
    }
  }
  return ret;
}

int ObPushDownTopNFilterMsg::update_filter_data(ObCompactRow *compact_row, const RowMeta *row_meta,
                                                bool &is_updated)
{
  int ret = OB_SUCCESS;
  is_updated = false;

  // TODO XUNSI: update data for shared topn msg, be care of thread safe
  if (check_has_null(compact_row)) {
    // do nothing, null will not be updated into filter
  } else if (OB_FAIL(copy_heap_top_datums_from(compact_row, row_meta))) {
    LOG_WARN("failed to copy");
  } else {
    is_updated = true;
    int64_t v = ATOMIC_AAF(&data_version_, 1);
    LOG_TRACE("[TopN Filter] update_filter_data", K(v));
  }
  if (OB_SUCC(ret) && is_updated && OB_UNLIKELY(is_empty_)) {
    is_empty_ = false;
    LOG_TRACE("[TopN Filter] first update filter data");
  }
  return ret;
}

int ObPushDownTopNFilterMsg::update_filter_data(ObChunkDatumStore::StoredRow *store_row,
                                                bool &is_updated)
{
  int ret = OB_SUCCESS;
  is_updated = false;
  // TODO XUNSI: update data for shared topn msg, be care of thread safe
  if (check_has_null(store_row)) {
    // do nothing, null will not be updated into filter
  } else if (OB_FAIL(copy_heap_top_datums_from(store_row))) {
    LOG_WARN("failed to copy");
  } else {
    is_updated = true;
    int64_t v = ATOMIC_AAF(&data_version_, 1);
    LOG_TRACE("[TopN Filter] update_filter_data", K(v));
  }
  if (OB_SUCC(ret) && is_updated && OB_UNLIKELY(is_empty_)) {
    is_empty_ = false;
    LOG_TRACE("[TopN Filter] first update filter data");
  }
  return ret;
}

bool ObPushDownTopNFilterMsg::check_has_null(ObCompactRow *compact_row)
{
  int ret = OB_SUCCESS;
  bool has_null = false;
  for (int64_t i = 0; i < heap_top_datums_.count() && OB_SUCC(ret); ++i) {
    if (compact_row->is_null(i)) {
      has_null = true;
      break;
    }
  }
  return has_null;
}

bool ObPushDownTopNFilterMsg::check_has_null(ObChunkDatumStore::StoredRow *store_row)
{
  int ret = OB_SUCCESS;
  bool has_null = false;
  const common::ObDatum *incomming_datums = store_row->cells();
  for (int64_t i = 0; i < heap_top_datums_.count() && OB_SUCC(ret); ++i) {
    if (incomming_datums[i].is_null()) {
      has_null = true;
      break;
    }
  }
  return ret;
}

int ObPushDownTopNFilterMsg::prepare_storage_white_filter_data(
    ObDynamicFilterExecutor &dynamic_filter, ObEvalCtx &eval_ctx, ObRuntimeFilterParams &params,
    bool &is_data_prepared)
{
  int ret = OB_SUCCESS;
  int col_idx = dynamic_filter.get_col_idx();
  if (is_empty_) {
    dynamic_filter.set_filter_action(DynamicFilterAction::FILTER_ALL);
    is_data_prepared = true;
  } else if (heap_top_datums_.at(col_idx).is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect no null in topn runtime filter");
  } else if (OB_FAIL(params.push_back(heap_top_datums_.at(col_idx)))) {
    LOG_WARN("failed to push back bound data");
  } else {
    int64_t now_data_version = ATOMIC_LOAD(&data_version_);
    dynamic_filter.set_filter_action(DynamicFilterAction::DO_FILTER);
    dynamic_filter.set_filter_val_meta(compares_.at(col_idx).build_meta_.obj_meta_);
    dynamic_filter.set_stored_data_version(now_data_version);
    // caution, see bool ObOpRawExpr::is_white_runtime_filter_expr() const
    // now only one column topn filter can be pushdown as white filter,
    // if these column is the last sort key, means we can filter the data which the compare reuslt is equal.
    ObWhiteFilterOperatorType op_type;
    if (col_idx == total_sk_cnt_ - 1) {
      op_type = compares_.at(col_idx).is_ascending_ ? WHITE_OP_LT : WHITE_OP_GT;
    } else {
      op_type = compares_.at(col_idx).is_ascending_ ? WHITE_OP_LE : WHITE_OP_GE;
    }
    dynamic_filter.get_filter_node().set_op_type(op_type);
    is_data_prepared = true;
  }
  return ret;
}

int ObPushDownTopNFilterMsg::update_storage_white_filter_data(
    ObDynamicFilterExecutor &dynamic_filter, ObRuntimeFilterParams &params, bool &is_update)
{
  int ret = OB_SUCCESS;
  int64_t now_data_version = ATOMIC_LOAD(&data_version_);
  int col_idx = dynamic_filter.get_col_idx();
  if (heap_top_datums_.at(col_idx).is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect no null in topn runtime filter");
  } else if (OB_FAIL(params.push_back(heap_top_datums_.at(col_idx)))) {
    LOG_WARN("failed to push back bound data");
  } else {
    dynamic_filter.set_stored_data_version(now_data_version);
    is_update = true;
  }
  return ret;
}

// private interface
int ObPushDownTopNFilterMsg::merge_heap_top_datums(ObIArray<ObDatum> &incomming_datums)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_empty_)) {
    // if self is empty, directly copy data from the incomming msg
    if (OB_FAIL(copy_heap_top_datums_from(incomming_datums))) {
      LOG_WARN("failed to copy");
    } else {
      is_empty_ = false;
    }
  } else {
    // compare in vector format
    int cmp_res = 0;
    for (int i = 0; i < incomming_datums.count() && OB_SUCC(ret); ++i) {
      const ObTopNFilterCompare &compare = compares_.at(i);
      const ObDatum &incomming_datum = incomming_datums.at(i);
      ObDatum &origin_datum = heap_top_datums_.at(i);
      cmp_res = 0;
      if (OB_FAIL(compare.compare_for_build(incomming_datum, origin_datum, cmp_res))) {
        LOG_WARN("fail to compare_for_build", K(ret));
      } else if (cmp_res < 0) {
        break;
      }
    }
    // the new incomming_datums is less than self, we need copy it.
    if (OB_SUCC(ret) && cmp_res < 0) {
      if (OB_FAIL(copy_heap_top_datums_from(incomming_datums))) {
        LOG_WARN("failed to copy");
      }
    }
  }
  return ret;
}

int ObPushDownTopNFilterMsg::copy_heap_top_datums_from(ObIArray<ObDatum> &incomming_datums)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < heap_top_datums_.count() && OB_SUCC(ret); ++i) {
    const ObDatum &incomming_datum = incomming_datums.at(i);
    ObDatum &origin_datum = heap_top_datums_.at(i);
    int64_t &cell_size = cells_size_.at(i);
    if (OB_FAIL(dynamic_copy_cell(incomming_datum, origin_datum, cell_size))) {
      LOG_WARN("fail to deep copy datum");
    }
  }
  return ret;
}

int ObPushDownTopNFilterMsg::copy_heap_top_datums_from(ObCompactRow *compact_row,
                                                       const RowMeta *row_meta)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < heap_top_datums_.count() && OB_SUCC(ret); ++i) {
    const ObDatum &incomming_datum = compact_row->get_datum(*row_meta, i);
    ObDatum &origin_datum = heap_top_datums_.at(i);
    int64_t &cell_size = cells_size_.at(i);
    if (OB_FAIL(dynamic_copy_cell(incomming_datum, origin_datum, cell_size))) {
      LOG_WARN("fail to deep copy datum");
    }
  }
  return ret;
}

int ObPushDownTopNFilterMsg::copy_heap_top_datums_from(ObChunkDatumStore::StoredRow *store_row)
{
  int ret = OB_SUCCESS;
  const common::ObDatum *incomming_datums = store_row->cells();
  for (int64_t i = 0; i < heap_top_datums_.count() && OB_SUCC(ret); ++i) {
    const ObDatum &incomming_datum = incomming_datums[i];
    ObDatum &origin_datum = heap_top_datums_.at(i);
    int64_t &cell_size = cells_size_.at(i);
    if (OB_FAIL(dynamic_copy_cell(incomming_datum, origin_datum, cell_size))) {
      LOG_WARN("fail to deep copy datum");
    }
  }
  return ret;
}

int ObPushDownTopNFilterMsg::dynamic_copy_cell(const ObDatum &src, ObDatum &target,
                                               int64_t &cell_size)
{
  int ret = OB_SUCCESS;
  int64_t need_size = src.len_;
  if (src.is_null()) {
    target.null_ = 1;
  } else {
    if (need_size > cell_size) {
      need_size = need_size * 2;
      char *buff_ptr = NULL;
      if (OB_ISNULL(buff_ptr = static_cast<char *>(allocator_.alloc(need_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "fall to alloc buff", K(need_size), K(ret));
      } else {
        memcpy(buff_ptr, src.ptr_, src.len_);
        target.pack_ = src.pack_;
        target.ptr_ = buff_ptr;
        cell_size = need_size;
      }
    } else {
      memcpy(const_cast<char *>(target.ptr_), src.ptr_, src.len_);
      target.pack_ = src.pack_;
    }
  }
  return ret;
}

int ObPushDownTopNFilterMsg::adjust_cell_size()
{
  int ret = OB_SUCCESS;
  CK(cells_size_.count() == heap_top_datums_.count());
  for (int i = 0; OB_SUCC(ret) && i < cells_size_.count(); ++i) {
    cells_size_.at(i) = std::min(cells_size_.at(i), (int64_t)heap_top_datums_.at(i).len_);
  }
  return ret;
}

int ObPushDownTopNFilterMsg::do_filter_out_data_batch(
    const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size,
    ObExprTopNFilterContext &filter_ctx)
{
  int ret = OB_SUCCESS;
  int64_t filter_count = 0;
  int64_t total_count = 0;
  ObDatum *results = expr.locate_batch_datums(ctx);
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(batch_size);
  for (int idx = 0; OB_SUCC(ret) && idx < expr.arg_cnt_; ++idx) {
    if (OB_FAIL(expr.args_[idx]->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("eval_batch failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int cmp_res = 0;
    ObDatum *datum = nullptr;
    bool is_filtered = false;
    for (int64_t batch_i = 0; OB_SUCC(ret) && batch_i < batch_size; ++batch_i) {
      if (skip.at(batch_i)) {
        continue;
      }
      cmp_res = 0;
      is_filtered = false;
      total_count++;
      batch_info_guard.set_batch_idx(batch_i);
      for (int arg_i = 0; OB_SUCC(ret) && arg_i < expr.arg_cnt_; ++arg_i) {
        datum = &expr.args_[arg_i]->locate_expr_datum(ctx, batch_i);
        if (OB_FAIL(get_compare_result(arg_i, *datum, cmp_res))) {
          LOG_WARN("fail to compare", K(ret));
        } else if (cmp_res > 0) {
          // the data bigger than head top data should be filterd out.
          filter_count++;
          is_filtered = true;
          break;
        } else if (cmp_res < 0) {
          // the data less than head top data is selected
          break;
        } else {
          // only if the data of the previous column is equal, we need compare the next column
        }
      }
      results[batch_i].set_int(is_filtered ? 0 : 1);
    }
  }
  if (OB_SUCC(ret)) {
    filter_ctx.filter_count_ += filter_count;
    filter_ctx.total_count_ += total_count;
    filter_ctx.check_count_ += total_count;
    filter_ctx.collect_sample_info(filter_count, total_count);
  }
  return ret;
}

#define MULTI_COL_COMPARE_DISPATCH_VECTOR_RES_FORMAT(func_name, arg_format, res_format, ...)       \
  if (res_format == VEC_FIXED) {                                                                   \
    MULTI_COL_COMPARE_DISPATCH_VECTOR_ARG_FORMAT(func_name, arg_format, IntegerFixedVec,           \
                                                 __VA_ARGS__);                                     \
  } else {                                                                                         \
    MULTI_COL_COMPARE_DISPATCH_VECTOR_ARG_FORMAT(func_name, arg_format, IntegerUniVec,           \
                                                 __VA_ARGS__);                                     \
  }

#define MULTI_COL_COMPARE_DISPATCH_VECTOR_ARG_FORMAT(func_name, arg_format, result_format, ...)    \
  switch (arg_format) {                                                                            \
  case VEC_FIXED: {                                                                                \
    ret = func_name<ObFixedLengthBase, result_format>(__VA_ARGS__);                                \
    break;                                                                                         \
  }                                                                                                \
  case VEC_DISCRETE: {                                                                             \
    ret = func_name<ObDiscreteFormat, result_format>(__VA_ARGS__);                                 \
    break;                                                                                         \
  }                                                                                                \
  case VEC_CONTINUOUS: {                                                                           \
    ret = func_name<ObContinuousFormat, result_format>(__VA_ARGS__);                               \
    break;                                                                                         \
  }                                                                                                \
  case VEC_UNIFORM: {                                                                              \
    ret = func_name<ObUniformFormat<false>, result_format>(__VA_ARGS__);                           \
    break;                                                                                         \
  }                                                                                                \
  case VEC_UNIFORM_CONST: {                                                                        \
    ret = func_name<ObUniformFormat<true>, result_format>(__VA_ARGS__);                            \
    break;                                                                                         \
  }                                                                                                \
  default: {                                                                                       \
    ret = func_name<ObVectorBase, result_format>(__VA_ARGS__);                                     \
  }                                                                                                \
  }

template <typename ArgVec, typename ResVec>
int ObPushDownTopNFilterMsg::process_multi_columns(int64_t arg_idx, const ObExpr &expr,
                                                   ObEvalCtx &ctx, uint16_t *selector,
                                                   int64_t old_row_selector_cnt,
                                                   int64_t &new_row_selector_cnt,
                                                   int64_t &filter_count)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[arg_idx]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  const char *fixed_base_arg_payload = nullptr;
  bool arg_hash_null = arg_vec->has_null();
  ObLength arg_len = 0;
  ObDatum datum;
  int cmp_res = 0;
  const int64_t not_filtered_payload = 1; // for VEC_FIXED set set_payload, always 1
  new_row_selector_cnt = 0;

#define FILL_MUL_COLUMN_RESULT                                                                     \
  if (OB_FAIL(get_compare_result(arg_idx, datum, cmp_res))) {                                      \
    LOG_WARN("fail to get_compare_result", K(ret));                                                \
  } else {                                                                                         \
    if (std::is_same<ResVec, IntegerFixedVec>::value) {                                            \
      if (cmp_res > 0) {                                                                           \
        filter_count += 1;                                                                         \
      } else if (cmp_res < 0) {                                                                    \
        res_vec->set_payload(batch_idx, &not_filtered_payload, sizeof(int64_t));                   \
      } else {                                                                                     \
        selector[new_row_selector_cnt++] = batch_idx;                                              \
      }                                                                                            \
    } else {                                                                                       \
      if (cmp_res > 0) {                                                                           \
        filter_count += 1;                                                                         \
        res_vec->set_int(batch_idx, 0);                                                            \
      } else if (cmp_res < 0) {                                                                    \
        res_vec->set_int(batch_idx, 1);                                                            \
      } else {                                                                                     \
        selector[new_row_selector_cnt++] = batch_idx;                                              \
      }                                                                                            \
    }                                                                                              \
  }

  if (std::is_same<ArgVec, ObFixedLengthBase>::value) {
    fixed_base_arg_payload = (reinterpret_cast<ObFixedLengthBase *>(arg_vec))->get_data();
    ObLength arg_len = (reinterpret_cast<ObFixedLengthBase *>(arg_vec))->get_length();
    datum.len_ = arg_len;
    if (!arg_hash_null) {
      datum.null_ = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < old_row_selector_cnt; ++i) {
        int64_t batch_idx = selector[i];
        datum.ptr_ = fixed_base_arg_payload + arg_len * batch_idx;
        FILL_MUL_COLUMN_RESULT
      }
    } else {
      ObBitmapNullVectorBase *bn_vec_base = reinterpret_cast<ObBitmapNullVectorBase *>(arg_vec);
      for (int64_t i = 0; OB_SUCC(ret) && i < old_row_selector_cnt; ++i) {
        int64_t batch_idx = selector[i];
        datum.ptr_ = fixed_base_arg_payload + arg_len * batch_idx;
        datum.null_ = bn_vec_base->is_null(batch_idx);
        FILL_MUL_COLUMN_RESULT
      }
    }
  } else {
    if (!arg_hash_null) {
      datum.null_ = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < old_row_selector_cnt; ++i) {
        int64_t batch_idx = selector[i];
        arg_vec->get_payload(batch_idx, datum.ptr_, arg_len);
        datum.len_ = arg_len;
        FILL_MUL_COLUMN_RESULT
      }
    } else {
      if (!std::is_same<ArgVec, ObUniformFormat<false>>::value
          && !std::is_same<ArgVec, ObUniformFormat<true>>::value) {
        ObBitmapNullVectorBase *bn_vec_base = reinterpret_cast<ObBitmapNullVectorBase *>(arg_vec);
        for (int64_t i = 0; OB_SUCC(ret) && i < old_row_selector_cnt; ++i) {
          int64_t batch_idx = selector[i];
          arg_vec->get_payload(batch_idx, datum.ptr_, arg_len);
          datum.len_ = arg_len;
          datum.null_ = bn_vec_base->is_null(batch_idx);
          FILL_MUL_COLUMN_RESULT
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < old_row_selector_cnt; ++i) {
          int64_t batch_idx = selector[i];
          arg_vec->get_payload(batch_idx, datum.ptr_, arg_len);
          datum.len_ = arg_len;
          datum.null_ = arg_vec->is_null(batch_idx);
          FILL_MUL_COLUMN_RESULT
        }
      }
    }
  }
#undef FILL_MUL_COLUMN_RESULT
  return ret;
}

int ObPushDownTopNFilterMsg::do_filter_out_data_vector(
    const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound,
    ObExprTopNFilterContext &filter_ctx)
{
  int ret = OB_SUCCESS;
  int64_t total_count = 0;
  int64_t filter_count = 0;
  int64_t batch_size = bound.batch_size();
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  VectorFormat res_format = expr.get_format(ctx);

  if (VEC_FIXED == res_format) {
    IntegerFixedVec *res_vec = static_cast<IntegerFixedVec *>(expr.get_vector(ctx));
    if (OB_FAIL(preset_not_match(res_vec, bound))) {
      LOG_WARN("failed to preset_not_match", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else  {
    // init selector
    int64_t old_row_selector_cnt = 0;
    int64_t new_row_selector_cnt = 0;
    if (bound.get_all_rows_active()) {
      for (int64_t batch_i = bound.start(); batch_i < bound.end(); ++batch_i) {
        filter_ctx.row_selector_[old_row_selector_cnt++] = batch_i;
      }
    } else {
      InitSelecterOP init_select_op(filter_ctx.row_selector_, old_row_selector_cnt);
      (void)ObBitVector::flip_foreach(skip, bound, init_select_op);
    }
    total_count += old_row_selector_cnt;
    // compare by column
    for (int arg_idx = 0; OB_SUCC(ret) && arg_idx < expr.arg_cnt_; ++arg_idx) {
      const ObExpr &arg_expr = *expr.args_[arg_idx];
      VectorFormat arg_format = arg_expr.get_format(ctx);
      MULTI_COL_COMPARE_DISPATCH_VECTOR_RES_FORMAT(
          process_multi_columns, arg_format, res_format, arg_idx, expr, ctx,
          filter_ctx.row_selector_, old_row_selector_cnt, new_row_selector_cnt, filter_count);
      old_row_selector_cnt = new_row_selector_cnt;
      new_row_selector_cnt = 0;
    }
    if (OB_SUCC(ret)) {
      filter_ctx.total_count_ += total_count;
      filter_ctx.check_count_ += total_count;
      filter_ctx.filter_count_ += filter_count;
      filter_ctx.collect_sample_info(filter_count, total_count);
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
