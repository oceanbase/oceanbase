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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_vec_msg.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_rpc_proxy.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_rpc_process.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_msg.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"
#include "sql/engine/expr/ob_expr_join_filter.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/ob_operator.h"
#include "share/detect/ob_detect_manager_utils.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;

class SmallHashSetBatchInsertOP
{
public:
  SmallHashSetBatchInsertOP(ObSmallHashSet<false> &sm_hash_set, uint64_t *batch_hash_values)
      : sm_hash_set_(sm_hash_set), batch_hash_values_(batch_hash_values)
  {}
  OB_INLINE int operator()(int64_t batch_i)
  {
    return sm_hash_set_.insert_hash(batch_hash_values_[batch_i]);
  }

private:
  ObSmallHashSet<false> &sm_hash_set_;
  uint64_t *batch_hash_values_;
};

template<typename ResVec>
class InFilterProbeOP
{
public:
  InFilterProbeOP(ObSmallHashSet<false> &sm_hash_set, ResVec *res_vec, uint64_t *right_hash_values,
                  int64_t &total_count, int64_t &filter_count)
      : sm_hash_set_(sm_hash_set), res_vec_(res_vec), right_hash_values_(right_hash_values),
        total_count_(total_count), filter_count_(filter_count)
  {}
  OB_INLINE int operator()(int64_t batch_i)
  {
    bool is_match = false;
    constexpr int64_t is_match_payload = 1;
    total_count_ += 1;
    is_match = sm_hash_set_.test_hash(right_hash_values_[batch_i]);
    if (!is_match) {
      filter_count_++;
      if (std::is_same<ResVec, IntegerUniVec>::value) {
        res_vec_->set_int(batch_i, 0);
      }
    } else {
      if (std::is_same<ResVec, IntegerUniVec>::value) {
        res_vec_->set_int(batch_i, 1);
      } else {
        res_vec_->set_payload(batch_i, &is_match_payload, sizeof(int64_t));
      }
    }
    return OB_SUCCESS;
  }
private:
  ObSmallHashSet<false> &sm_hash_set_;
  ResVec *res_vec_;
  uint64_t *right_hash_values_;
  int64_t &total_count_;
  int64_t &filter_count_;
};


template <typename ResVec>
static int proc_filter_not_active(ResVec *res_vec, const ObBitVector &skip, const EvalBound &bound);

template <>
int proc_filter_not_active<IntegerUniVec>(IntegerUniVec *res_vec, const ObBitVector &skip,
                                          const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBitVector::flip_foreach(
          skip, bound, [&](int64_t idx) __attribute__((always_inline)) {
            res_vec->set_int(idx, 1);
            return OB_SUCCESS;
          }))) {
    LOG_WARN("fail to do for each operation", K(ret));
  }
  return ret;
}

template <>
int proc_filter_not_active<IntegerFixedVec>(IntegerFixedVec *res_vec, const ObBitVector &skip,
                                            const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  uint64_t *data = reinterpret_cast<uint64_t *>(res_vec->get_data());
  MEMSET(data + bound.start(), 1, (bound.range_size() * res_vec->get_length(0)));
  return ret;
}

OB_SERIALIZE_MEMBER(ObRFCmpInfo, ser_cmp_func_, obj_meta_);
OB_SERIALIZE_MEMBER(ObRFRangeFilterVecMsg::MinMaxCellSize, min_datum_buf_size_,
                    max_datum_buf_size_);

OB_DEF_SERIALIZE(ObRFRangeFilterVecMsg)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObRFRangeFilterVecMsg, ObP2PDatahubMsgBase));
  LST_DO_CODE(OB_UNIS_ENCODE,
              lower_bounds_,
              upper_bounds_,
              need_null_cmp_flags_,
              cells_size_,
              build_row_cmp_info_,
              probe_row_cmp_info_,
              query_range_info_);
  return ret;
}

OB_DEF_DESERIALIZE(ObRFRangeFilterVecMsg)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObRFRangeFilterVecMsg, ObP2PDatahubMsgBase));
  LST_DO_CODE(OB_UNIS_DECODE,
              lower_bounds_,
              upper_bounds_,
              need_null_cmp_flags_,
              cells_size_,
              build_row_cmp_info_,
              probe_row_cmp_info_,
              query_range_info_);
  if (OB_FAIL(adjust_cell_size())) {
    LOG_WARN("fail do adjust cell size", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRFRangeFilterVecMsg)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObRFRangeFilterVecMsg, ObP2PDatahubMsgBase));
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              lower_bounds_,
              upper_bounds_,
              need_null_cmp_flags_,
              cells_size_,
              build_row_cmp_info_,
              probe_row_cmp_info_,
              query_range_info_);
  return len;
}


OB_SERIALIZE_MEMBER(ObRowWithHash, row_, hash_val_);

OB_DEF_SERIALIZE(ObRFInFilterVecMsg::ObRFInFilterRowStore)
{
  int ret = OB_SUCCESS;
  int64_t row_cnt = get_row_cnt();
  OB_UNIS_ENCODE(row_cnt);
  OB_UNIS_ENCODE(row_sizes_);
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
    const int64_t row_size = row_sizes_.at(i);
    if (buf_len - pos < row_size) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("failed to serilize");
    } else {
      MEMCPY(buf + pos, serial_rows_.at(i), row_size);
      pos += row_size;
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObRFInFilterVecMsg::ObRFInFilterRowStore)
{
  int ret = OB_SUCCESS;
  int64_t row_cnt = 0;
  OB_UNIS_DECODE(row_cnt);
  OB_UNIS_DECODE(row_sizes_);
  void *alloc_buf = nullptr;
  ObCompactRow *row = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(serial_rows_.reserve(row_cnt))) {
    LOG_WARN("failed to prepare_allocate serial_rows_");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
    const int64_t row_size = row_sizes_.at(i);
    if (OB_ISNULL(alloc_buf = allocator_.alloc(row_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate");
    } else {
      MEMCPY(alloc_buf, buf + pos, row_size);
      pos += row_size;
      row = reinterpret_cast<ObCompactRow *>(alloc_buf);
      if (OB_FAIL(serial_rows_.push_back(row))) {
        LOG_WARN("failed to push_back");
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRFInFilterVecMsg::ObRFInFilterRowStore)
{
  int64_t len = 0;
  int64_t row_cnt = get_row_cnt();
  OB_UNIS_ADD_LEN(row_cnt);
  OB_UNIS_ADD_LEN(row_sizes_);
  for (int64_t i = 0; i < row_cnt; ++i) {
    len += row_sizes_.at(i);
  }
  return len;
}

OB_DEF_SERIALIZE(ObRFInFilterVecMsg)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObRFInFilterVecMsg, ObP2PDatahubMsgBase));
  OB_UNIS_ENCODE(max_in_num_);
  OB_UNIS_ENCODE(need_null_cmp_flags_);
  OB_UNIS_ENCODE(build_row_cmp_info_);
  OB_UNIS_ENCODE(probe_row_cmp_info_);
  OB_UNIS_ENCODE(build_row_meta_);
  if (is_active_) {
    OB_UNIS_ENCODE(row_store_);
  }
  OB_UNIS_ENCODE(hash_funcs_for_insert_);
  OB_UNIS_ENCODE(query_range_info_);
  return ret;
}

OB_DEF_DESERIALIZE(ObRFInFilterVecMsg)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObRFInFilterVecMsg, ObP2PDatahubMsgBase));
  OB_UNIS_DECODE(max_in_num_);
  OB_UNIS_DECODE(need_null_cmp_flags_);
  OB_UNIS_DECODE(build_row_cmp_info_);
  OB_UNIS_DECODE(probe_row_cmp_info_);
  OB_UNIS_DECODE(build_row_meta_);
  if (OB_SUCC(ret) && is_active_) {
    OB_UNIS_DECODE(row_store_);
    int64_t row_cnt = row_store_.get_row_cnt();
    int64_t buckets_cnt = max(row_cnt, 1);
    if (OB_FAIL(rows_set_.create(buckets_cnt * 2,
        "RFDEInFilter",
        "RFDEInFilter"))) {
      LOG_WARN("fail to init in hash set", K(ret));
    } else if (OB_FAIL(sm_hash_set_.init(buckets_cnt, tenant_id_))) {
      LOG_WARN("faield to init small hash set", K(row_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
      ObRFInFilterNode node(&build_row_cmp_info_, &build_row_meta_, row_store_.get_row(i), nullptr);
      if (OB_FAIL(rows_set_.set_refactored(node))) {
        LOG_WARN("fail to insert in filter node", K(ret));
      } else if (OB_FAIL(sm_hash_set_.insert_hash(row_store_.get_hash_value(i, build_row_meta_)))) {
        LOG_WARN("fail to insert hash value into sm_hash_set_", K(ret));
      }
    }
  }
  OB_UNIS_DECODE(hash_funcs_for_insert_);
  OB_UNIS_DECODE(query_range_info_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRFInFilterVecMsg)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObRFInFilterVecMsg, ObP2PDatahubMsgBase));
  OB_UNIS_ADD_LEN(max_in_num_);
  OB_UNIS_ADD_LEN(need_null_cmp_flags_);
  OB_UNIS_ADD_LEN(build_row_cmp_info_);
  OB_UNIS_ADD_LEN(probe_row_cmp_info_);
  OB_UNIS_ADD_LEN(build_row_meta_);
  if (is_active_) {
    OB_UNIS_ADD_LEN(row_store_);
  }
  OB_UNIS_ADD_LEN(hash_funcs_for_insert_);
  OB_UNIS_ADD_LEN(query_range_info_);
  return len;
}


//ObRFRangeFilterVecMsg
ObRFRangeFilterVecMsg::ObRFRangeFilterVecMsg()
: ObP2PDatahubMsgBase(),
  build_row_cmp_info_(allocator_), probe_row_cmp_info_(allocator_),
  lower_bounds_(allocator_), upper_bounds_(allocator_),
  need_null_cmp_flags_(allocator_), cells_size_(allocator_),
  query_range_info_(allocator_), query_range_(), is_query_range_ready_(false),
  query_range_allocator_()
{
}

int ObRFRangeFilterVecMsg::reuse()
{
  int ret = OB_SUCCESS;
  is_empty_ = true;
  int64_t col_cnt = lower_bounds_.count();
  lower_bounds_.reset();
  upper_bounds_.reset();
  cells_size_.reset();
  if (OB_FAIL(lower_bounds_.prepare_allocate(col_cnt))) {
    LOG_WARN("fail to prepare allocate col cnt", K(ret));
  } else if (OB_FAIL(upper_bounds_.prepare_allocate(col_cnt))) {
    LOG_WARN("fail to prepare allocate col cnt", K(ret));
  } else if (OB_FAIL(cells_size_.prepare_allocate(col_cnt))) {
    LOG_WARN("fail to prepare allocate col cnt", K(ret));
  }
  (void)reuse_query_range();
  return ret;
}

int ObRFRangeFilterVecMsg::assign(const ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  const ObRFRangeFilterVecMsg &other_msg = static_cast<const ObRFRangeFilterVecMsg &>(msg);
  if (OB_FAIL(ObP2PDatahubMsgBase::assign(msg))) {
    LOG_WARN("failed to assign base data", K(ret));
  } else if (OB_FAIL(lower_bounds_.assign(other_msg.lower_bounds_))) {
    LOG_WARN("fail to assign lower bounds", K(ret));
  } else if (OB_FAIL(upper_bounds_.assign(other_msg.upper_bounds_))) {
    LOG_WARN("fail to assign upper bounds", K(ret));
  } else if (OB_FAIL(need_null_cmp_flags_.assign(other_msg.need_null_cmp_flags_))) {
    LOG_WARN("failed to assign cmp flags", K(ret));
  } else if (OB_FAIL(cells_size_.assign(other_msg.cells_size_))) {
    LOG_WARN("failed to assign cell size", K(ret));
  } else if (OB_FAIL(adjust_cell_size())) {
    LOG_WARN("fail to adjust cell size", K(ret));
  } else if (OB_FAIL(build_row_cmp_info_.assign(other_msg.build_row_cmp_info_))) {
    LOG_WARN("fail to assign build_row_cmp_info_", K(ret));
  } else if (OB_FAIL(probe_row_cmp_info_.assign(other_msg.probe_row_cmp_info_))) {
    LOG_WARN("fail to assign probe_row_cmp_info_", K(ret));
  } else if (OB_FAIL(query_range_info_.assign(other_msg.query_range_info_))) {
    LOG_WARN("fail to assign query_range_info_", K(ret));
  }
  return ret;
}

int ObRFRangeFilterVecMsg::deep_copy_msg(ObP2PDatahubMsgBase *&new_msg_ptr)
{
  int ret = OB_SUCCESS;
  ObRFRangeFilterVecMsg *rf_msg = nullptr;
  ObMemAttr attr(tenant_id_, "RANGEVECMSG");
  if (OB_FAIL(PX_P2P_DH.alloc_msg<ObRFRangeFilterVecMsg>(attr, rf_msg))) {
    LOG_WARN("fail to alloc rf msg", K(ret));
  } else if (OB_FAIL(rf_msg->assign(*this))) {
    LOG_WARN("fail to assign rf msg", K(ret));
  } else {
    for (int i = 0; i < rf_msg->lower_bounds_.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(rf_msg->lower_bounds_.at(i).deep_copy(lower_bounds_.at(i),
          rf_msg->get_allocator()))) {
        LOG_WARN("fail to deep copy rf msg", K(ret));
      } else if (OB_FAIL(rf_msg->upper_bounds_.at(i).deep_copy(upper_bounds_.at(i),
          rf_msg->get_allocator()))) {
        LOG_WARN("fail to deep copy rf msg", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(rf_msg->build_row_cmp_info_.assign(build_row_cmp_info_))) {
      LOG_WARN("fail to assign build_row_cmp_info_", K(ret));
    } else if (OB_FAIL(rf_msg->probe_row_cmp_info_.assign(probe_row_cmp_info_))) {
      LOG_WARN("fail to assign probe_row_cmp_info_", K(ret));
    }
    if (OB_SUCC(ret)) {
      new_msg_ptr = rf_msg;
    }
  }
  return ret;
}

int ObRFRangeFilterVecMsg::merge(ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  ObRFRangeFilterVecMsg &range_msg = static_cast<ObRFRangeFilterVecMsg &>(msg);
  CK(range_msg.lower_bounds_.count() == lower_bounds_.count() &&
     range_msg.upper_bounds_.count() == upper_bounds_.count());
  if (OB_FAIL(ret)) {
    LOG_WARN("unexpected bounds count", K(lower_bounds_.count()),
             K(range_msg.lower_bounds_.count()));
  } else if (range_msg.is_empty_) {
    /*do nothing*/
  } else {
    ObSpinLockGuard guard(lock_);
    if (OB_FAIL(merge_min(range_msg.lower_bounds_))) {
      LOG_WARN("fail to get min lower bounds", K(ret));
    } else if (OB_FAIL(merge_max(range_msg.upper_bounds_))) {
      LOG_WARN("fail to get max lower bounds", K(ret));
    } else if (is_empty_) {
      is_empty_ = false;
    }
  }
  return ret;
}

int ObRFRangeFilterVecMsg::adjust_cell_size()
{
  int ret = OB_SUCCESS;
  CK(cells_size_.count() == lower_bounds_.count() &&
     lower_bounds_.count() == upper_bounds_.count());
  for (int i = 0; OB_SUCC(ret) && i < cells_size_.count(); ++i) {
    cells_size_.at(i).min_datum_buf_size_ =
        std::min(cells_size_.at(i).min_datum_buf_size_, (int64_t)lower_bounds_.at(i).len_);
    cells_size_.at(i).max_datum_buf_size_ =
        std::min(cells_size_.at(i).max_datum_buf_size_, (int64_t)upper_bounds_.at(i).len_);
  }
  return ret;
}

int ObRFRangeFilterVecMsg::dynamic_copy_cell(const ObDatum &src, ObDatum &target,
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
      if (OB_ISNULL(buff_ptr = static_cast<char*>(allocator_.alloc(need_size)))) {
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

int ObRFRangeFilterVecMsg::merge_min(ObIArray<ObDatum> &vals)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < vals.count() && OB_SUCC(ret); ++i) {
    // null value is also suitable
    if (OB_FAIL(update_min(build_row_cmp_info_.at(i), lower_bounds_.at(i),
        vals.at(i), cells_size_.at(i).min_datum_buf_size_))) {
      LOG_WARN("fail to compare value", K(ret));
    }
  }
  return ret;
}

int ObRFRangeFilterVecMsg::merge_max(ObIArray<ObDatum> &vals)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < vals.count() && OB_SUCC(ret); ++i) {
    // null value is also suitable
    if (OB_FAIL(update_max(build_row_cmp_info_.at(i), upper_bounds_.at(i),
        vals.at(i), cells_size_.at(i).max_datum_buf_size_))) {
      LOG_WARN("fail to compare value", K(ret));
    }
  }
  return ret;
}

int ObRFRangeFilterVecMsg::update_min(ObRFCmpInfo &cmp_info, ObDatum &l, ObDatum &r,
                                      int64_t &cell_size)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  // when [null, null] merge [a, b], the expect result in mysql mode is [null, b]
  // the lower bound l, with ptr==NULL and null_==true, should not be covered by a
  //
  // the reason we remove the OB_ISNULL(l.ptr_) condition is that when l is a empty char with
  // l.ptr=0x0 and l.len=0 and null_=false, it should not be corver by r directly
  if (is_empty_) {
    if (OB_FAIL(dynamic_copy_cell(r, l, cell_size))) {
      LOG_WARN("fail to deep copy datum");
    }
  } else if (OB_FAIL(cmp_info.cmp_func_(cmp_info.obj_meta_, cmp_info.obj_meta_,
      l.ptr_, l.len_, l.is_null(),
      r.ptr_, r.len_, r.is_null(),
      cmp))) {
    LOG_WARN("fail to cmp", K(ret));
  } else if (cmp > 0) {
    if (OB_FAIL(dynamic_copy_cell(r, l, cell_size))) {
      LOG_WARN("fail to deep copy datum");
    }
  }
  return ret;
}

int ObRFRangeFilterVecMsg::update_max(ObRFCmpInfo &cmp_info, ObDatum &l, ObDatum &r,
                                      int64_t &cell_size)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (is_empty_) {
    if (OB_FAIL(dynamic_copy_cell(r, l, cell_size))) {
      LOG_WARN("fail to deep copy datum");
    }
  } else if (OB_FAIL(cmp_info.cmp_func_(cmp_info.obj_meta_, cmp_info.obj_meta_,
      l.ptr_, l.len_, l.is_null(),
      r.ptr_, r.len_, r.is_null(),
      cmp))) {
    LOG_WARN("fail to cmp value", K(ret));
  } else if (cmp < 0) {
    if (OB_FAIL(dynamic_copy_cell(r, l, cell_size))) {
      LOG_WARN("fail to deep copy datum");
    }
  }
  return ret;
}

int ObRFRangeFilterVecMsg::probe_with_lower(int64_t col_idx, ObDatum &prob_datum, int &cmp_min)
{
  int ret = OB_SUCCESS;
  ObRFCmpInfo &probe_meta = probe_row_cmp_info_.at(col_idx);
  ObRFCmpInfo &build_meta = build_row_cmp_info_.at(col_idx);
  ObDatum &build_datum = lower_bounds_.at(col_idx);
  if (OB_FAIL(probe_meta.cmp_func_(probe_meta.obj_meta_, build_meta.obj_meta_,
      prob_datum.ptr_, prob_datum.len_, prob_datum.is_null(),
      build_datum.ptr_, build_datum.len_, build_datum.is_null(), cmp_min))) {
    LOG_WARN("fail to compare value", K(ret));
  }
  return ret;
}

int ObRFRangeFilterVecMsg::probe_with_upper(int64_t col_idx, ObDatum &prob_datum, int &cmp_max)
{
  int ret = OB_SUCCESS;
  ObRFCmpInfo &probe_meta = probe_row_cmp_info_.at(col_idx);
  ObRFCmpInfo &build_meta = build_row_cmp_info_.at(col_idx);
  ObDatum &build_datum = upper_bounds_.at(col_idx);
  if (OB_FAIL(probe_meta.cmp_func_(probe_meta.obj_meta_, build_meta.obj_meta_,
      prob_datum.ptr_, prob_datum.len_, prob_datum.is_null(),
      build_datum.ptr_, build_datum.len_, build_datum.is_null(), cmp_max))) {
    LOG_WARN("fail to compare value", K(ret));
  }
  return ret;
}

int ObRFRangeFilterVecMsg::insert_by_row_vector(
    const ObBatchRows *child_brs,
    const common::ObIArray<ObExpr *> &expr_array,
    const common::ObHashFuncs &hash_funcs,
    const ObExpr *calc_tablet_id_expr,
    ObEvalCtx &eval_ctx,
    uint64_t *batch_hash_values)
{
  UNUSED(batch_hash_values);
  UNUSED(calc_tablet_id_expr);
  int ret = OB_SUCCESS;
  if (child_brs->size_ > 0) {
    EvalBound bound(child_brs->size_, child_brs->all_rows_active_);
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_array.count(); ++i) {
      ObExpr *expr = expr_array.at(i); // expr ptr check in cg, not check here
      if (OB_FAIL(expr->eval_vector(eval_ctx, *(child_brs->skip_), bound))) {
        LOG_WARN("eval_vector failed", K(ret));
      }
    }
    ObDatum datum;
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
    batch_info_guard.set_batch_size(child_brs->size_);
    for (int64_t batch_i = 0; OB_SUCC(ret) && batch_i < child_brs->size_; ++batch_i) {
      if (child_brs->skip_->at(batch_i)) {
        continue;
      }
      batch_info_guard.set_batch_idx(batch_i);
      if (is_empty_) {
        bool ignore_null = false;
        for (int64_t arg_i = 0; OB_SUCC(ret) && arg_i < expr_array.count(); ++arg_i) {
          ObExpr *expr = expr_array.at(arg_i);
          ObIVector *arg_vec = expr->get_vector(eval_ctx);
          if (arg_vec->is_null(batch_i) && !need_null_cmp_flags_.at(arg_i)) {
            ignore_null = true;
            break;
          } else {
            datum.ptr_ = arg_vec->get_payload(batch_i);
            datum.len_ = arg_vec->get_length(batch_i);
            datum.null_ = arg_vec->is_null(batch_i) ? 1 : 0;
            if (OB_FAIL(dynamic_copy_cell(datum, lower_bounds_.at(arg_i),
                                          cells_size_.at(arg_i).min_datum_buf_size_))) {
              LOG_WARN("fail to deep copy datum", K(ret));
            } else if (OB_FAIL(dynamic_copy_cell(datum, upper_bounds_.at(arg_i),
                                                 cells_size_.at(arg_i).max_datum_buf_size_))) {
              LOG_WARN("fail to deep copy datum", K(ret));
            }
          }
        }
        if (OB_SUCC(ret) && !ignore_null) {
          is_empty_ = false;
        }
      } else {
        for (int64_t arg_i = 0; OB_SUCC(ret) && arg_i < expr_array.count(); ++arg_i) {
          ObExpr *expr = expr_array.at(arg_i);
          ObIVector *arg_vec = expr->get_vector(eval_ctx);
          if (arg_vec->is_null(batch_i) && !need_null_cmp_flags_.at(arg_i)) {
            /*do nothing*/
            break;
          } else {
            datum.ptr_ = arg_vec->get_payload(batch_i);
            datum.len_ = arg_vec->get_length(batch_i);
            datum.null_ = arg_vec->is_null(batch_i) ? 1 : 0;
            if (OB_FAIL(update_min(build_row_cmp_info_.at(arg_i),
                lower_bounds_.at(arg_i), datum, cells_size_.at(arg_i).min_datum_buf_size_))) {
              LOG_WARN("failed to update_min", K(ret));
            } else if (OB_FAIL(update_max(build_row_cmp_info_.at(arg_i),
                upper_bounds_.at(arg_i), datum, cells_size_.at(arg_i).max_datum_buf_size_))) {
              LOG_WARN("failed to update_max", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObRFRangeFilterVecMsg::might_contain(const ObExpr &expr,
    ObEvalCtx &ctx,
    ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx,
    ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  int cmp_min = 0;
  int cmp_max = 0;
  bool is_match = true;
  if (OB_UNLIKELY(is_empty_)) {
    res.set_int(0);
    filter_ctx.filter_count_++;
    filter_ctx.check_count_++;
  } else {
    for (int i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
      if (OB_FAIL(expr.args_[i]->eval(ctx, datum))) {
        LOG_WARN("failed to eval datum", K(ret));
      } else {
        cmp_min = 0;
        cmp_max = 0;
        if (OB_FAIL(probe_with_lower(i, *datum, cmp_min))) {
          LOG_WARN("fail to probe_with_lower", K(ret));
        } else if (cmp_min < 0) {
          is_match = false;
          break;
        } else if (OB_FAIL(probe_with_upper(i, *datum, cmp_max))) {
          LOG_WARN("fail to probe_with_upper", K(ret));
        } else if (cmp_max > 0) {
          is_match = false;
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!is_match) {
        filter_ctx.filter_count_++;
      }
      filter_ctx.check_count_++;
      res.set_int(is_match ? 1 : 0);
      filter_ctx.collect_sample_info(!is_match, 1);
    }
  }
  return ret;
}

int ObRFRangeFilterVecMsg::do_might_contain_batch(const ObExpr &expr,
    ObEvalCtx &ctx,
    const ObBitVector &skip,
    const int64_t batch_size,
    ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx) {
  int ret = OB_SUCCESS;
  int64_t filter_count = 0;
  int64_t total_count = 0;
  ObDatum *results = expr.locate_batch_datums(ctx);
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(batch_size);
  for (int idx = 0; OB_SUCC(ret) && idx < expr.arg_cnt_; ++idx) {
    if (OB_FAIL(expr.args_[idx]->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("eval failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int cmp_min = 0;
    int cmp_max = 0;
    ObDatum *datum = nullptr;
    bool is_match = true;
    for (int64_t batch_i = 0; OB_SUCC(ret) && batch_i < batch_size; ++batch_i) {
      if (skip.at(batch_i)) {
        continue;
      }
      cmp_min = 0;
      cmp_max = 0;
      is_match = true;
      total_count++;
      batch_info_guard.set_batch_idx(batch_i);
      for (int arg_i = 0; OB_SUCC(ret) && arg_i < expr.arg_cnt_; ++arg_i) {
        datum = &expr.args_[arg_i]->locate_expr_datum(ctx, batch_i);
        if (OB_FAIL(probe_with_lower(arg_i, *datum, cmp_min))) {
          LOG_WARN("fail to probe_with_lower", K(ret));
        } else if (cmp_min < 0) {
          filter_count++;
          is_match = false;
          break;
        } else if (OB_FAIL(probe_with_upper(arg_i, *datum, cmp_max))) {
          LOG_WARN("fail to probe_with_upper", K(ret));
        } else if (cmp_max > 0) {
          filter_count++;
          is_match = false;
          break;
        }
      }
      results[batch_i].set_int(is_match ? 1 : 0);
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

int ObRFRangeFilterVecMsg::might_contain_batch(
      const ObExpr &expr,
      ObEvalCtx &ctx,
      const ObBitVector &skip,
      const int64_t batch_size,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx)
{
  int ret = OB_SUCCESS;
  ObDatum *results = expr.locate_batch_datums(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  if (OB_UNLIKELY(is_empty_)) {
    for (int64_t i = 0; i < batch_size; i++) {
      results[i].set_int(0);
    }
  } else if (OB_FAIL(do_might_contain_batch(expr, ctx, skip, batch_size, filter_ctx))) {
    LOG_WARN("failed to do_might_contain_batch");
  }
  if (OB_SUCC(ret)) {
    eval_flags.set_all(batch_size);
  }
  return ret;
}

int ObRFRangeFilterVecMsg::do_might_contain_vector(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    const ObBitVector &skip,
    const EvalBound &bound,
    ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx)
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

  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    ObExpr *e = expr.args_[i];
    if (OB_FAIL(e->eval_vector(ctx, skip, bound))) {
      LOG_WARN("evaluate vector failed", K(ret), K(*e));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_size(batch_size);
    int cmp_min = 0;
    int cmp_max = 0;
    bool is_match = true;
    const int64_t is_match_payload = 1; // for VEC_FIXED set set_payload, always 1
    ObDatum datum;
    for (int64_t batch_i  = bound.start(); batch_i < bound.end() && OB_SUCC(ret); ++batch_i) {
      if (skip.at(batch_i)) {
        continue;
      } else {
        total_count++;
        eval_flags.set(batch_i);
        batch_info_guard.set_batch_idx(batch_i);
        is_match = true;
        for (int arg_i = 0; OB_SUCC(ret) && arg_i < expr.arg_cnt_; ++arg_i) {
          cmp_min = 0;
          cmp_max = 0;
          ObIVector *arg_vec = expr.args_[arg_i]->get_vector(ctx);
          datum.ptr_ = arg_vec->get_payload(batch_i);
          datum.len_ = arg_vec->get_length(batch_i);
          datum.null_ = arg_vec->is_null(batch_i) ? 1 : 0;
          if (OB_FAIL(probe_with_lower(arg_i, datum, cmp_min))) {
            LOG_WARN("fail to probe_with_lower", K(ret));
          } else if (cmp_min < 0) {
            is_match = false;
            filter_count++;
            break;
          } else if (OB_FAIL(probe_with_upper(arg_i, datum, cmp_max))) {
            LOG_WARN("fail to probe_with_upper", K(ret));
          } else if (cmp_max > 0) {
            is_match = false;
            filter_count++;
            break;
          }
        }
        if (OB_SUCC(ret)) {
          if (VEC_UNIFORM == res_format) {
            IntegerUniVec *res_vec = static_cast<IntegerUniVec *>(expr.get_vector(ctx));
            res_vec->set_int(batch_i, is_match ? 1 : 0);
          } else if (VEC_FIXED == res_format) {
            IntegerFixedVec *res_vec = static_cast<IntegerFixedVec *>(expr.get_vector(ctx));
            if (is_match) {
              res_vec->set_payload(batch_i, &is_match_payload, sizeof(int64_t));
            } else {
              // do nothing, already set not match in preset_not_match
            }
          }
        }
      }
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

int ObRFRangeFilterVecMsg::might_contain_vector(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    const ObBitVector &skip,
    const EvalBound &bound,
    ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx)
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
  } else if (OB_FAIL(do_might_contain_vector(expr, ctx, skip, bound, filter_ctx))) {
    LOG_WARN("fail to do might contain vector");
  }
  return ret;
}

int ObRFRangeFilterVecMsg::prepare_query_range()
{
  (void)reuse_query_range();
  int ret = OB_SUCCESS;
  if (!query_range_info_.can_extract()) {
    is_query_range_ready_ = false;
  } else if (is_empty_) {
    // make empty range
    if (OB_FAIL(fill_empty_query_range(query_range_info_, query_range_allocator_, query_range_))) {
      LOG_WARN("faild to fill_empty_query_range");
    } else {
      is_query_range_ready_ = true;
    }
  } else {
    // only extract the first column
    int64_t prefix_col_idx = query_range_info_.prefix_col_idxs_.at(0);
    int64_t range_column_cnt = query_range_info_.range_column_cnt_;
    const ObObjMeta &prefix_col_obj_meta = query_range_info_.prefix_col_obj_metas_.at(0);

    query_range_.table_id_ = query_range_info_.table_id_;
    query_range_.border_flag_.set_inclusive_start();
    query_range_.border_flag_.set_inclusive_end();

    const ObDatum &lower_bound = lower_bounds_.at(prefix_col_idx);
    const ObDatum &upper_bound = upper_bounds_.at(prefix_col_idx);
    ObObj *start = NULL;
    ObObj *end = NULL;
    if (OB_ISNULL(start = static_cast<ObObj *>(
                      query_range_allocator_.alloc(sizeof(ObObj) * range_column_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory for start_obj failed", K(ret));
    } else if (OB_ISNULL(end = static_cast<ObObj *>(
                             query_range_allocator_.alloc(sizeof(ObObj) * range_column_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory for end_obj failed", K(ret));
    } else {
      new(start) ObObj();
      new(end) ObObj();
      lower_bound.to_obj(*start, prefix_col_obj_meta);
      upper_bound.to_obj(*end, prefix_col_obj_meta);
      // fill left coloumn with (min, max)
      for (int64_t i = 1; i < range_column_cnt; ++i) {
        new(start + i) ObObj();
        new(end + i) ObObj();
        (start + i)->set_min_value();
        (end + i)->set_max_value();
      }
      ObRowkey start_key(start, range_column_cnt);
      ObRowkey end_key(end, range_column_cnt);
      query_range_.start_key_ = start_key;
      query_range_.end_key_ = end_key;
    }

    if (OB_SUCC(ret)) {
      is_query_range_ready_ = true;
    }
  }
  LOG_TRACE("range filter prepare query range", K(ret), K(is_query_range_ready_), K(query_range_));
  return ret;
}

void ObRFRangeFilterVecMsg::after_process()
{
  // prepare_query_range can be failed, but rf still worked
  (void)prepare_query_range();
}

int ObRFRangeFilterVecMsg::try_extract_query_range(bool &has_extract, ObIArray<ObNewRange> &ranges)
{
  int ret = OB_SUCCESS;
  if (!is_query_range_ready_) {
    has_extract = false;
  } else {
    // overwrite ranges
    ranges.reset();
    if (OB_FAIL(ranges.push_back(query_range_))) {
      LOG_WARN("failed to push_back range");
    } else {
      has_extract = true;
    }
  }
  return ret;
}
int ObRFRangeFilterVecMsg::prepare_storage_white_filter_data(
    ObDynamicFilterExecutor &dynamic_filter, ObEvalCtx &eval_ctx, ObRuntimeFilterParams &params,
    bool &is_data_prepared)
{
  int ret = OB_SUCCESS;
  int col_idx = dynamic_filter.get_col_idx();
  if (is_empty_) {
    dynamic_filter.set_filter_action(DynamicFilterAction::FILTER_ALL);
    is_data_prepared = true;
  } else if (OB_FAIL(params.push_back(lower_bounds_.at(col_idx)))) {
    LOG_WARN("failed to push back lower_bound");
  } else if (OB_FAIL(params.push_back(upper_bounds_.at(col_idx)))) {
    LOG_WARN("failed to push back upper_bound");
  } else {
    dynamic_filter.set_filter_val_meta(build_row_cmp_info_.at(col_idx).obj_meta_);
    is_data_prepared = true;
  }
  return ret;
}


// end ObRFRangeFilterVecMsg

// ObRFInFilterVecMsg

int ObRFInFilterVecMsg::ObRFInFilterRowStore::deep_copy_one_row(const ObCompactRow *src,
    ObCompactRow *&dest,
    common::ObIAllocator &allocator,
    int64_t row_size)
{
  int ret = OB_SUCCESS;
  void *alloc_buf = nullptr;
  if (OB_ISNULL(alloc_buf = allocator.alloc(row_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate");
  } else {
    const char *buf = reinterpret_cast<const char *>(src);
    MEMCPY(alloc_buf, buf, row_size);
    dest = reinterpret_cast<ObCompactRow *>(alloc_buf);
  }
  return ret;
}

int ObRFInFilterVecMsg::ObRFInFilterRowStore::add_row(ObCompactRow *new_row, int64_t row_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serial_rows_.push_back(new_row))) {
    LOG_WARN("failed to push back new row");
  } else if (OB_FAIL(row_sizes_.push_back(row_size))) {
    LOG_WARN("failed to push back row_size");
  }
  return ret;
}

int ObRFInFilterVecMsg::ObRFInFilterRowStore::create_and_add_row(
    const common::ObIArray<ObExpr *> &exprs, const RowMeta &row_meta, const int64_t row_size,
    ObEvalCtx &ctx, ObCompactRow *&new_row, uint64_t hash_val)
{
  int ret = OB_SUCCESS;
  const int64_t batch_idx = ctx.get_batch_idx();

  void *alloc_buf = nullptr;
  ObCompactRow *row = nullptr;
  if (OB_ISNULL(alloc_buf = allocator_.alloc(row_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate");
  } else {
    row = reinterpret_cast<ObCompactRow *>(alloc_buf);
    (void) row->init(row_meta);
    row->set_row_size(row_size);
  }

  for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); ++i) {
    ObExpr *expr = exprs.at(i);
    ObIVector *vec = expr->get_vector(ctx);
    OZ(vec->to_row(row_meta, row, batch_idx, i));
  }
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(row->extra_payload<uint64_t>(row_meta) = hash_val)) {
  } else if (OB_FAIL(serial_rows_.push_back(row))) {
    LOG_WARN("failed to push back row");
  } else if (OB_FAIL(row_sizes_.push_back(row_size))) {
    LOG_WARN("failed to push back row_size");
  } else {
    new_row = row;
  }
  return ret;
}

int ObRFInFilterVecMsg::ObRFInFilterRowStore::assign(const ObRFInFilterRowStore &other)
{
  int ret = OB_SUCCESS;
  int64_t row_size = 0;
  void *alloc_buf = nullptr;
  ObCompactRow *row = nullptr;
  int64_t row_cnt = other.get_row_cnt();
  if (OB_FAIL(serial_rows_.reserve(row_cnt))) {
    LOG_WARN("failed to reserve serial_rows_");
  } else if (OB_FAIL(row_sizes_.assign(other.row_sizes_))) {
    LOG_WARN("failed to assign row_sizes_");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
    row_size = other.row_sizes_.at(i);
    if (OB_ISNULL(alloc_buf = allocator_.alloc(row_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate");
    } else {
      char *buf = reinterpret_cast<char *>(other.serial_rows_.at(i));
      MEMCPY(alloc_buf, buf, row_size);
      row = reinterpret_cast<ObCompactRow *>(alloc_buf);
      if (OB_FAIL(serial_rows_.push_back(row))) {
        LOG_WARN("failed to push_back");
      }
    }
  }
  return ret;
}

void ObRFInFilterVecMsg::ObRFInFilterRowStore::reset()
{
  serial_rows_.reset();
  row_sizes_.reset();
}

int ObRFInFilterVecMsg::assign(const ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  const ObRFInFilterVecMsg &other_msg = static_cast<const ObRFInFilterVecMsg &>(msg);
  int64_t bucket_cnt = max(other_msg.row_store_.get_row_cnt(), 1);
  if (OB_FAIL(ObP2PDatahubMsgBase::assign(msg))) {
    LOG_WARN("failed to assign base data", K(ret));
  } else if (OB_FAIL(need_null_cmp_flags_.assign(other_msg.need_null_cmp_flags_))) {
    LOG_WARN("failed to assign filter indexes", K(ret));
  } else if (OB_FAIL(build_row_cmp_info_.assign(other_msg.build_row_cmp_info_))) {
    LOG_WARN("fail to assign build_row_cmp_info_", K(ret));
  } else if (OB_FAIL(probe_row_cmp_info_.assign(other_msg.probe_row_cmp_info_))) {
    LOG_WARN("fail to assign probe_row_cmp_info_", K(ret));
  } else if (OB_FAIL(build_row_meta_.deep_copy(other_msg.build_row_meta_, build_row_meta_.allocator_))) {
    LOG_WARN("fail to deep copy row meta", K(ret));
  } else if (OB_FAIL(row_store_.assign(other_msg.row_store_))) {
    LOG_WARN("fail to assign row_store_", K(ret));
  } else if (OB_FAIL(rows_set_.create(bucket_cnt * 2, "RFCPInFilter", "RFCPInFilter"))) {
    LOG_WARN("fail to init in hash set", K(ret));
  } else if (OB_FAIL(sm_hash_set_.init(bucket_cnt, tenant_id_))) {
    LOG_WARN("failed to init sm_hash_set_", K(other_msg.row_store_.get_row_cnt()));
  } else if (OB_FAIL(hash_funcs_for_insert_.assign(other_msg.hash_funcs_for_insert_))) {
    LOG_WARN("fail to assign hash_funcs_for_insert_", K(ret));
  } else if (OB_FAIL(query_range_info_.assign(other_msg.query_range_info_))) {
    LOG_WARN("fail to assign query_range_info_", K(ret));
  } else {
    max_in_num_ = other_msg.max_in_num_;
    int64_t row_cnt = other_msg.row_store_.get_row_cnt();
    if (0 == row_cnt) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
        ObRFInFilterNode node(&build_row_cmp_info_, &build_row_meta_, row_store_.get_row(i),
                              nullptr);
        if (OB_FAIL(rows_set_.set_refactored(node))) {
          LOG_WARN("fail to insert in filter node", K(ret));
        } else if (OB_FAIL(sm_hash_set_.insert_hash(row_store_.get_hash_value(i, build_row_meta_)))) {
          LOG_WARN("fail to insert hash value into sm_hash_set_", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRFInFilterVecMsg::deep_copy_msg(ObP2PDatahubMsgBase *&new_msg_ptr)
{
  int ret = OB_SUCCESS;
  ObRFInFilterVecMsg *in_msg = nullptr;
  ObMemAttr attr(tenant_id_, "INVECMSG");
  if (OB_FAIL(PX_P2P_DH.alloc_msg<ObRFInFilterVecMsg>(attr, in_msg))) {
    LOG_WARN("fail to alloc rf msg", K(ret));
  } else if (OB_FAIL(in_msg->assign(*this))) {
    LOG_WARN("fail to assign rf msg", K(ret));
  }
  if (OB_SUCC(ret)) {
    new_msg_ptr = in_msg;
  }
  return ret;
}

int ObRFInFilterVecMsg::insert_by_row_vector(
    const ObBatchRows *child_brs,
    const common::ObIArray<ObExpr *> &expr_array,
    const common::ObHashFuncs &hash_funcs,
    const ObExpr *calc_tablet_id_expr,
    ObEvalCtx &eval_ctx,
    uint64_t *batch_hash_values)
{
  UNUSED(calc_tablet_id_expr);
  int ret = OB_SUCCESS;
  uint64_t seed = ObExprJoinFilter::JOIN_FILTER_SEED;

  if (child_brs->size_ > 0 && is_active_) {
    EvalBound bound(child_brs->size_, child_brs->all_rows_active_);
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_array.count(); ++i) {
      ObExpr *expr = expr_array.at(i); // expr ptr check in cg, not check here
      if (OB_FAIL(expr->eval_vector(eval_ctx, *(child_brs->skip_), bound))) {
        LOG_WARN("eval_vector failed", K(ret));
      } else {
        const bool is_batch_seed = (i > 0);
        ObIVector *arg_vec = expr->get_vector(eval_ctx);
        arg_vec->murmur_hash_v3(*expr, batch_hash_values, *(child_brs->skip_), bound,
                                is_batch_seed ? batch_hash_values : &seed, is_batch_seed);
      }
    }

    SmallHashSetBatchInsertOP sm_hash_set_batch_ins_op(sm_hash_set_, batch_hash_values);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(
                   ObBitVector::flip_foreach(*child_brs->skip_, bound, sm_hash_set_batch_ins_op))) {
      LOG_WARN("failed insert batch_hash_values into sm_hash_set_");
    }

    ObRowWithHash &cur_row = cur_row_with_hash_;
    ObDatum datum;
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
    batch_info_guard.set_batch_size(child_brs->size_);
    for (int64_t batch_i = 0; OB_SUCC(ret) && is_active_ && batch_i < child_brs->size_; ++batch_i) {
      if (child_brs->skip_->at(batch_i)) {
        continue;
      }
      batch_info_guard.set_batch_idx(batch_i);
      bool ignore_null = false;
      for (int64_t arg_i = 0; OB_SUCC(ret) && arg_i < expr_array.count(); ++arg_i) {
        ObExpr *expr = expr_array.at(arg_i);
        ObIVector *arg_vec = expr->get_vector(eval_ctx);
        if (arg_vec->is_null(batch_i) && !need_null_cmp_flags_.at(arg_i)) {
          ignore_null = true;
          break;
        } else {
          datum.ptr_ = arg_vec->get_payload(batch_i);
          datum.len_ = arg_vec->get_length(batch_i);
          datum.null_ = arg_vec->is_null(batch_i) ? 1 : 0;
          cur_row.row_.at(arg_i) = (datum);
          cur_row.hash_val_ = batch_hash_values[batch_i];
        }
      }
      if (OB_SUCC(ret) && !ignore_null) {
        ObRFInFilterNode node(&build_row_cmp_info_, &build_row_meta_, nullptr /*compact_row*/,
                              &cur_row);
        if (OB_FAIL(try_insert_node(node, expr_array, eval_ctx))) {
          LOG_WARN("fail to insert node", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRFInFilterVecMsg::try_insert_node(ObRFInFilterNode &node,
    const common::ObIArray<ObExpr *> &exprs,
    ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rows_set_.exist_refactored(node))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (row_store_.get_row_cnt() > max_in_num_) {
        is_active_ = false;
      } else if (OB_FAIL(append_node(node, exprs, ctx))) {
        LOG_WARN("fail to append node");
      } else if (is_empty_) {
        is_empty_ = false;
      }
    } else if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check node", K(ret));
    }
  }
  return ret;
}

int ObRFInFilterVecMsg::try_merge_node(ObRFInFilterNode &node, int64_t row_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rows_set_.exist_refactored(node))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (row_store_.get_row_cnt() > max_in_num_) {
        is_active_ = false;
      } else if (OB_FAIL(append_node(node, row_size))) {
        LOG_WARN("fail to append node");
      } else if (is_empty_) {
        is_empty_ = false;
      }
    } else if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check node");
    }
  }
  return ret;
}

int ObRFInFilterVecMsg::append_node(ObRFInFilterNode &node,
    const common::ObIArray<ObExpr *> &exprs,
    ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObCompactRow *new_row = nullptr;
  int64_t row_size = 0;
  if (OB_FAIL(ObTempRowStore::RowBlock::calc_row_size(exprs, build_row_meta_, ctx, row_size))) {
    LOG_WARN("fail calc_row_size");
  } else if (OB_FAIL(row_store_.create_and_add_row(exprs, build_row_meta_, row_size, ctx,
                                                   new_row, node.row_with_hash_->hash_val_))) {
    LOG_WARN("fail to deep copy one row");
  } else if (FALSE_IT(node.switch_compact_row(new_row))) {
  } else if (OB_FAIL(rows_set_.set_refactored(node))) {
    LOG_WARN("fail to insert in filter node", K(ret));
  }
  return ret;
}

int ObRFInFilterVecMsg::append_node(ObRFInFilterNode &node, int64_t row_size)
{
  int ret = OB_SUCCESS;
  ObCompactRow *new_row = nullptr;
  if (OB_FAIL(ObRFInFilterRowStore::deep_copy_one_row(node.compact_row_, new_row, allocator_,
                                                      row_size))) {
    LOG_WARN("fail to deep copy one row");
  } else if (FALSE_IT(node.switch_compact_row(new_row))) {
  } else if (OB_FAIL(row_store_.add_row(new_row, row_size))) {
    LOG_WARN("failed to add row to row_store_");
  } else if (OB_FAIL(rows_set_.set_refactored(node))) {
    LOG_WARN("fail to insert in filter node", K(ret));
  }
  return ret;
}

int ObRowWithHash::assign(const ObRowWithHash &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_.init(other.row_.count()))) {
    LOG_WARN("failed to init row_");
  } else {
    hash_val_ = other.hash_val_;
    ObDatum datum;
    for (int i = 0; i < other.row_.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(datum.deep_copy(other.row_.at(i), allocator_))) {
        LOG_WARN("fail to deep copy datum", K(ret));
      } else if (OB_FAIL(row_.push_back(datum))) {
        LOG_WARN("fail to push back datum", K(ret));
      }
    }
  }
  return ret;
}

int ObRFInFilterVecMsg::ObRFInFilterNode::hash(uint64_t &hash_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(compact_row_)) {
    // if compact_row_ is not null, means now is insert(insert into hashset)/merge process
    hash_ret = compact_row_->extra_payload<uint64_t>(*row_meta_);
  } else if(OB_NOT_NULL(row_with_hash_)) {
    // if row_with_hash_ is not null, means now is insert(compare whether node exsit)
    //  or probe process(compare whether node exsit)
    hash_ret = row_with_hash_->hash_val_;
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

// the ObRFInFilterNode stores in ObRFInFilter always be the datum of build table,
// while the other node can be the data of build table(during insert or merge process)
// or the data of probe table(during probe process)
// so the compare function relies on the other node.
bool ObRFInFilterVecMsg::ObRFInFilterNode::operator==(const ObRFInFilterNode &other) const
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  bool bool_ret = true;
  uint64_t self_hash = 0;
  uint64_t other_hash = 0;
  if (OB_FAIL(hash(self_hash)) || OB_FAIL(other.hash(other_hash))) {
    LOG_WARN("faild to hash", K(ret));
  } else if (self_hash != other_hash) {
    bool_ret = false;
  } else if (nullptr != other.compact_row_) {
    // comapre the row in the hashset (during merge process, merge other's hashset into own hashset)
    // compare other.compact_row_ with self.compact_row_
    const char *self_payload = nullptr;
    const char *other_payload = nullptr;
    ObLength self_len = 0;
    ObLength other_len = 0;
    for (int i = 0; i < other.row_meta_->col_cnt_; ++i) {
      if (compact_row_->is_null(i) && other.compact_row_->is_null(i)) {
        continue;
      } else {
        // always using other's cmp_func_,
        const ObObjMeta &self_meta = row_cmp_infos_->at(i).obj_meta_;
        const ObObjMeta &other_meta = other.row_cmp_infos_->at(i).obj_meta_;
        compact_row_->get_cell_payload(*row_meta_, i, self_payload, self_len);
        other.compact_row_->get_cell_payload(*other.row_meta_, i, other_payload, other_len);
        int tmp_ret = other.row_cmp_infos_->at(i).cmp_func_(
            other_meta, self_meta,
            other_payload, other_len, other.compact_row_->is_null(i),
            self_payload, self_len, compact_row_->is_null(i), cmp_ret);
        if (cmp_ret != 0) {
          bool_ret = false;
          break;
        }
      }
    }
  } else if (nullptr != other.row_with_hash_) {
    // judge whether in the hashset (insert process / probe process)
    // compare other.row_with_hash_ with self.compact_row_
    const char *self_payload = nullptr;
    const char *other_payload = nullptr;
    ObLength self_len = 0;
    ObLength other_len = 0;
    for (int i = 0; i < row_meta_->col_cnt_; ++i) {
      if (compact_row_->is_null(i) && other.row_with_hash_->row_.at(i).is_null()) {
        continue;
      } else {
        // always using other's cmp_func_,
        const ObObjMeta &self_meta = row_cmp_infos_->at(i).obj_meta_;
        const ObObjMeta &other_meta = other.row_cmp_infos_->at(i).obj_meta_;
        compact_row_->get_cell_payload(*row_meta_, i, self_payload, self_len);
        const ObDatum &other_datum = other.row_with_hash_->row_.at(i);
        other_payload = other_datum.ptr_;
        other_len = other_datum.len_;
        int tmp_ret = other.row_cmp_infos_->at(i).cmp_func_(
            other_meta, self_meta,
            other_payload, other_len, other_datum.is_null(),
            self_payload, self_len, compact_row_->is_null(i), cmp_ret);
        if (cmp_ret != 0) {
          bool_ret = false;
          break;
        }
      }
    }
  }
  return bool_ret;
}

int ObRFInFilterVecMsg::merge(ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  ObRFInFilterVecMsg &other_msg = static_cast<ObRFInFilterVecMsg &>(msg);
  if (!msg.is_active()) {
    is_active_ = false;
  } else if (!msg.is_empty() && is_active_) {
    ObSpinLockGuard guard(lock_);
    for (int64_t i = 0; i < other_msg.row_store_.get_row_cnt() && OB_SUCC(ret); ++i) {
      ObCompactRow *cur_row = other_msg.row_store_.get_row(i);
      int64_t row_size = other_msg.row_store_.get_row_size(i);
      // when merge, we must compare the node exist or not
      ObRFInFilterNode node(&build_row_cmp_info_, &build_row_meta_, cur_row,
                            nullptr /*row_with_hash*/);
      if (OB_FAIL(try_merge_node(node, row_size))) {
        LOG_WARN("fail to insert node", K(ret));
      } else if (OB_FAIL(sm_hash_set_.insert_hash(
                     other_msg.row_store_.get_hash_value(i, build_row_meta_)))) {
        LOG_WARN("failed to insert hash value into sm_hash_set_");
      }
    }
  }
  return ret;
}

int ObRFInFilterVecMsg::reuse()
{
  int ret = OB_SUCCESS;
  is_empty_ = true;
  row_store_.reset();
  rows_set_.reuse();
  sm_hash_set_.clear();
  (void)reuse_query_range();
  return ret;
}

int ObRFInFilterVecMsg::might_contain(const ObExpr &expr,
    ObEvalCtx &ctx,
    ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx,
    ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum datum;
  bool is_match = true;
  uint64_t hash_val = ObExprJoinFilter::JOIN_FILTER_SEED;
  ObRowWithHash &cur_row = *filter_ctx.cur_row_with_hash_;
  if (OB_UNLIKELY(!is_active_)) {
    res.set_int(1);
  } else if (OB_UNLIKELY(is_empty_)) {
    res.set_int(0);
    filter_ctx.filter_count_++;
    filter_ctx.check_count_++;
  } else {
    bool all_rows_active = false;
    int64_t batch_idx = ctx.get_batch_idx();
    int64_t batch_size = ctx.get_batch_size();
    EvalBound eval_bound(batch_size, batch_idx, batch_idx + 1, all_rows_active);
    for (int i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
      if (OB_FAIL(expr.args_[i]->eval_vector(ctx, *filter_ctx.skip_vector_, eval_bound))) {
        LOG_WARN("failed to eval_vector", K(ret));
      } else {
        ObIVector *arg_vec = expr.args_[i]->get_vector(ctx);
        datum.ptr_ = arg_vec->get_payload(batch_idx);
        datum.len_ = arg_vec->get_length(batch_idx);
        datum.null_ = arg_vec->is_null(batch_idx) ? 1 : 0;
        cur_row.row_.at(i) = datum;
        if (OB_FAIL(arg_vec->murmur_hash_v3_for_one_row(*expr.args_[i], hash_val, batch_idx,
            batch_size, hash_val))) {
          LOG_WARN("failed to cal hash");
        }
      }
    }
    if (OB_SUCC(ret)) {
      cur_row.hash_val_ = hash_val;
      ObRFInFilterNode node(&probe_row_cmp_info_, nullptr, nullptr /*compact_row*/, &cur_row);
      if (OB_FAIL(rows_set_.exist_refactored(node))) {
        if (OB_HASH_NOT_EXIST == ret) {
          is_match = false;
          ret = OB_SUCCESS;
        } else if (OB_HASH_EXIST == ret) {
          is_match = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to check node", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!is_match) {
        filter_ctx.filter_count_++;
      }
      filter_ctx.check_count_++;
      res.set_int(is_match ? 1 : 0);
      filter_ctx.collect_sample_info(!is_match, 1);
    }
  }
  return ret;
}

int ObRFInFilterVecMsg::do_might_contain_batch(const ObExpr &expr,
    ObEvalCtx &ctx,
    const ObBitVector &skip,
    const int64_t batch_size,
    ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx) {
  int ret = OB_SUCCESS;
  int64_t filter_count = 0;
  int64_t total_count = 0;
  uint64_t *right_hash_vals = reinterpret_cast<uint64_t *>(
                                ctx.frames_[expr.frame_idx_] + expr.res_buf_off_);
  uint64_t seed = ObExprJoinFilter::JOIN_FILTER_SEED;
  for (int idx = 0; OB_SUCC(ret) && idx < expr.arg_cnt_; ++idx) {
    if (OB_FAIL(expr.args_[idx]->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("eval failed", K(ret));
    } else {
      const bool is_batch_seed = (idx > 0);
      ObBatchDatumHashFunc hash_func = filter_ctx.hash_funcs_.at(idx).batch_hash_func_;
      hash_func(right_hash_vals,
                expr.args_[idx]->locate_batch_datums(ctx), expr.args_[idx]->is_batch_result(),
                skip, batch_size,
                is_batch_seed ? right_hash_vals : &seed,
                is_batch_seed);
    }
  }
  ObRowWithHash &cur_row = *filter_ctx.cur_row_with_hash_;
  ObRFInFilterNode node(&probe_row_cmp_info_, nullptr, nullptr /*compact_row*/, &cur_row);
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(batch_size);
  ObDatum *res_datums = expr.locate_batch_datums(ctx);
  for (int64_t batch_i = 0; OB_SUCC(ret) && batch_i < batch_size; ++batch_i) {
    if (skip.at(batch_i)) {
      continue;
    }
    total_count++;
    cur_row.hash_val_ = right_hash_vals[batch_i];
    batch_info_guard.set_batch_idx(batch_i);
    for (int64_t arg_i = 0; OB_SUCC(ret) && arg_i < expr.arg_cnt_; ++arg_i) {
      cur_row.row_.at(arg_i) = expr.args_[arg_i]->locate_expr_datum(ctx, batch_i);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(rows_set_.exist_refactored(node))) {
      if (OB_HASH_NOT_EXIST == ret) {
        res_datums[batch_i].set_int(0);
        filter_count++;
        ret = OB_SUCCESS;
      } else if (OB_HASH_EXIST == ret) {
        res_datums[batch_i].set_int(1);
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to check node", K(ret));
      }
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

int ObRFInFilterVecMsg::might_contain_batch(
      const ObExpr &expr,
      ObEvalCtx &ctx,
      const ObBitVector &skip,
      const int64_t batch_size,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx)
{
  int ret = OB_SUCCESS;
  ObDatum *results = expr.locate_batch_datums(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(batch_size);
  if (!is_active_) {
    for (int64_t i = 0; i < batch_size; i++) {
      results[i].set_int(1);
    }
  } else if (OB_UNLIKELY(is_empty_)) {
    for (int64_t i = 0; i < batch_size; i++) {
      results[i].set_int(0);
    }
  } else if (OB_FAIL(do_might_contain_batch(expr, ctx, skip, batch_size, filter_ctx))) {
    LOG_WARN("failed to do_might_contain_batch");
  }
  if (OB_SUCC(ret)) {
    eval_flags.set_all(batch_size);
  }
  return ret;
}

int ObRFInFilterVecMsg::do_might_contain_vector(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    const ObBitVector &skip,
    const EvalBound &bound,
    ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx)
{
  int ret = OB_SUCCESS;
  int64_t total_count = 0;
  int64_t filter_count = 0;
  int64_t batch_size = bound.batch_size();
  uint64_t seed = ObExprJoinFilter::JOIN_FILTER_SEED;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  VectorFormat res_format = expr.get_format(ctx);
  uint64_t *right_hash_vals = filter_ctx.right_hash_vals_;
  if (VEC_FIXED == res_format) {
    IntegerFixedVec *res_vec = static_cast<IntegerFixedVec *>(expr.get_vector(ctx));
    if (OB_FAIL(preset_not_match(res_vec, bound))) {
      LOG_WARN("failed to preset_not_match", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    ObExpr *e = expr.args_[i];
    if (OB_FAIL(e->eval_vector(ctx, skip, bound))) {
      LOG_WARN("evaluate vector failed", K(ret), K(*e));
    } else {
      const bool is_batch_seed = (i > 0);
      ObIVector *arg_vec = e->get_vector(ctx);
      if (OB_FAIL(arg_vec->murmur_hash_v3(*e, right_hash_vals, skip,
          bound, is_batch_seed ? right_hash_vals : &seed, is_batch_seed))) {
        LOG_WARN("failed to cal hash");
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    ObRowWithHash &cur_row = *filter_ctx.cur_row_with_hash_;
    ObRFInFilterNode node(&probe_row_cmp_info_, nullptr, nullptr /*compact_row*/, &cur_row);
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_size(batch_size);
    bool is_match = true;
    const int64_t is_match_payload = 1; // for VEC_FIXED set set_payload, always 1
    ObDatum datum;
    for (int64_t batch_i  = bound.start(); batch_i < bound.end() && OB_SUCC(ret); ++batch_i) {
      if (skip.at(batch_i)) {
        continue;
      } else {
        total_count++;
        eval_flags.set(batch_i);
        cur_row.hash_val_ = right_hash_vals[batch_i];
        batch_info_guard.set_batch_idx(batch_i);
        for (int arg_i = 0; OB_SUCC(ret) && arg_i < expr.arg_cnt_; ++arg_i) {
          ObIVector *arg_vec = expr.args_[arg_i]->get_vector(ctx);
          datum.ptr_ = arg_vec->get_payload(batch_i);
          datum.len_ = arg_vec->get_length(batch_i);
          datum.null_ = arg_vec->is_null(batch_i) ? 1 : 0;
          cur_row.row_.at(arg_i) = datum;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(rows_set_.exist_refactored(node))) {
          if (OB_HASH_NOT_EXIST == ret) {
            is_match = false;
            filter_count++;
            ret = OB_SUCCESS;
          } else if (OB_HASH_EXIST == ret) {
            is_match = true;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to check node", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (VEC_UNIFORM == res_format) {
            IntegerUniVec *res_vec = static_cast<IntegerUniVec *>(expr.get_vector(ctx));
            res_vec->set_int(batch_i, is_match ? 1 : 0);
          } else if (VEC_FIXED == res_format) {
            IntegerFixedVec *res_vec = static_cast<IntegerFixedVec *>(expr.get_vector(ctx));
            if (is_match) {
              res_vec->set_payload(batch_i, &is_match_payload, sizeof(int64_t));
            } else {
              // do nothing, already set not match in preset_not_match
            }
          }
        }
      }
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

template<typename ResVec>
int ObRFInFilterVecMsg::do_might_contain_vector_impl(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    const ObBitVector &skip,
    const EvalBound &bound,
    ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx)
{
  int ret = OB_SUCCESS;
  int64_t total_count = 0;
  int64_t filter_count = 0;
  uint64_t seed = ObExprJoinFilter::JOIN_FILTER_SEED;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  uint64_t *right_hash_vals = filter_ctx.right_hash_vals_;
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));

  if (std::is_same<ResVec, IntegerFixedVec>::value) {
    IntegerFixedVec *res_vec = static_cast<IntegerFixedVec *>(expr.get_vector(ctx));
    if (OB_FAIL(preset_not_match(res_vec, bound))) {
      LOG_WARN("failed to preset_not_match", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    ObExpr *e = expr.args_[i];
    if (OB_FAIL(e->eval_vector(ctx, skip, bound))) {
      LOG_WARN("evaluate vector failed", K(ret), K(*e));
    } else {
      const bool is_batch_seed = (i > 0);
      ObIVector *arg_vec = e->get_vector(ctx);
      if (OB_FAIL(arg_vec->murmur_hash_v3(*e, right_hash_vals, skip,
          bound, is_batch_seed ? right_hash_vals : &seed, is_batch_seed))) {
        LOG_WARN("failed to cal hash");
      }
    }
  }

#define IN_FILTER_PROBE_HELPER                                                                     \
  is_match = sm_hash_set_.test_hash(right_hash_vals[batch_i]);                                     \
  if (!is_match) {                                                                                 \
    filter_count++;                                                                                \
    if (std::is_same<ResVec, IntegerUniVec>::value) {                                              \
      res_vec->set_int(batch_i, 0);                                                                \
    }                                                                                              \
  } else {                                                                                         \
    if (std::is_same<ResVec, IntegerUniVec>::value) {                                              \
      res_vec->set_int(batch_i, 1);                                                                \
    } else {                                                                                       \
      res_vec->set_payload(batch_i, &is_match_payload, sizeof(int64_t));                           \
    }                                                                                              \
  }

  if (OB_FAIL(ret)) {
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_size(bound.batch_size());
    bool is_match = true;
    const int64_t is_match_payload = 1; // for VEC_FIXED set set_payload, always 1
    if (bound.get_all_rows_active()) {
      total_count += bound.end() - bound.start();
      for (int64_t batch_i = bound.start(); batch_i < bound.end() && OB_SUCC(ret); ++batch_i) {
        IN_FILTER_PROBE_HELPER
      }
    } else {
      InFilterProbeOP<ResVec> in_filter_probe_op(sm_hash_set_, res_vec, right_hash_vals,
                                                 total_count, filter_count);
      (void)ObBitVector::flip_foreach(skip, bound, in_filter_probe_op);
    }
    if (OB_SUCC(ret)) {
      eval_flags.set_all(true);
      filter_ctx.total_count_ += total_count;
      filter_ctx.check_count_ += total_count;
      filter_ctx.filter_count_ += filter_count;
      filter_ctx.collect_sample_info(filter_count, total_count);
    }
  }
#undef IN_FILTER_PROBE_HELPER
  return ret;
}

#define IN_FILTER_DISPATCH_RES_FORMAT(function, res_format)                                        \
  if (res_format == VEC_FIXED) {                                                                   \
    ret = function<IntegerFixedVec>(expr, ctx, skip, bound, filter_ctx);                           \
  } else {                                                                                         \
    ret = function<IntegerUniVec>(expr, ctx, skip, bound, filter_ctx);                             \
  }

int ObRFInFilterVecMsg::might_contain_vector(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    const ObBitVector &skip,
    const EvalBound &bound,
    ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx)
{
  int ret = OB_SUCCESS;
  if (!is_active_) {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_UNIFORM == res_format) {
      IntegerUniVec *res_vec = static_cast<IntegerUniVec *>(expr.get_vector(ctx));
      ret = proc_filter_not_active(res_vec, skip, bound);
    } else if (VEC_FIXED == res_format) {
      IntegerFixedVec *res_vec = static_cast<IntegerFixedVec *>(expr.get_vector(ctx));
      ret = proc_filter_not_active(res_vec, skip, bound);
    }
    if (OB_SUCC(ret)) {
      eval_flags.set_all(true);
    }
  } else if (OB_UNLIKELY(is_empty_)) {
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
    VectorFormat res_format = expr.get_format(ctx);
    IN_FILTER_DISPATCH_RES_FORMAT(do_might_contain_vector_impl, res_format);
  }
  return ret;
}

int ObRFInFilterVecMsg::prepare_storage_white_filter_data(ObDynamicFilterExecutor &dynamic_filter,
                                                          ObEvalCtx &eval_ctx,
                                                          ObRuntimeFilterParams &params,
                                                          bool &is_data_prepared)
{
  int ret = OB_SUCCESS;
  int col_idx = dynamic_filter.get_col_idx();
  if (is_empty_) {
    dynamic_filter.set_filter_action(DynamicFilterAction::FILTER_ALL);
    is_data_prepared = true;
  } else if (!is_active_) {
    dynamic_filter.set_filter_action(DynamicFilterAction::PASS_ALL);
    is_data_prepared = true;
  } else {
    for (int64_t i = 0; i < row_store_.get_row_cnt() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(params.push_back(row_store_.get_row(i)->get_datum(build_row_meta_, col_idx)))) {
        LOG_WARN("failed to push back");
      }
    }
  }
  if (OB_SUCC(ret)) {
    dynamic_filter.set_filter_val_meta(build_row_cmp_info_.at(col_idx).obj_meta_);
    is_data_prepared = true;
  }
  return ret;
}

int ObRFInFilterVecMsg::destroy()
{
  int ret = OB_SUCCESS;
  build_row_cmp_info_.reset();
  probe_row_cmp_info_.reset();
  build_row_meta_.reset();
  cur_row_with_hash_.row_.reset();
  rows_set_.destroy();
  sm_hash_set_.~ObSmallHashSet<false>();
  need_null_cmp_flags_.reset();
  row_store_.reset();
  hash_funcs_for_insert_.reset();
  query_range_info_.destroy();
  query_range_.destroy();
  query_range_allocator_.reset();
  allocator_.reset();
  return ret;
}

int ObRFInFilterVecMsg::prepare_query_ranges()
{
  int ret = OB_SUCCESS;
  (void)reuse_query_range();
  if (!query_range_info_.can_extract() || !is_active_) {
    is_query_range_ready_ = false;
  } else if (is_empty_) {
    // make empty range
    ObNewRange query_range;
    if (OB_FAIL(fill_empty_query_range(query_range_info_, query_range_allocator_, query_range))) {
      LOG_WARN("faild to fill_empty_query_range");
    } else if (OB_FAIL(query_range_.push_back(query_range))) {
      LOG_WARN("failed to push back query_range");
    } else {
      is_query_range_ready_ = true;
    }
  } else if (query_range_info_.prefix_col_idxs_.count() == build_row_meta_.col_cnt_) {
    // col count matches, the hashmap make sure all rows contain the filter are different
    // so not need to dedupcate
    ret = process_query_ranges_without_deduplicate();
  } else {
    // prefix col less than store col, need do deduplicate
    // for example:
    // there are three rows int the filter :{[1,2,3], [1,2,4], [1,2,5]}
    // and the range column is c1,c2
    // final query range extracted should be: range(1,2; 1,2)
    // we need to deduplicate to avoid duplicate range
    ret = process_query_ranges_with_deduplicate();
  }
  LOG_TRACE("in filter prepare query range", K(ret), K(query_range_.count()), K(rows_set_.size()),
            K(query_range_info_), K(is_query_range_ready_), K(query_range_));
  return ret;
}

struct TempHashNode
{
  TempHashNode() = default;
  TempHashNode(uint64_t hash_val, ObTMArray<ObDatum> *row, ObRFCmpInfos *cmp_infos)
      : hash_val_(hash_val), row_(row), cmp_infos_(cmp_infos)
  {}
  inline int hash(uint64_t &hash_ret) const
  {
    hash_ret = hash_val_;
    return OB_SUCCESS;
  }
  inline bool operator==(const TempHashNode &other) const
  {
    int ret = OB_SUCCESS;
    int cmp_ret = 0;
    bool bret = true;
    for (int i = 0; i < other.row_->count(); ++i) {
      if (row_->at(i).is_null() && other.row_->at(i).is_null()) {
        continue;
      } else {
        // because cmp_func is chosen as compare(probe_data/build_data, build_data)
        // so the other's data must be placed at first
        ObDatum &l = row_->at(i);
        ObDatum &r = other.row_->at(i);
        NullSafeRowCmpFunc &cmp_func = other.cmp_infos_->at(i).cmp_func_;
        ObObjMeta &obj_meta = other.cmp_infos_->at(i).obj_meta_;
        int tmp_ret = cmp_func(obj_meta, obj_meta, l.ptr_, l.len_, l.is_null(), r.ptr_, r.len_,
                               r.is_null(), cmp_ret);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("failed to do cmp_func", K(tmp_ret));
        }
        if (cmp_ret != 0) {
          bret = false;
          break;
        }
      }
    }
    return bret;
  }
public:
  uint64_t hash_val_;
  ObTMArray<ObDatum> *row_;
  ObRFCmpInfos *cmp_infos_;
};

int ObRFInFilterVecMsg::process_query_ranges_with_deduplicate()
{
  int ret = OB_SUCCESS;
  int64_t max_in_filter_query_range_count = ObPxQueryRangeInfo::MAX_IN_FILTER_QUERY_RANGE_COUNT;

#ifdef ERRSIM
  int tmp_ret = OB_E(EventTable::EN_PX_MAX_IN_FILTER_QR_COUNT) OB_SUCCESS;
  if (OB_SUCCESS != tmp_ret) {
    max_in_filter_query_range_count = max_in_num_;
  }
#endif

  hash::ObHashSet<TempHashNode, hash::NoPthreadDefendMode> tmp_rows_set;
  ObArenaAllocator tmp_allocator;
  ObRFCmpInfos cmp_infos(tmp_allocator);
  const ObIArray<int64_t> &prefix_col_idxs = query_range_info_.prefix_col_idxs_;

  if (OB_FAIL(
          tmp_rows_set.create(rows_set_.size() * 2, "RFInVecTmpHashSet", "RFInVecTmpHashSet"))) {
    LOG_WARN("fail to init in hash set", K(ret));
  } else if (OB_FAIL(cmp_infos.init(prefix_col_idxs.count()))) {
    LOG_WARN("failed to init compare func");
  }
  // reorder compare function
  for (int64_t j = 0; j < prefix_col_idxs.count() && OB_SUCC(ret); ++j) {
    int64_t col_idx = prefix_col_idxs.at(j);
    if (OB_FAIL(cmp_infos.push_back(build_row_cmp_info_.at(col_idx)))) {
      LOG_WARN("failed to pushback compare func");
    }
  }
  ObTMArray<ObTMArray<ObDatum>> tmp_rows;
  ObTMArray<int64_t> effective_row_idxs;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tmp_rows.prepare_allocate(row_store_.get_row_cnt()))) {
    LOG_WARN("failed to prepare_allocate query_range_", K(row_store_.get_row_cnt()));
  } else if (OB_FAIL(effective_row_idxs.reserve(row_store_.get_row_cnt()))) {
    LOG_WARN("failed to reserve query_range_", K(row_store_.get_row_cnt()));
  }
  for (int64_t row_idx = 0; row_idx < row_store_.get_row_cnt() && OB_SUCC(ret); ++row_idx) {
    if (OB_ISNULL(row_store_.get_row(row_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("this row is null", K(ret));
    } else {
      uint64_t hash_value = ObExprJoinFilter::JOIN_FILTER_SEED;
      ObTMArray<ObDatum> &tmp_row = tmp_rows.at(row_idx);
      if (OB_FAIL(tmp_row.prepare_allocate(prefix_col_idxs.count()))) {
        LOG_WARN("failed to prepare_allocate tmp_row");
      }
      for (int64_t j = 0; j < prefix_col_idxs.count() && OB_SUCC(ret); ++j) {
        int64_t col_idx = prefix_col_idxs.at(j);
        tmp_row.at(j) = row_store_.get_row(row_idx)->get_datum(build_row_meta_, col_idx);
        if (OB_FAIL(hash_funcs_for_insert_.at(col_idx).hash_func_(tmp_row.at(j), hash_value,
                                                                  hash_value))) {
          LOG_WARN("fail to calc hash value", K(ret), K(hash_value));
        }
      }
      bool is_duplicate = true;
      if (OB_SUCC(ret)) {
        TempHashNode node(hash_value, &tmp_row, &cmp_infos);
        if (OB_FAIL(tmp_rows_set.set_refactored(node, 0/*not cover*/))) {
          if (ret != OB_HASH_EXIST) {
            LOG_WARN("failed to set_refactored");
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          is_duplicate = false;
        }
      }
      if (!is_duplicate) {
        OZ(effective_row_idxs.push_back(row_idx));
        if (effective_row_idxs.count() > max_in_filter_query_range_count) {
          // no more than MAX_IN_FILTER_QUERY_RANGE_COUNT can be extracted
          // TODO[zhouhaiyu.zhy]: if the data of create table' prefix columns shows a high rate of
          // duplication and the final count of effective rows still exceeds
          // max_in_filter_query_range_count(128) the execution of the "prepare_query_ranges"
          // becomes redundant and may result in a decrease in performance.
          break;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (effective_row_idxs.count() > max_in_filter_query_range_count) {
      is_query_range_ready_ = false;
    } else {
      if (OB_FAIL(query_range_.reserve(effective_row_idxs.count()))) {
        LOG_WARN("failed to reserve query_range_", K(effective_row_idxs.count()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < effective_row_idxs.count(); ++i) {
        OZ(generate_one_range(effective_row_idxs.at(i)));
      }
      if (OB_SUCC(ret)) {
        is_query_range_ready_ = true;
        LOG_DEBUG("TBDelete in filter succ extract query range", K(query_range_.count()),
                  K(row_store_.get_row_cnt()));
      }
    }
  }
  return ret;
}

int ObRFInFilterVecMsg::process_query_ranges_without_deduplicate()
{
  int ret = OB_SUCCESS;
  int64_t max_in_filter_query_range_count = ObPxQueryRangeInfo::MAX_IN_FILTER_QUERY_RANGE_COUNT;

#ifdef ERRSIM
  int tmp_ret = OB_E(EventTable::EN_PX_MAX_IN_FILTER_QR_COUNT) OB_SUCCESS;
  if (OB_SUCCESS != tmp_ret) {
    max_in_filter_query_range_count = max_in_num_;
  }
#endif

  if (row_store_.get_row_cnt() > max_in_filter_query_range_count) {
    is_query_range_ready_ = false;
  } else {
    if (OB_FAIL(query_range_.reserve(row_store_.get_row_cnt()))) {
      LOG_WARN("failed to reserve query_range_", K(row_store_.get_row_cnt()));
    }
    for (int64_t row_idx = 0; row_idx < row_store_.get_row_cnt() && OB_SUCC(ret); ++row_idx) {
      if (OB_ISNULL(row_store_.get_row(row_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this row is null", K(ret));
      } else {
        OZ(generate_one_range(row_idx));
      }
    }
    if (OB_SUCC(ret)) {
      is_query_range_ready_ = true;
      LOG_DEBUG("TBDelete in filter succ extract query range", K(row_store_.get_row_cnt()),
                K(query_range_));
    }
  }
  return ret;
}

int ObRFInFilterVecMsg::generate_one_range(int row_idx)
{
  int ret = OB_SUCCESS;
  int64_t range_column_cnt = query_range_info_.range_column_cnt_;
  const ObIArray<int64_t> &prefix_col_idxs = query_range_info_.prefix_col_idxs_;
  const ObIArray<ObObjMeta> &prefix_col_obj_metas = query_range_info_.prefix_col_obj_metas_;

  ObNewRange query_range;
  query_range.table_id_ = query_range_info_.table_id_;
  query_range.border_flag_.set_inclusive_start();
  query_range.border_flag_.set_inclusive_end();
  ObObj *start = NULL;
  ObObj *end = NULL;
  if (OB_ISNULL(start = static_cast<ObObj *>(
                    query_range_allocator_.alloc(sizeof(ObObj) * range_column_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory for start_obj failed", K(ret));
  } else if (OB_ISNULL(end = static_cast<ObObj *>(
                           query_range_allocator_.alloc(sizeof(ObObj) * range_column_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory for end_obj failed", K(ret));
  }
  for (int64_t j = 0; j < prefix_col_idxs.count() && OB_SUCC(ret); ++j) {
    int64_t col_idx = prefix_col_idxs.at(j);
    const ObObjMeta &obj_meta = prefix_col_obj_metas.at(j);
    // ObDatum &datum = serial_rows_.at(row_idx)->at(col_idx);
    ObDatum datum = row_store_.get_row(row_idx)->get_datum(build_row_meta_, col_idx);
    new (start + j) ObObj();
    new (end + j) ObObj();
    datum.to_obj(*(start + j), obj_meta);
    datum.to_obj(*(end + j), obj_meta);
  }
  for (int64_t j = prefix_col_idxs.count(); j < range_column_cnt && OB_SUCC(ret); ++j) {
    new (start + j) ObObj();
    new (end + j) ObObj();
    (start + j)->set_min_value();
    (end + j)->set_max_value();
  }
  if (OB_SUCC(ret)) {
    ObRowkey start_key(start, range_column_cnt);
    ObRowkey end_key(end, range_column_cnt);
    query_range.start_key_ = start_key;
    query_range.end_key_ = end_key;
    if (OB_FAIL(query_range_.push_back(query_range))) {
      LOG_WARN("failed to push range");
    }
  }
  return ret;
}

void ObRFInFilterVecMsg::check_finish_receive()
{
  if (ATOMIC_LOAD(&is_active_)) {
    if (msg_receive_expect_cnt_ == ATOMIC_LOAD(&msg_receive_cur_cnt_)) {
      (void)after_process();
      is_ready_ = true;
    }
  }
}

void ObRFInFilterVecMsg::after_process()
{
  // prepare_query_ranges can be failed, but rf still worked
  (void)prepare_query_ranges();
}

int ObRFInFilterVecMsg::try_extract_query_range(bool &has_extract, ObIArray<ObNewRange> &ranges)
{
  int ret = OB_SUCCESS;
  if (!is_query_range_ready_) {
    has_extract = false;
  } else {
    // overwrite ranges
    ranges.reset();
    if (OB_FAIL(ranges.assign(query_range_))) {
      LOG_WARN("failed to assign range");
    } else {
      has_extract = true;
    }
  }
  return ret;
}

//end ObRFInFilterVecMsg
