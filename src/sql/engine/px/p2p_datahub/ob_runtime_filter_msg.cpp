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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_msg.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_rpc_proxy.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_rpc_process.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_msg.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_rpc_proxy.h"
#include "sql/engine/expr/ob_expr_join_filter.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/ob_operator.h"
#include "share/detect/ob_detect_manager_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/basic/ob_pushdown_filter.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;

OB_SERIALIZE_MEMBER(ObRFRangeFilterMsg::MinMaxCellSize, min_datum_buf_size_, max_datum_buf_size_);

OB_DEF_SERIALIZE(ObRFBloomFilterMsg)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObRFBloomFilterMsg, ObP2PDatahubMsgBase));
  LST_DO_CODE(OB_UNIS_ENCODE,
              phase_,
              bloom_filter_,
              next_peer_addrs_,
              expect_first_phase_count_,
              piece_size_,
              use_rich_format_);
  return ret;
}

OB_DEF_DESERIALIZE(ObRFBloomFilterMsg)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObRFBloomFilterMsg, ObP2PDatahubMsgBase));
  bloom_filter_.allocator_.set_tenant_id(tenant_id_);
  bloom_filter_.allocator_.set_label("ObPxBFDESER");

  LST_DO_CODE(OB_UNIS_DECODE,
              phase_,
              bloom_filter_,
              next_peer_addrs_,
              expect_first_phase_count_,
              piece_size_,
              use_rich_format_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRFBloomFilterMsg)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObRFBloomFilterMsg, ObP2PDatahubMsgBase));
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              phase_,
              bloom_filter_,
              next_peer_addrs_,
              expect_first_phase_count_,
              piece_size_,
              use_rich_format_);
  return len;
}

OB_DEF_SERIALIZE(ObRFRangeFilterMsg)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObRFRangeFilterMsg, ObP2PDatahubMsgBase));
  LST_DO_CODE(OB_UNIS_ENCODE,
              lower_bounds_,
              upper_bounds_,
              need_null_cmp_flags_,
              cells_size_,
              cmp_funcs_,
              query_range_info_,
              build_obj_metas_);
  return ret;
}

OB_DEF_DESERIALIZE(ObRFRangeFilterMsg)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObRFRangeFilterMsg, ObP2PDatahubMsgBase));
  LST_DO_CODE(OB_UNIS_DECODE,
              lower_bounds_,
              upper_bounds_,
              need_null_cmp_flags_,
              cells_size_,
              cmp_funcs_,
              query_range_info_,
              build_obj_metas_);
  if (OB_FAIL(adjust_cell_size())) {
    LOG_WARN("fail do adjust cell size", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRFRangeFilterMsg)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObRFRangeFilterMsg, ObP2PDatahubMsgBase));
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              lower_bounds_,
              upper_bounds_,
              need_null_cmp_flags_,
              cells_size_,
              cmp_funcs_,
              query_range_info_,
              build_obj_metas_);
  return len;
}

OB_DEF_SERIALIZE(ObRFInFilterMsg)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObRFInFilterMsg, ObP2PDatahubMsgBase));
  int cnt = is_active_? serial_rows_.count() : 0;
  OB_UNIS_ENCODE(cnt);
  OB_UNIS_ENCODE(cmp_funcs_);
  OB_UNIS_ENCODE(hash_funcs_for_insert_);
  OB_UNIS_ENCODE(col_cnt_);
  OB_UNIS_ENCODE(max_in_num_);
  OB_UNIS_ENCODE(need_null_cmp_flags_);
  if (is_active_) {
    for (int i = 0; OB_SUCC(ret) && i < serial_rows_.count(); ++i) {
      if (OB_FAIL(serial_rows_.at(i)->serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize rows", K(ret));
      }
    }
  }
  OB_UNIS_ENCODE(query_range_info_);
  OB_UNIS_ENCODE(build_obj_metas_);
  return ret;
}

OB_DEF_DESERIALIZE(ObRFInFilterMsg)
{
  int ret = OB_SUCCESS;
  int64_t row_cnt = 0;
  BASE_DESER((ObRFInFilterMsg, ObP2PDatahubMsgBase));
  OB_UNIS_DECODE(row_cnt);
  OB_UNIS_DECODE(cmp_funcs_);
  OB_UNIS_DECODE(hash_funcs_for_insert_);
  OB_UNIS_DECODE(col_cnt_);
  OB_UNIS_DECODE(max_in_num_);
  OB_UNIS_DECODE(need_null_cmp_flags_);
  if (OB_SUCC(ret) && is_active_) {
    ObFixedArray<ObDatum, ObIAllocator> *new_row = nullptr;
    void *array_ptr = nullptr;
    int64_t buckets_cnt = max(row_cnt, 1);
    if (OB_FAIL(serial_rows_.reserve(row_cnt))) {
      LOG_WARN("fail to init row cnt", K(ret));
    } else if (OB_FAIL(rows_set_.create(buckets_cnt * 2,
        "RFDEInFilter",
        "RFDEInFilter"))) {
      LOG_WARN("fail to init in hash set", K(ret));
    } else if (OB_FAIL(cur_row_.prepare_allocate(col_cnt_))) {
      LOG_WARN("fail to prepare allocate col cnt datum", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
      new_row = nullptr;
      array_ptr = nullptr;
      if (OB_ISNULL(array_ptr = allocator_.alloc(sizeof(ObFixedArray<ObDatum, ObIAllocator>)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      } else {
        new_row = new(array_ptr) ObFixedArray<ObDatum, ObIAllocator>(allocator_);
        if (OB_FAIL(new_row->deserialize(buf, data_len, pos))) {
          LOG_WARN("fail to serialize rows", K(ret));
        } else if (OB_FAIL(serial_rows_.push_back(new_row))) {
          LOG_WARN("fail to push back new row", K(ret));
        } else {
          ObRFInFilterNode node(&cmp_funcs_, &hash_funcs_for_insert_, new_row);
          if (OB_FAIL(rows_set_.set_refactored(node))) {
            LOG_WARN("fail to insert in filter node", K(ret));
          }
        }
      }
    }
  }
  OB_UNIS_DECODE(query_range_info_);
  OB_UNIS_DECODE(build_obj_metas_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRFInFilterMsg)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObRFInFilterMsg, ObP2PDatahubMsgBase));
  int cnt = is_active_? serial_rows_.count() : 0;
  OB_UNIS_ADD_LEN(cnt);
  OB_UNIS_ADD_LEN(cmp_funcs_);
  OB_UNIS_ADD_LEN(hash_funcs_for_insert_);
  OB_UNIS_ADD_LEN(col_cnt_);
  OB_UNIS_ADD_LEN(max_in_num_);
  OB_UNIS_ADD_LEN(need_null_cmp_flags_);
  if (is_active_) {
    for (int i = 0; i < serial_rows_.count(); ++i) {
      len += serial_rows_.at(i)->get_serialize_size();
    }
  }
  OB_UNIS_ADD_LEN(query_range_info_);
  OB_UNIS_ADD_LEN(build_obj_metas_);
  return len;
}


//ObRFBloomFilterMsg
int ObRFBloomFilterMsg::process_msg_internal(bool &need_free)
{
  int ret = OB_SUCCESS;
  ObP2PDhKey dh_key(p2p_datahub_id_, px_sequence_id_, task_id_);
  ObP2PDatahubManager::P2PMsgSetCall set_call(dh_key, *this);
  ObP2PDatahubManager::MsgMap &map = PX_P2P_DH.get_map();
  start_time_ = ObTimeUtility::current_time();

  bool need_merge = true;
  if (OB_FAIL(generate_receive_count_array(piece_size_, bloom_filter_.get_begin_idx()))) {
    need_free = true;
    LOG_WARN("fail to generate receive count array", K(ret));
  } else {
    //set msg
    ObP2PDatahubMsgGuard guard(this);
    if (OB_FAIL(map.set_refactored(dh_key, this, 0/*flag*/, 0/*broadcast*/, 0/*overwrite_key*/, &set_call))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to set refactored", K(ret));
      }
      need_free = true;
    } else {
      need_merge = false; // set success, not need to merge
      int reg_dm_ret = OB_SUCCESS;
#ifdef ERRSIM
      reg_dm_ret = OB_E(EventTable::EN_PX_P2P_MSG_REG_DM_FAILED) OB_SUCCESS;
      if (OB_SUCCESS != reg_dm_ret) {
        LOG_WARN("p2p msg reg dm failed by design", K(ret));
        reg_dm_ret = OB_ALLOCATE_MEMORY_FAILED;
      }
#endif
      if (OB_SUCCESS == reg_dm_ret) {
        reg_dm_ret = ObDetectManagerUtils::p2p_datahub_register_check_item_into_dm(
            register_dm_info_, dh_key, dm_cb_node_seq_id_);
      }
      if (OB_SUCCESS != reg_dm_ret) {
        LOG_WARN("[DM] failed to register check item to dm", K(reg_dm_ret));
      }
    }

    // merge piece bloom filter
    if (OB_SUCC(ret) && need_merge) {
      // for bloom filter msg, we can merge several msgs concurrently in an atomic manner without holding the map lock.
      // thus, we need handle the reference count carefully here to make sure the msg not been destroyed during the merge process.
      ObP2PDatahubMsgBase *rf_msg_in_map = nullptr;
      ObRFBloomFilterMsg *bf_msg = nullptr;
      if (OB_FAIL(PX_P2P_DH.atomic_get_msg(dh_key, rf_msg_in_map))) { // inc ref_count is integrated
        LOG_WARN("fail to get msg", K(ret));
      } else if (FALSE_IT(bf_msg = static_cast<ObRFBloomFilterMsg *>(rf_msg_in_map))) {
      } else if (OB_FAIL(bf_msg->atomic_merge(*this))) {
        LOG_WARN("fail to merge p2p dh msg", K(ret));
      }
      if (OB_NOT_NULL(rf_msg_in_map)) {
        // after merge, dec ref_count
        rf_msg_in_map->dec_ref_count();
      }
    }
    if (need_free) {
       // msg not in map, dec ref count
      guard.dec_msg_ref_count();
    }
  }
  return ret;
}

int ObRFBloomFilterMsg::generate_receive_count_array(int64_t piece_size, int64_t cur_begin_idx)
{
  int ret = OB_SUCCESS;
  int64_t bits_array_length = ceil((double)bloom_filter_.get_bits_count() / 64);
  int64_t count = ceil(bits_array_length / (double)piece_size);
  int64_t begin_idx = 0;
  if (OB_FAIL(receive_count_array_.init(count))) {
    LOG_WARN("fail to init receive_count_array_", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < count; ++i) {
    begin_idx = i * piece_size;
    if (begin_idx >= bits_array_length) {
      begin_idx = bits_array_length - 1;
    }
    if (cur_begin_idx != begin_idx) {
      OZ(receive_count_array_.push_back(BloomFilterReceiveCount(begin_idx, 0)));
    } else {
      OZ(receive_count_array_.push_back(BloomFilterReceiveCount(begin_idx, 1)));
    }

  }
  return ret;
}

int ObRFBloomFilterMsg::reuse()
{
  int ret = OB_SUCCESS;
  is_empty_ = true;
  bloom_filter_.reset_filter();
  need_send_msg_ = true;
  return ret;
}

int ObRFBloomFilterMsg::process_first_phase_recieve_count(
    ObRFBloomFilterMsg &msg, bool &first_phase_end)
{
  int ret = OB_SUCCESS;
  CK(msg.get_msg_receive_expect_cnt() > 0 && msg_receive_expect_cnt_ > 0);
  int64_t begin_idx = msg.bloom_filter_.get_begin_idx();
  // msg_receive_cur_cnt_ is msg total cnt, msg_receive_expect_cnt_ equals to sqc_count * peice_count
  int64_t received_cnt = ATOMIC_AAF(&msg_receive_cur_cnt_, 1);
  if (received_cnt > msg_receive_expect_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to process receive count", K(ret), K(received_cnt),
        K(msg_receive_expect_cnt_));
  } else if (receive_count_array_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("emptry receive count array", K(ret));
  } else {
    bool find = false;
    for (int i = 0; OB_SUCC(ret) && i < receive_count_array_.count(); ++i) {
      if (begin_idx == receive_count_array_.at(i).begin_idx_) {
        // receive count of a specific peice msg, expect_first_phase_count_ equals to sqc count
        int64_t cur_count = ATOMIC_AAF(&receive_count_array_.at(i).reciv_count_, 1);
        first_phase_end = (cur_count == expect_first_phase_count_);
        find = true;
        break;
      }
    }
    if (!find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected process first phase", K(ret), K(receive_count_array_.count()));
    }
  }
  return ret;
}

int ObRFBloomFilterMsg::process_receive_count(ObP2PDatahubMsgBase &rf_msg)
{
  int ret = OB_SUCCESS;
  bool first_phase_end = false;
  ObRFBloomFilterMsg &bf_msg = static_cast<ObRFBloomFilterMsg &>(rf_msg);
  auto process_second_phase = [&](ObRFBloomFilterMsg &bf_msg) {
    LOG_WARN("process second phase", K(ret));
    if (OB_FAIL(ObP2PDatahubMsgBase::process_receive_count(bf_msg))) {
      LOG_WARN("fail to process receive count", K(ret));
    }
    return ret;
  };

  auto process_first_phase = [&](ObRFBloomFilterMsg &bf_msg) {
    if (OB_FAIL(process_first_phase_recieve_count(
        bf_msg, first_phase_end))) {
      LOG_WARN("fail to process receive count", K(ret));
    }
    return ret;
  };
  if (bf_msg.is_first_phase()) {
    if (OB_FAIL(process_first_phase(bf_msg))) {
      LOG_WARN("fail to process first phase", K(ret));
    } else if (first_phase_end && !bf_msg.get_next_phase_addrs().empty()) {
      obrpc::ObP2PDhRpcProxy &rpc_proxy = PX_P2P_DH.get_proxy();
      ObPxP2PDatahubArg arg;
      ObRFBloomFilterMsg second_phase_msg;
      arg.msg_ = &second_phase_msg;
      if (OB_FAIL(second_phase_msg.shadow_copy(*this))) {
        LOG_WARN("fail to shadow copy second phase msg", K(ret));
      } else {
        second_phase_msg.phase_ = SECOND_LEVEL;
        second_phase_msg.set_msg_cur_cnt(expect_first_phase_count_);
        second_phase_msg.bloom_filter_.set_begin_idx(bf_msg.bloom_filter_.get_begin_idx());
        second_phase_msg.bloom_filter_.set_end_idx(bf_msg.bloom_filter_.get_end_idx());
      }
      for (int i = 0; OB_SUCC(ret) && i < bf_msg.get_next_phase_addrs().count(); ++i) {
        if (bf_msg.get_next_phase_addrs().at(i) != GCTX.self_addr()) {
          if (OB_FAIL(rpc_proxy.to(bf_msg.get_next_phase_addrs().at(i))
              .by(bf_msg.get_tenant_id())
              .timeout(bf_msg.get_timeout_ts())
              .compressed(ObCompressorType::LZ4_COMPRESSOR)
              .send_p2p_dh_message(arg, NULL))) {
            LOG_WARN("fail to send bloom filter", K(ret));
          }
        }
      }
      (void)check_finish_receive();
    } else if (bf_msg.get_next_phase_addrs().empty()) {
      (void)check_finish_receive();
    }
  } else if (OB_FAIL(process_second_phase(bf_msg))) {
      LOG_WARN("fail to process second phase", K(ret));
  }
  return ret;
}

int ObRFBloomFilterMsg::deep_copy_msg(ObP2PDatahubMsgBase *&new_msg_ptr)
{
  int ret = OB_SUCCESS;
  ObRFBloomFilterMsg *bf_msg = nullptr;
  ObMemAttr attr(tenant_id_, "PxBfMsg");
  if (OB_FAIL(PX_P2P_DH.alloc_msg<ObRFBloomFilterMsg>(attr, bf_msg))) {
    LOG_WARN("fail to alloc rf msg", K(ret));
  } else if (OB_FAIL(bf_msg->assign(*this))) {
    LOG_WARN("fail to assign rf msg", K(ret));
  } else {
    new_msg_ptr = bf_msg;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(bf_msg)) {
    bf_msg->destroy();
    ob_free(bf_msg);
  }
  return ret;
}

int ObRFBloomFilterMsg::assign(const ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  const ObRFBloomFilterMsg &other_msg = static_cast<const ObRFBloomFilterMsg &>(msg);
  phase_ = other_msg.phase_;
  expect_first_phase_count_ = other_msg.expect_first_phase_count_;
  piece_size_ = other_msg.piece_size_;
  use_rich_format_ = other_msg.use_rich_format_;
  if (OB_FAIL(ObP2PDatahubMsgBase::assign(msg))) {
    LOG_WARN("failed to assign base data", K(ret));
  } else if (OB_FAIL(next_peer_addrs_.assign(other_msg.next_peer_addrs_))) {
    LOG_WARN("fail to assign bf msg", K(ret));
  } else if (OB_FAIL(bloom_filter_.assign(other_msg.bloom_filter_, msg.get_tenant_id()))) {
    LOG_WARN("fail to assign bf msg", K(ret));
  } else if (OB_FAIL(filter_indexes_.prepare_allocate(other_msg.filter_indexes_.count()))) {
    LOG_WARN("failed to prepare_allocate filter indexes", K(ret));
  } else {
    // The reason we don't use filter_indexes_.assign(other_msg.filter_indexes_) here is that:
    // channel_ids_ is an ObFixedArray in BloomFilterIndex, we need to set allocator before assign channel_ids_
    for (int64_t i = 0; i < other_msg.filter_indexes_.count() && OB_SUCC(ret); ++i) {
      filter_indexes_.at(i).channel_ids_.set_allocator(&allocator_);
      const BloomFilterIndex &other_filter_index = other_msg.filter_indexes_.at(i);
      if (OB_FAIL(filter_indexes_.at(i).assign(other_filter_index))) {
        LOG_WARN("fail to assign BloomFilterIndex", K(ret));
      }
    }
  }
  return ret;
}

int ObRFBloomFilterMsg::shadow_copy(const ObRFBloomFilterMsg &other_msg)
{
  int ret = OB_SUCCESS;
  phase_ = other_msg.phase_;
  expect_first_phase_count_ = other_msg.expect_first_phase_count_;
  piece_size_ = other_msg.piece_size_;
  use_rich_format_ = other_msg.use_rich_format_;
  if (OB_FAIL(ObP2PDatahubMsgBase::assign(other_msg))) {
    LOG_WARN("failed to assign base data", K(ret));
  } else if (OB_FAIL(bloom_filter_.init(&other_msg.bloom_filter_))) {
    LOG_WARN("fail to assign bf msg", K(ret));
  }
  return ret;
}

int ObRFBloomFilterMsg::regenerate()
{
  int ret = OB_SUCCESS;
  if (!is_finish_regen_) {
    if (receive_count_array_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to reset receive count array", K(ret));
    } else if (1 == receive_count_array_.count()) {
      is_finish_regen_ = true;
    } else if (OB_FAIL(bloom_filter_.regenerate())) {
      LOG_WARN("fail to to regnerate bloom filter", K(ret));
    } else {
      is_finish_regen_ = true;
    }
  }
  return ret;
}

int ObRFBloomFilterMsg::atomic_merge(ObP2PDatahubMsgBase &other_msg)
{
  int ret = OB_SUCCESS;
  if (!other_msg.is_empty() && (OB_FAIL(merge(other_msg)))) {
    LOG_WARN("fail to merge dh msg", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(process_receive_count(other_msg))) {
    LOG_WARN("fail to process receive count", K(ret));
  }
  return ret;
}

// the merge process of bloom_filter_ is atomic by using CAS
int ObRFBloomFilterMsg::merge(ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  ObRFBloomFilterMsg &bf_msg = static_cast<ObRFBloomFilterMsg &>(msg);
  if (bf_msg.is_empty_) {
  } else if (OB_FAIL(bloom_filter_.merge_filter(&bf_msg.bloom_filter_))) {
    LOG_WARN("fail to merge bloom filter msg", K(ret));
  } else {
    is_empty_ = false;
  }
  return ret;
}

int ObRFBloomFilterMsg::destroy()
{
  int ret = OB_SUCCESS;
  next_peer_addrs_.reset();
  bloom_filter_.reset();
  filter_indexes_.reset();
  receive_count_array_.reset();
  allocator_.reset();
  return ret;
}

int ObRFBloomFilterMsg::might_contain(const ObExpr &expr,
    ObEvalCtx &ctx,
    ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx,
    ObDatum &res)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = ObExprJoinFilter::JOIN_FILTER_SEED;
  ObDatum *datum = nullptr;
  ObHashFunc hash_func;
  if (OB_UNLIKELY(is_empty_)) {
    res.set_int(0);
    filter_ctx.filter_count_++;
    filter_ctx.check_count_++;
  } else {
    if (!use_rich_format_) {
      for (int i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
        if (OB_FAIL(expr.args_[i]->eval(ctx, datum))) {
          LOG_WARN("failed to eval datum", K(ret));
        } else {
          hash_func.hash_func_ = filter_ctx.hash_funcs_.at(i).hash_func_;
          if (OB_FAIL(hash_func.hash_func_(*datum, hash_val, hash_val))) {
            LOG_WARN("fail to calc hash val", K(ret));
          }
        }
      }
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
          if (OB_FAIL(arg_vec->murmur_hash_v3_for_one_row(*expr.args_[i], hash_val, batch_idx,
                                                          batch_size, hash_val))) {
            LOG_WARN("failed to cal hash");
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      bool is_match = true;
      if (OB_FAIL(bloom_filter_.might_contain(hash_val, is_match))) {
        LOG_WARN("fail to check filter might contain value", K(ret), K(hash_val));
      } else {
        if (!is_match) {
          filter_ctx.filter_count_++;
        }
        filter_ctx.check_count_++;
        res.set_int(is_match ? 1 : 0);
        filter_ctx.collect_sample_info(!is_match, 1);
      }
    }
  }
  return ret;
}

int ObRFBloomFilterMsg::might_contain_batch(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    const ObBitVector &skip,
    const int64_t batch_size,
    ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx)
{
  int ret = OB_SUCCESS;
  bool is_match = true;
  uint64_t seed = ObExprJoinFilter::JOIN_FILTER_SEED;
  ObDatum *results = expr.locate_batch_datums(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  uint64_t *hash_values = reinterpret_cast<uint64_t *>(
                                ctx.frames_[expr.frame_idx_] + expr.res_buf_off_);
  int64_t total_count = 0;
  int64_t filter_count = 0;
  if (OB_UNLIKELY(is_empty_)) {
    if (OB_FAIL(ObBitVector::flip_foreach(skip, batch_size,
        [&](int64_t idx) __attribute__((always_inline)) {
      results[idx].set_int(0);
      ++filter_count;
      ++total_count;
      return OB_SUCCESS;
    }))) {
      LOG_WARN("fail to do for each operation", K(ret));
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
      if (OB_FAIL(e->eval_batch(ctx, skip, batch_size))) {
        LOG_WARN("evaluate batch failed", K(ret), K(*e));
      } else {
        const bool is_batch_seed = (i > 0);
        ObBatchDatumHashFunc hash_func_batch = filter_ctx.hash_funcs_.at(i).batch_hash_func_;
        hash_func_batch(hash_values,
                        e->locate_batch_datums(ctx), e->is_batch_result(),
                        skip, batch_size,
                        is_batch_seed ? hash_values : &seed,
                        is_batch_seed);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObBitVector::flip_foreach(skip, batch_size,
          [&](int64_t idx) __attribute__((always_inline)) {
            bloom_filter_.prefetch_bits_block(hash_values[idx]); return OB_SUCCESS;
          }))) {
    } else if (OB_FAIL(ObBitVector::flip_foreach(skip, batch_size,
        [&](int64_t idx) __attribute__((always_inline)) {
          int tmp_ret = bloom_filter_.might_contain(hash_values[idx], is_match);
          if (OB_SUCCESS == tmp_ret) {
            filter_count += !is_match;
            ++total_count;
            results[idx].set_int(is_match);
          }
          return tmp_ret;
        }))) {
      LOG_WARN("failed to process prefetch block", K(ret));
    } else {
      eval_flags.set_all(true);
      filter_ctx.filter_count_ += filter_count;
      filter_ctx.check_count_ += total_count;
      filter_ctx.total_count_ += total_count;
      filter_ctx.collect_sample_info(filter_count, total_count);
    }
  }
  return ret;
}

int ObRFBloomFilterMsg::insert_by_row_batch(
  const ObBatchRows *child_brs,
  const common::ObIArray<ObExpr *> &expr_array,
  const common::ObHashFuncs &hash_funcs,
  const ObExpr *calc_tablet_id_expr,
  ObEvalCtx &eval_ctx,
  uint64_t *batch_hash_values)
{
  int ret = OB_SUCCESS;
  if (child_brs->size_ > 0) {
    uint64_t seed = ObExprJoinFilter::JOIN_FILTER_SEED;
    if (OB_NOT_NULL(calc_tablet_id_expr)) {
      if (OB_ISNULL(calc_tablet_id_expr) || hash_funcs.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected part id expr", K(ret));
      } else if (OB_FAIL(calc_tablet_id_expr->eval_batch(eval_ctx,
        *(child_brs->skip_), child_brs->size_))) {
        LOG_WARN("failed to eval", K(ret));
      } else {
        ObBatchDatumHashFunc hash_func_batch = hash_funcs.at(0).batch_hash_func_;
        hash_func_batch(batch_hash_values,
                        calc_tablet_id_expr->locate_batch_datums(eval_ctx),
                        calc_tablet_id_expr->is_batch_result(),
                        *child_brs->skip_, child_brs->size_,
                        &seed,
                        false);
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr_array.count(); ++i) {
        ObExpr *expr = expr_array.at(i); // expr ptr check in cg, not check here
        if (OB_FAIL(expr->eval_batch(eval_ctx, *(child_brs->skip_), child_brs->size_))) {
          LOG_WARN("eval failed", K(ret));
        } else {
          ObBatchDatumHashFunc hash_func_batch = hash_funcs.at(i).batch_hash_func_;
          const bool is_batch_seed = (i > 0);
          hash_func_batch(batch_hash_values,
                          expr->locate_batch_datums(eval_ctx), expr->is_batch_result(),
                          *child_brs->skip_, child_brs->size_,
                          is_batch_seed ? batch_hash_values : &seed,
                          is_batch_seed);
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_brs->size_; ++i) {
      if (OB_NOT_NULL(calc_tablet_id_expr)) {
        ObDatum &datum = calc_tablet_id_expr->locate_expr_datum(eval_ctx, i);
        if (ObExprCalcPartitionId::NONE_PARTITION_ID == datum.get_int()) {
          continue;
        }
      }
      if (OB_SUCC(ret)) {
        if (child_brs->skip_->at(i)) {
          continue;
        } else if (OB_FAIL(bloom_filter_.put(batch_hash_values[i]))) {
          LOG_WARN("fail to put  hash value to px bloom filter", K(ret));
        } else if (is_empty_) {
          is_empty_ = false;
        }
      }
    }
  }
  return ret;
}
int ObRFBloomFilterMsg::insert_by_row(
    const common::ObIArray<ObExpr *> &expr_array,
    const common::ObHashFuncs &hash_funcs,
    const ObExpr *calc_tablet_id_expr,
    ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  bool ignore = false;
  if (OB_FAIL(calc_hash_value(expr_array,
    hash_funcs, calc_tablet_id_expr,
    eval_ctx, hash_value, ignore))) {
    LOG_WARN("failed to calc hash value", K(ret));
  } else if (ignore) {
      /*do nothing*/
  } else if (OB_FAIL(bloom_filter_.put(hash_value))) {
    LOG_WARN("fail to put  hash value to px bloom filter", K(ret));
  } else if (is_empty_) {
    is_empty_ = false;
  }
  return ret;
}

template <VectorFormat ResFormat, typename ResVec>
int ObRFBloomFilterMsg::fill_vec_result(ResVec *res_vec, const ObBitVector &skip,
                                        const EvalBound &bound, uint64_t *hash_values,
                                        int64_t &total_count, int64_t &filter_count)
{
  int ret = OB_SUCCESS;
  bool is_match = true;
  static const int64_t is_match_payload = 1; // for set_payload, always 1
  if (bound.get_all_rows_active()) {
    for (int64_t i = bound.start(); i < bound.end(); ++i) {
      (void)bloom_filter_.prefetch_bits_block(hash_values[i]);
    }
    for (int64_t i = bound.start(); i < bound.end(); ++i) {
      (void)bloom_filter_.might_contain(hash_values[i], is_match);
      if (is_match) {
        if (ResFormat == VEC_FIXED) {
          res_vec->set_payload(i, &is_match_payload, sizeof(int64_t));
        } else {
          res_vec->set_int(i, is_match_payload);
        }
      } else {
        // already set not match in preset_not_match
        filter_count += 1;
      }
    }
    total_count += bound.end() - bound.start();
  } else if (OB_FAIL(ObBitVector::flip_foreach(
          skip, bound, [&](int64_t idx) __attribute__((always_inline)) {
            bloom_filter_.prefetch_bits_block(hash_values[idx]);
            return OB_SUCCESS;
          }))) {
  } else if (OB_FAIL(ObBitVector::flip_foreach(
                 skip, bound, [&](int64_t idx) __attribute__((always_inline)) {
                   ret = bloom_filter_.might_contain(hash_values[idx], is_match);
                   if (OB_SUCC(ret)) {
                     if (ResFormat == VEC_FIXED) {
                       if (is_match) {
                         res_vec->set_payload(idx, &is_match_payload, sizeof(int64_t));
                       } else {
                         // do nothing, already set not match in preset_not_match
                       }
                     } else {
                       res_vec->set_int(idx, is_match);
                     }
                     ++total_count;
                     filter_count += !is_match;
                   }
                   return ret;
                 }))) {
    LOG_WARN("failed to process prefetch block", K(ret));
  }
  return ret;
}

int ObRFBloomFilterMsg::do_might_contain_vector(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    const ObBitVector &skip,
    const EvalBound &bound,
    ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx)
{
  int ret = OB_SUCCESS;
  int64_t total_count = 0;
  int64_t filter_count = 0;
  bool is_match = true;
  uint64_t seed = ObExprJoinFilter::JOIN_FILTER_SEED;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  uint64_t *hash_values = filter_ctx.right_hash_vals_;

  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    ObExpr *e = expr.args_[i];
    if (OB_FAIL(e->eval_vector(ctx, skip, bound))) {
      LOG_WARN("evaluate vector failed", K(ret), K(*e));
    } else {
      const bool is_batch_seed = (i > 0);
      ObIVector *arg_vec = e->get_vector(ctx);
      if (OB_FAIL(arg_vec->murmur_hash_v3(*e, hash_values, skip,
          bound, is_batch_seed ? hash_values : &seed, is_batch_seed))) {
        LOG_WARN("failed to cal hash");
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    ret = bloom_filter_.might_contain_vector(expr, ctx, skip, bound, hash_values, total_count,
                                             filter_count);
  }
  if (OB_FAIL(ret)) {
  } else {
    eval_flags.set_all(true);
    filter_ctx.check_count_ += total_count;
    filter_ctx.total_count_ += total_count;
    filter_ctx.filter_count_ += filter_count;
    filter_ctx.collect_sample_info(filter_count, total_count);
  }
  return ret;
}

int ObRFBloomFilterMsg::might_contain_vector(
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

template <typename ArgVec>
int ObRFBloomFilterMsg::insert_partition_bloom_filter(ArgVec *arg_vec,
                                                      const ObBatchRows *child_brs,
                                                      uint64_t *batch_hash_values)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_brs->size_; ++i) {
    if (ObExprCalcPartitionId::NONE_PARTITION_ID == arg_vec->get_int(i)) {
      continue;
    }
    if (child_brs->skip_->at(i)) {
      continue;
    } else if (OB_FAIL(bloom_filter_.put(batch_hash_values[i]))) {
      LOG_WARN("fail to put  hash value to px bloom filter", K(ret));
    } else if (is_empty_) {
      is_empty_ = false;
    }
  }
  return ret;
}

int ObRFBloomFilterMsg::insert_by_row_vector(
    const ObBatchRows *child_brs,
    const common::ObIArray<ObExpr *> &expr_array,
    const common::ObHashFuncs &hash_funcs,
    const ObExpr *calc_tablet_id_expr,
    ObEvalCtx &eval_ctx,
    uint64_t *batch_hash_values)
{
  int ret = OB_SUCCESS;
  if (child_brs->size_ > 0) {
    uint64_t seed = ObExprJoinFilter::JOIN_FILTER_SEED;
    EvalBound bound(child_brs->size_, child_brs->all_rows_active_);
    if (OB_NOT_NULL(calc_tablet_id_expr)) {
      if (hash_funcs.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected part id expr", K(ret));
      } else if (OB_FAIL(calc_tablet_id_expr->eval_vector(eval_ctx,
        *child_brs))) {
        LOG_WARN("failed to eval vector", K(ret));
      } else {
        VectorFormat arg_format = calc_tablet_id_expr->get_format(eval_ctx);
        if (VEC_UNIFORM == arg_format) {
          IntegerUniVec *arg_vec =
              static_cast<IntegerUniVec *>(calc_tablet_id_expr->get_vector(eval_ctx));
          if (OB_FAIL(arg_vec->murmur_hash_v3(*calc_tablet_id_expr, batch_hash_values,
                                              *(child_brs->skip_), bound, &seed, false))) {
            LOG_WARN("failed to cal murmur_hash_v2");
          } else if (OB_FAIL(insert_partition_bloom_filter(arg_vec,
                                                           child_brs, batch_hash_values))) {
             LOG_WARN("failed to cal insert_partition_bloom_filter");
          }
        } else if (VEC_FIXED == arg_format) {
          IntegerFixedVec *arg_vec =
              static_cast<IntegerFixedVec *>(calc_tablet_id_expr->get_vector(eval_ctx));
          if (OB_FAIL(arg_vec->murmur_hash_v3(*calc_tablet_id_expr, batch_hash_values,
                                              *(child_brs->skip_), bound, &seed, false))) {
            LOG_WARN("failed to cal murmur_hash_v2");
          } else if (OB_FAIL(insert_partition_bloom_filter(arg_vec,
                                                           child_brs, batch_hash_values))) {
             LOG_WARN("failed to cal insert_partition_bloom_filter");
          }
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr_array.count(); ++i) {
        ObExpr *expr = expr_array.at(i); // expr ptr check in cg, not check here
        if (OB_FAIL(expr->eval_vector(eval_ctx, *child_brs))) {
          LOG_WARN("eval_vector failed", K(ret));
        } else {
          const bool is_batch_seed = (i > 0);
          ObIVector *arg_vec = expr->get_vector(eval_ctx);
          arg_vec->murmur_hash_v3(*expr, batch_hash_values, *(child_brs->skip_), bound, is_batch_seed ? batch_hash_values : &seed, is_batch_seed);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(bloom_filter_.put_batch(batch_hash_values, bound, *child_brs->skip_, is_empty_))) {
        LOG_WARN("failed to push hash value to px bloom filter");
      }
    }
  }
  return ret;
}

int ObRFBloomFilterMsg::calc_hash_value(
    const common::ObIArray<ObExpr *> &expr_array,
    const common::ObHashFuncs &hash_funcs,
    const ObExpr *calc_tablet_id_expr,
    ObEvalCtx &eval_ctx,
    uint64_t &hash_value, bool &ignore)
{
  int ret = OB_SUCCESS;
  hash_value = ObExprJoinFilter::JOIN_FILTER_SEED;
  ignore = false;
  ObDatum *datum = nullptr;
  if (OB_NOT_NULL(calc_tablet_id_expr)) {
    int64_t partition_id = 0;
    if (hash_funcs.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected part id expr", K(ret));
    } else if (OB_FAIL(calc_tablet_id_expr->eval(eval_ctx, datum))) {
      LOG_WARN("failed to eval datum", K(ret));
    } else if (ObExprCalcPartitionId::NONE_PARTITION_ID == (partition_id = datum->get_int())) {
      ignore = true;
    } else if (OB_FAIL(hash_funcs.at(0).hash_func_(*datum, hash_value, hash_value))) {
      LOG_WARN("failed to do hash funcs", K(ret));
    }
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < expr_array.count() ; ++idx) {
      if (OB_FAIL(expr_array.at(idx)->eval(eval_ctx, datum))) {
        LOG_WARN("failed to eval datum", K(ret));
      } else if (OB_FAIL(hash_funcs.at(idx).hash_func_(*datum, hash_value, hash_value))) {
        LOG_WARN("failed to do hash funcs", K(ret));
      }
    }
  }
  return ret;
}

int ObRFBloomFilterMsg::broadcast(ObIArray<ObAddr> &target_addrs,
    obrpc::ObP2PDhRpcProxy &p2p_dh_proxy)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  CK(OB_NOT_NULL(filter_idx_) && OB_NOT_NULL(create_finish_));
  int64_t cur_idx = 0;
  ObRFBloomFilterMsg msg;
  ObPxP2pDhMsgCB msg_cb(GCTX.self_addr(),
      *ObCurTraceId::get_trace_id(),
      ObTimeUtility::current_time(),
      timeout_ts_,
      p2p_datahub_id_);
  ObPxP2PDatahubArg arg;

  arg.msg_ = &msg;
  while (!*create_finish_ && need_send_msg_ && OB_SUCC(ret)) {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("fail to check status", K(ret));
    }
    ob_usleep(10);
  }
  if (OB_FAIL(ret)) {
  } else if (!need_send_msg_) {
    // when drain_exch, not need to send msg
  } else if (OB_FAIL(msg.shadow_copy(*this))) {
    LOG_WARN("fail to shadow copy second phase msg", K(ret));
  } else if (OB_ISNULL(create_finish_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected create finish ptr", K(ret));
  } else {
    while (*filter_idx_ <  filter_indexes_.count() && OB_SUCC(ret)) {
      cur_idx = ATOMIC_FAA(filter_idx_, 1);
      if (cur_idx < filter_indexes_.count()) {
        msg.next_peer_addrs_.reuse();
        const BloomFilterIndex &addr_filter_idx = filter_indexes_.at(cur_idx);
        msg.bloom_filter_.set_begin_idx(addr_filter_idx.begin_idx_);
        msg.bloom_filter_.set_end_idx(addr_filter_idx.end_idx_);
        if (OB_FAIL(msg.next_peer_addrs_.init(addr_filter_idx.channel_ids_.count()))) {
          LOG_WARN("fail to init next_peer_addrs_", K(ret));
        }
        for (int i = 0; OB_SUCC(ret) && i < addr_filter_idx.channel_ids_.count(); ++i) {
          if (OB_FAIL(msg.next_peer_addrs_.push_back(
              target_addrs.at(addr_filter_idx.channel_ids_.at(i))))) {
            LOG_WARN("failed push back peer addr", K(i), K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (addr_filter_idx.channel_id_ >= target_addrs.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected channel id", K(addr_filter_idx.channel_id_), K(target_addrs.count()));
        } else if (OB_FAIL(p2p_dh_proxy.to(target_addrs.at(addr_filter_idx.channel_id_))
                  .by(tenant_id_)
                  .timeout(timeout_ts_)
                  .compressed(ObCompressorType::LZ4_COMPRESSOR)
                  .send_p2p_dh_message(arg, &msg_cb))) {
          LOG_WARN("fail to send bloom filter", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRFBloomFilterMsg::generate_filter_indexes(
  int64_t each_group_size,
  int64_t addr_cnt,
  int64_t piece_size)
{
  int ret = OB_SUCCESS;
  int64_t filter_len = bloom_filter_.get_bits_array_length();
  int64_t count = ceil(filter_len / (double)piece_size);
  int64_t start_idx = 0, end_idx = 0;
  int64_t group_addr_cnt = each_group_size > addr_cnt ?
        addr_cnt : each_group_size;
  lib::ObMemAttr attr(tenant_id_, "TmpBFIdxAlloc");
  common::ObArenaAllocator tmp_allocator(attr);
  BloomFilterIndex filter_index;
  ObSEArray<BloomFilterIndex *, 64> tmp_filter_indexes;
  filter_index.channel_ids_.set_allocator(&tmp_allocator);
  BloomFilterIndex *filter_index_ptr = nullptr;
  for (int i = 0; OB_SUCC(ret) && i < count; ++i) {
    start_idx = i * piece_size;
    end_idx = (i + 1) * piece_size;
    if (start_idx >= filter_len) {
      start_idx = filter_len - 1;
    }
    if (end_idx >= filter_len) {
      end_idx = filter_len - 1;
    }
    filter_index.begin_idx_ = start_idx;
    filter_index.end_idx_ = end_idx;
    int64_t group_count = ceil((double)addr_cnt / group_addr_cnt);
    int64_t start_channel = ObRandom::rand(0, group_count - 1);
    start_channel *= group_addr_cnt;
    int pos = 0;
    for (int j = start_channel; OB_SUCC(ret) &&
        j < start_channel + addr_cnt;
        j += group_addr_cnt) {
      pos = (j >= addr_cnt ? j - addr_cnt : j);
      pos = (pos / group_addr_cnt) * group_addr_cnt;
      filter_index.channel_ids_.reset();
      if (pos + group_addr_cnt > addr_cnt) {
        filter_index.channel_id_ = (i % (addr_cnt - pos)) + pos;
      } else {
        filter_index.channel_id_ = (i % group_addr_cnt) + pos;
      }
      if (OB_FAIL(filter_index.channel_ids_.init(min(addr_cnt, pos + group_addr_cnt) - pos + 1))) {
        LOG_WARN("failed to init channel_ids_");
      }
      for (int k = pos; OB_SUCC(ret) && k < addr_cnt && k < pos + group_addr_cnt; ++k) {
        OZ(filter_index.channel_ids_.push_back(k));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(filter_index_ptr = OB_NEWx(BloomFilterIndex, &tmp_allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc BloomFilterIndex");
      } else if (FALSE_IT(filter_index_ptr->channel_ids_.set_allocator(&tmp_allocator))) {
      } else if (OB_FAIL(filter_index_ptr->assign(filter_index))) {
        LOG_WARN("failed to assign");
      } else if (OB_FAIL(tmp_filter_indexes.push_back(filter_index_ptr))) {
        LOG_WARN("failed to push_back");
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(filter_indexes_.prepare_allocate(tmp_filter_indexes.count()))) {
    LOG_WARN("failed to prepare_allocate filter_indexes_");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_filter_indexes.count(); ++i) {
      filter_indexes_.at(i).channel_ids_.set_allocator(&allocator_);
      if (OB_FAIL(filter_indexes_.at(i).assign(*tmp_filter_indexes.at(i)))) {
        LOG_WARN("failed to assign filter_indexes", K(i));
      }
    }
  }
  filter_index.channel_ids_.destroy();
  return ret;
}
//end ObRFBloomFilterMsg

//ObRFRangeFilterMsg
ObRFRangeFilterMsg::ObRFRangeFilterMsg()
: ObP2PDatahubMsgBase(), lower_bounds_(allocator_), upper_bounds_(allocator_),
  need_null_cmp_flags_(allocator_), cells_size_(allocator_),
  cmp_funcs_(allocator_), query_range_info_(allocator_),
  query_range_(), is_query_range_ready_(false), query_range_allocator_(),
  build_obj_metas_(allocator_)
{
}

int ObRFRangeFilterMsg::reuse()
{
  int ret = OB_SUCCESS;
  is_empty_ = true;
  lower_bounds_.reset();
  upper_bounds_.reset();
  cells_size_.reset();
  if (OB_FAIL(lower_bounds_.prepare_allocate(cmp_funcs_.count()))) {
    LOG_WARN("fail to prepare allocate col cnt", K(ret));
  } else if (OB_FAIL(upper_bounds_.prepare_allocate(cmp_funcs_.count()))) {
    LOG_WARN("fail to prepare allocate col cnt", K(ret));
  } else if (OB_FAIL(cells_size_.prepare_allocate(cmp_funcs_.count()))) {
    LOG_WARN("fail to prepare allocate col cnt", K(ret));
  }
  (void)reuse_query_range();
  return ret;
}

int ObRFRangeFilterMsg::assign(const ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  const ObRFRangeFilterMsg &other_msg = static_cast<const ObRFRangeFilterMsg &>(msg);
  if (OB_FAIL(ObP2PDatahubMsgBase::assign(msg))) {
    LOG_WARN("failed to assign base data", K(ret));
  } else if (OB_FAIL(lower_bounds_.assign(other_msg.lower_bounds_))) {
    LOG_WARN("fail to assign lower bounds", K(ret));
  } else if (OB_FAIL(upper_bounds_.assign(other_msg.upper_bounds_))) {
    LOG_WARN("fail to assign upper bounds", K(ret));
  } else if (OB_FAIL(cmp_funcs_.assign(other_msg.cmp_funcs_))) {
    LOG_WARN("failed to assign cmp funcs", K(ret));
  } else if (OB_FAIL(build_obj_metas_.assign(other_msg.build_obj_metas_))) {
    LOG_WARN("failed to assign build obj_metas", K(ret));
  } else if (OB_FAIL(need_null_cmp_flags_.assign(other_msg.need_null_cmp_flags_))) {
    LOG_WARN("failed to assign cmp flags", K(ret));
  } else if (OB_FAIL(cells_size_.assign(other_msg.cells_size_))) {
    LOG_WARN("failed to assign cell size", K(ret));
  } else if (OB_FAIL(adjust_cell_size())) {
    LOG_WARN("fail to adjust cell size", K(ret));
  } else if (OB_FAIL(query_range_info_.assign(other_msg.query_range_info_))) {
    LOG_WARN("fail to assign query_range_info_", K(ret));
  }
  return ret;
}

int ObRFRangeFilterMsg::deep_copy_msg(ObP2PDatahubMsgBase *&new_msg_ptr)
{
  int ret = OB_SUCCESS;
  ObRFRangeFilterMsg *rf_msg = nullptr;
  ObMemAttr attr(tenant_id_, "PxRangeMsg");
  if (OB_FAIL(PX_P2P_DH.alloc_msg<ObRFRangeFilterMsg>(attr, rf_msg))) {
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
    if (OB_SUCC(ret)) {
      new_msg_ptr = rf_msg;
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(rf_msg)) {
    rf_msg->destroy();
    ob_free(rf_msg);
  }
  return ret;
}

int ObRFRangeFilterMsg::merge(ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  ObRFRangeFilterMsg &range_msg = static_cast<ObRFRangeFilterMsg &>(msg);
  CK(range_msg.lower_bounds_.count() == lower_bounds_.count() &&
     range_msg.upper_bounds_.count() == upper_bounds_.count());
  if (OB_FAIL(ret)) {
    LOG_WARN("unexpected bounds count", K(lower_bounds_.count()), K(range_msg.lower_bounds_.count()));
  } else if (range_msg.is_empty_) {
    /*do nothing*/
  } else {
    ObSpinLockGuard guard(lock_);
    if (OB_FAIL(get_min(range_msg.lower_bounds_))) {
      LOG_WARN("fail to get min lower bounds", K(ret));
    } else if (OB_FAIL(get_max(range_msg.upper_bounds_))) {
      LOG_WARN("fail to get max lower bounds", K(ret));
    } else if (is_empty_) {
      is_empty_ = false;
    }
  }
  return ret;
}

int ObRFRangeFilterMsg::get_min(ObIArray<ObDatum> &vals)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < vals.count() && OB_SUCC(ret); ++i) {
    // null value is also suitable
    if (OB_FAIL(get_min(cmp_funcs_.at(i), lower_bounds_.at(i),
        vals.at(i), cells_size_.at(i).min_datum_buf_size_))) {
      LOG_WARN("fail to compare value", K(ret));
    }
  }
  return ret;
}

int ObRFRangeFilterMsg::get_max(ObIArray<ObDatum> &vals)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < vals.count() && OB_SUCC(ret); ++i) {
    // null value is also suitable
    if (OB_FAIL(get_max(cmp_funcs_.at(i), upper_bounds_.at(i),
        vals.at(i), cells_size_.at(i).max_datum_buf_size_))) {
      LOG_WARN("fail to compare value", K(ret));
    }
  }
  return ret;
}

int ObRFRangeFilterMsg::get_min(ObCmpFunc &func, ObDatum &l, ObDatum &r, int64_t &cell_size)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  // when [null, null] merge [a, b], the expect result in mysql mode is [null, b]
  // the lower bound l, with ptr==NULL and null_==true, should not be covered by a.
  //
  // the reason we remove the OB_ISNULL(l.ptr_) condition is that when l is a empty char with l.ptr=0x0 and
  // l.len=0 and null_=false, it should not be corver by r directly
  if (is_empty_) {
    if (OB_FAIL(dynamic_copy_cell(r, l, cell_size))) {
      LOG_WARN("fail to deep copy datum");
    }
  } else if (OB_FAIL(func.cmp_func_(l, r, cmp))) {
    LOG_WARN("fail to cmp", K(ret));
  } else if (cmp > 0) {
    if (OB_FAIL(dynamic_copy_cell(r, l, cell_size))) {
      LOG_WARN("fail to deep copy datum");
    }
  }
  return ret;
}

int ObRFRangeFilterMsg::prepare_query_range()
{
  int ret = OB_SUCCESS;
  (void)reuse_query_range();
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
  LOG_TRACE("range filter prepare query range", K(ret), K(is_query_range_ready_), K(query_range_), K(query_range_info_), K(is_empty_));
  return ret;
}

void ObRFRangeFilterMsg::after_process()
{
  // prepare_query_range can be failed, but rf still worked
  (void)prepare_query_range();
}

int ObRFRangeFilterMsg::try_extract_query_range(bool &has_extract, ObIArray<ObNewRange> &ranges)
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

int ObRFRangeFilterMsg::adjust_cell_size()
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

int ObRFRangeFilterMsg::dynamic_copy_cell(const ObDatum &src, ObDatum &target, int64_t &cell_size)
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

int ObRFRangeFilterMsg::get_max(ObCmpFunc &func, ObDatum &l, ObDatum &r, int64_t &cell_size)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (is_empty_) {
    if (OB_FAIL(dynamic_copy_cell(r, l, cell_size))) {
      LOG_WARN("fail to deep copy datum");
    }
  } else if (OB_FAIL(func.cmp_func_(l, r, cmp))) {
    LOG_WARN("fail to cmp value", K(ret));
  } else if (cmp < 0) {
    if (OB_FAIL(dynamic_copy_cell(r, l, cell_size))) {
      LOG_WARN("fail to deep copy datum");
    }
  }
  return ret;
}

int ObRFRangeFilterMsg::insert_by_row(
    const common::ObIArray<ObExpr *> &expr_array,
    const common::ObHashFuncs &hash_funcs,
    const ObExpr *calc_tablet_id_expr,
    ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(hash_funcs);
  ObDatum *datum = nullptr;
  if (is_empty_) {
    bool ignore_null = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_array.count(); ++i) {
      ObExpr *expr = expr_array.at(i);
      if (OB_FAIL(expr->eval(eval_ctx, datum))) {
        LOG_WARN("fail to eval expr", K(ret));
      } else if (datum->is_null() && !need_null_cmp_flags_.at(i)) {
        ignore_null = true;
        break;
      } else if (OB_FAIL(dynamic_copy_cell(*datum, lower_bounds_.at(i), cells_size_.at(i).min_datum_buf_size_))) {
        LOG_WARN("fail to deep copy datum", K(ret));
      } else if (OB_FAIL(dynamic_copy_cell(*datum, upper_bounds_.at(i), cells_size_.at(i).max_datum_buf_size_))) {
        LOG_WARN("fail to deep copy datum", K(ret));
      }
    }
    if (!ignore_null) {
      is_empty_ = false;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_array.count(); ++i) {
      ObExpr *expr = expr_array.at(i);
      if (OB_FAIL(expr->eval(eval_ctx, datum))) {
        LOG_WARN("fail to eval expr", K(ret));
      } else if (datum->is_null() && !need_null_cmp_flags_.at(i)) {
        /*do nothing*/
        break;
      } else if (OB_FAIL(get_min(cmp_funcs_.at(i), lower_bounds_.at(i), *datum, cells_size_.at(i).min_datum_buf_size_))) {
        LOG_WARN("failed to compare value", K(ret));
      } else if (OB_FAIL(get_max(cmp_funcs_.at(i), upper_bounds_.at(i), *datum, cells_size_.at(i).max_datum_buf_size_))) {
        LOG_WARN("failed to compare value", K(ret));
      }
    }
  }
  return ret;
}

int ObRFRangeFilterMsg::insert_by_row_batch(
  const ObBatchRows *child_brs,
  const common::ObIArray<ObExpr *> &expr_array,
  const common::ObHashFuncs &hash_funcs,
  const ObExpr *calc_tablet_id_expr,
  ObEvalCtx &eval_ctx,
  uint64_t *batch_hash_values)
{
  int ret = OB_SUCCESS;
  UNUSED(batch_hash_values);
  UNUSED(calc_tablet_id_expr);
  if (child_brs->size_ > 0) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
    batch_info_guard.set_batch_size(child_brs->size_);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < child_brs->size_; ++idx) {
      if (child_brs->skip_->at(idx)) {
        continue;
      } else {
        batch_info_guard.set_batch_idx(idx);
        if (OB_FAIL(insert_by_row(expr_array, hash_funcs,
            calc_tablet_id_expr, eval_ctx))) {
          LOG_WARN("fail to insert by row", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRFRangeFilterMsg::might_contain(const ObExpr &expr,
      ObEvalCtx &ctx,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx,
      ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  ObCmpFunc cmp_func;
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
        cmp_func.cmp_func_ = filter_ctx.cmp_funcs_.at(i).cmp_func_;
        if (OB_FAIL(cmp_func.cmp_func_(*datum, lower_bounds_.at(i), cmp_min))) {
          LOG_WARN("fail to compare value", K(ret));
        } else if (cmp_min < 0) {
          is_match = false;
          break;
        } else if (OB_FAIL(cmp_func.cmp_func_(*datum, upper_bounds_.at(i), cmp_max))) {
          LOG_WARN("fail to compare value", K(ret));
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

int ObRFRangeFilterMsg::do_might_contain_batch(const ObExpr &expr,
    ObEvalCtx &ctx,
    const ObBitVector &skip,
    const int64_t batch_size,
    ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx) {
  int ret = OB_SUCCESS;
  int64_t filter_count = 0;
  int64_t total_count = 0;
  ObDatum *results = expr.locate_batch_datums(ctx);
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
      for (int arg_i = 0; OB_SUCC(ret) && arg_i < expr.arg_cnt_; ++arg_i) {
        datum = &expr.args_[arg_i]->locate_expr_datum(ctx, batch_i);
        if (OB_FAIL(filter_ctx.cmp_funcs_.at(arg_i).cmp_func_(*datum, lower_bounds_.at(arg_i), cmp_min))) {
          LOG_WARN("fail to compare value", K(ret));
        } else if (cmp_min < 0) {
          filter_count++;
          is_match = false;
          break;
        } else if (OB_FAIL(filter_ctx.cmp_funcs_.at(arg_i).cmp_func_(*datum, upper_bounds_.at(arg_i), cmp_max))) {
          LOG_WARN("fail to compare value", K(ret));
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

int ObRFRangeFilterMsg::might_contain_batch(
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

int ObRFRangeFilterMsg::prepare_storage_white_filter_data(ObDynamicFilterExecutor &dynamic_filter,
                                ObEvalCtx &eval_ctx,
                                ObRuntimeFilterParams &params,
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
    dynamic_filter.set_filter_val_meta(build_obj_metas_.at(col_idx));
    is_data_prepared = true;
  }
  return ret;
}

// end ObRFRangeFilterMsg

// ObRFInFilterMsg

int ObRFInFilterMsg::assign(const ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  const ObRFInFilterMsg &other_msg = static_cast<const ObRFInFilterMsg &>(msg);
  if (OB_FAIL(ObP2PDatahubMsgBase::assign(msg))) {
    LOG_WARN("failed to assign base data", K(ret));
  } else if (OB_FAIL(cmp_funcs_.assign(other_msg.cmp_funcs_))) {
    LOG_WARN("fail to assign bf msg", K(ret));
  } else if (OB_FAIL(hash_funcs_for_insert_.assign(other_msg.hash_funcs_for_insert_))) {
    LOG_WARN("fail to assign bf msg", K(ret));
  } else if (OB_FAIL(cur_row_.assign(other_msg.cur_row_))) {
    LOG_WARN("failed to assign filter indexes", K(ret));
  } else if (OB_FAIL(need_null_cmp_flags_.assign(other_msg.need_null_cmp_flags_))) {
    LOG_WARN("failed to assign filter indexes", K(ret));
  } else if (OB_FAIL(query_range_info_.assign(other_msg.query_range_info_))) {
    LOG_WARN("fail to assign query_range_info_", K(ret));
  } else if (OB_FAIL(build_obj_metas_.assign(other_msg.build_obj_metas_))) {
    LOG_WARN("failed to assign build obj_metas", K(ret));
  } else {
    col_cnt_ = other_msg.col_cnt_;
    max_in_num_ = other_msg.max_in_num_;
  }
  return ret;
}

int ObRFInFilterMsg::deep_copy_msg(ObP2PDatahubMsgBase *&new_msg_ptr)
{
  int ret = OB_SUCCESS;
  ObRFInFilterMsg *in_msg = nullptr;
  int64_t row_cnt = max(serial_rows_.count(), 1);
  ObMemAttr attr(tenant_id_, "PxInMsg");
  if (OB_FAIL(PX_P2P_DH.alloc_msg<ObRFInFilterMsg>(attr, in_msg))) {
    LOG_WARN("fail to alloc rf msg", K(ret));
  } else if (OB_FAIL(in_msg->assign(*this))) {
    LOG_WARN("fail to assign rf msg", K(ret));
  } else if (OB_FAIL(in_msg->rows_set_.create(row_cnt * 2,
        "RFCPInFilter",
        "RFCPInFilter"))) {
    LOG_WARN("fail to init in hash set", K(ret));
  } else {
    int64_t row_cnt = serial_rows_.count();
    if (0 == row_cnt) {
    } else {
      for (int i = 0; i < row_cnt && OB_SUCC(ret); ++i) {
        for (int j = 0; j < col_cnt_ && OB_SUCC(ret); ++j) {
          in_msg->cur_row_.at(j) = serial_rows_.at(i)->at(j);
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(in_msg->append_row())) {
            LOG_WARN("fail to append row", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      new_msg_ptr = in_msg;
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(in_msg)) {
    in_msg->destroy();
    ob_free(in_msg);
  }
  return ret;
}

int ObRFInFilterMsg::insert_by_row_batch(
  const ObBatchRows *child_brs,
  const common::ObIArray<ObExpr *> &expr_array,
  const common::ObHashFuncs &hash_funcs,
  const ObExpr *calc_tablet_id_expr,
  ObEvalCtx &eval_ctx,
  uint64_t *batch_hash_values)
{
  int ret = OB_SUCCESS;
  UNUSED(batch_hash_values);
  UNUSED(calc_tablet_id_expr);
  if (child_brs->size_ > 0 && is_active_) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
    batch_info_guard.set_batch_size(child_brs->size_);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < child_brs->size_; ++idx) {
      if (child_brs->skip_->at(idx)) {
        continue;
      } else {
        batch_info_guard.set_batch_idx(idx);
        ObDatum *datum = nullptr;
        bool ignore_null_row = false;
        for (int64_t i = 0; OB_SUCC(ret) && i < expr_array.count(); ++i) {
          ObExpr *expr = expr_array.at(i);
          if (OB_FAIL(expr->eval(eval_ctx, datum))) {
            LOG_WARN("fail to eval expr", K(ret));
          } else if (datum->is_null() && !need_null_cmp_flags_.at(i)) {
            ignore_null_row = true;
            break;
          } else {
            cur_row_.at(i) = (*datum);
          }
        }
        if (OB_SUCC(ret) && !ignore_null_row) {
          if (OB_FAIL(insert_node())) {
            LOG_WARN("fail to insert node", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObRFInFilterMsg::insert_node()
{
  int ret = OB_SUCCESS;
  ObRFInFilterNode node(&cmp_funcs_, &hash_funcs_for_insert_, &cur_row_);
  if (OB_FAIL(rows_set_.exist_refactored(node))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (serial_rows_.count() > max_in_num_) {
        is_active_ = false;
      } else if (OB_FAIL(append_row())) {
        LOG_WARN("fail to append row", K(ret));
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

int ObRFInFilterMsg::insert_by_row(
    const common::ObIArray<ObExpr *> &expr_array,
    const common::ObHashFuncs &hash_funcs,
    const ObExpr *calc_tablet_id_expr,
    ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  if (is_active_) {
    bool ignore_null_row = false;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < expr_array.count() ; ++idx) {
      datum = nullptr;
      if (OB_FAIL(expr_array.at(idx)->eval(eval_ctx, datum))) {
        LOG_WARN("failed to eval datum", K(ret));
      } else if (datum->is_null() && !need_null_cmp_flags_.at(idx)) {
        ignore_null_row = true;
        break;
      } else {
        cur_row_.at(idx) = (*datum);
      }
    }
    if (OB_SUCC(ret) && !ignore_null_row) {
      if (OB_FAIL(insert_node())) {
        LOG_WARN("fail to insert node", K(ret));
      }
    }
  }

  return ret;
}

int ObRFInFilterMsg::append_row()
{
  int ret = OB_SUCCESS;
  ObFixedArray<ObDatum, ObIAllocator> *new_row = nullptr;
  void *array_ptr = nullptr;
  if (OB_ISNULL(array_ptr = allocator_.alloc(sizeof(ObFixedArray<ObDatum, ObIAllocator>)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    new_row = new(array_ptr) ObFixedArray<ObDatum, ObIAllocator>(allocator_);
    if (OB_FAIL(new_row->init(cur_row_.count()))) {
      LOG_WARN("fail to init cur row", K(ret));
    } else {
      ObDatum datum;
      for (int i = 0; i < cur_row_.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(datum.deep_copy(cur_row_.at(i), allocator_))) {
          LOG_WARN("fail to deep copy datum", K(ret));
        } else if (OB_FAIL(new_row->push_back(datum))) {
          LOG_WARN("fail to push back new row", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(serial_rows_.push_back(new_row))) {
          LOG_WARN("fail to push back serial rows", K(ret));
        } else {
          ObRFInFilterNode node(&cmp_funcs_, &hash_funcs_for_insert_, new_row);
          if (OB_FAIL(rows_set_.set_refactored(node))) {
            LOG_WARN("fail to insert in filter node", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObRFInFilterMsg::ObRFInFilterNode::hash(uint64_t &hash_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hash_funcs_)) {
    hash_ret = hash_val_;
  } else {
    hash_ret = ObExprJoinFilter::JOIN_FILTER_SEED;
    for (int i = 0; i < row_->count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(hash_funcs_->at(i).hash_func_(row_->at(i), hash_ret, hash_ret))) {
        LOG_WARN("fail to calc hash value", K(ret), K(hash_ret));
      }
    }
  }

  return ret;
}

// the ObRFInFilterNode stores in ObRFInFilter always be the datum of build table,
// while the other node can be the build table(during insert or merge process)
// or the probe table(during filter process),
// so the compare process relys on the other node, always using other's cmp_func_.
bool ObRFInFilterMsg::ObRFInFilterNode::operator==(const ObRFInFilterNode &other) const
{
  int cmp_ret = 0;
  bool ret = true;
  for (int i = 0; i < other.row_->count(); ++i) {
    if (row_->at(i).is_null() && other.row_->at(i).is_null()) {
      continue;
    } else {
      // because cmp_func is chosen as compare(probe_data/build_data, build_data)
      // so the other's data must be placed at first
      int tmp_ret = other.cmp_funcs_->at(i).cmp_func_(other.row_->at(i), row_->at(i), cmp_ret);
      if (cmp_ret != 0) {
        ret = false;
        break;
      }
    }
  }
  return ret;
}

int ObRFInFilterMsg::merge(ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  ObRFInFilterMsg &in_msg = static_cast<ObRFInFilterMsg &>(msg);
  if (!msg.is_active()) {
    is_active_ = false;
  } else if (!msg.is_empty() && is_active_) {
    ObSpinLockGuard guard(lock_);
    for (int i = 0; i < in_msg.serial_rows_.count() && OB_SUCC(ret); ++i) {
      for (int j = 0; j < in_msg.serial_rows_.at(i)->count(); ++j) {
        cur_row_.at(j) = in_msg.serial_rows_.at(i)->at(j);
      }
      if (OB_FAIL(insert_node())) {
        LOG_WARN("fail to insert node", K(ret));
      }
    }
  }
  return ret;
}

int ObRFInFilterMsg::might_contain(const ObExpr &expr,
      ObEvalCtx &ctx,
      ObExprJoinFilter::ObExprJoinFilterContext &filter_ctx,
      ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  bool is_match = true;
  uint64_t hash_val = ObExprJoinFilter::JOIN_FILTER_SEED;
  ObIArray<ObDatum> &cur_row = filter_ctx.cur_row_;
  if (OB_UNLIKELY(!is_active_)) {
    res.set_int(1);
  } else if (OB_UNLIKELY(is_empty_)) {
    res.set_int(0);
    filter_ctx.filter_count_++;
    filter_ctx.check_count_++;
  } else {
    for (int i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
      if (OB_FAIL(expr.args_[i]->eval(ctx, datum))) {
        LOG_WARN("failed to eval datum", K(ret));
      } else {
        cur_row.at(i) = *datum;
        ObHashFunc hash_func;
        hash_func.hash_func_ = filter_ctx.hash_funcs_.at(i).hash_func_;
        if (OB_FAIL(hash_func.hash_func_(*datum, hash_val, hash_val))) {
          LOG_WARN("fail to calc hash val", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObRFInFilterNode node(&filter_ctx.cmp_funcs_, nullptr, &cur_row, hash_val);
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

int ObRFInFilterMsg::reuse()
{
  int ret = OB_SUCCESS;
  is_empty_ = true;
  serial_rows_.reset();
  rows_set_.reuse();
  (void)reuse_query_range();
  return ret;
}

void ObRFInFilterMsg::check_finish_receive()
{
  if (ATOMIC_LOAD(&is_active_)) {
    if (msg_receive_expect_cnt_ == ATOMIC_LOAD(&msg_receive_cur_cnt_)) {
      (void)after_process();
      is_ready_ = true;
    }
  }
}

int ObRFInFilterMsg::do_might_contain_batch(const ObExpr &expr,
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
  ObIArray<ObDatum> &cur_row = filter_ctx.cur_row_;
  ObRFInFilterNode node(&filter_ctx.cmp_funcs_, nullptr, &cur_row, 0);
  ObDatum *res_datums = expr.locate_batch_datums(ctx);
  for (int64_t batch_i = 0; OB_SUCC(ret) && batch_i < batch_size; ++batch_i) {
    if (skip.at(batch_i)) {
      continue;
    }
    total_count++;
    node.hash_val_ = right_hash_vals[batch_i];
    for (int64_t arg_i = 0; OB_SUCC(ret) && arg_i < expr.arg_cnt_; ++arg_i) {
      cur_row.at(arg_i) = expr.args_[arg_i]->locate_expr_datum(ctx, batch_i);
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

int ObRFInFilterMsg::might_contain_batch(
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

int ObRFInFilterMsg::prepare_storage_white_filter_data(ObDynamicFilterExecutor &dynamic_filter,
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
    for (int64_t i = 0; i < serial_rows_.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(params.push_back(serial_rows_.at(i)->at(col_idx)))) {
        LOG_WARN("failed to push back");
      }
    }
    if (OB_SUCC(ret)) {
      dynamic_filter.set_filter_val_meta(build_obj_metas_.at(col_idx));
      is_data_prepared = true;
    }
  }
  return ret;
}

int ObRFInFilterMsg::destroy()
{
  int ret = OB_SUCCESS;
  rows_set_.destroy();
  hash_funcs_for_insert_.reset();
  cmp_funcs_.reset();
  need_null_cmp_flags_.reset();
  build_obj_metas_.reset();
  cur_row_.reset();
  for (int i = 0; i < serial_rows_.count(); ++i) {
    if (OB_NOT_NULL(serial_rows_.at(i))) {
      serial_rows_.at(i)->reset();
    }
  }
  serial_rows_.reset();
  query_range_info_.destroy();
  query_range_.destroy();
  query_range_allocator_.reset();
  allocator_.reset();
  return ret;
}

int ObRFInFilterMsg::prepare_query_ranges()
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
  } else if (query_range_info_.prefix_col_idxs_.count() == col_cnt_) {
    // col count matches, the hashmap make sure all rows in the filter are different
    // so not need to dedupcate
    ret = process_query_ranges_without_deduplicate();
  } else {
    // prefix col less than index column, need do deduplicate
    // for example:
    // there are three rows int the filter :{[1,2,3], [1,2,4], [1,2,5]}
    // and the range column is c1,c2
    // final query range extracted should be: range(1,2; 1,2)
    // we need to deduplicate to avoid duplicate range
    ret = process_query_ranges_with_deduplicate();
  }
  LOG_TRACE("in filter prepare query range", K(ret), K(is_query_range_ready_),
            K(query_range_.count()), K(rows_set_.size()),
            K(query_range_info_.prefix_col_idxs_.count()), K(col_cnt_), K(query_range_),
            K(query_range_info_), K(is_empty_));
  return ret;
}

int ObRFInFilterMsg::process_query_ranges_with_deduplicate()
{
  int ret = OB_SUCCESS;
  int64_t max_in_filter_query_range_count = ObPxQueryRangeInfo::MAX_IN_FILTER_QUERY_RANGE_COUNT;

#ifdef ERRSIM
  int tmp_ret = OB_E(EventTable::EN_PX_MAX_IN_FILTER_QR_COUNT) OB_SUCCESS;
  if (OB_SUCCESS != tmp_ret) {
    max_in_filter_query_range_count = max_in_num_;
  }
#endif

  hash::ObHashSet<ObRFInFilterNode, hash::NoPthreadDefendMode> tmp_rows_set;
  ObArenaAllocator tmp_allocator;
  ObHashFuncs hash_func(tmp_allocator);
  ObCmpFuncs cmp_funcs(tmp_allocator);
  const ObIArray<int64_t> &prefix_col_idxs = query_range_info_.prefix_col_idxs_;

  if (OB_FAIL(tmp_rows_set.create(rows_set_.size() * 2, "RFInTmpHashSet", "RFInTmpHashSet"))) {
    LOG_WARN("fail to init in hash set", K(ret));
  } else if (OB_FAIL(hash_func.init(prefix_col_idxs.count()))) {
    LOG_WARN("failed to init hash func");
  } else if (OB_FAIL(cmp_funcs.init(prefix_col_idxs.count()))) {
    LOG_WARN("failed to init compare func");
  }
  // reorder compare function and hash function
  for (int64_t j = 0; j < prefix_col_idxs.count() && OB_SUCC(ret); ++j) {
    int64_t col_idx = prefix_col_idxs.at(j);
    if (OB_FAIL(hash_func.push_back(hash_funcs_for_insert_.at(col_idx)))) {
      LOG_WARN("failed to pushback hash func");
    } else if (OB_FAIL(cmp_funcs.push_back(cmp_funcs_.at(col_idx)))) {
      LOG_WARN("failed to pushback compare func");
    }
  }
  ObTMArray<ObTMArray<ObDatum>> tmp_rows;
  ObTMArray<int64_t> effective_row_idxs;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tmp_rows.prepare_allocate(serial_rows_.count()))) {
    LOG_WARN("failed to prepare_allocate query_range_", K(serial_rows_.count()));
  } else if (OB_FAIL(effective_row_idxs.reserve(serial_rows_.count()))) {
    LOG_WARN("failed to reserve query_range_", K(serial_rows_.count()));
  }
  for (int64_t row_idx = 0; row_idx < serial_rows_.count() && OB_SUCC(ret); ++row_idx) {
    if (OB_ISNULL(serial_rows_.at(row_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("this row is null", K(ret));
    } else {
      ObTMArray<ObDatum> &tmp_row = tmp_rows.at(row_idx);
      if (OB_FAIL(tmp_row.prepare_allocate(prefix_col_idxs.count()))) {
        LOG_WARN("failed to prepare_allocate tmp_row");
      }
      for (int64_t j = 0; j < prefix_col_idxs.count() && OB_SUCC(ret); ++j) {
        int64_t col_idx = prefix_col_idxs.at(j);
        tmp_row.at(j) = serial_rows_.at(row_idx)->at(col_idx);
      }
      bool is_duplicate = true;
      if (OB_SUCC(ret)) {
        ObRFInFilterNode node(&cmp_funcs, &hash_func, &tmp_row);
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
                  K(serial_rows_.count()), K(query_range_));
      }
    }
  }
  return ret;
}

int ObRFInFilterMsg::process_query_ranges_without_deduplicate()
{
  int ret = OB_SUCCESS;
  int64_t max_in_filter_query_range_count = ObPxQueryRangeInfo::MAX_IN_FILTER_QUERY_RANGE_COUNT;

#ifdef ERRSIM
  int tmp_ret = OB_E(EventTable::EN_PX_MAX_IN_FILTER_QR_COUNT) OB_SUCCESS;
  if (OB_SUCCESS != tmp_ret) {
    max_in_filter_query_range_count = max_in_num_;
  }
#endif

  if (serial_rows_.count() > max_in_filter_query_range_count) {
    is_query_range_ready_ = false;
  } else {
    if (OB_FAIL(query_range_.reserve(serial_rows_.count()))) {
      LOG_WARN("failed to reserve query_range_", K(serial_rows_.count()));
    }
    for (int64_t row_idx = 0; row_idx < serial_rows_.count() && OB_SUCC(ret); ++row_idx) {
      if (OB_ISNULL(serial_rows_.at(row_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this row is null", K(ret));
      } else {
        OZ(generate_one_range(row_idx));
      }
    }
    if (OB_SUCC(ret)) {
      is_query_range_ready_ = true;
      LOG_DEBUG("TBDelete in filter succ extract query range", K(serial_rows_.count()),
                K(query_range_));
    }
  }
  return ret;
}

int ObRFInFilterMsg::generate_one_range(int row_idx)
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
    ObDatum &datum = serial_rows_.at(row_idx)->at(col_idx);
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

void ObRFInFilterMsg::after_process()
{
  // prepare_query_ranges can be failed, but rf still worked
  (void)prepare_query_ranges();
}

int ObRFInFilterMsg::try_extract_query_range(bool &has_extract, ObIArray<ObNewRange> &ranges)
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

//end ObRFInFilterMsg



