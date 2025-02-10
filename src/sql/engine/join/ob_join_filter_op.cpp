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
#include "sql/engine/join/ob_join_filter_op.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"
#include "sql/engine/join/hash_join/ob_hash_join_vec_op.h"
#include "sql/engine/join/ob_partition_store.h"
#include "sql/engine/px/ob_px_sqc_handler.h"


using namespace oceanbase;
using namespace common;
using namespace omt;
using namespace sql;
using namespace oceanbase::sql::dtl;

ERRSIM_POINT_DEF(DHSENDOPTLOCAL);

OB_SERIALIZE_MEMBER(ObJoinFilterShareInfo,
    unfinished_count_ptr_,
    ch_provider_ptr_,
    release_ref_ptr_,
    filter_ptr_,
    shared_msgs_,
    ser_shared_jf_constructor_);

OB_SERIALIZE_MEMBER(ObJoinFilterRuntimeConfig,
    bloom_filter_ratio_,
    each_group_size_,
    bf_piece_size_,
    runtime_filter_wait_time_ms_,
    runtime_filter_max_in_num_,
    runtime_bloom_filter_max_size_,
    px_message_compression_,
    build_send_opt_);

OB_SERIALIZE_MEMBER(ObRuntimeFilterInfo,
                    filter_expr_id_,
                    p2p_datahub_id_,
                    filter_shared_type_,
                    dh_msg_type_);

OB_SERIALIZE_MEMBER((ObJoinFilterSpec, ObOpSpec),
                    mode_,
                    filter_id_,
                    filter_len_,
                    join_keys_,
                    hash_funcs_,
                    cmp_funcs_,
                    filter_shared_type_,
                    calc_tablet_id_expr_,
                    rf_infos_,
                    need_null_cmp_flags_,
                    is_shuffle_,
                    each_group_size_,
                    rf_build_cmp_infos_,
                    rf_probe_cmp_infos_,
                    px_query_range_info_,
                    bloom_filter_ratio_,
                    send_bloom_filter_size_,
                    jf_material_control_info_,
                    join_type_,
                    full_hash_join_keys_,
                    hash_join_is_ns_equal_cond_,
                    rf_max_wait_time_ms_, // FARM COMPAT WHITELIST
                    use_ndv_runtime_bloom_filter_size_ // FARM COMPAT WHITELIST
                    );

OB_SERIALIZE_MEMBER(ObJoinFilterOpInput,
    share_info_,
    bf_idx_at_sqc_proxy_,
    px_sequence_id_,
    config_);

int SharedJoinFilterConstructor::init()
{
  return cond_.init(common::ObWaitEventIds::DEFAULT_COND_WAIT);
}

int SharedJoinFilterConstructor::reset_for_rescan()
{
  // Generally speaking, shared join filter is not supported to rescan, this code
  // is only for defense.
  int ret = OB_ERR_UNEXPECTED;
  if (try_release_constructor()) {
    is_bloom_filter_constructed_ = false;
  }
  LOG_ERROR("This may be a unexpected way");
  return ret;
}

int SharedJoinFilterConstructor::wait_constructed(ObOperator *join_filter_op,
                                                      ObRFBloomFilterMsg *bf_msg)
{
  int ret = OB_SUCCESS;
  int64_t loop_time = 0;
  while (OB_SUCC(ret)) {
    {
      ObThreadCondGuard guard(cond_);
      if (is_bloom_filter_constructed_) {
        break;
      }
      // wait for timeout or until notified.
      cond_.wait_us(COND_WAIT_TIME_USEC);
      if (is_bloom_filter_constructed_) {
        break;
      }
    }
    ++loop_time;
    if (OB_FAIL(join_filter_op->try_check_status())) {
      LOG_WARN("failed to check status", K(loop_time));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!bf_msg->bloom_filter_.is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bloom filter initialized failed, please check the leader worker's error report");
  }
  return ret;
}

int SharedJoinFilterConstructor::notify_constructed()
{
  int ret = OB_SUCCESS;
  {
    ObThreadCondGuard guard(cond_);
    is_bloom_filter_constructed_ = true;
  }
  if (OB_FAIL(cond_.broadcast())) {
    LOG_WARN("failed to broadcast");
  }
  return ret;
}

int ObJoinFilterOpInput::init(ObTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  UNUSED(task_info);
  //new parallel framework do not use this interface to set parameters
  ret = OB_ERR_UNEXPECTED;
  LOG_WARN("the interface should not be used", K(ret));
  return ret;
}

bool ObJoinFilterOpInput::is_finish()
{
  bool ret = true;
  uint64_t *count_ptr = reinterpret_cast<uint64_t *>(share_info_.unfinished_count_ptr_);
  if (OB_NOT_NULL(count_ptr)) {
    if (0 != *count_ptr) {
      ret = false;
    }
  }
  return ret;
}

bool ObJoinFilterOpInput::check_release()
{
  bool res = false;
  uint64_t *release_ref_ptr = reinterpret_cast<uint64_t *>(share_info_.release_ref_ptr_);
  if (OB_ISNULL(release_ref_ptr)) {
    res = false;
  } else if (0 == ATOMIC_AAF(release_ref_ptr, -1)) {
    res = true;
  } else {
    res = false;
  }
  return res;
}


ERRSIM_POINT_DEF(JF_BS_OPT);
int ObJoinFilterOpInput::load_runtime_config(const ObJoinFilterSpec &spec, ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  if (0 == spec.bloom_filter_ratio_ && 0 == spec.send_bloom_filter_size_) {
    // bloom_filter_ratio_ and send_bloom_filter_size_ are default value, which indicates the
    // cluster is upgrading. for compatibility, use the value from GCONF
    config_.bloom_filter_ratio_ = ((double)GCONF._bloom_filter_ratio / 100.0);
    config_.bf_piece_size_ = GCONF._send_bloom_filter_size;
  } else {
    // bf_piece_size_ means how many int64_t a piece bloom filter contains
    // we expect to split bloom filter into k pieces with 1MB = 2^20B
    // so a piece bloom filter should contain
    // 1024(send_bloom_filter_size_) * 128 = 131,072 int64_t, i.e. 1MB
    config_.bloom_filter_ratio_ = ((double)spec.bloom_filter_ratio_ / 100.0);
    config_.bf_piece_size_ = spec.send_bloom_filter_size_ * 128;
  }
  config_.each_group_size_ = spec.each_group_size_;
  int64_t sess_wait_time_ms = ctx.get_my_session()->get_runtime_filter_wait_time_ms();
  if (sess_wait_time_ms != 0) {
    config_.runtime_filter_wait_time_ms_ = sess_wait_time_ms;
  } else {
    // use adaptive max wait time if session variable is 0
    int64_t sqc_count = ctx.get_sqc_handler()->get_sqc_init_arg().sqc_.get_sqc_count();
    config_.runtime_filter_wait_time_ms_ = spec.rf_max_wait_time_ms_ / sqc_count;
  }

  config_.runtime_filter_max_in_num_ = ctx.get_my_session()->
      get_runtime_filter_max_in_num();
  config_.runtime_bloom_filter_max_size_ = ctx.get_my_session()->
      get_runtime_bloom_filter_max_size();
  config_.px_message_compression_ = true;

  config_.build_send_opt_ =
      (spec.get_phy_plan()->get_min_cluster_version() >= CLUSTER_VERSION_4_3_5_0
       && spec.use_realistic_runtime_bloom_filter_size() && JF_BS_OPT == OB_SUCCESS);

  LOG_TRACE("load runtime filter config", K(spec.get_id()), K(config_));
  return ret;
}

int ObJoinFilterOpInput::init_share_info(
    const ObJoinFilterSpec &spec,
    ObExecContext &ctx,
    int64_t task_count,
    int64_t sqc_count)
{
  int ret = OB_SUCCESS;
  uint64_t *ptr = NULL;
  void *constructor_buf = nullptr;

  if (spec.is_shuffle_ && spec.jf_material_control_info_.each_sqc_has_full_data_
      && config_.build_send_opt_) {
    // for shuffled join filter, we gather K piece bloom filter create by all K sqcs.
    // opt: if is shared hash join, each sqc has full data of build table, we only
    // need to gather one piece join filter
    sqc_count = 1;
  }

  common::ObIAllocator &allocator = ctx.get_allocator();
  if (OB_ISNULL(ptr = (uint64_t *)allocator.alloc(sizeof(uint64_t) * 2))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ptr", K(ret));
  } else if (OB_ISNULL(constructor_buf = allocator.alloc(sizeof(SharedJoinFilterConstructor)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc bool ptr", K(ret));
  } else if (OB_FAIL(init_shared_msgs(spec, ctx, sqc_count))) {
    LOG_WARN("fail to init shared msgs", K(ret));
  } else {
    ptr[0] = task_count;
    ptr[1] = task_count;
    share_info_.release_ref_ptr_ = reinterpret_cast<uint64_t>(&ptr[0]);
    share_info_.unfinished_count_ptr_ = reinterpret_cast<uint64_t>(&ptr[1]);
    share_info_.shared_jf_constructor_ = new(constructor_buf) SharedJoinFilterConstructor;
    if (OB_FAIL(share_info_.shared_jf_constructor_->init())) {
      LOG_WARN("failed to init shared_jf_constructor_");
    }
  }
  return ret;
}

int ObJoinFilterOpInput::init_shared_msgs(
    const ObJoinFilterSpec &spec,
    ObExecContext &ctx,
    int64_t sqc_count)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = ctx.get_my_session()->get_effective_tenant_id();
  const int64_t timeout_ts = GET_PHY_PLAN_CTX(ctx)->get_timeout_timestamp();
  common::ObIAllocator &allocator = ctx.get_allocator();
  ObArray<ObP2PDatahubMsgBase *> *array_ptr = nullptr;
  ObPxSQCProxy *sqc_proxy = reinterpret_cast<ObPxSQCProxy *>(share_info_.ch_provider_ptr_);
  void *ptr = nullptr;
  if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObArray<ObP2PDatahubMsgBase *>)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    array_ptr = new(ptr) ObArray<ObP2PDatahubMsgBase *>();
    array_ptr->set_attr(ObMemAttr(tenant_id, "JFArray"));
    ObP2PDatahubMsgBase *msg_ptr = nullptr;
    for (int i = 0; i < spec.rf_infos_.count() && OB_SUCC(ret); ++i) {
      msg_ptr = nullptr;
      bool construct_bloom_filter_later = spec.use_realistic_runtime_bloom_filter_size()
          && spec.rf_infos_.at(i).dh_msg_type_ == ObP2PDatahubMsgBase::BLOOM_FILTER_VEC_MSG;
      if (OB_FAIL(PX_P2P_DH.alloc_msg(allocator, spec.rf_infos_.at(i).dh_msg_type_, msg_ptr))) {
        LOG_WARN("fail to alloc msg", K(ret));
      } else if (OB_FAIL(array_ptr->push_back(msg_ptr))) {
        // push_back failed, must destroy the msg immediately
        // if init or construct_msg_details failed, destroy msg after the for loop
        msg_ptr->destroy();
        allocator.free(msg_ptr);
        LOG_WARN("fail to push back array ptr", K(ret));
      } else if (OB_FAIL(msg_ptr->init(spec.rf_infos_.at(i).p2p_datahub_id_,
          px_sequence_id_, 0/*task_id*/, tenant_id, timeout_ts, register_dm_info_))) {
        LOG_WARN("fail to init msg", K(ret));
      } else if (!construct_bloom_filter_later
                 && OB_FAIL(construct_msg_details(spec, sqc_proxy, config_, *msg_ptr, sqc_count,
                                                  spec.filter_len_))) {
        LOG_WARN("fail to construct msg details", K(ret), K(tenant_id));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(array_ptr)) {
    share_info_.shared_msgs_ = reinterpret_cast<uint64_t>(array_ptr);
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(array_ptr)) {
    // if failed, destroy all msgs
    for (int64_t i = 0; i < array_ptr->count(); ++i) {
      if (OB_NOT_NULL(array_ptr->at(i))) {
        array_ptr->at(i)->destroy();
        allocator.free(array_ptr->at(i));
        array_ptr->at(i) = nullptr;
      }
    }
    array_ptr->reset();
    allocator.free(array_ptr);
    array_ptr = nullptr;
  }

  return ret;
}

int ObJoinFilterOpInput::construct_msg_details(
    const ObJoinFilterSpec &spec,
    ObPxSQCProxy *sqc_proxy,
    ObJoinFilterRuntimeConfig &config,
    ObP2PDatahubMsgBase &msg,
    int64_t sqc_count,
    int64_t estimated_rows)
{
  int ret = OB_SUCCESS;
  switch(msg.get_msg_type()) {
    case ObP2PDatahubMsgBase::BLOOM_FILTER_VEC_MSG: {
      ObRFBloomFilterMsg &bf_msg = static_cast<ObRFBloomFilterMsg &>(msg);
      bf_msg.set_use_rich_format(true);
      if (spec.use_realistic_runtime_bloom_filter_size()) {
        bf_msg.set_use_hash_join_seed(true);
      }
    }
    case ObP2PDatahubMsgBase::BLOOM_FILTER_MSG: {
      ObSArray<ObAddr> *target_addrs = nullptr;
      ObRFBloomFilterMsg &bf_msg = static_cast<ObRFBloomFilterMsg &>(msg);
      ObPxSQCProxy::SQCP2PDhMap &dh_map = sqc_proxy->get_p2p_dh_map();
      if (OB_FAIL(bf_msg.bloom_filter_.init(estimated_rows,
          bf_msg.get_allocator(),
          bf_msg.get_tenant_id(),
          config.bloom_filter_ratio_,
          config.runtime_bloom_filter_max_size_))) {
        LOG_WARN("failed to init bloom filter", K(ret));
      } else if (!spec.is_shared_join_filter() || !spec.is_shuffle_) {
        bf_msg.set_msg_expect_cnt(1);
        bf_msg.set_msg_cur_cnt(1);
      } else if (OB_FAIL(dh_map.get_refactored(bf_msg.get_p2p_datahub_id(), target_addrs))) {
        LOG_WARN("fail to get dh map", K(ret));
      } else if (target_addrs->count() == 1 &&
                 GCTX.self_addr() == target_addrs->at(0) &&
                 sqc_count == 1) {
        bf_msg.set_msg_expect_cnt(1);
        bf_msg.set_msg_cur_cnt(1);
      } else {
        int64_t target_cnt = target_addrs->count();
        int64_t each_group_size = (OB_INVALID_ID == config.each_group_size_) ?
          sqrt(target_cnt) : config.each_group_size_;
        if (OB_FAIL(bf_msg.generate_filter_indexes(each_group_size, target_cnt, config.bf_piece_size_))) {
          LOG_WARN("fail to generate filter indexes", K(ret));
        } else {
          bf_msg.filter_idx_ = 0;
          bf_msg.create_finish_ = false;
          int64_t piece_cnt = ceil(bf_msg.bloom_filter_.get_bits_array_length() /
                                   (double)config.bf_piece_size_);
          bf_msg.set_msg_expect_cnt(sqc_count * piece_cnt);
          bf_msg.set_msg_cur_cnt(1);
          bf_msg.expect_first_phase_count_ = sqc_count;
          bf_msg.piece_size_ = config.bf_piece_size_;
        }
      }
      break;
    }
    case ObP2PDatahubMsgBase::RANGE_FILTER_MSG: {
      ObRFRangeFilterMsg &range_msg = static_cast<ObRFRangeFilterMsg &>(msg);
      int64_t col_cnt = spec.join_keys_.count();
      if (OB_FAIL(range_msg.lower_bounds_.prepare_allocate(col_cnt))) {
        LOG_WARN("fail to prepare allocate col cnt", K(ret));
      } else if (OB_FAIL(range_msg.upper_bounds_.prepare_allocate(col_cnt))) {
        LOG_WARN("fail to prepare allocate col cnt", K(ret));
      } else if (OB_FAIL(range_msg.cells_size_.prepare_allocate(col_cnt))) {
        LOG_WARN("fail to prepare allocate col cnt", K(ret));
      } else if (OB_FAIL(range_msg.cmp_funcs_.assign(spec.cmp_funcs_))) {
        LOG_WARN("fail to init cmp funcs", K(ret));
      } else if (OB_FAIL(range_msg.need_null_cmp_flags_.assign(spec.need_null_cmp_flags_))) {
        LOG_WARN("fail to init cmp flags", K(ret));
      } else if (OB_FAIL(range_msg.build_obj_metas_.init(col_cnt))) {
        LOG_WARN("fail to prepare init build obj_metas", K(ret));
      } else {
        range_msg.set_msg_expect_cnt(sqc_count);
        range_msg.set_msg_cur_cnt(1);
        if (spec.px_query_range_info_.can_extract()) {
          if (OB_FAIL(range_msg.init_query_range_info(spec.px_query_range_info_))) {
            LOG_WARN("failed to init_query_range_info");
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
        if (OB_FAIL(range_msg.build_obj_metas_.push_back(spec.join_keys_.at(i)->obj_meta_))) {
          LOG_WARN("fail to push_back build obj_metas");
        }
      }
      break;
    }
    case ObP2PDatahubMsgBase::RANGE_FILTER_VEC_MSG: {
      ObRFRangeFilterVecMsg &range_msg = static_cast<ObRFRangeFilterVecMsg &>(msg);
      int64_t col_cnt = spec.join_keys_.count();
      if (OB_FAIL(range_msg.lower_bounds_.prepare_allocate(col_cnt))) {
        LOG_WARN("fail to prepare allocate col cnt", K(ret));
      } else if (OB_FAIL(range_msg.upper_bounds_.prepare_allocate(col_cnt))) {
        LOG_WARN("fail to prepare allocate col cnt", K(ret));
      } else if (OB_FAIL(range_msg.cells_size_.prepare_allocate(col_cnt))) {
        LOG_WARN("fail to prepare allocate col cnt", K(ret));
      } else if (OB_FAIL(range_msg.need_null_cmp_flags_.assign(spec.need_null_cmp_flags_))) {
        LOG_WARN("fail to init cmp flags", K(ret));
      } else if (OB_FAIL(range_msg.build_row_cmp_info_.assign(spec.rf_build_cmp_infos_))) {
        LOG_WARN("fail to prepare allocate build_row_cmp_info_", K(ret));
      } else if (OB_FAIL(range_msg.probe_row_cmp_info_.assign(spec.rf_probe_cmp_infos_))) {
        LOG_WARN("fail to prepare allocate probe_row_cmp_info_", K(ret));
      } else {
        range_msg.set_msg_expect_cnt(sqc_count);
        range_msg.set_msg_cur_cnt(1);
        if (spec.px_query_range_info_.can_extract()) {
          if (OB_FAIL(range_msg.init_query_range_info(spec.px_query_range_info_))) {
            LOG_WARN("failed to init_query_range_info");
          }
        }
      }
      break;
    }
    case ObP2PDatahubMsgBase::IN_FILTER_MSG: {
      ObRFInFilterMsg &in_msg = static_cast<ObRFInFilterMsg &>(msg);
      int64_t col_cnt = spec.join_keys_.count();
      if (OB_FAIL(in_msg.rows_set_.create(config.runtime_filter_max_in_num_ * 2,
          "RFInFilter",
          "RFInFilter"))) {
        LOG_WARN("fail to init in hash set", K(ret));
      } else if (OB_FAIL(in_msg.cur_row_.prepare_allocate(col_cnt))) {
        LOG_WARN("fail to prepare allocate col cnt", K(ret));
      } else if (OB_FAIL(in_msg.cmp_funcs_.assign(spec.cmp_funcs_))) {
        LOG_WARN("fail to init cmp funcs", K(ret));
      } else if (OB_FAIL(in_msg.hash_funcs_for_insert_.assign(spec.hash_funcs_))) {
        LOG_WARN("fail to init cmp funcs", K(ret));
      } else if (OB_FAIL(in_msg.need_null_cmp_flags_.assign(spec.need_null_cmp_flags_))) {
        LOG_WARN("fail to init cmp flags", K(ret));
      } else if (OB_FAIL(in_msg.build_obj_metas_.init(col_cnt))) {
        LOG_WARN("fail to prepare init build obj_metas_", K(ret));
      } else {
        in_msg.set_msg_expect_cnt(sqc_count);
        in_msg.set_msg_cur_cnt(1);
        in_msg.col_cnt_ = col_cnt;
        in_msg.max_in_num_ = config.runtime_filter_max_in_num_;
        if (spec.px_query_range_info_.can_extract()) {
          if (OB_FAIL(in_msg.init_query_range_info(spec.px_query_range_info_))) {
            LOG_WARN("failed to init_query_range_info");
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
        if (OB_FAIL(in_msg.build_obj_metas_.push_back(spec.join_keys_.at(i)->obj_meta_))) {
          LOG_WARN("fail to push_back build obj_metas_");
        }
      }
      break;
    }
    case ObP2PDatahubMsgBase::IN_FILTER_VEC_MSG: {
      ObRFInFilterVecMsg &in_msg = static_cast<ObRFInFilterVecMsg &>(msg);
      if (spec.use_realistic_runtime_bloom_filter_size()) {
        in_msg.set_use_hash_join_seed(true);
      }
      int64_t col_cnt = spec.join_keys_.count();
      if (OB_FAIL(in_msg.rows_set_.create(config.runtime_filter_max_in_num_ * 2,
          "RFInFilter",
          "RFInFilter"))) {
        LOG_WARN("fail to init in hash set", K(ret));
      } else if (OB_FAIL(in_msg.sm_hash_set_.init(config.runtime_filter_max_in_num_,
                                                  in_msg.get_tenant_id()))) {
        LOG_WARN("failed to init sm_hash_set_", K(config.runtime_filter_max_in_num_));
      } else if (OB_FAIL(in_msg.need_null_cmp_flags_.assign(spec.need_null_cmp_flags_))) {
        LOG_WARN("fail to init cmp flags", K(ret));
      } else if (OB_FAIL(in_msg.build_row_cmp_info_.assign(spec.rf_build_cmp_infos_))) {
        LOG_WARN("fail to prepare allocate build_row_cmp_info_", K(ret));
      } else if (OB_FAIL(in_msg.probe_row_cmp_info_.assign(spec.rf_probe_cmp_infos_))) {
        LOG_WARN("fail to prepare allocate probe_row_cmp_info_", K(ret));
      } else if (OB_FAIL(in_msg.build_row_meta_.init(spec.join_keys_, sizeof(uint64_t)))) {
        LOG_WARN("failed to init row meta");
      } else if (OB_FAIL(in_msg.cur_row_with_hash_.row_.prepare_allocate(col_cnt))) {
        LOG_WARN("failed to prepare_allocate cur_row_with_hash_");
      } else {
        in_msg.build_send_opt_ = config.build_send_opt_;
        in_msg.set_msg_expect_cnt(sqc_count);
        in_msg.set_msg_cur_cnt(1);
        in_msg.max_in_num_ = config.runtime_filter_max_in_num_;
        if (spec.px_query_range_info_.can_extract()) {
          if (OB_FAIL(in_msg.init_query_range_info(spec.px_query_range_info_))) {
            LOG_WARN("failed to init_query_range_info");
          } else if (OB_FAIL(in_msg.hash_funcs_for_insert_.assign(spec.hash_funcs_))) {
            LOG_WARN("fail to init cmp funcs", K(ret));
          }
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected msg type", K(ret));
    }
  }
  return ret;
}

ObJoinFilterSpec::ObJoinFilterSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObOpSpec(alloc, type),
    mode_(NOT_INIT),
    filter_id_(OB_INVALID_ID),
    filter_len_(0),
    join_keys_(alloc),
    hash_funcs_(alloc),
    cmp_funcs_(alloc),
    filter_shared_type_(JoinFilterSharedType::INVALID_TYPE),
    calc_tablet_id_expr_(NULL),
    rf_infos_(alloc),
    need_null_cmp_flags_(alloc),
    is_shuffle_(false),
    each_group_size_(OB_INVALID_ID),
    rf_build_cmp_infos_(alloc),
    rf_probe_cmp_infos_(alloc),
    px_query_range_info_(alloc),
    bloom_filter_ratio_(0),
    send_bloom_filter_size_(0),
    full_hash_join_keys_(alloc),
    hash_join_is_ns_equal_cond_(alloc)
{
}

int ObJoinFilterSpec::register_to_datahub(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  // if this is a material controller, and at least one join filter is shared join filter, we need
  // to send datahub msg to synchronize row count
  if (!need_sync_row_count()) {
  } else if (OB_ISNULL(ctx.get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ret));
  } else {
    void *buf = ctx.get_allocator().alloc(sizeof(ObJoinFilterCountRowWholeMsg::WholeMsgProvider));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ObJoinFilterCountRowWholeMsg::WholeMsgProvider *provider =
          new (buf) ObJoinFilterCountRowWholeMsg::WholeMsgProvider();
      ObSqcCtx &sqc_ctx = ctx.get_sqc_handler()->get_sqc_ctx();
      if (OB_FAIL(sqc_ctx.add_whole_msg_provider(get_id(), DH_JOIN_FILTER_COUNT_ROW_WHOLE_MSG,
                                                 *provider))) {
        LOG_WARN("fail add whole msg provider", K(ret));
      }
    }
  }
  return ret;
}

/*
 For upgrade compatibility,
 if cluster version >= 435, we can directly use flag need_sync_row_count_,
 if it is during upgrade and cluster version < 435, the flag need_sync_row_count_ will be false,
 we should visit child to check whether need to synchronize row count
*/
int ObJoinFilterSpec::update_sync_row_count_flag()
{
  int ret = OB_SUCCESS;
  const ObOpSpec *cur_spec = this;
  int64_t join_filter_count = under_control_join_filter_count();
  // if at least one join filter is shared join filter, we need to send datahub msg to synchronize
  // row count
  for (int64_t i = 0; i < join_filter_count && OB_NOT_NULL(cur_spec) && OB_SUCC(ret); ++i) {
    if (cur_spec->get_type() != PHY_JOIN_FILTER) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("must be join filter bellow");
    } else {
      const ObJoinFilterSpec &spec = static_cast<const ObJoinFilterSpec &>(*cur_spec);
      if (spec.is_shared_join_filter()) {
        jf_material_control_info_.need_sync_row_count_ = true;
        break;
      }
      cur_spec = cur_spec->get_child();
    }
  }
  return ret;
}

//------------------------------------------ ObJoinFilterOp --------------------------------
int ObJoinFilterOp::destroy_filter()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObJoinFilterOp::link_ch_sets(ObPxBloomFilterChSets &ch_sets,
                               common::ObIArray<dtl::ObDtlChannel *> &channels)
{
  int ret = OB_SUCCESS;
  int64_t thread_id = GETTID();
  dtl::ObDtlChannelInfo ci;
  for (int i = 0; i < ch_sets.count() && OB_SUCC(ret); ++i) {
    for (int j = 0; j < ch_sets.at(i).count() && OB_SUCC(ret); ++j) {
      dtl::ObDtlChannel *ch = NULL;
      ObPxBloomFilterChSet &ch_set = ch_sets.at(i);
      if (OB_FAIL(ch_set.get_channel_info(j, ci))) {
        LOG_WARN("fail get channel info", K(j), K(ret));
      } else if (OB_FAIL(dtl::ObDtlChannelGroup::link_channel(ci, ch, NULL))) {
        LOG_WARN("fail link channel", K(ci), K(ret));
      } else if (OB_ISNULL(ch)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ch channel is null", K(ret));
      } else if (OB_FAIL(channels.push_back(ch))) {
        LOG_WARN("fail push back channel ptr", K(ci), K(ret));
      } else {
        ch->set_join_filter_owner();
        ch->set_thread_id(thread_id);
      }
    }
  }
  return ret;
}

bool ObJoinFilterOp::is_valid()
{
  return child_ != NULL && MY_SPEC.mode_ != JoinFilterMode::NOT_INIT &&
         MY_SPEC.filter_shared_type_ != JoinFilterSharedType::INVALID_TYPE;
}

ObJoinFilterOp::ObJoinFilterOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input), filter_create_msg_(nullptr), join_filter_hash_values_(NULL),
      lucky_devil_champions_(), profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
      sql_mem_processor_(profile_, op_monitor_info_)
{
}

ObJoinFilterOp::~ObJoinFilterOp()
{
  destroy();
}

int ObJoinFilterOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("builder operator is invalid", K(ret), K(MY_SPEC.filter_shared_type_), K(MY_SPEC.mode_));
  } else if (MY_SPEC.is_create_mode() && OB_FAIL(open_join_filter_create())) {
    LOG_WARN("fail to open join filter create op", K(ret));
  } else if ((MY_SPEC.is_use_mode() && OB_FAIL(open_join_filter_use()))) {
    LOG_WARN("fail to open join filter use op", K(ret));
  }
  return ret;
}

int ObJoinFilterOp::do_create_filter_rescan()
{
  int ret = OB_SUCCESS;
  ObRFBloomFilterMsg *bf_msg = nullptr;
  for (int i = 0; i < local_rf_msgs_.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(local_rf_msgs_.at(i)->reuse())) {
      LOG_WARN("fail to reuse local rf msgs", K(ret));
    }
  }
  if (MY_SPEC.use_realistic_runtime_bloom_filter_size() && OB_NOT_NULL(bf_vec_msg_)) {
    // in new fashion, the bloom filter will be reinited
    bf_vec_msg_->bloom_filter_.reset_for_rescan();
  }
  if (MY_SPEC.is_material_controller()) {
    partition_splitter_->reuse_for_rescan();
    if (MY_SPEC.is_shared_join_filter()) {
      static_cast<ObJoinFilterOpInput *>(input_)
          ->share_info_.shared_jf_constructor_->reset_for_rescan();
    }
  }
  return ret;
}

int ObJoinFilterOp::do_use_filter_rescan()
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < MY_SPEC.rf_infos_.count() && OB_SUCC(ret); ++i) {
    if (OB_INVALID_ID != MY_SPEC.rf_infos_.at(i).filter_expr_id_) {
      ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx = nullptr;
      if (OB_NOT_NULL(join_filter_ctx = static_cast<ObExprJoinFilter::ObExprJoinFilterContext *>(
                          ctx_.get_expr_op_ctx(MY_SPEC.rf_infos_.at(i).filter_expr_id_)))) {
        join_filter_ctx->rescan();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join filter ctx is unexpected", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinFilterOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.is_create_mode() && OB_FAIL(do_create_filter_rescan())) {
    LOG_WARN("fail to do create filter rescan", K(ret));
  } else if (MY_SPEC.is_use_mode() && OB_FAIL(do_use_filter_rescan())) {
    LOG_WARN("fail to do use filter rescan", K(ret));
  } else if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("operator rescan failed", K(ret));
  }
  return ret;
}

// see issue:
int ObJoinFilterOp::mark_not_need_send_bf_msg()
{
  int ret = OB_SUCCESS;
  ObJoinFilterOpInput *filter_input = static_cast<ObJoinFilterOpInput*>(input_);
  ObPxSQCProxy *sqc_proxy = reinterpret_cast<ObPxSQCProxy *>(
      filter_input->share_info_.ch_provider_ptr_);
  if (MY_SPEC.is_shared_join_filter() && MY_SPEC.is_shuffle_) {
    for (int i = 0; i < local_rf_msgs_.count() && OB_SUCC(ret); ++i) {
      if (local_rf_msgs_.at(i)->get_msg_type() == ObP2PDatahubMsgBase::BLOOM_FILTER_MSG
          || local_rf_msgs_.at(i)->get_msg_type() == ObP2PDatahubMsgBase::BLOOM_FILTER_VEC_MSG) {
        ObRFBloomFilterMsg *shared_bf_msg = static_cast<ObRFBloomFilterMsg *>(shared_rf_msgs_.at(i));
        bool is_local_dh = false;
        if (OB_FAIL(sqc_proxy->check_is_local_dh(local_rf_msgs_.at(i)->get_p2p_datahub_id(),
            is_local_dh,
            local_rf_msgs_.at(i)->get_msg_receive_expect_cnt()))) {
          LOG_WARN("fail to check local dh", K(ret));
        } else if (is_local_dh) {
        } else {
          // only the msg is a shared and shuffled BLOOM_FILTER_MSG
          // let other worker threads stop trying send join_filter
          if (!shared_bf_msg->create_finish_) {
            shared_bf_msg->need_send_msg_ = false;
          }
        }
      }
    }
  }
  return ret;
}

// for create mode, need add mark_not_need_send_bf_msg for shared shuffled bloom filter
// for use mode, update_plan_monitor_info cause get_next_batch may not get iter end
int ObJoinFilterOp::inner_drain_exch()
{
  int ret = OB_SUCCESS;
  if (row_reach_end_ || batch_reach_end_) {
  // already iter end, not need to mark not send or update_plan_monitor_info again (already done in get_next_row/batch)
  } else if (MY_SPEC.is_create_mode()) {
    ret = mark_not_need_send_bf_msg();
  } else if (MY_SPEC.is_use_mode()) {
    ret = update_plan_monitor_info();
  }
  return ret;
}

int ObJoinFilterOp::do_drain_exch()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::do_drain_exch())) {
    LOG_WARN("failed to basic do_drain_exch");
  } else if (MY_SPEC.is_material_controller()) {
    // if one worker is drained without send_datahub_count_row_msg, other workers who has
    // send_datahub_count_row_msg will never break wait, so must send one piece

    /* we must do send_datahub_count_row_msg after child drained, or we may trap into endless loop
                 Good Way                                               Bad Way
                 HashJoin                                               HashJoin
                    |                                                      |
                |----                                                  |----
          JoinFilter 3. after drain, send datahub row count msg    JoinFilter  1. drain exch, send datahub row count msg, trap into endless loop
              |                                                        |
           Receive   1. drain exch, send channel ready msg          Receive    2. wait for parent drain, can't send channel ready msg
              |                                                        |
          Transmit   2. wait channel succ, send rows                Transmit   3. wait channel failed
    */
    int64_t worker_row_count = partition_splitter_->get_total_row_count();
    int64_t total_row_count = 0;
    if (OB_FAIL(get_exec_row_count_and_ndv(worker_row_count, total_row_count, true/*is_in_drain*/))) {
      LOG_WARN("failed to get exec row count");
    }
  }
  return ret;
}

int ObJoinFilterOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObJoinFilterOpInput *filter_input = static_cast<ObJoinFilterOpInput*>(input_);
  while (OB_SUCC(ret)) {
    clear_evaluated_flag();
    ret = child_->get_next_row();
    if (OB_ITER_END == ret) {
      if (MY_SPEC.is_create_mode()) {
        ret = OB_SUCCESS;
        if (OB_FAIL(try_merge_join_filter())) {
          LOG_WARN("fail to merge join filter", K(ret));
        } else if (OB_FAIL(try_send_join_filter())) {
          LOG_WARN("fail to send bloom filter to use filter", K(ret));
        }
        if (OB_SUCC(ret)) {
          ret = OB_ITER_END;
        }
      }
    } else if (OB_SUCCESS != ret) {
      LOG_WARN("fail to get next row", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (MY_SPEC.is_create_mode()) {
        if (OB_FAIL(insert_by_row())) {
          LOG_WARN("fail to insert bf", K(ret));
        } else {
          break;
        }
      } else if (MY_SPEC.is_use_mode()) {
        /*交给表达式计算, 算子中无需处理相关计算逻辑*/
        break;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the mode of px bloom filter is unexpected", K(MY_SPEC.mode_), K(ret));
      }
    }
  }
  if (MY_SPEC.is_use_mode() && ret == OB_ITER_END) {
    if (OB_FAIL(update_plan_monitor_info())) {
      LOG_WARN("fail to update plan monitor info", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObJoinFilterOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
  if (MY_SPEC.is_create_mode()) {
    if (MY_SPEC.use_realistic_runtime_bloom_filter_size()) {
      // in this way, join filter will return brs_.end_ && 0 == brs_.size_ to parent operator
      if (MY_SPEC.is_material_controller()) {
        // the controller is responsible for do material
        ret = join_filter_create_do_material(batch_cnt);
      } else {
        // the below join filter only bypass all
        ret = join_filter_create_bypass_all(batch_cnt);
      }
    } else {
      ret = join_filter_create_get_next_batch(batch_cnt);
    }
  } else if (MY_SPEC.is_use_mode()) {
    ret = join_filter_use_get_next_batch(batch_cnt);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the mode of join filter is unexpected", K(MY_SPEC.mode_), K(ret));
  }
  return ret;
}

// for these function, the hash join operator guarantees it will never call get_next_batch
// if join filter create operator iterators end unless in rescan scene
int ObJoinFilterOp::join_filter_create_do_material(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const ObBatchRows *child_brs = nullptr;
  while (OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
      // get_next_batch will not return OB_ITER_END
      LOG_WARN("failed to get next batch for create op", K(ret));
    } else if (FALSE_IT(brs_.copy(child_brs))) {
    } else if (brs_.size_ > 0) {
      if (!got_first_row_) {
        op_monitor_info_.first_row_time_ = ObClockGenerator::getClock();
        got_first_row_ = true;
      }
      /*
        For each batch:
        1. calc partition hash value(for hash join)
        2. group fill range filter(for all join filter, range filter can directly filled without
        material)
        3. group calc join filter hash value(for all join filter, if a join filter reuse hash join's
        hash value, skip it)
        4. group fill in filter(for all join filter)
        5. add batch into store
        6. process dump
      */
      for (int64_t i = 0; i < build_rows_output_->count() && OB_SUCC(ret); ++i) {
        // if join filter's output is not same with child, it is necessary to eval output exprs
        if (OB_FAIL(build_rows_output_->at(i)->eval_vector(eval_ctx_, brs_))) {
          LOG_WARN("failed to eval vector");
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(partition_splitter_->calc_partition_hash_value(
                   &brs_, eval_ctx_, hash_join_hash_values_,
                   use_hllc_estimate_ndv(),
                   group_controller_->hash_join_keys_hllc_, skip_left_null_,
                   &MY_SPEC.hash_join_is_ns_equal_cond_))) {
        LOG_WARN("failed to calc_partition_hash_value");
      } else if (OB_FAIL(group_controller_->apply(group_fill_range_filter, brs_))) {
        LOG_WARN("failed to group fill range filter");
      } else if (OB_FAIL(group_controller_->apply(group_calc_join_filter_hash_values, brs_))) {
        LOG_WARN("failed to group calculate join filter hash values");
      } else if (OB_FAIL(
                   group_controller_->apply(group_fill_in_filter, brs_, hash_join_hash_values_))) {
      } else if (OB_FAIL(partition_splitter_->add_batch(*group_controller_, hash_join_hash_values_,
                                                        brs_,
                                                        ObHashJoinVecOp::MAX_PART_LEVEL << 3))) {
      } else if (OB_FAIL(process_dump())) {
        LOG_WARN("failed to process dump");
      }
    } else if (brs_.end_ && 0 == brs_.size_) {
      if (force_dump_ && OB_FAIL(partition_splitter_->force_dump_all_partition())) {
        LOG_WARN("failed to force dump all partition");
      } else if (OB_FAIL(partition_splitter_->finish_add_row())) {
        LOG_WARN("failed to finish add row");
      }
      break;
    }
  }
  if (OB_SUCC(ret) && brs_.end_ && 0 == brs_.size_) {
    // send piece datahub msg, and wait the whole msg, then the total row is get
    int64_t worker_row_count = partition_splitter_->get_total_row_count();
    int64_t total_row_count = 0;
    if (OB_FAIL(get_exec_row_count_and_ndv(worker_row_count, total_row_count, false /*is_in_drain*/))) {
      LOG_WARN("failed to get exec row count");
    } else if (OB_FAIL(group_controller_->apply(group_init_bloom_filter, worker_row_count,
                                                total_row_count))) {
      LOG_WARN("failed to group_init_bloom_filter");
    } else if (OB_FAIL(fill_bloom_filter())) { // group fill bloom filter inner
      LOG_WARN("failed to fill bloom filter");
    } else if (OB_FAIL(group_controller_->apply(group_merge_and_send_join_filter))) {
      LOG_WARN("failed to group control merge_and_send_join_filter");
    } else {
      // for the controller, it need to add row info manually
      op_monitor_info_.output_row_count_ += partition_splitter_->get_total_row_count();
    }
  }
  // should return B_SUCCESS, brs_.end_ && 0 == brs_.size_ to parent operator
  return ret;
}

int ObJoinFilterOp::join_filter_create_bypass_all(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const ObBatchRows *child_brs = nullptr;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
    // get_next_batch will not return OB_ITER_END
    LOG_WARN("failed to get next batch for use op", K(ret));
  } else if (FALSE_IT(brs_.copy(child_brs))) {
  }
  return ret;
}

int ObJoinFilterOp::join_filter_create_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const ObBatchRows *child_brs = nullptr;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
    // get_next_batch will not return OB_ITER_END
    LOG_WARN("failed to get next batch for use op", K(ret));
  } else if (FALSE_IT(brs_.copy(child_brs))) {
  } else if (brs_.size_ > 0) {
    if (OB_FAIL(insert_by_row_batch(child_brs))) {
      LOG_WARN("fail to insert join filter", K(ret));
    }
  }
  if (OB_SUCC(ret) && brs_.end_ && 0 == brs_.size_) {
    if (OB_FAIL(try_merge_join_filter())) {
      LOG_WARN("fail to merge join filter", K(ret));
    } else if (OB_FAIL(try_send_join_filter())) {
      LOG_WARN("fail to send bloom filter to use filter", K(ret));
    }
  }
  return ret;
}

int ObJoinFilterOp::join_filter_use_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const ObBatchRows *child_brs = nullptr;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
    // get_next_batch will not return OB_ITER_END
    LOG_WARN("failed to get next batch for use op", K(ret));
  } else if (FALSE_IT(brs_.copy(child_brs))) {
  } else if (brs_.end_ && 0 == brs_.size_) {
    if (OB_FAIL(update_plan_monitor_info())) {
      LOG_WARN("fail to update plan monitor info", K(ret));
    }
  }
  return ret;
}

int ObJoinFilterOp::insert_by_row()
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < local_rf_msgs_.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(local_rf_msgs_.at(i)->insert_by_row(MY_SPEC.join_keys_,
        MY_SPEC.hash_funcs_, MY_SPEC.calc_tablet_id_expr_, eval_ctx_))) {
      LOG_WARN("fail to insert rf by row", K(ret));
    }
  }
  return ret;
}

int ObJoinFilterOp::insert_by_row_batch(const ObBatchRows *child_brs)
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.use_rich_format_) {
    for (int i = 0; i < local_rf_msgs_.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(local_rf_msgs_.at(i)->insert_by_row_batch(child_brs,
          MY_SPEC.join_keys_, MY_SPEC.hash_funcs_,
          MY_SPEC.calc_tablet_id_expr_, eval_ctx_,
          join_filter_hash_values_))) {
        LOG_WARN("fail to insert rf by row batch", K(ret));
      }
    }
  } else {
    for (int i = 0; i < local_rf_msgs_.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(local_rf_msgs_.at(i)->insert_by_row_vector(child_brs,
          MY_SPEC.join_keys_, MY_SPEC.hash_funcs_,
          MY_SPEC.calc_tablet_id_expr_, eval_ctx_,
          join_filter_hash_values_))) {
        LOG_WARN("fail to insert rf by row vector", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinFilterOp::inner_close()
{
  int ret = OB_SUCCESS;
  ObJoinFilterOpInput *filter_input = static_cast<ObJoinFilterOpInput*>(input_);
  if (MY_SPEC.is_create_mode() && OB_FAIL(close_join_filter_create())) {
    LOG_WARN("fail to open join filter create op", K(ret));
  } else if ((MY_SPEC.is_use_mode() && OB_FAIL(close_join_filter_use()))) {
    LOG_WARN("fail to open join filter use op", K(ret));
  }
  return ret;
}

int ObJoinFilterOp::try_merge_join_filter()
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  int ecode = EventTable::EN_PX_JOIN_FILTER_NOT_MERGE_MSG;
  if (OB_SUCCESS != ecode && OB_SUCC(ret)) {
    LOG_WARN("ERRSIM match, don't merge_join_filter by design", K(ret));
    return OB_SUCCESS;
  }
#endif

  ObJoinFilterOpInput *filter_input = static_cast<ObJoinFilterOpInput*>(input_);
  uint64_t *count_ptr = reinterpret_cast<uint64_t *>(
      filter_input->share_info_.unfinished_count_ptr_);
  ObPxSQCProxy *sqc_proxy = reinterpret_cast<ObPxSQCProxy *>(
      filter_input->share_info_.ch_provider_ptr_);
  int64_t cur_cnt = 0;

  for (int i = 0; i < local_rf_msgs_.count() && OB_SUCC(ret); ++i) {
    if (!MY_SPEC.is_shared_join_filter()) {
      lucky_devil_champions_.at(i) = true;
    } else if (local_rf_msgs_.at(i)->get_msg_type() == ObP2PDatahubMsgBase::BLOOM_FILTER_MSG
        || local_rf_msgs_.at(i)->get_msg_type() == ObP2PDatahubMsgBase::BLOOM_FILTER_VEC_MSG) {
    } else if (OB_FAIL(shared_rf_msgs_.at(i)->merge(*local_rf_msgs_.at(i)))) {
      LOG_WARN("fail to do rf merge", K(ret));
    }
  }
  if (OB_SUCC(ret) && MY_SPEC.is_shared_join_filter()) {
    cur_cnt = ATOMIC_AAF(count_ptr, -1);
    for (int i = 0; i < local_rf_msgs_.count() && OB_SUCC(ret); ++i) {
      if (local_rf_msgs_.at(i)->get_msg_type() == ObP2PDatahubMsgBase::BLOOM_FILTER_MSG
          || local_rf_msgs_.at(i)->get_msg_type() == ObP2PDatahubMsgBase::BLOOM_FILTER_VEC_MSG) {
        if (MY_SPEC.is_shuffle_) {
          bool is_local_dh = false;
          if (OB_FAIL(sqc_proxy->check_is_local_dh(local_rf_msgs_.at(i)->get_p2p_datahub_id(),
              is_local_dh,
              local_rf_msgs_.at(i)->get_msg_receive_expect_cnt()))) {
            LOG_WARN("fail to check local dh", K(ret));
          } else if (is_local_dh) {
            lucky_devil_champions_.at(i) = (0 == cur_cnt);
          } else {
            lucky_devil_champions_.at(i) = true;
          }
        } else {
          lucky_devil_champions_.at(i) = (0 == cur_cnt);
        }
        if (OB_SUCC(ret)) {
          if (0 == cur_cnt) {
            static_cast<ObRFBloomFilterMsg *>(shared_rf_msgs_.at(i))->create_finish_ = true;
          }
        }
      } else if (0 == cur_cnt) {
        lucky_devil_champions_.at(i) = true;
      }
    }
  }
  return ret;
}

int ObJoinFilterOp::try_send_join_filter()
{
  int ret = OB_SUCCESS;
  ObJoinFilterOpInput *filter_input = static_cast<ObJoinFilterOpInput*>(input_);
  ObPxSQCProxy *sqc_proxy = reinterpret_cast<ObPxSQCProxy *>(
      filter_input->share_info_.ch_provider_ptr_);
  CK(OB_NOT_NULL(sqc_proxy));

  for (int i = 0; i < local_rf_msgs_.count() && OB_SUCC(ret); ++i) {
    if (!lucky_devil_champions_.at(i)) {
    } else if (!MY_SPEC.is_shared_join_filter()) {
      if (OB_FAIL(PX_P2P_DH.send_local_p2p_msg(*local_rf_msgs_.at(i)))) {
        LOG_WARN("fail to send local p2p msg", K(ret));
      }
    } else if (!MY_SPEC.is_shuffle_) {
      if (OB_FAIL(PX_P2P_DH.send_local_p2p_msg(*shared_rf_msgs_.at(i)))) {
        LOG_WARN("fail to send local p2p msg", K(ret));
      }
    } else if (MY_SPEC.is_shared_join_filter()) {
      bool need_send_shuffle_msg = true;
      if (build_send_opt()) {
        int64_t sqc_id = ctx_.get_sqc_handler()->get_sqc_proxy().get_sqc_id();
        // if each sqc has full data of build table, only the NO.0 sqc needs to send
        // shuffle runtime filter message
        need_send_shuffle_msg =
            (MY_SPEC.jf_material_control_info_.each_sqc_has_full_data_ && sqc_id == 0)
            || !MY_SPEC.jf_material_control_info_.each_sqc_has_full_data_;
      }

      if (need_send_shuffle_msg
          && OB_FAIL(PX_P2P_DH.send_p2p_msg(*shared_rf_msgs_.at(i), *sqc_proxy))) {
        LOG_WARN("fail to send p2p msg", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinFilterOp::prepre_bloom_filter_ctx(ObBloomFilterSendCtx *bf_ctx)
{
  int ret = OB_SUCCESS;
  int64_t &each_group_size = bf_ctx->get_each_group_size();
  if (OB_FAIL(calc_each_bf_group_size(each_group_size))) {
    LOG_WARN("fail to calc each bf group size", K(ret));
  } else {
    ObRFBloomFilterMsg *bf_msg = filter_create_msg_->bf_msg_;
    ObPxBloomFilter &bf = bf_msg->bloom_filter_;
    int64_t sqc_count = 0;
    int64_t peer_sqc_count = 0;
    int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    // 125 = 1000/8, send size means how many byte of partial bloom filter will be send at once
    int64_t send_size = GCONF._send_bloom_filter_size * 125;
    // how many piece of partial bloom filter will be send by all threads(sqc level)
    int64_t send_count = ceil(bf.get_bits_array_length() / (double)send_size);
    int64_t bloom_filter_count = send_count * send_size;
    ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid() && true == tenant_config->_px_message_compression) {
      bf_ctx->set_bf_compress_type(ObCompressorType::LZ4_COMPRESSOR);
    }
    if (OB_FAIL(bf_ctx->generate_filter_indexes(each_group_size, peer_sqc_count))) {
      LOG_WARN("failed to generate filter indexs", K(ret));
    } else {
      bf_ctx->set_per_addr_bf_count(send_count);
      bf_ctx->set_bloom_filter_ready(true);
    }
  }
  return ret;
}

int ObJoinFilterOp::calc_each_bf_group_size(int64_t &each_group_size)
{
  int ret = OB_SUCCESS;
  if (0 == each_group_size) { // only need calc once
    int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    int64_t peer_target_cnt = 0;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_LIKELY(tenant_config.is_valid())) {
      const char *ptr = NULL;
      if (OB_ISNULL(ptr = tenant_config->_px_bloom_filter_group_size.get_value())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("each group size ptr is null", K(ret));
      } else if (0 == ObString::make_string("auto").case_compare(ptr)) {
        each_group_size = sqrt(peer_target_cnt); // auto calc group size
      } else {
        char *end_ptr = nullptr;
        each_group_size = strtoull(ptr, &end_ptr, 10); // get group size from tenant config
        if (*end_ptr != '\0') {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("each group size ptr is unexpected", K(ret));
        }
      }
    }
    each_group_size = (each_group_size <= 0 ? 1 : each_group_size);
  }
  return ret;
}

int ObJoinFilterOp::update_plan_monitor_info()
{
  int ret = OB_SUCCESS;
  op_monitor_info_.otherstat_1_value_ = 0;
  op_monitor_info_.otherstat_2_value_ = 0;
  op_monitor_info_.otherstat_3_value_ = 0;
  op_monitor_info_.otherstat_4_value_ = 0;
  op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::JOIN_FILTER_FILTERED_COUNT;
  op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::JOIN_FILTER_TOTAL_COUNT;
  op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::JOIN_FILTER_CHECK_COUNT;
  op_monitor_info_.otherstat_4_id_ = ObSqlMonitorStatIds::JOIN_FILTER_READY_TIMESTAMP;
  op_monitor_info_.otherstat_5_id_ = ObSqlMonitorStatIds::JOIN_FILTER_BY_BASS_COUNT_BEFORE_READY;
  op_monitor_info_.otherstat_5_value_ = 0;
  op_monitor_info_.otherstat_6_id_ = ObSqlMonitorStatIds::JOIN_FILTER_LENGTH;
  op_monitor_info_.otherstat_6_value_ = MY_SPEC.filter_len_;
  int64_t check_count = 0;
  int64_t total_count = 0;
  for (int i = 0; i < MY_SPEC.rf_infos_.count() && OB_SUCC(ret); ++i) {
    if (OB_INVALID_ID != MY_SPEC.rf_infos_.at(i).filter_expr_id_) {
      ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx = NULL;
      if (OB_ISNULL(join_filter_ctx = static_cast<ObExprJoinFilter::ObExprJoinFilterContext *>(
          ctx_.get_expr_op_ctx(MY_SPEC.rf_infos_.at(i).filter_expr_id_)))) {
        LOG_TRACE("join filter expr ctx is null");
      } else {
        op_monitor_info_.otherstat_1_value_ += join_filter_ctx->filter_count_;
        total_count = max(total_count, join_filter_ctx->total_count_);
        check_count = max(check_count, join_filter_ctx->check_count_);
        op_monitor_info_.otherstat_4_value_ = max(join_filter_ctx->ready_ts_, op_monitor_info_.otherstat_4_value_);
        op_monitor_info_.otherstat_5_value_ = max(join_filter_ctx->by_pass_count_before_ready_, op_monitor_info_.otherstat_5_value_);
      }
    }
  }
  if (OB_SUCC(ret)) {
    op_monitor_info_.otherstat_2_value_ += total_count;
    op_monitor_info_.otherstat_3_value_ += check_count;
  }
  return ret;
}

int ObJoinFilterOp::open_join_filter_create()
{
  int ret = OB_SUCCESS;
  int64_t filter_len = MY_SPEC.filter_len_;
  common::ObIAllocator &allocator = ctx_.get_allocator();
  int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  int64_t timeout_ts = GET_PHY_PLAN_CTX(ctx_)->get_timeout_timestamp();
  ObJoinFilterOpInput *filter_input = static_cast<ObJoinFilterOpInput*>(input_);
  ObPxSQCProxy *sqc_proxy = reinterpret_cast<ObPxSQCProxy *>(
      filter_input->share_info_.ch_provider_ptr_);
  if (!MY_SPEC.is_shared_join_filter() && OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
      &ctx_, MY_SPEC.px_est_size_factor_, filter_len, filter_len))) {
    LOG_WARN("failed to get px size", K(ret));
  } else if (!MY_SPEC.is_shared_join_filter()) {
    ObP2PDatahubMsgBase *msg_ptr = nullptr;
    for (int i = 0; i < MY_SPEC.rf_infos_.count() && OB_SUCC(ret); ++i) {
      msg_ptr = nullptr;
      bool construct_bloom_filter_later = MY_SPEC.use_realistic_runtime_bloom_filter_size()
          && MY_SPEC.rf_infos_.at(i).dh_msg_type_ == ObP2PDatahubMsgBase::BLOOM_FILTER_VEC_MSG;
      if (OB_FAIL(PX_P2P_DH.alloc_msg(allocator, MY_SPEC.rf_infos_.at(i).dh_msg_type_, msg_ptr))) {
        LOG_WARN("fail to alloc msg", K(ret));
      } else if (OB_FAIL(local_rf_msgs_.push_back(msg_ptr))) {
        // push_back failed, must destroy the msg immediately
        // if init or construct_msg_details failed, destroy msg during close
        msg_ptr->destroy();
        allocator.free(msg_ptr);
        LOG_WARN("fail to push back msg ptr", K(ret));
      } else if (OB_FAIL(msg_ptr->init(MY_SPEC.rf_infos_.at(i).p2p_datahub_id_,
          filter_input->px_sequence_id_, filter_input->task_id_, tenant_id, timeout_ts, filter_input->register_dm_info_))) {
        LOG_WARN("fail to init msg", K(ret));
      } else if (!construct_bloom_filter_later
                 && OB_FAIL(ObJoinFilterOpInput::construct_msg_details(
                        MY_SPEC, sqc_proxy, filter_input->config_, *msg_ptr, 1, filter_len))) {
        LOG_WARN("fail to construct msg details", K(ret));
      } else if (OB_FAIL(lucky_devil_champions_.push_back(false))) {
        LOG_WARN("fail to push back flag", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      IGNORE_RETURN release_local_msg();
    }
  } else if (OB_FAIL(init_shared_msgs_from_input())) {
    LOG_WARN("fail to init shared msgs from input", K(ret));
  }
  if (OB_SUCC(ret) && MY_SPEC.max_batch_size_ > 0) {
    if (OB_ISNULL(join_filter_hash_values_ =
            (uint64_t *)ctx_.get_allocator().alloc(sizeof(uint64_t) * MY_SPEC.max_batch_size_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc join_filter_hash_values_", K(ret), K(MY_SPEC.max_batch_size_));
    }
  }

  if (MY_SPEC.use_rich_format_) {
    for (int64_t i = 0; i < local_rf_msgs_.count(); ++i) {
      switch (local_rf_msgs_.at(i)->get_msg_type()) {
      case ObP2PDatahubMsgBase::BLOOM_FILTER_VEC_MSG: {
        bf_vec_msg_ = static_cast<ObRFBloomFilterMsg *>(local_rf_msgs_.at(i));
        break;
      }
      case ObP2PDatahubMsgBase::RANGE_FILTER_VEC_MSG: {
        range_vec_msg_ = static_cast<ObRFRangeFilterVecMsg *>(local_rf_msgs_.at(i));
        break;
      }
      case ObP2PDatahubMsgBase::IN_FILTER_VEC_MSG: {
        in_vec_msg_ = static_cast<ObRFInFilterVecMsg *>(local_rf_msgs_.at(i));
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected p2p msg type", K(local_rf_msgs_.at(i)->get_msg_type()));
        break;
      }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (MY_SPEC.is_material_controller() && OB_FAIL(init_material_parameters())) {
    LOG_WARN("failed to init material parameters");
  }
  return ret;
}

int ObJoinFilterOp::init_material_parameters()
{
  // 1.create mem ctx
  // 2.estimate row count, memory size of current worker
  // 3.init sql memory processor
  // 4.init partition splitter, create partitions
  // 5.other parameters

  int ret = OB_SUCCESS;
  int64_t total_row_count = MY_SPEC.filter_len_;
  int64_t worker_row_count = 0;
  int64_t worker_memory_size = 0;
  const int64_t max_batch_size = max(1, MY_SPEC.max_batch_size_);
  uint16_t extra_hash_count = MY_SPEC.jf_material_control_info_.extra_hash_count_;
  const common::ObCompressorType compress_type = parent_->get_spec().compress_type_;
  skip_left_null_ = (INNER_JOIN == MY_SPEC.join_type_) || (LEFT_SEMI_JOIN == MY_SPEC.join_type_)
                    || (RIGHT_SEMI_JOIN == MY_SPEC.join_type_)
                    || (RIGHT_OUTER_JOIN == MY_SPEC.join_type_);

  int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  lib::ContextParam param;
  param.set_mem_attr(tenant_id, "ArenaJoinFilter", ObCtxIds::WORK_AREA)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL);
  ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant config", K(ret));
  } else if (FALSE_IT(force_dump_ = tenant_config->_force_hash_join_spill)) {
  } else if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
    LOG_WARN("create memory context failed");
  } else if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
                 &ctx_, parent_->get_spec().px_est_size_factor_, spec_.rows_, worker_row_count))) {
    LOG_WARN("failed to get px size");
  } else {
    worker_row_count = max(worker_row_count, ObHashJoinVecOp::MIN_ROW_COUNT);
    worker_memory_size = worker_row_count * (MY_SPEC.width_ + sizeof(uint64_t));
  }

  // Consistent with build_row in hash join
  // eg: join cond is cast(left_table.c1) = right_table.c2, in hash join will material expr
  // cast(left_table.c1) into build_row, so need to use build_rows_output_.
  if (OB_FAIL(ret)) {
  } else {
    const ObHashJoinVecSpec *hj_spec = static_cast<const ObHashJoinVecSpec *>(&parent_->get_spec());
    if (OB_ISNULL(ctx_.get_physical_plan_ctx()) || OB_ISNULL(ctx_.get_physical_plan_ctx()->get_phy_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get phy plan", K(ret));
    } else if (ctx_.get_physical_plan_ctx()->get_phy_plan()->get_min_cluster_version() >= CLUSTER_VERSION_4_3_5_0) {
      build_rows_output_ = hj_spec->get_build_rows_output_();
    } else {
      // for compat, gen build_rows_output_ in join filter
      build_rows_output_for_compat_.set_allocator(&eval_ctx_.exec_ctx_.get_allocator());
      OZ (build_rows_output_for_compat_.init(MY_SPEC.output_.count() + hj_spec->build_keys_.count()));
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.output_.count(); i++) {
          OZ (build_rows_output_for_compat_.push_back(MY_SPEC.output_.at(i)));
        }
      }
      int64_t idx = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < hj_spec->build_keys_.count(); i++) {
        ObExpr *left_expr = hj_spec->build_keys_.at(i);
        if (!has_exist_in_array(MY_SPEC.output_, left_expr, &idx)) {
          OZ (build_rows_output_for_compat_.push_back(left_expr));
        }
      }
      build_rows_output_ = &build_rows_output_for_compat_;
    }
  }


  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql_mem_processor_.init(&(mem_context_->get_malloc_allocator()), tenant_id,
                                             worker_memory_size, MY_SPEC.type_, MY_SPEC.id_,
                                             &ctx_))) {
    LOG_WARN("failed to init sql mem mgr");
  } else if (OB_ISNULL(partition_splitter_ =
                           OB_NEWx(ObJoinFilterPartitionSplitter, &ctx_.get_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObJoinFilterPartitionSplitter");
  } else if (OB_FAIL(partition_splitter_->init(tenant_id, mem_context_, eval_ctx_,
                                               &sql_mem_processor_, MY_SPEC.full_hash_join_keys_,
                                               *build_rows_output_, extra_hash_count, max_batch_size,
                                               compress_type))) {
    LOG_WARN("failed to init partition splitter");
  } else if (OB_FAIL(partition_splitter_->prepare_join_partitions(
                 &io_event_observer_, worker_row_count, worker_memory_size))) {
    LOG_WARN("failed to prepare join partitions");
  }

  if (OB_SUCC(ret)) {
    void *buf = ctx_.get_allocator().alloc(sizeof(const ObJoinFilterStoreRow *) * max_batch_size);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate mem for part_stored_rows_");
    } else {
      part_stored_rows_ = static_cast<const ObJoinFilterStoreRow **>(buf);
    }
  }

  if (OB_SUCC(ret)) {
    if (MY_SPEC.can_reuse_hash_join_hash_value()) {
      // if join filter and hash join has same build key, reuse the hash value
      hash_join_hash_values_ = join_filter_hash_values_;
    } else {
      // create hash value for hash join partition
      void *buf = ctx_.get_allocator().alloc(sizeof(uint64_t) * max_batch_size);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc hash_join_hash_values_", K(max_batch_size));
      } else {
        hash_join_hash_values_ = (uint64_t *)(buf);
      }
    }
  }

  if (OB_SUCC(ret)) {
    row_meta_.set_allocator(&ctx_.get_allocator());
    OZ(row_meta_.init(*build_rows_output_, extra_hash_count * sizeof(uint64_t)));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_material_group_exec_info())) {
    LOG_WARN("failed to init_material_group_exec_info");
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("init material parameters", K(MY_SPEC.filter_len_), K(worker_row_count),
              K(worker_memory_size), K(max_batch_size), K(partition_splitter_->get_part_count()),
              K(skip_left_null_), K(force_dump_), K(MY_SPEC.join_keys_.count()),
              K(MY_SPEC.full_hash_join_keys_.count()), K(extra_hash_count));
  }

  return ret;
}

int ObJoinFilterOp::init_material_group_exec_info()
{
  int ret = OB_SUCCESS;
  uint16_t join_filter_count = MY_SPEC.jf_material_control_info_.join_filter_count_;
  uint16_t extra_hash_count = MY_SPEC.jf_material_control_info_.extra_hash_count_;
  void *buf = ctx_.get_allocator().alloc(sizeof(ObJoinFilterMaterialGroupController));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc group_controller_", K(join_filter_count));
  } else {
    group_controller_ =
        new (buf) ObJoinFilterMaterialGroupController(join_filter_count, extra_hash_count, ctx_.get_allocator());
  }

  if (OB_SUCC(ret)) {
    buf = ctx_.get_allocator().alloc(sizeof(uint64_t *) * (join_filter_count));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc group_join_filter_hash_values_", K(join_filter_count));
    } else {
      group_controller_->group_join_filter_hash_values_ = (uint64_t **)(buf);
    }
  }

  if (OB_SUCC(ret)) {
    buf = ctx_.get_allocator().alloc(sizeof(uint16_t) * (join_filter_count));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc hash_id_map_", K(join_filter_count));
    } else {
      group_controller_->hash_id_map_ = (uint16_t *)(buf);
    }
  }

  if (OB_SUCC(ret)) {
    buf = ctx_.get_allocator().alloc(sizeof(ObHyperLogLogCalculator));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc hash join keys hyperloglog calculator");
    } else {
      group_controller_->hash_join_keys_hllc_ = new (buf) ObHyperLogLogCalculator();
      if (OB_FAIL(group_controller_->hash_join_keys_hllc_->init(&ctx_.get_allocator(), N_HYPERLOGLOG_BIT))) {
        LOG_WARN("fail to init hyperloglog calculator", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObOperator *cur_op = this;
    ObJoinFilterOp *join_filter_op = nullptr;
    if (OB_FAIL(group_controller_->join_filter_ops_.init(join_filter_count))) {
      LOG_WARN("failed to init join_filter_ops_");
    }
    for (int64_t i = 0; i < join_filter_count && OB_SUCC(ret); ++i) {
      if (cur_op->get_spec().get_type() != PHY_JOIN_FILTER) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("must be join filter bellow");
      } else if (FALSE_IT(join_filter_op = static_cast<ObJoinFilterOp *>(cur_op))) {
      } else if (OB_FAIL(group_controller_->join_filter_ops_.push_back(join_filter_op))) {
        LOG_WARN("failed to push back join_filter_op");
      } else {
        const ObJoinFilterSpec &spec = get_my_spec(*join_filter_op);
        uint16_t hash_id = spec.jf_material_control_info_.hash_id_;
        group_controller_->group_join_filter_hash_values_[i] = join_filter_op->get_join_filter_hash_values();
        group_controller_->hash_id_map_[i] = hash_id;
        if (spec.can_reuse_hash_join_hash_value()) {
          join_filter_op->hllc_ = group_controller_->hash_join_keys_hllc_;
        } else {
          buf = ctx_.get_allocator().alloc(sizeof(ObHyperLogLogCalculator));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc hash join keys hyperloglog calculator");
          } else {
            join_filter_op->hllc_ = new (buf) ObHyperLogLogCalculator();
            if (OB_FAIL(join_filter_op->hllc_->init(&ctx_.get_allocator(), N_HYPERLOGLOG_BIT))) {
              LOG_WARN("fail to init hyperloglog calculator", K(ret));
            }
          }
        }
        cur_op = cur_op->get_child();
      }
    }
  }
  return ret;
}

int ObJoinFilterOp::process_dump()
{
  int ret = common::OB_SUCCESS;
  bool updated = false;
  bool should_dump = false;
  ObJoinFilterMaxAvailableMemChecker max_available_mem_checker(
      partition_splitter_->get_row_count_in_memory());
  ObJoinFilterExtendMaxMemChecker extend_max_mem_checker(sql_mem_processor_.get_data_size());
  if (!GCONF.is_sql_operator_dump_enabled()) {
    // do nothing, disable dump
  } else if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
               &mem_context_->get_malloc_allocator(), max_available_mem_checker, updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if ((updated || need_dump())
             && OB_FAIL(sql_mem_processor_.extend_max_memory_size(
                  &mem_context_->get_malloc_allocator(), extend_max_mem_checker, should_dump,
                  sql_mem_processor_.get_data_size()))) {
    LOG_WARN("fail to extend max memory size", K(ret), K(updated));
  } else if (should_dump) {
    int64_t need_dump_size = mem_context_->used() - sql_mem_processor_.get_mem_bound();
    if (OB_FAIL(partition_splitter_->dump_from_back_to_front(need_dump_size))) {
      LOG_WARN("failed to dump_from_back_to_front");
    }
  }
  return ret;
}

int ObJoinFilterOp::calc_join_filter_hash_values(const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.can_reuse_hash_join_hash_value()) {
    // do noting
  } else {
    uint64_t seed = ObHashJoinVecOp::HASH_SEED;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < MY_SPEC.join_keys_.count(); ++idx) {
      ObExpr *expr = MY_SPEC.join_keys_.at(idx);
      if (OB_FAIL(expr->eval_vector(eval_ctx_, brs))) {
        LOG_WARN("eval failed", K(ret));
      } else {
        const bool is_batch_seed = (idx > 0);
        ObIVector *vector = expr->get_vector(eval_ctx_);
        ret =
            vector->murmur_hash_v3(*expr, join_filter_hash_values_, *brs.skip_,
                                   EvalBound(brs.size_, brs.all_rows_active_),
                                   is_batch_seed ? join_filter_hash_values_ : &seed, is_batch_seed);
      }
    }
    if (OB_SUCC(ret)) {
      if (use_hllc_estimate_ndv()) {
        ObBitVector::flip_foreach(
          *brs.skip_, brs.size_, MarkHashValueAndSetHyperLogLogOP(join_filter_hash_values_, hllc_));
      } else {
        ObBitVector::flip_foreach(*brs.skip_, brs.size_, MarkHashValueOP(join_filter_hash_values_));
      }
    }
  }
  return ret;
}

void ObJoinFilterOp::read_join_filter_hash_values_from_store(
    const ObBatchRows &brs, const ObJoinFilterStoreRow **store_rows, const RowMeta &row_meta,
    uint64_t *join_filter_hash_values)
{
  uint16_t hash_id = MY_SPEC.jf_material_control_info_.hash_id_;
  for (int64_t i = 0; i < brs.size_; i++) {
    join_filter_hash_values[i] = store_rows[i]->get_join_filter_hash_value(row_meta, hash_id);
  }
}

int ObJoinFilterOp::send_datahub_count_row_msg(int64_t &total_row_count,
                                               ObTMArray<ObJoinFilterNdv *> &ndv_info,
                                               bool need_wait_whole_msg)
{
  int ret = OB_SUCCESS;
  ObJoinFilterCountRowPieceMsg piece_msg;
  ObPxSqcHandler *handler = ctx_.get_sqc_handler();
  if (OB_ISNULL(handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null arguments", K(ret), K(handler));
  } else {
    piece_msg.op_id_ = MY_SPEC.get_id();
    piece_msg.thread_id_ = GETTID();
    piece_msg.source_dfo_id_ = handler->get_sqc_proxy().get_dfo_id();
    piece_msg.target_dfo_id_ = handler->get_sqc_proxy().get_dfo_id();
    piece_msg.piece_count_ = 1;
    piece_msg.total_rows_ = partition_splitter_->get_total_row_count();
    piece_msg.sqc_id_ = handler->get_sqc_proxy().get_sqc_id();
    piece_msg.each_sqc_has_full_data_ = MY_SPEC.jf_material_control_info_.each_sqc_has_full_data_;
    if (OB_FAIL(piece_msg.ndv_info_.prepare_allocate(ndv_info.count()))) {
      LOG_WARN("failed to prepare_allocate", K(ndv_info.count()));
    }
    for (int64_t i = 0; i < ndv_info.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(piece_msg.ndv_info_.at(i).assign(*ndv_info.at(i)))) {
        LOG_WARN("failt to assign ObJoinFilterNdv", K(ret));
      }
    }
    const ObJoinFilterCountRowWholeMsg *whole_msg = nullptr;

    int is_local = (DHSENDOPTLOCAL == OB_SUCCESS) && can_sync_row_count_locally();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(handler->get_sqc_proxy().aggregate_sqc_pieces_and_get_dh_msg(
                   MY_SPEC.get_id(), dtl::DH_JOIN_FILTER_COUNT_ROW_WHOLE_MSG, piece_msg,
                   ctx_.get_physical_plan_ctx()->get_timeout_timestamp(), true /*need_sync*/,
                   is_local, need_wait_whole_msg, whole_msg))) {
      LOG_WARN("failed to aggregate_sqc_pieces_and_get_dh_msg");
    } else if (!need_wait_whole_msg) {
      // only send, not wait
    } else if (OB_ISNULL(whole_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null msg", K(ret));
    } else {
      total_row_count = whole_msg->get_total_rows();
      for (int64_t i = 0; i < ndv_info.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(ndv_info.at(i)->assign(whole_msg->ndv_info_.at(i)))) {
          LOG_WARN("failt to assign ObJoinFilterNdv", K(ret));
        } else if (use_hllc_estimate_ndv()) {
          group_controller_->join_filter_ops_.at(i)->total_ndv_ =
            whole_msg->get_ndv_info().at(i).bf_ndv_;
        }
      }
    }
    LOG_TRACE("print row from datahub", K(total_row_count), K(MY_SPEC.is_shuffle_),
              K(need_wait_whole_msg), K(ndv_info));
  }
  return ret;
}

bool ObJoinFilterOp::can_sync_row_count_locally()
{
  // if each sqc has full data of left table(in shared hash join),
  // or there is only one sqc, we can synchronize row count without datahub
  return MY_SPEC.jf_material_control_info_.each_sqc_has_full_data_
         || ctx_.get_sqc_handler()->get_sqc_init_arg().sqc_.get_sqc_count() == 1;
}

int ObJoinFilterOp::get_exec_row_count_and_ndv(const int64_t worker_row_count, int64_t &total_row_count,
                                       bool is_in_drain)
{
  int ret = OB_SUCCESS;
  // when drain exchange, not need to wait whole msg
  bool need_wait_whole = !is_in_drain;
  bool need_sync_row_count = MY_SPEC.need_sync_row_count();
  // In local join filter scenario(partition-wise, pkey-none), need_sync_row_count == false, so we
  // implement a new function *group_collect_worker_ndv_by_hllc()* which decouple collect ndv logic
  // from *group_build_ndv_info_before_aggregate()*. We need to call
  // group_collect_worker_ndv_by_hllc() before check *need_sync_row_count* so that we can collect ndv
  // info by hllc in local join filter scenario.
  if (use_hllc_estimate_ndv()
      && OB_FAIL(group_controller_->apply(group_collect_worker_ndv_by_hllc))) {
    LOG_WARN("failed to group group_collect_worker_ndv");
  } else if (has_sync_row_count_ || !need_sync_row_count) {
    // already done, or not need to sync
  } else {
    ObTMArray<ObJoinFilterNdv *> ndv_info;
    if (OB_FAIL(group_controller_->apply(group_build_ndv_info_before_aggregate, ndv_info))) {
      LOG_WARN("failed to group group_collect_worker_in_filter_ndv");
    } else if (OB_FAIL(send_datahub_count_row_msg(total_row_count, ndv_info, need_wait_whole))) {
      LOG_WARN("failed to sync row count");
    } else {
      has_sync_row_count_ = true;
    }
  }
  return ret;
}

int ObJoinFilterOp::fill_range_filter(const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(range_vec_msg_)) {
    if (OB_FAIL(range_vec_msg_->insert_by_row_vector(&brs, MY_SPEC.join_keys_, MY_SPEC.hash_funcs_,
                                                     MY_SPEC.calc_tablet_id_expr_, eval_ctx_,
                                                     join_filter_hash_values_))) {
      LOG_WARN("failed to insert rows to range vec msg", K(ret));
    }
  }
  return ret;
}

int ObJoinFilterOp::fill_in_filter(const ObBatchRows &brs, uint64_t *hash_join_hash_values)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(in_vec_msg_)) {
    bool reuse = MY_SPEC.can_reuse_hash_join_hash_value();
    if (OB_FAIL(in_vec_msg_->insert_by_row_vector_without_calc_hash_value(
            &brs, MY_SPEC.join_keys_, MY_SPEC.hash_funcs_, eval_ctx_,
            reuse ? hash_join_hash_values : join_filter_hash_values_))) {
      LOG_WARN("failed to insert rows to in vec msg");
    }
  }
  return ret;
}

int ObJoinFilterOp::build_ndv_info_before_aggregate(ObTMArray<ObJoinFilterNdv *> &ndv_info)
{
  int ret = OB_SUCCESS;
  // 1.set infomation which used to check if *in filter* active
  if (nullptr == in_vec_msg_ || !in_vec_msg_->is_active()) {
    dh_ndv_.in_filter_active_ = false;
  } else {
    dh_ndv_.in_filter_active_ = true;
    dh_ndv_.in_filter_ndv_ = in_vec_msg_->sm_hash_set_.size();
  }

  // 2.set infomation which used to estimate ndv of *bloom filter*
  if (use_hllc_estimate_ndv()) {
    dh_ndv_.use_hllc_estimate_ndv_ = true;
    dh_ndv_.hllc_.shadow_copy(*hllc_);
  }

  if (OB_FAIL(ndv_info.push_back(&dh_ndv_))) {
    LOG_WARN("failed to push back ndv info");
  }
  return ret;
}

void ObJoinFilterOp::check_in_filter_active(int64_t &in_filter_ndv)
{
  if (nullptr == in_vec_msg_) {
    in_filter_active_ = false;
  } else {
    if (!MY_SPEC.is_shared_join_filter()) {
      // for non shared join filter, each worker check self
      if (in_vec_msg_->is_active()) {
        in_filter_active_ = true;
      } else {
        in_filter_active_ = false;
      }
    } else {
      // for shared join filter, if each worker's ndv is less than runtime_filter_max_in_num_,
      // but total runtime_filter_max_in_num_ is large than runtime_filter_max_in_num_,
      // the in filter is inactive, but we can use ndv to build a smaller bloom filter
      if (dh_ndv_.in_filter_active_) {
        ObJoinFilterOpInput *filter_input = static_cast<ObJoinFilterOpInput *>(input_);
        in_filter_ndv = dh_ndv_.in_filter_ndv_;
        in_filter_active_ = dh_ndv_.in_filter_ndv_ <= filter_input->config_.runtime_filter_max_in_num_;
      } else {
        in_filter_active_ = false;
      }
    }
  }
}

int ObJoinFilterOp::init_bloom_filter(const int64_t worker_row_count, const int64_t total_row_count)
{
  int ret = OB_SUCCESS;
  int64_t bloom_filter_data_len = 0;
  bool construct_in_this_worker = false;
  int sqc_count = 1;
  if (MY_SPEC.is_shuffle_) {
    // for shuffled join filter, we gather K piece bloom filter create by all K sqcs.
    // opt: if is shared hash join, each sqc has full data of build table, we only
    // need to gather one piece join filter
    if (build_send_opt() && MY_SPEC.jf_material_control_info_.each_sqc_has_full_data_) {
      sqc_count = 1;
    } else {
      sqc_count = ctx_.get_sqc_handler()->get_sqc_init_arg().sqc_.get_sqc_count();
    }
  }

  ObJoinFilterOpInput *filter_input = static_cast<ObJoinFilterOpInput *>(input_);
  ObPxSQCProxy *sqc_proxy =
      reinterpret_cast<ObPxSQCProxy *>(filter_input->share_info_.ch_provider_ptr_);

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(bf_vec_msg_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null bloom filter msg");
  } else {
    if (!MY_SPEC.is_shared_join_filter()) {
      // for hash join with pq_distribute in (pkey, none), (none, none)
      // each worker has its own bloom filter, each worker can init its bloom filter
      bloom_filter_data_len = worker_row_count;
      construct_in_this_worker = true;
    } else {
      // for shared join filter, only the leader is responsible for init bloom filter
      bloom_filter_data_len = total_row_count;
      construct_in_this_worker =
          filter_input->share_info_.shared_jf_constructor_->try_acquire_constructor();
    }
    if (!construct_in_this_worker) {
      // waiting bloom filter constructed
      if (OB_FAIL(filter_input->share_info_.shared_jf_constructor_->wait_constructed(
              this, bf_vec_msg_))) {
        LOG_WARN("failed to wait bloom filter init");
      }
    } else {
      // only the constructor is responsible for init bloom filter
      if (OB_FAIL(ObJoinFilterOpInput::construct_msg_details(MY_SPEC, sqc_proxy,
                                                             filter_input->config_, *bf_vec_msg_,
                                                             sqc_count, bloom_filter_data_len))) {
        LOG_WARN("failed to construct bloom filter ", K(spec_.get_id()));
        // must notify to let other workers exit
        if (MY_SPEC.is_shared_join_filter()) {
          (void)filter_input->share_info_.shared_jf_constructor_->notify_constructed();
        }
      } else if (MY_SPEC.is_shared_join_filter()) {
        // for shared join filter, notify other worker exit
        (void)filter_input->share_info_.shared_jf_constructor_->notify_constructed();
      }
      LOG_TRACE("constructor init bloom filter with rows", K(spec_.get_id()),
                K(MY_SPEC.is_shared_join_filter()), K(worker_row_count),
                K(total_row_count), K(bloom_filter_data_len), K(sqc_count));
    }
  }
  if (OB_SUCC(ret)) {
    // add a join filter len monitor info
    op_monitor_info_.otherstat_6_id_ = ObSqlMonitorStatIds::JOIN_FILTER_LENGTH;
    op_monitor_info_.otherstat_6_value_ = construct_in_this_worker ? bloom_filter_data_len : 0;
  }
  return ret;
}

int ObJoinFilterOp::fill_bloom_filter() {
  int ret = OB_SUCCESS;
  int64_t max_batch_size = max(1, MY_SPEC.max_batch_size_);
  int64_t part_count = partition_splitter_->get_part_count();
  int64_t read_rows = 0;
  ObPartitionStore *cur_partition = nullptr;
  if (OB_ISNULL(bf_vec_msg_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null bloom filter msg");
  } else {
    int64_t total_read = 0;
    for (int64_t i = 0; i < part_count && OB_SUCC(ret); ++i) {
      cur_partition = partition_splitter_->get_partition(i);
      cur_partition->begin_iterator();
      while (OB_SUCC(ret)) {
        if (OB_FAIL(try_check_status())) {
          LOG_WARN("failed to check status", K(ret));
        } else if (OB_FAIL(cur_partition->get_next_batch(
                       reinterpret_cast<const ObCompactRow **>(part_stored_rows_), max_batch_size,
                       read_rows))) {
          if (OB_ITER_END == ret) {
            // break while and get next partition
            brs_.size_ = 0;
            brs_.end_ = true;
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next batch", K(ret));
          }
          LOG_WARN("failed to get_next_batch in cur_partition");
        } else {
          brs_.size_ = read_rows;
          total_read += read_rows;
          brs_.end_ = false;
          brs_.skip_->reset(read_rows);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(group_controller_->apply(group_fill_bloom_filter, brs_, part_stored_rows_,
                                                  row_meta_))) {
          LOG_WARN("failed to group control fill bloom filter");
        }
      }
    }
    LOG_TRACE("fill x rows to bloom filter", K(total_read));
  }
  return ret;
}

int ObJoinFilterOp::group_init_bloom_filter(ObJoinFilterOp *join_filter_op,
                                            int64_t worker_row_count, int64_t total_row_count)
{
  int ret = OB_SUCCESS;
  int64_t final_worker_row_count = worker_row_count;
  int64_t final_total_row_count = total_row_count;
  int64_t in_filter_ndv = total_row_count;  //only exist for compatibility

  (void)join_filter_op->check_in_filter_active(in_filter_ndv);

  uint64_t op_id = join_filter_op->get_spec().get_id();
  uint64_t task_id = join_filter_op->ctx_.get_px_task_id();
  LOG_TRACE("[NDV_BLOOM_FILTER][JoinFilter]: ", K(op_id),
            K(get_my_spec(*join_filter_op).is_shared_join_filter()), K(task_id),
            K(worker_row_count), K(total_row_count), K(join_filter_op->worker_ndv_),
            K(join_filter_op->total_ndv_), K(in_filter_ndv));

  // we can use ndv to build a smaller bloom filter
  if (join_filter_op->use_hllc_estimate_ndv()) {
    if (!get_my_spec(*join_filter_op).is_shared_join_filter()) {
      final_worker_row_count = join_filter_op->worker_ndv_;
    } else {
      final_total_row_count = join_filter_op->total_ndv_;
    }
  } else if (join_filter_op->build_send_opt()) {
    final_total_row_count = in_filter_ndv;
  }

  if (join_filter_op->skip_fill_bloom_filter()) {
    // in filter active, disable bloom filter
    join_filter_op->bf_vec_msg_->set_is_active(false);
    // in case of wait bloom filter too long, still need to build bloom filter message and send it
    final_worker_row_count = 1;
    final_total_row_count = 1;
  }
  LOG_TRACE("check in filter active", K(join_filter_op->in_filter_active_), K(final_worker_row_count),
            K(final_total_row_count));

  if (OB_FAIL(join_filter_op->init_bloom_filter(final_worker_row_count, final_total_row_count))) {
    LOG_WARN("failed to init bloom filter under control", K(join_filter_op->get_spec().get_id()),
             K(final_worker_row_count), K(final_total_row_count));
  }
  return ret;
}

int ObJoinFilterOp::group_fill_bloom_filter(ObJoinFilterOp *join_filter_op,
                                            const ObBatchRows &brs_from_controller,
                                            const ObJoinFilterStoreRow **part_stored_rows,
                                            const RowMeta &row_meta)
{
  int ret = OB_SUCCESS;
  ObRFBloomFilterMsg *bf_vec_msg = join_filter_op->bf_vec_msg_;
  uint64_t *join_filter_hash_values = join_filter_op->join_filter_hash_values_;
  if (OB_ISNULL(bf_vec_msg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null bloom filter msg", K(join_filter_op->get_spec().get_id()));
  } else if (join_filter_op->skip_fill_bloom_filter()) {
    // not need to insert
  } else if (FALSE_IT(join_filter_op->read_join_filter_hash_values_from_store(
                 brs_from_controller, part_stored_rows, row_meta, join_filter_hash_values))) {
  } else if (OB_FAIL(bf_vec_msg->insert_bloom_filter_with_hash_values(&brs_from_controller,
                                                                      join_filter_hash_values))) {
    LOG_WARN("failed to insert bloom filter", K(join_filter_op->get_spec().get_id()));
  }
  return ret;
}

int ObJoinFilterOp::group_merge_and_send_join_filter(ObJoinFilterOp *join_filter_op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(join_filter_op->try_merge_join_filter())) {
    LOG_WARN("failed to try merge join filter", K(join_filter_op->get_spec().get_id()));
  } else if (OB_FAIL(join_filter_op->try_send_join_filter())) {
    LOG_WARN("failed to try send join filter", K(join_filter_op->get_spec().get_id()));
  }
  return ret;
}

int ObJoinFilterOp::open_join_filter_use()
{
  int ret = OB_SUCCESS;
  ObJoinFilterOpInput *filter_input = static_cast<ObJoinFilterOpInput*>(input_);
  int64_t task_id = MY_SPEC.is_shared_join_filter() ? 0 : filter_input->task_id_;
  int64_t px_seq_id = filter_input->px_sequence_id_;
  for (int i = 0; i < MY_SPEC.rf_infos_.count() && OB_SUCC(ret); ++i) {
    if (OB_INVALID_ID != MY_SPEC.rf_infos_.at(i).filter_expr_id_) {
      ObP2PDatahubMsgBase::ObP2PDatahubMsgType dh_msg_type = MY_SPEC.rf_infos_.at(i).dh_msg_type_;
      ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx = NULL;
      if (OB_ISNULL(join_filter_ctx = static_cast<ObExprJoinFilter::ObExprJoinFilterContext *>(
          ctx_.get_expr_op_ctx(MY_SPEC.rf_infos_.at(i).filter_expr_id_)))) {
        if (OB_FAIL(ctx_.create_expr_op_ctx(MY_SPEC.rf_infos_.at(i).filter_expr_id_, join_filter_ctx))) {
          LOG_WARN("failed to create operator ctx", K(ret), K(MY_SPEC.rf_infos_.at(i).filter_expr_id_));
        } else {
          ObP2PDhKey dh_key(MY_SPEC.rf_infos_.at(i).p2p_datahub_id_, px_seq_id, task_id);
          join_filter_ctx->rf_key_ = dh_key;
          int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
          join_filter_ctx->slide_window_.set_window_size(ADAPTIVE_BF_WINDOW_ORG_SIZE);
          join_filter_ctx->max_wait_time_ms_ = filter_input->config_.runtime_filter_wait_time_ms_;
          join_filter_ctx->hash_funcs_.set_allocator(&ctx_.get_allocator());
          join_filter_ctx->cmp_funcs_.set_allocator(&ctx_.get_allocator());
          join_filter_ctx->is_partition_wise_jf_ = !MY_SPEC.is_shared_join_filter();
          if (OB_FAIL(join_filter_ctx->hash_funcs_.init(MY_SPEC.hash_funcs_.count()))) {
            LOG_WARN("failed to assign hash_func");
          } else if (OB_FAIL(join_filter_ctx->cmp_funcs_.init(MY_SPEC.cmp_funcs_.count()))) {
            LOG_WARN("failed to assign cmp_funcs_");
          } else if (OB_FAIL(join_filter_ctx->hash_funcs_.assign(MY_SPEC.hash_funcs_))) {
            LOG_WARN("failed to assign hash_func");
          } else if (OB_FAIL(join_filter_ctx->cmp_funcs_.assign(MY_SPEC.cmp_funcs_))) {
            LOG_WARN("failed to assign cmp_funcs_");
          } else if (ObP2PDatahubMsgBase::IN_FILTER_MSG == dh_msg_type &&
              OB_FAIL(join_filter_ctx->cur_row_.prepare_allocate(MY_SPEC.cmp_funcs_.count()))) {
            LOG_WARN("failed to prepare_allocate cur_row_");
          } else if (spec_.use_rich_format_
                     && OB_FAIL(prepare_extra_use_info_for_vec20(join_filter_ctx, dh_msg_type))) {
            LOG_WARN("failed to prepare_extra_use_info_for_vec20");
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join filter ctx is unexpected", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinFilterOp::prepare_extra_use_info_for_vec20(
    ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx,
    ObP2PDatahubMsgBase::ObP2PDatahubMsgType dh_msg_type)
{
  int ret = OB_SUCCESS;
  int64_t max_batch_size = spec_.max_batch_size_;
  if (0 >= max_batch_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected max_batch_size", K(max_batch_size));
  }

  // allocate row_with_hash
  if (OB_SUCC(ret)) {
    if (ObP2PDatahubMsgBase::IN_FILTER_VEC_MSG == dh_msg_type) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = ctx_.get_allocator().alloc(sizeof(ObRowWithHash)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObRowWithHash");
      } else if (FALSE_IT(join_filter_ctx->cur_row_with_hash_ =
                              new (buf) ObRowWithHash(ctx_.get_allocator()))) {
      } else if (OB_FAIL(join_filter_ctx->cur_row_with_hash_->row_.prepare_allocate(
                     MY_SPEC.cmp_funcs_.count()))) {
        LOG_WARN("failed to prepare_allocate cur_row_with_hash_");
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (ObP2PDatahubMsgBase::IN_FILTER_VEC_MSG == dh_msg_type
        || ObP2PDatahubMsgBase::BLOOM_FILTER_VEC_MSG == dh_msg_type) {
      // allocate skip vector
      void *buf = nullptr;
      if (OB_ISNULL(buf = ctx_.get_allocator().alloc(ObBitVector::memory_size(max_batch_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate skip_vector_", K(max_batch_size));
      } else {
        join_filter_ctx->skip_vector_ = to_bit_vector(buf);
        join_filter_ctx->skip_vector_->init(max_batch_size);
      }

      if (OB_SUCC(ret)) {
        // allocate right_hash_vals_
        buf = nullptr;
        if (OB_ISNULL(buf = ctx_.get_allocator().alloc((sizeof(uint64_t) * max_batch_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate right_hash_vals_", K(max_batch_size));
        } else {
          join_filter_ctx->right_hash_vals_ = static_cast<uint64_t *>(buf);
        }
      }
    }
  }
  return ret;
}

int ObJoinFilterOp::init_shared_msgs_from_input()
{
  int ret = OB_SUCCESS;
  ObJoinFilterOpInput *op_input = static_cast<ObJoinFilterOpInput*>(input_);
  ObArray<ObP2PDatahubMsgBase *> *array_ptr =
      reinterpret_cast<ObArray<ObP2PDatahubMsgBase *> *>(op_input->share_info_.shared_msgs_);
  if (OB_ISNULL(array_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array ptr is nullptr", K(ret));
  } else if (OB_FAIL(shared_rf_msgs_.assign(*array_ptr))) {
    LOG_WARN("fail to assign array ptr", K(ret));
  } else {
    for (int i = 0; i < shared_rf_msgs_.count() && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(shared_rf_msgs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected shared rf msgs", K(ret));
      } else if (OB_FAIL(init_local_msg_from_shared_msg(*shared_rf_msgs_.at(i)))) {
        LOG_WARN("fail to copy local msgs", K(ret));
      } else if (OB_FAIL(lucky_devil_champions_.push_back(false))) {
        LOG_WARN("fail to push back flag", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      IGNORE_RETURN release_local_msg();
    }
  }
  return ret;
}

int ObJoinFilterOp::init_local_msg_from_shared_msg(ObP2PDatahubMsgBase &msg)
{
  int ret = OB_SUCCESS;
  ObJoinFilterOpInput *filter_input = static_cast<ObJoinFilterOpInput*>(input_);
  ObPxSQCProxy *sqc_proxy = reinterpret_cast<ObPxSQCProxy *>(
      filter_input->share_info_.ch_provider_ptr_);
  switch(msg.get_msg_type()) {
    case ObP2PDatahubMsgBase::BLOOM_FILTER_VEC_MSG:
    case ObP2PDatahubMsgBase::BLOOM_FILTER_MSG: {
      ret = local_rf_msgs_.push_back(&msg);
      break;
    }
    case ObP2PDatahubMsgBase::RANGE_FILTER_MSG: {
      ObP2PDatahubMsgBase *range_ptr = nullptr;
      if (OB_FAIL(PX_P2P_DH.alloc_msg(ctx_.get_allocator(),
          ObP2PDatahubMsgBase::RANGE_FILTER_MSG, range_ptr))) {
        LOG_WARN("fail to alloc msg", K(ret));
      } else if (OB_FAIL(local_rf_msgs_.push_back(range_ptr))) {
        // push_back failed, must destroy the msg immediately
        // if init or construct_msg_details failed, destroy msg in release_local_msg
        range_ptr->destroy();
        ctx_.get_allocator().free(range_ptr);
        LOG_WARN("fail to push back local rf msgs", K(ret));
      } else if (OB_FAIL(range_ptr->init(msg.get_p2p_datahub_id(),
          msg.get_px_seq_id(), 0/*task_id*/, msg.get_tenant_id(),
          msg.get_timeout_ts(), filter_input->register_dm_info_))) {
        LOG_WARN("fail to init msg", K(ret));
      } else if (OB_FAIL(ObJoinFilterOpInput::construct_msg_details(
                     MY_SPEC, sqc_proxy, filter_input->config_, *range_ptr,
                     msg.get_msg_receive_expect_cnt(), MY_SPEC.filter_len_))) {
        LOG_WARN("fail to construct msg details", K(ret));
      }
      break;
    }
    case ObP2PDatahubMsgBase::RANGE_FILTER_VEC_MSG: {
      ObP2PDatahubMsgBase *range_ptr = nullptr;
      if (OB_FAIL(PX_P2P_DH.alloc_msg(ctx_.get_allocator(),
          ObP2PDatahubMsgBase::RANGE_FILTER_VEC_MSG, range_ptr))) {
        LOG_WARN("fail to alloc msg", K(ret));
      } else if (OB_FAIL(local_rf_msgs_.push_back(range_ptr))) {
        // push_back failed, must destroy the msg immediately
        // if init or construct_msg_details failed, destroy msg during close
        range_ptr->destroy();
        ctx_.get_allocator().free(range_ptr);
        LOG_WARN("fail to push back local rf msgs", K(ret));
      } else if (OB_FAIL(range_ptr->init(msg.get_p2p_datahub_id(),
          msg.get_px_seq_id(), 0/*task_id*/, msg.get_tenant_id(),
          msg.get_timeout_ts(), filter_input->register_dm_info_))) {
        LOG_WARN("fail to init msg", K(ret));
      } else if (OB_FAIL(ObJoinFilterOpInput::construct_msg_details(
                     MY_SPEC, sqc_proxy, filter_input->config_, *range_ptr,
                     msg.get_msg_receive_expect_cnt(), MY_SPEC.filter_len_))) {
        LOG_WARN("fail to construct msg details", K(ret));
      }
      break;
    }
    case ObP2PDatahubMsgBase::IN_FILTER_MSG: {
      ObP2PDatahubMsgBase *in_ptr = nullptr;
      if (OB_FAIL(PX_P2P_DH.alloc_msg(ctx_.get_allocator(),
          ObP2PDatahubMsgBase::IN_FILTER_MSG, in_ptr))) {
        LOG_WARN("fail to alloc msg", K(ret));
      } else if (OB_FAIL(local_rf_msgs_.push_back(in_ptr))) {
        // push_back failed, must destroy the msg immediately
        // if init or construct_msg_details failed, destroy msg in release_local_msg
        in_ptr->destroy();
        ctx_.get_allocator().free(in_ptr);
        LOG_WARN("fail to push back local rf msgs", K(ret));
      } else if (OB_FAIL(in_ptr->init(msg.get_p2p_datahub_id(),
        msg.get_px_seq_id(), 0/*task_id*/, msg.get_tenant_id(),
        msg.get_timeout_ts(), filter_input->register_dm_info_))) {
        LOG_WARN("fail to init msg", K(ret));
      } else if (OB_FAIL(ObJoinFilterOpInput::construct_msg_details(
                     MY_SPEC, sqc_proxy, filter_input->config_, *in_ptr,
                     msg.get_msg_receive_expect_cnt(), MY_SPEC.filter_len_))) {
        LOG_WARN("fail to construct msg details", K(ret));
      }
      break;
    }
    case ObP2PDatahubMsgBase::IN_FILTER_VEC_MSG: {
      ObP2PDatahubMsgBase *in_ptr = nullptr;
      if (OB_FAIL(PX_P2P_DH.alloc_msg(ctx_.get_allocator(),
          ObP2PDatahubMsgBase::IN_FILTER_VEC_MSG, in_ptr))) {
        LOG_WARN("fail to alloc msg", K(ret));
      } else if (OB_FAIL(local_rf_msgs_.push_back(in_ptr))) {
        // push_back failed, must destroy the msg immediately
        // if init or construct_msg_details failed, destroy msg during close
        in_ptr->destroy();
        ctx_.get_allocator().free(in_ptr);
        LOG_WARN("fail to push back local rf msgs", K(ret));
      } else if (OB_FAIL(in_ptr->init(msg.get_p2p_datahub_id(),
        msg.get_px_seq_id(), 0/*task_id*/, msg.get_tenant_id(),
        msg.get_timeout_ts(), filter_input->register_dm_info_))) {
        LOG_WARN("fail to init msg", K(ret));
      } else if (OB_FAIL(ObJoinFilterOpInput::construct_msg_details(
                     MY_SPEC, sqc_proxy, filter_input->config_, *in_ptr,
                     msg.get_msg_receive_expect_cnt(), MY_SPEC.filter_len_))) {
        LOG_WARN("fail to construct msg details", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected msg type", K(ret), K(msg.get_msg_type()));
    }
  }
  return ret;
}

int ObJoinFilterOp::close_join_filter_create()
{
  int ret = OB_SUCCESS;
  sql_mem_processor_.unregister_profile();
  if (OB_FAIL(release_local_msg())) {
    LOG_WARN("failed release local msg", K(ret));
  } else if (OB_FAIL(release_shared_msg())) {
    LOG_WARN("failed release local msg", K(ret));
  }
  if (MY_SPEC.is_material_controller()) {
    if (nullptr != mem_context_) {
      mem_context_->reuse();
    }
  }
  return ret;
}

int ObJoinFilterOp::release_local_msg()
{
  int ret = OB_SUCCESS;
  ObP2PDatahubMsgBase *msg = nullptr;
  for (int i = 0; i < local_rf_msgs_.count(); ++i) {
    if (OB_NOT_NULL(local_rf_msgs_.at(i))) {
      msg = nullptr;
      ObP2PDhKey key;
      key.p2p_datahub_id_ =
        local_rf_msgs_.at(i)->get_p2p_datahub_id();
      key.task_id_ = local_rf_msgs_.at(i)->get_task_id();
      key.px_sequence_id_ = local_rf_msgs_.at(i)->get_px_seq_id();
      if (!MY_SPEC.is_shared_join_filter()) {
        PX_P2P_DH.erase_msg(key, msg);
        local_rf_msgs_.at(i)->destroy();
      } else if (local_rf_msgs_.at(i)->get_msg_type() != ObP2PDatahubMsgBase::BLOOM_FILTER_MSG
          && local_rf_msgs_.at(i)->get_msg_type() != ObP2PDatahubMsgBase::BLOOM_FILTER_VEC_MSG) {
        local_rf_msgs_.at(i)->destroy();
      }
    }
  }
  return ret;
}

int ObJoinFilterOp::release_shared_msg()
{
  int ret = OB_SUCCESS;
  ObP2PDatahubMsgBase *msg = nullptr;
  ObJoinFilterOpInput *filter_input = static_cast<ObJoinFilterOpInput*>(input_);
  if (MY_SPEC.is_shared_join_filter() && !MY_SPEC.is_shuffle_) {
    bool need_release = filter_input->check_release();
    if (need_release) {
      // shared_rf_msgs_ may not init succ, so when close,
      // clear filter_input->share_info_.shared_msgs_ rather than this->shared_rf_msgs_
      ObArray<ObP2PDatahubMsgBase *> *shared_rf_msgs =
          reinterpret_cast<ObArray<ObP2PDatahubMsgBase *> *>(
              filter_input->share_info_.shared_msgs_);
      if (OB_NOT_NULL(shared_rf_msgs)) {
        for (int i = 0; i < shared_rf_msgs->count(); ++i) {
          if (OB_NOT_NULL(shared_rf_msgs->at(i))) {
            msg = nullptr;
            ObP2PDhKey key;
            key.p2p_datahub_id_ =
            shared_rf_msgs->at(i)->get_p2p_datahub_id();
            key.task_id_ = shared_rf_msgs->at(i)->get_task_id();
            key.px_sequence_id_ = shared_rf_msgs->at(i)->get_px_seq_id();
            PX_P2P_DH.erase_msg(key, msg);
            shared_rf_msgs->at(i)->destroy();
          }
        }
      }
    }
  }
  return ret;
}

int ObJoinFilterOp::close_join_filter_use()
{
  int ret = OB_SUCCESS;
  /*do nothing*/
  return ret;
}
