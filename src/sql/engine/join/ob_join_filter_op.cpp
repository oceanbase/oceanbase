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
#include "lib/utility/utility.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/expr/ob_expr_join_filter.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_vec_msg.h"


using namespace oceanbase;
using namespace common;
using namespace omt;
using namespace sql;
using namespace oceanbase::sql::dtl;

OB_SERIALIZE_MEMBER(ObJoinFilterShareInfo,
    unfinished_count_ptr_,
    ch_provider_ptr_,
    release_ref_ptr_,
    filter_ptr_,
    shared_msgs_);

OB_SERIALIZE_MEMBER(ObJoinFilterRuntimeConfig,
    bloom_filter_ratio_,
    each_group_size_,
    bf_piece_size_,
    runtime_filter_wait_time_ms_,
    runtime_filter_max_in_num_,
    runtime_bloom_filter_max_size_,
    px_message_compression_);

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
                    send_bloom_filter_size_);

OB_SERIALIZE_MEMBER(ObJoinFilterOpInput,
    share_info_,
    bf_idx_at_sqc_proxy_,
    px_sequence_id_,
    config_);

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
  config_.runtime_filter_wait_time_ms_ = ctx.get_my_session()->
      get_runtime_filter_wait_time_ms();
  config_.runtime_filter_max_in_num_ = ctx.get_my_session()->
      get_runtime_filter_max_in_num();
  config_.runtime_bloom_filter_max_size_ = ctx.get_my_session()->
      get_runtime_bloom_filter_max_size();
  config_.px_message_compression_ = true;
  LOG_TRACE("load runtime filter conifg", K(config_));
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
  common::ObIAllocator &allocator = ctx.get_allocator();
  if (OB_ISNULL(ptr = (uint64_t *)allocator.alloc(sizeof(uint64_t) * 2))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ptr", K(ret));
  } else if (OB_FAIL(init_shared_msgs(spec, ctx, sqc_count))) {
    LOG_WARN("fail to init shared msgs", K(ret));
  } else {
    ptr[0] = task_count;
    ptr[1] = task_count;
    share_info_.release_ref_ptr_ = reinterpret_cast<uint64_t>(&ptr[0]);
    share_info_.unfinished_count_ptr_ = reinterpret_cast<uint64_t>(&ptr[1]);
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
      if (OB_FAIL(PX_P2P_DH.alloc_msg(allocator, spec.rf_infos_.at(i).dh_msg_type_, msg_ptr))) {
        LOG_WARN("fail to alloc msg", K(ret));
      } else if (OB_FAIL(array_ptr->push_back(msg_ptr))) {
        // push_back failed, must destory the msg immediately
        // if init or construct_msg_details failed, destory msg after the for loop
        msg_ptr->destroy();
        allocator.free(msg_ptr);
        LOG_WARN("fail to push back array ptr", K(ret));
      } else if (OB_FAIL(msg_ptr->init(spec.rf_infos_.at(i).p2p_datahub_id_,
          px_sequence_id_, 0/*task_id*/, tenant_id, timeout_ts, register_dm_info_))) {
        LOG_WARN("fail to init msg", K(ret));
      } else if (OB_FAIL(construct_msg_details(spec, sqc_proxy, config_, *msg_ptr, sqc_count,
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
        int64_t *filter_idx_ptr = nullptr;
        bool *create_finish = nullptr;
        if (OB_FAIL(bf_msg.generate_filter_indexes(each_group_size, target_cnt, config.bf_piece_size_))) {
          LOG_WARN("fail to generate filter indexes", K(ret));
        } else if (OB_ISNULL(filter_idx_ptr = static_cast<int64_t *>(
              msg.get_allocator().alloc(sizeof(int64_t))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
         } else if (OB_ISNULL(create_finish = static_cast<bool *>(
              msg.get_allocator().alloc(sizeof(bool))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          *filter_idx_ptr = 0;
          *create_finish = false;
          bf_msg.filter_idx_ = filter_idx_ptr;
          bf_msg.create_finish_ = create_finish;
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
    send_bloom_filter_size_(0)
{
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
  : ObOperator(exec_ctx, spec, input),
    filter_create_msg_(nullptr),
    batch_hash_values_(NULL),
    lucky_devil_champions_()
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
  for (int i = 0; i < local_rf_msgs_.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(local_rf_msgs_.at(i)->reuse())) {
      LOG_WARN("fail to reuse local rf msgs", K(ret));
    }
  }
  return ret;
}

int ObJoinFilterOp::do_use_filter_rescan()
{
  int ret = OB_SUCCESS;
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
          if (!*shared_bf_msg->create_finish_) {
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
  LOG_DEBUG("ObJoinFilterOp get_next_batch start");
  int ret = OB_SUCCESS;
  ObJoinFilterOpInput *filter_input = static_cast<ObJoinFilterOpInput*>(input_);
  // for batch
  int64_t batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
  const ObBatchRows *child_brs = nullptr;

  // By @yishen.tmd:
  // This circulation will have uses in the future although it is destined to break at the first time now.
  while (OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_batch(batch_cnt, child_brs))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next batch", K(ret));
      }
    } else {
      brs_.copy(child_brs);
    }
    if (OB_SUCC(ret) && brs_.size_ > 0) {
      if (MY_SPEC.is_create_mode()) {
        if (OB_FAIL(insert_by_row_batch(child_brs))) {
          LOG_WARN("fail to insert bf", K(ret));
        } else {
          break;
        }
      } else if (MY_SPEC.is_use_mode()) {
        // caculate by expr join filter
        break;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the mode of px bloom filter is unexpected", K(MY_SPEC.mode_), K(ret));
      }
    } else {
      break;
    }
  }
  if (OB_ITER_END == ret) {
    brs_.size_ = 0;
    brs_.end_ = true;
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret) && brs_.end_) {
    if (MY_SPEC.is_create_mode()) {
      if (OB_FAIL(try_merge_join_filter())) {
        LOG_WARN("fail to merge join filter", K(ret));
      } else if (OB_FAIL(try_send_join_filter())) {
        LOG_WARN("fail to send bloom filter to use filter", K(ret));
      }
    } else if (MY_SPEC.is_use_mode()) {
      if (OB_FAIL(update_plan_monitor_info())) {
        LOG_WARN("fail to update plan monitor info", K(ret));
      }
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
          batch_hash_values_))) {
        LOG_WARN("fail to insert rf by row batch", K(ret));
      }
    }
  } else {
    for (int i = 0; i < local_rf_msgs_.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(local_rf_msgs_.at(i)->insert_by_row_vector(child_brs,
          MY_SPEC.join_keys_, MY_SPEC.hash_funcs_,
          MY_SPEC.calc_tablet_id_expr_, eval_ctx_,
          batch_hash_values_))) {
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
    LOG_WARN("ERRSIM match, don't merge_join_filter by desigin", K(ret));
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
            bool *create_finish = static_cast<ObRFBloomFilterMsg *>(shared_rf_msgs_.at(i))->create_finish_;
            if (OB_NOT_NULL(create_finish)) {
              *create_finish = true;
            }
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
      if (OB_FAIL(PX_P2P_DH.send_p2p_msg(*shared_rf_msgs_.at(i), *sqc_proxy))) {
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
  op_monitor_info_.otherstat_5_id_ = ObSqlMonitorStatIds::JOIN_FILTER_ID;
  op_monitor_info_.otherstat_5_value_ = MY_SPEC.filter_id_;
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
      if (OB_FAIL(PX_P2P_DH.alloc_msg(allocator, MY_SPEC.rf_infos_.at(i).dh_msg_type_, msg_ptr))) {
        LOG_WARN("fail to alloc msg", K(ret));
      } else if (OB_FAIL(local_rf_msgs_.push_back(msg_ptr))) {
        // push_back failed, must destory the msg immediately
        // if init or construct_msg_details failed, destory msg during close
        msg_ptr->destroy();
        allocator.free(msg_ptr);
        LOG_WARN("fail to push back msg ptr", K(ret));
      } else if (OB_FAIL(msg_ptr->init(MY_SPEC.rf_infos_.at(i).p2p_datahub_id_,
          filter_input->px_sequence_id_, filter_input->task_id_, tenant_id, timeout_ts, filter_input->register_dm_info_))) {
        LOG_WARN("fail to init msg", K(ret));
      } else if (OB_FAIL(ObJoinFilterOpInput::construct_msg_details(MY_SPEC, sqc_proxy,
          filter_input->config_, *msg_ptr, 1, filter_len))) {
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
    if (OB_ISNULL(batch_hash_values_ =
            (uint64_t *)ctx_.get_allocator().alloc(sizeof(uint64_t) * MY_SPEC.max_batch_size_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc batch_hash_values_", K(ret), K(MY_SPEC.max_batch_size_));
    }
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
        // push_back failed, must destory the msg immediately
        // if init or construct_msg_details failed, destory msg in release_local_msg
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
        // push_back failed, must destory the msg immediately
        // if init or construct_msg_details failed, destory msg during close
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
        // push_back failed, must destory the msg immediately
        // if init or construct_msg_details failed, destory msg in release_local_msg
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
        // push_back failed, must destory the msg immediately
        // if init or construct_msg_details failed, destory msg during close
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
  if (OB_FAIL(release_local_msg())) {
    LOG_WARN("failed release local msg", K(ret));
  } else if (OB_FAIL(release_shared_msg())) {
    LOG_WARN("failed release local msg", K(ret));
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
