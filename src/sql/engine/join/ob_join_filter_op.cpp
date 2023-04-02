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
#include "observer/omt/ob_tenant_config_mgr.h"

using namespace oceanbase;
using namespace common;
using namespace omt;
using namespace sql;
using namespace oceanbase::sql::dtl;

OB_SERIALIZE_MEMBER(ObJoinFilterShareInfo,
    unfinished_count_ptr_,
    ch_provider_ptr_,
    release_ref_ptr_,
    filter_ptr_);
OB_SERIALIZE_MEMBER(ObJoinFilterOpInput,
    share_info_,
    is_local_create_,
    bf_idx_at_sqc_proxy_);

int ObJoinFilterOpInput::init(ObTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  UNUSED(task_info);
  //new parallel framework do not use this interface to set parameters
  ret = OB_ERR_UNEXPECTED;
  LOG_WARN("the interface should not be used", K(ret));
  return ret;
}

int ObJoinFilterOpInput::check_finish(bool &is_end, bool is_shared)
{
  int ret = OB_SUCCESS;
  uint64_t *count_ptr = reinterpret_cast<uint64_t *>(share_info_.unfinished_count_ptr_);
  if (OB_ISNULL(count_ptr) || !is_shared) {
    is_end = true;
  } else if (0 == ATOMIC_AAF(count_ptr, -1)) {
    is_end = true;
  } else {
    is_end = false;
  }
  return ret;
}

bool ObJoinFilterOpInput::check_release(bool is_shared)
{
  bool res = false;
  uint64_t *release_ref_ptr = reinterpret_cast<uint64_t *>(share_info_.release_ref_ptr_);
  if (OB_ISNULL(release_ref_ptr) || !is_shared) {
    res = true;
  } else if (0 == ATOMIC_AAF(release_ref_ptr, -1)) {
    res = true;
  } else {
    res = false;
  }
  return res;
}

int ObJoinFilterOpInput::init_share_info(common::ObIAllocator &allocator, int64_t task_count)
{
  int ret = OB_SUCCESS;
  uint64_t *send_count_ptr = NULL;
  uint64_t *close_count_ptr = NULL;
  uint64_t *count_ptr = NULL;
  if (OB_ISNULL(send_count_ptr = (uint64_t *)allocator.alloc(sizeof(uint64_t)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ptr", K(ret));
  } else if (OB_ISNULL(close_count_ptr = (uint64_t *)allocator.alloc(sizeof(uint64_t)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ptr", K(ret));
  } else if (OB_ISNULL(count_ptr = (uint64_t *)allocator.alloc(sizeof(uint64_t)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ptr", K(ret));
  } else {
    *send_count_ptr = task_count;
    *close_count_ptr = task_count;
    *count_ptr = task_count;
    share_info_.release_ref_ptr_ = reinterpret_cast<uint64_t>(close_count_ptr);
    share_info_.unfinished_count_ptr_ = reinterpret_cast<uint64_t>(send_count_ptr);
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObJoinFilterSpec, ObOpSpec),
                    mode_,
                    filter_id_,
                    server_id_,
                    filter_len_,
                    is_shuffle_,
                    join_keys_,
                    hash_funcs_,
                    filter_expr_id_,
                    filter_type_,
                    calc_tablet_id_expr_);

ObJoinFilterSpec::ObJoinFilterSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObOpSpec(alloc, type),
    mode_(NOT_INIT),
    filter_id_(OB_INVALID_ID),
    server_id_(OB_INVALID_ID),
    filter_len_(0),
    is_shuffle_(false),
    join_keys_(alloc),
    hash_funcs_(alloc),
    filter_expr_id_(OB_INVALID_ID),
    filter_type_(JoinFilterType::INVALID_TYPE),
    calc_tablet_id_expr_(NULL)
{

}

//------------------------------------------ ObJoinFilterOp --------------------------------
int ObJoinFilterOp::destroy_filter()
{
  int ret = OB_SUCCESS;
  ObPxBloomFilter *filter = NULL;
  if (OB_FAIL(ObPxBloomFilterManager::instance()
      .erase_px_bloom_filter(bf_key_, filter))) {
    LOG_TRACE("fail to erase sqc px bloom filter", K(ret));
  } else if (OB_NOT_NULL(filter)) {
    while (!filter->is_merge_filter_finish()) {
      // 一个繁琐但安全的等待行为.
      // 工作线程退出需要确保DTL已经没人持有filter指针在做写入.
      ob_usleep(1000);
    }
    filter->reset();
  }
  if (ret == OB_HASH_NOT_EXIST) {
    //尽力而为 如果manager里不存在的话 说明create端提前结束没有发送filter.
    ret = OB_SUCCESS;
  }
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
         MY_SPEC.filter_type_ != JoinFilterType::INVALID_TYPE;
}

ObJoinFilterOp::ObJoinFilterOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    bf_key_(),
    filter_use_(NULL),
    filter_create_(NULL),
    bf_ch_sets_(NULL),
    batch_hash_values_(NULL)
{
}

ObJoinFilterOp::~ObJoinFilterOp()
{
  destroy();
}

int ObJoinFilterOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObJoinFilterOpInput *op_input = static_cast<ObJoinFilterOpInput*>(input_);
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("builder operator is invalid", K(ret), K(MY_SPEC.filter_type_), K(MY_SPEC.mode_));
  } else if (MY_SPEC.is_create_mode()) {
    int64_t filter_len = MY_SPEC.filter_len_;
    if (!MY_SPEC.is_shared_join_filter() && OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
        &ctx_, MY_SPEC.px_est_size_factor_, filter_len, filter_len))) {
      LOG_WARN("failed to get px size", K(ret));
    } else if (op_input->is_local_create_ || !MY_SPEC.is_shared_join_filter()) {
      if (OB_FAIL(ObPxBloomFilterManager::init_px_bloom_filter(filter_len,
                                                               ctx_.get_allocator(),
                                                               filter_create_))) {
        LOG_WARN("fail to init px bloom filter", K(ret));
      } else {
        LOG_TRACE("join filter buffer length",
          K(filter_len),
          K(MY_SPEC.filter_len_));
      }
    } else if (OB_ISNULL(filter_create_ = reinterpret_cast<ObPxBloomFilter *>(
      op_input->share_info_.filter_ptr_))) {
      ret = OB_NOT_INIT;
      LOG_WARN("the bloom filter is not init", K(ret));
    }
    if (OB_SUCC(ret) && MY_SPEC.max_batch_size_ > 0) {
      if (OB_ISNULL(batch_hash_values_ =
              (uint64_t *)ctx_.get_allocator().alloc(sizeof(uint64_t) * MY_SPEC.max_batch_size_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc batch_hash_values_", K(ret), K(MY_SPEC.max_batch_size_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    bf_key_.init(ctx_.get_my_session()->get_effective_tenant_id(),
                 MY_SPEC.filter_id_,
                 MY_SPEC.server_id_,
                 op_input->get_px_sequence_id(),
                 op_input->task_id_);
    if (MY_SPEC.is_use_mode()) {
      //在ctx中创建expr ctx, 并初始化bloom filter key
      ObExprJoinFilter::ObExprJoinFilterContext *join_filter_ctx = NULL;
      if (OB_ISNULL(join_filter_ctx = static_cast<ObExprJoinFilter::ObExprJoinFilterContext *>(
            ctx_.get_expr_op_ctx(MY_SPEC.filter_expr_id_)))) {
        if (OB_FAIL(ctx_.create_expr_op_ctx(MY_SPEC.filter_expr_id_, join_filter_ctx))) {
          LOG_WARN("failed to create operator ctx", K(ret), K(MY_SPEC.filter_expr_id_));
        } else {
          join_filter_ctx->bf_key_ = bf_key_;
          int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
          bool wait_bloom_filter_ready = false;
          omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
          if (OB_LIKELY(tenant_config.is_valid())) {
            wait_bloom_filter_ready = tenant_config->_enable_px_bloom_filter_sync;
          }
          join_filter_ctx->need_wait_bf_ = wait_bloom_filter_ready;
          join_filter_ctx->window_size_ = ADAPTIVE_BF_WINDOW_ORG_SIZE;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join filter ctx is unexpected", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinFilterOp::do_create_filter_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_create_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter create is unexpected", K(ret));
  } else {
    filter_create_->reset_filter();
  }
  return ret;
}

int ObJoinFilterOp::do_use_filter_rescan()
{
  int ret = OB_SUCCESS;
  ObExprJoinFilter::ObExprJoinFilterContext *filter_expr_ctx = NULL;
  if (OB_ISNULL(filter_expr_ctx = static_cast<ObExprJoinFilter::ObExprJoinFilterContext *>(
        ctx_.get_expr_op_ctx(MY_SPEC.filter_expr_id_)))) {
    LOG_TRACE("join filter expr ctx is null");
  } else {
    filter_expr_ctx->reset_monitor_info();
  }
  return ret;
}

int ObJoinFilterOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.is_create_mode() && do_create_filter_rescan()) {
    LOG_WARN("fail to do create filter rescan", K(ret));
  } else if (MY_SPEC.is_use_mode() && do_use_filter_rescan()) {
    LOG_WARN("fail to do use filter rescan", K(ret));
  } else if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("operator rescan failed", K(ret));
  }
  return ret;
}

int ObJoinFilterOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObJoinFilterOpInput *filter_input_ = static_cast<ObJoinFilterOpInput*>(input_);
  while (OB_SUCC(ret)) {
    clear_evaluated_flag();
    ret = child_->get_next_row();
    if (OB_ITER_END == ret) {
      if (MY_SPEC.is_create_mode()) {
        bool all_is_finished = false;
        // sqc 下多个 worker 上的 filter 都完成了 child 迭代，
        // 说明本 sqc 上的 filter 数据已经收集完毕，可以执行发送。
        // 对于local filter计划, 将filter写入manager
        // 对于shuffle filter计划, 将filter信息写入exec_ctx,由recieve算子发送rpc.
        if (OB_FAIL(filter_input_->check_finish(all_is_finished, MY_SPEC.is_shared_join_filter()))) {
          LOG_WARN("fail to check all worker end", K(ret));
        } else if (all_is_finished && OB_FAIL(send_filter())) {
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
    ObExprJoinFilter::ObExprJoinFilterContext *filter_expr_ctx = NULL;
    if (OB_ISNULL(filter_expr_ctx = static_cast<ObExprJoinFilter::ObExprJoinFilterContext *>(
            ctx_.get_expr_op_ctx(MY_SPEC.filter_expr_id_)))) {
      /*do nothing*/
      LOG_TRACE("join filter expr ctx is null");
    } else {
      // 记录相关信息到sql plan monitor中
      op_monitor_info_.otherstat_1_value_ = filter_expr_ctx->filter_count_;
      op_monitor_info_.otherstat_2_value_ = filter_expr_ctx->total_count_;
      op_monitor_info_.otherstat_3_value_ = filter_expr_ctx->check_count_;
      op_monitor_info_.otherstat_4_value_ = filter_expr_ctx->ready_ts_;
      op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::JOIN_FILTER_FILTERED_COUNT;
      op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::JOIN_FILTER_TOTAL_COUNT;
      op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::JOIN_FILTER_CHECK_COUNT;
      op_monitor_info_.otherstat_4_id_ = ObSqlMonitorStatIds::JOIN_FILTER_READY_TIMESTAMP;
    }
  }
  return ret;
}

int ObJoinFilterOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  LOG_DEBUG("ObJoinFilterOp get_next_batch start");
  int ret = OB_SUCCESS;
  ObJoinFilterOpInput *filter_input_ = static_cast<ObJoinFilterOpInput*>(input_);
  // for batch
  int64_t batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
  const ObBatchRows *child_brs = nullptr;

  // By @yishen.tmd:
  // This circulation will have uses in the future although it is destined to break at the first time now.
  while (OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_batch(batch_cnt, child_brs))) {
      LOG_WARN("child_op failed to get next row", K(ret));
    }
    if (OB_SUCC(ret)) {
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
    }
  }
  if (OB_SUCC(ret)) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
    if (OB_FAIL(brs_.copy(child_brs))) {
      LOG_WARN("copy child_brs to brs_ failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && brs_.end_) {
    if (MY_SPEC.is_create_mode()) {
      bool all_is_finished = false;
      if (OB_FAIL(filter_input_->check_finish(all_is_finished, MY_SPEC.is_shared_join_filter()))) {
        LOG_WARN("fail to check all worker end", K(ret));
      } else if (all_is_finished && OB_FAIL(send_filter())) {
        LOG_WARN("fail to send bloom filter to use filter", K(ret));
      }
    } else if (MY_SPEC.is_use_mode()) {
      ObExprJoinFilter::ObExprJoinFilterContext *filter_expr_ctx = NULL;
      if (OB_ISNULL(filter_expr_ctx = static_cast<ObExprJoinFilter::ObExprJoinFilterContext *>(
              ctx_.get_expr_op_ctx(MY_SPEC.filter_expr_id_)))) {
        // do nothing
        LOG_TRACE("join filter expr ctx is null");
      } else {
        // for sql plan monitor
        op_monitor_info_.otherstat_1_value_ = filter_expr_ctx->filter_count_;
        op_monitor_info_.otherstat_2_value_ = filter_expr_ctx->total_count_;
        op_monitor_info_.otherstat_3_value_ = filter_expr_ctx->check_count_;
        op_monitor_info_.otherstat_4_value_ = filter_expr_ctx->ready_ts_;
        op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::JOIN_FILTER_FILTERED_COUNT;
        op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::JOIN_FILTER_TOTAL_COUNT;
        op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::JOIN_FILTER_CHECK_COUNT;
        op_monitor_info_.otherstat_4_id_ = ObSqlMonitorStatIds::JOIN_FILTER_READY_TIMESTAMP;
      }
    }
  }

  return ret;
}

int ObJoinFilterOp::send_filter()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.is_shuffle() && OB_FAIL(send_local_filter())) {
    LOG_WARN("fail to send local bloom filter", K(ret));
  } else if (MY_SPEC.is_shuffle() &&
    is_acceptable_filter() &&
    OB_FAIL(mark_rpc_filter())) {
    LOG_WARN("fail to send rpc bllom filter", K(ret));
  }
  return ret;
}

bool ObJoinFilterOp::is_acceptable_filter()
{
  bool ret = true;
  if (OB_NOT_NULL(filter_create_)) {
    int64_t bits_cnt = 0;
    int64_t total_cnt =0;
    int64_t i = 0;
    int64_t step = 1;
    int64_t *bits_array_ptr = filter_create_->get_bits_array();
    int64_t len = filter_create_->get_bits_array_length();
    if (len > 1000L * 1000)  {
      //64MB bits.
      step = round(((double)len) / 1000L * 1000);
    }
    if (OB_NOT_NULL(bits_array_ptr)) {
      while (i < len) {
        bits_cnt += ObBitVector::popcount64(bits_array_ptr[i]);
        total_cnt += (sizeof(int64_t) * 8);
        i += step;
      }
      double bits_rate = bits_cnt / (double)total_cnt;
      if (bits_rate > ACCEPTABLE_FILTER_RATE &&
          bits_rate <= 1) {
        ret = false;
      }
      LOG_TRACE("record join bloom filter bits rate", K(bits_rate), K(ret), K(bits_cnt), K(total_cnt), K(len));
    }
  }
  return ret;
}

int ObJoinFilterOp::send_local_filter()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_create_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter create is unexpected", K(ret));
  } else {
    filter_create_->px_bf_recieve_count_ = 1;
    filter_create_->px_bf_recieve_size_ = 1;
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(ObPxBloomFilterManager::instance().set_px_bloom_filter(bf_key_, filter_create_))) {
    LOG_WARN("fail to set px bloom filter in bloom filter manager", K(ret));
  }
  return ret;
}


/*
 * 发送rpc filter 有两种情况.
 *
 *         HASH JOIN(root dfo)
 *  filter_c    recieve(px)
 *  tsc          transmit
 *                filter_use
 *                  tsc
 * 对于以上计划filter create是没有sqc proxy的, 将由px算子去link channel
 *
 * 第二种情况如下:
 *
 *          p x
 *        HASH JOIN
 *   filter_c
 *  recieve    recieve
 * transmit     transmit
 *tsc            filter_use
 *                tsc
 * 对于这种计划需要join filter create算子获取channel, link channel并发送
 * */

int ObJoinFilterOp::mark_rpc_filter()
{
  int ret = OB_SUCCESS;
  ObPxBloomFilterData *filter_data = NULL;
  void *filter_ptr = NULL;
  common::ObIArray<ObJoinFilterDataCtx> &bf_ctx_array = ctx_.get_bloom_filter_ctx_array();
  if (OB_ISNULL(filter_ptr = ctx_.get_allocator().alloc(sizeof(ObPxBloomFilterData)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to init ObPxBloomFilterData", K(ret));
  } else if (OB_ISNULL(filter_data = new(filter_ptr) ObPxBloomFilterData())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to init ObPxBloomFilterData", K(ret));
  } else if (OB_FAIL(filter_data->filter_.init(filter_create_))) {
    LOG_WARN("fail to init filter data", K(ret));
  } else if (OB_FAIL(bf_ctx_array.push_back(ObJoinFilterDataCtx()))) {
    LOG_WARN("failed to push back bloom filter context", K(ret));
  } else {
    // don't need considerate concurrency control, cause px bloom filter will be ready one after other in execute plan
    ObJoinFilterDataCtx &bf_ctx = bf_ctx_array.at(bf_ctx_array.count() - 1);
    ObTenantConfigGuard tenant_config(TENANT_CONF(bf_key_.tenant_id_));
    if (tenant_config.is_valid() && true == tenant_config->_px_message_compression) {
      bf_ctx.compressor_type_ = ObCompressorType::LZ4_COMPRESSOR;
    }
    ObJoinFilterOpInput *filter_input_ = static_cast<ObJoinFilterOpInput*>(input_);
    filter_data->tenant_id_ = bf_key_.tenant_id_;
    filter_data->server_id_ = MY_SPEC.server_id_;
    filter_data->filter_id_ = MY_SPEC.filter_id_;
    filter_data->px_sequence_id_ = bf_key_.px_sequence_id_;
    filter_data->bloom_filter_count_ = 0;
    bf_ctx.filter_data_ = filter_data;
    bf_ctx.filter_ready_ = true;
    bf_ctx.ch_set_.reset();
    bf_ctx.ch_provider_ptr_ = filter_input_->share_info_.ch_provider_ptr_;
    bf_ctx.bf_idx_at_sqc_proxy_ = filter_input_->get_bf_idx_at_sqc_proxy();
    LOG_DEBUG("join filter succ to mark rpc filter", K(bf_ctx_array.count()), K(bf_ctx_array), K(filter_data->filter_id_), K(ret));
  }
  return ret;
}

int ObJoinFilterOp::calc_hash_value(uint64_t &hash_value)
{
  int ret = OB_SUCCESS;
  bool ignore = false;
  if (OB_FAIL(calc_hash_value(hash_value, ignore))) {
    LOG_WARN("fail to calc hash values", K(ret));
  } else if (ignore) {
    ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
    LOG_WARN("unexpected partition id", K(ret));
  }
  return ret;
}

int ObJoinFilterOp::calc_hash_value(uint64_t &hash_value, bool &ignore)
{
  int ret = OB_SUCCESS;
  hash_value = ObExprJoinFilter::JOIN_FILTER_SEED;
  ObDatum *datum = nullptr;
  ignore = false;
  if (MY_SPEC.is_partition_filter()) {
    int64_t partition_id = 0;
    if (OB_ISNULL(MY_SPEC.calc_tablet_id_expr_) || MY_SPEC.hash_funcs_.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected part id expr", K(ret));
    } else if (OB_FAIL(MY_SPEC.calc_tablet_id_expr_->eval(eval_ctx_, datum))) {
      LOG_WARN("failed to eval datum", K(ret));
    } else if (ObExprCalcPartitionId::NONE_PARTITION_ID == (partition_id = datum->get_int())) {
      ignore = true;
    } else {
      hash_value = MY_SPEC.hash_funcs_.at(0).hash_func_(*datum, hash_value);
    }
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < MY_SPEC.join_keys_.count() ; ++idx) {
      if (OB_FAIL(MY_SPEC.join_keys_.at(idx)->eval(eval_ctx_, datum))) {
        LOG_WARN("failed to eval datum", K(ret));
      } else {
        hash_value = MY_SPEC.hash_funcs_.at(idx).hash_func_(*datum, hash_value);
      }
    }
  }
  return ret;
}

int ObJoinFilterOp::insert_by_row()
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  bool ignore = false;
  if (OB_FAIL(calc_hash_value(hash_value, ignore))) {
    LOG_WARN("failed to calc hash value", K(ret));
  } else if (ignore) {
    /*do nothing*/
  } else if (OB_FAIL(filter_create_->put(hash_value))) {
    LOG_WARN("fail to put  hash value to px bloom filter", K(ret));
  }
  return ret;
}

int ObJoinFilterOp::insert_by_row_batch(const ObBatchRows *child_brs)
{
  int ret = OB_SUCCESS;
  if (child_brs->size_ > 0) {
    uint64_t seed = ObExprJoinFilter::JOIN_FILTER_SEED;
    if (MY_SPEC.is_partition_filter()) {
      if (OB_ISNULL(MY_SPEC.calc_tablet_id_expr_) || MY_SPEC.hash_funcs_.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected part id expr", K(ret));
      } else if (OB_FAIL(MY_SPEC.calc_tablet_id_expr_->eval_batch(eval_ctx_,
        *(child_brs->skip_), child_brs->size_))) {
        LOG_WARN("failed to eval", K(ret));
      } else {
        ObBatchDatumHashFunc hash_func_batch = MY_SPEC.hash_funcs_.at(0).batch_hash_func_;
        hash_func_batch(batch_hash_values_,
                        MY_SPEC.calc_tablet_id_expr_->locate_batch_datums(eval_ctx_),
                        MY_SPEC.calc_tablet_id_expr_->is_batch_result(),
                        *child_brs->skip_, child_brs->size_,
                        &seed,
                        false);
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.join_keys_.count(); ++i) {
        ObExpr *expr = MY_SPEC.join_keys_.at(i); // expr ptr check in cg, not check here
        if (OB_FAIL(expr->eval_batch(eval_ctx_, *(child_brs->skip_), child_brs->size_))) {
          LOG_WARN("eval failed", K(ret));
        } else {
          ObBatchDatumHashFunc hash_func_batch = MY_SPEC.hash_funcs_.at(i).batch_hash_func_;
          const bool is_batch_seed = (i > 0);
          hash_func_batch(batch_hash_values_,
                          expr->locate_batch_datums(eval_ctx_), expr->is_batch_result(),
                          *child_brs->skip_, child_brs->size_,
                          is_batch_seed ? batch_hash_values_ : &seed,
                          is_batch_seed);
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_brs->size_; ++i) {
      if (MY_SPEC.is_partition_filter()) {
        ObDatum &datum = MY_SPEC.calc_tablet_id_expr_->locate_expr_datum(eval_ctx_, i);
        if (ObExprCalcPartitionId::NONE_PARTITION_ID == datum.get_int()) {
          continue;
        }
      }
      if (OB_SUCC(ret)) {
        if (child_brs->skip_->at(i)) {
          continue;
        } else if (OB_FAIL(filter_create_->put(batch_hash_values_[i]))) {
          LOG_WARN("fail to put  hash value to px bloom filter", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJoinFilterOp::check_contain_row(bool &match)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter_use_)) {
    if (OB_FAIL(ObPxBloomFilterManager::instance().get_px_bloom_filter(bf_key_, filter_use_))) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(filter_use_)) {
    if (!filter_use_->check_ready()) {
      LOG_TRACE("px bloom filter is not ready, just by pass",
        K(filter_use_->px_bf_recieve_count_),
        K(filter_use_->px_bf_recieve_size_));
    } else {
      uint64_t hash_value = 0;
      if (OB_FAIL(calc_hash_value(hash_value))) {
        LOG_WARN("failed to calc hash value", K(ret));
      } else if (OB_FAIL(filter_use_->might_contain(hash_value, match))) {
        LOG_WARN("fail to check filter might contain value", K(ret));
      }
    }
  }
  return ret;
}

int ObJoinFilterOp::inner_close()
{
  int ret = OB_SUCCESS;
  ObJoinFilterOpInput *filter_input_ = static_cast<ObJoinFilterOpInput*>(input_);
  if (MY_SPEC.is_create_mode()) {
    // do nothing
  } else if (filter_input_->check_release(MY_SPEC.is_shared_join_filter()) &&
        OB_FAIL(destroy_filter())) {
    //当close引用计数为0时, 释放use端内存.
  } else {
    /*do nothing*/
  }
  return ret;
}
