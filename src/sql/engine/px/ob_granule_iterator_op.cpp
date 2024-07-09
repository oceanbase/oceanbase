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

#include "ob_granule_iterator_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_granule_pump.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/engine/dml/ob_table_insert_op.h"
#include "sql/engine/expr/ob_expr_join_filter.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_msg.h"
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_msg.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_part_mgr_util.h"


namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

ObGIOpInput::ObGIOpInput(ObExecContext &ctx, const ObOpSpec &spec)
  : ObOpInput(ctx, spec),
    parallelism_(-1),
    worker_id_(common::OB_INVALID_INDEX),
    pump_(nullptr),
    px_sequence_id_(OB_INVALID_ID),
    rf_max_wait_time_(0),
    deserialize_allocator_(nullptr)
{}

int ObGIOpInput::init(ObTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  UNUSED(task_info);
  //new parallel framework do not use this interface to set parameters
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "init gi input");
  LOG_WARN("the interface should not be used", K(ret));
  return ret;
}

void ObGIOpInput::set_deserialize_allocator(common::ObIAllocator *allocator)
{
  deserialize_allocator_ = allocator;
}

int ObGIOpInput::deep_copy_range(ObIAllocator *allocator, const ObNewRange &src, ObNewRange &dst)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_SUCC(src.start_key_.deep_copy(dst.start_key_, *allocator))
             && OB_SUCC(src.end_key_.deep_copy(dst.end_key_, *allocator))) {
    dst.table_id_ = src.table_id_;
    dst.border_flag_ = src.border_flag_;
  }
  return ret;
}

int ObGIOpInput::add_table_location_keys(common::ObIArray<const ObTableScanSpec*> &tscs)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < tscs.count() && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(tscs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tsc is null", K(ret));
    } else if (OB_FAIL(table_location_keys_.push_back(tscs.at(i)->get_table_loc_id()))) {
      LOG_WARN("fail to get table location key", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObGIOpInput)
{
  int ret = OK_;
  UNF_UNUSED_SER;
  BASE_SER(ObGIOpInput);
  LST_DO_CODE(OB_UNIS_ENCODE, parallelism_, worker_id_);
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(pos + sizeof(pump_) > buf_len)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("size overflow", K(pos), K(buf_len));
    } else {
      MEMCPY(buf + pos, &pump_, sizeof(pump_));
      pos += sizeof(pump_);
      //LOG_TRACE("muhang(pump)SE", K(pump_));
    }
  }
  OB_UNIS_ENCODE(table_location_keys_);
  return ret;
}

OB_DEF_DESERIALIZE(ObGIOpInput)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER(ObGIOpInput);
  LST_DO_CODE(OB_UNIS_DECODE, parallelism_, worker_id_);
  if (OB_SUCC(ret)) {
    const char *str = buf + pos;;
    MEMCPY(&pump_, str, sizeof(pump_));
    pos = pos + sizeof(pump_);
  }
  OB_UNIS_DECODE(table_location_keys_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObGIOpInput)
{
  int64_t len = 0;
  BASE_ADD_LEN(ObGIOpInput);
  LST_DO_CODE(OB_UNIS_ADD_LEN, parallelism_, worker_id_);
  len += sizeof(pump_);
  OB_UNIS_ADD_LEN(table_location_keys_);
  return len;
}

OB_SERIALIZE_MEMBER((ObGranuleIteratorSpec, ObOpSpec),
                    index_table_id_,
                    tablet_size_,
                    affinitize_,
                    partition_wise_join_,
                    access_all_,
                    nlj_with_param_down_,
                    gi_attri_flag_,
                    bf_info_,
                    hash_func_,
                    tablet_id_expr_,
                    pw_dml_tsc_ids_,
                    repart_pruning_tsc_idx_,
                    px_rf_info_);

ObGranuleIteratorSpec::ObGranuleIteratorSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
: ObOpSpec(alloc, type),
  index_table_id_(OB_INVALID_ID),
  tablet_size_(common::OB_DEFAULT_TABLET_SIZE),
  affinitize_(false),
  partition_wise_join_(false),
  access_all_(false),
  nlj_with_param_down_(false),
  pw_op_tscs_(alloc),
  pw_dml_tsc_ids_(alloc),
  gi_attri_flag_(0),
  dml_op_(NULL),
  bf_info_(),
  hash_func_(),
  tablet_id_expr_(NULL),
  repart_pruning_tsc_idx_(OB_INVALID_ID),
  px_rf_info_()
{}

ObGranuleIteratorOp::ObGranuleIteratorOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
: ObOperator(exec_ctx, spec, input),
  parallelism_(-1),
  worker_id_(-1),
  tsc_op_id_(OB_INVALID_ID),
  pump_(nullptr),
  pump_version_(0),
  state_(GI_UNINITIALIZED),
  all_task_fetched_(false),
  is_rescan_(false),
  rescan_taskset_(nullptr),
  rescan_task_idx_(0),
  pwj_rescan_task_infos_(),
  filter_count_(0),
  total_count_(0),
  rf_msg_(NULL),
  rf_key_(),
  rf_start_wait_time_(0),
  tablet2part_id_map_(),
  real_child_(NULL),
  is_parallel_runtime_filtered_(false),
  is_parallel_rf_qr_extracted_(false)
{
  op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::FILTERED_GRANULE_COUNT;
  op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::TOTAL_GRANULE_COUNT;
}

void ObGranuleIteratorOp::destroy()
{
  rescan_tasks_info_.destroy();
  pwj_rescan_task_infos_.reset();
  table_location_keys_.reset();
  pruning_partition_ids_.reset();
  tablet2part_id_map_.destroy();
}

int ObGranuleIteratorOp::parameters_init()
{
  int ret = OB_SUCCESS;
  ObGIOpInput *input = static_cast<ObGIOpInput*>(input_);
  if (nullptr != pump_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the context has been inited", K(ret));
  } else if (nullptr == input->pump_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the pump can not be null", K(ret));
  } else if (FALSE_IT(pump_ = input->pump_)){
  } else if (OB_FAIL(table_location_keys_.assign(input->table_location_keys_))) {
    LOG_WARN("fail to assgin table location keys", K(ret));
  } else {
    parallelism_ = input->parallelism_;
    worker_id_ = input->worker_id_;
    pump_version_ = pump_->get_pump_version();
  }
  LOG_DEBUG("GI ctx init", K(this), K(parallelism_), K(pump_), K(tsc_op_id_));
  return ret;
}

int ObGranuleIteratorOp::try_pruning_repart_partition(
    const ObGITaskSet &taskset,
    int64_t &pos,
    bool &partition_pruned)
{
  int ret = OB_SUCCESS;
  uint64_t tablet_id = OB_INVALID_ID;
  if (OB_INVALID_ID == ctx_.get_gi_pruning_info().get_part_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pruning id is not set", K(ret));
  } else if (OB_FAIL(taskset.get_task_tablet_id_at_pos(pos, tablet_id))) {
    LOG_WARN("get task info failed", K(ret));
  } else {
    partition_pruned = tablet_id != ctx_.get_gi_pruning_info().get_part_id();
  }
  return ret;
}

// 逻辑说明：
// 对于 NLJ rescan 右表场景，它分为两步：
// 1. 开始扫描之前，is_rescan_ = false， 会反复调用 try_fetch_task
//    把所有分区都填到 rescan_tasks_pos_ 里， 然后会设置 is_rescan_ = true
// 2. 开始扫描后，对于左边来的每一行，都会反复从 rescan_tasks_pos_ 选择合适的 task 来做
//    扫描。之所以引入 partition pruning 是为了处理 NLJ 右表是分区表场景
//    下，避免扫描无效分区。
int ObGranuleIteratorOp::try_fetch_task(ObGranuleTaskInfo &info)
{
  int ret = OB_SUCCESS;
  ObGranulePump *gi_task_pump = nullptr;
  const ObGITaskSet *taskset = NULL;
  int64_t pos = 0;
  //init
  if (is_not_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the gi ctx is not init", K(ret));
  } else if (nullptr == (gi_task_pump = pump_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the pump can not be null", K(ret));
  } else {
    if (is_rescan_) {
      ret = get_next_task_pos(pos, taskset);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(taskset->get_task_at_pos(info, pos))) {
          LOG_WARN("get task info failed", K(ret));
        } else {
          info.task_id_ = worker_id_;
        }
      }
    } else {
      const bool from_share_pool = !MY_SPEC.affinitize_ && !MY_SPEC.access_all_;
      if (OB_FAIL(gi_task_pump->fetch_granule_task(taskset,
                                                   pos,
                                                   from_share_pool ? 0: worker_id_,
                                                   tsc_op_id_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to fetch next granule task", K(ret),
                   K(gi_task_pump), K(worker_id_), K(MY_SPEC.affinitize_));
        } else {
          all_task_fetched_ = true;
        }
      } else if (NULL == taskset) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL taskset returned", K(ret));
      } else if (OB_FAIL(taskset->get_task_at_pos(info, pos))) {
        LOG_WARN("get task info failed", K(ret));
      } else if (FALSE_IT(info.task_id_ = worker_id_)) {
      } else if (OB_FAIL(rescan_tasks_info_.insert_rescan_task(pos, info))) {
        LOG_WARN("array push back failed", K(ret), K(info));
      } else {
        if (NULL == rescan_taskset_) {
          rescan_taskset_ = taskset;
        } else if (rescan_taskset_ != taskset) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("taskset changed", K(ret));
        }
      }
    }
  }
  return ret;
}
//GI has its own rescan

int ObGranuleIteratorOp::get_next_task_pos(int64_t &pos, const ObGITaskSet *&taskset)
{
  int ret = OB_SUCCESS;
  if (rescan_tasks_info_.use_opt_) {
    taskset = rescan_taskset_;
    if (rescan_task_idx_ > 0) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(rescan_tasks_info_.rescan_tasks_map_.get_refactored(
                ctx_.get_gi_pruning_info().get_part_id(), pos))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ITER_END;
      } else {
        LOG_WARN("get tablet task pos failed", K(ret));
      }
    } else {
      rescan_task_idx_++;
    }
  } else {
    bool partition_pruned = false;
    do {
      partition_pruned = false;
      if (rescan_task_idx_ >= rescan_tasks_info_.rescan_tasks_pos_.count()) {
        ret = OB_ITER_END;
      } else {
        taskset = rescan_taskset_;
        pos = rescan_tasks_info_.rescan_tasks_pos_.at(rescan_task_idx_++);
        if (ObGranuleUtil::enable_partition_pruning(MY_SPEC.gi_attri_flag_)
            && OB_FAIL(try_pruning_repart_partition(*taskset, pos, partition_pruned))) {
          LOG_WARN("fail try prune partition", K(ret));
        } else if (partition_pruned) {
          // next task
        }
      }
    } while (OB_SUCC(ret) && partition_pruned);
  }
  return ret;
}

int ObGranuleIteratorOp::pw_get_next_task_pos(const common::ObIArray<int64_t> &op_ids)
{
  int ret = OB_SUCCESS;
  if (rescan_tasks_info_.use_opt_) {
    if (rescan_task_idx_ > 0) {
      ret = OB_ITER_END;
      state_ = GI_END;
      all_task_fetched_ = true;
    } else if (OB_FAIL(rescan_tasks_info_.rescan_tasks_map_.get_refactored(
                        ctx_.get_gi_pruning_info().get_part_id(), rescan_task_idx_))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ITER_END;
      } else {
        LOG_WARN("get tablet task pos failed", K(ret), K(ctx_.get_gi_pruning_info().get_part_id()));
      }
    }
  } else {
    bool partition_pruned = false;
    int64_t repart_idx = MY_SPEC.repart_pruning_tsc_idx_;
    if (OB_UNLIKELY(repart_idx < 0 || repart_idx >= op_ids.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected repart pruning tsc index", K(ret), K(repart_idx));
    }
    do {
      if (rescan_task_idx_ >= pwj_rescan_task_infos_.count()) {
        ret = OB_ITER_END;
        state_ = GI_END;
        all_task_fetched_ = true;
      } else if (OB_UNLIKELY(rescan_task_idx_ + repart_idx >= pwj_rescan_task_infos_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected pwj_rescan_task_infos_ count", K(ret), K(rescan_task_idx_),
                K(repart_idx), K(pwj_rescan_task_infos_.count()), K(op_ids));
      } else {
        partition_pruned = repart_partition_pruned(pwj_rescan_task_infos_.at(rescan_task_idx_ + repart_idx));
        if (partition_pruned) {
          rescan_task_idx_ += op_ids.count();
        }
      }
    } while (OB_SUCC(ret) && partition_pruned);
  }
  return ret;
}

int ObGranuleIteratorOp::rescan()
{
  int ret = ObOperator::inner_rescan();
  CK(NULL != pump_);
  if (OB_FAIL(ret)) {
  } else if (pump_version_ != pump_->get_pump_version()) {
    // We can not reused the processed tasks when task regenerated (pump version changed).
    // e.g.: px batch rescan.
    LOG_TRACE("rescan after task changes");
    pump_version_ = pump_->get_pump_version();
    is_rescan_ = false;
    rescan_taskset_ = NULL;
    rescan_tasks_info_.reset();
    all_task_fetched_ = false;
    pwj_rescan_task_infos_.reset();
    pruning_partition_ids_.reset();
    while (OB_SUCC(get_next_granule_task())) {}
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get all granule task", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      is_rescan_ = true;
      rescan_task_idx_ = 0;
      state_ = GI_GET_NEXT_GRANULE_TASK;
    }
  } else {
    if (GI_UNINITIALIZED == state_) {
      // NJ call rescan before iterator rows, need to nothing for the first scan.
    } else if (GI_PREPARED == state_) {
      // At the open-stage we get a granule task, and now, we fetch all the granule task.
      while (OB_SUCC(get_next_granule_task())) {}
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to get all granule task", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      if (!all_task_fetched_) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "rescan before all task in gi");
        LOG_WARN("rescan before all task fetched", K(ret), K(state_));
      }
    }
    if (OB_SUCC(ret)) {
      pruning_partition_ids_.reset();
      rescan_task_idx_ = 0;
      state_ = GI_GET_NEXT_GRANULE_TASK;
      is_rescan_ = true;
      if (MY_SPEC.full_partition_wise()) {
        // do nothing.
      } else if (OB_ISNULL(real_child_)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (PHY_BLOCK_SAMPLE_SCAN == real_child_->get_spec().type_ ||
          PHY_ROW_SAMPLE_SCAN == real_child_->get_spec().type_) {
        OZ(const_cast<ObGranulePump *>(pump_)->reset_gi_task());
      }
    }
  }

#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif

  return ret;
}

int ObGranuleIteratorOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObOperator *real_child = nullptr;
  if (OB_FAIL(parameters_init())) {
    LOG_WARN("parameters init failed", K(ret));
  } else if (OB_FAIL(init_rescan_tasks_info())) {
    LOG_WARN("init rescan tasks info failed", K(ret));
  } else {
    if (!MY_SPEC.full_partition_wise()) {
      if (OB_FAIL(get_gi_task_consumer_node(this, real_child))) {
        LOG_WARN("Failed to get real child", K(ret));
      } else {
        // 如果是 partition wise的情况，就不需要 tsc op io
        // 因为 partition wise的情况下，获得GI task array是直接通过 `pw_op_tscs_` 数组类实现的
        tsc_op_id_ = real_child->get_spec().id_;
        real_child_ = real_child;
      }
    }
  }

  bool skip_prepare_table_scan = false;
  if (OB_SUCC(ret)) {
    if (MY_SPEC.px_rf_info_.is_inited_) {
      ObGIOpInput *input = static_cast<ObGIOpInput *>(input_);
      for (int64_t i = 0; i < MY_SPEC.px_rf_info_.p2p_dh_ids_.count() && OB_SUCC(ret); ++i) {
        ObP2PDhKey rf_key;
        rf_key.task_id_ = MY_SPEC.px_rf_info_.is_shared_ ? 0 : worker_id_;
        rf_key.px_sequence_id_ = input->px_sequence_id_;
        rf_key.p2p_datahub_id_ = MY_SPEC.px_rf_info_.p2p_dh_ids_.at(i);
        if (OB_FAIL(query_range_rf_keys_.push_back(rf_key))) {
          LOG_WARN("failed to push back");
        } else if (OB_FAIL(query_range_rf_msgs_.push_back(nullptr))) {
          LOG_WARN("failed to push back");
        }
      }
      if (OB_SUCC(ret)) {
        state_ = GI_PREPARED;
        skip_prepare_table_scan = true;
      }
      LOG_TRACE("runtime filter extract query range in GI", K(ret), K(query_range_rf_keys_));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(child_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("child_op is null", K(ret));
    } else if (MY_SPEC.bf_info_.is_inited_) {
      // prepare_table_scan can get a gi task during open stage,
      // but during open stage, partition runtime filter may not ready, we cannot sure wthether
      // do pruning or not at this moment, if we not do
      // so we skip the get task process, the all tasks can be pruning during open or rescan stage.
      ObGIOpInput *input = static_cast<ObGIOpInput*>(input_);
      rf_key_.task_id_ = MY_SPEC.bf_info_.is_shared_? 0 : worker_id_;
      rf_key_.px_sequence_id_ = input->px_sequence_id_;
      rf_key_.p2p_datahub_id_ = MY_SPEC.bf_info_.p2p_dh_id_;
      // when partition runtime filter pushdown to the right TSC child of NLJ,
      // we must set state_ = GI_PREPARED to get all tasks during rescan
      state_ = GI_PREPARED;
      skip_prepare_table_scan = true;
    } else if (!skip_prepare_table_scan && OB_FAIL(prepare_table_scan())) {
      LOG_WARN("prepare table scan failed", K(ret));
    }
  }
  return ret;
}

int ObGranuleIteratorOp::inner_close()
{
  if (OB_NOT_NULL(rf_msg_)) {
    // rf_msg_ is got from PX_P2P_DH map
    // do not destroy it, because other worker thread may not start yet
    rf_msg_->dec_ref_count();
  }
  for (int64_t i = 0; i < query_range_rf_keys_.count(); ++i) {
    // rf_msg_ is got from PX_P2P_DH map
    // do not destroy it, because other worker thread may not start yet
    ObP2PDatahubMsgBase *&rf_msg = query_range_rf_msgs_.at(i);
    if (OB_NOT_NULL(rf_msg)) {
      rf_msg->dec_ref_count();
    }
  }
  return OB_SUCCESS;
}

int ObGranuleIteratorOp::inner_get_next_row()
{
  const int64_t row_cnt = 1;
  return try_get_rows(row_cnt);
}

int ObGranuleIteratorOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = try_get_rows(std::min(max_row_cnt, MY_SPEC.max_batch_size_));
  if (OB_ITER_END == ret) {
    brs_.size_ = 0;
    brs_.end_ = true;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObGranuleIteratorOp::try_get_rows(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  bool got_next_row = false;
  do {
    clear_evaluated_flag();
    switch (state_) {
    case GI_UNINITIALIZED : {
      //try check the pump
      state_ = GI_GET_NEXT_GRANULE_TASK;
      break;
    }
    case GI_GET_NEXT_GRANULE_TASK : {
      if (OB_FAIL(get_next_granule_task())) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to get next granule task", K(ret));
        } else {
          op_monitor_info_.otherstat_1_value_ = filter_count_;
          op_monitor_info_.otherstat_2_value_ = total_count_;
        }
      }
    }
    break;
    case GI_PREPARED :
    case GI_TABLE_SCAN : {
      if (!is_vectorized()) {
        if (OB_FAIL(child_->get_next_row())) {
          LOG_DEBUG("failed to get new row", K(ret),
                    K(MY_SPEC.affinitize_), K(MY_SPEC.index_table_id_), K(worker_id_));
          if (OB_ITER_END != ret) {
            LOG_WARN("try fetch task failed", K(ret));
          } else {
            ret = OB_SUCCESS;
            state_ = GI_GET_NEXT_GRANULE_TASK;
          }
        } else {
          LOG_DEBUG("get new row", K(ret),
                    K(MY_SPEC.affinitize_), K(MY_SPEC.index_table_id_), K(worker_id_));
          got_next_row = true;
        }
      } else {
        const ObBatchRows *brs = NULL;
        if (OB_FAIL(child_->get_next_batch(max_row_cnt, brs))) {
          LOG_WARN("get next batch failed", K(ret));
        } else if (OB_FAIL(brs_.copy(brs))) {
          LOG_WARN("copy failed", K(ret));
        } else {
          LOG_DEBUG("get batch rows", K(*brs));
          if (brs->size_ > 0) {
            got_next_row = true;
          }
          if (brs->end_) {
            brs_.end_ = false;
            state_ = GI_GET_NEXT_GRANULE_TASK;
          }
        }
      }
      break;
    }
    case GI_END : {
      if (!is_vectorized()) {
        ret = OB_ITER_END;
        got_next_row = true;
      } else {
        brs_.size_ = 0;
        brs_.end_ = true;
        got_next_row = true;
      }
      break;
    }
    default : {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected state", K(ret), K(state_));
    }
    }
  } while(!(got_next_row || OB_FAIL(ret)));
  return ret;
}

int ObGranuleIteratorOp::get_next_granule_task(bool prepare /* = false */)
{
  int ret = OB_SUCCESS;
  bool partition_pruning = true;
  while (OB_SUCC(ret) && partition_pruning) {
    if (OB_FAIL(do_get_next_granule_task(partition_pruning))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to get all granule task", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if(!prepare && OB_FAIL(child_->rescan())) {
      /**
       * In inner open stage, the prepare is true.
       * Tsc will do scan in his inner open, so we should skip rescan action.
       */
      LOG_WARN("fail to rescan gi' child", K(ret));
    } else {
      state_ = GI_TABLE_SCAN;
    }
  }
  return ret;
}

/*
 *  this function will get a scan task from the task pump,
 *  and reset the table-scan operator below this operator.
 *  if the plan below this granule iterator is a partition wise-join plan,
 *  we will reset all table-scan operator
 *  in this plan.
 * */
int ObGranuleIteratorOp::do_get_next_granule_task(bool &partition_pruning)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObGranuleTaskInfo, 4> gi_task_infos;
  partition_pruning = false;
  // gi_prepare_map: 通过 GIPrepareTaskMap 跨越算子来传递 gi_task_info
  // 需要考虑到：当前 GI 下面可能存在多个 TSC 算子，GI 需要为他们每个人准备好 ObGranuleTaskInfo
  //             然后保存到 TaskMap 映射中： table_id => ObGranuleTaskInfo
  //             所以，下面的代码都是围绕着构建 GIPrepareTaskMap 来的
  GIPrepareTaskMap *gi_prepare_map = nullptr;
  if (OB_FAIL(ctx_.get_gi_task_map(gi_prepare_map))) {
    LOG_WARN("Failed to get gi task map", K(ret));
  } else if (!MY_SPEC.full_partition_wise()) {
    ObGranuleTaskInfo gi_task_info;
    /* non-partition wise join */

    // both parallel runtime filter extract query range and parallel runtime filter partition
    // pruning need to regenerate_gi_task, to reduce the regenerate time to 1, we judge if both
    //
    // let A = parallel_runtime_filter_extract_query_range, B = parallel_runtime_filter_pruning
    // if enable A but disable B, we do A and do regenerate_gi_task in A
    // if both enable A and B, we do A in B and do regenerate_gi_task in B
    // if disable A but enable B, we do B and do regenerate_gi_task in B
    if (!enable_parallel_runtime_filter_pruning()
        && enable_parallel_runtime_filter_extract_query_range()
        && OB_FAIL(do_parallel_runtime_filter_extract_query_range(true))) {
      LOG_WARN("failed to do do_parallel_runtime_filter_extract_query_range");
    } else if (enable_parallel_runtime_filter_pruning()
               && OB_FAIL(do_parallel_runtime_filter_pruning())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to do parallel runtime filter pruning", K(ret));
      } else {
        state_ = GI_END;
        all_task_fetched_ = true;
      }
    } else if (OB_FAIL(try_fetch_task(gi_task_info))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("try fetch task failed", K(ret));
      } else {
        state_ = GI_END;
        all_task_fetched_ = true;
      }
    } else {
      if (OB_NOT_NULL(gi_prepare_map->get(tsc_op_id_))) {
        // GI在向Map中塞任务的时候，需要尝试清理上一次塞入的任务
        if (OB_FAIL(gi_prepare_map->erase_refactored(tsc_op_id_))) {
          if (OB_HASH_NOT_EXIST != ret) {
            LOG_WARN("failed to erase task", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
      }
      LOG_DEBUG("produce a gi task", K(tsc_op_id_), K(gi_task_info));
    }
    if (OB_SUCC(ret)) {
      if (enable_single_runtime_filter_pruning() &&
          OB_FAIL(do_single_runtime_filter_pruning(gi_task_info, partition_pruning))) {
          LOG_WARN("fail to do join filter partition pruning", K(ret));
      } else if (!partition_pruning && OB_FAIL(do_dynamic_partition_pruning(gi_task_info, partition_pruning))) {
        LOG_WARN("fail to do dynamic partition pruning", K(ret));
      } else if (!partition_pruning) {
        if (enable_single_runtime_filter_extract_query_range()
            && OB_FAIL(do_single_runtime_filter_extract_query_range(gi_task_info))) {
          LOG_WARN("failed to do single runtime filter extract_query_range");
        } else if (OB_FAIL(gi_prepare_map->set_refactored(tsc_op_id_, gi_task_info))) {
          LOG_WARN("reset table scan's ranges failed", K(ret), K(child_->get_spec().id_),
                   K(gi_task_info));
        }
      }
    }
  } else {
    /* partition wise join */
    ObSEArray<int64_t, 4> op_ids;
    const ObIArray<int64_t> *op_ids_pointer = NULL;
    if (MY_SPEC.pw_dml_tsc_ids_.count() > 0) {
      op_ids_pointer = &(MY_SPEC.pw_dml_tsc_ids_);
    } else {
      op_ids_pointer = &op_ids;
      if (OB_NOT_NULL(MY_SPEC.dml_op_)) {
         //GI对INSERT表进行任务划分，获取对应的INSERT的op id
        if (OB_FAIL(op_ids.push_back(MY_SPEC.dml_op_->id_))) {
          LOG_WARN("failed to push back op ids", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        // GI对TSCs进行划分，获得对应的TSC的op id
        for (int i = 0 ; i < MY_SPEC.pw_op_tscs_.count() && OB_SUCC(ret); i++) {
          const ObTableScanSpec *tsc = MY_SPEC.pw_op_tscs_.at(i);
          if (OB_FAIL(op_ids.push_back(tsc->id_))) {
            LOG_WARN("failed to push back op ids", K(ret));
          }
        }
      }
    }
    // 获得gi tasks:
    // 每一个`op_id`都会对应一个`gi_task_info`
    if (OB_SUCC(ret)) {
      if (is_rescan_) {
        if (OB_FAIL(fetch_rescan_pw_task_infos(*op_ids_pointer, gi_prepare_map, gi_task_infos))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to fetch rescan pw task infos", K(ret));
          }
        }
      } else if (OB_FAIL(fetch_normal_pw_task_infos(*op_ids_pointer, gi_prepare_map, gi_task_infos))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to fetch normal pw task infos", K(ret));
        }
      }
    }

    // 动态分区裁剪
    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_dynamic_partition_pruning(gi_task_infos, partition_pruning))) {
        LOG_WARN("fail to do partition pruning", K(ret));
      }
    }
  }

  if (OB_LIKELY(GI_END != state_)) {
    total_count_++;
  }
  return ret;
}

int ObGranuleIteratorOp::wait_partition_runtime_filter_ready(bool &partition_pruning) {
  int ret = OB_SUCCESS;
  if (0 == rf_start_wait_time_) {
    rf_start_wait_time_ = ObTimeUtility::current_time();
  }
  ObGIOpInput *gi_input = static_cast<ObGIOpInput*>(input_);
  while (OB_SUCC(ret) && (OB_ISNULL(rf_msg_) || !rf_msg_->check_ready())) {
    if (OB_ISNULL(rf_msg_) && OB_FAIL(PX_P2P_DH.atomic_get_msg(rf_key_, rf_msg_))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get px bloom filter", K(ret), K(rf_key_));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ctx_.fast_check_status())) {
        LOG_WARN("fail to check status", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(rf_msg_) || !rf_msg_->check_ready()) {
        if (MY_SPEC.bf_info_.is_shuffle_) {
          // For shuffled msg, no waiting time is required.
          partition_pruning = false;
          break;
        } else {
          // For local messages, the maximum waiting time does not exceed rf_max_wait_time.
          int64_t cur_time = ObTimeUtility::current_time();
          if (cur_time - rf_start_wait_time_ > gi_input->get_rf_max_wait_time() * 1000) {
            partition_pruning = false;
            break;
          } else {
            ob_usleep(100);
          }
        }
      }
    }
  }
  return ret;
}

int ObGranuleIteratorOp::wait_runtime_filter_ready(ObP2PDhKey &rf_key,
                                                   ObP2PDatahubMsgBase *&rf_msg)
{
  int ret = OB_SUCCESS;
  if (0 == rf_start_wait_time_) {
    rf_start_wait_time_ = ObTimeUtility::current_time();
  }
  ObGIOpInput *gi_input = static_cast<ObGIOpInput *>(input_);
  while (OB_SUCC(ret) && (OB_ISNULL(rf_msg) || !rf_msg->check_ready())) {
    if (OB_ISNULL(rf_msg) && OB_FAIL(PX_P2P_DH.atomic_get_msg(rf_key, rf_msg))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get px bloom filter", K(ret), K(rf_key));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ctx_.fast_check_status())) {
        LOG_WARN("fail to check status", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(rf_msg) || !rf_msg->check_ready()) {
        int64_t cur_time = ObTimeUtility::current_time();
        if (cur_time - rf_start_wait_time_ > gi_input->get_rf_max_wait_time() * 1000) {
          break;
        } else {
          ob_usleep(100);
        }
      }
    }
  }
  return ret;
}

int ObGranuleIteratorOp::do_join_filter_partition_pruning(
    int64_t tablet_id,
    bool &partition_pruning)
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  if (OB_FAIL(wait_partition_runtime_filter_ready(partition_pruning))) {
    LOG_WARN("failed to wait partition runtime_filter ready");
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(rf_msg_) && rf_msg_->check_ready()) {
    uint64_t hash_val = ObExprJoinFilter::JOIN_FILTER_SEED;
    if (MY_SPEC.bf_info_.skip_subpart_) {
      int64_t part_id = OB_INVALID_ID;
      if (OB_FAIL(try_build_tablet2part_id_map())) {
        LOG_WARN("fail to build tablet2part id map", K(ret));
      } else if (OB_FAIL(tablet2part_id_map_.get_refactored(tablet_id, part_id))) {
        ret = OB_HASH_NOT_EXIST == ret ? OB_SCHEMA_ERROR : ret;
        LOG_WARN("fail to get refactored part id", K(ret), K(tablet_id), K(part_id));
      } else {
        tablet_id = part_id;
      }
    }
    if (OB_SUCC(ret) && !is_match) {
      ObDatum datum;
      datum.int_ = &tablet_id;
      datum.len_ = sizeof(tablet_id);
      ObRFBloomFilterMsg *bf_msg = static_cast<ObRFBloomFilterMsg *>(rf_msg_);
      if (OB_FAIL(MY_SPEC.hash_func_.hash_func_(datum, hash_val, hash_val))) {
        LOG_WARN("fail to calc hash value", K(ret));
      } else if (OB_FAIL(bf_msg->bloom_filter_.might_contain(hash_val, is_match))) {
        LOG_WARN("fail to check filter might contain value", K(ret), K(hash_val));
      } else {
        partition_pruning = !is_match;
        if (!is_match) {
          LOG_DEBUG("GI task is filtered by partition bloom filter", K(tablet_id), K(MY_SPEC.bf_info_.skip_subpart_));
          filter_count_++;
        }
      }
    }
  }
  return ret;
}
/*
 * this interface is used to reassign scan tasks in the partition wise-join plan,
 * which is below this granule iterator.
 *
 * */
int ObGranuleIteratorOp::fetch_full_pw_tasks(
  ObIArray<ObGranuleTaskInfo> &infos, const ObIArray<int64_t> &op_ids)
{
  int ret = OB_SUCCESS;
  ObGranulePump *gi_task_pump = nullptr;
  //init
  if (is_not_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the gi ctx is not init", K(ret));
  } else if (nullptr == (gi_task_pump = pump_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the pump can not be null", K(ret));
  } else if (OB_FAIL(gi_task_pump->try_fetch_pwj_tasks(infos, op_ids, worker_id_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next granule task", K(ret), K(gi_task_pump));
    }
  } else if (op_ids.count() != infos.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the tsc does not match task count", K(ret), K(op_ids.count()),
        K(infos.count()));
  }
  return ret;
}

/*
 * At the moment we open this operator, we try to get granule task and give
 * it to the tsc operator below. So, after we open this operator, the tsc is
 * ready for reading.
 *
 * */
int ObGranuleIteratorOp::prepare_table_scan()
{
  int ret = OB_SUCCESS;
  if (!is_not_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected gi state", K(ret), K(state_));
  } else if (FALSE_IT(state_ = GI_GET_NEXT_GRANULE_TASK)) {
  } else if (OB_FAIL(get_next_granule_task(true /* prepare */))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next granule task", K(ret));
    } else {
      // we can not return OB_ITER_END, because in open-stage,
      // OB_ITER_END is regard as error.
      ret = OB_SUCCESS;
      // get there mean the state == GI_END
    }
  } else {
    //before state_ == GI_TABLE_SCAN
    state_ = GI_PREPARED;
  }
  LOG_DEBUG("do prepare table scan", K(ret), K(state_));
  return ret;
}

int ObGranuleIteratorOp::set_tscs(ObIArray<const ObTableScanSpec *> &tscs)
{
  int ret = OB_SUCCESS;
  //重复调用是安全的.
  //如果旧的count比新旧的大，会报错误OB_SIZE_OVERFLOW.否则维持旧的大小.
  ObGranuleIteratorSpec *gi_spec = static_cast<ObGranuleIteratorSpec*>(const_cast<ObOpSpec*>(&spec_));
  if (OB_FAIL(gi_spec->pw_op_tscs_.prepare_allocate(tscs.count()))) {
    LOG_WARN("Failed to init fixed array", K(ret));
  };
  ARRAY_FOREACH_X(tscs, idx, cnt, OB_SUCC(ret)) {
    gi_spec->pw_op_tscs_.at(idx) = tscs.at(idx);
  }
  LOG_DEBUG("Set table scan to GI", K(tscs), K(ret));
  return ret;
}
int ObGranuleIteratorOp::set_dml_op(const ObTableModifySpec *dml_op)
{
  const_cast<ObGranuleIteratorSpec &>(MY_SPEC).dml_op_ = const_cast<ObTableModifySpec *>(dml_op);
  return OB_SUCCESS;
}

// NOTE: this function is only used for the GI which only control one scan operator.
// Think about the following case, the GI attempt to control the right tsc op rather than
// the values table op(or maybe json table op), thus we need to traverse the OP tree to find the
// real consumer node(tsc op).
//          PX GI
//            |
//           GBY
//            |
//          SORT
//            |
//           HJ
//        /       \
//    values      tsc
//    table
//
int ObGranuleIteratorOp::get_gi_task_consumer_node(ObOperator *cur,
                                                   ObOperator *&consumer) const
{
  int ret = OB_SUCCESS;
  int64_t child_cnt = cur->get_child_cnt();
  if (0 == child_cnt) {
    if (PHY_TABLE_SCAN == cur->get_spec().type_
        || PHY_BLOCK_SAMPLE_SCAN == cur->get_spec().type_
        || PHY_ROW_SAMPLE_SCAN == cur->get_spec().type_) {
      consumer = cur;
      LOG_TRACE("find the gi_task consumer node", K(cur->get_spec().id_),
                K(cur->get_spec().type_));
    }
  } else {
    ObOperator *child = nullptr;
    for (int64_t i = 0; i < child_cnt && OB_SUCC(ret) && OB_ISNULL(consumer); ++i) {
      if (OB_ISNULL(child = cur->get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child is null", K(ret));
      } else if (OB_FAIL(get_gi_task_consumer_node(child, consumer))) {
        LOG_WARN("failed to get gi task consumer node", K(ret));
      }
    }
  }

  // cur == this means only check the output stack
  if (OB_SUCC(ret) && OB_ISNULL(consumer) && cur == this) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can't find the tsc phy op", K(ret));
  }
  return ret;
}

int ObGranuleIteratorOp::do_dynamic_partition_pruning(const ObGranuleTaskInfo &gi_task_info, bool &partition_pruning)
{
  int ret = OB_SUCCESS;
  partition_pruning = false;
  if (OB_ISNULL(pump_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pump is null", K(ret));
  } else if (pump_->need_partition_pruning()) {
    int64_t partition_id =  gi_task_info.tablet_loc_->tablet_id_.id();
    if (pruning_partition_ids_.empty()) {
      uint64_t table_location_key = OB_INVALID_ID;
      if (pump_->get_pruning_table_location()->empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table location key", K(ret));
      } else {
        common::ObIArray<ObTableLocation> *locations = pump_->get_pruning_table_location();
        for (int i = 0; i < table_location_keys_.count() && OB_SUCC(ret) && !partition_pruning; ++i) {
          table_location_key = table_location_keys_.at(i);
          for (int j = 0; j < locations->count() && OB_SUCC(ret) && !partition_pruning; ++j) {
            if (table_location_key == locations->at(j).get_table_id()) {
              OZ(locations->at(j).pruning_single_partition(partition_id, ctx_,
                  partition_pruning, pruning_partition_ids_));
            }
          }
        }
      }
    } else {
      partition_pruning = true;
      for (int i = 0; i < pruning_partition_ids_.count() && OB_SUCC(ret); ++i) {
        if (pruning_partition_ids_.at(i) == partition_id) {
          partition_pruning = false;
          break;
        }
      }
    }
  }
  return ret;
}

int ObGranuleIteratorOp::do_dynamic_partition_pruning(const common::ObIArray<ObGranuleTaskInfo> &gi_task_infos,
      bool &partition_pruning)
{
  int ret = OB_SUCCESS;
  partition_pruning = false;
  if (OB_ISNULL(pump_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pump is null", K(ret));
  } else if (pump_->need_partition_pruning()) {
    int64_t partition_id = OB_INVALID_ID;
    uint64_t table_location_key = OB_INVALID_ID;
    if (table_location_keys_.count() != gi_task_infos.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("count of table location is unexpected",
          K(gi_task_infos.count()), K(table_location_keys_.count()));
    } else {
      /*
       * In the previous dynamic partition pruning logic
       * and in the partition wise scenario
       * as long as one partition satisfies to be trimmed in each pair of
       * partitions of GI iteration, this pair of partitions will be trimmed.
       * In the union all scenario,
       * There is no logical correspondence between the pair of partitions
       * so it is necessary to calculate whether all partitions can be cropped.
       * bug:
       * */
      bool single_table_partition_pruning = false;
      for (int i = 0; i < gi_task_infos.count() && OB_SUCC(ret); ++i) {
        partition_id =  gi_task_infos.at(i).tablet_loc_->tablet_id_.id();
        single_table_partition_pruning = false;
        if (pump_->get_pruning_table_location()->empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected table location key", K(ret));
        } else {
          table_location_key = table_location_keys_.at(i);
          common::ObIArray<ObTableLocation> *locations = pump_->get_pruning_table_location();
          for (int j = 0; j < locations->count()  && OB_SUCC(ret); ++j) {
            if (table_location_key == locations->at(j).get_table_id()) {
              OZ(locations->at(j).pruning_single_partition(partition_id, ctx_,
                  single_table_partition_pruning, pruning_partition_ids_));
            }
          }
          if (OB_SUCC(ret)) {
            if (!single_table_partition_pruning) {
              partition_pruning = false;
              break;
            } else {
              partition_pruning = true;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObGranuleIteratorOp::fetch_rescan_pw_task_infos(const common::ObIArray<int64_t> &op_ids,
    GIPrepareTaskMap *gi_prepare_map,
    common::ObIArray<ObGranuleTaskInfo> &gi_task_infos)
{
  int ret = OB_SUCCESS;
  if (op_ids.empty() || !gi_task_infos.empty() || OB_ISNULL(gi_prepare_map)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(op_ids.count()), K(gi_task_infos.count()));
  } else if (rescan_task_idx_ >= pwj_rescan_task_infos_.count()) {
    ret = OB_ITER_END;
    state_ = GI_END;
    all_task_fetched_ = true;
  } else {
    if (ObGranuleUtil::enable_partition_pruning(MY_SPEC.gi_attri_flag_)) {
      if (OB_FAIL(pw_get_next_task_pos(op_ids))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("pw get next task pos failed", K(ret));
        }
      }
    }
    ARRAY_FOREACH_X(op_ids, idx, cnt, OB_SUCC(ret)) {
      // GI在向Map中塞任务的时候，需要尝试清理上一次塞入的任务
      if (OB_NOT_NULL(gi_prepare_map->get(op_ids.at(idx)))) {
        if (OB_FAIL(gi_prepare_map->erase_refactored(op_ids.at(idx)))) {
          LOG_WARN("failed to erase task", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (rescan_task_idx_ >= pwj_rescan_task_infos_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rescan task idx is unexpected", K(rescan_task_idx_),
              K(pwj_rescan_task_infos_.count()), K(cnt));
        } else if (OB_FAIL(gi_task_infos.push_back(pwj_rescan_task_infos_.at(rescan_task_idx_)))) {
          LOG_WARN("fail to push back gi task infos", K(ret));
        } else if (OB_FAIL(gi_prepare_map->set_refactored(op_ids.at(idx),
              pwj_rescan_task_infos_.at(rescan_task_idx_)))) {
          LOG_WARN("reset table scan's ranges failed", K(ret));
        } else {
          LOG_DEBUG("produce a gi task(PWJ)", K(op_ids.at(idx)),
            K(pwj_rescan_task_infos_.at(rescan_task_idx_)));
          rescan_task_idx_++;
        }
      }
    }
  }
  return ret;
}

int ObGranuleIteratorOp::fetch_normal_pw_task_infos(const common::ObIArray<int64_t> &op_ids,
    GIPrepareTaskMap *gi_prepare_map,
    common::ObIArray<ObGranuleTaskInfo> &gi_task_infos)
{
  int ret = OB_SUCCESS;
  if (op_ids.empty() || !gi_task_infos.empty() || OB_ISNULL(gi_prepare_map)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(get_spec().id_), K(op_ids.count()), K(gi_task_infos.count()));
  } else if (OB_FAIL(fetch_full_pw_tasks(gi_task_infos, op_ids))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("try fetch task failed", K(ret));
    } else {
      state_ = GI_END;
      all_task_fetched_ = true;
    }
  } else {
    if (rescan_tasks_info_.use_opt_) {
      int64_t repart_idx = MY_SPEC.repart_pruning_tsc_idx_;
      if (OB_UNLIKELY(repart_idx >= gi_task_infos.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected gi task infos count", K(repart_idx), K(gi_task_infos.count()),
                 K(gi_task_infos));
      } else if (OB_FAIL(rescan_tasks_info_.rescan_tasks_map_.set_refactored(
                  gi_task_infos.at(repart_idx).tablet_loc_->tablet_id_.id(), pwj_rescan_task_infos_.count()))) {
        LOG_WARN("set refactored failed", K(ret), KPC(gi_task_infos.at(repart_idx).tablet_loc_));
      } else {
        LOG_TRACE("set pw rescan task pos", K(spec_.id_), K(repart_idx), K(pwj_rescan_task_infos_.count()),
                  K(gi_task_infos.at(repart_idx).tablet_loc_->tablet_id_));
      }
    }
    for (int i = 0; i < gi_task_infos.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(pwj_rescan_task_infos_.push_back(gi_task_infos.at(i)))) {
        LOG_WARN("fail to rescan pwj task info", K(ret));
      }
    }
    ARRAY_FOREACH_X(op_ids, idx, cnt, OB_SUCC(ret)) {
      // GI在向Map中塞任务的时候，需要尝试清理上一次塞入的任务
      if (OB_NOT_NULL(gi_prepare_map->get(op_ids.at(idx)))) {
        if (OB_FAIL(gi_prepare_map->erase_refactored(op_ids.at(idx)))) {
          LOG_WARN("failed to erase task", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(gi_prepare_map->set_refactored(op_ids.at(idx), gi_task_infos.at(idx)))) {
        LOG_WARN("reset table scan's ranges failed", K(ret));
      } else {
        LOG_DEBUG("produce a gi task(PWJ)", K(op_ids.at(idx)), K(gi_task_infos.at(idx)));
      }
    }
  }
  return ret;
}

int ObGranuleIteratorOp::try_build_tablet2part_id_map()
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == MY_SPEC.index_table_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("loc is unexpected", K(ret));
  } else if (tablet2part_id_map_.created()) {
    /*do nothing*/
  } else if (OB_ISNULL(ctx_.get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  } else {
    const ObTableSchema *table_schema = NULL;
    int64_t index_table_id = MY_SPEC.index_table_id_;
    if (OB_FAIL(ctx_.get_sql_ctx()->schema_guard_->get_table_schema(
        MTL_ID(), index_table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("null table schema", K(MTL_ID()), K(index_table_id));
    } else if (PARTITION_LEVEL_TWO != table_schema->get_part_level()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected part level", K(ret));
    } else if (OB_FAIL(tablet2part_id_map_.create(max(1, table_schema->get_all_part_num()),
                                                  "GITabletMap",
                                                  ObModIds::OB_HASH_NODE,
                                                  MTL_ID()))) {
      LOG_WARN("fail create hashmap", K(ret));
    } else {
      ObPartitionSchemaIter iter(*table_schema, CHECK_PARTITION_MODE_NORMAL);
      ObPartitionSchemaIter::Info info;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter.next_partition_info(info))) {
          if (OB_ITER_END != ret) {
          LOG_WARN("fail get next partition item from iterator", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_ISNULL(info.part_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected part", K(ret));
        } else if (OB_FAIL(tablet2part_id_map_.set_refactored(info.tablet_id_.id(),
            info.part_->get_object_id()))) {
          LOG_WARN("fail to set tablet part id map", K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObGranuleIteratorOp::enable_parallel_runtime_filter_extract_query_range()
{
  return !is_parallel_rf_qr_extracted_ && MY_SPEC.px_rf_info_.is_inited_
         && !ObGranuleUtil::is_partition_task_mode(MY_SPEC.gi_attri_flag_);
}

bool ObGranuleIteratorOp::enable_single_runtime_filter_extract_query_range()
{
  // if right table's gi is partition gi, the range in gi_task can be directly coverd
  return MY_SPEC.px_rf_info_.is_inited_
         && ObGranuleUtil::is_partition_task_mode(MY_SPEC.gi_attri_flag_);
}

bool ObGranuleIteratorOp::enable_parallel_runtime_filter_pruning()
{
  return !is_parallel_runtime_filtered_ && MY_SPEC.bf_info_.is_inited_ &&
         !ObGranuleUtil::is_partition_task_mode(MY_SPEC.gi_attri_flag_);
}

bool ObGranuleIteratorOp::enable_single_runtime_filter_pruning()
{
  return MY_SPEC.bf_info_.is_inited_ && ObGranuleUtil::is_partition_task_mode(MY_SPEC.gi_attri_flag_);
}

int ObGranuleIteratorOp::do_single_runtime_filter_pruning(
    const ObGranuleTaskInfo &gi_task_info, bool &partition_pruning)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gi_task_info.tablet_loc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet loc", K(ret));
  } else {
    int64_t tablet_id = gi_task_info.tablet_loc_->tablet_id_.id();
    if (OB_FAIL(do_join_filter_partition_pruning(tablet_id, partition_pruning))) {
      LOG_WARN("fail to do join filter partition pruning", K(ret));
    }
  }
  return ret;
}

int ObGranuleIteratorOp::do_parallel_runtime_filter_pruning()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pump_) || 1 != pump_->get_pump_args().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pump is unexpected", K(pump_), K(ret));
  } else {
    ObGranulePumpArgs &args = pump_->get_pump_args().at(0);
    int64_t cur_tablet_idx = -1;
    int64_t finish_tablet_idx = -1;
    int64_t tablet_cnt = args.run_time_pruning_flags_.count();
    bool is_pruning = false;
    CK(0 != tablet_cnt);
    // 1 Firstly, all threads(not all is also ok)
    //   come to do the parallel pruning together via aotimic operations
    // 2 The last worker to finish pruning will be responsible for regenerating the gi task
    // 3 Other threads need to sleep to wait to be woken up
    while (OB_SUCC(ret) && args.cur_tablet_idx_ < tablet_cnt) {
      // Concurrency controls the tablet idx of the current operation
      cur_tablet_idx = ATOMIC_FAA(&args.cur_tablet_idx_, 1);
      if (cur_tablet_idx >= tablet_cnt) {
      } else {
        DASTabletLocArray &tablet_array = args.tablet_arrays_.at(0);
        is_pruning = false;
        if (OB_FAIL(do_join_filter_partition_pruning(
            tablet_array.at(cur_tablet_idx)->tablet_id_.id(), is_pruning))) {
          LOG_WARN("fail to do join filter partition pruning", K(ret));
        } else if (is_pruning) {
          args.run_time_pruning_flags_.at(cur_tablet_idx) = true;
        }
        // Record the current finish_tablet_idx
        // To find who is the last finished worker
        finish_tablet_idx = ATOMIC_AAF(&args.finish_pruning_tablet_idx_, 1);
      }
    }
    if (OB_SUCC(ret)) {
      is_parallel_runtime_filtered_ = true;
      if (finish_tablet_idx == tablet_cnt) {
        DASTabletLocArray pruning_remain_tablets;
        DASTabletLocArray &tablet_array = args.tablet_arrays_.at(0);
        for (int i = 0; OB_SUCC(ret) && i < tablet_array.count(); ++i) {
          if (!args.run_time_pruning_flags_.at(i)) {
            OZ(pruning_remain_tablets.push_back(tablet_array.at(i)));
          }
        }
        if (OB_SUCC(ret)) {
          if (pruning_remain_tablets.empty()) {
            ret = OB_ITER_END;
            args.sharing_iter_end_ = true;
            // all partition be pruned, no need to extract query range again
            is_parallel_rf_qr_extracted_ = true;
          } else if (pruning_remain_tablets.count() == tablet_array.count()) {
            // if no partition is pruned, still need to check whether can extract query range.
            // regenerate_gi_task in do_parallel_runtime_filter_extract_query_range
            if (OB_SUCC(ret) && enable_parallel_runtime_filter_extract_query_range()) {
              OZ(do_parallel_runtime_filter_extract_query_range(true));
            }
          } else {
            args.tablet_arrays_.reset();
            OZ(args.tablet_arrays_.push_back(pruning_remain_tablets));
            if (OB_SUCC(ret) && enable_parallel_runtime_filter_extract_query_range()) {
              // don't do regenerate_gi_task in do_parallel_runtime_filter_extract_query_range
              OZ(do_parallel_runtime_filter_extract_query_range(false));
            }
            OZ(pump_->regenerate_gi_task());
          }
        }
        args.set_pruning_ret(ret);
        args.set_finish_pruning();
      } else {
        while (OB_SUCC(ret) && !args.is_finish_pruning()) {
          if (OB_FAIL(ctx_.fast_check_status())) {
            LOG_WARN("fail to fast check status", K(ret));
          } else {
            ob_usleep(100);
          }
        }
        if (OB_SUCC(ret) && enable_parallel_runtime_filter_extract_query_range()) {
          // already extract by the lucky one thread(i.e. the thread who regenerating the gi task)
          is_parallel_rf_qr_extracted_ = true;
        }
        if (OB_SUCC(ret)
           && (args.sharing_iter_end_ || OB_UNLIKELY(OB_SUCCESS != args.get_pruning_ret()))) {
          ret = OB_ITER_END;
        }
      }
    }
  }
  return ret;
}

int ObGranuleIteratorOp::do_single_runtime_filter_extract_query_range(
    ObGranuleTaskInfo &gi_task_info)
{
  int ret = OB_SUCCESS;
  bool has_extrct = false;
  ObIArray<ObNewRange> &ranges = gi_task_info.ranges_;
  ObIArray<ObNewRange> &ss_ranges = gi_task_info.ss_ranges_;
  if (gi_task_info.ranges_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ranges_ is empty ", K(ret));
  } else {
    for (int64_t i = 0; i < query_range_rf_keys_.count() && OB_SUCC(ret) && !has_extrct; ++i) {
      ObP2PDatahubMsgBase *&rf_msg = query_range_rf_msgs_.at(i);
      ObP2PDhKey &rf_key = query_range_rf_keys_.at(i);
      if (OB_FAIL(wait_runtime_filter_ready(rf_key, rf_msg))) {
        LOG_WARN("failed to wait wait_runtime_filter_ready");
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(rf_msg) && rf_msg->check_ready()) {
        if (OB_FAIL(rf_msg->try_extract_query_range(has_extrct, ranges))) {
          LOG_WARN("failed to try_extract_query_range");
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (has_extrct) {
        ss_ranges.reset();
        if (ranges.empty()) {
          // do nothing
        } else {
          ObNewRange whole_range;
          const ObNewRange &key_range = ranges.at(0);
          whole_range.set_whole_range();
          whole_range.table_id_ = key_range.table_id_;
          whole_range.flag_ = key_range.flag_;
          for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
            if (OB_FAIL(ss_ranges.push_back(whole_range))) {
              LOG_WARN("push back ss_ranges failed", K(ret));
            }
          }
        }
      }
    }
    LOG_TRACE("single runtime filter extract query range", K(ret), K(has_extrct), K(ranges));
  }
  return ret;
}

int ObGranuleIteratorOp::do_parallel_runtime_filter_extract_query_range(
    bool need_regenerate_gi_task)
{
  int ret = OB_SUCCESS;
  bool has_extrct = false;
  if (OB_ISNULL(pump_) || 1 != pump_->get_pump_args().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pump is unexpected", K(pump_), K(ret));
  } else {
    ObGranulePumpArgs &args = pump_->get_pump_args().at(0);
    ObIArray<ObNewRange> &ranges = args.query_range_by_runtime_filter_;

    bool is_lucky_one = ATOMIC_CAS(&args.lucky_one_, true, false);
    if (is_lucky_one) {
      for (int64_t i = 0; i < query_range_rf_keys_.count() && OB_SUCC(ret) && !has_extrct; ++i) {
        ObP2PDatahubMsgBase *&rf_msg = query_range_rf_msgs_.at(i);
        ObP2PDhKey &rf_key = query_range_rf_keys_.at(i);
        if (OB_FAIL(wait_runtime_filter_ready(rf_key, rf_msg))) {
          LOG_WARN("failed to wait wait_runtime_filter_ready");
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(rf_msg) && rf_msg->check_ready()) {
          if (OB_FAIL(rf_msg->try_extract_query_range(has_extrct, ranges))) {
            LOG_WARN("failed to try_extract_query_range", K(ranges));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (has_extrct) {
        if (need_regenerate_gi_task) {
          OZ(pump_->regenerate_gi_task());
        }
      }
      LOG_TRACE("parallel runtime filter extract query range", K(ret), K(has_extrct), K(ranges));
      args.extract_finished_ = true;
    } else {
      while (!args.extract_finished_ && OB_SUCC(ret)) {
        if (OB_FAIL(ctx_.fast_check_status())) {
          LOG_WARN("fail to fast check status", K(ret));
        } else {
          ob_usleep(100);
        }
      }
    }
  }
  is_parallel_rf_qr_extracted_ = true;
  return ret;
}

int ObGranuleIteratorOp::RescanTasksInfo::insert_rescan_task(int64_t pos, const ObGranuleTaskInfo &info)
{
  int ret = OB_SUCCESS;
  if (use_opt_) {
    ret = rescan_tasks_map_.set_refactored(info.tablet_loc_->tablet_id_.id(), pos);
  } else {
    ret = rescan_tasks_pos_.push_back(pos);
  }
  return ret;
}

int ObGranuleIteratorOp::init_rescan_tasks_info()
{
  int ret = OB_SUCCESS;
  rescan_tasks_info_.use_opt_ = ObGranuleUtil::enable_partition_pruning(MY_SPEC.gi_attri_flag_);
  if (!rescan_tasks_info_.use_opt_) {
  } else if (OB_ISNULL(pump_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pump_", K(ret), K(spec_.id_));
  } else if (OB_UNLIKELY(parallelism_ <= 0 || pump_->get_pump_args().count() < 1
                         || pump_->get_pump_args().at(0).tablet_arrays_.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected argument", K(parallelism_), K(pump_->get_pump_args()));
  } else {
    int64_t tablet_cnt = pump_->get_pump_args().at(0).tablet_arrays_.count();
    if (tablet_cnt < parallelism_) {
      // no use optimization if parallelism_ is greater than tablet count.
      rescan_tasks_info_.use_opt_ = false;
    } else {
      const ObMemAttr attr(MTL_ID(), "GIRescanTaskMap");
      int64_t bucket_num = tablet_cnt / parallelism_ * 2;
      if (OB_FAIL(rescan_tasks_info_.rescan_tasks_map_.create(bucket_num, attr))) {
        LOG_WARN("init map failed", K(ret));
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
