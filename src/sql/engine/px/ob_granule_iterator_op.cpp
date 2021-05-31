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

namespace oceanbase {
using namespace common;
namespace sql {

ObGIOpInput::ObGIOpInput(ObExecContext& ctx, const ObOpSpec& spec)
    : ObOpInput(ctx, spec),
      parallelism_(-1),
      worker_id_(common::OB_INVALID_INDEX),
      ranges_(),
      pkeys_(),
      pump_(nullptr),
      deserialize_allocator_(nullptr)
{}

int ObGIOpInput::init(ObTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  UNUSED(task_info);
  // new parallel framework do not use this interface to set parameters
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("the interface shoud not be used", K(ret));
  return ret;
}

void ObGIOpInput::set_deserialize_allocator(common::ObIAllocator* allocator)
{
  deserialize_allocator_ = allocator;
}

int ObGIOpInput::assign_ranges(const common::ObIArray<common::ObNewRange>& ranges)
{
  int ret = OB_SUCCESS;
  FOREACH_CNT_X(it, ranges, OB_SUCC(ret))
  {
    if (OB_FAIL(ranges_.push_back(*it))) {
      LOG_WARN("failed to push range", K(ret));
    }
  }
  return ret;
}

int ObGIOpInput::assign_pkeys(const common::ObIArray<common::ObPartitionKey>& pkeys)
{
  int ret = OB_SUCCESS;
  FOREACH_CNT_X(it, pkeys, OB_SUCC(ret))
  {
    if (OB_FAIL(pkeys_.push_back(*it))) {
      LOG_WARN("failed to push range", K(ret));
    }
  }
  return ret;
}

int ObGIOpInput::deep_copy_range(ObIAllocator* allocator, const ObNewRange& src, ObNewRange& dst)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_SUCC(src.start_key_.deep_copy(dst.start_key_, *allocator)) &&
             OB_SUCC(src.end_key_.deep_copy(dst.end_key_, *allocator))) {
    dst.table_id_ = src.table_id_;
    dst.border_flag_ = src.border_flag_;
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
    MEMCPY(buf + pos, &pump_, sizeof(pump_));
    pos += sizeof(pump_);
    // LOG_TRACE("(pump)SE", K(pump_));
    if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, ranges_.count()))) {
      LOG_WARN("fail to encode key ranges count", K(ret), K(ranges_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges_.count(); ++i) {
      if (OB_FAIL(ranges_.at(i).serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize key range", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, pkeys_.count()))) {
      LOG_WARN("fail to encode key ranges count", K(ret), K(ranges_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys_.count(); ++i) {
      if (OB_FAIL(pkeys_.at(i).serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize key range", K(ret), K(i));
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObGIOpInput)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER(ObGIOpInput);
  LST_DO_CODE(OB_UNIS_DECODE, parallelism_, worker_id_);
  if (OB_SUCC(ret)) {
    int64_t count = 0;
    ranges_.reset();
    const char* str = buf + pos;
    ;
    MEMCPY(&pump_, str, sizeof(pump_));
    pos = pos + sizeof(pump_);
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
      LOG_WARN("fail to decode key ranges count", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      if (OB_ISNULL(deserialize_allocator_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("deserialize allocator is NULL", K(ret));
      } else {
        ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
        ObNewRange copy_range;
        ObNewRange key_range;
        copy_range.start_key_.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);
        copy_range.end_key_.assign(array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
        if (OB_FAIL(copy_range.deserialize(buf, data_len, pos))) {
          LOG_WARN("fail to deserialize range", K(ret));
        } else if (OB_FAIL(deep_copy_range(deserialize_allocator_, copy_range, key_range))) {
          LOG_WARN("fail to deep copy range", K(ret));
        } else if (OB_FAIL(ranges_.push_back(key_range))) {
          LOG_WARN("fail to add key range to array", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t count = 0;
    pkeys_.reset();
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
      LOG_WARN("fail to decode key ranges count", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      if (OB_ISNULL(deserialize_allocator_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("deserialize allocator is NULL", K(ret));
      } else {
        ObPartitionKey key;
        if (OB_FAIL(key.deserialize(buf, data_len, pos))) {
          LOG_WARN("fail to deserialize range", K(ret));
        } else if (OB_FAIL(pkeys_.push_back(key))) {
          LOG_WARN("fail to add key range to array", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObGIOpInput)
{
  int64_t len = 0;
  BASE_ADD_LEN(ObGIOpInput);
  LST_DO_CODE(OB_UNIS_ADD_LEN, parallelism_, worker_id_);
  len += sizeof(pump_);
  len += serialization::encoded_length_vi64(ranges_.count());
  for (int64_t i = 0; i < ranges_.count(); ++i) {
    len += ranges_.at(i).get_serialize_size();
  }
  len += serialization::encoded_length_vi64(pkeys_.count());
  for (int64_t i = 0; i < pkeys_.count(); ++i) {
    len += pkeys_.at(i).get_serialize_size();
  }
  return len;
}

OB_SERIALIZE_MEMBER((ObGranuleIteratorSpec, ObOpSpec), ref_table_id_, tablet_size_, affinitize_, partition_wise_join_,
    access_all_, nlj_with_param_down_, gi_attri_flag_);

ObGranuleIteratorSpec::ObGranuleIteratorSpec(ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      ref_table_id_(OB_INVALID_ID),
      tablet_size_(common::OB_DEFAULT_TABLET_SIZE),
      affinitize_(false),
      partition_wise_join_(false),
      access_all_(false),
      nlj_with_param_down_(false),
      pw_op_tscs_(alloc),
      gi_attri_flag_(0),
      dml_op_(NULL)
{}

ObGranuleIteratorOp::ObGranuleIteratorOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObOperator(exec_ctx, spec, input),
      parallelism_(-1),
      worker_id_(-1),
      tsc_op_id_(OB_INVALID_ID),
      ranges_(),
      pkeys_(),
      pump_(nullptr),
      state_(GI_UNINITIALIZED),
      all_task_fetched_(false),
      is_rescan_(false),
      rescan_taskset_(nullptr),
      rescan_task_idx_(0)
{}

void ObGranuleIteratorOp::destroy()
{
  ranges_.reset();
  pkeys_.reset();
  rescan_tasks_.reset();
}

int ObGranuleIteratorOp::parameters_init()
{
  int ret = OB_SUCCESS;
  ObGIOpInput* input = static_cast<ObGIOpInput*>(input_);
  if (nullptr != pump_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the context has been inited", K(ret));
  } else if (nullptr == input->pump_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the pump can not be null", K(ret));
  } else if (FALSE_IT(pump_ = input->pump_)) {
  } else if (OB_FAIL(ranges_.assign(input->ranges_))) {
    LOG_WARN("assign range failed", K(ret));
  } else if (OB_FAIL(pkeys_.assign(input->pkeys_))) {
    LOG_WARN("assign pkeys failed", K(ret));
  } else {
    parallelism_ = input->parallelism_;
    worker_id_ = input->worker_id_;
  }
  LOG_DEBUG("GI ctx init", K(this), K(ranges_), K(pkeys_), K(parallelism_), K(pump_), K(tsc_op_id_));
  return ret;
}

int ObGranuleIteratorOp::try_pruning_partition(
    const ObGITaskSet& taskset, ObGITaskSet::Pos& pos, bool& partition_pruned)
{
  int ret = OB_SUCCESS;
  ObGranuleTaskInfo info;
  if (OB_INVALID_ID == ctx_.get_gi_pruning_info().get_part_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pruning id is not set", K(ret));
  } else if (OB_FAIL(taskset.get_task_at_pos(info, pos))) {
    LOG_WARN("get task info failed", K(ret));
  } else if (info.partition_id_ == ctx_.get_gi_pruning_info().get_part_id()) {
    partition_pruned = false;
  } else {
    partition_pruned = true;
  }
  return ret;
}

int ObGranuleIteratorOp::try_fetch_task(ObGranuleTaskInfo& info)
{
  int ret = OB_SUCCESS;
  ObGranulePump* gi_task_pump = nullptr;
  const ObGITaskSet* taskset = NULL;
  ObGITaskSet::Pos pos;
  // init
  if (is_not_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the gi ctx is not init", K(ret));
  } else if (nullptr == (gi_task_pump = pump_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the pump can not be null", K(ret));
  } else {
    if (is_rescan_) {
      bool partition_pruned = false;
      do {
        partition_pruned = false;
        if (rescan_task_idx_ >= rescan_tasks_.count()) {
          ret = OB_ITER_END;
        } else {
          taskset = rescan_taskset_;
          pos = rescan_tasks_.at(rescan_task_idx_++);
          if (MY_SPEC.enable_partition_pruning() && OB_FAIL(try_pruning_partition(*taskset, pos, partition_pruned))) {
            LOG_WARN("fail try prune partition", K(ret));
          } else if (partition_pruned) {
            // next task
          }
        }
      } while (OB_SUCC(ret) && partition_pruned);
    } else {
      const bool from_share_pool = !MY_SPEC.affinitize_ && !MY_SPEC.access_all_;
      if (OB_FAIL(gi_task_pump->fetch_granule_task(taskset, pos, from_share_pool ? 0 : worker_id_, tsc_op_id_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to fetch next granule task", K(ret), K(gi_task_pump), K(worker_id_), K(MY_SPEC.affinitize_));
        } else {
          all_task_fetched_ = true;
        }
      } else if (NULL == taskset) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL taskset returned", K(ret));
      } else if (OB_FAIL(rescan_tasks_.push_back(pos))) {
        LOG_WARN("array push back failed", K(ret));
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

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(taskset->get_task_at_pos(info, pos))) {
    LOG_WARN("get task info failed", K(ret));
  } else {
    info.task_id_ = worker_id_;
  }
  return ret;
}

int ObGranuleIteratorOp::rescan()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.partition_wise()) {
    if (GI_UNINITIALIZED == state_) {
      // NJ call rescan before iterator rows, need to nothing for the first scan.
    } else if (GI_PREPARED == state_) {
      // At the open-stage we get a granule task, and now, we fetch all the granule task.
      while (OB_SUCC(do_get_next_granule_task())) {}
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to get all granule task", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      if (!all_task_fetched_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("rescan before all task fetched", K(ret), K(state_));
      }
    }
    if (OB_SUCC(ret)) {
      is_rescan_ = true;
      rescan_task_idx_ = 0;
      state_ = GI_GET_NEXT_GRANULE_TASK;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("the partition wise join GI rescan not supported", K(ret));
  }
  return ret;
}

int ObGranuleIteratorOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObOperator* real_child = nullptr;
  if (OB_FAIL(parameters_init())) {
    LOG_WARN("parameters init failed", K(ret));
  } else {
    if (!MY_SPEC.partition_wise()) {
      if (OB_FAIL(get_gi_task_consumer_node(this, real_child))) {
        LOG_WARN("Failed to get real child", K(ret));
      } else {
        tsc_op_id_ = real_child->get_spec().id_;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(child_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("child_op is null", K(ret));
    } else if (OB_FAIL(prepare_table_scan())) {
      LOG_WARN("prepare table scan failed", K(ret));
    }
  }
  return ret;
}

int ObGranuleIteratorOp::inner_close()
{
  return OB_SUCCESS;
}

int ObGranuleIteratorOp::inner_get_next_row()
{
  return try_get_next_row();
}

int ObGranuleIteratorOp::try_get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_next_row = false;
  do {
    clear_evaluated_flag();
    switch (state_) {
      case GI_UNINITIALIZED: {
        // try check the pump
        state_ = GI_GET_NEXT_GRANULE_TASK;
        break;
      }
      case GI_GET_NEXT_GRANULE_TASK: {
        if (OB_FAIL(do_get_next_granule_task())) {
          if (ret != OB_ITER_END) {
            LOG_WARN("fail to get next granule task", K(ret));
          }
        }
      } break;
      case GI_PREPARED:
      case GI_TABLE_SCAN: {
        if (OB_FAIL(child_->get_next_row())) {
          LOG_DEBUG("failed to get new row", K(ret), K(MY_SPEC.affinitize_), K(MY_SPEC.ref_table_id_), K(worker_id_));
          if (OB_ITER_END != ret) {
            LOG_WARN("try fetch task failed", K(ret));
          } else {
            ret = OB_SUCCESS;
            state_ = GI_GET_NEXT_GRANULE_TASK;
          }
        } else {
          LOG_DEBUG("get new row", K(ret), K(MY_SPEC.affinitize_), K(MY_SPEC.ref_table_id_), K(worker_id_));
          got_next_row = true;
        }
        break;
      }
      case GI_END: {
        ret = OB_ITER_END;
        got_next_row = true;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state", K(ret), K(state_));
      }
    }
  } while (!(got_next_row || OB_FAIL(ret)));
  return ret;
}

/*
 *  this function will get a scan task from the task pump,
 *  and reset the table-scan operator below this operator.
 *  if the plan below this granule iterator is a partition wise-join plan,
 *  we will reset all table-scan operator
 *  in this plan.
 * */
int ObGranuleIteratorOp::do_get_next_granule_task(bool prepare /* = false */)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObGranuleTaskInfo, 4> gi_task_infos;
  GIPrepareTaskMap* gi_prepare_map = nullptr;
  ObExecContext::ObPlanRestartGuard restart_plan(ctx_);
  IGNORE_RETURN ctx_.set_gi_restart();
  if (OB_FAIL(ctx_.get_gi_task_map(gi_prepare_map))) {
    LOG_WARN("Failed to get gi task map", K(ret));
  } else if (!MY_SPEC.partition_wise()) {
    ObGranuleTaskInfo gi_task_info;
    /* non-partition wise join */
    if (OB_FAIL(try_fetch_task(gi_task_info))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("try fetch task failed", K(ret));
      } else {
        state_ = GI_END;
      }
    } else {
      if (OB_NOT_NULL(gi_prepare_map->get(tsc_op_id_))) {
        if (OB_FAIL(gi_prepare_map->erase_refactored(tsc_op_id_))) {
          LOG_WARN("failed to erase task", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(gi_prepare_map->set_refactored(tsc_op_id_, gi_task_info))) {
        LOG_WARN("reset table scan's ranges failed", K(ret), K(child_->get_spec().id_), K(gi_task_info));
      } else if (!prepare && OB_FAIL(child_->rescan())) {
        /**
         * In inner open stage, the prepare is true.
         * Tsc will do scan in his inner open, so we should skip rescan action.
         */
        LOG_WARN("fail to rescan gi' child", K(ret));
      } else {
        state_ = GI_TABLE_SCAN;
      }
      LOG_DEBUG("produce a gi task", K(tsc_op_id_), K(gi_task_info), K(prepare));
    }
  } else {
    /* partition wise join */
    ObSEArray<int64_t, 4> op_ids;
    if (OB_NOT_NULL(MY_SPEC.dml_op_)) {
      if (OB_FAIL(op_ids.push_back(MY_SPEC.dml_op_->id_))) {
        LOG_WARN("failed to push back op ids", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      for (int i = 0; i < MY_SPEC.pw_op_tscs_.count() && OB_SUCC(ret); i++) {
        const ObTableScanSpec* tsc = MY_SPEC.pw_op_tscs_.at(i);
        if (OB_FAIL(op_ids.push_back(tsc->id_))) {
          LOG_WARN("failed to push back op ids", K(ret));
        }
      }
    }
    if (OB_FAIL(fetch_full_pw_tasks(gi_task_infos, op_ids))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("try fetch task failed", K(ret));
      } else {
        state_ = GI_END;
      }
    }

    ARRAY_FOREACH_X(op_ids, idx, cnt, OB_SUCC(ret))
    {
      if (OB_NOT_NULL(gi_prepare_map->get(op_ids.at(idx)))) {
        if (OB_FAIL(gi_prepare_map->erase_refactored(op_ids.at(idx)))) {
          LOG_WARN("failed to erase task", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(gi_prepare_map->set_refactored(op_ids.at(idx), gi_task_infos.at(idx)))) {
        LOG_WARN("reset table scan's ranges failed", K(ret));
      }
      LOG_DEBUG("produce a gi task(PWJ)", K(op_ids.at(idx)), K(gi_task_infos.at(idx)));
    }

    if (OB_SUCC(ret)) {
      /**
       * In inner open stage, the prepare is true.
       * Tsc will do scan in his inner open, so we should skip rescan action.
       */
      if (!prepare && OB_FAIL(child_->rescan())) {
        LOG_WARN("fail to rescan gi' child", K(ret));
      } else {
        state_ = GI_TABLE_SCAN;
      }
    }
  }
  IGNORE_RETURN ctx_.reset_gi_restart();
  return ret;
}

/*
 * this interface is used to reassign scan tasks in the partition wise-join plan,
 * which is below this granule iterator.
 *
 * */
int ObGranuleIteratorOp::fetch_full_pw_tasks(ObIArray<ObGranuleTaskInfo>& infos, const ObIArray<int64_t>& op_ids)
{
  int ret = OB_SUCCESS;
  ObGranulePump* gi_task_pump = nullptr;
  // init
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
    LOG_WARN("the tsc does not match task count", K(ret), K(op_ids.count()), K(infos.count()));
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
  } else if (OB_FAIL(do_get_next_granule_task(true /* prapare */))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next granule task", K(ret));
    } else {
      // we can not return OB_ITER_END, because in open-stage,
      // OB_ITER_END is regard as error.
      ret = OB_SUCCESS;
      // get there mean the state == GI_END
    }
  } else {
    // before state_ == GI_TABLE_SCAN
    state_ = GI_PREPARED;
  }
  LOG_DEBUG("do prepare table scan", K(ret), K(state_));
  return ret;
}

int ObGranuleIteratorOp::set_tscs(ObIArray<const ObTableScanSpec*>& tscs)
{
  int ret = OB_SUCCESS;
  ObGranuleIteratorSpec* gi_spec = static_cast<ObGranuleIteratorSpec*>(const_cast<ObOpSpec*>(&spec_));
  if (OB_FAIL(gi_spec->pw_op_tscs_.prepare_allocate(tscs.count()))) {
    LOG_WARN("Failed to init fixed array", K(ret));
  };
  ARRAY_FOREACH_X(tscs, idx, cnt, OB_SUCC(ret))
  {
    gi_spec->pw_op_tscs_.at(idx) = tscs.at(idx);
  }
  LOG_DEBUG("Set table scan to GI", K(tscs), K(ret));
  return ret;
}

int ObGranuleIteratorOp::set_dml_op(const ObTableModifySpec* dml_op)
{
  const_cast<ObGranuleIteratorSpec&>(MY_SPEC).dml_op_ = const_cast<ObTableModifySpec*>(dml_op);
  return OB_SUCCESS;
}

int ObGranuleIteratorOp::get_gi_task_consumer_node(ObOperator* cur, ObOperator*& child) const
{
  int ret = OB_SUCCESS;
  ObOperator* first_child = NULL;
  if (0 >= cur->get_child_cnt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can't get the consumer node", K(ret), K(cur->get_child_cnt()));
  } else if (OB_ISNULL(first_child = cur->get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (PHY_TABLE_SCAN == first_child->get_spec().type_ ||
             PHY_BLOCK_SAMPLE_SCAN == first_child->get_spec().type_ ||
             PHY_ROW_SAMPLE_SCAN == first_child->get_spec().type_) {
    child = first_child;
  } else if (get_gi_task_consumer_node(first_child, child)) {
    LOG_WARN("failed to get gi task consumer node", K(ret));
  }
  if (OB_SUCC(ret) && OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can't find the tsc phy op", K(ret));
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
