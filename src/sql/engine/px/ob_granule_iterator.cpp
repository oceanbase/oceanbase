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
#include "sql/engine/px/ob_granule_iterator.h"
#include "sql/engine/px/ob_granule_pump.h"
#include "sql/executor/ob_task_spliter.h"

using namespace std;

namespace oceanbase {
namespace sql {

int ObGIInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(task_info);
  UNUSED(op);
  // new parallel framework do not use this interface to set parameters
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("the interface shoud not be used", K(ret));
  return ret;
}

void ObGIInput::set_deserialize_allocator(common::ObIAllocator* allocator)
{
  deserialize_allocator_ = allocator;
}

ObPhyOperatorType ObGIInput::get_phy_op_type() const
{
  return PHY_GRANULE_ITERATOR;
}

int ObGIInput::assign_ranges(const common::ObIArray<common::ObNewRange>& ranges)
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

int ObGIInput::assign_pkeys(const common::ObIArray<common::ObPartitionKey>& pkeys)
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

int ObGIInput::deep_copy_range(ObIAllocator* allocator, const ObNewRange& src, ObNewRange& dst)
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

OB_DEF_SERIALIZE(ObGIInput)
{
  int ret = OK_;
  UNF_UNUSED_SER;
  BASE_SER(ObGIInput);
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

OB_DEF_DESERIALIZE(ObGIInput)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER(ObGIInput);
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

OB_DEF_SERIALIZE_SIZE(ObGIInput)
{
  int64_t len = 0;
  BASE_ADD_LEN(ObGIInput);
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

////////////////////////////////////////////////////////////////////////////////////////

ObGranuleIterator::ObGranuleIterator(common::ObIAllocator& alloc)
    : ObSingleChildPhyOperator(alloc),
      ref_table_id_(OB_INVALID_ID),
      tablet_size_(common::OB_DEFAULT_TABLET_SIZE),
      affinitize_(false),
      partition_wise_join_(false),
      access_all_(false),
      nlj_with_param_down_(false),
      pw_op_tscs_(alloc),
      gi_attri_flag_(0),
      dml_op_(NULL){};

ObGranuleIterator::~ObGranuleIterator(){};

void ObGranuleIterator::ObGranuleIteratorCtx::destroy()
{
  ranges_.reset();
  pkeys_.reset();
  rescan_tasks_.reset();
}

int ObGranuleIterator::ObGranuleIteratorCtx::parameters_init(const ObGIInput* input)
{
  int ret = OB_SUCCESS;
  if (nullptr == input) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the input can not be null", K(ret));
  } else if (nullptr != pump_) {
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

int ObGranuleIterator::try_fetch_task(ObExecContext& ctx, ObGranuleTaskInfo& info) const
{
  int ret = OB_SUCCESS;
  ObGranuleIteratorCtx* gi_ctx = nullptr;
  ObGranulePump* gi_task_pump = nullptr;
  const ObGITaskSet* taskset = NULL;
  ObGITaskSet::Pos pos;
  // init
  if (OB_ISNULL(gi_ctx = GET_PHY_OPERATOR_CTX(ObGranuleIteratorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (gi_ctx->is_not_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the gi ctx is not init", K(ret));
  } else if (nullptr == (gi_task_pump = gi_ctx->pump_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the pump can not be null", K(ret));
  } else {
    if (gi_ctx->is_rescan_) {
      if (gi_ctx->rescan_task_idx_ >= gi_ctx->rescan_tasks_.count()) {
        ret = OB_ITER_END;
      } else {
        taskset = gi_ctx->rescan_taskset_;
        pos = gi_ctx->rescan_tasks_.at(gi_ctx->rescan_task_idx_++);
      }
    } else {
      const bool from_share_pool = !affinitize_ && !access_all_;
      if (OB_FAIL(gi_task_pump->fetch_granule_task(
              taskset, pos, from_share_pool ? 0 : gi_ctx->worker_id_, gi_ctx->tsc_op_id_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to fetch next granule task", K(ret), K(gi_task_pump), K(gi_ctx->worker_id_), K(affinitize_));
        } else {
          gi_ctx->all_task_fetched_ = true;
        }
      } else if (NULL == taskset) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL taskset returned", K(ret));
      } else if (OB_FAIL(gi_ctx->rescan_tasks_.push_back(pos))) {
        LOG_WARN("array push back failed", K(ret));
      } else {
        if (NULL == gi_ctx->rescan_taskset_) {
          gi_ctx->rescan_taskset_ = taskset;
        } else if (gi_ctx->rescan_taskset_ != taskset) {
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
    info.task_id_ = gi_ctx->worker_id_;
  }
  return ret;
}

int ObGranuleIterator::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObGranuleIteratorCtx* gi_ctx = nullptr;
  if (OB_ISNULL(gi_ctx = GET_PHY_OPERATOR_CTX(ObGranuleIteratorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (!partition_wise()) {
    if (GI_UNINITIALIZED == gi_ctx->state_) {
      // NJ call rescan before iterator rows, need to nothing for the first scan.
    } else if (GI_PREPARED == gi_ctx->state_) {
      // At the open-stage we get a granule task, and now, we fetch all the granule task.
      while (OB_SUCC(do_get_next_granule_task(*gi_ctx, ctx))) {}
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to get all granule task", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      if (!gi_ctx->all_task_fetched_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("rescan before all task fetched", K(ret), K(gi_ctx->state_));
      }
    }
    if (OB_SUCC(ret)) {
      gi_ctx->is_rescan_ = true;
      gi_ctx->rescan_task_idx_ = 0;
      gi_ctx->state_ = GI_GET_NEXT_GRANULE_TASK;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("the partition wise join GI rescan not supported", K(ret));
  }
  return ret;
}

void ObGranuleIterator::reset()
{
  pw_op_tscs_.reset();
  dml_op_ = NULL;
  ObSingleChildPhyOperator::reset();
}

void ObGranuleIterator::reuse()
{
  pw_op_tscs_.reuse();
  dml_op_ = NULL;
  ObSingleChildPhyOperator::reuse();
}

int64_t ObGranuleIterator::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K(ref_table_id_), K(tablet_size_), K(affinitize_), K(access_all_));
  return pos;
}

int ObGranuleIterator::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObGIInput* op_input = NULL;
  ObGranuleIteratorCtx* gi_ctx = NULL;
  ObPhyOperator* real_child = nullptr;

  if (OB_ISNULL(op_input = GET_PHY_OP_INPUT(ObGIInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the granule iterator input is null", K(ret), "op_id", get_id(), "op_type", get_type());
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (nullptr == op_input) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the input cannot be null", K(ret));
  } else if (OB_FAIL(inner_create_operator_ctx(ctx, op_ctx))) {
    LOG_WARN("inner create operator context failed", K(ret));
  } else if ((OB_ISNULL(gi_ctx = GET_PHY_OPERATOR_CTX(ObGranuleIteratorCtx, ctx, get_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get gi ctx", K(ret));
  } else if (OB_FAIL(gi_ctx->parameters_init(op_input))) {
    LOG_WARN("parameters init failed", K(ret));
  } else if (OB_FAIL(init_cur_row(*gi_ctx, need_copy_row_for_compute()))) {
    LOG_WARN("init cur row failed", K(ret));
  } else {
    gi_ctx->tablet_size_ = tablet_size_;
    if (!partition_wise()) {
      if (OB_FAIL(get_gi_task_consumer_node(this, real_child))) {
        LOG_WARN("failed to get real child", K(ret));
      } else {
        // If it is partition wise, tsc op io is not needed
        // Because in the case of partition wise,
        // the GI task array is obtained directly through the `pw_op_tscs_` array class
        gi_ctx->tsc_op_id_ = real_child->get_id();
      }
    }
  }
  return ret;
}

int ObGranuleIterator::inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObGranuleIteratorCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create phy ctx faile", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObGranuleIterator::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObGranuleIteratorCtx* gi_ctx = NULL;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("failed to init operator context", K(ret));
  } else if (OB_ISNULL(gi_ctx = GET_PHY_OPERATOR_CTX(ObGranuleIteratorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (OB_ISNULL(child_op_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("child_op is null", K(ret));
  } else if (OB_FAIL(prepare_table_scan(ctx))) {
    LOG_WARN("prepare table scan failed", K(ret));
  }
  return ret;
}

int ObGranuleIterator::inner_close(ObExecContext& ctx) const
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

int ObGranuleIterator::inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const
{
  return try_get_next_row(exec_ctx, row);
}

int ObGranuleIterator::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObGIInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create table scan input", K(ret), "op_id", get_id(), "op_type", get_type());
  } else {
    // do nothing
    UNUSED(input);
  }
  return ret;
}

int ObGranuleIterator::try_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObGranuleIteratorCtx* gi_ctx = NULL;
  bool got_next_row = false;

  if ((OB_ISNULL(gi_ctx = GET_PHY_OPERATOR_CTX(ObGranuleIteratorCtx, exec_ctx, get_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get gi ctx", K(ret));
  } else {
    do {
      switch (gi_ctx->state_) {
        case GI_UNINITIALIZED: {
          // try check the pump
          gi_ctx->state_ = GI_GET_NEXT_GRANULE_TASK;
          break;
        }
        case GI_GET_NEXT_GRANULE_TASK: {
          if (OB_FAIL(do_get_next_granule_task(*gi_ctx, exec_ctx))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("fail to get next granule task", K(ret));
            }
          }
        } break;
        case GI_PREPARED:
        case GI_TABLE_SCAN: {
          if (OB_FAIL(child_op_->get_next_row(exec_ctx, row))) {
            LOG_DEBUG("failed to get new row", K(row), K(ret), K(affinitize_), K(ref_table_id_), K(gi_ctx->worker_id_));
            if (OB_ITER_END != ret) {
              LOG_WARN("try fetch task failed", K(ret));
            } else {
              ret = OB_SUCCESS;
              gi_ctx->state_ = GI_GET_NEXT_GRANULE_TASK;
            }
          } else if (OB_FAIL(copy_cur_row(*gi_ctx, row))) {
            LOG_WARN("copy row failed", K(ret));
          } else {
            LOG_DEBUG("get new row", K(row), K(*row), K(ret), K(affinitize_), K(ref_table_id_), K(gi_ctx->worker_id_));
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
          LOG_WARN("unexpected state", K(ret), K(gi_ctx->state_));
        }
      }
    } while (!(got_next_row || OB_FAIL(ret)));
  }
  return ret;
}

/*
 *  this function will get a scan task from the task pump, and reset the table-scan operator below this operator.
 *  if the plan below this granule iterator is a partition wise-join plan, we will reset all table-scan operator
 *  in this plan.
 *
 *  IN    gi_ctx      the gi operator's context
 *  IN    exec_ctx    the execution context
 *
 * */
int ObGranuleIterator::do_get_next_granule_task(
    ObGranuleIteratorCtx& gi_ctx, ObExecContext& exec_ctx, bool prepare /* = false */) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObGranuleTaskInfo, 4> gi_task_infos;
  GIPrepareTaskMap* gi_prepare_map = nullptr;
  ObExecContext::ObPlanRestartGuard restart_plan(exec_ctx);
  IGNORE_RETURN exec_ctx.set_gi_restart();
  if (OB_FAIL(exec_ctx.get_gi_task_map(gi_prepare_map))) {
    LOG_WARN("Failed to get gi task map", K(ret));
  } else if (!partition_wise()) {
    ObGranuleTaskInfo gi_task_info;
    /* non-partition wise join */
    if (OB_FAIL(try_fetch_task(exec_ctx, gi_task_info))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("try fetch task failed", K(ret));
      } else {
        gi_ctx.state_ = GI_END;
      }
    } else {
      if (OB_NOT_NULL(gi_prepare_map->get(gi_ctx.tsc_op_id_))) {
        // When GI stuffs tasks into Map, it needs to try to clean up the last stuffed tasks
        if (OB_FAIL(gi_prepare_map->erase_refactored(gi_ctx.tsc_op_id_))) {
          LOG_WARN("failed to erase task", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(gi_prepare_map->set_refactored(gi_ctx.tsc_op_id_, gi_task_info))) {
          LOG_WARN("reset table scan's ranges failed", K(ret), K(child_op_->get_id()), K(gi_task_info));
        } else if (!prepare && OB_FAIL(child_op_->rescan(exec_ctx))) {
          /**
           * In inner open stage, the prepare is true.
           * Tsc will do scan in his inner open, so we should skip rescan action.
           */
          LOG_WARN("fail to rescan gi' child", K(ret));
        } else {
          gi_ctx.state_ = GI_TABLE_SCAN;
        }
        LOG_DEBUG("produce a gi task", K(gi_ctx.tsc_op_id_), K(gi_task_info));
      }
    }
  } else {
    /* partition wise join */
    ObSEArray<int64_t, 4> op_ids;
    if (OB_NOT_NULL(dml_op_)) {
      // GI divides the tasks of the INSERT/REPLACE table and obtains the op id of the corresponding INSERT/REPLACE
      if (OB_FAIL(op_ids.push_back(dml_op_->get_id()))) {
        LOG_WARN("failed to push back op ids", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      for (int i = 0; i < pw_op_tscs_.count() && OB_SUCC(ret); i++) {
        const ObTableScan* tsc = pw_op_tscs_.at(i);
        if (OB_FAIL(op_ids.push_back(tsc->get_id()))) {
          LOG_WARN("failed to push back op ids", K(ret));
        }
      }
    }
    if (OB_FAIL(fetch_full_pw_tasks(exec_ctx, gi_task_infos, op_ids))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("try fetch task failed", K(ret));
      } else {
        gi_ctx.state_ = GI_END;
      }
    }
    ARRAY_FOREACH_X(op_ids, idx, cnt, OB_SUCC(ret))
    {
      if (OB_NOT_NULL(gi_prepare_map->get(op_ids.at(idx)))) {
        if (OB_FAIL(gi_prepare_map->erase_refactored(op_ids.at(idx)))) {
          LOG_WARN("failed to erase task", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(gi_prepare_map->set_refactored(op_ids.at(idx), gi_task_infos.at(idx)))) {
          LOG_WARN("reset table scan's ranges failed", K(ret));
        }
        LOG_DEBUG("produce a gi task(PWJ)", K(op_ids.at(idx)), K(gi_task_infos.at(idx)));
      }
    }

    if (OB_SUCC(ret)) {
      /**
       * In inner open stage, the prepare is true.
       * Tsc will do scan in his inner open, so we should skip rescan action.
       */
      if (!prepare && OB_FAIL(child_op_->rescan(exec_ctx))) {
        LOG_WARN("fail to rescan gi' child", K(ret));
      } else {
        gi_ctx.state_ = GI_TABLE_SCAN;
      }
    }
  }
  IGNORE_RETURN exec_ctx.reset_gi_restart();
  return ret;
}

int ObGranuleIterator::fetch_full_pw_tasks(
    ObExecContext& ctx, ObIArray<ObGranuleTaskInfo>& infos, const ObIArray<int64_t>& op_ids) const
{
  int ret = OB_SUCCESS;
  ObGranuleIteratorCtx* gi_ctx = nullptr;
  ObGranulePump* gi_task_pump = nullptr;
  // init
  if (OB_ISNULL(gi_ctx = GET_PHY_OPERATOR_CTX(ObGranuleIteratorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (gi_ctx->is_not_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the gi ctx is not init", K(ret));
  } else if (nullptr == (gi_task_pump = gi_ctx->pump_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the pump can not be null", K(ret));
  } else if (OB_FAIL(gi_task_pump->try_fetch_pwj_tasks(infos, op_ids, gi_ctx->worker_id_))) {
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
int ObGranuleIterator::prepare_table_scan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObGranuleIteratorCtx* gi_ctx = nullptr;
  if (OB_ISNULL(gi_ctx = GET_PHY_OPERATOR_CTX(ObGranuleIteratorCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (!gi_ctx->is_not_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected gi state", K(ret), K(gi_ctx->state_));
  } else if (FALSE_IT(gi_ctx->state_ = GI_GET_NEXT_GRANULE_TASK)) {
  } else if (OB_FAIL(do_get_next_granule_task(*gi_ctx, ctx, true /* prapare */))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next granule task", K(ret));
    } else {
      // we can not return OB_ITER_END, because in open-stage,
      // OB_ITER_END is regard as error.
      ret = OB_SUCCESS;
      // get there mean the gi_ctx->state == GI_END
    }
  } else {
    // before gi_ctx->state_ == GI_TABLE_SCAN
    gi_ctx->state_ = GI_PREPARED;
  }
  LOG_DEBUG("do prepare table scan", K(ret), K(gi_ctx->state_));
  return ret;
}

int ObGranuleIterator::set_tscs(ObIArray<const ObTableScan*>& tscs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pw_op_tscs_.prepare_allocate(tscs.count()))) {
    LOG_WARN("Failed to init fixed array", K(ret));
  };
  ARRAY_FOREACH_X(tscs, idx, cnt, OB_SUCC(ret))
  {
    pw_op_tscs_.at(idx) = tscs.at(idx);
  }
  LOG_DEBUG("Set table scan to GI", K(tscs), K(ret));
  return ret;
}

int ObGranuleIterator::set_dml_op(ObTableModify* dml_op)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(dml_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml op is not null, may be not reset", K(ret));
  } else {
    dml_op_ = dml_op;
  }
  LOG_DEBUG("Set table dml to GI", K(dml_op), K(ret));
  return ret;
}

int ObGranuleIterator::get_gi_task_consumer_node(const ObPhyOperator* cur, ObPhyOperator*& child) const
{
  int ret = OB_SUCCESS;
  ObPhyOperator* first_child = NULL;
  if (0 >= cur->get_child_num()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can't get the consumer node", K(ret), K(cur->get_child_num()));
  } else if (OB_ISNULL(first_child = cur->get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (PHY_TABLE_SCAN == first_child->get_type() || PHY_BLOCK_SAMPLE_SCAN == first_child->get_type() ||
             PHY_ROW_SAMPLE_SCAN == first_child->get_type()) {
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

OB_SERIALIZE_MEMBER((ObGranuleIterator, ObSingleChildPhyOperator), ref_table_id_, tablet_size_, affinitize_,
    partition_wise_join_, access_all_, nlj_with_param_down_, gi_attri_flag_)

}  // namespace sql
}  // namespace oceanbase
