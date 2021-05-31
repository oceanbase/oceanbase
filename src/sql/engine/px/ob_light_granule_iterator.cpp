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
#include "sql/engine/px/ob_light_granule_iterator.h"
#include "sql/executor/ob_task_spliter.h"

using namespace std;

namespace oceanbase {
namespace sql {

int ObLGIInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx);
  location_idx_list_.set_allocator(&ctx.get_allocator());
  part_stmt_ids_.set_allocator(&ctx.get_allocator());
  if (OB_FAIL(location_idx_list_.assign(task_info.get_location_idx_list()))) {
    LOG_WARN("assign location idx list failed", K(ret), K(task_info.get_location_idx_list()));
  } else if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan ctx is null", K(ret));
  } else if (!plan_ctx->get_batched_stmt_param_idxs().empty()) {
    // for batch stmt
    if (ctx.get_task_exec_ctx().get_table_locations().count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table location is invalid", K(ret));
    } else if (OB_FAIL(init_param_array_list(*plan_ctx,
                   ctx.get_task_exec_ctx().get_table_locations().at(0),
                   task_info,
                   static_cast<const ObLightGranuleIterator&>(op)))) {
      LOG_WARN("init param array list failed", K(ret));
    }
  }
  LOG_DEBUG("init light granule iterator end", K(ret), K(task_info), KPC(this));
  return ret;
}

// only serialize params of this partition
int ObLGIInput::init_param_array_list(
    ObPhysicalPlanCtx& plan_ctx, ObPhyTableLocation& table_loc, ObTaskInfo& task_info, const ObLightGranuleIterator& op)
{
  int ret = OB_SUCCESS;
  const ParamStore& param_store = plan_ctx.get_param_store();
  const ObIArray<int64_t>& array_param_idxs = op.get_array_param_idxs();
  const ObIArray<uint64_t>& location_idxs = task_info.get_location_idx_list();
  const ObPartitionReplicaLocationIArray& part_list = table_loc.get_partition_location_list();
  if (OB_FAIL(part_stmt_ids_.prepare_allocate(location_idxs.count()))) {
    LOG_WARN("prepare allocate part stmt ids failed", K(ret), K(location_idxs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < location_idxs.count(); ++i) {
    int64_t location_idx = location_idxs.at(i);
    const ObIArray<int64_t>* param_idxs = nullptr;
    // find param idx list by location idx
    if (OB_UNLIKELY(location_idx < 0) || OB_UNLIKELY(location_idx >= part_list.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("location idx is invalid", K(ret), K(location_idx), K(part_list));
    } else if (OB_ISNULL(param_idxs = plan_ctx.get_part_param_idxs(part_list.at(location_idx).get_partition_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get part param index failed", K(ret), K(part_list.at(i)));
    } else if (OB_FAIL(part_stmt_ids_.at(i).assign(*param_idxs))) {
      LOG_WARN("set part stmt ids failed", K(ret), KPC(param_idxs), K(i));
    }
    for (int64_t k = 0; OB_SUCC(ret) && k < array_param_idxs.count(); ++k) {
      // find param list in param store by param idx list, and store to param array list.
      int64_t param_idx = array_param_idxs.at(k);
      ObArrayParamInfo param_info;
      param_info.param_store_idx_ = param_idx;
      if (OB_UNLIKELY(param_idx < 0) || OB_UNLIKELY(param_idx >= param_store.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param idx is invalid", K(ret), K(param_idx), K(param_store.count()));
      } else if (OB_UNLIKELY(!param_store.at(param_idx).is_ext())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query param is invalid", K(param_idx), K(param_store.at(param_idx)));
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(param_array_list_.push_back(param_info))) {
          LOG_WARN("store param info failed", K(ret));
        }
      }
    }
  }

  return ret;
}

void ObLGIInput::set_deserialize_allocator(common::ObIAllocator* allocator)
{
  deserialize_allocator_ = allocator;
}

ObPhyOperatorType ObLGIInput::get_phy_op_type() const
{
  return PHY_LIGHT_GRANULE_ITERATOR;
}

OB_SERIALIZE_MEMBER(ObLGIInput::ObArrayParamInfo, param_store_idx_);

OB_DEF_SERIALIZE(ObLGIInput)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(location_idx_list_);
  OB_UNIS_ENCODE(param_array_list_);
  OB_UNIS_ENCODE(part_stmt_ids_);
  return ret;
}

OB_DEF_DESERIALIZE(ObLGIInput)
{
  int ret = OB_SUCCESS;
  location_idx_list_.set_allocator(deserialize_allocator_);
  part_stmt_ids_.set_allocator(deserialize_allocator_);
  OB_UNIS_DECODE(location_idx_list_);
  OB_UNIS_DECODE(param_array_list_);
  OB_UNIS_DECODE(part_stmt_ids_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLGIInput)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(location_idx_list_);
  OB_UNIS_ADD_LEN(param_array_list_);
  OB_UNIS_ADD_LEN(part_stmt_ids_);
  return len;
}

////////////////////////////////////////////////////////////////////////////////////////

ObLightGranuleIterator::ObLightGranuleIterator(common::ObIAllocator& alloc)
    : ObSingleChildPhyOperator(alloc),
      dml_location_key_(OB_INVALID_ID),
      dml_op_id_(OB_INVALID_ID),
      is_pwj_(false),
      lgi_scan_infos_(alloc),
      array_param_idxs_(alloc)
{}

ObLightGranuleIterator::~ObLightGranuleIterator()
{}

void ObLightGranuleIterator::ObLGICtx::destroy()
{}

bool ObLightGranuleIterator::is_task_end(ObLGICtx& lgi_ctx, ObLGIInput& lgi_input)
{
  bool bret = false;
  if (lgi_input.param_array_list_.empty()) {
    if (lgi_ctx.cur_granule_pos_ >= lgi_input.location_idx_list_.count()) {
      bret = true;
    }
  } else {
    if (lgi_ctx.cur_granule_pos_ >= lgi_input.location_idx_list_.count() && lgi_ctx.cur_part_id_ < 0) {
      bret = true;
    }
  }
  return bret;
}

int ObLightGranuleIterator::try_fetch_task_with_pwj(ObExecContext& ctx, ObIArray<ObGranuleTaskInfo>& lgi_tasks) const
{
  int ret = OB_SUCCESS;
  ObLGICtx* lgi_ctx = nullptr;
  ObLGIInput* lgi_input = nullptr;
  ObPhysicalPlanCtx* plan_ctx = nullptr;
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  const ObPhyTableLocation* table_location = nullptr;
  ObGranuleTaskInfo lgi_task_info;
  // init
  if (OB_ISNULL(lgi_ctx = GET_PHY_OPERATOR_CTX(ObLGICtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (OB_ISNULL(lgi_input = GET_PHY_OP_INPUT(ObLGIInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator input failed", K(ret), K(get_id()));
  } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    LOG_WARN("get physical plan ctx failed", K(ret));
  } else if (lgi_ctx->is_not_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the gi ctx is not init", K(ret));
  } else if (OB_UNLIKELY(is_task_end(*lgi_ctx, *lgi_input))) {
    ret = OB_ITER_END;
    LOG_DEBUG("granule task end", KPC(lgi_ctx), KPC(lgi_input));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < lgi_scan_infos_.count(); ++i) {
    const ObLGIScanInfo& lgi_scan_info = lgi_scan_infos_.at(i);
    if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
            task_exec_ctx, lgi_scan_info.table_location_key_, lgi_scan_info.ref_table_id_, table_location))) {
      LOG_WARN("failed to get physical table location", K(ret), K(lgi_scan_info));
    } else {
      const ObPartitionReplicaLocationIArray& part_loc_list = table_location->get_partition_location_list();
      int64_t location_idx = lgi_input->location_idx_list_.at(lgi_ctx->cur_granule_pos_);
      if (OB_UNLIKELY(location_idx >= part_loc_list.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected location index", K(ret), K(location_idx), K(part_loc_list.count()));
      } else {
        lgi_task_info.partition_id_ = part_loc_list.at(location_idx).get_partition_id();
        if (OB_FAIL(lgi_tasks.push_back(lgi_task_info))) {
          LOG_WARN("store lgi task info failed", K(ret), K(lgi_task_info));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ++lgi_ctx->cur_granule_pos_;
  }
  return ret;
}

int ObLightGranuleIterator::try_fetch_task(ObExecContext& ctx, ObGranuleTaskInfo& info) const
{
  int ret = OB_SUCCESS;
  ObLGICtx* lgi_ctx = nullptr;
  ObLGIInput* lgi_input = nullptr;
  ObPhysicalPlanCtx* plan_ctx = nullptr;
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  const ObPhyTableLocation* table_location = nullptr;
  // init
  if (OB_ISNULL(lgi_ctx = GET_PHY_OPERATOR_CTX(ObLGICtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (OB_ISNULL(lgi_input = GET_PHY_OP_INPUT(ObLGIInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator input failed", K(ret), K(get_id()));
  } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    LOG_WARN("get physical plan ctx failed", K(ret));
  } else if (lgi_ctx->is_not_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the gi ctx is not init", K(ret));
  } else if (OB_UNLIKELY(is_task_end(*lgi_ctx, *lgi_input))) {
    ret = OB_ITER_END;
    LOG_DEBUG("granule task end", KPC(lgi_ctx), KPC(lgi_input));
  } else if (lgi_input->param_array_list_.empty() || -1 == lgi_ctx->cur_part_id_) {
    const ObLGIScanInfo& lgi_scan_info = lgi_scan_infos_.at(0);
    if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
            task_exec_ctx, lgi_scan_info.table_location_key_, lgi_scan_info.ref_table_id_, table_location))) {
      LOG_WARN("failed to get physical table location", K(ret), K(lgi_scan_info));
    } else if (OB_UNLIKELY(lgi_ctx->cur_granule_pos_ < 0) ||
               OB_UNLIKELY(lgi_ctx->cur_granule_pos_ >= lgi_input->location_idx_list_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur granule pos is invalid", K(ret), KPC(lgi_ctx), KPC(lgi_input));
    } else {
      const ObPartitionReplicaLocationIArray& part_loc_list = table_location->get_partition_location_list();
      int64_t location_idx = lgi_input->location_idx_list_.at(lgi_ctx->cur_granule_pos_);
      if (OB_UNLIKELY(location_idx >= part_loc_list.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected location index", K(ret), K(location_idx), K(part_loc_list.count()));
      } else {
        lgi_ctx->cur_part_id_ = part_loc_list.at(location_idx).get_partition_id();
        ++lgi_ctx->cur_granule_pos_;
      }
    }
  }
  if (OB_SUCC(ret)) {
    info.partition_id_ = lgi_ctx->cur_part_id_;
  }
  if (OB_SUCC(ret) && !lgi_input->param_array_list_.empty()) {
    // get parameters of partition
    int64_t part_idx = lgi_ctx->cur_granule_pos_ - 1;
    int64_t start_pos = part_idx * array_param_idxs_.count();
    if (OB_UNLIKELY(part_idx < 0) || OB_UNLIKELY(part_idx >= lgi_input->part_stmt_ids_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part idx is invalid", K(ret), K(part_idx), KPC(lgi_input));
    } else if (OB_UNLIKELY(lgi_ctx->cur_param_idx_ < 0) ||
               OB_UNLIKELY(lgi_ctx->cur_param_idx_ >= lgi_input->part_stmt_ids_.at(part_idx).count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur param idx is invalid", K(ret), KPC(lgi_ctx), K(lgi_input));
    } else {
      plan_ctx->set_cur_stmt_id(lgi_input->part_stmt_ids_.at(part_idx).at(lgi_ctx->cur_param_idx_));
    }
    for (int64_t i = start_pos; OB_SUCC(ret) && i < start_pos + array_param_idxs_.count(); ++i) {
      if (OB_UNLIKELY(i < 0) || OB_UNLIKELY(i >= lgi_input->param_array_list_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("idx is invalid", KPC(lgi_ctx), KPC(lgi_input));
      } else {
        const ObLGIInput::ObArrayParamInfo& array_param_info = lgi_input->param_array_list_.at(i);
        ObObjParam& obj_param = plan_ctx->get_param_store_for_update().at(array_param_info.param_store_idx_);
        obj_param = array_param_info.array_param_.at(lgi_ctx->cur_param_idx_);
        obj_param.set_param_meta();
        LOG_DEBUG("replace param store", K(obj_param), KPC(lgi_ctx), K(array_param_info));
      }
    }
    ++lgi_ctx->cur_param_idx_;
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(start_pos < 0) || OB_UNLIKELY(start_pos >= lgi_input->param_array_list_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("start pos is invalid", K(ret), KPC(lgi_input), K(start_pos));
      } else if (lgi_ctx->cur_param_idx_ >= lgi_input->param_array_list_.at(start_pos).array_param_.count()) {
        // reset status start iterate next partition
        lgi_ctx->cur_part_id_ = -1;
        lgi_ctx->cur_param_idx_ = 0;
      }
    }
  }

  return ret;
}

int ObLightGranuleIterator::rescan(ObExecContext& ctx) const
{
  UNUSED(ctx);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

void ObLightGranuleIterator::reset()
{
  ObSingleChildPhyOperator::reset();
}

void ObLightGranuleIterator::reuse()
{
  ObSingleChildPhyOperator::reuse();
}

int64_t ObLightGranuleIterator::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(dml_location_key), K_(dml_op_id), K_(is_pwj), K_(lgi_scan_infos), K_(array_param_idxs));
  return pos;
}

int ObLightGranuleIterator::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObLGICtx* gi_ctx = NULL;

  if (OB_FAIL(inner_create_operator_ctx(ctx, op_ctx))) {
    LOG_WARN("inner create operator context failed", K(ret));
  } else if ((OB_ISNULL(gi_ctx = GET_PHY_OPERATOR_CTX(ObLGICtx, ctx, get_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get gi ctx", K(ret));
  } else if (OB_FAIL(init_cur_row(*gi_ctx, need_copy_row_for_compute()))) {
    LOG_WARN("init cur row failed", K(ret));
  } else {
    // do nothing
  }

  return ret;
}

int ObLightGranuleIterator::inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const
{
  int ret = OB_SUCCESS;
  ObLGIInput* lgi_input = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObLGICtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create phy ctx faile", K(ret));
  } else if (OB_ISNULL(lgi_input = GET_PHY_OP_INPUT(ObLGIInput, ctx, get_id()))) {
    // construct task info for local execution LGI plan.
    ObTaskInfo task_info(ctx.get_allocator());
    if (OB_FAIL(CREATE_PHY_OP_INPUT(ObLGIInput, ctx, get_id(), get_type(), lgi_input))) {
      LOG_WARN("create physical operator input failed", K(ret));
    } else if (OB_FAIL(task_info.init_location_idx_array(1))) {
      LOG_WARN("init local location index array failed", K(ret));
    } else if (OB_FAIL(task_info.add_location_idx(0))) {
      LOG_WARN("add locaton idx to task info failed", K(ret));
    } else if (OB_FAIL(lgi_input->init(ctx, task_info, *this))) {
      LOG_WARN("init lgi input failed", K(ret));
    }
  }
  return ret;
}

int ObLightGranuleIterator::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObLGICtx* lgi_ctx = nullptr;
  ObLGIInput* lgi_input = nullptr;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("failed to init operator context", K(ret));
  } else if (OB_ISNULL(lgi_ctx = GET_PHY_OPERATOR_CTX(ObLGICtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (OB_ISNULL(lgi_input = GET_PHY_OP_INPUT(ObLGIInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator input failed", K(ret), K(get_id()));
  } else if (OB_FAIL(prepare_table_scan(ctx))) {
    LOG_WARN("prepare table scan failed", K(ret));
  }
  return ret;
}

int ObLightGranuleIterator::after_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObLGIInput* lgi_input = nullptr;
  if (OB_ISNULL(lgi_input = GET_PHY_OP_INPUT(ObLGIInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator input failed", K(ret), K(get_id()));
  } else if (OB_ISNULL(get_phy_plan())) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical plan is null", K(ret));
  } else if (get_phy_plan()->is_local_or_remote_plan() && !get_phy_plan()->has_uncertain_local_operator() &&
             !lgi_input->param_array_list_.empty()) {
    // get_next_row() for DML, since LGI need get_next_row() to iterate next param
    const ObNewRow* row = NULL;
    while (OB_SUCC(get_next_row(ctx, row)))
      ;
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLightGranuleIterator::inner_close(ObExecContext& ctx) const
{
  UNUSED(ctx);
  return OB_SUCCESS;
}

int ObLightGranuleIterator::inner_get_next_row(ObExecContext& exec_ctx, const ObNewRow*& row) const
{
  return try_get_next_row(exec_ctx, row);
}

int ObLightGranuleIterator::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObLGIInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create table scan input", K(ret), "op_id", get_id(), "op_type", get_type());
  } else {
    // do nothing
    UNUSED(input);
  }
  return ret;
}

int ObLightGranuleIterator::handle_batch_stmt_implicit_cursor(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObLGICtx* lgi_ctx = nullptr;
  ObPhysicalPlanCtx* plan_ctx = nullptr;
  if (OB_ISNULL(lgi_ctx = GET_PHY_OPERATOR_CTX(ObLGICtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lgi ctx is null", K(ret), K(get_id()));
  } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan ctx is null", K(ret), K(get_id()));
  } else if (OB_INVALID_ID != dml_op_id_) {
    /**
     *  +------------------------+
     *  | LIGHT GRANULE ITERATOR |
     *  ++-----------------------+
     *   | +------------+
     *   +->  UPDATE    |
     *     +--+---------+
     *        | +-------------+
     *        +>+ TABLE SCAN  |
     *          +-------------+
     */
    ObImplicitCursorInfo implicit_cursor;
    implicit_cursor.stmt_id_ = plan_ctx->get_cur_stmt_id();
    implicit_cursor.affected_rows_ = plan_ctx->get_affected_rows();
    implicit_cursor.found_rows_ = plan_ctx->get_found_rows();
    implicit_cursor.matched_rows_ = plan_ctx->get_row_matched_count();
    implicit_cursor.duplicated_rows_ = plan_ctx->get_row_duplicated_count();
    implicit_cursor.deleted_rows_ = plan_ctx->get_row_deleted_count();
    plan_ctx->reset_cursor_info();
    if (OB_FAIL(plan_ctx->merge_implicit_cursor_info(implicit_cursor))) {
      LOG_WARN("merge implicit cursor info to plan ctx failed", K(ret), K(implicit_cursor));
    }
  }
  return ret;
}

int ObLightGranuleIterator::try_get_next_row(ObExecContext& exec_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObLGICtx* gi_ctx = NULL;
  bool got_next_row = false;

  if ((OB_ISNULL(gi_ctx = GET_PHY_OPERATOR_CTX(ObLGICtx, exec_ctx, get_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get gi ctx", K(ret));
  } else {
    do {
      switch (gi_ctx->state_) {
        case LGI_UNINITIALIZED: {
          gi_ctx->state_ = LGI_GET_NEXT_GRANULE_TASK;
          break;
        }
        case LGI_GET_NEXT_GRANULE_TASK: {
          if (OB_FAIL(do_get_next_granule_task(*gi_ctx, false, exec_ctx))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("fail to get next granule task", K(ret));
            }
          }
          break;
        }
        case LGI_TABLE_SCAN: {
          if (OB_FAIL(child_op_->get_next_row(exec_ctx, row))) {
            LOG_DEBUG("failed to get new row", K(ret), K(lgi_scan_infos_));
            if (OB_ITER_END != ret) {
              LOG_WARN("try fetch task failed", K(ret));
            } else if (!array_param_idxs_.empty()) {
              // is batched update, handl implicit cursor
              if (OB_FAIL(handle_batch_stmt_implicit_cursor(exec_ctx))) {
                LOG_WARN("handle batch stmt inlicit cursor failed", K(ret));
              }
            } else {
              ret = OB_SUCCESS;
            }
            gi_ctx->state_ = LGI_GET_NEXT_GRANULE_TASK;
          } else if (OB_FAIL(copy_cur_row(*gi_ctx, row))) {
            LOG_WARN("copy row failed", K(ret));
          } else {
            LOG_DEBUG("get new row", KPC(row), K(lgi_scan_infos_));
            got_next_row = true;
          }
          break;
        }
        case LGI_END: {
          ret = OB_ITER_END;
          got_next_row = true;
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected state", K(ret), K(gi_ctx->state_));
        }
      }
    } while (OB_SUCC(ret) && !got_next_row);
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
int ObLightGranuleIterator::do_get_next_granule_task(ObLGICtx& lgi_ctx, bool is_prepare, ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObGranuleTaskInfo, 4> gi_task_infos;
  GIPrepareTaskMap* gi_prepare_map = nullptr;
  ObExecContext::ObPlanRestartGuard restart_plan(exec_ctx);
  IGNORE_RETURN exec_ctx.set_gi_restart();
  if (OB_FAIL(exec_ctx.get_gi_task_map(gi_prepare_map))) {
    LOG_WARN("Failed to get gi task map", K(ret));
  } else if (!is_pwj_) {
    ObGranuleTaskInfo gi_task_info;
    /* non-partition wise join */
    if (OB_FAIL(try_fetch_task(exec_ctx, gi_task_info))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("try fetch task failed", K(ret));
      } else {
        lgi_ctx.state_ = LGI_END;
      }
    } else {
      const ObLGIScanInfo& lgi_scan_info = lgi_scan_infos_.at(0);
      if (OB_NOT_NULL(gi_prepare_map->get(lgi_scan_info.tsc_op_id_))) {
        if (OB_FAIL(gi_prepare_map->erase_refactored(lgi_scan_info.tsc_op_id_))) {
          LOG_WARN("failed to erase tsc task",
              K(lgi_scan_info.table_location_key_),
              K(dml_location_key_),
              K(lgi_scan_info.tsc_op_id_),
              K(dml_op_id_),
              K(ret));
        } else if (OB_LIKELY(OB_INVALID_ID != dml_op_id_)) {
          if (OB_FAIL(gi_prepare_map->erase_refactored(dml_op_id_))) {
            LOG_WARN("failed to erase dml task",
                K(lgi_scan_info.table_location_key_),
                K(dml_location_key_),
                K(lgi_scan_info.tsc_op_id_),
                K(dml_op_id_),
                K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(gi_prepare_map->set_refactored(lgi_scan_info.tsc_op_id_, gi_task_info))) {
          LOG_WARN("reset table scan's ranges failed", K(ret), K(lgi_scan_info), K(gi_task_info));
        } else if (OB_LIKELY(OB_INVALID_ID != dml_op_id_)) {
          if (OB_FAIL(gi_prepare_map->set_refactored(dml_op_id_, gi_task_info))) {
            LOG_WARN("reset table scan's ranges failed", K(ret), K(dml_op_id_), K(gi_task_info));
          }
        }
      }
      /**
       * In inner open stage, the prepare is true.
       * Tsc will do scan in his inner open, so we should skip rescan action.
       */
      if (OB_SUCC(ret) && !is_prepare) {
        if (OB_FAIL(child_op_->rescan(exec_ctx))) {
          LOG_WARN("fail to rescan gi' child", K(ret));
        }
      }
      lgi_ctx.state_ = LGI_TABLE_SCAN;
      LOG_DEBUG("produce a gi task", K(ret), K(lgi_scan_infos_), K(dml_op_id_), K(gi_task_info));
    }
  } else {
    /* partition wise join */
    ObSEArray<ObGranuleTaskInfo, 2> lgi_task_infos;
    if (OB_FAIL(try_fetch_task_with_pwj(exec_ctx, lgi_task_infos))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("try fetch task failed", K(ret));
      } else {
        lgi_ctx.state_ = LGI_END;
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < lgi_task_infos.count(); ++i) {
        const ObLGIScanInfo& lgi_scan_info = lgi_scan_infos_.at(i);
        const ObGranuleTaskInfo gi_task_info = lgi_task_infos.at(i);
        if (OB_NOT_NULL(gi_prepare_map->get(lgi_scan_info.tsc_op_id_))) {
          if (OB_FAIL(gi_prepare_map->erase_refactored(lgi_scan_info.tsc_op_id_))) {
            LOG_WARN("failed to erase tsc task",
                K(lgi_scan_info.table_location_key_),
                K(lgi_scan_info.tsc_op_id_),
                K(dml_op_id_),
                K(ret));
          } else if (lgi_scan_info.table_location_key_ == dml_location_key_) {
            if (OB_FAIL(gi_prepare_map->erase_refactored(dml_op_id_))) {
              LOG_WARN("failed to erase tsc task",
                  K(lgi_scan_info.table_location_key_),
                  K(lgi_scan_info.tsc_op_id_),
                  K(dml_op_id_),
                  K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(gi_prepare_map->set_refactored(lgi_scan_info.tsc_op_id_, gi_task_info))) {
            LOG_WARN("reset table scan's ranges failed", K(ret), K(lgi_scan_info), K(gi_task_info));
          } else if (OB_LIKELY(lgi_scan_info.table_location_key_ == dml_location_key_)) {
            if (OB_FAIL(gi_prepare_map->set_refactored(dml_op_id_, gi_task_info))) {
              LOG_WARN("reset table scan's ranges failed", K(ret), K(dml_op_id_), K(gi_task_info));
            }
          }
        }
      }
      if (OB_SUCC(ret) && !is_prepare) {
        if (OB_FAIL(child_op_->rescan(exec_ctx))) {
          LOG_WARN("fail to rescan gi' child", K(ret));
        }
      }
      lgi_ctx.state_ = LGI_TABLE_SCAN;
      LOG_DEBUG("produce a gi task", K(ret), K(lgi_scan_infos_), K(dml_op_id_), K(lgi_task_infos));
    }
  }
  IGNORE_RETURN exec_ctx.reset_gi_restart();
  return ret;
}

/*
 * At the moment we open this operator, we try to get granule task and give
 * it to the tsc operator below. So, after we open this operator, the tsc is
 * ready for reading.
 *
 * */
int ObLightGranuleIterator::prepare_table_scan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObLGICtx* gi_ctx = nullptr;
  if (OB_ISNULL(gi_ctx = GET_PHY_OPERATOR_CTX(ObLGICtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (!gi_ctx->is_not_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected gi state", K(ret), K(gi_ctx->state_));
  } else if (FALSE_IT(gi_ctx->state_ = LGI_GET_NEXT_GRANULE_TASK)) {
  } else if (OB_FAIL(do_get_next_granule_task(*gi_ctx, true /* prapare */, ctx))) {
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
    gi_ctx->state_ = LGI_TABLE_SCAN;
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("do prepare table scan", K(ret), K(gi_ctx->state_));
  }
  return ret;
}

OB_SERIALIZE_MEMBER_SIMPLE(ObLightGranuleIterator::ObLGIScanInfo, ref_table_id_, table_location_key_, tsc_op_id_);

OB_SERIALIZE_MEMBER((ObLightGranuleIterator, ObSingleChildPhyOperator), dml_location_key_, dml_op_id_, is_pwj_,
    lgi_scan_infos_, array_param_idxs_);

}  // namespace sql
}  // namespace oceanbase
