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
#include "lib/json/ob_json_print_utils.h"
#include "sql/engine/dml/ob_multi_dml_plan_mgr.h"
#include "sql/engine/dml/ob_table_modify.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/executor/ob_task_spliter.h"
#include "lib/utility/ob_macro_utils.h"
namespace oceanbase {
using namespace common;
using namespace share;
namespace sql {
class ObMultiDMLPlanMgr::ServerOpInfo {
public:
  ServerOpInfo()
  {}
  inline void reset()
  {
    runner_server_.reset();
    part_values_.reset();
  }
  TO_STRING_KV(K_(runner_server), K_(part_values));

  ObAddr runner_server_;
  ObSEArray<PartValuesInfo, 1> part_values_;
};

void ObMultiDMLPlanMgr::reset()
{
  table_dml_ctxs_ = NULL;
  exec_ctx_ = NULL;
  table_need_first_ = false;
  for (int64_t i = 0; i < mini_task_infos_.count(); ++i) {
    if (mini_task_infos_.at(i) != NULL) {
      mini_task_infos_.at(i)->~ObTaskInfo();
      mini_task_infos_.at(i) = NULL;
    }
  }
  mini_task_infos_.reset();
  release_part_row_store();
}

void ObMultiDMLPlanMgr::release_part_row_store()
{
  for (int64_t i = 0; i < table_subplan_array_.count(); ++i) {
    for (int64_t j = 0; j < table_subplan_array_.at(i).count(); ++j) {
      table_subplan_array_.at(i).at(j).reset();
    }
  }
}

int ObMultiDMLPlanMgr::init(ObExecContext* exec_ctx, ObIArrayWrap<ObTableDMLCtx>* table_dml_ctxs,
    const ObPhysicalPlan* my_plan, const ObPhyOperator* subplan_root, const ObOpSpec* se_subplan_root)
{
  int ret = OB_SUCCESS;
  table_dml_ctxs_ = table_dml_ctxs;
  exec_ctx_ = exec_ctx;
  subplan_job_.set_phy_plan(my_plan);
  subplan_job_.set_root_op(subplan_root);
  subplan_job_.set_root_spec(se_subplan_root);
  CK(OB_NOT_NULL(table_dml_ctxs));
  OZ(table_subplan_array_.allocate_array(allocator_, table_dml_ctxs->count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < table_subplan_array_.count(); ++i) {
    OZ(table_subplan_array_.at(i).allocate_array(allocator_, table_dml_ctxs->at(i).index_ctxs_.count()));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if ((NULL != se_subplan_root) && OB_FAIL(se_subplan_root->create_op_input(*exec_ctx))) {
    LOG_WARN("create op input failed", K(ret));
  }
  return ret;
}

int ObMultiDMLPlanMgr::add_part_row(
    int64_t table_idx, int64_t index_idx, int64_t part_idx, int64_t op, const ObNewRow& row)
{
  int ret = OB_SUCCESS;
  PartValuesInfo* part_info = NULL;
  if (OB_FAIL(get_or_create_part_subplan_info(table_idx, index_idx, part_idx, op, part_info))) {
    LOG_WARN("get or create part subplan info failed", K(ret));
  } else if (OB_ISNULL(part_info) || OB_ISNULL(part_info->row_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subplan part info is invalid", KPC(part_info));
  } else if (OB_FAIL(part_info->row_store_->add_row(row))) {
    LOG_WARN("add row to part row store failed", K(ret));
  }
  return ret;
}

int ObMultiDMLPlanMgr::add_part_row(
    int64_t table_idx, int64_t index_idx, int64_t part_idx, int64_t op, const ObIArray<ObExpr*>& row)
{
  int ret = OB_SUCCESS;
  PartValuesInfo* part_info = NULL;
  if (OB_FAIL(get_or_create_part_subplan_info(table_idx, index_idx, part_idx, op, part_info, true /*static_engine*/))) {
    LOG_WARN("get or create part subplan info failed", K(ret));
  } else if (OB_ISNULL(part_info) || OB_ISNULL(part_info->datum_store_) || OB_ISNULL(exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subplan part info is invalid", KPC(part_info));
  } else if (OB_FAIL(part_info->datum_store_->add_row(row, exec_ctx_->get_eval_ctx()))) {
    LOG_WARN("add row to part row store failed", K(ret));
  }
  return ret;
}

int ObMultiDMLPlanMgr::get_or_create_part_subplan_info(int64_t table_idx, int64_t index_idx, int64_t part_idx,
    int64_t op, PartValuesInfo*& part_info, bool is_static_engine /*= false*/)
{
  int ret = OB_SUCCESS;
  PartSubPlanArray* part_subplan_array = NULL;
  CK(OB_NOT_NULL(table_dml_ctxs_));
  CK(table_idx < table_subplan_array_.count() && table_idx >= 0);
  CK(index_idx < table_subplan_array_.at(table_idx).count() && index_idx >= 0);
  if (OB_SUCC(ret)) {
    IndexSubPlanInfo& index_subplan_info = table_subplan_array_.at(table_idx).at(index_idx);
    CK(part_idx >= 0 && part_idx <= index_subplan_info.count());
    if (OB_SUCC(ret) && part_idx == index_subplan_info.count()) {
      PartSubPlanArray empty_array;
      OZ(index_subplan_info.push_back(empty_array));
    }
    if (OB_SUCC(ret)) {
      part_subplan_array = &(index_subplan_info.at(part_idx));
    }
  }
  if (OB_SUCC(ret) && part_subplan_array->count() <= 0) {
    int64_t count = 0;
    if (is_static_engine) {
      count = table_dml_ctxs_->at(table_idx).index_ctxs_.at(index_idx).se_subplans_.count();
    } else {
      count = table_dml_ctxs_->at(table_idx).index_ctxs_.at(index_idx).dml_subplans_.count();
    }
    if (OB_FAIL(part_subplan_array->allocate_array(allocator_, count))) {
      LOG_WARN("prepare allocate values op array failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(op < 0) || OB_UNLIKELY(op >= part_subplan_array->count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid op", K(op), K(part_subplan_array->count()));
    } else if ((!is_static_engine && OB_ISNULL(part_subplan_array->at(op).row_store_)) ||
               (is_static_engine && OB_ISNULL(part_subplan_array->at(op).datum_store_))) {
      ObGlobalIndexDMLCtx& index_dml_ctx = table_dml_ctxs_->at(table_idx).index_ctxs_.at(index_idx);
      void* ptr = NULL;
      if (OB_FAIL(part_subplan_array->at(op).part_key_.init(
              index_dml_ctx.index_tid_, index_dml_ctx.partition_ids_.at(part_idx), index_dml_ctx.part_cnt_))) {
        LOG_WARN("init partition key failed", K(ret));
      } else if (is_static_engine) {
        const ObOpSpec* spec = NULL;
        if (OB_ISNULL(spec = index_dml_ctx.se_subplans_.at(op).subplan_root_) || OB_ISNULL(spec->get_child())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("physical operator is null", KP(spec));
        } else {
          part_subplan_array->at(op).part_key_ref_id_ = spec->id_;
          part_subplan_array->at(op).value_ref_id_ = spec->get_child()->id_;
        }
      } else {
        ObPhyOperator* phy_op = NULL;
        if (OB_ISNULL(phy_op = index_dml_ctx.dml_subplans_.at(op).subplan_root_) || OB_ISNULL(phy_op->get_child(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("physical operator is null", K(phy_op));
        } else {
          part_subplan_array->at(op).part_key_ref_id_ = phy_op->get_id();
          part_subplan_array->at(op).value_ref_id_ = phy_op->get_child(0)->get_id();
        }
      }
      if (OB_SUCC(ret)) {
        if (is_static_engine) {
          if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObChunkDatumStore)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc row store failed", K(ret), K(sizeof(ObChunkDatumStore)));
          } else {
            ObChunkDatumStore* datum_store = new (ptr) ObChunkDatumStore(&allocator_);
            if (OB_FAIL(datum_store->init(UINT64_MAX,
                    OB_SERVER_TENANT_ID,
                    ObCtxIds::DEFAULT_CTX_ID,
                    ObModIds::OB_SQL_CHUNK_ROW_STORE,
                    false /*enable_dump*/))) {
              LOG_WARN("fail to init datum store", K(ret));
            } else {
              part_subplan_array->at(op).datum_store_ = datum_store;
            }
          }
        } else {
          if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObRowStore)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc row store failed", K(ret), K(sizeof(ObRowStore)));
          } else {
            part_subplan_array->at(op).row_store_ = new (ptr) ObRowStore(allocator_);
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      part_info = &part_subplan_array->at(op);
    }
  }
  return ret;
}

int ObMultiDMLPlanMgr::build_multi_part_dml_task()
{
  int ret = OB_SUCCESS;
  ObAddr cur_runner_server;
  ObAddr table_runner_server;
  table_need_first_ = true;
  ObSEArray<ServerOpInfo, 2> server_ops;
  ServerOpInfo* server_op = NULL;
  if (OB_ISNULL(table_dml_ctxs_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("dml_table_infos_ is null");
  }
  for (int64_t k = 0; OB_SUCC(ret) && k < table_dml_ctxs_->count(); ++k) {
    ObArrayWrap<ObGlobalIndexDMLCtx>& index_ctxs = table_dml_ctxs_->at(k).index_ctxs_;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_ctxs.count(); ++i) {
      ObGlobalIndexDMLCtx& dml_table_ctx = index_ctxs.at(i);
      uint64_t table_id = dml_table_ctx.table_id_;
      uint64_t index_tid = dml_table_ctx.index_tid_;
      ObIArray<int64_t>& part_ids = dml_table_ctx.partition_ids_;
      for (int64_t j = 0; OB_SUCC(ret) && j < part_ids.count(); ++j) {
        cur_runner_server.reset();
        if (OB_FAIL(get_runner_server(table_id, index_tid, part_ids.at(j), cur_runner_server))) {
          LOG_WARN("get runner server failed", K(ret), K(table_id), K(index_tid), K(part_ids.at(j)));
        } else if (dml_table_ctx.is_table_) {
          if (!table_runner_server.is_valid()) {
            table_runner_server = cur_runner_server;
          } else if (table_runner_server != cur_runner_server) {
            table_need_first_ = false;
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(get_server_op_info(server_ops, cur_runner_server, server_op))) {
            LOG_WARN("get server op info failed", K(ret));
          }
          CK(OB_NOT_NULL(server_op));
        }
        const PartSubPlanArray& part_subplan_array = table_subplan_array_.at(k).at(i).at(j);
        for (int64_t idx = 0; OB_SUCC(ret) && idx < part_subplan_array.count(); ++idx) {
          if (part_subplan_array.at(idx).row_store_ != NULL || part_subplan_array.at(idx).datum_store_ != NULL) {
            if (OB_FAIL(server_op->part_values_.push_back(part_subplan_array.at(idx)))) {
              LOG_WARN("store partition values failed", K(ret));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (table_need_first_ && !table_runner_server.is_valid()) {
      table_need_first_ = false;
    }
    if (!table_need_first_) {
      table_runner_server.reset();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(generate_all_task_info(table_runner_server, server_ops))) {
      LOG_WARN("generate all dml subplan failed", K(ret));
    }
  }
  return ret;
}

int ObMultiDMLPlanMgr::get_server_op_info(
    ObIArray<ServerOpInfo>& server_ops, const ObAddr& runner_server, ServerOpInfo*& server_op)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < server_ops.count(); ++i) {
    if (runner_server == server_ops.at(i).runner_server_) {
      server_op = &(server_ops.at(i));
      is_found = true;
    }
  }
  if (!is_found) {
    ServerOpInfo empty_op;
    empty_op.runner_server_ = runner_server;
    if (OB_FAIL(server_ops.push_back(empty_op))) {
      LOG_WARN("store empty op failed", K(ret));
    } else {
      server_op = &(server_ops.at(server_ops.count() - 1));
    }
  }
  return ret;
}

int ObMultiDMLPlanMgr::get_runner_server(uint64_t table_id, uint64_t index_tid, int64_t part_id, ObAddr& runner_server)
{
  int ret = OB_SUCCESS;
  const ObPhyTableLocation* table_location = NULL;
  const ObPartitionReplicaLocation* part_replica = NULL;
  if (OB_ISNULL(exec_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("exec_ctx is null");
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(*exec_ctx_, table_id, index_tid, table_location))) {
    LOG_WARN("get physical table location failed", K(ret));
  } else if (OB_ISNULL(table_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy table location is null", K(table_id), K(ret));
  } else if (OB_ISNULL(part_replica = table_location->get_part_replic_by_part_id(part_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy part replica is null", K(part_id));
  } else {
    runner_server = part_replica->get_replica_location().server_;
  }
  return ret;
}

int ObMultiDMLPlanMgr::allocate_mini_task_info(ObTaskInfo*& task_info)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObTaskInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate task info failed", K(ret), K(sizeof(ObTaskInfo)));
  } else {
    task_info = new (ptr) ObTaskInfo(allocator_);
    if (OB_FAIL(mini_task_infos_.push_back(task_info))) {
      LOG_WARN("store mini task info failed", K(ret));
      task_info->~ObTaskInfo();
      task_info = NULL;
    }
  }
  return ret;
}

int ObMultiDMLPlanMgr::generate_all_task_info(const ObAddr& table_runner_server, ObIArray<ServerOpInfo>& server_ops)
{
  int ret = OB_SUCCESS;
  ObJobID ob_job_id;
  int64_t op_id = 0;
  if (OB_FAIL(mini_task_infos_.init(server_ops.count()))) {
    LOG_WARN("init dml task list failed", K(ret));
  } else if (OB_ISNULL(subplan_job_.get_phy_plan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(subplan_job_));
  } else if (subplan_job_.get_phy_plan()->is_new_engine()) {
    if (OB_ISNULL(subplan_job_.get_root_spec())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subplan root spec is null", K(ret));
    } else {
      op_id = subplan_job_.get_root_spec()->id_;
    }
  } else {
    if (OB_ISNULL(subplan_job_.get_root_op())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subplan root op is null", K(ret));
    } else {
      op_id = subplan_job_.get_root_op()->get_id();
    }
  }
  if (OB_SUCC(ret)) {
    ObCurTraceId::TraceId execution_id;
    execution_id.init(ObCurTraceId::get_addr());
    ob_job_id.set_mini_task_type();
    ob_job_id.set_server(execution_id.get_addr());
    ob_job_id.set_execution_id(execution_id.get_seq());
    ob_job_id.set_job_id(op_id);
    if (table_need_first_) {
      ObTaskInfo* empty_task = NULL;
      if (OB_FAIL(allocate_mini_task_info(empty_task))) {
        LOG_WARN("store empty dml task failed", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < server_ops.count(); ++i) {
    ServerOpInfo& server_op = server_ops.at(i);
    if (table_runner_server == server_op.runner_server_) {
      if (OB_FAIL(generate_mini_task_info(server_op, ob_job_id, 0, *mini_task_infos_.at(0)))) {
        LOG_WARN("genenrate first table dml task failed", K(ret));
      }
    } else {
      ObTaskInfo* new_task_info = NULL;
      if (OB_FAIL(allocate_mini_task_info(new_task_info))) {
        LOG_WARN("allocate new mini task list failed", K(ret));
      } else if (OB_FAIL(generate_mini_task_info(server_op, ob_job_id, i, *new_task_info))) {
        LOG_WARN("generate new mini task failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("all mini task info", "mini job", SJ(subplan_job_), K_(mini_task_infos));
  }
  return ret;
}

int ObMultiDMLPlanMgr::generate_mini_task_info(
    ServerOpInfo server_op, const ObJobID& ob_job_id, uint64_t task_id, ObTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObTaskLocation task_loc;
  ObTaskInfo::ObPartLoc part_loc;
  int64_t values_cnt = server_op.part_values_.count();
  task_loc.set_ob_job_id(ob_job_id);
  task_loc.set_task_id(task_id);
  task_loc.set_server(server_op.runner_server_);
  task_info.set_task_location(task_loc);
  task_info.set_task_split_type(ObTaskSpliter::DISTRIBUTED_SPLIT);
  task_info.get_range_location().server_ = server_op.runner_server_;
  task_info.get_range_location().part_locs_.set_allocator(&allocator_);
  if (OB_FAIL(task_info.get_range_location().part_locs_.init(values_cnt))) {
    LOG_WARN("init task info partition location failed", K(ret), K(values_cnt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < values_cnt; ++i) {
    part_loc.reset();
    part_loc.partition_key_ = server_op.part_values_.at(i).part_key_;
    part_loc.row_store_ = server_op.part_values_.at(i).row_store_;
    part_loc.datum_store_ = server_op.part_values_.at(i).datum_store_;
    part_loc.part_key_ref_id_ = server_op.part_values_.at(i).part_key_ref_id_;
    part_loc.value_ref_id_ = server_op.part_values_.at(i).value_ref_id_;
    if (OB_FAIL(task_info.get_range_location().part_locs_.push_back(part_loc))) {
      LOG_WARN("store partition location failed", K(ret));
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
