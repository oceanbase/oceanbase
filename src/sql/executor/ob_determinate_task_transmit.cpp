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

#define USING_LOG_PREFIX SQL_EXE

#include "ob_determinate_task_transmit.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/optimizer/ob_table_location.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase {
namespace sql {

using namespace common;
using namespace share;

OB_SERIALIZE_MEMBER(ObDeterminateTaskTransmit::TaskIndex, loc_idx_, part_loc_idx_);

OB_SERIALIZE_MEMBER(ObDeterminateTaskTransmit::IdRange, begin_, end_);

OB_SERIALIZE_MEMBER(ObDeterminateTaskTransmit::ResultRange, task_range_, slice_range_);

struct ObDeterminateTaskTransmit::RangeStartCompare {
  RangeStartCompare(int& ret) : ret_(ret)
  {}
  bool operator()(const ObNewRange& range, const ObNewRow& row)
  {
    start_row_.assign(const_cast<ObObj*>(range.start_key_.get_obj_ptr()), range.start_key_.get_obj_cnt());
    int cmp = 0;
    if (OB_SUCCESS == ret_) {
      ret_ = ObRowUtil::compare_row(start_row_, row, cmp);
      if (OB_SUCCESS != ret_) {
        LOG_WARN("compare row failed", K(ret_));
      } else {
        if (0 == cmp && !range.border_flag_.inclusive_start()) {
          cmp = 1;
        }
      }
    }
    return cmp < 0;
  }

private:
  ObNewRow start_row_;
  int& ret_;
};

OB_DEF_SERIALIZE(ObDeterminateTaskTransmit)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObDeterminateTaskTransmit, ObDistributedTransmit));
  LST_DO_CODE(OB_UNIS_ENCODE, result_reusable_, shuffle_by_part_, shuffle_by_range_, start_slice_ids_, result_mapping_);
  int64_t cnt = shuffle_ranges_.count();
  LST_DO_CODE(OB_UNIS_ENCODE, cnt);
  FOREACH_CNT_X(ranges, shuffle_ranges_, OB_SUCC(ret))
  {
    LST_DO_CODE(OB_UNIS_ENCODE, *ranges);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDeterminateTaskTransmit)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObDeterminateTaskTransmit, ObDistributedTransmit));
  LST_DO_CODE(OB_UNIS_DECODE, result_reusable_, shuffle_by_part_, shuffle_by_range_, start_slice_ids_, result_mapping_);
  int64_t cnt = 0;
  LST_DO_CODE(OB_UNIS_DECODE, cnt);
  if (OB_ISNULL(get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL physical plan", K(ret));
  }
  if (OB_SUCC(ret) && cnt > 0) {
    if (OB_FAIL(shuffle_ranges_.prepare_allocate(cnt))) {
      LOG_WARN("fix array prepare allocate failed", K(ret), K(cnt));
    } else {
      FOREACH_CNT_X(ranges, shuffle_ranges_, OB_SUCC(ret))
      {
        ranges->set_allocator(&const_cast<ObPhysicalPlan*>(get_phy_plan())->get_allocator());
        LST_DO_CODE(OB_UNIS_DECODE, *ranges);
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDeterminateTaskTransmit)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  BASE_ADD_LEN((ObDeterminateTaskTransmit, ObDistributedTransmit));
  LST_DO_CODE(
      OB_UNIS_ADD_LEN, result_reusable_, shuffle_by_part_, shuffle_by_range_, start_slice_ids_, result_mapping_);
  int64_t cnt = shuffle_ranges_.count();
  LST_DO_CODE(OB_UNIS_ADD_LEN, cnt);
  FOREACH_CNT_X(ranges, shuffle_ranges_, OB_SUCC(ret))
  {
    LST_DO_CODE(OB_UNIS_ADD_LEN, *ranges);
  }
  return len;
}

ObDeterminateTaskTransmit::ObDeterminateTaskTransmit(ObIAllocator& alloc)
    : ObDistributedTransmit(alloc),
      result_reusable_(false),
      range_locations_(alloc),
      tasks_(alloc),
      shuffle_by_part_(false),
      shuffle_by_range_(false),
      shuffle_ranges_(alloc),
      start_slice_ids_(alloc),
      result_mapping_(alloc),
      task_route_policy_(ITaskRouting::DATA_REPLICA_PICKER),
      task_routing_(NULL),
      background_(false)
{}

int ObDeterminateTaskTransmit::init_op_ctx(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObDeterminateTaskTransmitCtx, exec_ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL operator context", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, false))) {
    LOG_WARN("init current row failed", K(ret));
  }

  return ret;
}

OperatorOpenOrder ObDeterminateTaskTransmit::get_operator_open_order(ObExecContext& ctx) const
{
  UNUSED(ctx);
  OperatorOpenOrder open_order = OPEN_CHILDREN_FIRST;
  if (result_reusable_) {
    open_order = OPEN_SELF_ONLY;
  }
  return open_order;
}

int ObDeterminateTaskTransmit::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = GET_MY_SESSION(exec_ctx);
  ObDistributedTransmitInput* input = GET_PHY_OP_INPUT(ObDistributedTransmitInput, exec_ctx, get_id());
  ObIntermResultManager* result_mgr = ObIntermResultManager::get_instance();
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  ObDeterminateTaskTransmitCtx* op_ctx = NULL;
  ObIntermResult** results = NULL;
  TaskIDSet* tasks = executing_tasks();
  bool task_added = false;
  ObIntermResultInfo res_key;
  ObSliceID slice_id;
  if (NULL != input) {
    slice_id.set_ob_task_id(input->get_ob_task_id());
  }

  // check whether interm result already exist
  bool reuse_result = result_reusable_;
  if (OB_ISNULL(child_op_) || get_split_task_count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not inited", K(ret), KP(child_op_), "task_cnt", get_split_task_count());
  } else if (OB_ISNULL(session) || OB_ISNULL(input) || OB_ISNULL(result_mgr) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got NULL context", KP(session), KP(input), KP(result_mgr), KP(plan_ctx));
  } else if (NULL == tasks) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("NULL tasks", K(ret));
  } else if (OB_FAIL(ObTransmit::inner_open(exec_ctx))) {
    LOG_WARN("init operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx = GET_PHY_OPERATOR_CTX(ObDeterminateTaskTransmitCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get operator ctx failed", K(ret));
  } else if (OB_FAIL(tasks->set_refactored(input->get_ob_task_id(), 0 /* no overwrite */))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_ENTRY_EXIST;
    }
    LOG_WARN("add task to set failed, may be in executing", K(ret), "task_id", input->get_ob_task_id());
  } else {
    task_added = true;
    for (int64_t i = 0; OB_SUCC(ret) && reuse_result && i < get_split_task_count(); i++) {
      slice_id.set_slice_id(i);
      res_key.init(slice_id);
      if (OB_FAIL(result_mgr->update_expire_time(res_key, input->get_expire_time()))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          reuse_result = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("update expire time failed", K(ret), K(res_key), "expire_time", input->get_expire_time());
        }
      }
    }
    if (!reuse_result) {
      if (OB_FAIL(delete_all_results(*result_mgr, input->get_ob_task_id(), get_split_task_count()))) {
        LOG_WARN("delete all results failed", K(ret));
      }
      if (OB_SUCC(ret) && OPEN_SELF_ONLY == get_operator_open_order(exec_ctx)) {
        if (OB_FAIL(child_op_->open(exec_ctx))) {
          LOG_WARN("open child failed", K(ret));
        }
        op_ctx->close_child_manually_ = true;
      }
    }
  }

  // generate interm result
  if (OB_SUCC(ret) && !reuse_result) {
    ObTableLocation table_location;
    Id2IdxMap partition_id2idx_map;
    schema::ObSchemaGetterGuard schema_guard;
    ObSqlSchemaGuard sql_schema_guard;
    const int check_status_per_row = E(EventTable::EN_BKGD_TRANSMIT_CHECK_STATUS_PER_ROW) 0;
    if (OB_ISNULL(exec_ctx.get_task_exec_ctx().schema_service_) || OB_ISNULL(get_phy_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL schema service or physical plan", K(ret));
    } else if (OB_FAIL(exec_ctx.get_task_exec_ctx().schema_service_->get_tenant_schema_guard(
                   session->get_effective_tenant_id(), schema_guard, get_phy_plan()->get_tenant_schema_version()))) {
      LOG_WARN("get schema guard failed", K(ret));
    } else if (FALSE_IT(sql_schema_guard.set_schema_guard(&schema_guard))) {
    } else if (OB_FAIL(alloc_result_array(exec_ctx, *result_mgr, get_split_task_count(), results))) {
      LOG_WARN("alloc result array failed", K(ret));
    } else {
      const ObNewRow* row = NULL;
      while (OB_SUCC(ret)) {
        int64_t slice_idx = -1;
        if (0 != check_status_per_row && OB_FAIL(check_status(exec_ctx))) {
          LOG_WARN("check status failed", K(ret));
        } else if (OB_FAIL(try_check_status(exec_ctx))) {
          LOG_WARN("check status failed", K(ret));
        } else if (OB_FAIL(get_next_row(exec_ctx, row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get row from child failed", K(ret));
          } else {
            ret = OB_SUCCESS;
            auto& res = *results[0];
            res.set_found_rows(plan_ctx->get_found_rows());
            res.set_affected_rows(plan_ctx->get_affected_rows());
            res.set_matched_rows(plan_ctx->get_row_matched_count());
            res.set_duplicated_rows(plan_ctx->get_row_duplicated_count());
            res.set_last_insert_id_session(plan_ctx->calc_last_insert_id_session());
            if (!plan_ctx->is_result_accurate()) {
              res.set_is_result_accurate(false);
            }
          }
          break;
        } else if (OB_ISNULL(row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL row returned", K(ret));
        } else if (OB_FAIL(shuffle_row(
                       exec_ctx, sql_schema_guard, table_location, partition_id2idx_map, *row, slice_idx))) {
          LOG_WARN("shuffle row failed", K(ret));
        } else if (slice_idx < 0 || slice_idx >= get_split_task_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid slice idx", K(ret), K(slice_idx), "split_task_cnt", get_split_task_count());
        } else if (OB_FAIL(results[slice_idx]->add_row(session->get_effective_tenant_id(), *row))) {
          LOG_WARN("add row to interm result failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < get_split_task_count(); i++) {
        slice_id.set_slice_id(i);
        res_key.init(slice_id);
        if (OB_FAIL(results[i]->complete_add_rows(session->get_effective_tenant_id()))) {
          LOG_WARN("complete add rows failed", K(ret));
        } else if (OB_FAIL(result_mgr->add_result(res_key, results[i], input->get_expire_time()))) {
          LOG_WARN("add result failed", K(ret));
        } else {
          results[i] = NULL;
        }
      }
    }
  }

  // build slice events
  if (OB_SUCC(ret)) {
    auto events = input->get_slice_events_for_update();
    if (OB_ISNULL(events)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL slice events", K(ret));
    } else {
      events->reset();
      if (OB_FAIL(events->prepare_allocate(get_split_task_count()))) {
        LOG_WARN("array prepare allocate failed", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < get_split_task_count(); i++) {
          slice_id.set_slice_id(i);
          res_key.init(slice_id);
          events->at(i).set_ob_slice_id(slice_id);
        }
      }
    }
  }

  if (OB_FAIL(ret) && NULL != results) {
    int tmp_ret = delete_all_results(*result_mgr, input->get_ob_task_id(), get_split_task_count());
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("delete added results failed", K(tmp_ret));
    }
    tmp_ret = free_result_array(*result_mgr, get_split_task_count(), results);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("free result array failed", K(tmp_ret));
    }
  }

  if (task_added) {
    int tmp_ret = tasks->erase_refactored(input->get_ob_task_id());
    if (OB_SUCCESS != tmp_ret) {
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }

  return ret;
}

int ObDeterminateTaskTransmit::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;

  auto* op_ctx = GET_PHY_OPERATOR_CTX(ObDeterminateTaskTransmitCtx, ctx, get_id());
  if (NULL != op_ctx && NULL != child_op_ && op_ctx->close_child_manually_) {
    if (OB_FAIL(child_op_->close(ctx))) {
      LOG_WARN("close child failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDistributedTransmit::inner_close(ctx))) {
      LOG_WARN("distributed transmit close failed", K(ret));
    }
  }
  return ret;
}

int ObDeterminateTaskTransmit::alloc_result_array(
    ObExecContext& exec_ctx, ObIntermResultManager& mgr, const int64_t cnt, ObIntermResult**& results) const
{
  int ret = OB_SUCCESS;
  results = NULL;
  if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transmit op has no child", K(ret));
  } else if (cnt > 0) {
    results = static_cast<ObIntermResult**>(exec_ctx.get_allocator().alloc(sizeof(ObIntermResult*) * cnt));
    if (OB_ISNULL(results)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret));
    } else {
      MEMSET(results, 0, sizeof(ObIntermResult*) * cnt);
      for (int64_t i = 0; OB_SUCC(ret) && i < cnt; i++) {
        if (OB_FAIL(mgr.alloc_result(results[i]))) {
          LOG_WARN("alloc result failed", K(ret));
        } else if (NULL == results[i]) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL result", K(ret));
        } else {
          results[i]->set_row_reclaim_func(child_op_->reclaim_row_func());
        }
      }
      if (OB_FAIL(ret)) {
        int tmp_ret = free_result_array(mgr, cnt, results);
        LOG_WARN("free result array failed", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObDeterminateTaskTransmit::free_result_array(
    ObIntermResultManager& mgr, const int64_t cnt, ObIntermResult**& results) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < cnt; i++) {
    if (NULL != results[i]) {
      int tmp_ret = mgr.free_result(results[i]);
      if (OB_SUCCESS != tmp_ret) {
        LOG_ERROR("free result failed", K(tmp_ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
      results[i] = NULL;
    }
  }
  return ret;
}

int ObDeterminateTaskTransmit::delete_all_results(
    ObIntermResultManager& mgr, const ObTaskID& task_id, const int64_t cnt) const
{
  int final_ret = OB_SUCCESS;
  int ret = OB_SUCCESS;
  ObSliceID slice_id;
  slice_id.set_ob_task_id(task_id);
  ObIntermResultInfo res_key;
  for (int64_t i = 0; i < cnt; i++) {
    slice_id.set_slice_id(i);
    res_key.init(slice_id);
    if (OB_FAIL(mgr.delete_result(res_key))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("delete result failed", K(ret));
      }
      if (OB_FAIL(ret) && OB_SUCCESS == final_ret) {
        final_ret = ret;
      }
    }
  }
  return final_ret;
}

ERRSIM_POINT_DEF(ERRSIM_DETERMINATE_TRANSMIT_SHUFFLE_ROW);

int ObDeterminateTaskTransmit::shuffle_row(ObExecContext& exec_ctx, ObSqlSchemaGuard& schema_guard,
    ObTableLocation& table_location, Id2IdxMap& partition_id2idx_map, const ObNewRow& row, int64_t& slice_idx) const
{
  int ret = OB_SUCCESS;
  slice_idx = 0;
  int64_t part_idx = 0;
  if (shuffle_by_part_) {
    int64_t part_id = 0;
    if (!table_location.is_inited()) {
      const schema::ObTableSchema* table = NULL;
      if (OB_FAIL(schema_guard.get_table_schema(repartition_table_id_, table))) {
        LOG_WARN("get table schema failed", K(ret));
      } else if (OB_ISNULL(table)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(ret), K(repartition_table_id_));
      } else if (OB_FAIL(partition_id2idx_map.create(
                     hash::cal_next_prime(table->get_all_part_num()), ObModIds::OB_HASH_BUCKET))) {
        LOG_WARN("create map failed", K(ret));
      } else {
        bool check_dropped_schema = false;
        schema::ObTablePartitionKeyIter keys(*table, check_dropped_schema);
        for (int64_t i = 0; OB_SUCC(ret) && i < keys.get_partition_num(); i++) {
          if (OB_FAIL(keys.next_partition_id_v2(part_id))) {
            LOG_WARN("get partition id failed", K(ret));
          } else if (OB_FAIL(partition_id2idx_map.set_refactored(part_id, i))) {
            LOG_WARN("add to map failed", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(table_location.init_table_location_with_rowkey(
                     schema_guard, repartition_table_id_, *exec_ctx.get_my_session()))) {
        LOG_WARN("init table location failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(table_location.calculate_partition_id_by_row(
                   exec_ctx, schema_guard.get_schema_guard(), row, part_id))) {
      LOG_WARN("calculate partition id by row failed", K(ret));
    } else if (OB_FAIL(partition_id2idx_map.get_refactored(part_id, part_idx))) {
      LOG_WARN("get partition index failed", K(part_id));
    }

    if (OB_SUCC(ret)) {
      if (start_slice_ids_.empty()) {
        slice_idx = part_idx;
      } else if (part_idx >= start_slice_ids_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partitions more than start_slice_ids", K(ret));
      } else {
        slice_idx = start_slice_ids_.at(part_idx);
      }
    }
    ret = ERRSIM_DETERMINATE_TRANSMIT_SHUFFLE_ROW ?: ret;
  }

  if (OB_SUCC(ret) && shuffle_by_range_) {
    const auto& ranges = shuffle_ranges_.at(shuffle_ranges_.count() > 1 ? part_idx : 0);
    if (!ranges.empty()) {
      auto begin = &ranges.at(0);
      auto end = begin + ranges.count();
      RangeStartCompare range_row_cmp(ret);
      auto iter = std::lower_bound(begin, end, row, range_row_cmp);
      if (OB_FAIL(ret)) {
        LOG_WARN("compare range and row failed", K(ret));
      } else if (iter == begin) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ranges not start with min value", K(ret), K(row), "range", ranges.at(0));
      } else {
        slice_idx += iter - begin - 1;
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("shuffle row", K(ret), K(slice_idx), K(row));
  }

  return ret;
}

ObLatch ObDeterminateTaskTransmit::task_set_init_lock_;
ObDeterminateTaskTransmit::TaskIDSet ObDeterminateTaskTransmit::executing_task_set_instance_;

ObDeterminateTaskTransmit::TaskIDSet* ObDeterminateTaskTransmit::executing_tasks()
{
  static volatile bool inited = false;
  if (!inited) {
    task_set_init_lock_.wrlock(0);
    int ret = OB_SUCCESS;
    if (!inited) {
      if (OB_FAIL(executing_task_set_instance_.create(4096))) {
        LOG_WARN("create set failed", K(ret));
      } else {
        inited = true;
      }
    }
    task_set_init_lock_.unlock();
  }
  return inited ? &executing_task_set_instance_ : NULL;
}

}  // end namespace sql
}  // end namespace oceanbase
