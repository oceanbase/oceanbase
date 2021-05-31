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

#include "share/ob_cluster_version.h"
#include "observer/ob_server.h"
#include "sql/executor/ob_distributed_transmit.h"
#include "sql/executor/ob_interm_result_manager.h"
#include "sql/executor/ob_task_info.h"
#include "sql/executor/ob_slice_calc.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_range_hash_key_getter.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::observer;
namespace oceanbase {
namespace sql {

int ObDistributedTransmitInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  UNUSED(op);
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("physical plan ctx is NULL", K(ret), K(ctx));
  } else {
    expire_time_ = plan_ctx->get_timeout_timestamp();
    // meta data
    ob_task_id_ = task_info.get_task_location().get_ob_task_id();
    force_save_interm_result_ = task_info.is_force_save_interm_result();
  }
  return ret;
}

OB_SERIALIZE_MEMBER(
    (ObDistributedTransmitInput, ObTransmitInput), expire_time_, ob_task_id_, force_save_interm_result_);

ObDistributedTransmit::ObDistributedTransmit(ObIAllocator& alloc) : ObTransmit(alloc), shuffle_func_(NULL)
{}

ObDistributedTransmit::~ObDistributedTransmit()
{}

int ObDistributedTransmit::get_part_shuffle_key(
    const ObTableSchema* table_schema, int64_t part_idx, ObShuffleKey& part_shuffle_key) const
{
  int ret = OB_SUCCESS;
  if (NULL != table_schema && part_idx >= 0) {
    if (OB_FAIL(part_shuffle_key.set_shuffle_type(*table_schema))) {
      LOG_WARN("fail to set part shuffle type");
    } else if (OB_SUCC(ret) && OB_FAIL(table_schema->get_part_shuffle_key(
                                   part_idx, part_shuffle_key.get_value0(), part_shuffle_key.get_value1()))) {
      LOG_WARN("fail to get part shuffle key", K(ret));
    }
  } else {
    part_shuffle_key.set_shuffle_type(ST_NONE);
  }
  return ret;
}

int ObDistributedTransmit::get_subpart_shuffle_key(
    const ObTableSchema* table_schema, int64_t part_idx, int64_t subpart_idx, ObShuffleKey& subpart_shuffle_key) const
{
  int ret = OB_SUCCESS;
  if (NULL != table_schema && subpart_idx >= 0) {
    if (OB_FAIL(subpart_shuffle_key.set_sub_shuffle_type(*table_schema))) {
      LOG_WARN("fail to sub part set shuffle key");
    } else if (OB_FAIL(table_schema->get_subpart_shuffle_key(
                   part_idx, subpart_idx, subpart_shuffle_key.get_value0(), subpart_shuffle_key.get_value1()))) {
      LOG_WARN("fail to get subpart shuffle key", K(ret));
    }
  } else {
    subpart_shuffle_key.set_shuffle_type(ST_NONE);
  }
  return ret;
}

int ObDistributedTransmit::get_shuffle_part_key(
    const ObTableSchema* table_schema, int64_t part_idx, int64_t subpart_idx, ObPartitionKey& shuffle_part_key) const
{
  int ret = OB_SUCCESS;
  shuffle_part_key.reset();
  if (NULL != table_schema) {
    uint64_t table_id = table_schema->get_table_id();
    int64_t part_id = (subpart_idx < 0) ? part_idx : generate_phy_part_id(part_idx, subpart_idx);
    int64_t part_num = table_schema->get_partition_cnt();
    if (OB_FAIL(shuffle_part_key.init(table_id, part_id, part_num))) {
      LOG_WARN("fail to init shuffle part key", K(ret));
    }
  }
  return ret;
}

int ObDistributedTransmit::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_UNLIKELY(calc_exprs_.get_size() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("calc exprs should be empty", K(ret), K(calc_exprs_.get_size()));
  } else if (OB_UNLIKELY(filter_exprs_.get_size() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter exprs should be empty", K(ret), K(filter_exprs_.get_size()));
  } else if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObDistributedTransmitCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("fail to create phy op ctx", K(ret), K(get_id()), K(get_type()));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op ctx is NULL", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, false))) {
    LOG_WARN("fail to int cur row", K(ret));
  }
  return ret;
}

bool ObDistributedTransmit::skip_empty_slice() const
{
  return GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_1432;
}

int ObDistributedTransmit::prepare_interm_result(
    ObIntermResultManager& interm_result_mgr, ObIntermResult*& interm_result) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(interm_result)) {
    if (OB_FAIL(interm_result_mgr.alloc_result(interm_result))) {
      LOG_WARN("fail alloc result", K(ret));
    }
  }
  return ret;
}

int ObDistributedTransmit::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* row = NULL;
  // int64_t last_fail_res_idx = 0;
  ObSEArray<ObIntermResultInfo, 1> added_ir_info_list;
  ObSEArray<ObSliceInfo, 8> slice_infos;

  ObSQLSessionInfo* session = NULL;
  ObDistributedTransmitInput* trans_input = NULL;
  ObIntermResultManager* interm_result_mgr = ObIntermResultManager::get_instance();
  ObIntermResult** interm_result = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObDistributedTransmitCtx* transimt_ctx = NULL;
  ObIArray<ObSliceEvent>* slice_events = NULL;

  uint64_t slice_table_id = repartition_table_id_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* table_schema = NULL;

  int64_t interm_result_buf_len = get_split_task_count() * sizeof(ObIntermResult*);
  if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is NULL", K(ret));
  } else if (OB_UNLIKELY(get_split_task_count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("split task count must > 0", K(ret), K(get_split_task_count()));
  } else if (OB_FAIL(ObTransmit::inner_open(exec_ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_ISNULL(session = GET_MY_SESSION(exec_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session is NULL", K(ret));
  } else if (OB_ISNULL(trans_input = GET_PHY_OP_INPUT(ObDistributedTransmitInput, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get op ctx", K(ret), "op_id", get_id(), "op_type", get_type());
  } else if (OB_ISNULL(transimt_ctx = GET_PHY_OPERATOR_CTX(ObDistributedTransmitCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get physical operator context failed", K(ret), K_(id));
  } else if (OB_ISNULL(slice_events = trans_input->get_slice_events_for_update())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("slice events is NULL", K(ret), K_(id));
  } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(exec_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get plan ctx", K(ret));
  } else if (OB_ISNULL(interm_result_mgr) || OB_UNLIKELY(interm_result_buf_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("mgr is NULL or nbytes <= 0", K(ret), K(interm_result_mgr), K(interm_result_buf_len));
  } else if (OB_ISNULL(interm_result =
                           static_cast<ObIntermResult**>(exec_ctx.get_allocator().alloc(interm_result_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc intermediate result buffer", K(ret), K(interm_result_buf_len));
  } else {
    memset(static_cast<void*>(interm_result), 0, interm_result_buf_len);
    // some meta info will be saved in interm_result[0],
    // we create it no matter whether empty.
    if (skip_empty_slice()) {
      if (OB_FAIL(interm_result_mgr->alloc_result(interm_result[0]))) {
        LOG_WARN("fail alloc result 0", K(ret));
      }
    } else {
      for (int idx = 0; OB_SUCC(ret) && idx < get_split_task_count(); ++idx) {
        if (OB_FAIL(interm_result_mgr->alloc_result(interm_result[idx]))) {
          LOG_WARN("fail alloc result", K(ret), K(idx));
        }
      }
    }
  }

  if (OB_SUCC(ret) && OB_INVALID_ID != slice_table_id) {
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(session->get_effective_tenant_id(), schema_guard))) {
      LOG_WARN("faile to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(slice_table_id, table_schema))) {
      LOG_WARN("faile to get table schema", K(ret), K(slice_table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret), K(slice_table_id));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t round_robin_idx = 0;
    ObRangeHashKeyGetter range_hash_key_getter(repartition_table_id_, repart_columns_, repart_sub_columns_);
    while (OB_SUCC(ret)) {
      int64_t slice_idx = -1;
      int64_t part_idx = -1;
      int64_t subpart_idx = -1;
      bool skip_row = false;
      bool skip_get_partition_ids = false;
      if (OB_ISNULL(table_schema)) {
        skip_get_partition_ids = true;
        slice_idx = 0;
      }
      if (OB_FAIL(get_next_row(exec_ctx, row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row from child op", K(ret), K(child_op_->get_type()));
        } else {
          // iter end
          // set found rows
          interm_result[0]->set_found_rows(plan_ctx->get_found_rows());
          interm_result[0]->set_affected_rows(plan_ctx->get_affected_rows());
          interm_result[0]->set_matched_rows(plan_ctx->get_row_matched_count());
          interm_result[0]->set_duplicated_rows(plan_ctx->get_row_duplicated_count());
          interm_result[0]->set_last_insert_id_session(plan_ctx->calc_last_insert_id_session());
          if (!plan_ctx->is_result_accurate()) {
            interm_result[0]->set_is_result_accurate(plan_ctx->is_result_accurate());
          }
          NG_TRACE_EXT(transmit,
              OB_ID(found_rows),
              plan_ctx->get_found_rows(),
              OB_ID(last_insert_id),
              plan_ctx->calc_last_insert_id_session());
        }
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row is NULL", K(ret));
      } else if (OB_FAIL(copy_cur_row(*transimt_ctx, row))) {
        LOG_WARN("copy current row failed", K(ret));
      } else if (!skip_get_partition_ids && OB_FAIL(get_slice_idx(exec_ctx,
                                                table_schema,
                                                row,
                                                repart_func_,
                                                repart_sub_func_,
                                                repart_columns_,
                                                repart_sub_columns_,
                                                get_split_task_count(),
                                                slice_idx,
                                                skip_row))) {
        LOG_WARN("fail get slice idx", K(ret), K(part_idx), K(subpart_idx));
      } else if (skip_row && ObPQDistributeMethod::DROP == unmatch_row_dist_method_) {
        // do nothing
      } else {
        if (OB_UNLIKELY(0 >= get_split_task_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected task count", K(ret));
        } else if (skip_row && ObPQDistributeMethod::RANDOM == unmatch_row_dist_method_) {
          round_robin_idx++;
          slice_idx = round_robin_idx % get_split_task_count();
        }
        if (OB_FAIL(ret)) {
          /*do nothing*/
        } else if (OB_UNLIKELY(slice_idx < 0 || slice_idx >= get_split_task_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid slice idx", K(ret), K(slice_idx), K(split_task_count_));
        } else if (OB_FAIL(prepare_interm_result(*interm_result_mgr, interm_result[slice_idx]))) {
          LOG_WARN("fail prepare interm_result[slice_idx]", K(ret), K(slice_idx));
        } else if (OB_ISNULL(interm_result[slice_idx])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("interm_result[slice_idx] is NULL", K(ret), K(slice_idx));
        } else if (OB_FAIL(interm_result[slice_idx]->add_row(session->get_effective_tenant_id(), *row))) {
          if (OB_UNLIKELY(OB_ITER_END == ret)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("fail emit row to interm result, but ret is OB_ITER_END", K(ret));
          } else {
            LOG_WARN("fail emit row to interm result", K(ret));
          }
        } else {
          // empty
        }
      }
    }
    if (OB_ITER_END == ret) {
      LOG_DEBUG("all rows are fetched");
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)) {
      for (int64_t slice_idx = 0; OB_SUCC(ret) && slice_idx < get_split_task_count(); ++slice_idx) {
        if (NULL == interm_result[slice_idx]) {
          continue;
        }
        if (OB_FAIL(interm_result[slice_idx]->complete_add_rows(session->get_effective_tenant_id()))) {
          LOG_WARN("fail to complete add rows", K(ret), K(slice_idx), K(session->get_effective_tenant_id()));
        }
      }
      const static int64_t TOTAL_SMALL_RESULT_MEM_LIMIT = 8 * 1024;  // 8k
      int64_t total_interm_result_size = 0;
      int64_t slice_event_count = 0;
      for (int64_t slice_idx = 0; OB_SUCC(ret) && slice_idx < get_split_task_count(); ++slice_idx) {
        int64_t all_data_size = 0;
        if (NULL == interm_result[slice_idx]) {
          continue;
        }
        if (OB_FAIL(interm_result[slice_idx]->get_all_data_size(all_data_size))) {
          LOG_WARN("fail to get all used mem size of irm", K(ret), K(slice_idx));
        } else {
          total_interm_result_size += all_data_size;
          slice_event_count++;
        }
      }
      if (OB_SUCC(ret)) {
        slice_events->reset();
        if (OB_FAIL(slice_events->prepare_allocate(slice_event_count))) {
          LOG_WARN("fail to prepare allocate small result list", K(ret), K(get_split_task_count()));
        }
      }
      int64_t slice_event_idx = 0;
      ObSliceID ob_slice_id;
      ob_slice_id.set_ob_task_id(trans_input->get_ob_task_id());
      int64_t part_idx = -1;
      int64_t subpart_idx = -1;

      if (OB_SUCC(ret) && nullptr != table_schema) {
        if (OB_FAIL(slice_infos.prepare_allocate(get_split_task_count()))) {
          LOG_WARN("Prepare allocate failed", K(ret));
        } else if (OB_FAIL(init_slice_infos(*table_schema, slice_infos))) {
          LOG_WARN("fail init slice info", K(ret));
        }
      }

      for (int64_t slice_idx = 0; OB_SUCC(ret) && slice_idx < get_split_task_count(); ++slice_idx) {
        if (NULL == interm_result[slice_idx]) {
          continue;
        }
        bool need_save_interm_result = true;
        ObSliceEvent& slice_event = slice_events->at(slice_event_idx++);
        int64_t data_size = 0;
        ob_slice_id.set_slice_id(slice_idx);
        slice_event.set_ob_slice_id(ob_slice_id);
        if (nullptr != table_schema) {
          part_idx = slice_infos[slice_idx].part_idx_;
          subpart_idx = slice_infos[slice_idx].subpart_idx_;
        } else {
          part_idx = 0;
          subpart_idx = 0;
        }
        if (OB_FAIL(get_part_shuffle_key(table_schema, part_idx, slice_event.get_part_shuffle_key()))) {
          LOG_WARN("fail to get part shuffle key", K(ret), K(slice_idx), K(part_idx), K(subpart_idx));
        } else if (OB_FAIL(get_subpart_shuffle_key(
                       table_schema, part_idx, subpart_idx, slice_event.get_subpart_shuffle_key()))) {
          LOG_WARN("fail to get subpart shuffle key", K(ret), K(slice_idx), K(part_idx), K(subpart_idx));
        } else if (OB_FAIL(
                       get_shuffle_part_key(table_schema, part_idx, subpart_idx, slice_event.get_shuffle_part_key()))) {
          LOG_WARN("fail to get shuffle partition key", K(ret), K(slice_idx), K(part_idx), K(subpart_idx));
        } else if (OB_FAIL(interm_result[slice_idx]->get_all_data_size(data_size))) {
          LOG_WARN("fail to get data size of irm", K(ret), K(slice_idx));
        }

        if (OB_SUCC(ret) && (total_interm_result_size < TOTAL_SMALL_RESULT_MEM_LIMIT || 0 == data_size)) {
          if (interm_result[slice_idx]->get_scanner_count() <= 1) {
            if (!trans_input->is_force_save_interm_result()) {
              ObTaskSmallResult& small_result = slice_event.get_small_result_for_update();
              if (OB_FAIL(interm_result[slice_idx]->try_fetch_single_scanner(small_result))) {
                LOG_WARN("fail copy small result scanner", K(ret), K(slice_idx));
              } else {
                if (small_result.has_data()) {
                  need_save_interm_result = false;
                }
              }
            }
          }
        }

        need_save_interm_result = true;
        if (OB_SUCC(ret)) {
          if (need_save_interm_result) {
            ObIntermResultInfo res_info;
            res_info.init(ob_slice_id);
            if (OB_FAIL(interm_result_mgr->add_result(
                    res_info, interm_result[slice_idx], trans_input->get_expire_time()))) {
              LOG_WARN("fail add one result. free all result", K(slice_idx), "total", get_split_task_count(), K(ret));
              // last_fail_res_idx = slice_idx;
            } else if (OB_FAIL(added_ir_info_list.push_back(res_info))) {
              int free_ret = OB_SUCCESS;
              if (OB_SUCCESS != (free_ret = interm_result_mgr->delete_result(res_info))) {
                LOG_ERROR("fail free interm result, possible memory leak!", K(free_ret), K(slice_idx));
              }
              interm_result[slice_idx] = NULL;
            }
          } else {
            if (OB_FAIL(interm_result_mgr->free_result(interm_result[slice_idx]))) {
              LOG_ERROR("fail free interm result, possible memory leak!", K(ret), K(slice_idx));
            }
            interm_result[slice_idx] = NULL;
          }
        }
      }
    }
  }

  // free all result
  if (OB_FAIL(ret)) {
    if (OB_ISNULL(trans_input) || OB_ISNULL(interm_result_mgr)) {
      LOG_ERROR("trans input or mgr is NULL", K(trans_input), K(interm_result_mgr));
    } else {
      int64_t idx = 0;
      int free_ret = OB_SUCCESS;
      // (1) delete those already added result
      for (idx = 0; idx < added_ir_info_list.count(); ++idx) {
        ObIntermResultIterator iter;
        const ObIntermResultInfo& res_info = added_ir_info_list.at(idx);
        if (OB_SUCCESS != (free_ret = interm_result_mgr->get_result(res_info, iter))) {
          LOG_ERROR("fail get result. possible memory leak. will try recycle later", K(idx), K(free_ret));
        } else if (OB_SUCCESS != (free_ret = interm_result_mgr->delete_result(iter))) {
          LOG_ERROR("fail free interm result, possible memory leak!", K(free_ret));
        }
      }
      // (2) delete those not yet added result
      if (NULL != interm_result) {
        for (/*cont.*/; idx < get_split_task_count(); ++idx) {
          if (NULL != interm_result[idx]) {  // this cond deal with alloc_result() fail case
            if (OB_SUCCESS != (free_ret = interm_result_mgr->free_result(interm_result[idx]))) {
              LOG_ERROR("fail free interm result, possible memory leak!", K(idx), K(free_ret));
            }
          }
        }
      } else {
      }
    }
  }
  return ret;
}

int ObDistributedTransmit::get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ret = ObPhyOperator::get_next_row(ctx, row);
  return ret;
}

int ObDistributedTransmit::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null");
  } else if (child_op_->is_dml_without_output()) {
    ret = OB_ITER_END;
  } else {
    ret = child_op_->get_next_row(ctx, row);
  }
  return ret;
}

int ObDistributedTransmit::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObIPhyOperatorInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObDistributedTransmitInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create phy op input", K(ret), K(get_id()), K(get_type()));
  } else {
  }
  UNUSED(input);
  return ret;
}

int ObDistributedTransmit::init_slice_infos(
    const share::schema::ObTableSchema& table_schema, ObIArray<ObSliceInfo>& slice_infos) const
{
  int ret = OB_SUCCESS;
  ObSliceInfo slice_info;
  common::ObPartitionKey pkey;
  ObPartitionKeyIter iter(table_schema.get_table_id(), table_schema, false);
  while (OB_SUCC(ret) && OB_SUCC(iter.next_partition_key_v2(pkey))) {
    slice_info.part_idx_ = pkey.get_part_idx();
    slice_info.subpart_idx_ = pkey.get_subpart_idx();
    if (OB_FAIL(get_slice_idx_by_partition_ids(
            slice_info.part_idx_, slice_info.subpart_idx_, table_schema, slice_info.slice_idx_))) {
      LOG_WARN("Failed to get slice idx", K(ret), K(slice_info));
    } else if (slice_info.slice_idx_ < 0 || slice_info.slice_idx_ >= slice_infos.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid slice idx", K(ret), K(slice_info.slice_idx_), K(slice_infos.count()));
    } else {
      slice_infos.at(slice_info.slice_idx_) = slice_info;
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDistributedTransmit::get_slice_idx(ObExecContext& exec_ctx, const share::schema::ObTableSchema* table_schema,
    const common::ObNewRow* row, const ObSqlExpression& part_func, const ObSqlExpression& subpart_func,
    const ObIArray<ObTransmitRepartColumn>& repart_columns, const ObIArray<ObTransmitRepartColumn>& repart_sub_columns,
    int64_t slices_count, int64_t& slice_idx, bool& no_match_partiton) const
{
  int ret = OB_SUCCESS;
  ObSliceInfo slice_info;
  ObShuffleService shuffle_service(exec_ctx.get_allocator());
  if (OB_FAIL(shuffle_service.get_partition_ids(exec_ctx,
          *table_schema,
          *row,
          part_func,
          subpart_func,
          repart_columns,
          repart_sub_columns,
          slice_info.part_idx_,
          slice_info.subpart_idx_,
          no_match_partiton))) {
    LOG_WARN("Failed to get part/subpart idx", K(ret), K(slice_info));
  } else if (no_match_partiton) {
    // do nothing
  } else if (OB_FAIL(get_slice_idx_by_partition_ids(
                 slice_info.part_idx_, slice_info.subpart_idx_, *table_schema, slice_info.slice_idx_))) {
    LOG_WARN("Failed to get slice idx", K(ret), K(slice_info));
  } else if (slice_info.slice_idx_ < 0 || slice_info.slice_idx_ >= slices_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid slice idx", K(ret), K(slice_info.slice_idx_), K(slices_count));
  } else {
    slice_idx = slice_info.slice_idx_;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObDistributedTransmit)
{
  int ret = OK_;
  UNF_UNUSED_SER;
  BASE_SER((ObDistributedTransmit, ObTransmit));
#if 0
  OB_ASSERT(shuffle_func_);
  LST_DO_CODE(OB_UNIS_ENCODE, *shuffle_func_);
#endif
  return ret;
}

OB_DEF_DESERIALIZE(ObDistributedTransmit)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER((ObDistributedTransmit, ObTransmit));
  return ret;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, shuffle_func_))) {
      LOG_WARN("make sql expression failed", K(ret));
    } else {
#if 0
      LST_DO_CODE(OB_UNIS_DECODE, *shuffle_func_);
#endif
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDistributedTransmit)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObDistributedTransmit, ObTransmit));
  return len;
#if 0
  OB_ASSERT(shuffle_func_);
  LST_DO_CODE(OB_UNIS_ADD_LEN, *shuffle_func_);
  return len;
#endif
}

}  // namespace sql
}  // namespace oceanbase
