/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_ls_execute_processor.h"
#include "ob_table_rpc_processor_util.h"
#include "share/table/ob_table.h"
#include "ob_table_query_and_mutate_helper.h"
#include "ob_table_end_trans_cb.h"

using namespace oceanbase::observer;
using namespace oceanbase::table;
using namespace oceanbase::common;

ObTableLSExecuteP::ObTableLSExecuteP(const ObGlobalContext &gctx)
  : ObTableRpcProcessor(gctx),
    allocator_("TableLSExecuteP", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    default_entity_factory_("TableLSExeEntFac", MTL_ID())
{
}

int ObTableLSExecuteP::deserialize()
{
  arg_.ls_op_.set_deserialize_allocator(&allocator_);
  return ParentType::deserialize();
}

int ObTableLSExecuteP::before_process()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObString>& all_rowkey_names = arg_.ls_op_.get_all_rowkey_names();
  const ObIArray<ObString>& all_properties_names = arg_.ls_op_.get_all_properties_names();
  bool need_all_prop = arg_.ls_op_.need_all_prop_bitmap();
  if (OB_FAIL(result_.assign_rowkey_names(all_rowkey_names))) {
    LOG_WARN("fail to assign rowkey names", K(ret), K(all_rowkey_names));
  } else if (!need_all_prop && OB_FAIL(result_.assign_properties_names(all_properties_names))) {
    LOG_WARN("fail to assign properties names", K(ret), K(all_properties_names));
  } else {
    ret = ParentType::before_process();
  }

  return ret;
}

int ObTableLSExecuteP::check_arg()
{
  int ret = OB_SUCCESS;
  if (!(arg_.consistency_level_ == ObTableConsistencyLevel::STRONG ||
      arg_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "consistency level");
    LOG_WARN("some options not supported yet", K(ret), "consistency_level", arg_.consistency_level_);
  }
  return ret;
}

int ObTableLSExecuteP::check_arg_for_query_and_mutate(const ObTableSingleOp &single_op)
{
  int ret = OB_SUCCESS;
  const ObTableQuery *query = nullptr;
  const ObHTableFilter *hfilter = nullptr;
  const ObIArray<ObTableSingleOpEntity> &entities = single_op.get_entities();
  bool is_hkv = ObTableEntityType::ET_HKV == arg_.entity_type_;
  if (single_op.get_op_type() != ObTableOperationType::CHECK_AND_INSERT_UP) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "single op type is not check and insert up");
    LOG_WARN("invalid single op type", KR(ret), "single op type", single_op.get_op_type());
  } else if (OB_ISNULL(query = single_op.get_query()) || OB_ISNULL(hfilter = &(query->htable_filter()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query or htable filter is NULL", K(ret), KP(query));
  } else if (!query->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query is invalid");
    LOG_WARN("invalid table query request", K(ret), K(query));
  } else if (is_hkv && !hfilter->is_valid()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ob-hbase model but not set hfilter");
    LOG_WARN("QueryAndMutate hbase model should set hfilter", K(ret));
  } else if (!is_hkv && (1 != entities.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "the count of entities must be 1 for non-hbase's single operation");
    LOG_WARN("table api single operation has unexpected entities count, expect 1", K(ret), K(entities.count()));
  } else if (entities.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "the count of entities must greater than 0");
    LOG_WARN("should have at least one entities for single operation", K(ret), K(entities));
  } else {
    // these options are meaningless for QueryAndMutate users but we should control them internally
    const_cast<ObTableQuery *>(query)->set_batch(1);  // mutate for each row
    const_cast<ObTableQuery *>(query)->set_max_result_size(-1);
    const_cast<ObHTableFilter *>(hfilter)->set_max_versions(1);
    const_cast<ObHTableFilter *>(hfilter)->set_row_offset_per_column_family(0);
    const_cast<ObHTableFilter *>(hfilter)->set_max_results_per_column_family(-1);
  }
  return ret;
}

uint64_t ObTableLSExecuteP::get_request_checksum()
{
  uint64_t checksum = arg_.ls_op_.get_ls_id().id();
  int64_t table_checksum = arg_.ls_op_.get_table_id();
  checksum = ob_crc64(checksum, &table_checksum, sizeof(table_checksum));
  int64_t tablet_checksum = 0;
  uint64_t single_op_checksum = 0;
  for (int64_t i = 0; i < arg_.ls_op_.count(); i++) {
    tablet_checksum = arg_.ls_op_.at(i).get_tablet_id().id();
    checksum = ob_crc64(checksum, &tablet_checksum, sizeof(tablet_checksum));
    for (int64_t j = 0; j < arg_.ls_op_.at(i).count(); j++) {
      single_op_checksum = arg_.ls_op_.at(i).at(j).get_checksum();
      checksum = ob_crc64(checksum, &single_op_checksum, sizeof(single_op_checksum));
    }
  }
  return checksum;
}

void ObTableLSExecuteP::reset_ctx()
{
  need_retry_in_queue_ = false;
  ObTableApiProcessorBase::reset_ctx();
  result_.reset();
}

int ObTableLSExecuteP::get_ls_id(ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  const ObTableLSOp &ls_op = arg_.ls_op_;
  const ObLSID &client_ls_id = arg_.ls_op_.get_ls_id();
  if (client_ls_id.is_valid()) {
    ls_id = client_ls_id;
  } else if (ls_op.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ls op count", K(ret));
  } else {
    const ObTabletID &first_tablet_id = ls_op.at(0).get_tablet_id();
    const uint64_t &first_table_id = ls_op.get_table_id();
    ObTabletID real_tablet_id;
    if (OB_FAIL(get_tablet_id(first_tablet_id, first_table_id, real_tablet_id))) {
      LOG_WARN("fail to get tablet id", K(ret), K(first_table_id), K(first_table_id));
    } else if (OB_FAIL(ParentType::get_ls_id(real_tablet_id, ls_id))) {
      LOG_WARN("fail to get ls id", K(ret), K(real_tablet_id));
    }
  }
  return ret;
}

int ObTableLSExecuteP::try_process()
{
  int ret = OB_SUCCESS;
  ObTableLSOp &ls_op = arg_.ls_op_;
  ObLSID ls_id = ls_op.get_ls_id();
  uint64_t table_id = ls_op.get_table_id();
  bool exist_global_index = false;
  bool need_all_prop = arg_.ls_op_.need_all_prop_bitmap();
  table_id_ = table_id;  // init move response need
  if (OB_FAIL(init_schema_info(table_id))) {
    if (ret == OB_TABLE_NOT_EXIST) {
      ObString db("");
      const ObString &table_name = ls_op.get_table_name();
      LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(db), to_cstring(table_name));
    }
    LOG_WARN("fail to init schema info", K(ret), K(table_id));
  } else if (OB_FAIL(get_ls_id(ls_id))) {
    LOG_WARN("fail to get ls id", K(ret));
  } else if (OB_FAIL(check_table_has_global_index(exist_global_index))) {
    LOG_WARN("fail to check global index", K(ret), K(table_id));
  } else if (need_all_prop) {
    ObSEArray<ObString, 8> all_prop_name;
    const ObIArray<ObTableColumnInfo *>&column_info_array = schema_cache_guard_.get_column_info_array();
    if (OB_FAIL(ObTableApiUtil::expand_all_columns(column_info_array, all_prop_name))) {
      LOG_WARN("fail to expand all columns", K(ret));
    } else if (OB_FAIL(result_.assign_properties_names(all_prop_name))) {
      LOG_WARN("fail to assign property names to result", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(start_trans(false, /* is_readonly */
                                 arg_.consistency_level_,
                                 ls_id,
                                 get_timeout_ts(),
                                 exist_global_index))) {
    LOG_WARN("fail to start transaction", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < ls_op.count(); i++) {
    ObTableTabletOp &tablet_op = ls_op.at(i);
    ObTableTabletOpResult tablet_result;
    if (OB_FAIL(execute_tablet_op(tablet_op, tablet_result))) {
      LOG_WARN("fail to execute tablet op", KR(ret), K(tablet_op));
    } else if (OB_FAIL(add_tablet_result(tablet_result))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }

  ObTableLSExecuteCreateCbFunctor functor;
  if (OB_SUCC(ret) && OB_FAIL(functor.init(req_, &result_))) {
    LOG_WARN("fail to init create ls callback functor", K(ret));
  }

  int tmp_ret = ret;
  const bool use_sync = false;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, &functor, use_sync))) {
    LOG_WARN("failed to end trans", K(ret));
  }

  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;

#ifndef NDEBUG
  // debug mode
  LOG_INFO("[TABLE] execute ls batch operation", K(ret), K_(result), K_(retry_count));
#else
  // release mode
  LOG_TRACE("[TABLE] execute ls batch operation", K(ret), K_(result), K_(retry_count), "receive_ts", get_receive_timestamp());
#endif
  return ret;
}

int ObTableLSExecuteP::add_tablet_result(const ObTableTabletOpResult &tablet_result)
{
  int ret = OB_SUCCESS;
  bool return_one_res = arg_.ls_op_.return_one_result();
  if (return_one_res && result_.count() != 0) {
    if (result_.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ls result count", K(ret), K(result_.count()));
    } else {
      ObTableTabletOpResult &ls_tablet_res = result_.at(0);
      if (ls_tablet_res.count() != 1 || tablet_result.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tablet result count", K(ret), K(ls_tablet_res.count()), K(tablet_result.count()));
      } else {
        ObTableSingleOpResult &ls_single_op_res = ls_tablet_res.at(0);
        ls_single_op_res.set_affected_rows(ls_single_op_res.get_affected_rows() + tablet_result.at(0).get_affected_rows());
      }
    }
  } else {
    result_.push_back(tablet_result);
  }
  return ret;
}

int ObTableLSExecuteP::execute_tablet_op(const ObTableTabletOp &tablet_op, ObTableTabletOpResult&tablet_result)
{
  int ret = OB_SUCCESS;
  if (tablet_op.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet operations count is less than 1", K(ret));
  } else {
    ObTableOperationType::Type op_type = tablet_op.at(0).get_op_type();
    if (op_type == ObTableOperationType::CHECK_AND_INSERT_UP) {
      if (OB_FAIL(execute_tablet_query_and_mutate(tablet_op, tablet_result))) {
        LOG_WARN("fail to execute tablet query and mutate", K(ret));
      }
    } else {
      // other op type will check its validity in its inner logic
      if (OB_FAIL(execute_tablet_batch_ops(tablet_op, tablet_result))) {
        LOG_WARN("fail to execute tablet batch operations", K(ret));
      }
    }
  }

#ifndef NDEBUG
  // debug mode
  LOG_INFO("[TABLE] execute ls batch tablet operation", K(ret), K(tablet_op), K(tablet_result), K_(retry_count));
#else
  // release mode
  LOG_TRACE("[TABLE] execute ls batch tablet operation", K(ret), K(tablet_op), K(tablet_result), K_(retry_count), "receive_ts", get_receive_timestamp());
#endif

  return ret;
}

int ObTableLSExecuteP::execute_tablet_query_and_mutate(const ObTableTabletOp &tablet_op, ObTableTabletOpResult &tablet_result)
{
  int ret = OB_SUCCESS;
  if (tablet_op.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operation count", KR(ret), K(tablet_op.count()));
  } else {
    uint64_t table_id = arg_.ls_op_.get_table_id();
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_op.count(); i++) {
      const ObTableSingleOp &single_op = tablet_op.at(i);
      const ObTableSingleOpEntity& req_entity= single_op.get_entities().at(0);
      const common::ObTabletID tablet_id = tablet_op.get_tablet_id();
      ObTableSingleOpResult single_op_result;
      single_op_result.set_errno(OB_SUCCESS);
      ObITableEntity *result_entity = default_entity_factory_.alloc();

      if (OB_ISNULL(result_entity)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memroy for result_entity", K(ret));
      } else if (FALSE_IT(result_entity->set_dictionary(&arg_.ls_op_.get_all_rowkey_names(),
                                                       &arg_.ls_op_.get_all_properties_names()))) {
      } else if (FALSE_IT(single_op_result.set_entity(*result_entity))) {
      } else if (OB_FAIL(execute_single_query_and_mutate(table_id, tablet_id, single_op, single_op_result))) {
        LOG_WARN("fail to execute tablet op", KR(ret), K(tablet_op));
      } else if (OB_FAIL(result_entity->construct_names_bitmap(req_entity))) {
        LOG_WARN("fail to construct_names_bitmap", KR(ret), KPC(result_entity));
      } else if (OB_FAIL(tablet_result.push_back(single_op_result))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLSExecuteP::execute_tablet_batch_ops(const ObTableTabletOp &tablet_op, ObTableTabletOpResult &tablet_result)
{
  int ret = OB_SUCCESS;
  if (tablet_op.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operation count", KR(ret), K(tablet_op.count()));
  } else {
    ObSEArray<ObTableOperation, 16> table_operations;
    SMART_VAR(ObTableBatchCtx, batch_ctx, allocator_, audit_ctx_) {
      if (OB_FAIL(init_batch_ctx(batch_ctx, tablet_op, table_operations, tablet_result))) {
        LOG_WARN("fail to init batch ctx", K(ret));
      } else if (OB_FAIL(ObTableBatchService::execute(batch_ctx))) {
        LOG_WARN("fail to execute batch operation", K(ret));
      } else if (OB_FAIL(add_dict_and_bm_to_result_entity(tablet_op, tablet_result))) {
        LOG_WARN("fail to add dictionary and bitmap", K(ret));
      }
    }
    // record events
    stat_row_count_ += tablet_op.count();
  }
  return ret;
}

int ObTableLSExecuteP::init_batch_ctx(table::ObTableBatchCtx &batch_ctx,
                                      const table::ObTableTabletOp &tablet_op,
                                      ObIArray<table::ObTableOperation> &table_operations,
                                      table::ObTableTabletOpResult &tablet_result)
{
  int ret = OB_SUCCESS;
  ObTableLSOp &ls_op = arg_.ls_op_;
  // 1. 构造batch_service需要的入参
  batch_ctx.stat_event_type_ = &stat_event_type_;
  batch_ctx.trans_param_ = &trans_param_;
  batch_ctx.entity_type_ = arg_.entity_type_;
  batch_ctx.consistency_level_ = arg_.consistency_level_;
  batch_ctx.credential_ = &credential_;
  batch_ctx.table_id_ = simple_table_schema_->get_table_id();
  batch_ctx.tablet_id_ = tablet_op.get_tablet_id();
  batch_ctx.is_atomic_ = true; /* batch atomic always true*/
  batch_ctx.is_readonly_ = tablet_op.is_readonly();
  batch_ctx.is_same_type_ = tablet_op.is_same_type();
  batch_ctx.is_same_properties_names_ = tablet_op.is_same_properties_names();
  batch_ctx.use_put_ = tablet_op.is_use_put();
  batch_ctx.returning_affected_entity_ = tablet_op.is_returning_affected_entity();
  batch_ctx.returning_rowkey_ = tablet_op.is_returning_rowkey();
  batch_ctx.entity_factory_ = &default_entity_factory_;
  batch_ctx.return_one_result_ = ls_op.return_one_result();
  // construct batch operation
  batch_ctx.ops_ = &table_operations;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_op.count(); i++) {
    const ObTableSingleOp &single_op = tablet_op.at(i);
    if (single_op.get_op_type() == ObTableOperationType::CHECK_AND_INSERT_UP) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "check_and_insertup in batch");
      LOG_WARN("invalid single op type", KR(ret), "single op type", single_op.get_op_type());
    } else {
      ObTableOperation table_op;
      table_op.set_entity(single_op.get_entities().at(0));
      table_op.set_type(single_op.get_op_type());
      if (OB_FAIL(table_operations.push_back(table_op))) {
        LOG_WARN("fail to push table operation", K(ret));
      }
    }
  }
  // construct batch operation result
  if (OB_SUCC(ret)) {
    batch_ctx.results_ = &tablet_result;
    batch_ctx.result_entity_ = default_entity_factory_.alloc(); // only use in hbase mutation
    if (OB_ISNULL(batch_ctx.result_entity_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memroy for result_entity", K(ret));
    } else if (OB_FAIL(init_tb_ctx(batch_ctx.tb_ctx_, tablet_op, table_operations.at(0)))) { // init tb_ctx
      LOG_WARN("fail to init table context", K(ret));
    }
  }

  return ret;
}

int ObTableLSExecuteP::init_tb_ctx(table::ObTableCtx &tb_ctx,
                                   const table::ObTableTabletOp &tablet_op,
                                   const table::ObTableOperation &table_operation)
{
  int ret = OB_SUCCESS;
  tb_ctx.set_entity(&table_operation.entity());
  tb_ctx.set_entity_type(arg_.entity_type_);
  tb_ctx.set_operation_type(table_operation.type());
  tb_ctx.set_schema_cache_guard(&schema_cache_guard_);
  tb_ctx.set_schema_guard(&schema_guard_);
  tb_ctx.set_simple_table_schema(simple_table_schema_);
  tb_ctx.set_sess_guard(&sess_guard_);
  if (tb_ctx.is_init()) {
    LOG_INFO("tb ctx has been inited", K(tb_ctx));
  } else if (OB_FAIL(tb_ctx.init_common(credential_, tablet_op.get_tablet_id(), get_timeout_ts()))) {
    LOG_WARN("fail to init table ctx common part", K(ret), K(arg_.ls_op_.get_table_id()));
  } else {
    ObTableOperationType::Type op_type = table_operation.type();
    switch (op_type) {
      case ObTableOperationType::GET: {
        if (OB_FAIL(tb_ctx.init_get())) {
          LOG_WARN("fail to init get ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::PUT: {
        if (OB_FAIL(tb_ctx.init_put())) {
          LOG_WARN("fail to init put ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INSERT: {
        if (OB_FAIL(tb_ctx.init_insert())) {
          LOG_WARN("fail to init insert ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::DEL: {
        if (OB_FAIL(tb_ctx.init_delete())) {
          LOG_WARN("fail to init delete ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::UPDATE: {
        if (OB_FAIL(tb_ctx.init_update())) {
          LOG_WARN("fail to init update ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INSERT_OR_UPDATE: {
        if (OB_FAIL(tb_ctx.init_insert_up(tablet_op.is_use_put()))) {
          LOG_WARN("fail to init insert up ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::REPLACE: {
        if (OB_FAIL(tb_ctx.init_replace())) {
          LOG_WARN("fail to init replace ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::APPEND: {
        if (OB_FAIL(tb_ctx.init_append(tablet_op.is_returning_affected_entity(),
                                       tablet_op.is_returning_rowkey()))) {
          LOG_WARN("fail to init append ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INCREMENT: {
        if (OB_FAIL(tb_ctx.init_increment(tablet_op.is_returning_affected_entity(),
                                          tablet_op.is_returning_rowkey()))) {
          LOG_WARN("fail to init increment ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected operation type", "type", op_type);
        break;
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(tb_ctx.init_exec_ctx())) {
    LOG_WARN("fail to init exec ctx", K(ret), K(tb_ctx));
  } else if (OB_FAIL(tb_ctx.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret));
  }

  return ret;
}

int ObTableLSExecuteP::add_dict_and_bm_to_result_entity(const table::ObTableTabletOp &tablet_op,
                                                        table::ObTableTabletOpResult &tablet_result)
{
  int ret = OB_SUCCESS;
  ObTableLSOp &ls_op = arg_.ls_op_;
  bool is_hkv = ObTableEntityType::ET_HKV == arg_.entity_type_;
  if (!is_hkv && !ls_op.return_one_result() && tablet_op.count() != tablet_result.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet op count is not match to tablet results", K(ret),
             "table_op_count", tablet_op.count(), "tablet_result_count", tablet_result.count());
  } else if (ls_op.return_one_result() && tablet_result.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet op count should equal to 1 when return one result", K(ret),
             "table_op_count", tablet_op.count(), "tablet_result_count", tablet_result.count());
  }

  for (int i = 0; i < tablet_result.count() && OB_SUCC(ret); i++) {
    const ObTableSingleOp &single_op = tablet_op.at(i);
    const ObTableSingleOpEntity &req_entity= single_op.get_entities().at(0);
    ObTableSingleOpEntity *result_entity = static_cast<ObTableSingleOpEntity *>(tablet_result.at(i).get_entity());
    bool need_rebuild_bitmap = arg_.ls_op_.need_all_prop_bitmap() && single_op.get_op_type() == ObTableOperationType::GET;
    result_entity->set_dictionary(&result_.get_rowkey_names(), &result_.get_properties_names());
    if (need_rebuild_bitmap) { // construct result entity bitmap based on all columns dict
      if (OB_FAIL(result_entity->construct_names_bitmap_by_dict(req_entity))) {
        LOG_WARN("fail to construct name bitmap by all columns", K(ret), K(i));
      }
    } else if (OB_FAIL(result_entity->construct_names_bitmap(req_entity))) { // directly use request bitmap as result bitmap
      LOG_WARN("fail to construct name bitmap", K(ret), K(i));
    }
  }
  return ret;
}

int ObTableLSExecuteP::execute_single_query_and_mutate(const uint64_t table_id,
                                                       const common::ObTabletID tablet_id,
                                                       const ObTableSingleOp &single_op,
                                                       ObTableSingleOpResult &result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_arg_for_query_and_mutate(single_op))) {
    LOG_WARN("fail to check arg for query and mutate", K(ret));
  } else {
    // query is NULL has been check in check_arg_for_query_and_mutate
    const ObTableQuery *query = single_op.get_query();
    ObTableSingleOpQAM query_and_mutate(*query, true, single_op.is_check_no_exists());
    if (OB_FAIL(query_and_mutate.set_mutations(single_op))) {
      LOG_WARN("fail to set mutations", K(ret), "single_op", single_op);
    } else {
      ObTableQMParam qm_param(query_and_mutate);
      qm_param.table_id_ = table_id;
      qm_param.tablet_id_ = tablet_id;
      qm_param.timeout_ts_ = get_timeout_ts();
      qm_param.credential_ = credential_;
      qm_param.entity_type_ = arg_.entity_type_;
      qm_param.trans_desc_ = get_trans_desc();
      qm_param.tx_snapshot_ = &get_tx_snapshot();
      qm_param.single_op_result_ = &result;
      qm_param.schema_guard_ = &schema_guard_;
      qm_param.simple_table_schema_ = simple_table_schema_;
      qm_param.schema_cache_guard_ = &schema_cache_guard_;
      qm_param.sess_guard_ = &sess_guard_;
      SMART_VAR(QueryAndMutateHelper, helper, allocator_, qm_param, audit_ctx_) {
        if (OB_FAIL(helper.execute_query_and_mutate())) {
          LOG_WARN("fail to execute query and mutate", K(ret), K(single_op));
        } else {}
      }
    }
  }

  #ifndef NDEBUG
    // debug mode
    LOG_INFO("[TABLE] execute ls batch single operation", K(ret), K(single_op), K_(result));
  #else
    // release mode
    LOG_TRACE("[TABLE] execute ls batch single operation", K(ret), K(single_op), K_(result));
  #endif

  return ret;
}
