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
    default_entity_factory_("TableLSExeEntFac", MTL_ID()),
    tb_ctx_(allocator_)
{
}

int ObTableLSExecuteP::deserialize()
{
  arg_.ls_op_.set_deserialize_allocator(&allocator_);
  arg_.ls_op_.set_entity_factory(&default_entity_factory_);

  int ret = ParentType::deserialize();
  if (OB_SUCC(ret) && ObTableEntityType::ET_HKV == arg_.entity_type_) {
    // For HKV table, modify the timestamp value to be negative
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_.ls_op_.count(); i++) {
      const ObTableTabletOp &tablet_op = arg_.ls_op_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < tablet_op.count(); j++) {
        const ObTableSingleOp &single_op = tablet_op.at(j);
        const ObIArray<ObTableSingleOpEntity> &entities = single_op.get_entities();
        for (int64_t k = 0; OB_SUCC(ret) && k < entities.count(); ++i) {
          ObTableSingleOpEntity &entity = const_cast<ObTableSingleOpEntity &>(entities.at(k));
          if (OB_FAIL(ObTableRpcProcessorUtil::negate_htable_timestamp(entity))) {
            LOG_WARN("failed to negate timestamp value", K(ret));
          }
        } // end for
      }
    }
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
  bool is_hkv = ObTableEntityType::ET_HKV == arg_.entity_type_;
  for (int64_t i = 0; OB_SUCC(ret) && i < arg_.ls_op_.count(); i++) {
    ObTableTabletOp &tablet_op = arg_.ls_op_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < tablet_op.count(); j++) {
      ObTableSingleOp &single_op = tablet_op.at(j);
      ObTableQuery &query = single_op.get_query();
      ObHTableFilter &hfilter = query.htable_filter();
      const ObIArray<ObTableSingleOpEntity> &entities = single_op.get_entities();
      if (single_op.get_op_type() != ObTableOperationType::CHECK_AND_INSERT_UP) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "single op type is not check and insert up");
        LOG_WARN("invalid single op type", KR(ret), "single op type", single_op.get_op_type());
      } else if (!query.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query is invalid");
        LOG_WARN("invalid table query request", K(ret), K(query));
      } else if (is_hkv && !hfilter.is_valid()) {
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
        query.set_batch(1);  // mutate for each row
        query.set_max_result_size(-1);
        hfilter.set_max_versions(1);
        hfilter.set_row_offset_per_column_family(0);
        hfilter.set_max_results_per_column_family(-1);
      }
    }
  }
  return ret;
}

void ObTableLSExecuteP::audit_on_finish()
{
  audit_record_.consistency_level_ = ObConsistencyLevel::STRONG;
  audit_record_.table_scan_ = true;
  audit_record_.try_cnt_ = retry_count_ + 1;
  for (int64_t i = 0; i < result_.count(); i++) {
    for (int64_t j = 0; j < result_.at(i).count(); j++) {
      audit_record_.affected_rows_ += result_.at(i).at(j).get_affected_rows();
      audit_record_.return_rows_ += OB_ISNULL(result_.at(i).at(j).get_entity()) ? 0 : 1;
    }
  }
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
}

ObTableAPITransCb *ObTableLSExecuteP::new_callback(rpc::ObRequest *req)
{
  ObTableLSExecuteEndTransCb *cb = OB_NEW(ObTableLSExecuteEndTransCb, ObModIds::TABLE_PROC, req);
  if (NULL != cb) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(cb->assign_ls_execute_result(result_))) {
      LOG_WARN("fail to assign result", K(ret));
      cb->~ObTableLSExecuteEndTransCb();
      cb = NULL;
    } else {
      LOG_DEBUG("copy ls execute result", K_(result));
    }
  }
  return cb;
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
  const ObTableLSOp &ls_op = arg_.ls_op_;
  ObLSID ls_id = ls_op.get_ls_id();
  bool exist_global_index = false;
  if (OB_FAIL(get_ls_id(ls_id))) {
    LOG_WARN("fail to get ls id", K(ret));
  } else if (OB_FAIL(check_has_global_index(exist_global_index))) {
    LOG_WARN("fail to check global index", K(ret), K_(table_id));
  } else if (OB_FAIL(start_trans(false, /* is_readonly */
                                 sql::stmt::T_UPDATE,
                                 arg_.consistency_level_,
                                 ls_id,
                                 get_timeout_ts(),
                                 exist_global_index))) {
      LOG_WARN("fail to start transaction", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < ls_op.count(); i++) {
    const ObTableTabletOp &tablet_op = ls_op.at(i);
    ObTableTabletOpResult tablet_result;
    if (OB_FAIL(execute_tablet_op(tablet_op, tablet_result))) {
      LOG_WARN("fail to execute tablet op", KR(ret), K(tablet_op));
    } else if (OB_FAIL(result_.push_back(tablet_result))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }

  int tmp_ret = ret;
  const bool use_sync = false;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts(), use_sync))) {
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


int ObTableLSExecuteP::execute_tablet_op(const ObTableTabletOp &tablet_op, ObTableTabletOpResult&tablet_result)
{
  int ret = OB_SUCCESS;
  if (tablet_op.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operation count", KR(ret), K(tablet_op.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_op.count(); i++) {
      const ObTableSingleOp &single_op = tablet_op.at(i);
      const ObTableSingleOpEntity& req_entity= single_op.get_entities().at(0);

      ObTableSingleOpResult single_op_result;
      single_op_result.set_errno(OB_SUCCESS);
      ObITableEntity *result_entity = default_entity_factory_.alloc();

      if (OB_ISNULL(result_entity)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memroy for result_entity", K(ret));
      } else if (FALSE_IT(result_entity->set_dictionary(&arg_.ls_op_.get_all_rowkey_names(),
                                                       &arg_.ls_op_.get_all_properties_names()))) {
      } else if (FALSE_IT(single_op_result.set_entity(*result_entity))) {
      } else if (OB_FAIL(execute_single_op(
                     arg_.ls_op_.get_table_id(), tablet_op.get_tablet_id(), single_op, single_op_result))) {
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

int ObTableLSExecuteP::execute_single_op(const uint64_t table_id, const common::ObTabletID tablet_id, const ObTableSingleOp &single_op, ObTableSingleOpResult &result)
{
  int ret = OB_SUCCESS;
  ObTableSingleOpQAM query_and_mutate(single_op.get_query(), true, single_op.is_check_no_exists());
  if (OB_FAIL(query_and_mutate.set_mutations(single_op))) {
    LOG_WARN("fail to set mutations", K(ret), "single_op", single_op);
  } else {
    SMART_VAR(QueryAndMutateHelper, helper, allocator_, query_and_mutate,
              table_id, tablet_id, get_timeout_ts(), credential_, arg_.entity_type_,
              get_trans_desc(), get_tx_snapshot(), result) {
      if (OB_FAIL(helper.execute_query_and_mutate())) {
        LOG_WARN("fail to execute query and mutate", K(ret), K(single_op));
      } else {}
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

int ObTableLSExecuteP::check_has_global_index(bool &exist_global_index)
{
  int ret = OB_SUCCESS;
  exist_global_index = false;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObSimpleTableSchemaV2 *table_schema = NULL;
  if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(credential_.tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(credential_.tenant_id_));
  } else {
    const ObTableLSOp &ls_op = arg_.ls_op_;
    const uint64_t table_id = ls_op.get_table_id();
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_op.count() && !exist_global_index; i++) {
      if (OB_FAIL(schema_guard.get_simple_table_schema(credential_.tenant_id_, table_id, table_schema))) {
        LOG_WARN("fail to get table schema", K(ret), K(table_id));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("get null table schema", K(ret), K(table_id));
      } else if (OB_FAIL(schema_guard.check_global_index_exist(credential_.tenant_id_, table_id, exist_global_index))) {
        LOG_WARN("fail to check global index", K(ret), K(table_id));
      }
    }
  }
  return ret;
}
