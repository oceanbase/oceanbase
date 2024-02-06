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

#define USING_LOG_PREFIX SERVER
#include "ob_table_batch_execute_processor.h"
#include "ob_table_rpc_processor_util.h"
#include "observer/ob_service.h"
#include "ob_table_end_trans_cb.h"
#include "sql/optimizer/ob_table_location.h"  // ObTableLocation
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "ob_htable_utils.h"
#include "ob_table_cg_service.h"
#include "observer/ob_req_time_service.h"
#include "ob_table_move_response.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

ObTableBatchExecuteP::ObTableBatchExecuteP(const ObGlobalContext &gctx)
    :ObTableRpcProcessor(gctx),
     default_entity_factory_("TableBatchEntFac", MTL_ID()),
     allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
     tb_ctx_(allocator_),
     need_rollback_trans_(false),
     batch_ops_atomic_(false)
{
}

int ObTableBatchExecuteP::deserialize()
{
  // we should set entity factory before deserialize
  arg_.batch_operation_.set_entity_factory(&default_entity_factory_);
  result_.set_entity_factory(&default_entity_factory_);
  int ret = ParentType::deserialize();
  if (OB_SUCC(ret) && ObTableEntityType::ET_HKV == arg_.entity_type_) {
    // for HKV, modify the value of timestamp to be negative
    const int64_t N = arg_.batch_operation_.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      ObITableEntity *entity = nullptr;
      if (OB_FAIL(const_cast<ObTableOperation&>(arg_.batch_operation_.at(i)).get_entity(entity))) {
        LOG_WARN("fail to get entity", K(ret), K(i));
      } else if (OB_FAIL(ObTableRpcProcessorUtil::negate_htable_timestamp(*entity))) {
        LOG_WARN("fail to negate timestamp value", K(ret));
      }
    } // end for
  }
  return ret;
}

int ObTableBatchExecuteP::check_arg()
{
  int ret = OB_SUCCESS;
  if (!(arg_.consistency_level_ == ObTableConsistencyLevel::STRONG ||
      arg_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("some options not supported yet", K(ret),
             "consistency_level", arg_.consistency_level_);
  }
  return ret;
}

int ObTableBatchExecuteP::check_arg2() const
{
  int ret = OB_SUCCESS;
  if (arg_.returning_rowkey_
      || arg_.returning_affected_entity_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("some options not supported yet", K(ret),
             "returning_rowkey", arg_.returning_rowkey_,
             "returning_affected_entity", arg_.returning_affected_entity_);
  }
  return ret;
}

OB_INLINE bool is_errno_need_retry(int ret)
{
  return OB_TRY_LOCK_ROW_CONFLICT == ret
      || OB_TRANSACTION_SET_VIOLATION == ret
      || OB_SCHEMA_ERROR == ret;
}

void ObTableBatchExecuteP::audit_on_finish()
{
  audit_record_.consistency_level_ = ObTableConsistencyLevel::STRONG == arg_.consistency_level_ ?
      ObConsistencyLevel::STRONG : ObConsistencyLevel::WEAK;
  audit_record_.return_rows_ = arg_.returning_affected_rows_ ? result_.count() : 0;
  audit_record_.table_scan_ = false;
  audit_record_.affected_rows_ = result_.count();
  audit_record_.try_cnt_ = retry_count_ + 1;
}

uint64_t ObTableBatchExecuteP::get_request_checksum()
{
  uint64_t checksum = 0;
  checksum = ob_crc64(checksum, arg_.table_name_.ptr(), arg_.table_name_.length());
  const uint64_t op_checksum = arg_.batch_operation_.get_checksum();
  checksum = ob_crc64(checksum, &op_checksum, sizeof(op_checksum));
  checksum = ob_crc64(checksum, &arg_.consistency_level_, sizeof(arg_.consistency_level_));
  checksum = ob_crc64(checksum, &arg_.returning_rowkey_, sizeof(arg_.returning_rowkey_));
  checksum = ob_crc64(checksum, &arg_.returning_affected_entity_, sizeof(arg_.returning_affected_entity_));
  checksum = ob_crc64(checksum, &arg_.returning_affected_rows_, sizeof(arg_.returning_affected_rows_));
  checksum = ob_crc64(checksum, &arg_.binlog_row_image_type_, sizeof(arg_.binlog_row_image_type_));
  return checksum;
}

int ObTableBatchExecuteP::response(const int retcode)
{
  int ret = OB_SUCCESS;
  if (!need_retry_in_queue_ && !had_do_response()) {
    // For HKV table, modify the value of timetamp to be positive
    if (ObTableEntityType::ET_HKV == arg_.entity_type_) {
      const int64_t N = result_.count();
      for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
      {
        ObITableEntity *entity = nullptr;
        if (OB_FAIL(result_.at(i).get_entity(entity))) {
          LOG_WARN("fail to get entity", K(ret), K(i));
        } else if (OB_FAIL(ObTableRpcProcessorUtil::negate_htable_timestamp(*entity))) {
          LOG_WARN("fail to negate timestamp value", K(ret));
        }
      } // end for
    }

    // return the package even if negate_htable_timestamp fails
    const obrpc::ObRpcPacket *rpc_pkt = &reinterpret_cast<const obrpc::ObRpcPacket&>(req_->get_packet());
    if (ObTableRpcProcessorUtil::need_do_move_response(retcode, *rpc_pkt)) {
      // response rerouting packet
      ObTableMoveResponseSender sender(req_, retcode);
      if (OB_FAIL(sender.init(arg_.table_id_, arg_.tablet_id_, *gctx_.schema_service_))) {
        LOG_WARN("fail to init move response sender", K(ret), K_(arg));
      } else if (OB_FAIL(sender.response())) {
        LOG_WARN("fail to do move response", K(ret));
      }
      if (OB_FAIL(ret)) {
        ret = ObRpcProcessor::response(retcode); // do common response when do move response failed
      }
    } else {
      ret = ObRpcProcessor::response(retcode);
    }
  }
  return ret;
}

void ObTableBatchExecuteP::reset_ctx()
{
  need_retry_in_queue_ = false;
  need_rollback_trans_ = false;
  result_.reset();
  ObTableApiProcessorBase::reset_ctx();
}


int ObTableBatchExecuteP::try_process()
{
  int ret = OB_SUCCESS;
  batch_ops_atomic_ = arg_.batch_operation_as_atomic_;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  uint64_t table_id = OB_INVALID_ID;
  bool is_index_supported = true;
  if (batch_operation.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no operation in the batch", K(ret));
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (FALSE_IT(table_id_ = arg_.table_id_)) {
  } else if (FALSE_IT(tablet_id_ = arg_.tablet_id_)) {
  } else if (OB_FAIL(check_table_index_supported(table_id_, is_index_supported))) {
    LOG_WARN("fail to check index supported", K(ret), K(table_id_));
  } else if (OB_UNLIKELY(!is_index_supported)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("index type is not supported by table api", K(ret));
  } else {
    if (batch_operation.is_readonly()) {
      if (batch_operation.is_same_properties_names()) {
        stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_GET;
        ret = multi_get();
      } else {
        stat_event_type_ = ObTableProccessType::TABLE_API_BATCH_RETRIVE;
        ret = batch_execute(true);
      }
    } else if (batch_operation.is_same_type()) {
      switch(batch_operation.at(0).type()) {
        case ObTableOperationType::INSERT:
          stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_INSERT;
          ret = multi_insert();
          break;
        case ObTableOperationType::DEL:
          if (ObTableEntityType::ET_HKV == arg_.entity_type_) {
            stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_DELETE;
            ret = htable_delete();
          } else {
            stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_DELETE;
            ret = multi_delete();
          }
          break;
        case ObTableOperationType::UPDATE:
          stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_UPDATE;
          ret = batch_execute(false);
          break;
        case ObTableOperationType::INSERT_OR_UPDATE:
          if (ObTableEntityType::ET_HKV == arg_.entity_type_) {
            stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_PUT;
            ret = htable_put();
          } else {
            stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_INSERT_OR_UPDATE;
            ret = batch_execute(false);
          }
          break;
        case ObTableOperationType::REPLACE:
          stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_REPLACE;
          ret = multi_replace();
          break;
        case ObTableOperationType::APPEND:
          stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_APPEND;
          ret = batch_execute(false);
          break;
        case ObTableOperationType::INCREMENT:
          stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_INCREMENT;
          ret = batch_execute(false);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected operation type", "type", batch_operation.at(0).type(), K(stat_event_type_));
          break;
      }
    } else {
      if (ObTableEntityType::ET_HKV == arg_.entity_type_) {
        // HTable mutate_row(RowMutations)
        stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_HYBRID;
        ret = htable_mutate_row();
      } else {
        // complex batch hybrid operation
        stat_event_type_ = ObTableProccessType::TABLE_API_BATCH_HYBRID;
        ret = batch_execute(false);
      }
    }
  }

  // record events
  audit_row_count_ = arg_.batch_operation_.count();

#ifndef NDEBUG
  // debug mode
  LOG_INFO("[TABLE] execute batch operation", K(ret), K_(arg), K_(result), "timeout", rpc_pkt_->get_timeout(), K_(retry_count));
#else
  // release mode
  LOG_TRACE("[TABLE] execute batch operation", K(ret), K_(arg), K_(result), "timeout", rpc_pkt_->get_timeout(), K_(retry_count),
            "receive_ts", get_receive_timestamp());
#endif
  return ret;
}

ObTableAPITransCb *ObTableBatchExecuteP::new_callback(rpc::ObRequest *req)
{
  ObTableBatchExecuteEndTransCb *cb = OB_NEW(ObTableBatchExecuteEndTransCb, ObModIds::TABLE_PROC, req, arg_.batch_operation_.at(0).type());
  if (NULL != cb) {
    // @todo optimize to avoid this copy
    int ret = OB_SUCCESS;
    if (OB_FAIL(cb->assign_batch_execute_result(result_))) {
      LOG_WARN("fail to assign result", K(ret));
      cb->~ObTableBatchExecuteEndTransCb();
      cb = NULL;
    } else {
      LOG_DEBUG("[yzfdebug] copy result", K_(result));
    }
  }
  return cb;
}

int ObTableBatchExecuteP::get_rowkeys(ObIArray<ObRowkey> &rowkeys)
{
  int ret = OB_SUCCESS;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  const int64_t N = batch_operation.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    const ObTableOperation &table_op = batch_operation.at(i);
    ObRowkey rowkey = const_cast<ObITableEntity&>(table_op.entity()).get_rowkey();
    if (OB_FAIL(rowkeys.push_back(rowkey))) {
      LOG_WARN("fail to push back", K(ret));
    }
  } // end for
  return ret;
}

int ObTableBatchExecuteP::get_tablet_ids(uint64_t table_id, ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id = arg_.tablet_id_;
  if (!tablet_id.is_valid()) {
    ObSEArray<ObRowkey, 3> rowkeys;
    if (OB_FAIL(get_rowkeys(rowkeys))) {
      LOG_WARN("fail to get rowkeys", K(ret));
    } else if (OB_FAIL(get_tablet_by_rowkey(table_id, rowkeys, tablet_ids))) {
      LOG_WARN("fail to get partition", K(ret), K(rowkeys));
    }
  } else {
    if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  return ret;
}

int ObTableBatchExecuteP::htable_put()
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  int64_t affected_rows = 0;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
  ObTableApiCacheGuard cache_guard;
  ObHTableLockHandle *lock_handle = nullptr;
  uint64_t table_id = OB_INVALID_ID;

  if (OB_FAIL(check_arg2())) {
    LOG_WARN("fail to check arg", K(ret));
  } else if (OB_FAIL(init_single_op_tb_ctx(tb_ctx_, batch_operation.at(0)))) {
    LOG_WARN("fail to init table ctx", K(ret));
  } else if (FALSE_IT(table_id = tb_ctx_.get_table_id())) {
  } else if (OB_FAIL(start_trans(false, /* is_readonly */
                                 sql::stmt::T_INSERT,
                                 arg_.consistency_level_,
                                 table_id,
                                 tb_ctx_.get_ls_id(),
                                 get_timeout_ts()))) {
    LOG_WARN("failed to start readonly transaction", K(ret));
  } else if (OB_FAIL(tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret), K(tb_ctx_));
  } else if (OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(get_trans_desc()->tid(), lock_handle))) {
    LOG_WARN("fail to get htable lock handle", K(ret));
  } else if (OB_FAIL(ObHTableUtils::lock_htable_rows(table_id, batch_operation, *lock_handle, ObHTableLockMode::SHARED))) {
    LOG_WARN("fail to lock htable rows", K(ret), K(table_id), K(batch_operation));
  } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_INSERT_UP>(tb_ctx_,
                                                                                    cache_guard,
                                                                                    spec))) {
    LOG_WARN("fail to get or create spec", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_operation.count(); ++i) {
      const ObTableOperation &table_operation = batch_operation.at(i);
      ObTableOperationResult single_op_result;
      tb_ctx_.set_entity(&table_operation.entity());
      if (OB_FAIL(ObTableOpWrapper::process_op_with_spec(tb_ctx_, spec, single_op_result))) {
        LOG_WARN("fail to process op with spec", K(ret));
      }
      table::ObTableApiUtil::replace_ret_code(ret);
      affected_rows += single_op_result.get_affected_rows();
    }
  }

  if (OB_SUCC(ret)) {
    ObTableOperationResult op_result;
    op_result.set_type(ObTableOperationType::INSERT_OR_UPDATE);
    op_result.set_entity(result_entity_);
    op_result.set_errno(ret);
    op_result.set_affected_rows(affected_rows);
    result_.reset();
    if (OB_FAIL(result_.push_back(op_result))) {
      LOG_WARN("failed to add result", K(ret));
    }
  }

  int tmp_ret = ret;
  const bool use_sync = false;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts(), use_sync, lock_handle))) {
    LOG_WARN("failed to end trans");
  }

  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableBatchExecuteP::multi_get()
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
  ObTableApiCacheGuard cache_guard;

  if (OB_FAIL(check_arg2())) {
    LOG_WARN("fail to check arg", K(ret));
  } else if (OB_FAIL(init_single_op_tb_ctx(tb_ctx_, batch_operation.at(0)))) {
    LOG_WARN("fail to init table ctx", K(ret));
  } else if (OB_FAIL(init_read_trans(arg_.consistency_level_,
                                     tb_ctx_.get_ls_id(),
                                     tb_ctx_.get_timeout_ts()))) {
    LOG_WARN("fail to init wead read trans", K(ret), K(tb_ctx_));
  } else if (OB_FAIL(tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret), K(tb_ctx_));
  } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_SCAN>(tb_ctx_,
                                                                               cache_guard,
                                                                               spec))) {
    LOG_WARN("fail to get or create spec", K(ret));
  } else {
    tb_ctx_.set_read_latest(false);
    const ObTableSchema *table_schema = tb_ctx_.get_table_schema();
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_operation.count(); ++i) {
      const ObTableOperation &table_operation = batch_operation.at(i);
      tb_ctx_.set_entity(&table_operation.entity());
      ObTableOperationResult op_result;
      ObITableEntity *result_entity = result_.get_entity_factory()->alloc();
      ObNewRow *row = nullptr;
      if (OB_FAIL(ObTableOpWrapper::process_get_with_spec(tb_ctx_, spec, row))) {
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to process get with spec", K(ret), K(i));
        }
      } else {
        // fill result entity
        ObArray<ObString> properties;
        const ObITableEntity *request_entity = tb_ctx_.get_entity();
        if (OB_FAIL(request_entity->get_properties_names(properties))) {
          LOG_WARN("fail to get entity properties", K(ret), K(i));
        } else if (OB_FAIL(ObTableApiUtil::construct_entity_from_row(allocator_,
                                                                     row,
                                                                     table_schema,
                                                                     properties,
                                                                     result_entity))) {
          LOG_WARN("fail to fill result entity", K(ret), K(i));
        }
      }
      op_result.set_entity(*result_entity);
      op_result.set_errno(ret);
      op_result.set_type(tb_ctx_.get_opertion_type());
      if (OB_FAIL(result_.push_back(op_result))) {
        LOG_WARN("fail to push back op result", K(ret), K(i));
      } else if (batch_ops_atomic_ && OB_FAIL(op_result.get_errno())) {
        LOG_WARN("fail to execute one operation when batch execute as atomic", K(ret), K(table_operation));
      }
    }
  }
  release_read_trans();

  return ret;
}

int ObTableBatchExecuteP::multi_delete()
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
  ObTableApiCacheGuard cache_guard;

  if (OB_FAIL(check_arg2())) {
    LOG_WARN("fail to check arg", K(ret));
  } else if (OB_FAIL(init_single_op_tb_ctx(tb_ctx_, batch_operation.at(0)))) {
    LOG_WARN("fail to init table ctx", K(ret));
  } else if (OB_FAIL(start_trans(false, /* is_readonly */
                                 sql::stmt::T_INSERT,
                                 arg_.consistency_level_,
                                 tb_ctx_.get_table_id(),
                                 tb_ctx_.get_ls_id(),
                                 get_timeout_ts()))) {
    LOG_WARN("fail to start readonly transaction", K(ret));
  } else if (OB_FAIL(tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret), K(tb_ctx_));
  } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_DELETE>(tb_ctx_, cache_guard, spec))) {
    LOG_WARN("fail to get or create spec", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_operation.count(); ++i) {
      const ObTableOperation &table_operation = batch_operation.at(i);
      tb_ctx_.set_entity(&table_operation.entity());
      ObTableOperationResult op_result;
      ObTableApiExecutor *executor = nullptr;
      ObITableEntity *result_entity = result_.get_entity_factory()->alloc();
      if (OB_ISNULL(result_entity)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memroy for result_entity", K(ret));
      } else if (FALSE_IT(op_result.set_entity(*result_entity))) {
      } else if (OB_FAIL(ObTableOpWrapper::process_op_with_spec(tb_ctx_, spec, op_result))) {
        LOG_WARN("fail to process insert with spec", K(ret), K(i));
        table::ObTableApiUtil::replace_ret_code(ret);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(result_.push_back(op_result))) {
        LOG_WARN("fail to push back result", K(ret));
      } else if (batch_ops_atomic_ && OB_FAIL(op_result.get_errno())) {
        LOG_WARN("fail to execute one operation when batch execute as atomic", K(ret), K(table_operation));
      }
    }
  }

  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("fail to end trans");
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableBatchExecuteP::htable_delete()
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  ObTableApiExecutor *executor = nullptr;
  int64_t affected_rows = 0;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  tb_ctx_.set_batch_operation(&batch_operation);
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
  ObTableApiCacheGuard cache_guard;
  ObHTableLockHandle *lock_handle = nullptr;
  uint64_t table_id = OB_INVALID_ID;

  if (OB_FAIL(check_arg2())) {
    LOG_WARN("fail to check arg", K(ret));
  } else if (OB_FAIL(init_single_op_tb_ctx(tb_ctx_, batch_operation.at(0)))) {
    LOG_WARN("fail to init table ctx", K(ret));
  } else if (FALSE_IT(table_id = tb_ctx_.get_table_id())) {
  } else if (OB_FAIL(start_trans(false, /* is_readonly */
                                 sql::stmt::T_INSERT,
                                 arg_.consistency_level_,
                                 table_id,
                                 tb_ctx_.get_ls_id(),
                                 get_timeout_ts()))) {
    LOG_WARN("failed to start readonly transaction", K(ret));
  } else if (OB_FAIL(tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret), K(tb_ctx_));
  } else if (OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(get_trans_desc()->tid(), lock_handle))) {
    LOG_WARN("fail to get htable lock handle", K(ret));
  } else if (OB_FAIL(ObHTableUtils::lock_htable_rows(table_id, batch_operation, *lock_handle, ObHTableLockMode::SHARED))) {
    LOG_WARN("fail to lock htable rows", K(ret), K(table_id), K(batch_operation));
  } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_DELETE>(tb_ctx_,
                                                                                 cache_guard,
                                                                                 spec))) {
    LOG_WARN("fail to get or create spec", K(ret));
  } else if (OB_FAIL(spec->create_executor(tb_ctx_, executor))) {
    LOG_WARN("fail to create executor", K(ret));
  } else {
    ObHTableDeleteExecutor delete_executor(tb_ctx_, static_cast<ObTableApiDeleteExecutor *>(executor));
    if (OB_FAIL(delete_executor.open())) {
      LOG_WARN("fail to open htable delete executor", K(ret));
    } else if (OB_FAIL(delete_executor.get_next_row())) {
      LOG_WARN("fail to call htable delete get_next_row", K(ret));
    } else if (FALSE_IT(affected_rows = delete_executor.get_affected_rows())) {
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = delete_executor.close())) {
      LOG_WARN("fail to close htable delete executor", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }

  if (OB_NOT_NULL(spec)) {
    spec->destroy_executor(executor);
    tb_ctx_.set_expr_info(nullptr);
  }

  if (OB_SUCC(ret)) {
    ObTableOperationResult single_op_result;
    single_op_result.set_entity(result_entity_);
    single_op_result.set_type(ObTableOperationType::DEL);
    single_op_result.set_errno(ret);
    single_op_result.set_affected_rows(affected_rows);
    result_.reset();
    if (OB_FAIL(result_.push_back(single_op_result))) {
      LOG_WARN("failed to add result", K(ret));
    }
  }
  const bool use_sync = false;
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts(), use_sync, lock_handle))) {
    LOG_WARN("failed to end trans");
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableBatchExecuteP::multi_insert()
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
  ObTableApiCacheGuard cache_guard;

  if (OB_FAIL(check_arg2())) {
    LOG_WARN("fail to check arg", K(ret));
  } else if (OB_FAIL(init_single_op_tb_ctx(tb_ctx_, batch_operation.at(0)))) {
    LOG_WARN("fail to init table ctx", K(ret));
  } else if (OB_FAIL(start_trans(false, /* is_readonly */
                                 sql::stmt::T_INSERT,
                                 arg_.consistency_level_,
                                 tb_ctx_.get_table_id(),
                                 tb_ctx_.get_ls_id(),
                                 get_timeout_ts()))) {
    LOG_WARN("fail to start readonly transaction", K(ret));
  } else if (OB_FAIL(tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret), K(tb_ctx_));
  } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_INSERT>(tb_ctx_,
                                                                                 cache_guard,
                                                                                 spec))) {
    LOG_WARN("fail to get or create spec", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_operation.count(); ++i) {
      const ObTableOperation &table_operation = batch_operation.at(i);
      tb_ctx_.set_entity(&table_operation.entity());
      ObTableOperationResult op_result;
      ObITableEntity *result_entity = result_.get_entity_factory()->alloc();
      if (OB_ISNULL(result_entity)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc entity", K(ret), K(i));
      } else if (FALSE_IT(op_result.set_entity(*result_entity))) {
      } else if (OB_FAIL(ObTableOpWrapper::process_op_with_spec(tb_ctx_, spec, op_result))) {
        LOG_WARN("fail to process insert with spec", K(ret), K(i));
        table::ObTableApiUtil::replace_ret_code(ret);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(result_.push_back(op_result))) {
        LOG_WARN("fail to push back result", K(ret));
      } else if (batch_ops_atomic_ && OB_FAIL(op_result.get_errno())) {
        LOG_WARN("fail to execute one operation when batch execute as atomic", K(ret), K(table_operation));
      }
    }
  }

  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("fail to end trans");
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableBatchExecuteP::multi_replace()
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
  ObTableApiCacheGuard cache_guard;

  if (OB_FAIL(check_arg2())) {
    LOG_WARN("fail to check arg", K(ret));
  } else if (OB_FAIL(init_single_op_tb_ctx(tb_ctx_, batch_operation.at(0)))) {
    LOG_WARN("fail to init table ctx", K(ret));
  } else if (OB_FAIL(start_trans(false, /* is_readonly */
                                 sql::stmt::T_INSERT,
                                 arg_.consistency_level_,
                                 tb_ctx_.get_table_id(),
                                 tb_ctx_.get_ls_id(),
                                 get_timeout_ts()))) {
    LOG_WARN("fail to start readonly transaction", K(ret));
  } else if (OB_FAIL(tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret), K(tb_ctx_));
  } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_REPLACE>(tb_ctx_,
                                                                                  cache_guard,
                                                                                  spec))) {
    LOG_WARN("fail to get or create spec", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_operation.count(); ++i) {
      const ObTableOperation &table_operation = batch_operation.at(i);
      tb_ctx_.set_entity(&table_operation.entity());
      ObTableOperationResult op_result;
      ObITableEntity *result_entity = result_.get_entity_factory()->alloc();
      if (OB_ISNULL(result_entity)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc entity", K(ret), K(i));
      } else if (FALSE_IT(op_result.set_entity(*result_entity))) {
      } else if (OB_FAIL(ObTableOpWrapper::process_op_with_spec(tb_ctx_, spec, op_result))) {
        LOG_WARN("fail to process insert with spec", K(ret), K(i));
        table::ObTableApiUtil::replace_ret_code(ret);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(result_.push_back(op_result))) {
        LOG_WARN("fail to push back result", K(ret));
      } else if (batch_ops_atomic_ && OB_FAIL(op_result.get_errno())) {
        LOG_WARN("fail to execute one operation when batch execute as atomic", K(ret), K(table_operation));
      }
    }
  }

  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("fail to end trans");
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableBatchExecuteP::batch_execute(bool is_readonly)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObLSID ls_id;
  if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("fail to get table id", K(ret));
  } else if (OB_FAIL(get_tablet_ids(table_id, tablet_ids))) {
    LOG_WARN("fail to get tablet id", K(ret));
  } else if (1 != tablet_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("should have one tablet", K(ret), K(tablet_ids));
  } else if (OB_FAIL(get_ls_id(tablet_ids.at(0), ls_id))) {
    LOG_WARN("fail to get ls id", K(ret));
  } else if (OB_FAIL(start_trans(is_readonly, /* is_readonly */
                                 (is_readonly ? sql::stmt::T_SELECT : sql::stmt::T_UPDATE),
                                 arg_.consistency_level_,
                                 table_id,
                                 ls_id,
                                 get_timeout_ts()))) {
    LOG_WARN("fail to start readonly transaction", K(ret));
  } else if (OB_FAIL(batch_execute_internal(arg_.batch_operation_, result_))) {
    LOG_WARN("fail to execute batch", K(ret));
  }

  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("fail to end trans");
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableBatchExecuteP::batch_execute_internal(const ObTableBatchOperation &batch_operation,
                                                 ObTableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  // loop: process each op
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_operation.count(); ++i) {
    const ObTableOperation &table_operation = batch_operation.at(i);
    ObTableOperationResult op_result;
    ObITableEntity *result_entity = result.get_entity_factory()->alloc();

    SMART_VAR(table::ObTableCtx, op_tb_ctx, allocator_) {
      if (OB_FAIL(init_single_op_tb_ctx(op_tb_ctx, table_operation))) {
        LOG_WARN("fail to init table ctx for single operation", K(ret));
      }  else if (OB_FAIL(op_tb_ctx.init_trans(get_trans_desc(), get_tx_snapshot()))) {
        LOG_WARN("fail to init trans", K(ret), K(op_tb_ctx));
      } else if (OB_ISNULL(result_entity)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memroy for result_entity", K(ret));
      } else {
        op_result.set_entity(*result_entity);
      }
      if (OB_SUCC(ret)) {
        switch(table_operation.type()) {
          case ObTableOperationType::GET:
            ret = process_get(op_tb_ctx, op_result);
            break;
          case ObTableOperationType::INSERT:
            ret = ObTableOpWrapper::process_op<TABLE_API_EXEC_INSERT>(op_tb_ctx, op_result);
            break;
          case ObTableOperationType::DEL:
            ret = ObTableOpWrapper::process_op<TABLE_API_EXEC_DELETE>(op_tb_ctx, op_result);
            break;
          case ObTableOperationType::UPDATE:
            ret = ObTableOpWrapper::process_op<TABLE_API_EXEC_UPDATE>(op_tb_ctx, op_result);
            break;
          case ObTableOperationType::INSERT_OR_UPDATE:
            ret = ObTableOpWrapper::process_op<TABLE_API_EXEC_INSERT_UP>(op_tb_ctx, op_result);
            break;
          case ObTableOperationType::REPLACE:
            ret = ObTableOpWrapper::process_op<TABLE_API_EXEC_REPLACE>(op_tb_ctx, op_result);
            break;
          case ObTableOperationType::APPEND:
          case ObTableOperationType::INCREMENT:
            ret = ObTableOpWrapper::process_op<TABLE_API_EXEC_INSERT_UP>(op_tb_ctx, op_result);
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("unexpected operation type", "type", table_operation.type());
            break;
        }
        ObTableApiUtil::replace_ret_code(ret);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(result.push_back(op_result))) {
        LOG_WARN("fail to push back result", K(ret));
      } else if (batch_ops_atomic_ && OB_FAIL(op_result.get_errno())) {
        LOG_WARN("fail to execute one operation when batch execute as atomic", K(ret), K(table_operation));
      }
    } else {
      LOG_WARN("fail to execute batch operation, ", K(ret), K(table_operation.type()), K(i));
    }
  } // end for
  return ret;
}

int ObTableBatchExecuteP::init_single_op_tb_ctx(table::ObTableCtx &ctx,
                                                const ObTableOperation &table_operation)
{
  int ret = OB_SUCCESS;
  ctx.set_entity(&table_operation.entity());
  ctx.set_entity_type(arg_.entity_type_);
  ctx.set_operation_type(table_operation.type());
  ObExprFrameInfo *expr_frame_info = nullptr;
  if (ctx.is_init()) {
    LOG_INFO("tb ctx has been inited", K(ctx));
  } else if (OB_FAIL(ctx.init_common(credential_,
                                     arg_.tablet_id_,
                                     arg_.table_name_,
                                     get_timeout_ts()))) {
    LOG_WARN("fail to init table ctx common part", K(ret), K(arg_.table_name_));
  } else {
    ObTableOperationType::Type op_type = table_operation.type();
    switch (op_type) {
      case ObTableOperationType::GET: {
        if (OB_FAIL(ctx.init_get())) {
          LOG_WARN("fail to init get ctx", K(ret), K(ctx));
        }
        break;
      }
      case ObTableOperationType::INSERT: {
        if (OB_FAIL(ctx.init_insert())) {
          LOG_WARN("fail to init insert ctx", K(ret), K(ctx));
        }
        break;
      }
      case ObTableOperationType::DEL: {
        if (OB_FAIL(ctx.init_delete())) {
          LOG_WARN("fail to init delete ctx", K(ret), K(ctx));
        }
        break;
      }
      case ObTableOperationType::UPDATE: {
        if (OB_FAIL(ctx.init_update())) {
          LOG_WARN("fail to init update ctx", K(ret), K(ctx));
        }
        break;
      }
      case ObTableOperationType::INSERT_OR_UPDATE: {
        if (OB_FAIL(ctx.init_insert_up())) {
          LOG_WARN("fail to init insert up ctx", K(ret), K(ctx));
        }
        break;
      }
      case ObTableOperationType::REPLACE: {
        if (OB_FAIL(ctx.init_replace())) {
          LOG_WARN("fail to init replace ctx", K(ret), K(ctx));
        }
        break;
      }
      case ObTableOperationType::APPEND: {
        if (OB_FAIL(ctx.init_append(arg_.returning_affected_entity_,
                                    arg_.returning_rowkey_))) {
          LOG_WARN("fail to init append ctx", K(ret), K(ctx));
        }
        break;
      }
      case ObTableOperationType::INCREMENT: {
        if (OB_FAIL(ctx.init_increment(arg_.returning_affected_entity_,
                                       arg_.returning_rowkey_))) {
          LOG_WARN("fail to init increment ctx", K(ret), K(ctx));
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
  } else if (OB_FAIL(ctx.init_exec_ctx())) {
    LOG_WARN("fail to init exec ctx", K(ret), K(ctx));
  }

  return ret;
}

int ObTableBatchExecuteP::process_get(table::ObTableCtx &op_tb_ctx,
                                      ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObNewRow *row;
  ObITableEntity *result_entity = nullptr;
  const ObTableSchema *table_schema = op_tb_ctx.get_table_schema();
  const ObTableEntity *request_entity = static_cast<const ObTableEntity *>(op_tb_ctx.get_entity());
  const ObIArray<ObString> &cnames = request_entity->get_properties_names();
  if (OB_FAIL(ObTableOpWrapper::process_get(op_tb_ctx, row))) {
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to process get", K(ret));
    }
  } else if (OB_FAIL(result.get_entity(result_entity))) {
    LOG_WARN("fail to get result entity", K(ret));
  } else if (OB_FAIL(ObTableApiUtil::construct_entity_from_row(allocator_,
                                                               row,
                                                               table_schema,
                                                               cnames,
                                                               result_entity))) {
    LOG_WARN("fail to cosntruct result entity", K(ret));
  }
  result.set_errno(ret);
  result.set_type(op_tb_ctx.get_opertion_type());
  return ret;
}

int ObTableBatchExecuteP::htable_mutate_row()
{
  int ret = OB_SUCCESS;
  const ObTableConsistencyLevel consistency_level = arg_.consistency_level_;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  uint64_t table_id = OB_INVALID_ID;
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObLSID ls_id;
  ObHTableLockHandle *lock_handle = nullptr;

  if (OB_FAIL(check_arg2())) {
    LOG_WARN("fail to check arg", K(ret));
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("fail to get table id", K(ret));
  } else if (OB_FAIL(get_tablet_ids(table_id, tablet_ids))) {
    LOG_WARN("fail to get tablet id", K(ret));
  } else if (1 != tablet_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("should have one tablet", K(ret), K(tablet_ids));
  } else if (OB_FAIL(get_ls_id(tablet_ids.at(0), ls_id))) {
    LOG_WARN("fail to get ls id", K(ret));
  } else if (OB_FAIL(start_trans(false, /* is_readonly */
                                 sql::stmt::T_DELETE,
                                 consistency_level,
                                 table_id,
                                 ls_id,
                                 get_timeout_ts()))) {
    LOG_WARN("fail to start transaction", K(ret));
  } else if (OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(get_trans_desc()->tid(), lock_handle))) {
    LOG_WARN("fail to get htable lock handle", K(ret));
  } else if (OB_FAIL(ObHTableUtils::lock_htable_rows(table_id, batch_operation, *lock_handle, ObHTableLockMode::SHARED))) {
    LOG_WARN("fail to lock htable rows", K(ret), K(table_id), K(batch_operation));
  } else {
    int64_t N = batch_operation.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      // execute each mutation one by one
      const ObTableOperation &table_operation = batch_operation.at(i);
      ObTableBatchOperation batch_ops;
      if (OB_FAIL(batch_ops.add(table_operation))) {
        LOG_WARN("failed to add operation", K(ret), K(table_operation));
      } else {
        switch (table_operation.type()) {
          case ObTableOperationType::INSERT_OR_UPDATE: {
            if (OB_FAIL(execute_htable_put(batch_ops))) {
              LOG_WARN("fail to execute htable put", K(ret), K(i), K(N), K(table_operation));
            }
            break;
          }

          case ObTableOperationType::DEL: {
            if (OB_FAIL(execute_htable_delete(batch_ops))) {
              LOG_WARN("fail to execute htable delete", K(ret), K(i), K(N), K(table_operation));
            }
            break;
          }

          default: {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not supported mutation type", K(ret), K(table_operation));
            break;
          }
        }
      }
    }
  }

  int tmp_ret = ret;
  const bool use_sync = false;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts(), use_sync, lock_handle))) {
    LOG_WARN("failed to end trans");
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;

  return ret;
}

int ObTableBatchExecuteP::execute_htable_delete(const ObTableBatchOperation &batch_operation)
{
  int ret = OB_SUCCESS;

  SMART_VAR(ObTableCtx, tb_ctx, allocator_) {
    ObTableApiSpec *spec = nullptr;
    ObTableApiExecutor *executor = nullptr;
    observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
    ObTableApiCacheGuard cache_guard;
    int64_t affected_rows = 0;
    tb_ctx.set_batch_operation(&batch_operation);

    if (OB_FAIL(init_single_op_tb_ctx(tb_ctx, batch_operation.at(0)))) {
      LOG_WARN("fail to init table ctx", K(ret));
    } else if (OB_FAIL(tb_ctx.init_trans(get_trans_desc(), get_tx_snapshot()))) {
      LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
    } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_DELETE>(tb_ctx, cache_guard, spec))) {
      LOG_WARN("fail to get or create spec", K(ret));
    } else if (OB_FAIL(spec->create_executor(tb_ctx, executor))) {
      LOG_WARN("fail to create executor", K(ret));
    } else {
      ObHTableDeleteExecutor delete_executor(tb_ctx, static_cast<ObTableApiDeleteExecutor *>(executor));
      if (OB_FAIL(delete_executor.open())) {
        LOG_WARN("fail to open htable delete executor", K(ret));
      } else if (OB_FAIL(delete_executor.get_next_row())) {
        LOG_WARN("fail to call htable delete get_next_row", K(ret));
      } else if (FALSE_IT(affected_rows = delete_executor.get_affected_rows())) {
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = delete_executor.close())) {
        LOG_WARN("fail to close htable delete executor", K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
    if (OB_NOT_NULL(spec)) {
      spec->destroy_executor(executor);
      tb_ctx.set_expr_info(nullptr);
    }

    if (OB_SUCC(ret)) {
      ObTableOperationResult single_op_result;
      single_op_result.set_entity(result_entity_);
      single_op_result.set_type(ObTableOperationType::DEL);
      single_op_result.set_errno(ret);
      single_op_result.set_affected_rows(affected_rows);
      result_.reset();
      if (OB_FAIL(result_.push_back(single_op_result))) {
        LOG_WARN("failed to add result", K(ret));
      }
    }
  }

  return ret;
}

int ObTableBatchExecuteP::execute_htable_put(const ObTableBatchOperation &batch_operation)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObTableCtx, tb_ctx, allocator_) {
    ObTableApiSpec *spec = nullptr;
    ObTableApiExecutor *executor = nullptr;
    int64_t affected_rows = 0;
    tb_ctx.set_batch_operation(&batch_operation);
    ObTableOperationResult single_op_result;
    single_op_result.set_entity(result_entity_);
    if (OB_FAIL(init_single_op_tb_ctx(tb_ctx, batch_operation.at(0)))) {
      LOG_WARN("fail to init table ctx", K(ret));
    } else if (OB_FAIL(tb_ctx.init_trans(get_trans_desc(), get_tx_snapshot()))) {
      LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
    } else if (OB_FAIL(ObTableOpWrapper::process_op<TABLE_API_EXEC_INSERT_UP>(tb_ctx, single_op_result))) {
      LOG_WARN("fail to process insertup op", K(ret));
    } else if (FALSE_IT(result_.reset())) {
    } else if (OB_FAIL(result_.push_back(single_op_result))) {
      LOG_WARN("fail to push add result", K(ret));
    }
  }

  return ret;
}