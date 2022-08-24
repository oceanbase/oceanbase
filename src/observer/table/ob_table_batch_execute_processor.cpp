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
#include "storage/ob_partition_service.h"
#include "ob_table_end_trans_cb.h"
#include "sql/optimizer/ob_table_location.h"  // ObTableLocation
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "ob_htable_utils.h"
using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

ObTableBatchExecuteP::ObTableBatchExecuteP(const ObGlobalContext &gctx)
    :ObTableRpcProcessor(gctx),
     allocator_(ObModIds::TABLE_PROC),
     table_service_ctx_(allocator_),
     need_rollback_trans_(false)
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
        LOG_WARN("failed to get entity", K(ret), K(i));
      } else if (OB_FAIL(ObTableRpcProcessorUtil::negate_htable_timestamp(*entity))) {
        LOG_WARN("failed to negate timestamp value", K(ret));
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
  if (!need_retry_in_queue_ && !did_async_end_trans()) {
    // For HKV table, modify the value of timetamp to be positive
    if (OB_SUCC(ret) && ObTableEntityType::ET_HKV == arg_.entity_type_) {
      const int64_t N = result_.count();
      for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
      {
        ObITableEntity *entity = nullptr;
        if (OB_FAIL(result_.at(i).get_entity(entity))) {
          LOG_WARN("failed to get entity", K(ret), K(i));
        } else if (OB_FAIL(ObTableRpcProcessorUtil::negate_htable_timestamp(*entity))) {
          LOG_WARN("failed to negate timestamp value", K(ret));
        }
      } // end for
    }
    if (OB_SUCC(ret)) {
      ret = ObRpcProcessor::response(retcode);
    }
  }
  return ret;
}

void ObTableBatchExecuteP::reset_ctx()
{
  table_service_ctx_.reset_dml();
  need_retry_in_queue_ = false;
  need_rollback_trans_ = false;
  result_.reset();
  ObTableApiProcessorBase::reset_ctx();
}


int ObTableBatchExecuteP::try_process()
{
  int ret = OB_SUCCESS;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  uint64_t table_id = OB_INVALID_ID;
  bool is_index_supported = true;
  if (batch_operation.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no operation in the batch", K(ret));
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (OB_FAIL(check_table_index_supported(table_id, is_index_supported))) {
    LOG_WARN("fail to check index supported", K(ret), K(table_id));
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
          ret = multi_update();
          break;
        case ObTableOperationType::INSERT_OR_UPDATE:
          if (ObTableEntityType::ET_HKV == arg_.entity_type_) {
            stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_PUT;
            ret = htable_put();
          } else {
            stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_INSERT_OR_UPDATE;
            ret = multi_insert_or_update();
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
      LOG_WARN("failed to assign result", K(ret));
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
      LOG_WARN("failed to push back", K(ret));
    }
  } // end for
  return ret;
}

int ObTableBatchExecuteP::get_partition_ids(uint64_t table_id, ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;
  uint64_t partition_id = arg_.partition_id_;
  if (OB_INVALID_ID == partition_id) {
    ObSEArray<sql::RowkeyArray, 3> rowkeys_per_part;
    ObSEArray<ObRowkey, 3> rowkeys;
    if (OB_FAIL(get_rowkeys(rowkeys))) {
      LOG_WARN("failed to get rowkeys", K(ret));
    } else if (OB_FAIL(get_partition_by_rowkey(table_id, rowkeys, part_ids, rowkeys_per_part))) {
      LOG_WARN("failed to get partition", K(ret), K(rowkeys));
    }
  } else {
    if (OB_FAIL(part_ids.push_back(partition_id))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObTableBatchExecuteP::multi_insert_or_update()
{
  int ret = OB_SUCCESS;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  const bool is_readonly = false;
  uint64_t &table_id = table_service_ctx_.param_table_id();
  table_service_ctx_.init_param(get_timeout_ts(), this->get_trans_desc(), &allocator_,
                                arg_.returning_affected_rows_,
                                arg_.entity_type_,
                                arg_.binlog_row_image_type_);
  ObSEArray<int64_t, 1> part_ids;
  if (OB_FAIL(check_arg2())) {
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (OB_FAIL(get_partition_ids(table_id, part_ids))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (1 != part_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("should have one partition", K(ret), K(part_ids));
  } else if (FALSE_IT(table_service_ctx_.param_partition_id() = part_ids.at(0))) {
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_INSERT, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else if (OB_FAIL(table_service_->multi_insert_or_update(table_service_ctx_, batch_operation, result_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to insert_or_update", K(ret), K(table_id));
    }
  }
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans");
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableBatchExecuteP::htable_put()
{
  int ret = OB_SUCCESS;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  const bool is_readonly = false;
  uint64_t table_id = OB_INVALID_ID;
  ObSEArray<int64_t, 1> part_ids;

  if (OB_FAIL(check_arg2())) {
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (OB_FAIL(get_partition_ids(table_id, part_ids))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (1 != part_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("should have one partition", K(ret), K(part_ids));
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_INSERT, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObHTablePutExecutor put_executor(allocator_,
                                     table_id,
                                     part_ids.at(0),
                                     get_timeout_ts(),
                                     this,
                                     table_service_,
                                     part_service_);
    ret = put_executor.htable_put(batch_operation, affected_rows);
    if (OB_SUCC(ret)) {
      ObTableOperationResult single_op_result;
      single_op_result.set_entity(result_entity_);
      single_op_result.set_type(ObTableOperationType::INSERT_OR_UPDATE);
      single_op_result.set_errno(ret);
      single_op_result.set_affected_rows(affected_rows);
      result_.reset();
      if (OB_FAIL(result_.push_back(single_op_result))) {
        LOG_WARN("failed to add result", K(ret));
      }
    }
  }
  
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans");
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableBatchExecuteP::multi_get()
{
  int ret = OB_SUCCESS;
  need_rollback_trans_ = false;
  uint64_t &table_id = table_service_ctx_.param_table_id();
  table_service_ctx_.init_param(get_timeout_ts(), this->get_trans_desc(), &allocator_,
                                arg_.returning_affected_rows_,
                                arg_.entity_type_,
                                arg_.binlog_row_image_type_);
  ObSEArray<int64_t, 1> part_ids;
  const bool is_readonly = true;
  const ObTableConsistencyLevel consistency_level = arg_.consistency_level_;
  if (OB_FAIL(check_arg2())) {
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (OB_FAIL(get_partition_ids(table_id, part_ids))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (1 != part_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("should have one partition", K(ret), K(part_ids));
  } else if (FALSE_IT(table_service_ctx_.param_partition_id() = part_ids.at(0))) {
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_SELECT, consistency_level, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start readonly transaction", K(ret));
  } else if (OB_FAIL(table_service_->multi_get(table_service_ctx_, arg_.batch_operation_, result_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to execute get", K(ret), K(table_id));
    }
  } else {}
  need_rollback_trans_ = (OB_SUCCESS != ret);
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(need_rollback_trans_, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans", K(ret), "rollback", need_rollback_trans_);
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableBatchExecuteP::multi_delete()
{
  int ret = OB_SUCCESS;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  const bool is_readonly = false;
  uint64_t &table_id = table_service_ctx_.param_table_id();
  table_service_ctx_.init_param(get_timeout_ts(), this->get_trans_desc(), &allocator_,
                                arg_.returning_affected_rows_,
                                arg_.entity_type_,
                                arg_.binlog_row_image_type_);
  ObSEArray<int64_t, 1> part_ids;
  if (OB_FAIL(check_arg2())) {
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (OB_FAIL(get_partition_ids(table_id, part_ids))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (1 != part_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("should have one partition", K(ret), K(part_ids));
  } else if (FALSE_IT(table_service_ctx_.param_partition_id() = part_ids.at(0))) {
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_DELETE, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else if (OB_FAIL(table_service_->multi_delete(table_service_ctx_, batch_operation, result_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to multi_delete", K(ret), K(table_id));
    }
  }
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans");
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableBatchExecuteP::htable_delete()
{
  int ret = OB_SUCCESS;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  const bool is_readonly = false;
  uint64_t table_id = OB_INVALID_ID;
  ObSEArray<int64_t, 1> part_ids;

  if (OB_FAIL(check_arg2())) {
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (OB_FAIL(get_partition_ids(table_id, part_ids))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (1 != part_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("should have one partition", K(ret), K(part_ids));
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_DELETE, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObHTableDeleteExecutor delete_executor(allocator_,
                                           table_id,
                                           part_ids.at(0),
                                           get_timeout_ts(),
                                           this,
                                           table_service_,
                                           part_service_);
    ret = delete_executor.htable_delete(batch_operation, affected_rows);
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
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans");
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableBatchExecuteP::multi_insert()
{
  int ret = OB_SUCCESS;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  const bool is_readonly = false;
  uint64_t &table_id = table_service_ctx_.param_table_id();
  table_service_ctx_.init_param(get_timeout_ts(), this->get_trans_desc(), &allocator_,
                                arg_.returning_affected_rows_,
                                arg_.entity_type_,
                                arg_.binlog_row_image_type_);
  ObSEArray<int64_t, 1> part_ids;
  if (OB_FAIL(check_arg2())) {
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (OB_FAIL(get_partition_ids(table_id, part_ids))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (1 != part_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("should have one partition", K(ret), K(part_ids));
  } else if (FALSE_IT(table_service_ctx_.param_partition_id() = part_ids.at(0))) {
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_INSERT, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else if (OB_FAIL(table_service_->multi_insert(table_service_ctx_, batch_operation, result_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to multi_insert", K(ret), K(table_id));
    }
  }
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans");
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableBatchExecuteP::multi_replace()
{
  int ret = OB_SUCCESS;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  const bool is_readonly = false;
  uint64_t &table_id = table_service_ctx_.param_table_id();
  table_service_ctx_.init_param(get_timeout_ts(), this->get_trans_desc(), &allocator_,
                                arg_.returning_affected_rows_,
                                arg_.entity_type_,
                                arg_.binlog_row_image_type_);
  ObSEArray<int64_t, 1> part_ids;
  if (OB_FAIL(check_arg2())) {
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (OB_FAIL(get_partition_ids(table_id, part_ids))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (1 != part_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("should have one partition", K(ret), K(part_ids));
  } else if (FALSE_IT(table_service_ctx_.param_partition_id() = part_ids.at(0))) {
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_REPLACE, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else if (OB_FAIL(table_service_->multi_replace(table_service_ctx_, batch_operation, result_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to multi_replace", K(ret), K(table_id));
    }
  }
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans");
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableBatchExecuteP::multi_update()
{
  int ret = OB_SUCCESS;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  const bool is_readonly = false;
  uint64_t &table_id = table_service_ctx_.param_table_id();
  table_service_ctx_.init_param(get_timeout_ts(), this->get_trans_desc(), &allocator_,
                                arg_.returning_affected_rows_,
                                arg_.entity_type_,
                                arg_.binlog_row_image_type_/*important*/);
  ObSEArray<int64_t, 1> part_ids;
  if (OB_FAIL(check_arg2())) {
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (OB_FAIL(get_partition_ids(table_id, part_ids))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (1 != part_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("should have one partition", K(ret), K(part_ids));
  } else if (FALSE_IT(table_service_ctx_.param_partition_id() = part_ids.at(0))) {
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_UPDATE, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else if (OB_FAIL(table_service_->multi_update(table_service_ctx_, batch_operation, result_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to multi_update", K(ret), K(table_id));
    }
  }
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans");
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableBatchExecuteP::batch_execute(bool is_readonly)
{
  int ret = OB_SUCCESS;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  uint64_t &table_id = table_service_ctx_.param_table_id();
  table_service_ctx_.init_param(get_timeout_ts(), this->get_trans_desc(), &allocator_,
                                arg_.returning_affected_rows_,
                                arg_.entity_type_,
                                arg_.binlog_row_image_type_,
                                arg_.returning_affected_entity_,
                                arg_.returning_rowkey_);
  ObSEArray<int64_t, 1> part_ids;
  const ObTableConsistencyLevel consistency_level = arg_.consistency_level_;
  if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (OB_FAIL(get_partition_ids(table_id, part_ids))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (1 != part_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("should have one partition", K(ret), K(part_ids));
  } else if (FALSE_IT(table_service_ctx_.param_partition_id() = part_ids.at(0))) {
  } else if (OB_FAIL(start_trans(is_readonly, (is_readonly ? sql::stmt::T_SELECT : sql::stmt::T_UPDATE),
                                 consistency_level, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else if (OB_FAIL(table_service_->batch_execute(table_service_ctx_, batch_operation, result_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to execute batch", K(ret), K(table_id));
    }
  }
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans");
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableBatchExecuteP::htable_mutate_row()
{
  int ret = OB_SUCCESS;
  const ObTableBatchOperation &batch_operation = arg_.batch_operation_;
  const bool is_readonly = false;
  uint64_t table_id = OB_INVALID_ID;
  ObSEArray<int64_t, 1> part_ids;
  int64_t now_ms = -ObHTableUtils::current_time_millis();
  if (OB_FAIL(check_arg2())) {
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (OB_FAIL(get_partition_ids(table_id, part_ids))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (1 != part_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("should have one partition", K(ret), K(part_ids));
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_DELETE, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else {
    int64_t N = batch_operation.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      // execute each mutation one by one
      const ObTableOperation &table_operation = batch_operation.at(i);
      ObTableBatchOperation batch_ops;
      if (OB_FAIL(batch_ops.add(table_operation))) {
        LOG_WARN("failed to add", K(ret));
        break;
      }
      switch(table_operation.type()) {
        case ObTableOperationType::INSERT_OR_UPDATE:
          {
            int64_t affected_rows = 0;
            ObHTablePutExecutor put_executor(allocator_,
                                             table_id,
                                             part_ids.at(0),
                                             get_timeout_ts(),
                                             this,
                                             table_service_,
                                             part_service_);
            ret = put_executor.htable_put(batch_ops, affected_rows, now_ms);
          }
          break;
        case ObTableOperationType::DEL:
          {
            int64_t affected_rows = 0;
            ObHTableDeleteExecutor delete_executor(allocator_,
                                                   table_id,
                                                   part_ids.at(0),
                                                   get_timeout_ts(),
                                                   this,
                                                   table_service_,
                                                   part_service_);
            ret = delete_executor.htable_delete(batch_ops, affected_rows);
          }
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported mutation type", K(ret), K(table_operation));
          break;
      }  // end switch
    }    // end for
  }
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans");
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}