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
#include "ob_table_execute_processor.h"
#include "ob_table_rpc_processor_util.h"
#include "observer/ob_service.h"
#include "storage/ob_partition_service.h"
#include "ob_table_end_trans_cb.h"
#include "sql/optimizer/ob_table_location.h"  // ObTableLocation
#include "lib/stat/ob_session_stat.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

int ObTableRpcProcessorUtil::negate_htable_timestamp(table::ObITableEntity &entity)
{
  int ret = OB_SUCCESS;
  // negative the value of T
  ObObj T_val;
  int64_t val = 0;
  if (3 == entity.get_rowkey_size()) {
    if (OB_FAIL(entity.get_rowkey_value(2, T_val))) {
      LOG_WARN("failed to get T from entity", K(ret), K(entity));
    } else if (OB_FAIL(T_val.get_int(val))) {
      LOG_WARN("invalid obj type for T", K(ret), K(T_val));
    } else {
      T_val.set_int(-val);
      if (OB_FAIL(entity.set_rowkey_value(2, T_val))) {
        LOG_WARN("failed to negate T value", K(ret));
      } else {
        LOG_DEBUG("[yzfdebug] nenative T value", K(ret), K(T_val));
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObTableApiExecuteP::ObTableApiExecuteP(const ObGlobalContext &gctx)
    :ObTableRpcProcessor(gctx),
     allocator_(ObModIds::TABLE_PROC),
     get_ctx_(allocator_),
     need_rollback_trans_(false),
     query_timeout_ts_(0)
{
}

int ObTableApiExecuteP::deserialize()
{
  // we should set entity before deserialize
  arg_.table_operation_.set_entity(request_entity_);
  result_.set_entity(result_entity_);
  int ret = ParentType::deserialize();
  if (OB_SUCC(ret) && ObTableEntityType::ET_HKV == arg_.entity_type_) {
    // @note modify the timestamp to be negative
    ret = ObTableRpcProcessorUtil::negate_htable_timestamp(request_entity_);
  }
  return ret;
}

int ObTableApiExecuteP::check_arg()
{
  int ret = OB_SUCCESS;
  if (!(arg_.consistency_level_ == ObTableConsistencyLevel::STRONG ||
      arg_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("some options not supported yet", K(ret),
             "consistency_level", arg_.consistency_level_,
             "operation_type", arg_.table_operation_.type());
  }
  return ret;
}

int ObTableApiExecuteP::check_arg2() const
{
  int ret = OB_SUCCESS;
  if (arg_.returning_rowkey_
      || arg_.returning_affected_entity_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("some options not supported yet", K(ret),
             "returning_rowkey", arg_.returning_rowkey_,
             "returning_affected_entity", arg_.returning_affected_entity_,
             "operation_type", arg_.table_operation_.type());
  }
  return ret;
}

int ObTableApiExecuteP::process()
{
  int ret = OB_SUCCESS;
  ret = ParentType::process();
  int tmp_ret = revert_get_ctx();
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("fail to revert get ctx", K(tmp_ret));
  }
  return ret;
}

int ObTableApiExecuteP::try_process()
{
  int ret = OB_SUCCESS;
  uint64_t table_id = arg_.table_id_;
  bool is_index_supported = true;
  const ObTableOperation &table_operation = arg_.table_operation_;
  if (ObTableOperationType::GET != table_operation.type()) {
    if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
      LOG_WARN("failed to get table id", K(ret));
    } else if (OB_FAIL(check_table_index_supported(table_id, is_index_supported))) {
      LOG_WARN("fail to check index supported", K(ret), K(table_id));
    }
  }
  
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY(!is_index_supported)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("index type is not supported by table api", K(ret));
  } else {
    switch (table_operation.type()) {
      case ObTableOperationType::INSERT:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_INSERT;
        ret = process_insert();
        break;
      case ObTableOperationType::GET:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_GET;
        ret = process_get();
        break;
      case ObTableOperationType::DEL:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_DELETE;
        ret = process_del();
        break;
      case ObTableOperationType::UPDATE:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_UPDATE;
        ret = process_update();
        break;
      case ObTableOperationType::INSERT_OR_UPDATE:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_INSERT_OR_UPDATE;
        ret = process_insert_or_update();
        break;
      case ObTableOperationType::REPLACE:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_REPLACE;
        ret = process_replace();
        break;
      case ObTableOperationType::INCREMENT:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_INCREMENT;
        ret = process_increment();
        break;
      case ObTableOperationType::APPEND:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_APPEND;
        // for both increment and append
        ret = process_increment();
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid table operation type", K(ret), K(table_operation));
        break;
    }
    audit_row_count_ = 1;
  }

#ifndef NDEBUG
  // debug mode
  LOG_INFO("[TABLE] execute operation", K(ret), K_(arg), K_(result), "timeout", rpc_pkt_->get_timeout(), K_(retry_count));
#else
  // release mode
  LOG_TRACE("[TABLE] execute operation", K(ret), K_(arg), K_(result),
            "timeout", rpc_pkt_->get_timeout(), "receive_ts", get_receive_timestamp(), K_(retry_count));
#endif
  return ret;
}

int ObTableApiExecuteP::revert_get_ctx()
{
  int ret = OB_SUCCESS;
  if (ObTableOperationType::GET == arg_.table_operation_.type()) {
    if (NULL != get_ctx_.scan_result_) {
      part_service_->revert_scan_iter(get_ctx_.scan_result_);
      get_ctx_.scan_result_ = NULL;
    }
    if (query_timeout_ts_ <= 0) {
      // for robust purpose
      query_timeout_ts_ = ObTimeUtility::current_time() + 1000000;
    }
    if (OB_FAIL(end_trans(need_rollback_trans_, req_, query_timeout_ts_))) {
      LOG_WARN("failed to end trans", K(ret));
    }
  }
  return ret;
}

void ObTableApiExecuteP::audit_on_finish()
{
  audit_record_.consistency_level_ = ObTableConsistencyLevel::STRONG == arg_.consistency_level_ ?
      ObConsistencyLevel::STRONG : ObConsistencyLevel::WEAK;
  audit_record_.return_rows_ = arg_.returning_affected_rows_ ? 1 : 0;
  audit_record_.table_scan_ = false;
  audit_record_.affected_rows_ = result_.get_affected_rows();
  audit_record_.try_cnt_ = retry_count_ + 1;
}

uint64_t ObTableApiExecuteP::get_request_checksum()
{
  uint64_t checksum = 0;
  checksum = ob_crc64(checksum, arg_.table_name_.ptr(), arg_.table_name_.length());
  checksum = ob_crc64(checksum, &arg_.consistency_level_, sizeof(arg_.consistency_level_));
  checksum = ob_crc64(checksum, &arg_.returning_rowkey_, sizeof(arg_.returning_rowkey_));
  checksum = ob_crc64(checksum, &arg_.returning_affected_entity_, sizeof(arg_.returning_affected_entity_));
  checksum = ob_crc64(checksum, &arg_.returning_affected_rows_, sizeof(arg_.returning_affected_rows_));
  checksum = ob_crc64(checksum, &arg_.binlog_row_image_type_, sizeof(arg_.binlog_row_image_type_));
  const uint64_t op_checksum = arg_.table_operation_.get_checksum();
  checksum = ob_crc64(checksum, &op_checksum, sizeof(op_checksum));
  return checksum;
}

int ObTableApiExecuteP::response(const int retcode)
{
  int ret = OB_SUCCESS;
  if (!need_retry_in_queue_ && !did_async_end_trans()) {
    if (OB_SUCC(ret) && ObTableEntityType::ET_HKV == arg_.entity_type_) {
      // @note modify the value of timestamp to be positive
      ret = ObTableRpcProcessorUtil::negate_htable_timestamp(result_entity_);
    }
    if (OB_SUCC(ret)) {
      ret = ObRpcProcessor::response(retcode);
    }
  }
  return ret;
}

void ObTableApiExecuteP::reset_ctx()
{
  (void)revert_get_ctx();
  get_ctx_.reset_dml();
  ObTableApiProcessorBase::reset_ctx();
  need_rollback_trans_ = false;
  need_retry_in_queue_ = false;
}

int ObTableApiExecuteP::get_partition_id(uint64_t table_id, const ObRowkey &rowkey, uint64_t &partition_id)
{
  int ret = OB_SUCCESS;
  partition_id = arg_.partition_id_;
  if (OB_INVALID_ID == partition_id) {
    ObSEArray<ObRowkey, 1> rowkeys;
    ObSEArray<int64_t, 1> part_ids;
    ObSEArray<sql::RowkeyArray, 1> rowkeys_per_part;
    if (OB_FAIL(rowkeys.push_back(rowkey))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(get_partition_by_rowkey(table_id, rowkeys, part_ids, rowkeys_per_part))) {
      LOG_WARN("failed to get partition", K(ret), K(table_id), K(rowkeys));
    } else if (1 != part_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("should have one partition", K(ret));
    } else {
      partition_id = part_ids.at(0);
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
// get
int ObTableApiExecuteP::process_get()
{
  int ret = OB_SUCCESS;
  need_rollback_trans_ = false;
  uint64_t &table_id = get_ctx_.param_table_id();
  get_ctx_.init_param(get_timeout_ts(), this->get_trans_desc(), &allocator_,
                      arg_.returning_affected_rows_,
                      arg_.entity_type_,
                      arg_.binlog_row_image_type_);
  const bool is_readonly = true;
  const ObTableConsistencyLevel consistency_level = arg_.consistency_level_;
  ObRowkey rowkey = const_cast<ObITableEntity&>(arg_.table_operation_.entity()).get_rowkey();
  ObSEArray<int64_t, 1> part_ids;
  if (OB_FAIL(check_arg2())) {
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret), K(table_id));
  } else if (OB_FAIL(get_partition_id(table_id, rowkey, get_ctx_.param_partition_id()))) {
    LOG_WARN("failed to get partition id", K(ret));
  } else if (OB_FAIL(part_ids.push_back(get_ctx_.param_partition_id()))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_SELECT, consistency_level, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start readonly transaction", K(ret));
  } else if (OB_FAIL(table_service_->execute_get(get_ctx_, arg_.table_operation_, result_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to execute get", K(ret), K(table_id));
    }
  } else {}
  // end trans in after_process()
  need_rollback_trans_ = (OB_SUCCESS != ret);
  return ret;
}

////////////////////////////////////////////////////////////////
// insert_or_update
ObTableAPITransCb *ObTableApiExecuteP::new_callback(rpc::ObRequest *req)
{
  ObTableExecuteEndTransCb *cb = OB_NEW(ObTableExecuteEndTransCb, ObModIds::TABLE_PROC, req, arg_.table_operation_.type());
  if (NULL != cb) {
    // @todo optimize to avoid this copy
    int ret = OB_SUCCESS;
    if (OB_FAIL(cb->assign_execute_result(result_))) {
      LOG_WARN("failed to assign result", K(ret));
      cb->~ObTableExecuteEndTransCb();
      cb = NULL;
    } else {
      LOG_DEBUG("yzfdebug copy result", K_(result));
    }
  }
  return cb;
}

int ObTableApiExecuteP::process_insert_or_update()
{
  int ret = OB_SUCCESS;
  uint64_t &table_id = get_ctx_.param_table_id();
  get_ctx_.init_param(get_timeout_ts(), this->get_trans_desc(), &allocator_,
                      arg_.returning_affected_rows_,
                      arg_.entity_type_,
                      arg_.binlog_row_image_type_);

  const bool is_readonly = false;
  ObRowkey rowkey = const_cast<ObITableEntity&>(arg_.table_operation_.entity()).get_rowkey();
  ObSEArray<int64_t, 1> part_ids;
  if (OB_FAIL(check_arg2())) {
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret), K(table_id));
  } else if (OB_FAIL(get_partition_id(table_id, rowkey, get_ctx_.param_partition_id()))) {
    LOG_WARN("failed to get partition id", K(ret));
  } else if (OB_FAIL(part_ids.push_back(get_ctx_.param_partition_id()))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_INSERT, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else if (OB_FAIL(table_service_->execute_insert_or_update(get_ctx_, arg_.table_operation_, result_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to insert_or_update", K(ret), K(table_id));
    }
  }
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans", K(ret));
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableApiExecuteP::process_del()
{
  int ret = OB_SUCCESS;
  uint64_t &table_id = get_ctx_.param_table_id();
  get_ctx_.init_param(get_timeout_ts(), this->get_trans_desc(), &allocator_,
                      arg_.returning_affected_rows_,
                      arg_.entity_type_,
                      arg_.binlog_row_image_type_);
  const bool is_readonly = false;
  ObRowkey rowkey = const_cast<ObITableEntity&>(arg_.table_operation_.entity()).get_rowkey();
  ObSEArray<int64_t, 1> part_ids;
  if (OB_FAIL(check_arg2())) {
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret), K(table_id));
  } else if (OB_FAIL(get_partition_id(table_id, rowkey, get_ctx_.param_partition_id()))) {
    LOG_WARN("failed to get partition id", K(ret));
  } else if (OB_FAIL(part_ids.push_back(get_ctx_.param_partition_id()))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_DELETE, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else if (OB_FAIL(table_service_->execute_delete(get_ctx_, arg_.table_operation_, result_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to delete", K(ret), K(table_id));
    }
  }
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans", K(ret));
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableApiExecuteP::process_replace()
{
  int ret = OB_SUCCESS;
  uint64_t &table_id = get_ctx_.param_table_id();
  get_ctx_.init_param(get_timeout_ts(), this->get_trans_desc(), &allocator_,
                      arg_.returning_affected_rows_,
                      arg_.entity_type_,
                      arg_.binlog_row_image_type_);
  const bool is_readonly = false;
  ObRowkey rowkey = const_cast<ObITableEntity&>(arg_.table_operation_.entity()).get_rowkey();
  ObSEArray<int64_t, 1> part_ids;
  if (OB_FAIL(check_arg2())) {
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret), K(table_id));
  } else if (OB_FAIL(get_partition_id(table_id, rowkey, get_ctx_.param_partition_id()))) {
    LOG_WARN("failed to get partition id", K(ret));
  } else if (OB_FAIL(part_ids.push_back(get_ctx_.param_partition_id()))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_REPLACE, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else if (OB_FAIL(table_service_->execute_replace(get_ctx_, arg_.table_operation_, result_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to replace", K(ret), K(table_id));
    }
  }
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans", K(ret));
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableApiExecuteP::process_insert()
{
  int ret = OB_SUCCESS;
  ObNewRowIterator *duplicate_row_iter = nullptr;
  uint64_t &table_id = get_ctx_.param_table_id();
  get_ctx_.init_param(get_timeout_ts(), this->get_trans_desc(), &allocator_,
                      arg_.returning_affected_rows_,
                      arg_.entity_type_,
                      arg_.binlog_row_image_type_);

  const bool is_readonly = false;
  ObRowkey rowkey = const_cast<ObITableEntity&>(arg_.table_operation_.entity()).get_rowkey();
  ObSEArray<int64_t, 1> part_ids;
  if (OB_FAIL(check_arg2())) {
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret), K(table_id));
  } else if (OB_FAIL(get_partition_id(table_id, rowkey, get_ctx_.param_partition_id()))) {
    LOG_WARN("failed to get partition id", K(ret));
  } else if (OB_FAIL(part_ids.push_back(get_ctx_.param_partition_id()))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_INSERT, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else if (OB_FAIL(table_service_->execute_insert(get_ctx_,
      arg_.table_operation_, result_, duplicate_row_iter))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to insert", K(ret), K(table_id));
    }
  }
  int tmp_ret = ret;
  const bool did_rollback = (OB_SUCCESS != ret || OB_SUCCESS != result_.get_errno());
  if (OB_FAIL(end_trans(did_rollback, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans", K(ret));
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableApiExecuteP::process_update()
{
  int ret = OB_SUCCESS;
  uint64_t &table_id = get_ctx_.param_table_id();
  get_ctx_.init_param(get_timeout_ts(), this->get_trans_desc(), &allocator_,
                      arg_.returning_affected_rows_,
                      arg_.entity_type_,
                      arg_.binlog_row_image_type_);
  const bool is_readonly = false;
  ObRowkey rowkey = const_cast<ObITableEntity&>(arg_.table_operation_.entity()).get_rowkey();
  ObSEArray<int64_t, 1> part_ids;
  if (OB_FAIL(check_arg2())) {
  } else if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret), K(table_id));
  } else if (OB_FAIL(get_partition_id(table_id, rowkey, get_ctx_.param_partition_id()))) {
    LOG_WARN("failed to get partition id", K(ret));
  } else if (OB_FAIL(part_ids.push_back(get_ctx_.param_partition_id()))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_UPDATE, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else if (OB_FAIL(table_service_->execute_update(get_ctx_, arg_.table_operation_, nullptr, result_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to update", K(ret), K(table_id));
    }
  }
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans", K(ret));
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableApiExecuteP::process_increment()
{
  int ret = OB_SUCCESS;
  uint64_t &table_id = get_ctx_.param_table_id();
  get_ctx_.init_param(get_timeout_ts(), this->get_trans_desc(), &allocator_,
                      arg_.returning_affected_rows_,
                      arg_.entity_type_,
                      arg_.binlog_row_image_type_,
                      arg_.returning_affected_entity_,
                      arg_.returning_rowkey_);
  const bool is_readonly = false;
  ObRowkey rowkey = const_cast<ObITableEntity&>(arg_.table_operation_.entity()).get_rowkey();
  ObSEArray<int64_t, 1> part_ids;
  if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret), K(table_id));
  } else if (OB_FAIL(get_partition_id(table_id, rowkey, get_ctx_.param_partition_id()))) {
    LOG_WARN("failed to get partition id", K(ret));
  } else if (OB_FAIL(part_ids.push_back(get_ctx_.param_partition_id()))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(start_trans(is_readonly, sql::stmt::T_UPDATE, table_id, part_ids, get_timeout_ts()))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else if (OB_FAIL(table_service_->execute_increment(get_ctx_, arg_.table_operation_, result_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to update", K(ret), K(table_id));
    }
  }
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, get_timeout_ts()))) {
    LOG_WARN("failed to end trans", K(ret));
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}
