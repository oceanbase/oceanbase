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
#include "observer/ob_service.h"
#include "ob_table_end_trans_cb.h"
#include "sql/optimizer/ob_table_location.h"  // ObTableLocation
#include "lib/stat/ob_session_stat.h"
#include "storage/tx_storage/ob_access_service.h"
#include "ob_table_scan_executor.h"
#include "ob_table_cg_service.h"
#include "observer/ob_req_time_service.h"
#include "ob_table_move_response.h"

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
      LOG_WARN("fail to get T from entity", K(ret), K(entity));
    } else if (OB_FAIL(T_val.get_int(val))) {
      LOG_WARN("invalid obj type for T", K(ret), K(T_val));
    } else {
      T_val.set_int(-val);
      if (OB_FAIL(entity.set_rowkey_value(2, T_val))) {
        LOG_WARN("fail to negate T value", K(ret));
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
     allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
     tb_ctx_(allocator_),
     default_entity_factory_("TableExecuteEncFac", MTL_ID()),
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
  ObTableOperationType::Type op_type = arg_.table_operation_.type();

  if (ObTableOperationType::Type::APPEND != op_type &&
      ObTableOperationType::Type::INCREMENT != op_type) {
    if (arg_.returning_rowkey_ || arg_.returning_affected_entity_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("some options not supported yet", K(ret),
              "returning_rowkey", arg_.returning_rowkey_,
              "returning_affected_entity", arg_.returning_affected_entity_,
              "operation_type", op_type);
    }
  }

  return ret;
}

int ObTableApiExecuteP::init_tb_ctx()
{
  int ret = OB_SUCCESS;
  ObExprFrameInfo *expr_frame_info = nullptr;
  ObTableOperationType::Type op_type = arg_.table_operation_.type();
  tb_ctx_.set_entity(&arg_.table_operation_.entity());
  tb_ctx_.set_operation_type(op_type);

  if (tb_ctx_.is_init()) {
    LOG_INFO("tb ctx has been inited", K_(tb_ctx));
  } else if (OB_FAIL(tb_ctx_.init_common(credential_,
                                         arg_.tablet_id_,
                                         arg_.table_name_,
                                         get_timeout_ts()))) {
    LOG_WARN("fail to init table ctx common part", K(ret), K(arg_.table_name_));
  } else {
    switch(op_type) {
      case ObTableOperationType::INSERT: {
        if (tb_ctx_.is_ttl_table()) {
          if (OB_FAIL(tb_ctx_.init_insert_up())) {
            LOG_WARN("fail to init insert up ctx", K(ret), K(tb_ctx_));
          }
        } else {
          if (OB_FAIL(tb_ctx_.init_insert())) {
            LOG_WARN("fail to init insert ctx", K(ret), K(tb_ctx_));
          }
        }
        break;
      }
      case ObTableOperationType::UPDATE: {
        if (OB_FAIL(tb_ctx_.init_update())) {
          LOG_WARN("fail to init update ctx", K(ret), K(tb_ctx_));
        }
        break;
      }
      case ObTableOperationType::DEL: {
        if (OB_FAIL(tb_ctx_.init_delete())) {
          LOG_WARN("fail to init delete ctx", K(ret), K(tb_ctx_));
        }
        break;
      }
      case ObTableOperationType::REPLACE: {
        if (OB_FAIL(tb_ctx_.init_replace())) {
          LOG_WARN("fail to init replace ctx", K(ret), K(tb_ctx_));
        }
        break;
      }
      case ObTableOperationType::INSERT_OR_UPDATE: {
        if (OB_FAIL(tb_ctx_.init_insert_up())) {
          LOG_WARN("fail to init insert up ctx", K(ret), K(tb_ctx_));
        }
        break;
      }
      case ObTableOperationType::APPEND: {
        if (OB_FAIL(tb_ctx_.init_append(arg_.returning_affected_entity_,
                                        arg_.returning_rowkey_))) {
          LOG_WARN("fail to init append ctx", K(ret), K(tb_ctx_));
        }
        break;
      }
      case ObTableOperationType::INCREMENT: {
        if (OB_FAIL(tb_ctx_.init_increment(arg_.returning_affected_entity_,
                                           arg_.returning_rowkey_))) {
          LOG_WARN("fail to init increment ctx", K(ret), K(tb_ctx_));
        }
        break;
      }
      case ObTableOperationType::GET: {
        if (OB_FAIL(tb_ctx_.init_get())) {
          LOG_WARN("fail to init get ctx", K(ret), K(tb_ctx_));
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid operation type", K(ret), K(op_type));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tb_ctx_.init_exec_ctx())) {
      LOG_WARN("fail to init exec ctx", K(ret), K(tb_ctx_));
    }
  }

  return ret;
}

int ObTableApiExecuteP::process()
{
  int ret = OB_SUCCESS;
  ret = ParentType::process();
  return ret;
}

int ObTableApiExecuteP::try_process()
{
  int ret = OB_SUCCESS;
  uint64_t table_id = arg_.table_id_;
  bool is_index_supported = true;
  const ObTableOperation &table_operation = arg_.table_operation_;
  if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (FALSE_IT(table_id_ = arg_.table_id_)) {
  } else if (FALSE_IT(tablet_id_ = arg_.tablet_id_)) {
  } else if (ObTableOperationType::GET != table_operation.type()) {
    if (OB_FAIL(check_table_index_supported(table_id, is_index_supported))) {
      LOG_WARN("fail to check index supported", K(ret), K(table_id));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY(!is_index_supported)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("index type is not supported by table api", K(ret));
  } else if (OB_FAIL(check_arg2())) {
    LOG_WARN("fail to check arg", K(ret));
  } else if (OB_FAIL(init_tb_ctx())) {
    LOG_WARN("fail to init tb ctx", K(ret));
  } else {
    switch (table_operation.type()) {
      case ObTableOperationType::INSERT:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_INSERT;
        if (tb_ctx_.is_ttl_table()) {
          ret = process_dml_op<TABLE_API_EXEC_TTL>();
        } else {
          ret = process_dml_op<TABLE_API_EXEC_INSERT>();
        }
        break;
      case ObTableOperationType::GET:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_GET;
        ret = process_get();
        break;
      case ObTableOperationType::DEL:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_DELETE;
        ret = process_dml_op<TABLE_API_EXEC_DELETE>();
        break;
      case ObTableOperationType::UPDATE:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_UPDATE;
        ret = process_dml_op<TABLE_API_EXEC_UPDATE>();
        break;
      case ObTableOperationType::INSERT_OR_UPDATE:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_INSERT_OR_UPDATE;
        if (tb_ctx_.is_ttl_table()) {
          ret = process_dml_op<TABLE_API_EXEC_TTL>();
        } else {
          ret = process_dml_op<TABLE_API_EXEC_INSERT_UP>();
        }
        break;
      case ObTableOperationType::REPLACE:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_REPLACE;
        ret = process_dml_op<TABLE_API_EXEC_REPLACE>();
        break;
      case ObTableOperationType::INCREMENT:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_INCREMENT;
        if (tb_ctx_.is_ttl_table()) {
          ret = process_dml_op<TABLE_API_EXEC_TTL>();
        } else {
          ret = process_dml_op<TABLE_API_EXEC_INSERT_UP>();
        }
        break;
      case ObTableOperationType::APPEND:
        stat_event_type_ = ObTableProccessType::TABLE_API_SINGLE_APPEND;
        if (tb_ctx_.is_ttl_table()) {
          ret = process_dml_op<TABLE_API_EXEC_TTL>();
        } else {
          ret = process_dml_op<TABLE_API_EXEC_INSERT_UP>();
        }
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid table operation type", K(ret), K(table_operation));
        break;
    }
    audit_row_count_ = 1;
  }

  if (OB_FAIL(ret)) {
    // init_tb_ctx will return some replaceable error code
    result_.set_errno(ret);
    table::ObTableApiUtil::replace_ret_code(ret);
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
  if (!need_retry_in_queue_ && !had_do_response()) {
    if (OB_SUCC(ret) && ObTableEntityType::ET_HKV == arg_.entity_type_) {
      // @note modify the value of timestamp to be positive
      ret = ObTableRpcProcessorUtil::negate_htable_timestamp(result_entity_);
    }

    // return the package even if negate_htable_timestamp fails
    const ObRpcPacket *rpc_pkt = &reinterpret_cast<const ObRpcPacket&>(req_->get_packet());
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

void ObTableApiExecuteP::reset_ctx()
{
  ObTableApiProcessorBase::reset_ctx();
  need_rollback_trans_ = false;
  need_retry_in_queue_ = false;
}

int ObTableApiExecuteP::get_tablet_id(uint64_t table_id, const ObRowkey &rowkey, ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_id = arg_.tablet_id_;
  if (!tablet_id.is_valid()) {
    ObSEArray<ObRowkey, 1> rowkeys;
    ObSEArray<ObTabletID, 1> tablet_ids;
    if (OB_FAIL(rowkeys.push_back(rowkey))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(get_tablet_by_rowkey(table_id, rowkeys, tablet_ids))) {
      LOG_WARN("fail to get partition", K(ret), K(table_id), K(rowkeys));
    } else if (1 != tablet_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("should have one tablet", K(ret));
    } else {
      tablet_id = tablet_ids.at(0);
    }
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("partitioned table not supported", K(ret), K(table_id));
  }
  return ret;
}

int ObTableApiExecuteP::process_get()
{
  int ret = OB_SUCCESS;
  ObNewRow *row = nullptr;
  if (OB_FAIL(check_arg2())) {
    LOG_WARN("fail to check arg", K(ret));
  } else if (OB_FAIL(init_read_trans(arg_.consistency_level_,
                                     tb_ctx_.get_ls_id(),
                                     tb_ctx_.get_timeout_ts()))) {
    LOG_WARN("fail to init wead read trans", K(ret), K(tb_ctx_));
  } else if (OB_FAIL(tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret), K(tb_ctx_));
  } else if (FALSE_IT(tb_ctx_.set_read_latest(false))) {
    // do nothing
  } else if (OB_FAIL(ObTableOpWrapper::process_get(tb_ctx_, row))) {
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get row", K(ret));
    }
  } else {
    // fill result entity
    ObITableEntity *result_entity = nullptr;
    const ObTableSchema *table_schema = tb_ctx_.get_table_schema();
    if (OB_FAIL(result_.get_entity(result_entity))) {
      LOG_WARN("fail to get result entity", K(ret));
    } else if (OB_FAIL(ObTableApiUtil::construct_entity_from_row(allocator_,
                                                                 row,
                                                                 table_schema,
                                                                 tb_ctx_.get_query_col_names(),
                                                                 result_entity))) {
      LOG_WARN("fail to fill result entity", K(ret));
    }
  }

  release_read_trans();
  result_.set_errno(ret);
  ObTableApiUtil::replace_ret_code(ret);
  result_.set_type(arg_.table_operation_.type());

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
      LOG_WARN("fail to assign result", K(ret));
      cb->~ObTableExecuteEndTransCb();
      cb = NULL;
    } else {
      LOG_DEBUG("yzfdebug copy result", K_(result));
    }
  }
  return cb;
}

int ObTableApiExecuteP::before_response(int error_code)
{
  // NOTE: when check_timeout failed, the result.entity_ is null, and serialize result cause coredump
  if (!had_do_response() && OB_ISNULL(result_.get_entity())) {
    result_.set_entity(result_entity_);
  }
  return ObTableRpcProcessor::before_response(error_code);
}
