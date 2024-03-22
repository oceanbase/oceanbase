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
    : ObTableRpcProcessor(gctx),
      default_entity_factory_("TableBatchEntFac", MTL_ID()),
      allocator_("TbBatExeP", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      batch_ctx_(allocator_)
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

void ObTableBatchExecuteP::init_batch_ctx()
{
  batch_ctx_.stat_event_type_ = &stat_event_type_;
  batch_ctx_.trans_param_ = &trans_param_;
  batch_ctx_.ops_ = &arg_.batch_operation_.get_table_operations();
  batch_ctx_.results_ = &result_;
  batch_ctx_.table_id_ = arg_.table_id_;
  batch_ctx_.tablet_id_ = arg_.tablet_id_;
  batch_ctx_.is_atomic_ = arg_.batch_operation_as_atomic_;
  batch_ctx_.is_readonly_ = arg_.batch_operation_.is_readonly();
  batch_ctx_.is_same_type_ = arg_.batch_operation_.is_same_type();
  batch_ctx_.is_same_properties_names_ = arg_.batch_operation_.is_same_properties_names();
  batch_ctx_.use_put_ = arg_.use_put();
  batch_ctx_.returning_affected_entity_ = arg_.returning_affected_entity();
  batch_ctx_.returning_rowkey_ = arg_.returning_rowkey();
  batch_ctx_.return_one_result_ = arg_.return_one_result();
  batch_ctx_.entity_type_ = arg_.entity_type_;
  batch_ctx_.consistency_level_ = arg_.consistency_level_;
  batch_ctx_.entity_factory_ = &default_entity_factory_;
  batch_ctx_.result_entity_ = &result_entity_;
  batch_ctx_.credential_ = &credential_;
}

int ObTableBatchExecuteP::before_process()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ParentType::before_process())) {
    LOG_WARN("before process failed", K(ret));
  } else {
    init_batch_ctx();
  }

  return ret;
}

int ObTableBatchExecuteP::check_arg()
{
  int ret = OB_SUCCESS;
  if (arg_.return_one_result()) {
    if (arg_.returning_rowkey() || arg_.returning_affected_entity()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "can not return one result, when return rowkey or return affected entity");
      LOG_WARN("can not return one result, when return rowkey or return affected entity.", K(ret));
    }
  }
  if (!(arg_.consistency_level_ == ObTableConsistencyLevel::STRONG ||
      arg_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid consistency level");
    LOG_WARN("some options not supported yet", K(ret),
             "consistency_level", arg_.consistency_level_);
  }
  return ret;
}

void ObTableBatchExecuteP::audit_on_finish()
{
  audit_record_.consistency_level_ = ObTableConsistencyLevel::STRONG == arg_.consistency_level_ ?
      ObConsistencyLevel::STRONG : ObConsistencyLevel::WEAK;
  audit_record_.table_scan_ = false;
  audit_record_.try_cnt_ = retry_count_ + 1;
  audit_record_.return_rows_ = 0;
  audit_record_.affected_rows_ = 0;
  for (int i = 0; i < result_.count(); i++) {
     audit_record_.return_rows_ += result_.at(i).get_return_rows();
     audit_record_.affected_rows_ += result_.at(i).get_affected_rows();
  }
}

uint64_t ObTableBatchExecuteP::get_request_checksum()
{
  uint64_t checksum = 0;
  checksum = ob_crc64(checksum, arg_.table_name_.ptr(), arg_.table_name_.length());
  const uint64_t op_checksum = arg_.batch_operation_.get_checksum();
  checksum = ob_crc64(checksum, &op_checksum, sizeof(op_checksum));
  checksum = ob_crc64(checksum, &arg_.consistency_level_, sizeof(arg_.consistency_level_));
  checksum = ob_crc64(checksum, &arg_.option_flag_, sizeof(arg_.option_flag_));
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
  result_.reset();
  ObTableApiProcessorBase::reset_ctx();
  batch_ctx_.tb_ctx_.reset();
  result_entity_.reset();
}

int ObTableBatchExecuteP::start_trans()
{
  int ret = OB_SUCCESS;

  if (batch_ctx_.is_readonly_ && batch_ctx_.is_same_properties_names_) { // multi get
    if (OB_FAIL(trans_param_.init(batch_ctx_.consistency_level_,
                                  batch_ctx_.tb_ctx_.get_ls_id(),
                                  get_timeout_ts(),
                                  batch_ctx_.tb_ctx_.need_dist_das()))) {
      LOG_WARN("fail to init trans param", K(ret));
    } else if (OB_FAIL(ObTableTransUtils::init_read_trans(trans_param_))) {
      LOG_WARN("fail to init read trans", K(ret));
    }
  } else { // other batch operation
    if (OB_FAIL(ObTableApiProcessorBase::start_trans(batch_ctx_.is_readonly_,
                                                     batch_ctx_.consistency_level_,
                                                     batch_ctx_.tb_ctx_.get_ls_id(),
                                                     get_timeout_ts(),
                                                     batch_ctx_.tb_ctx_.need_dist_das()))) {
      LOG_WARN("fail to start trans", K(ret));
    }
  }

  return ret;
}

int ObTableBatchExecuteP::end_trans(bool is_rollback)
{
  int ret = OB_SUCCESS;

  if (batch_ctx_.is_readonly_ && batch_ctx_.is_same_properties_names_) { // multi get
    ObTableTransUtils::release_read_trans(trans_param_.trans_desc_);
  } else { // other batch operation
    ObTableBatchExecuteCreateCbFunctor functor;
    if (OB_FAIL(functor.init(req_, &result_, arg_.batch_operation_.at(0).type()))) {
      LOG_WARN("fail to init create batch execute callback functor", K(ret));
    } else if (OB_FAIL(ObTableApiProcessorBase::end_trans(is_rollback, req_, &functor))) {
      LOG_WARN("fail to end trans", K(ret), K(is_rollback));
    }
  }

  return ret;
}

int ObTableBatchExecuteP::try_process()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_schema_info(arg_.table_name_))) {
    LOG_WARN("fail to init schema info", K(ret), K(arg_.table_name_));
  } else if (OB_FAIL(init_single_op_tb_ctx(batch_ctx_.tb_ctx_, batch_ctx_.ops_->at(0)))) {
    LOG_WARN("fail to init table ctx", K(ret));
  } else if (OB_FAIL(start_trans())) {
    LOG_WARN("fail to start trans", K(ret));
  } else if (OB_FAIL(batch_ctx_.tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret));
  } else if (OB_FAIL(ObTableBatchService::execute(batch_ctx_))) {
    LOG_WARN("fail to execute batch operation", K(ret));
  }

  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret))) {
    LOG_WARN("fail to end trans", K(ret));
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;

  // record events
  audit_row_count_ = arg_.batch_operation_.count();

#ifndef NDEBUG
  // debug mode
  LOG_INFO("[TABLE] execute batch operation", K(ret), K_(result), K_(retry_count));
#else
  // release mode
  LOG_TRACE("[TABLE] execute batch operation", K(ret), K_(result), K_(retry_count),
            "receive_ts", get_receive_timestamp());
#endif
  return ret;
}

int ObTableBatchExecuteP::init_single_op_tb_ctx(table::ObTableCtx &ctx,
                                                const ObTableOperation &table_operation)
{
  int ret = OB_SUCCESS;
  ctx.set_entity(&table_operation.entity());
  ctx.set_entity_type(arg_.entity_type_);
  ctx.set_operation_type(table_operation.type());
  ctx.set_schema_cache_guard(&schema_cache_guard_);
  ctx.set_schema_guard(&schema_guard_);
  ctx.set_simple_table_schema(simple_table_schema_);
  ctx.set_sess_guard(&sess_guard_);

  if (ctx.is_init()) {
    LOG_INFO("tb ctx has been inited", K(ctx));
  } else if (OB_FAIL(ctx.init_common(credential_, arg_.tablet_id_, get_timeout_ts()))) {
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
      case ObTableOperationType::PUT: {
        if (OB_FAIL(ctx.init_put())) {
          LOG_WARN("fail to init put ctx", K(ret), K(ctx));
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
        if (OB_FAIL(ctx.init_insert_up(arg_.use_put()))) {
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
        if (OB_FAIL(ctx.init_append(arg_.returning_affected_entity(),
                                    arg_.returning_rowkey()))) {
          LOG_WARN("fail to init append ctx", K(ret), K(ctx));
        }
        break;
      }
      case ObTableOperationType::INCREMENT: {
        if (OB_FAIL(ctx.init_increment(arg_.returning_affected_entity(),
                                       arg_.returning_rowkey()))) {
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
