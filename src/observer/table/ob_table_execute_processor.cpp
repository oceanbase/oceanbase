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
#include "ob_table_move_response.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

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
     allocator_("TbExeP", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
     tb_ctx_(allocator_),
     is_group_commit_(false),
     is_group_trigger_(false),
     group_single_op_(nullptr)
{
}

int ObTableApiExecuteP::deserialize()
{
  int ret = OB_SUCCESS;

  arg_.table_operation_.set_entity(request_entity_); // deserialize to request_entity_
  result_.set_entity(result_entity_);
  if (OB_FAIL(ParentType::deserialize())) {
    LOG_WARN("fail to deserialize parent type", K(ret));
  } else if (ObTableEntityType::ET_HKV == arg_.entity_type_
      && OB_FAIL(ObTableRpcProcessorUtil::negate_htable_timestamp(request_entity_))) {
    LOG_WARN("fail to  modify the timestamp to be negative", K(ret));
  }

  return ret;
}

int ObTableApiExecuteP::check_arg()
{
  int ret = OB_SUCCESS;
  if (!(arg_.consistency_level_ == ObTableConsistencyLevel::STRONG ||
      arg_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "consistency level");
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
    if (arg_.returning_rowkey() || arg_.returning_affected_entity()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "returning rowkey or affected entity");
      LOG_WARN("some options not supported yet", K(ret),
              "returning_rowkey", arg_.returning_rowkey(),
              "returning_affected_entity", arg_.returning_affected_entity(),
              "operation_type", op_type);
    }
  }

  return ret;
}

int ObTableApiExecuteP::init_tb_ctx()
{
  int ret = OB_SUCCESS;
  ObTableOperationType::Type op_type = arg_.table_operation_.type();
  tb_ctx_.set_entity(&arg_.table_operation_.entity());
  tb_ctx_.set_operation_type(op_type);
  tb_ctx_.set_entity_type(arg_.entity_type_);
  tb_ctx_.set_schema_cache_guard(&schema_cache_guard_);
  tb_ctx_.set_schema_guard(&schema_guard_);
  tb_ctx_.set_simple_table_schema(simple_table_schema_);
  tb_ctx_.set_sess_guard(&sess_guard_);
  tb_ctx_.set_audit_ctx(&audit_ctx_);
  if (tb_ctx_.is_init()) {
    LOG_INFO("tb ctx has been inited", K_(tb_ctx));
  } else if (OB_FAIL(tb_ctx_.init_common(credential_,
                                         arg_.tablet_id_,
                                         get_timeout_ts()))) {
    LOG_WARN("fail to init table ctx common part", K(ret), K(arg_.table_name_));
  } else {
    switch(op_type) {
      case ObTableOperationType::PUT: {
        if (OB_FAIL(tb_ctx_.init_put())) {
          LOG_WARN("fail to init put ctx", K(ret), K(tb_ctx_));
        }
        break;
      }
      case ObTableOperationType::INSERT: {
        if (tb_ctx_.is_ttl_table()) {
          if (OB_FAIL(tb_ctx_.init_insert_up(arg_.use_put()))) {
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
        if (OB_FAIL(tb_ctx_.init_insert_up(arg_.use_put()))) {
          LOG_WARN("fail to init insert up ctx", K(ret), K(tb_ctx_));
        }
        break;
      }
      case ObTableOperationType::APPEND: {
        if (OB_FAIL(tb_ctx_.init_append(arg_.returning_affected_entity(),
                                        arg_.returning_rowkey()))) {
          LOG_WARN("fail to init append ctx", K(ret), K(tb_ctx_));
        }
        break;
      }
      case ObTableOperationType::INCREMENT: {
        if (OB_FAIL(tb_ctx_.init_increment(arg_.returning_affected_entity(),
                                           arg_.returning_rowkey()))) {
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

bool ObTableApiExecuteP::is_group_commit_enable(ObTableOperationType::Type op_type) const
{
  bool bret = false;
  if (arg_.returning_rowkey() || arg_.returning_affected_entity()) {
    bret = false;
  } else {
    bret = ObTableGroupUtils::is_group_commit_enable(op_type);
  }
  return bret;
}

int ObTableApiExecuteP::before_process()
{
  int ret = OB_SUCCESS;
  ObTableOperationType::Type op_type = arg_.table_operation_.type();

  if (op_type == ObTableOperationType::Type::TRIGGER) {
    is_group_trigger_ = true;
    audit_ctx_.need_audit_ = false; // no need audit when packet is group commit trigger packet
  } else {
    ObITableOp *op = nullptr;
    if (is_group_commit_enable(op_type)) {
      if (OB_FAIL(TABLEAPI_GROUP_COMMIT_MGR->alloc_op(ObTableGroupType::TYPE_TABLE_GROUP, op))) {
        LOG_WARN("fail to alloc group single op", K(ret));
      } else {
        group_single_op_ = static_cast<ObTableOp *>(op);
        group_single_op_->op_ = arg_.table_operation_; // shaddow copy
        group_single_op_->request_entity_ = request_entity_; // shaddow copy, its member memory is from rpc buffer
        group_single_op_->op_.set_entity(group_single_op_->request_entity_);
        group_single_op_->req_ = req_;
        group_single_op_->timeout_ts_ = get_timeout_ts();
        group_single_op_->timeout_ = get_timeout();
        group_single_op_->result_.set_errno(OB_SUCCESS);
        group_single_op_->result_.set_type(arg_.table_operation_.type());
        group_single_op_->result_.set_entity(&group_single_op_->result_entity_);
        is_group_commit_ = true;
      }
    }
  }

  if (OB_SUCC(ret)) {
    ret = ParentType::before_process();
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(group_single_op_)) {
    TABLEAPI_GROUP_COMMIT_MGR->free_op(group_single_op_);
    group_single_op_ = nullptr;
  }

  return ret;
}

int ObTableApiExecuteP::process()
{
  int ret = OB_SUCCESS;
  ret = ParentType::process();
  return ret;
}

int ObTableApiExecuteP::init_group_ctx(ObTableGroupCtx &ctx, ObLSID ls_id)
{
  int ret = OB_SUCCESS;
  ctx.group_type_ = ObTableGroupType::TYPE_TABLE_GROUP;
  ctx.type_ = arg_.table_operation_.type();
  ctx.entity_type_ = arg_.entity_type_;
  ctx.credential_ = credential_;
  ctx.timeout_ts_ = get_timeout_ts();
  ctx.trans_param_ = &trans_param_;
  ctx.schema_cache_guard_ = &schema_cache_guard_;
  ctx.schema_guard_ = &schema_guard_;
  ctx.simple_schema_ = simple_table_schema_;
  ctx.sess_guard_ = &sess_guard_;
  ctx.retry_count_ = retry_count_;
  ctx.user_client_addr_ = user_client_addr_;
  ctx.audit_ctx_.exec_timestamp_ = audit_ctx_.exec_timestamp_;
  ctx.table_id_ = simple_table_schema_->get_table_id();
  ctx.schema_version_ = simple_table_schema_->get_schema_version();
  ctx.ls_id_ = ls_id;
  return ret;
}

int ObTableApiExecuteP::process_group_commit()
{
  int ret = OB_SUCCESS;
  const ObTableOperation &op = arg_.table_operation_;
  ObSchemaGetterGuard schema_guard;
  uint64_t tenant_id = credential_.tenant_id_;
  int64_t schema_version = -1;
  bool is_cache_hit = false;
  ObLSID ls_id(ObLSID::INVALID_LS_ID);

  if (!tablet_id_.is_valid()) {
    if (!simple_table_schema_->is_partitioned_table()) {
      tablet_id_ = simple_table_schema_->get_tablet_id();
    } else {
      // maybe drop a non-partitioned table and create a
      // partitioned table with same name
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("partitioned table should pass right tablet id from client", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(group_single_op_)) {
    group_single_op_->tablet_id_ = tablet_id_;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(GCTX.location_service_->get(tenant_id,
                                          tablet_id_,
                                          0, /* expire_renew_time */
                                          is_cache_hit,
                                          ls_id))) {
    LOG_WARN("fail to get ls id", K(ret), K(tenant_id), K_(tablet_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(TABLE_SCHEMA, tenant_id, table_id_, schema_version))) {
    LOG_WARN("fail to get schema version", K(ret), K(tenant_id), K_(table_id));
  } else {
    ObTableGroupKey key(ls_id, table_id_, schema_version, op.type());
    ObTableGroupCtx ctx(allocator_);
    bool is_insup_use_put = false;
    int64_t binlog_row_image_type = TABLEAPI_SESS_POOL_MGR->get_binlog_row_image();
    ctx.key_ = &key;
    if (arg_.table_operation_.type() == ObTableOperationType::Type::INSERT_OR_UPDATE) {
      if (OB_FAIL(ObTableCtx::check_insert_up_can_use_put(schema_cache_guard_,
                                                                &arg_.table_operation_.entity(),
                                                                arg_.use_put(),
                                                                arg_.entity_type_ == ObTableEntityType::ET_HKV,
                                                                binlog_row_image_type == ObBinlogRowImage::FULL,
                                                                is_insup_use_put))) {
        LOG_WARN("fail to check insert up can use put", K(ret), K(arg_.use_put()),
                K(arg_.entity_type_), K(binlog_row_image_type));
      } else if (is_insup_use_put) {
        group_single_op_->is_insup_use_put_ = is_insup_use_put;
        key.is_insup_use_put_ = is_insup_use_put;
        key.op_type_ = ObTableOperationType::Type::PUT;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_group_ctx(ctx, ls_id))) {
      LOG_WARN("fail to init group ctx", K(ret), K(ctx));
    } else if (OB_FAIL(ObTableGroupService::process(ctx, group_single_op_))) {
      LOG_WARN("fail to process group commit", K(ret)); // can not K(ctx) or KPC_(group_single_op), cause req may have been free
    } else {
      this->set_req_has_wokenup(); // do not response packet
    }
  }

  return ret;
}

ObTableProccessType ObTableApiExecuteP::get_stat_process_type()
{
  ObTableProccessType event_type = ObTableProccessType::TABLE_API_PROCESS_TYPE_INVALID;
  const ObTableOperation &table_operation = arg_.table_operation_;
  switch (table_operation.type()) {
    case ObTableOperationType::INSERT:
      event_type = ObTableProccessType::TABLE_API_SINGLE_INSERT;
      break;
    case ObTableOperationType::GET:
      event_type = ObTableProccessType::TABLE_API_SINGLE_GET;
      break;
    case ObTableOperationType::DEL:
      event_type = ObTableProccessType::TABLE_API_SINGLE_DELETE;
      break;
    case ObTableOperationType::UPDATE:
      event_type = ObTableProccessType::TABLE_API_SINGLE_UPDATE;
      break;
    case ObTableOperationType::INSERT_OR_UPDATE:
      event_type = ObTableProccessType::TABLE_API_SINGLE_INSERT_OR_UPDATE;
      break;
    case ObTableOperationType::PUT:
      event_type = ObTableProccessType::TABLE_API_SINGLE_PUT;
      break;
    case ObTableOperationType::REPLACE:
      event_type = ObTableProccessType::TABLE_API_SINGLE_REPLACE;
      break;
    case ObTableOperationType::INCREMENT:
      event_type = ObTableProccessType::TABLE_API_SINGLE_INCREMENT;
      break;
    case ObTableOperationType::APPEND:
      event_type = ObTableProccessType::TABLE_API_SINGLE_APPEND;
      break;
    case ObTableOperationType::TRIGGER:
      event_type = ObTableProccessType::TABLE_API_GROUP_TRIGGER;
      break;
    default:
      event_type = ObTableProccessType::TABLE_API_PROCESS_TYPE_INVALID;
      break;
  }
  return event_type;
}

int ObTableApiExecuteP::try_process()
{
  int ret = OB_SUCCESS;
  const ObTableOperation &table_operation = arg_.table_operation_;
  stat_process_type_ = get_stat_process_type();
  table_id_ = arg_.table_id_; // init move response need
  tablet_id_ = arg_.tablet_id_;
  OB_TABLE_START_AUDIT(credential_,
                       sess_guard_,
                       arg_.table_name_,
                       &audit_ctx_,
                       table_operation);

  if (is_group_trigger_) {
    if (OB_FAIL(ObTableGroupService::process_trigger())) {
      LOG_WARN("fail to process group commit trigger", K(ret));
    }
    result_.set_err(ret);
  } else if (OB_FAIL(init_schema_info(arg_.table_name_, table_id_))) {
    LOG_WARN("fail to init schema guard", K(ret), K(arg_.table_name_));
  } else if (OB_FAIL(check_arg2())) {
    LOG_WARN("fail to check arg", K(ret));
  } else if (is_group_commit_) {
    if (OB_FAIL(process_group_commit())) {
      LOG_WARN("fail to process group commit", K(ret));
    }
  } else if (OB_FAIL(init_tb_ctx())) {
    LOG_WARN("fail to init tb ctx", K(ret));
  } else {
    switch (table_operation.type()) {
      case ObTableOperationType::INSERT:
        ret = process_insert();
        break;
      case ObTableOperationType::GET:
        ret = process_get();
        break;
      case ObTableOperationType::DEL:
        ret = process_dml_op<TABLE_API_EXEC_DELETE>();
        break;
      case ObTableOperationType::UPDATE:
        ret = process_dml_op<TABLE_API_EXEC_UPDATE>();
        break;
      case ObTableOperationType::INSERT_OR_UPDATE:
        ret = process_insert_up();
        break;
      case ObTableOperationType::PUT:
        ret = process_put();
        break;
      case ObTableOperationType::REPLACE:
        ret = process_dml_op<TABLE_API_EXEC_REPLACE>();
        break;
      case ObTableOperationType::INCREMENT:
      case ObTableOperationType::APPEND:
        ret = process_incr_or_append_op();
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid table operation type", K(ret), K(table_operation));
        break;
    }
    stat_row_count_ = 1;
  }

  if (OB_FAIL(ret)) {
    // init_tb_ctx will return some replaceable error code
    result_.set_err(ret);
    table::ObTableApiUtil::replace_ret_code(ret);

    if (OB_NOT_NULL(group_single_op_)) {
      // In group commit scene:
      // ret != OB_SUCCESS mean we should response packet by the responser in rpc processor
      // and here we should free group_single_op_ in hand
      TABLEAPI_GROUP_COMMIT_MGR->free_op(group_single_op_);
      group_single_op_ = nullptr;
    }
  }

#ifndef NDEBUG
  // debug mode
  LOG_INFO("[TABLE] execute operation", K(ret), K_(result), K_(retry_count));
#else
  // release mode
  LOG_TRACE("[TABLE] execute operation", K(ret), K_(result),
              "receive_ts", get_receive_timestamp(), K_(retry_count));
#endif
  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, get_tx_snapshot(),
                     stmt_type, ObTableAuditUtils::get_stmt_type(table_operation.type()));
  return ret;
}

uint64_t ObTableApiExecuteP::get_request_checksum()
{
  uint64_t checksum = 0;
  checksum = ob_crc64(checksum, arg_.table_name_.ptr(), arg_.table_name_.length());
  checksum = ob_crc64(checksum, &arg_.consistency_level_, sizeof(arg_.consistency_level_));
  checksum = ob_crc64(checksum, &arg_.option_flag_, sizeof(arg_.option_flag_));
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
  tb_ctx_.reset();
  need_retry_in_queue_ = false;
}

int ObTableApiExecuteP::process_get()
{
  int ret = OB_SUCCESS;
  ObNewRow *row = nullptr;

  if (OB_FAIL(check_arg2())) {
    LOG_WARN("fail to check arg", K(ret));
  } else if (OB_FAIL(trans_param_.init(arg_.consistency_level_,
                                      tb_ctx_.get_ls_id(),
                                      tb_ctx_.get_timeout_ts(),
                                      false))) {
    LOG_WARN("fail to inti trans param", K(ret));
  } else if (OB_FAIL(ObTableTransUtils::init_read_trans(trans_param_))) {
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
    ObKvSchemaCacheGuard *schema_cache_guard = tb_ctx_.get_schema_cache_guard();
    if (OB_ISNULL(schema_cache_guard) || !schema_cache_guard->is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_cache_cache is NULL or not inited", K(ret));
    } else if (OB_FAIL(result_.get_entity(result_entity))) {
      LOG_WARN("fail to get result entity", K(ret));
    } else if (OB_FAIL(ObTableApiUtil::construct_entity_from_row(allocator_,
                                                                 row,
                                                                 *schema_cache_guard,
                                                                 tb_ctx_.get_query_col_names(),
                                                                 result_entity))) {
      LOG_WARN("fail to fill result entity", K(ret));
    }
  }

  ObTableTransUtils::release_read_trans(trans_param_.trans_desc_);
  result_.set_err(ret);
  ObTableApiUtil::replace_ret_code(ret);
  result_.set_type(arg_.table_operation_.type());

  return ret;
}

int ObTableApiExecuteP::process_incr_or_append_op()
{
  int ret = OB_SUCCESS;
  table::ObTableExecuteCreateCbFunctor functor;
  if (OB_FAIL(start_trans(false,
                          arg_.consistency_level_,
                          tb_ctx_.get_ls_id(),
                          get_timeout_ts(),
                          tb_ctx_.need_dist_das()))) {
    SERVER_LOG(WARN, "fail to start transaction", K(ret), K_(tb_ctx));
  } else if (OB_FAIL(tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    SERVER_LOG(WARN, "fail to init trans", K(ret));
  } else if (OB_FAIL(table::ObTableOpWrapper::process_incr_or_append_op(tb_ctx_, result_))) {
    SERVER_LOG(WARN, "fail to process op", K(ret));
  } else if (OB_FAIL(functor.init(req_, &result_, arg_.table_operation_.type()))) {
    SERVER_LOG(WARN, "fail to init create execute callback functor", K(ret));
  }

  int tmp_ret = ret;
  if (OB_FAIL(end_trans(OB_SUCCESS != ret, req_, &functor))) {
    SERVER_LOG(WARN, "fail to end trans", K(ret));
  }

  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObTableApiExecuteP::before_response(int error_code)
{
  // NOTE: when check_timeout failed, the result.entity_ is null, and serialize result cause coredump
  if (!had_do_response() && OB_ISNULL(result_.get_entity())) {
    result_.set_entity(result_entity_);
  }
  return ObTableRpcProcessor::before_response(error_code);
}
