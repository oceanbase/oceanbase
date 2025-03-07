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
#include "ob_redis_execute_processor.h"
#include "ob_table_move_response.h"
#include "redis/cmd/ob_redis_cmd.h"
#include "group/ob_table_tenant_group.h"
#include "redis/ob_redis_command_factory.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

ObRedisExecuteP::ObRedisExecuteP(const ObGlobalContext &gctx)
    : ObTableRpcProcessor(gctx),
      allocator_("TbRedisExeP", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      default_entity_factory_("TableRedisEntFac", MTL_ID()),
      redis_result_(&allocator_),
      redis_ctx_(allocator_, &default_entity_factory_, arg_, redis_result_)
{}

int ObRedisExecuteP::deserialize()
{
  int ret = OB_SUCCESS;

  arg_.table_operation_.set_entity(request_entity_);  // deserialize to request_entity_
  result_.set_entity(result_entity_);
  if (OB_FAIL(ParentType::deserialize())) {
    LOG_WARN("fail to deserialize parent type", K(ret));
  }
  return ret;
}

int ObRedisExecuteP::before_process()
{
  int ret = OB_SUCCESS;

  bool is_enable_group_op = false;
  bool is_cmd_support_group = false;
  if (OB_FAIL(ParentType::before_process())) {
    LOG_WARN("before process failed", K(ret));
  } else if (OB_FAIL(redis_ctx_.decode_request())) {
    LOG_WARN("init redis_ctx set req failed", K(ret), K(redis_ctx_));
  } else if (OB_FAIL(ObRedisCommandFactory::cmd_is_support_group(redis_ctx_.request_.get_cmd_name(), is_cmd_support_group))) {
    LOG_WARN("fail to get group commit config", K(ret));
  } else if (ObTableGroupUtils::is_group_commit_enable(ObTableOperationType::REDIS)) {
    is_enable_group_op = true;
  }

  redis_ctx_.set_is_cmd_support_group(is_cmd_support_group);
  redis_ctx_.set_is_enable_group_op(is_enable_group_op);

  return ret;
}

int ObRedisExecuteP::init_redis_ctx()
{
  int ret = OB_SUCCESS;

  if (!tablet_id_.is_valid()) {
    tablet_id_ = simple_table_schema_->get_tablet_id();
  }

  redis_ctx_.entity_factory_ = &default_entity_factory_;
  redis_ctx_.table_id_ = arg_.table_id_;
  redis_ctx_.tablet_id_ = tablet_id_;
  redis_ctx_.timeout_ts_ = get_timeout_ts();
  redis_ctx_.timeout_ = get_timeout();
  redis_ctx_.credential_ = &credential_;
  redis_ctx_.trans_param_ = &trans_param_;
  redis_ctx_.consistency_level_ = arg_.consistency_level_;
  redis_ctx_.rpc_req_ = req_;
  redis_ctx_.request_.set_table_name(simple_table_schema_->get_table_name());

  bool is_cache_hit = false;
  if (OB_FAIL(GCTX.location_service_->get(credential_.tenant_id_,
                                          tablet_id_,
                                          0, /* expire_renew_time */
                                          is_cache_hit,
                                          redis_ctx_.ls_id_))) {
    LOG_WARN("fail to get ls id", K(ret), K(credential_.tenant_id_), K_(tablet_id));
  }

  return ret;
}

int ObRedisExecuteP::check_arg()
{
  int ret = OB_SUCCESS;
  if (!arg_.returning_affected_entity()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "need return affected entity");
    LOG_WARN("need return affected entity.", K(ret));
  } else if (!(arg_.consistency_level_ == ObTableConsistencyLevel::STRONG ||
               arg_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid consistency level");
    LOG_WARN("some options not supported yet", K(ret), "consistency_level", arg_.consistency_level_);
  }
  return ret;
}

void ObRedisExecuteP::reset_ctx()
{
  result_.reset();
  ObTableApiProcessorBase::reset_ctx();
  redis_ctx_.reset();
  result_entity_.reset();
  need_retry_in_queue_ = false;
  redis_result_.reset();
}

void ObRedisExecuteP::init_redis_common(table::ObRedisCtx &ctx)
{
  ctx.redis_guard_.schema_cache_guard_ = &schema_cache_guard_;
  ctx.redis_guard_.schema_guard_ = &schema_guard_;
  ctx.redis_guard_.simple_table_schema_ = simple_table_schema_;
  ctx.redis_guard_.sess_guard_ = &sess_guard_;
  ctx.audit_ctx_ = &audit_ctx_;
}

int ObRedisExecuteP::check_tenant_version()
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() < MOCK_CLUSTER_VERSION_4_2_5_0
      || (CLUSTER_VERSION_4_3_0_0 <= GET_MIN_CLUSTER_VERSION() && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_5_1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "min cluster version is less than 4.2.5 or between 4.3.0.0 and 4.3.5.1, redis is");
    LOG_WARN("min cluster version is less than 4.2.5 or between 4.3.0.0 and 4.3.5.1, redis is not supported", K(ret), K(GET_MIN_CLUSTER_VERSION()));
  }
  return ret;
}

int ObRedisExecuteP::try_process()
{
  int ret = OB_SUCCESS;
  table::ObTableAuditRedisOp op(redis_ctx_.request_.get_cmd_name());
  OB_TABLE_START_AUDIT(credential_,
                       sess_guard_,
                       arg_.table_name_,
                       &audit_ctx_,
                       op);
  // NOTE(xiongliyao): Prevent redundant audit records during the execution of Redis commands
  audit_ctx_.need_audit_ = false;
  table_id_ = arg_.table_id_;
  tablet_id_ = arg_.tablet_id_;
  // note: use single get tmp
  if (OB_FAIL(check_arg())) {
    LOG_WARN("check arg failed", K(ret));
  } else if (OB_FAIL(check_tenant_version())) {
    LOG_WARN("fail to check tenant version", K(ret), K(credential_));
  } else if (OB_FAIL(init_schema_info(arg_.table_id_, arg_.table_name_))) {
    LOG_WARN("fail to init schema info", K(ret), K(arg_));
  } else if (simple_table_schema_->get_table_id() != table_id_) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("info from request is not up to date, need to refresh schema info",
              K(ret), K(table_id_), K(tablet_id_));
  } else if (OB_FAIL(init_redis_ctx())) {
    LOG_WARN("faild init redis ctx", K(ret));
  } else if (OB_FALSE_IT(init_redis_common(redis_ctx_))) {
  } else if (OB_FAIL(ObRedisService::execute(redis_ctx_))) {
    LOG_WARN("fail to execute redis service", K(ret));
  }
  stat_process_type_ = ObRedisService::get_stat_process_type(redis_ctx_.cmd_type_);
  if (redis_ctx_.did_async_commit_) {
    // if end_trans async_commit, do not response rpc immediately
    // @note the req_ may be freed, req_processor can not be read any more.
    // The req_has_wokenup_ MUST set to be true, otherwise req_processor will invoke req_->set_process_start_end_diff, cause memory core
    // @see ObReqProcessor::run() req_->set_process_start_end_diff(ObTimeUtility::fast_current_time());
    this->set_req_has_wokenup();
  }

  if (OB_FAIL(ret)) {
    reinterpret_cast<ObRedisSingleCtx&>(redis_ctx_).response_.return_table_error(ret);
  }

#ifndef NDEBUG
  // debug mode
  LOG_INFO("[TABLE] execute redis operation", K(ret), K_(result), K_(retry_count));
#else
  // release mode
  LOG_TRACE(
      "[TABLE] execute redis operation", K(ret), K_(result), K_(retry_count), "receive_ts", get_receive_timestamp());
#endif
  // NOTE(xiongliyao): Prevent redundant audit records during the execution of Redis commands
  audit_ctx_.need_audit_ = true;
  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, get_tx_snapshot(),
                     stmt_type, sql::stmt::StmtType::T_REDIS);
  return ret;
}

uint64_t ObRedisExecuteP::get_request_checksum()
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

int ObRedisExecuteP::before_response(int error_code)
{
  // NOTE: when check_timeout failed, the result.entity_ is null, and serialize result cause coredump
  if (!had_do_response() && OB_ISNULL(result_.get_entity())) {
    result_.set_entity(result_entity_);
  }
  return ObTableRpcProcessor::before_response(error_code);
}

int ObRedisExecuteP::response(const int retcode)
{
  int ret = OB_SUCCESS;
  if (!need_retry_in_queue_ && !had_do_response()) {
    // return the package
    const ObRpcPacket *rpc_pkt = &reinterpret_cast<const ObRpcPacket &>(req_->get_packet());
    if (ObTableRpcProcessorUtil::need_do_move_response(retcode, *rpc_pkt)) {
      // response rerouting packet
      ObTableMoveResponseSender sender(req_, retcode);
      if (OB_FAIL(sender.init(arg_.table_id_, arg_.tablet_id_, *gctx_.schema_service_))) {
        LOG_WARN("fail to init move response sender", K(ret), K_(arg));
      } else if (OB_FAIL(sender.response())) {
        LOG_WARN("fail to do move response", K(ret));
      }
      if (OB_FAIL(ret)) {
        ret = ObRpcProcessor::response(retcode);  // do common response when do move response failed
      }
    } else {
      if (OB_ISNULL(result_.get_entity())) {
        result_.set_entity(result_entity_);
      }
      redis_result_.convert_to_table_op_result(result_); // ignore ret
      ret = ObRpcProcessor::response(retcode);
    }
  }
  return ret;
}
