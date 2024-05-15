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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_das_task.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/das/ob_das_rpc_processor.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase
{
namespace common
{
namespace serialization
{
template <>
struct EnumEncoder<false, const sql::ObDASBaseCtDef*> : sql::DASCtEncoder<sql::ObDASBaseCtDef>
{
};

template <>
struct EnumEncoder<false, sql::ObDASBaseRtDef*> : sql::DASRtEncoder<sql::ObDASBaseRtDef>
{
};
} // end namespace serialization
} // end namespace common

using namespace common;
namespace sql
{
OB_DEF_SERIALIZE(ObDASRemoteInfo)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = exec_ctx_->get_my_session();
  OB_UNIS_ENCODE(flags_);
  if (OB_SUCC(ret) && need_tx_) {
    OB_UNIS_ENCODE(*trans_desc_);
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_ENCODE(snapshot_);
  }
  if (OB_SUCC(ret) && (need_calc_expr_ || need_calc_udf_)) {
    OB_UNIS_ENCODE(session->get_effective_tenant_id());
    OB_UNIS_ENCODE(*session);
  }
  if (OB_SUCC(ret) && has_expr_) {
    OZ(ObPxTreeSerializer::serialize_expr_frame_info(
        buf, buf_len, pos, *exec_ctx_, const_cast<ObExprFrameInfo &>(*frame_info_)));
  }
  OB_UNIS_ENCODE(ctdefs_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < ctdefs_.count(); ++i) {
    const ObDASBaseCtDef *ctdef = ctdefs_.at(i);
    OB_UNIS_ENCODE(ctdef->op_type_);
    OB_UNIS_ENCODE(*ctdef);
  }
  OB_UNIS_ENCODE(rtdefs_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < rtdefs_.count(); ++i) {
    ObDASBaseRtDef *rtdef = rtdefs_.at(i);
    OB_UNIS_ENCODE(rtdef->op_type_);
    OB_UNIS_ENCODE(*rtdef);
  }

  OB_UNIS_ENCODE(static_cast<int64_t>(sizeof(sql_id_)));
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(pos + sizeof(sql_id_) > buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("serialization of ObDASRemoteInfo has not enough buffer", KR(ret), K(pos), K(buf_len), K(sizeof(sql_id_)));
  } else {
    MEMCPY(buf + pos, sql_id_, sizeof(sql_id_));
    pos += sizeof(sql_id_);
  }
  OB_UNIS_ENCODE(user_id_);
  OB_UNIS_ENCODE(session_id_);
  OB_UNIS_ENCODE(plan_id_);
  OB_UNIS_ENCODE(plan_hash_);
  //Serializing the reference relationship between ctdefs and rtdefs.
  for (int i = 0; OB_SUCC(ret) && i < ctdefs_.count(); ++i) {
    const ObDASBaseCtDef *ctdef = ctdefs_.at(i);
    OB_UNIS_ENCODE(ctdef->children_cnt_);
    for (int j = 0; OB_SUCC(ret) && j < ctdef->children_cnt_; ++j) {
      const ObDASBaseCtDef *child_ctdef = ctdef->children_[j];
      OB_UNIS_ENCODE(child_ctdef);
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < rtdefs_.count(); ++i) {
    ObDASBaseRtDef *rtdef = rtdefs_.at(i);
    OB_UNIS_ENCODE(rtdef->ctdef_);
    OB_UNIS_ENCODE(rtdef->children_cnt_);
    for (int j = 0; OB_SUCC(ret) && j < rtdef->children_cnt_; ++j) {
      ObDASBaseRtDef *child_rtdef = rtdef->children_[j];
      OB_UNIS_ENCODE(child_rtdef);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDASRemoteInfo)
{
  int ret = OB_SUCCESS;
  bool need_session = false;
  int64_t ctdef_cnt = 0;
  int64_t rtdef_cnt = 0;
  int64_t sql_id_len = 0;
  ObEvalCtx *eval_ctx = nullptr;
  ObDASTaskFactory *das_factory =
      ObDASAsyncAccessP::get_das_factory() != nullptr
          ? ObDASAsyncAccessP::get_das_factory()
          : ObDASSyncAccessP::get_das_factory();
#if !defined(NDEBUG)
  CK(typeid(*exec_ctx_) == typeid(ObDesExecContext));
#endif
  OB_UNIS_DECODE(flags_);
  if (OB_SUCC(ret) && need_tx_) {
    auto txs = MTL(transaction::ObTransService*);
    if (OB_FAIL(txs->acquire_tx(buf, data_len, pos, trans_desc_))) {
      LOG_WARN("acquire tx by deserialized failed", K(data_len), K(pos), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(snapshot_);
  }
  if (OB_SUCC(ret) && (need_calc_expr_ || need_calc_udf_)) {
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    ObDesExecContext *des_exec_ctx = static_cast<ObDesExecContext*>(exec_ctx_);
    OB_UNIS_DECODE(tenant_id);
    OZ(des_exec_ctx->create_my_session(tenant_id));
    OB_UNIS_DECODE(*des_exec_ctx->get_my_session());
    if (OB_SUCC(ret)) {
      //notice: can't unlink exec context and session info here
      typedef ObSQLSessionInfo::ExecCtxSessionRegister MyExecCtxSessionRegister;
      des_exec_ctx->get_my_session()->set_is_remote(true);
      MyExecCtxSessionRegister ctx_register(*des_exec_ctx->get_my_session(), *des_exec_ctx);
      // for remote das, we use thread local ash stat to record ash.
      //   des_exec_ctx->get_my_session()->set_session_type_with_flag();
      // if (OB_FAIL(des_exec_ctx->get_my_session()->set_session_active(
      //     ObString::make_string("REMOTE/DISTRIBUTE DAS PLAN EXECUTING"),
      //     obmysql::COM_QUERY))) {
      //   LOG_WARN("set das remote session active failed", K(ret));
      // }
      // EVENT_INC(ACTIVE_SESSIONS);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(des_exec_ctx->get_my_session()->set_session_state(QUERY_ACTIVE))) {
      LOG_WARN("set session state failed", K(ret));
    } else if (OB_FAIL(des_exec_ctx->get_my_session()->store_query_string(
        ObString::make_string("DAS REMOTE SCHEDULING")))) {
      LOG_WARN("store query string failed", K(ret));
    } else {
      des_exec_ctx->get_my_session()->set_mysql_cmd(obmysql::COM_QUERY);
    }
  }
  OZ(exec_ctx_->create_physical_plan_ctx());
  if (OB_SUCC(ret) && has_expr_) {
    OZ(ObPxTreeSerializer::deserialize_expr_frame_info(
        buf, data_len, pos, *exec_ctx_, const_cast<ObExprFrameInfo &>(*frame_info_)));
    OZ(exec_ctx_->init_expr_op(frame_info_->rt_exprs_.count()));
    if (OB_SUCC(ret)) {
      eval_ctx = OB_NEWx(ObEvalCtx, (&exec_ctx_->get_allocator()), (*exec_ctx_));
      if (OB_ISNULL(eval_ctx)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate eval ctx failed", K(ret));
      }
    }
  }
  OB_UNIS_DECODE(ctdef_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < ctdef_cnt; ++i) {
    ObDASBaseCtDef *ctdef = nullptr;
    ObDASOpType op_type = DAS_OP_INVALID;
    OB_UNIS_DECODE(op_type);
    OZ(das_factory->create_das_ctdef(op_type, ctdef));
    OB_UNIS_DECODE(*ctdef);
    OZ(ctdefs_.push_back(ctdef));
  }
  OB_UNIS_DECODE(rtdef_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < rtdef_cnt; ++i) {
    ObDASBaseRtDef *rtdef = nullptr;
    ObDASOpType op_type = DAS_OP_INVALID;
    OB_UNIS_DECODE(op_type);
    OZ(das_factory->create_das_rtdef(op_type, rtdef));
    OB_UNIS_DECODE(*rtdef);
    OX(rtdef->eval_ctx_ = eval_ctx);
    OZ(rtdefs_.push_back(rtdef));
  }
  OB_UNIS_DECODE(sql_id_len);
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(pos + sql_id_len > data_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("deserialization of ObDASRemoteInfo has not enough buffer", KR(ret), K(pos), K(data_len), K(sql_id_len));
  } else {
    MEMCPY(sql_id_, buf + pos, sql_id_len);
    pos += sql_id_len;
  }
  OB_UNIS_DECODE(user_id_);
  OB_UNIS_DECODE(session_id_);
  OB_UNIS_DECODE(plan_id_);
  OB_UNIS_DECODE(plan_hash_);
  //rebuilding the reference relationship between ctdefs and rtdefs after deserialization.
  for (int i = 0; OB_SUCC(ret) && i < ctdefs_.count(); ++i) {
    ObDASBaseCtDef *ctdef = const_cast<ObDASBaseCtDef*>(ctdefs_.at(i));
    OB_UNIS_DECODE(ctdef->children_cnt_);
    if (OB_SUCC(ret) && ctdef->children_cnt_ > 0) {
      if (OB_ISNULL(ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &exec_ctx_->get_allocator(), ctdef->children_cnt_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate ctdef children_ failed", K(ret), K(ctdef->children_cnt_));
      }
    }
    for (int j = 0; OB_SUCC(ret) && j < ctdef->children_cnt_; ++j) {
      const ObDASBaseCtDef *child_ctdef = nullptr;
      OB_UNIS_DECODE(child_ctdef);
      if (OB_SUCC(ret)) {
        ctdef->children_[j] = const_cast<ObDASBaseCtDef*>(child_ctdef);
      }
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < rtdefs_.count(); ++i) {
    ObDASBaseRtDef *rtdef = rtdefs_.at(i);
    OB_UNIS_DECODE(rtdef->ctdef_);
    OB_UNIS_DECODE(rtdef->children_cnt_);
    if (OB_SUCC(ret) && rtdef->children_cnt_ > 0) {
      if (OB_ISNULL(rtdef->children_ = OB_NEW_ARRAY(ObDASBaseRtDef*, &exec_ctx_->get_allocator(), rtdef->children_cnt_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate rtdef children_ failed", K(ret), K(rtdef->children_cnt_));
      }
    }
    for (int j = 0; OB_SUCC(ret) && j < rtdef->children_cnt_; ++j) {
      ObDASBaseRtDef *child_rtdef = nullptr;
      OB_UNIS_DECODE(child_rtdef);
      if (OB_SUCC(ret)) {
        rtdef->children_[j] = child_rtdef;
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASRemoteInfo)
{
  int64_t len = 0;
  ObSQLSessionInfo *session = exec_ctx_->get_my_session();
  OB_UNIS_ADD_LEN(flags_);
  if (need_tx_) {
    OB_UNIS_ADD_LEN(*trans_desc_);
  }
  OB_UNIS_ADD_LEN(snapshot_);
  if (need_calc_expr_ || need_calc_udf_) {
    OB_UNIS_ADD_LEN(session->get_effective_tenant_id());
    OB_UNIS_ADD_LEN(*session);
  }
  if (has_expr_) {
    len += ObPxTreeSerializer::get_serialize_expr_frame_info_size(*exec_ctx_,
             const_cast<ObExprFrameInfo&>(*frame_info_));
  }
  OB_UNIS_ADD_LEN(ctdefs_.count());
  for (int64_t i = 0; i < ctdefs_.count(); ++i) {
    const ObDASBaseCtDef *ctdef = ctdefs_.at(i);
    OB_UNIS_ADD_LEN(ctdef->op_type_);
    OB_UNIS_ADD_LEN(*ctdef);
  }
  OB_UNIS_ADD_LEN(rtdefs_.count());
  for (int64_t i = 0; i < rtdefs_.count(); ++i) {
    ObDASBaseRtDef *rtdef = rtdefs_.at(i);
    OB_UNIS_ADD_LEN(rtdef->op_type_);
    OB_UNIS_ADD_LEN(*rtdef);
  }

  OB_UNIS_ADD_LEN(sizeof(sql_id_));
  len += sizeof(sql_id_);
  OB_UNIS_ADD_LEN(user_id_);
  OB_UNIS_ADD_LEN(session_id_);
  OB_UNIS_ADD_LEN(plan_id_);
  OB_UNIS_ADD_LEN(plan_hash_);
  //Serializing the reference relationship between ctdefs and rtdefs.
  for (int i = 0; i < ctdefs_.count(); ++i) {
    const ObDASBaseCtDef *ctdef = ctdefs_.at(i);
    OB_UNIS_ADD_LEN(ctdef->children_cnt_);
    for (int j = 0; j < ctdef->children_cnt_; ++j) {
      const ObDASBaseCtDef *child_ctdef = ctdef->children_[j];
      OB_UNIS_ADD_LEN(child_ctdef);
    }
  }
  for (int i = 0; i < rtdefs_.count(); ++i) {
    ObDASBaseRtDef *rtdef = rtdefs_.at(i);
    OB_UNIS_ADD_LEN(rtdef->ctdef_);
    OB_UNIS_ADD_LEN(rtdef->children_cnt_);
    for (int j = 0; j < rtdef->children_cnt_; ++j) {
      ObDASBaseRtDef *child_rtdef = rtdef->children_[j];
      OB_UNIS_ADD_LEN(child_rtdef);
    }
  }
  return len;
}

int ObIDASTaskOp::start_das_task()
{
  int &ret = errcode_;
  int simulate_error = EVENT_CALL(EventTable::EN_DAS_SIMULATE_OPEN_ERROR);
  int need_dump = EVENT_CALL(EventTable::EN_DAS_SIMULATE_DUMP_WRITE_BUFFER);
  if (OB_UNLIKELY(!is_in_retry() && OB_SUCCESS != simulate_error)) {
    ret = simulate_error;
  } else {
    task_started_ = true;
    if (OB_FAIL(open_op())) {
      LOG_WARN("open das task op failed", K(ret));
      if (OB_ERR_DEFENSIVE_CHECK == ret) {
        //dump das task data to help analysis defensive bug
        dump_data();
      }
    } else if (OB_SUCCESS != need_dump) {
      dump_data();
    }
  }
  // no need to advance state here because this function could be called on remote executor.
  if (OB_FAIL(ret)) {
    set_task_status(ObDasTaskStatus::FAILED);
  } else {
    set_task_status(ObDasTaskStatus::FINISHED);
  }
  return ret;
}

void ObIDASTaskOp::set_task_status(ObDasTaskStatus status)
{
  task_status_ = status;
};

int ObIDASTaskOp::end_das_task()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  //release op，then rollback transcation
  if (task_started_) {
    if (OB_SUCCESS != (tmp_ret = release_op())) {
      LOG_WARN("release das task op failed", K(ret), K_(errcode));
    }
    ret = COVER_SUCC(tmp_ret);
  }
  task_started_ = false;
  ret = COVER_SUCC(tmp_ret);
  errcode_ = OB_SUCCESS;
  return ret;
}

OB_SERIALIZE_MEMBER(ObIDASTaskOp,
                    tenant_id_,
                    task_id_,
                    task_flag_,
                    tablet_id_,
                    ls_id_,
                    related_ctdefs_,
                    related_rtdefs_,
                    related_tablet_ids_,
                    attach_ctdef_,
                    attach_rtdef_,
                    das_gts_opt_info_);

OB_DEF_SERIALIZE(ObDASGTSOptInfo)
{
  int ret = OB_SUCCESS;
  bool serialize_specify_snapshot = specify_snapshot_ == nullptr ? false : true;
  LST_DO_CODE(OB_UNIS_ENCODE,
              use_specify_snapshot_,
              isolation_level_,
              serialize_specify_snapshot);
  if (serialize_specify_snapshot) {
    OB_UNIS_ENCODE(*specify_snapshot_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDASGTSOptInfo)
{
  int ret = OB_SUCCESS;
  bool serialize_specify_snapshot = false;
  LST_DO_CODE(OB_UNIS_DECODE,
              use_specify_snapshot_,
              isolation_level_,
              serialize_specify_snapshot);
  if (serialize_specify_snapshot) {
    if (OB_FAIL(init(isolation_level_))) {
      LOG_WARN("fail to init gts_opt_info", K(ret));
    } else {
      OB_UNIS_DECODE(*specify_snapshot_);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASGTSOptInfo)
{
  int64_t len = 0;
  bool serialize_specify_snapshot = specify_snapshot_ == nullptr ? false : true;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              use_specify_snapshot_,
              isolation_level_,
              serialize_specify_snapshot);
  if (serialize_specify_snapshot) {
    OB_UNIS_ADD_LEN(*specify_snapshot_);
  }
  return len;
}

int ObDASGTSOptInfo::init(transaction::ObTxIsolationLevel isolation_level)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  void *buf2 = nullptr;
  if (OB_ISNULL(buf = alloc_.alloc(sizeof(ObTxReadSnapshot)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObTxReadSnapshot", K(ret), K(sizeof(ObTxReadSnapshot)));
  } else if (OB_ISNULL(buf2 = alloc_.alloc(sizeof(ObTxReadSnapshot)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObTxReadSnapshot", K(ret), K(sizeof(ObTxReadSnapshot)));
  } else {
    use_specify_snapshot_ = true;
    isolation_level_ = isolation_level;
    specify_snapshot_ = new(buf) ObTxReadSnapshot();
    response_snapshot_ = new(buf2) ObTxReadSnapshot();
  }
  return ret;
}

ObDASTaskArg::ObDASTaskArg()
  : timeout_ts_(0),
    ctrl_svr_(),
    runner_svr_(),
    task_ops_(),
    remote_info_(nullptr)
{
}

OB_DEF_SERIALIZE(ObDASTaskArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              timeout_ts_,
              ctrl_svr_,
              runner_svr_,
              *remote_info_); //remote info must be serialized before task op
  if (OB_SUCC(ret) && OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, task_ops_.count()))) {
    LOG_WARN("fail to encode ob array count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < task_ops_.count(); i++) {
    OB_UNIS_ENCODE(task_ops_.at(i)->get_type());
    OB_UNIS_ENCODE(*task_ops_.at(i));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDASTaskArg)
{
  int ret = OB_SUCCESS;
  ObDASOpType op_type = DAS_OP_INVALID;
  int64_t count = 0;
  ObIDASTaskOp *task_op = nullptr;
  ObDASTaskFactory *das_factory =
      ObDASAsyncAccessP::get_das_factory() != nullptr
          ? ObDASAsyncAccessP::get_das_factory()
          : ObDASSyncAccessP::get_das_factory();
  CK(OB_NOT_NULL(das_factory));
  LST_DO_CODE(OB_UNIS_DECODE,
              timeout_ts_,
              ctrl_svr_,
              runner_svr_,
              *remote_info_); //remote info must be deserialized before task op
  task_ops_.reset();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    LOG_WARN("fail to decode ob array count", K(ret));
  } else if (OB_FAIL(task_ops_.prepare_allocate(count))) {
    LOG_WARN("fail to allocate space", K(ret), K(count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
    OB_UNIS_DECODE(op_type);
    OZ(das_factory->create_das_task_op(op_type, task_op));
    // Here you must init first, you need to set the allocator
    OZ(task_op->init_task_info(ObDASWriteBuffer::DAS_ROW_DEFAULT_EXTEND_SIZE));
    if (OB_SUCC(ret)) {
      task_ops_.at(i) = task_op;
    }
    OB_UNIS_DECODE(*task_op);
    OZ(task_op->swizzling_remote_task(remote_info_));
  }
  if (OB_SUCC(ret)) {
    (void)ObSQLUtils::adjust_time_by_ntp_offset(timeout_ts_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASTaskArg)
{
  int64_t len = 0;
  if (task_ops_.count() != 0) {
    LST_DO_CODE(OB_UNIS_ADD_LEN,
                timeout_ts_,
                ctrl_svr_,
                runner_svr_,
                *remote_info_);
    len += serialization::encoded_length_vi64(task_ops_.count());
    for (int i = 0; i < task_ops_.count(); i++) {
      len += serialization::encoded_length(task_ops_.at(i)->get_type());
      len += serialization::encoded_length(*task_ops_.at(i));
    }
  }
  return len;
}

int ObDASTaskArg::add_task_op(ObIDASTaskOp *task_op)
{
  return task_ops_.push_back(task_op);
}

ObIDASTaskOp *ObDASTaskArg::get_task_op()
{
  return task_ops_.at(0);
}

int ObIDASTaskOp::state_advance()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(cur_agg_list_ != nullptr);
  OB_ASSERT(task_status_ != ObDasTaskStatus::UNSTART);
  if (task_status_ == ObDasTaskStatus::FINISHED) {
    if (OB_FAIL(get_agg_tasks()->move_to_success_tasks(this))) {
      LOG_WARN("failed to move task to success tasks", KR(ret));
    }
  } else if (task_status_ == ObDasTaskStatus::FAILED) {
    if (OB_FAIL(get_agg_tasks()->move_to_failed_tasks(this))) {
      LOG_WARN("failed to move task to success tasks", KR(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid task state",KR(ret), K_(task_status));
  }
  return ret;
}

ObDASTaskResp::ObDASTaskResp()
  : has_more_(false),
    ctrl_svr_(),
    runner_svr_(),
    op_results_(),
    rcode_(),
    trans_result_(),
    das_factory_(nullptr)
{
}

void ObDASTaskResp::store_err_msg(const ObString &msg)
{
  int ret = OB_SUCCESS;
  if (!msg.empty()) {
    //这里使用databuff_printf的原因是databuff_printf在遇到buffer溢出时会保证buffer以'\0'结束，确保print的安全性
    if (OB_FAIL(databuff_printf(rcode_.msg_, OB_MAX_ERROR_MSG_LEN, "%.*s", msg.length(), msg.ptr()))) {
      SHARE_LOG(WARN, "store err msg failed", K(ret), K(msg));
      if (OB_SIZE_OVERFLOW == ret) {
        rcode_.msg_[OB_MAX_ERROR_MSG_LEN - 1] = '\0';
      }
    }
  } else {
    rcode_.msg_[0] = '\0';
  }
}

int ObDASTaskResp::store_warning_msg(const ObWarningBuffer &wb)
{
  int ret = OB_SUCCESS;
  bool not_null = true;
  for (uint32_t idx = 0; OB_SUCC(ret) && not_null && idx < wb.get_readable_warning_count(); idx++) {
    const ObWarningBuffer::WarningItem *item = wb.get_warning_item(idx);
    if (item != NULL) {
      if (OB_FAIL(rcode_.warnings_.push_back(*item))) {
        RPC_OBRPC_LOG(WARN, "Failed to add warning", K(ret));
      }
    } else {
      not_null = false;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObDASTaskResp)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              has_more_,
              ctrl_svr_,
              runner_svr_);
  if (OB_SUCC(ret) && OB_FAIL(serialization::encode_vi64(
      buf, buf_len, pos, op_results_.count()))) {
    LOG_WARN("fail to encode ob array count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < op_results_.count(); i ++) {
    if (OB_FAIL(serialization::encode(buf, buf_len, pos, *op_results_.at(i)))) {
      LOG_WARN("fail to encode item", K(i), K(ret));
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              rcode_,
              trans_result_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDASTaskResp)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  ObIDASTaskResult *op_result = nullptr;
  LST_DO_CODE(OB_UNIS_DECODE,
              has_more_,
              ctrl_svr_,
              runner_svr_);
  if (OB_SUCC(ret) && OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    LOG_WARN("fail to decode ob array count", K(ret));
  } else if (count > op_results_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("receive das task response count mismatch", K(count), K(op_results_.count()));
  }
  while (op_results_.count() > count) {
    op_results_.pop_back();
  }
  OB_ASSERT(op_results_.count() == count);
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    if (OB_FAIL(serialization::decode(buf, data_len, pos, *op_results_.at(i)))) {
      LOG_WARN("fail to decode array item", K(ret), K(i), K(count));
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              rcode_,
              trans_result_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASTaskResp)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              has_more_,
              ctrl_svr_,
              runner_svr_);
  len += serialization::encoded_length_vi64(op_results_.count());
  for (int i = 0; i < op_results_.count(); i++) {
    len += serialization::encoded_length(*op_results_.at(i));
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              rcode_,
              trans_result_);
  return len;
}

int ObDASTaskResp::add_op_result(ObIDASTaskResult *op_result)
{
  return op_results_.push_back(op_result);
}

OB_SERIALIZE_MEMBER(ObIDASTaskResult, task_id_);

OB_SERIALIZE_MEMBER(ObDASDataFetchReq, tenant_id_, task_id_);

int ObDASDataFetchReq::init(const uint64_t tenant_id, const int64_t task_id)
{
  tenant_id_ = tenant_id;
  task_id_ = task_id;
  return OB_SUCCESS;
}

OB_SERIALIZE_MEMBER(ObDASDataEraseReq, tenant_id_, task_id_);

int ObDASDataEraseReq::init(const uint64_t tenant_id, const int64_t task_id)
{
  tenant_id_ = tenant_id;
  task_id_ = task_id;
  return OB_SUCCESS;
}

OB_SERIALIZE_MEMBER(ObDASDataFetchRes,
                    datum_store_,
                    tenant_id_, task_id_, has_more_,
                    enable_rich_format_, vec_row_store_,
                    io_read_bytes_,
                    ssstore_read_bytes_,
                    ssstore_read_row_cnt_,
                    memstore_read_row_cnt_);

ObDASDataFetchRes::ObDASDataFetchRes()
        : datum_store_("DASDataFetch"),
          tenant_id_(0),
          task_id_(0),
          has_more_(false),
          enable_rich_format_(false),
          vec_row_store_(),
          io_read_bytes_(0),
          ssstore_read_bytes_(0),
          ssstore_read_row_cnt_(0),
          memstore_read_row_cnt_(0)
{
}

int ObDASDataFetchRes::init(const uint64_t tenant_id, const int64_t task_id)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  task_id_ = task_id;
  return ret;
}

int DASOpResultIter::get_next_row()
{
  int ret = OB_SUCCESS;
  if (!task_iter_.is_end()) {
    ObDASScanOp *scan_op = DAS_SCAN_OP(*task_iter_);
    if (OB_ISNULL(scan_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected das task op type", K(ret), KPC(*task_iter_));
    } else {
      ret = scan_op->get_output_result_iter()->get_next_row();
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int DASOpResultIter::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (!task_iter_.is_end()) {
    ObDASScanOp *scan_op = DAS_SCAN_OP(*task_iter_);
    if (OB_ISNULL(scan_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected das task op type", K(ret), KPC(scan_op));
    } else {
      if (scan_op->is_local_task()) {
        //output row from remote DAS task
        //maybe change the expr datum ptr to its RPC datum store
        //if we need to fetch next row from the next das task,
        //must reset the datum ptr to expr preallocate frame buffer
        //otherwise, get_next_row in the local das task maybe touch a wild datum ptr
        reset_wild_datums_ptr();
      }
      LOG_DEBUG("das get next rows", K(enable_rich_format_), K(scan_op->is_local_task()), KP(scan_op));
      if (OB_FAIL(scan_op->get_output_result_iter()->get_next_rows(count, capacity))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get rows", K(ret));
        }
      }
      if (!scan_op->is_local_task()) {
        //remote task will change datum ptr, need to mark this flag
        //in order to let the next local task reset datum ptr before get_next_rows
        wild_datum_info_->exprs_ = &scan_op->get_result_outputs();
        //Now in the group scan op, we implement jump read.
        //We may touch more rows than count return.
        //So we need to reset all of ptr in the output expr datum.
        wild_datum_info_->max_output_rows_ = max(capacity, wild_datum_info_->max_output_rows_);
      }
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int DASOpResultIter::next_result()
{
  int ret = OB_SUCCESS;
  if (!task_iter_.is_end()) {
    ++task_iter_;
  }
  if (OB_UNLIKELY(task_iter_.is_end())) {
    ret = OB_ITER_END;
    LOG_DEBUG("fetch next das task end", K(ret));
  }
  return ret;
}

int DASOpResultIter::reset_wild_datums_ptr()
{
  int ret = OB_SUCCESS;
  if (wild_datum_info_ != nullptr) {
    if (wild_datum_info_->exprs_ != nullptr &&
        wild_datum_info_->max_output_rows_ > 0) {
      FOREACH_CNT(e, *wild_datum_info_->exprs_) {
        (*e)->locate_datums_for_update(wild_datum_info_->eval_ctx_,
                                       wild_datum_info_->max_output_rows_);
        ObEvalInfo &info = (*e)->get_eval_info(wild_datum_info_->eval_ctx_);
        info.point_to_frame_ = true;
      }
      wild_datum_info_->exprs_ = nullptr;
      wild_datum_info_->max_output_rows_ = 0;
    }
    //global index scan and its lookup maybe share some expr,
    //so remote lookup task change its datum ptr,
    //and also lead index scan to touch the wild datum ptr
    //so need to associate the result iterator of scan and lookup
    //resetting the index scan result datum ptr will also reset the lookup result datum ptr
    if (wild_datum_info_->lookup_iter_ != nullptr) {
      wild_datum_info_->lookup_iter_->reset_wild_datums_ptr();
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
