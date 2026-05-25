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
#include "ob_das_task.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/utility/utility.h"
#include "sql/das/ob_das_scan_op.h"
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
using namespace transaction;
namespace sql
{
ERRSIM_POINT_DEF(ERRSIM_LOOKUP_BATCH_FILL_BUFFER);

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
    OZ(ObPxTreeSerializer::serialize_expr_frame_info<true>(
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
  if (need_subschema_ctx_) {
    if (OB_NOT_NULL(exec_ctx_->get_physical_plan_ctx()->get_phy_plan())) {
      OB_UNIS_ENCODE(exec_ctx_->get_physical_plan_ctx()->get_phy_plan()->get_subschema_ctx());
    } else {
      OB_UNIS_ENCODE(exec_ctx_->get_physical_plan_ctx()->get_subschema_ctx());
    }
  }
  OB_UNIS_ENCODE(detectable_id_);
  OB_UNIS_ENCODE(stmt_type_);
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
      MyExecCtxSessionRegister ctx_register(*des_exec_ctx->get_my_session(), des_exec_ctx);
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
    OZ(ObPxTreeSerializer::deserialize_expr_frame_info<true>(
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
  if (need_subschema_ctx_) {
    OB_UNIS_DECODE(exec_ctx_->get_physical_plan_ctx()->get_subschema_ctx());
  }
  OB_UNIS_DECODE(detectable_id_);
  OB_UNIS_DECODE(stmt_type_);
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
    len += ObPxTreeSerializer::get_serialize_expr_frame_info_size<true>(*exec_ctx_,
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
  if (need_subschema_ctx_) {
    if (OB_NOT_NULL(exec_ctx_->get_physical_plan_ctx()->get_phy_plan())) {
      OB_UNIS_ADD_LEN(exec_ctx_->get_physical_plan_ctx()->get_phy_plan()->get_subschema_ctx());
    } else {
      OB_UNIS_ADD_LEN(exec_ctx_->get_physical_plan_ctx()->get_subschema_ctx());
    }
  }
  OB_UNIS_ADD_LEN(detectable_id_);
  OB_UNIS_ADD_LEN(stmt_type_);
  return len;
}

int ObIDASTaskOp::swizzling_remote_task(ObDASRemoteInfo *remote_info)
{
  int ret = OB_SUCCESS;
  if (remote_info != nullptr) {
    snapshot_ = &remote_info->snapshot_;
    if (das_gts_opt_info_.use_specify_snapshot_) {
      if (OB_ISNULL(das_gts_opt_info_.get_specify_snapshot())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr of specify_snapshot", K(ret));
      } else {
        snapshot_ = das_gts_opt_info_.get_specify_snapshot();
      }
    }
  }
  return ret;
}

int ObIDASTaskOp::start_das_task()
{
  int &ret = errcode_;
  int simulate_error = EVENT_CALL(EventTable::EN_DAS_SIMULATE_OPEN_ERROR);
  int need_dump = EVENT_CALL(EventTable::EN_DAS_SIMULATE_DUMP_WRITE_BUFFER);
  das_task_start_timestamp_ = common::ObTimeUtility::current_time();
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
      LOG_WARN("release das task op failed", K(tmp_ret), K_(errcode));
    }
    ret = COVER_SUCC(tmp_ret);
  }

  task_started_ = false;
  errcode_ = OB_SUCCESS;
  return ret;
}

int ObIDASTaskOp::init_das_gts_opt_info(transaction::ObTxIsolationLevel isolation_level)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_das_gts_opt_info().init(isolation_level))) {
    LOG_WARN("fail to init das gts opt", K(ret), K(isolation_level));
  } else {
    snapshot_ = get_das_gts_opt_info().get_specify_snapshot();
  }
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
                    das_gts_opt_info_,
                    plan_line_id_);

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
  int64_t mem_size = sizeof(transaction::ObTxReadSnapshot);
  if (OB_ISNULL(buf = alloc_.alloc(mem_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObTxReadSnapshot", K(ret), K(mem_size));
  } else if (OB_ISNULL(buf2 = alloc_.alloc(mem_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObTxReadSnapshot", K(ret), K(mem_size));
  } else {
    use_specify_snapshot_ = true;
    isolation_level_ = isolation_level;
    specify_snapshot_ = new(buf) transaction::ObTxReadSnapshot();
    response_snapshot_ = new(buf2) transaction::ObTxReadSnapshot();
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

int64_t ObDASTaskArg::get_estimated_serialize_size() const
{
  int64_t len = 0;
  if (task_ops_.count() != 0) {
    len = META_SERIALIZE_SIZE_ESTIMATE;
    for (int64_t i = 0; i < task_ops_.count(); i++) {
      len += task_ops_.at(i)->get_write_buffer_mem_used();
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
    if (OB_FAIL(get_agg_task()->move_to_success_tasks(this))) {
      LOG_WARN("failed to move task to success tasks", KR(ret));
    }
  } else if (task_status_ == ObDasTaskStatus::FAILED) {
    if (OB_FAIL(get_agg_task()->move_to_failed_tasks(this))) {
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


ObDASLookupBatchTask::ObDASLookupBatchTask()
  : tenant_id_(OB_INVALID_TENANT_ID),
    request_id_(OB_INVALID_ID),
    task_arg_(nullptr),
    estimate_failed_(false)
{
}

void ObDASLookupBatchTask::init(const uint64_t tenant_id,
                                const int64_t request_id,
                                const ObDASTaskArg &task_arg)
{
  tenant_id_ = tenant_id;
  request_id_ = request_id;
  task_arg_ = &task_arg;
  estimate_failed_ = false;
}

int ObDASLookupBatchTask::fill_buffer(char *buf, int64_t size, int64_t &filled_size) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int simulate_ret = OB_SUCCESS;
  if (OB_ISNULL(task_arg_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(serialization::encode(buf, size, pos, tenant_id_))) {
    LOG_WARN("encode tenant id failed", K(ret), K_(tenant_id));
  } else if (OB_FAIL(serialization::encode(buf, size, pos, request_id_))) {
    LOG_WARN("encode request id failed", K(ret), K_(request_id));
  // Inject size errors only once so the retry can fall back to exact
  // encoded_length() after estimate_failed_ is set.
  } else if (OB_UNLIKELY(OB_SUCCESS != (simulate_ret = EVENT_CALL(ERRSIM_LOOKUP_BATCH_FILL_BUFFER)))
             && ((OB_SIZE_OVERFLOW != simulate_ret && OB_BUF_NOT_ENOUGH != simulate_ret)
                 || !estimate_failed_)) {
    ret = simulate_ret;
    if (OB_SIZE_OVERFLOW == ret || OB_BUF_NOT_ENOUGH == ret) {
      estimate_failed_ = true;
    }
    LOG_WARN("simulate lookup batch fill buffer failed", K(ret), K_(tenant_id), K_(request_id));
  } else if (OB_FAIL(serialization::encode(buf, size, pos, *task_arg_))) {
    if (OB_SIZE_OVERFLOW == ret || OB_BUF_NOT_ENOUGH == ret) {
      estimate_failed_ = true;
    }
    LOG_WARN("encode task arg failed", K(ret), KPC_(task_arg));
  } else {
    filled_size = pos;
  }
  return ret;
}

int64_t ObDASLookupBatchTask::get_req_size() const
{
  int64_t len = serialization::encoded_length(tenant_id_);
  len += serialization::encoded_length(request_id_);
  if (OB_NOT_NULL(task_arg_)) {
    if (!estimate_failed_) {
      int64_t estimate = task_arg_->get_estimated_serialize_size();
      len += (estimate > 0) ? estimate : serialization::encoded_length(*task_arg_);
    } else {
      len += serialization::encoded_length(*task_arg_);
    }
  }
  return len;
}

int ObDASLookupBatchTask::peek_request_id(const char *buf, int64_t size,
                                          uint64_t &tenant_id, int64_t &request_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  tenant_id = OB_INVALID_TENANT_ID;
  request_id = OB_INVALID_ID;
  if (OB_ISNULL(buf) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode(buf, size, pos, tenant_id))) {
    LOG_WARN("peek tenant id failed", K(ret), K(size));
  } else if (OB_FAIL(serialization::decode(buf, size, pos, request_id))) {
    LOG_WARN("peek request id failed", K(ret), K(size), K(pos), K(tenant_id));
  }
  return ret;
}

ObDASLookupBatchResult::ObDASLookupBatchResult()
  : tenant_id_(OB_INVALID_TENANT_ID),
    request_id_(OB_INVALID_ID),
    task_resp_(nullptr)
{
}

void ObDASLookupBatchResult::init(const uint64_t tenant_id,
                                  const int64_t request_id,
                                  const ObDASTaskResp &task_resp)
{
  tenant_id_ = tenant_id;
  request_id_ = request_id;
  task_resp_ = &task_resp;
}

int ObDASLookupBatchResult::fill_buffer(char *buf, int64_t size, int64_t &filled_size) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(task_resp_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lookup batch task resp is null", K(ret), K_(tenant_id), K_(request_id));
  } else if (OB_FAIL(serialization::encode(buf, size, pos, tenant_id_))) {
    LOG_WARN("encode tenant id failed", K(ret), K_(tenant_id), K(size), K(pos));
  } else if (OB_FAIL(serialization::encode(buf, size, pos, request_id_))) {
    LOG_WARN("encode request id failed", K(ret), K_(request_id), K(size), K(pos));
  } else if (OB_FAIL(task_resp_->serialize(buf, size, pos))) {
    LOG_WARN("encode lookup batch task resp failed", K(ret), K(size), K(pos),
             K_(tenant_id), K_(request_id));
  } else {
    filled_size = pos;
  }
  return ret;
}

int64_t ObDASLookupBatchResult::get_req_size() const
{
  int64_t len = serialization::encoded_length(tenant_id_);
  len += serialization::encoded_length(request_id_);
  if (OB_NOT_NULL(task_resp_)) {
    len += task_resp_->get_serialize_size();
  }
  return len;
}

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
                    base_read_row_cnt_,  // FARM COMPAT WHITELIST
                    delta_read_row_cnt_, // FARM COMPAT WHITELIST
                    das_execute_remote_info_);

ObDASDataFetchRes::ObDASDataFetchRes()
        : datum_store_("DASDataFetch"),
          tenant_id_(0),
          task_id_(0),
          has_more_(false),
          enable_rich_format_(false),
          vec_row_store_(),
          io_read_bytes_(0),
          ssstore_read_bytes_(0),
          base_read_row_cnt_(0),
          delta_read_row_cnt_(0),
          das_execute_remote_info_()
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
        info.set_point_to_frame(true);
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
namespace obrpc
{
  int rpc_encode_req(ObRpcProxy & proxy, uint64_t gtid, ObRpcPacketCode pcode,
                     const ObDASTaskArg &args, const ObRpcOpts &opts,
                     char *&req, int64_t &req_sz, bool unneed_resp,
                     bool is_next = false, bool is_last = false,
                     int64_t session_id = 0) {
    ACTIVE_SESSION_FLAG_SETTER_GUARD(in_rpc_encode);
    int ret = common::OB_SUCCESS;
    ObRpcPacket pkt;
    const int64_t header_sz = pkt.get_header_size();
    int64_t extra_payload_size = calc_extra_payload_size();
    int64_t args_len = 0;
    int64_t payload_sz = 0;
    char *header_buf = NULL;
    char *payload_buf = NULL;
    int64_t pos = 0;
    bool fast_path_ok = false;
    UNIS_VERSION_GUARD(opts.unis_version_);

    // Fast path: try with fixed buffer size, skip encoded_length
    int64_t estimated_serialize_size = args.get_estimated_serialize_size();
    if (estimated_serialize_size < 16000) {
      // only small serialize_size can reach the fast path
      payload_sz = estimated_serialize_size;
      header_buf = (char *)pn_send_alloc(gtid, header_sz + payload_sz);
      if (NULL != header_buf) {
        payload_buf = header_buf + header_sz;
        int64_t encode_limit = payload_sz - extra_payload_size;
        ret =
            common::serialization::encode(payload_buf, encode_limit, pos, args);
        if (OB_SUCC(ret)) {
          args_len = pos;
          payload_sz = pos + extra_payload_size;
          fast_path_ok = true;
        } else if (common::OB_SIZE_OVERFLOW == ret || OB_BUF_NOT_ENOUGH == ret) {
          RPC_OBRPC_LOG(WARN, "rpc encode req fast path failed, fallback to slow path",
              K(estimated_serialize_size), K(extra_payload_size));
          pn_send_free(header_buf);
          header_buf = NULL;
          payload_buf = NULL;
          ret = common::OB_SUCCESS;
        }
      }
    }

    // Slow path: fallback to original logic with encoded_length
    if (OB_SUCC(ret) && !fast_path_ok) {
#ifdef ENABLE_SERIALIZATION_CHECK
      lib::begin_record_serialization();
      args_len = common::serialization::encoded_length(args);
      lib::finish_record_serialization();
#else
      args_len = common::serialization::encoded_length(args);
#endif
      payload_sz = extra_payload_size + args_len;
      if (payload_sz > get_max_rpc_packet_size()) {
        ret = common::OB_RPC_PACKET_TOO_LONG;
        RPC_OBRPC_LOG(ERROR, "obrpc packet payload execced its limit",
                      K(payload_sz), "limit", get_max_rpc_packet_size(),
                      K(ret));
      } else {
        header_buf = (char *)pn_send_alloc(gtid, header_sz + payload_sz);
        payload_buf = header_buf + header_sz;
      }
      pos = 0;
      if (OB_FAIL(ret)) {
        // OB_RPC_PACKET_TOO_LONG
      } else if (NULL == header_buf) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        RPC_OBRPC_LOG(WARN, "alloc buffer fail", K(payload_sz));
      } else if (OB_FAIL(common::serialization::encode(payload_buf, payload_sz,
                                                       pos, args))) {
        RPC_OBRPC_LOG(WARN, "serialize argument fail", K(pos), K(payload_sz),
                      K(ret));
      } else if (OB_UNLIKELY(args_len < pos)) {
#ifdef ENABLE_SERIALIZATION_CHECK
        lib::begin_check_serialization();
        common::serialization::encoded_length(args);
        lib::finish_check_serialization();
#endif
        ret = OB_ERR_UNEXPECTED;
        RPC_OBRPC_LOG(ERROR, "arg encoded length greater than arg length",
                      K(ret), K(payload_sz), K(args_len), K(extra_payload_size),
                      K(pos), K(pcode));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fill_extra_payload(pkt, payload_buf, payload_sz, pos))) {
      RPC_OBRPC_LOG(WARN, "fill extra payload fail", K(ret), K(pos),
                    K(payload_sz), K(args_len), K(extra_payload_size),
                    K(pcode));
    } else {
      const common::ObCompressorType &compressor_type =
          get_proxy_compressor_type(proxy);
      bool need_compressed =
          common::ObCompressorPool::get_instance().need_common_compress(
              compressor_type);
      if (need_compressed) {
        // compress
        EVENT_INC(RPC_COMPRESS_ORIGINAL_PACKET_CNT);
        EVENT_ADD(RPC_COMPRESS_ORIGINAL_SIZE, payload_sz);
        int tmp_ret = OB_SUCCESS;
        common::ObCompressor *compressor = NULL;
        char *compressed_buf = NULL;
        int64_t dst_data_size = 0;
        int64_t max_overflow_size = 0;
        if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor(
                compressor_type, compressor))) {
          RPC_OBRPC_LOG(WARN, "get_compressor failed", K(ret),
                        K(compressor_type));
        } else if (OB_FAIL(compressor->get_max_overflow_size(
                       payload_sz, max_overflow_size))) {
          RPC_OBRPC_LOG(WARN, "get_max_overflow_size failed", K(ret),
                        K(payload_sz), K(max_overflow_size));
        } else if (NULL ==
                   (compressed_buf = static_cast<char *>(common::ob_malloc(
                        payload_sz + max_overflow_size,
                        common::ObModIds::OB_RPC_PROCESSOR)))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          RPC_OBRPC_LOG(WARN, "Allocate memory failed", K(ret));
        } else if (OB_SUCCESS !=
                   (tmp_ret = compressor->compress(
                        payload_buf, payload_sz, compressed_buf,
                        payload_sz + max_overflow_size, dst_data_size))) {
          RPC_OBRPC_LOG(WARN, "compress failed", K(tmp_ret));
        } else if (dst_data_size >= payload_sz) {
        } else {
          RPC_OBRPC_LOG(DEBUG, "compress request success", K(compressor_type),
                        K(dst_data_size), K(payload_sz));
          // replace buf
          pkt.set_compressor_type(compressor_type);
          pkt.set_original_len(static_cast<int32_t>(payload_sz));
          memcpy(payload_buf, compressed_buf, dst_data_size);
          payload_sz = dst_data_size;
          EVENT_INC(RPC_COMPRESS_COMPRESSED_PACKET_CNT);
          EVENT_ADD(RPC_COMPRESS_COMPRESSED_SIZE, dst_data_size);
        }
        if (NULL != compressed_buf) {
          ob_free(compressed_buf);
          compressed_buf = NULL;
        }
      }
      int64_t header_pos = 0;
      pkt.set_content(payload_buf, payload_sz);
      if (OB_FAIL(init_packet(proxy, pkt, pcode, opts, unneed_resp))) {
        RPC_OBRPC_LOG(WARN, "init packet fail", K(ret));
      } else {
        if (is_next) {
          pkt.set_stream_next();
        }
        if (is_last) {
          pkt.set_stream_last();
        }
        if (session_id) {
          pkt.set_session_id(session_id);
        }
        if (OB_FAIL(pkt.encode_header(header_buf, header_sz, header_pos))) {
          RPC_OBRPC_LOG(WARN, "encode header fail", K(ret));
        } else {
          req = header_buf;
          req_sz = header_sz + payload_sz;
        }
      }
    }
    if (OB_FAIL(ret) && NULL != header_buf) {
      pn_send_free(header_buf);
    }
    return ret;
  }

} // namespace obrpc
}  // namespace oceanbase
