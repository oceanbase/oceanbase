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

#define USING_LOG_PREFIX SQL_EXE

#include "lib/utility/serialization.h"
#include "sql/executor/ob_task.h"
#include "sql/executor/ob_job.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_des_exec_context.h"
#include "sql/engine/px/ob_px_util.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/json/ob_yson.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObTask::ObTask()
    : exec_ctx_(NULL),
      ser_phy_plan_(NULL),
      des_phy_plan_(NULL),
      root_spec_(NULL),
      job_(NULL),
      runner_svr_(),
      ctrl_svr_(),
      ob_task_id_(),
      location_idx_(OB_INVALID_INDEX),
      max_sql_no_(-1)
{
}

ObTask::~ObTask()
{
}

int ObTask::load_code() {
  int ret = OB_SUCCESS;
  if (NULL == des_phy_plan_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("please deserialized plan first", K(ret));
  } else {
    // ret = des_phy_plan_->load_code();
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTask)
{
  int ret = OB_SUCCESS;
  if (OB_I(t1) OB_ISNULL(exec_ctx_) || OB_ISNULL(ser_phy_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not init",  K_(exec_ctx), K_(ser_phy_plan));
  } else if (ser_phy_plan_->is_dist_insert_or_replace_plan() && location_idx_ != OB_INVALID_INDEX) {
    OZ(exec_ctx_->reset_one_row_id_list(exec_ctx_->get_part_row_manager().get_row_id_list(location_idx_)));
  }
  LST_DO_CODE(OB_UNIS_ENCODE, *ser_phy_plan_);
  LST_DO_CODE(OB_UNIS_ENCODE, *exec_ctx_);
  LST_DO_CODE(OB_UNIS_ENCODE, ctrl_svr_);
  LST_DO_CODE(OB_UNIS_ENCODE, runner_svr_);
  LST_DO_CODE(OB_UNIS_ENCODE, ob_task_id_);

  // 序列化Task的核心部分到远端：sub plan tree
  if (OB_SUCC(ret)) {
    const ObExprFrameInfo *frame_info = &ser_phy_plan_->get_expr_frame_info();
    if (OB_ISNULL(root_spec_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: op root is null", K(ret));
    } else if (OB_FAIL(ObPxTreeSerializer::serialize_expr_frame_info(
        buf, buf_len, pos, *exec_ctx_, *const_cast<ObExprFrameInfo *>(frame_info)))) {
      LOG_WARN("failed to serialize rt expr", K(ret));
    } else if (OB_FAIL(ObPxTreeSerializer::serialize_tree(
                buf, buf_len, pos, *root_spec_, false /**is full tree*/, runner_svr_))) {
      LOG_WARN("fail serialize root_op", K(ret), K(buf_len), K(pos));
    } else if (OB_FAIL(ObPxTreeSerializer::serialize_op_input(
        buf, buf_len, pos, *root_spec_, exec_ctx_->get_kit_store(), false/*is full tree*/))) {
      LOG_WARN("failed to deserialize kit store", K(ret));
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE, ranges_);
  LST_DO_CODE(OB_UNIS_ENCODE, max_sql_no_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTask)
{
  int ret = OB_SUCCESS;
  if (OB_I(t1) OB_ISNULL(exec_ctx_) || OB_ISNULL(des_phy_plan_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not init", K(ret), K_(exec_ctx), K_(des_phy_plan));
  } else {
    root_spec_ = NULL;
  }

  LST_DO_CODE(OB_UNIS_DECODE, *des_phy_plan_);
  LST_DO_CODE(OB_UNIS_DECODE, *exec_ctx_);
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(exec_ctx_->get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else {
      exec_ctx_->get_my_session()->set_cur_phy_plan(des_phy_plan_);
    }
  }
  if (OB_SUCC(ret)) {
    // Compact mode may not set while rpc argument deserialize, set it manually.
    // See:
    lib::CompatModeGuard g(ORACLE_MODE == exec_ctx_->get_my_session()->get_compatibility_mode()
        ? lib::Worker::CompatMode::ORACLE
        : lib::Worker::CompatMode::MYSQL);

    LST_DO_CODE(OB_UNIS_DECODE, ctrl_svr_);
    LST_DO_CODE(OB_UNIS_DECODE, runner_svr_);
    LST_DO_CODE(OB_UNIS_DECODE, ob_task_id_);

    if (OB_SUCC(ret)) {
      const ObExprFrameInfo *frame_info = &des_phy_plan_->get_expr_frame_info();
      if (OB_FAIL(ObPxTreeSerializer::deserialize_expr_frame_info(
          buf, data_len, pos, *exec_ctx_, *const_cast<ObExprFrameInfo *>(frame_info)))) {
        LOG_WARN("failed to serialize rt expr", K(ret));
      } else if (OB_FAIL(ObPxTreeSerializer::deserialize_tree(
          buf, data_len, pos, *des_phy_plan_, root_spec_))) {
        LOG_WARN("fail deserialize tree", K(ret));
      } else if (OB_FAIL(root_spec_->create_op_input(*exec_ctx_))) {
        LOG_WARN("create operator from spec failed", K(ret));
      } else if (OB_FAIL(ObPxTreeSerializer::deserialize_op_input(
                         buf, data_len, pos, exec_ctx_->get_kit_store()))) {
        LOG_WARN("failed to deserialize kit store", K(ret));
      } else {
        des_phy_plan_->set_root_op_spec(root_spec_);
        exec_ctx_->reference_my_plan(des_phy_plan_);
      }
    }

    LST_DO_CODE(OB_UNIS_DECODE, ranges_);

    if (OB_FAIL(ret) && OB_NOT_NULL(exec_ctx_)) {
      ObDesExecContext *des_exec_ctx = static_cast<ObDesExecContext *>(exec_ctx_);
      des_exec_ctx->cleanup_session();
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE, max_sql_no_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTask)
{
  int64_t len = 0;
  if (OB_I(t1) OB_ISNULL(exec_ctx_) || OB_ISNULL(ser_phy_plan_)) {
    LOG_ERROR_RET(OB_NOT_INIT, "task not init", K_(exec_ctx), K_(ser_phy_plan));
  } else {
    if (ser_phy_plan_->is_dist_insert_or_replace_plan() && location_idx_ != OB_INVALID_INDEX) {
      exec_ctx_->reset_one_row_id_list(exec_ctx_->get_part_row_manager().get_row_id_list(location_idx_));
    }
    LST_DO_CODE(OB_UNIS_ADD_LEN, *ser_phy_plan_);
    LST_DO_CODE(OB_UNIS_ADD_LEN, *exec_ctx_);
    LST_DO_CODE(OB_UNIS_ADD_LEN, ctrl_svr_);
    LST_DO_CODE(OB_UNIS_ADD_LEN, runner_svr_);
    LST_DO_CODE(OB_UNIS_ADD_LEN, ob_task_id_);
    if (OB_ISNULL(root_spec_)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected status: op root is null");
    } else {
      const ObExprFrameInfo *frame_info = &ser_phy_plan_->get_expr_frame_info();
      len += ObPxTreeSerializer::get_serialize_expr_frame_info_size(*exec_ctx_,
                                    *const_cast<ObExprFrameInfo *>(frame_info));
      len += ObPxTreeSerializer::get_tree_serialize_size(*root_spec_, false/*is fulltree*/);
      len += ObPxTreeSerializer::get_serialize_op_input_size(
        *root_spec_, exec_ctx_->get_kit_store(),  false/*is fulltree*/);
    }
    LOG_TRACE("trace get ser rpc init sqc args size", K(len));
    LST_DO_CODE(OB_UNIS_ADD_LEN, ranges_);
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN, max_sql_no_);
  return len;
}

DEFINE_TO_YSON_KV(ObTask, OB_ID(task_id), ob_task_id_,
                          OB_ID(runner_svr), runner_svr_,
                          OB_ID(ctrl_svr), ctrl_svr_);

int ObTask::assign_ranges(const ObIArray<ObNewRange> &ranges)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("assign ranges to task", K(ranges));
  FOREACH_CNT_X(it, ranges, OB_SUCC(ret)) {
    if (OB_FAIL(ranges_.push_back(*it))) {
      LOG_WARN("push back range failed", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObMiniTask)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObMiniTask, ObTask));
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    bool has_extend_root_spec = (extend_root_spec_ != NULL);
    OB_UNIS_ENCODE(has_extend_root_spec);
    if (OB_SUCC(ret) && has_extend_root_spec) {
      if (OB_FAIL(ObPxTreeSerializer::serialize_tree(
                  buf, buf_len, pos, *extend_root_spec_ , false /**is full tree*/, runner_svr_))) {
        LOG_WARN("fail serialize root_op", K(ret), K(buf_len), K(pos));
      } else if (OB_FAIL(ObPxTreeSerializer::serialize_op_input(
          buf, buf_len, pos, *extend_root_spec_, exec_ctx_->get_kit_store(), false/*is full tree*/))) {
        LOG_WARN("failed to deserialize kit store", K(ret));
      }
    }
  }

  return ret;
}

OB_DEF_DESERIALIZE(ObMiniTask)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObMiniTask, ObTask));
  if (OB_FAIL(ret)) {
  } else {
    bool has_extend_spec = false;
    OB_UNIS_DECODE(has_extend_spec);
    if (OB_SUCC(ret) && has_extend_spec) {
      if (OB_ISNULL(exec_ctx_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else if (OB_FAIL(ObPxTreeSerializer::deserialize_tree(
          buf, data_len, pos, *des_phy_plan_, extend_root_spec_))) {
        LOG_WARN("fail deserialize tree", K(ret));
      } else if (OB_FAIL(extend_root_spec_->create_op_input(*exec_ctx_))) {
        LOG_WARN("create operator from spec failed", K(ret));
      } else if (OB_FAIL(ObPxTreeSerializer::deserialize_op_input(buf,
                          data_len, pos, exec_ctx_->get_kit_store()))) {
        LOG_WARN("failed to deserialize kit store", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObMiniTask)
{
  int64_t len = 0;
  bool has_extend_root_spec = (extend_root_spec_ != NULL);
  BASE_ADD_LEN((ObMiniTask, ObTask));
  OB_UNIS_ADD_LEN(has_extend_root_spec);
  if (has_extend_root_spec) {
    if (OB_ISNULL(extend_root_spec_) || OB_ISNULL(exec_ctx_)) {
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid argument", KP(extend_root_spec_), KP(exec_ctx_));
    } else {
      len += ObPxTreeSerializer::get_tree_serialize_size(*extend_root_spec_,
                                                         false/*is fulltree*/);
      len += ObPxTreeSerializer::get_serialize_op_input_size(
        *extend_root_spec_, exec_ctx_->get_kit_store(), false/*is fulltree*/);
    }
  }
  return len;
}

ObPingSqlTask::ObPingSqlTask()
  : trans_id_(),
    sql_no_(OB_INVALID_ID),
    task_id_(),
    exec_svr_(),
    cur_status_(0)
{}

ObPingSqlTask::~ObPingSqlTask()
{}

OB_SERIALIZE_MEMBER(ObPingSqlTask, trans_id_, sql_no_, task_id_, cur_status_);

ObPingSqlTaskResult::ObPingSqlTaskResult()
  : err_code_(OB_SUCCESS),
    ret_status_(0)
{}

ObPingSqlTaskResult::~ObPingSqlTaskResult()
{}

OB_SERIALIZE_MEMBER(ObPingSqlTaskResult, err_code_, ret_status_);

OB_DEF_SERIALIZE(ObRemoteTask)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = OB_INVALID_ID;
  ParamStore *ps_params = nullptr;
  //for serialize ObObjParam' param_meta_
  int64_t param_meta_count = 0;
  if (OB_ISNULL(remote_sql_info_)
      || OB_ISNULL(session_info_)
      || OB_ISNULL(ps_params = remote_sql_info_->ps_params_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("remote task not init", K(ret), K_(remote_sql_info), K_(session_info), K(ps_params));
  } else {
    tenant_id = session_info_->get_effective_tenant_id();
    param_meta_count = ps_params->count();
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_schema_version_,
              sys_schema_version_,
              runner_svr_,
              ctrl_svr_,
              task_id_,
              remote_sql_info_->use_ps_,
              remote_sql_info_->remote_sql_,
              *ps_params,
              tenant_id,
              *session_info_,
              remote_sql_info_->is_batched_stmt_,
              dependency_tables_,
              snapshot_);
  OB_UNIS_ENCODE(param_meta_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < param_meta_count; ++i) {
    OB_UNIS_ENCODE(ps_params->at(i).get_param_meta());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_meta_count; ++i) {
    OB_UNIS_ENCODE(ps_params->at(i).get_param_flag());
  }
  OB_UNIS_ENCODE(remote_sql_info_->is_original_ps_mode_);
  OB_UNIS_ENCODE(remote_sql_info_->sql_from_pl_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRemoteTask)
{
  int64_t len = 0;
  ParamStore *ps_params = nullptr;
  int64_t param_meta_count = 0;
  if (OB_ISNULL(remote_sql_info_)
      || OB_ISNULL(session_info_)
      || OB_ISNULL(ps_params = remote_sql_info_->ps_params_)) {
    LOG_WARN_RET(OB_NOT_INIT, "remote task not init", K_(remote_sql_info), K_(session_info), K(ps_params));
  } else {
    int64_t tenant_id = session_info_->get_effective_tenant_id();
    LST_DO_CODE(OB_UNIS_ADD_LEN,
                tenant_schema_version_,
                sys_schema_version_,
                runner_svr_,
                ctrl_svr_,
                task_id_,
                remote_sql_info_->use_ps_,
                remote_sql_info_->remote_sql_,
                *ps_params,
                tenant_id,
                *session_info_,
                remote_sql_info_->is_batched_stmt_,
                dependency_tables_,
                snapshot_);
    param_meta_count = ps_params->count();
    OB_UNIS_ADD_LEN(param_meta_count);
    for (int64_t i = 0; i < param_meta_count; ++i) {
      OB_UNIS_ADD_LEN(ps_params->at(i).get_param_meta());
    }
    for (int64_t i = 0; i < param_meta_count; ++i) {
      OB_UNIS_ADD_LEN(ps_params->at(i).get_param_flag());
    }
    OB_UNIS_ADD_LEN(remote_sql_info_->is_original_ps_mode_);
    OB_UNIS_ADD_LEN(remote_sql_info_->sql_from_pl_);
  }
  return len;
}

OB_DEF_DESERIALIZE(ObRemoteTask)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = OB_INVALID_ID;
  ParamStore *ps_params = nullptr;
  ObObjMeta tmp_meta;
  ParamFlag tmp_flag;
  int64_t param_meta_count = 0;
  if (OB_ISNULL(remote_sql_info_)
      || OB_ISNULL(ps_params = remote_sql_info_->ps_params_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("remote task not init", K(ret), K_(remote_sql_info), K(ps_params));
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_schema_version_,
              sys_schema_version_,
              runner_svr_,
              ctrl_svr_,
              task_id_,
              remote_sql_info_->use_ps_,
              remote_sql_info_->remote_sql_,
              *ps_params,
              tenant_id);
  if (OB_SUCC(ret)) {
    //后续创建session需要依赖tenant_id
    remote_sql_info_->ps_param_cnt_ = static_cast<int32_t>(ps_params->count());
    if (OB_FAIL(exec_ctx_->create_my_session(tenant_id))) {
      LOG_WARN("create my session failed", K(ret), K(tenant_id));
    } else {
      session_info_ = exec_ctx_->get_my_session();
      ObSQLSessionInfo::LockGuard query_guard(session_info_->get_query_lock());
      ObSQLSessionInfo::LockGuard data_guard(session_info_->get_thread_data_lock());
      OB_UNIS_DECODE(*session_info_);
      OB_UNIS_DECODE(remote_sql_info_->is_batched_stmt_);
    }
    dependency_tables_.set_allocator(&(exec_ctx_->get_allocator()));
    OB_UNIS_DECODE(dependency_tables_);
    OB_UNIS_DECODE(snapshot_);
    exec_ctx_->get_das_ctx().set_snapshot(snapshot_);
    //DESERIALIZE param_meta_count if 0, (1) params->count() ==0 (2) old version -> new version
    //for (2) just set obj.meta as param_meta
    OB_UNIS_DECODE(param_meta_count);
    if (OB_SUCC(ret)) {
      if (param_meta_count > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < param_meta_count; ++i) {
          OB_UNIS_DECODE(tmp_meta);
          ps_params->at(i).set_param_meta(tmp_meta);
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < param_meta_count; ++i) {
          OB_UNIS_DECODE(tmp_flag);
          ps_params->at(i).set_param_flag(tmp_flag);
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ps_params->count(); ++i) {
          ps_params->at(i).set_param_meta();
        }
      }
    }
    OB_UNIS_DECODE(remote_sql_info_->is_original_ps_mode_);
    OB_UNIS_DECODE(remote_sql_info_->sql_from_pl_);
  }
  return ret;
}

int ObRemoteTask::assign_dependency_tables(const DependenyTableStore &dependency_tables) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(dependency_tables_.assign(dependency_tables))) {
    LOG_WARN("failed to assign file list", K(ret));
  }
  return ret;
}

DEFINE_TO_YSON_KV(ObRemoteTask, OB_ID(task_id), task_id_,
                                OB_ID(runner_svr), runner_svr_,
                                OB_ID(ctrl_svr), ctrl_svr_);

}/* ns sql*/
}/* ns oceanbase */
