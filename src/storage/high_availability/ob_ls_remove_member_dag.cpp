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

#define USING_LOG_PREFIX STORAGE
#include "ob_ls_remove_member_dag.h"
#include "observer/ob_server.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "logservice/ob_log_service.h"
#include "lib/hash/ob_hashset.h"
#include "storage/high_availability/ob_storage_ha_utils.h"

using namespace oceanbase;
using namespace share;
using namespace storage;


ObLSRemoveMemberCtx::ObLSRemoveMemberCtx()
  : arg_(),
    start_ts_(0),
    finish_ts_(0),
    result_(OB_SUCCESS),
    storage_rpc_(nullptr)
{
}

bool ObLSRemoveMemberCtx::is_valid() const
{
  return arg_.is_valid() && OB_NOT_NULL(storage_rpc_);
}

void ObLSRemoveMemberCtx::reset()
{
  arg_.reset();
  start_ts_ = 0;
  finish_ts_ = 0;
  result_ = OB_SUCCESS;
  storage_rpc_ = nullptr;
}

ObLSRemoveMemberDagParam::ObLSRemoveMemberDagParam()
  : arg_(),
    storage_rpc_(nullptr)
{
}

bool ObLSRemoveMemberDagParam::is_valid() const
{
  return arg_.is_valid() && OB_NOT_NULL(storage_rpc_);
}

void ObLSRemoveMemberDagParam::reset()
{
  arg_.reset();
  storage_rpc_ = nullptr;
}

/******************ObLSRemoveMemberDag*********************/
ObLSRemoveMemberDag::ObLSRemoveMemberDag()
  : ObIDag(share::ObDagType::DAG_TYPE_REMOVE_MEMBER),
    is_inited_(false),
    ctx_()
{
}

ObLSRemoveMemberDag::~ObLSRemoveMemberDag()
{
}

int64_t ObLSRemoveMemberDag::to_string(char* buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls remove member dag do not init", K(ret));
  } else if (FALSE_IT(pos = ObIDag::to_string(buf, buf_len))) {
  } else if (pos >= buf_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dag to string buffer length is over limit", K(ret), K(pos), K(buf_len));
  } else if (FALSE_IT(pos = ctx_.to_string(buf + pos, buf_len))) {
  }
  return pos;
}

bool ObLSRemoveMemberDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (!is_inited_) {
    LOG_ERROR_RET(OB_NOT_INIT, "ls remove mmeber dag do not init");
    is_same = false;
  } else if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObLSRemoveMemberDag &other_ls_remove_member_dag = static_cast<const ObLSRemoveMemberDag &>(other);
    if (ctx_.arg_.ls_id_ != other_ls_remove_member_dag.ctx_.arg_.ls_id_) {
      is_same = false;
    }
  }
  return is_same;
}

int64_t ObLSRemoveMemberDag::hash() const
{
  int64_t hash_value = 0;
  if (!is_inited_) {
    LOG_ERROR_RET(OB_NOT_INIT, "ls remove member dag do not init");
  } else {
    hash_value = common::murmurhash(
        &ctx_.arg_.ls_id_, sizeof(ctx_.arg_.ls_id_), hash_value);
  }
  return hash_value;
}

int ObLSRemoveMemberDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls remove member dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  ctx_.arg_.ls_id_.id(),
                                  "remove_member", to_cstring(ctx_.arg_.remove_member_.get_server())))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObLSRemoveMemberDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls remove member dag do not init", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObLSRemoveMemberDag: ls_id = %s", to_cstring(ctx_.arg_.ls_id_)))) {
    LOG_WARN("failed to fill dag key", K(ret), K(ctx_));
  }
  return ret;
}

int ObLSRemoveMemberDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObLSRemoveMemberDagParam *remove_member_param = nullptr;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(param));
  } else if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input param is null", K(ret), K(param));
  } else if (FALSE_IT(remove_member_param = static_cast<const ObLSRemoveMemberDagParam *>(param))) {
  } else {
    ctx_.arg_ = remove_member_param->arg_;
    ctx_.storage_rpc_ = remove_member_param->storage_rpc_;
    ctx_.start_ts_ = ObTimeUtil::current_time();
    ctx_.finish_ts_ = 0;
    ctx_.result_ = OB_SUCCESS;
    is_inited_ = true;
  }
  return ret;
}

int ObLSRemoveMemberDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObLSRemoveMemberTask *remove_member_task = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls remove member dag is not init", K(ret));
  } else if (OB_FAIL(alloc_task(remove_member_task))) {
    STORAGE_LOG(WARN, "fail to alloc task", K(ret));
  } else if (OB_FAIL(remove_member_task->init())) {
    STORAGE_LOG(WARN, "failed to init remove member task", K(ret));
  } else if (OB_FAIL(add_task(*remove_member_task))) {
    STORAGE_LOG(WARN, "fail to add task", K(ret), K_(ctx));
  }
  return ret;
}

/******************ObLSRemoveMemberTask*********************/
ObLSRemoveMemberTask::ObLSRemoveMemberTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr)
{
}

ObLSRemoveMemberTask::~ObLSRemoveMemberTask()
{
}

int ObLSRemoveMemberTask::init()
{
  int ret = OB_SUCCESS;
  ObIDag *dag = nullptr;
  ObLSRemoveMemberDag *remove_member_dag = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls remove member task init twice", K(ret));
  } else if (FALSE_IT(dag = this->get_dag())) {
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be NULL", K(ret), KP(dag));
  } else if (ObDagType::DAG_TYPE_REMOVE_MEMBER != dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag type is unexpected", K(ret), KPC(dag));
  } else if (FALSE_IT(remove_member_dag = static_cast<ObLSRemoveMemberDag*>(dag))) {
  } else {
    ctx_ = remove_member_dag->get_ctx();
    is_inited_ = true;
    LOG_INFO("succeed init ls remove member task", "ls id", ctx_->arg_.ls_id_, "task id", ctx_->arg_.task_id_);
  }
  return ret;
}

int ObLSRemoveMemberTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls remove member task do not init", K(ret));
  } else {

    if (OB_FAIL(do_change_member_())) {
      LOG_WARN("failed to change member", K(ret), KPC(ctx_));
    }

    if (OB_SUCCESS != (tmp_ret = report_to_rs_())) {
      LOG_WARN("failed to report to rs", K(ret), KPC(ctx_));
    }

    ctx_->finish_ts_ = ObTimeUtil::current_time();

    LOG_INFO("finish remove memebr task", "ls_id", ctx_->arg_.ls_id_,
        "remove member", ctx_->arg_.remove_member_.get_server(), "cost", ctx_->finish_ts_ - ctx_->start_ts_);
  }
  return ret;
}

int ObLSRemoveMemberTask::do_change_member_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSService *ls_service = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls remove member task do not init", K(ret));
  } else {
    if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(ctx_->arg_.ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
      LOG_WARN("fail to get log stream", KR(ret), K(*ctx_));
    } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", KR(ret), K(*ctx_), KP(ls));
    } else {
      switch (ctx_->arg_.type_) {
      case ObLSChangeMemberType::LS_REMOVE_MEMBER: {
        if (OB_FAIL(remove_member_(ls))) {
          LOG_WARN("failed to do remove member", K(ret), KPC(ctx_));
        }
        break;
      }
      case ObLSChangeMemberType::LS_MODIFY_REPLICA_NUMBER : {
        if (OB_FAIL(modify_member_number_(ls))) {
          LOG_WARN("failed to modify member number", K(ret), KPC(ctx_));
        }
        break;
      }
      case ObLSChangeMemberType::LS_TRANSFORM_MEMBER : {
        if (OB_FAIL(transform_member_(ls))) {
          LOG_WARN("failed to transform member", K(ret), KPC(ctx_));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid type", K(ret), KPC(ctx_));
      }
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(ls)) {
      if (OB_SUCCESS != (tmp_ret = ls->report_replica_info())) {
        LOG_WARN("failed to report replica info", K(tmp_ret), KPC(ctx_));
      }
    }

    if (OB_FAIL(ret)) {
      ctx_->result_ = ret;
    }
  }
  return ret;
}

int ObLSRemoveMemberTask::remove_member_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  const int64_t change_member_list_timeout_us = GCONF.sys_bkgd_migration_change_member_list_timeout;
  if (!ctx_->arg_.type_.is_remove_member()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("remove member get invalid argument", K(ret), KPC(ctx_));
  } else {
    if (ctx_->arg_.is_paxos_member_) {
      if (OB_FAIL(ls->remove_member(ctx_->arg_.remove_member_, ctx_->arg_.new_paxos_replica_number_, change_member_list_timeout_us))) {
        LOG_WARN("failed to remove paxos member", K(ret), KPC(ctx_));
      }
    } else {
      if (OB_FAIL(ls->remove_learner(ctx_->arg_.remove_member_, change_member_list_timeout_us))) {
        LOG_WARN("failed to remove learner member", K(ret), KPC(ctx_));
      }
    }
  }
  return ret;
}

int ObLSRemoveMemberTask::modify_member_number_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  const int64_t change_member_list_timeout_us = GCONF.sys_bkgd_migration_change_member_list_timeout;
  if (!ctx_->arg_.type_.is_modify_replica_number()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("modify member number get invalid argument", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ls->change_replica_num(ctx_->arg_.member_list_, ctx_->arg_.orig_paxos_replica_number_,
      ctx_->arg_.new_paxos_replica_number_, change_member_list_timeout_us))) {
    LOG_WARN("failed to modify paxos replica number", KR(ret), KPC(ctx_));
  }
  return ret;
}

int ObLSRemoveMemberTask::transform_member_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  const int64_t change_member_list_timeout_us = GCONF.sys_bkgd_migration_change_member_list_timeout;
  const ObReplicaType &src_type = ctx_->arg_.src_.get_replica_type();
  const ObReplicaType &dest_type = ctx_->arg_.dest_.get_replica_type();
  palf::LogConfigVersion config_version;

  if (!ctx_->arg_.type_.is_transform_member()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transform member get invalid argument", K(ret), KPC(ctx_));
  } else {
    if (ObReplicaTypeCheck::is_full_replica(src_type) && ObReplicaTypeCheck::is_readonly_replica(dest_type)) {
      //F -> R
      if (OB_FAIL(ls->switch_acceptor_to_learner(ctx_->arg_.src_, ctx_->arg_.new_paxos_replica_number_, change_member_list_timeout_us))) {
        LOG_WARN("failed to switch acceptor to learner", KR(ret), KPC(ctx_));
      }
    } else if (ObReplicaTypeCheck::is_readonly_replica(src_type) && ObReplicaTypeCheck::is_full_replica(dest_type)) {
      if (OB_FAIL(switch_learner_to_acceptor_(ls))) {
        LOG_WARN("failed to switch learner to acceptor", KR(ret), KPC(ctx_));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica type is unexpected", K(ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObLSRemoveMemberTask::switch_learner_to_acceptor_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = GCONF.sys_bkgd_migration_change_member_list_timeout;
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be null", K(ret));
  } else if (OB_FAIL(ls->switch_learner_to_acceptor(ctx_->arg_.src_,
                                             ctx_->arg_.new_paxos_replica_number_,
                                             timeout))) {
    LOG_WARN("failed to switch learner to acceptor", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObLSRemoveMemberTask::report_to_rs_()
{
  int ret = OB_SUCCESS;
  obrpc::ObDRTaskReplyResult res;
  ObRsMgr *rs_mgr = GCTX.rs_mgr_;
  int64_t retry_count = 0;
  const int64_t MAX_RETRY_TIMES = 3;
  ObAddr rs_addr;
  const int64_t REPORT_RETRY_INTERVAL_MS = 100 * 1000; //100ms

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls remove member handler do not init", K(ret));
  } else if (OB_ISNULL(rs_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs mgr should not be NULL", K(ret), KP(rs_mgr));
  } else {
    res.task_id_ = ctx_->arg_.task_id_;
    res.tenant_id_ = ctx_->arg_.tenant_id_;
    res.ls_id_ = ctx_->arg_.ls_id_;
    res.result_ = ctx_->result_;
    while (retry_count++ < MAX_RETRY_TIMES) {
      if (OB_FAIL(rs_mgr->get_master_root_server(rs_addr))) {
        STORAGE_LOG(WARN, "get master root service failed", K(ret));
      } else if (OB_FAIL(ctx_->storage_rpc_->post_ls_disaster_recovery_res(rs_addr, res))) {
        LOG_WARN("failed to post ls diaster recovery res", K(ret), K(rs_addr), K(res));
      }
      if (OB_RS_NOT_MASTER != ret) {
        if (OB_SUCC(ret)) {
          STORAGE_LOG(INFO, "post ls remove member result success",
              K(rs_addr), KPC(ctx_));
        }
        break;
      } else if (OB_FAIL(rs_mgr->renew_master_rootserver())) {
        STORAGE_LOG(WARN, "renew master root service failed", K(ret));
      }

      if (OB_FAIL(ret)) {
        ob_usleep(REPORT_RETRY_INTERVAL_MS);
      }
    }
  }
  return ret;
}


