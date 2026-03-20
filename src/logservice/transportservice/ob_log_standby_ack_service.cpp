/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX CLOG

#include "ob_log_standby_ack_service.h"
#include "logservice/ob_log_handler.h"
#include "logservice/ob_log_service.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_define.h"
#include "share/ob_server_struct.h"
#include "share/restore/ob_log_restore_source_mgr.h"
#include "share/backup/ob_log_restore_config.h"
#include "logservice/palf/palf_handle.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/rc/ob_tenant_base.h"  // MTL_GET_TENANT_ROLE_CACHE
#include "share/ob_tenant_role.h"     // ObTenantRole
#include "share/ob_share_util.h"      // ObShareUtil

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace share;
using namespace lib;

namespace logservice
{

// 备库 ACK 回调，用于发送 ACK 后不需要处理响应
class StandbyAckCallback : public obrpc::ObLogTransportRpcProxy::AsyncCB<obrpc::OB_LOG_SYNC_STANDBY_INFO>
{
public:
  void set_args(const typename obrpc::ObLogTransportRpcProxy::AsyncCB<obrpc::OB_LOG_SYNC_STANDBY_INFO>::Request &args) {
    UNUSED(args);
  }

  rpc::frame::ObReqTransport::AsyncCB *clone(const rpc::frame::SPAlloc &alloc) const {
    void *buf = alloc(sizeof(*this));
    StandbyAckCallback *newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) StandbyAckCallback();
    }
    return newcb;
  }

  int process() override { return OB_SUCCESS; }
  void on_timeout() override {}
};

//---------------ObStandbyFsCb---------------//
ObStandbyFsCb::ObStandbyFsCb()
  : is_inited_(false),
    ls_id_(),
    rpc_proxy_(nullptr),
    ack_service_(nullptr)
{
}

ObStandbyFsCb::~ObStandbyFsCb()
{
  destroy();
}

int ObStandbyFsCb::init(const share::ObLSID &ls_id, obrpc::ObLogTransportRpcProxy *rpc_proxy,
  StandbyAckStatus *ack_status)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObStandbyFsCb already inited", K(ret));
  } else if (!ls_id.is_valid() || OB_ISNULL(rpc_proxy) || OB_ISNULL(ack_status)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(ls_id), KP(rpc_proxy), KP(ack_status));
  } else {
    ls_id_ = ls_id;
    rpc_proxy_ = rpc_proxy;
    ack_status_ = ack_status;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObStandbyFsCb init success", K(ls_id));
  }
  return ret;
}

void ObStandbyFsCb::destroy()
{
  is_inited_ = false;
  ls_id_.reset();
  rpc_proxy_ = nullptr;
  ack_service_ = nullptr;
  ack_status_ = nullptr;
}

int ObStandbyFsCb::update_end_lsn(int64_t id,
                                  const palf::LSN &end_lsn,
                                  const share::SCN &end_scn,
                                  const int64_t proposal_id)
{
  UNUSED(id);
  UNUSED(proposal_id);
  palf::AccessMode access_mode;

  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObStandbyFsCb not inited", K(ret));
  } else if (OB_ISNULL(ack_service_) || OB_ISNULL(ack_status_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "ack_service_ or ack_status_ is null", K(ret), K(ls_id_), KP(ack_service_), KP(ack_status_));
  } else if (OB_FAIL(ack_status_->get_access_mode(access_mode))) {
    CLOG_LOG(WARN, "get_access_mode failed", K(ret), K(ls_id_));
  } else if (access_mode == palf::AccessMode::APPEND) {
    CLOG_LOG(TRACE, "access mode is not raw write, skip sending ack", K(ls_id_), K(end_lsn), K(end_scn));
    return OB_SUCCESS;
  } else {
    bool is_sync_mode = false;
    if (OB_FAIL(ack_service_->get_sync_mode_from_cache(ls_id_, is_sync_mode))) {
      // 如果缓存不存在，默认提交任务，让独立线程去检查
      CLOG_LOG(TRACE, "get_sync_mode_from_cache_ failed, will check in ack thread", K(ret), K(ls_id_));
      is_sync_mode = true;
    }

    if (is_sync_mode) {
      // 创建任务并提交到独立线程处理
      StandbyAckTask task(ls_id_);
      if (OB_FAIL(ack_status_->update_current_end_lsn_scn(end_lsn, end_scn))) {
        CLOG_LOG(WARN, "update_current_end_lsn_scn failed", K(ret), K(ls_id_), K(end_lsn), K(end_scn));
      } else if (OB_FAIL(ack_status_->get_access_mode(access_mode))) {
        CLOG_LOG(WARN, "get_access_mode failed after update lsn", K(ret), K(ls_id_));
      } else if (palf::AccessMode::APPEND == access_mode) {
        CLOG_LOG(INFO, "access mode changed to APPEND after update lsn, skip sending ack",
                 K(ls_id_), K(end_lsn), K(end_scn));
      } else if (OB_FAIL(ack_service_->push_ack_task(task))) {
        CLOG_LOG(WARN, "push_ack_task failed", K(ret), K(ls_id_));
      } else {
        CLOG_LOG(TRACE, "ObStandbyFsCb push ack task success", K(ls_id_));
      }
    } else {
      // 非强同步模式，不需要发送
      CLOG_LOG(TRACE, "not in sync mode, skip sending ack", K(ls_id_), K(end_lsn), K(end_scn));
    }
  }
  return ret;
}

//---------------StandbyAckStatus---------------//
StandbyAckStatus::StandbyAckStatus()
  : is_inited_(false),
    is_in_stop_state_(false),
    ls_id_(),
    ack_service_(nullptr),
    palf_env_(nullptr),
    palf_handle_(nullptr),
    fs_cb_(),
    ref_cnt_(0)
{
}

StandbyAckStatus::~StandbyAckStatus()
{
  destroy();
}

int StandbyAckStatus::init(const share::ObLSID &ls_id,
                           ipalf::IPalfEnv *palf_env,
                           ObLogStandbyAckService *ack_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "StandbyAckStatus already inited", K(ret));
  } else if (!ls_id.is_valid() || OB_ISNULL(palf_env) || OB_ISNULL(ack_service)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(ls_id), KP(palf_env), KP(ack_service));
  } else if (OB_FAIL(palf_env->open(ls_id.id(), palf_handle_))) {
    CLOG_LOG(WARN, "open palf failed", K(ret), K(ls_id));
  } else {
    ls_id_ = ls_id;
    palf_env_ = palf_env;
    ack_service_ = ack_service;
    committed_end_lsn_.reset();
    committed_end_scn_.reset();
    // 初始化 sync_mode 缓存
    if (OB_FAIL(ack_service_->init_sync_mode_cache(ls_id))) {
      CLOG_LOG(WARN, "init_sync_mode_cache_ failed", K(ret), K(ls_id));
      // 初始化缓存失败不影响主流程，继续执行
    }

    // 注册 file_size_cb
    if (OB_SUCC(ret) && OB_FAIL(register_file_size_cb())) {
      CLOG_LOG(WARN, "register_file_size_cb failed", K(ret), K(ls_id));
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
      CLOG_LOG(INFO, "StandbyAckStatus init success", K(ls_id));
    }
  }

  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    if (OB_NOT_NULL(palf_env_) && OB_NOT_NULL(palf_handle_) && palf_handle_->is_valid()) {
      palf_env_->close(palf_handle_);
    }
    palf_handle_ = nullptr;
  }

  return ret;
}

void StandbyAckStatus::destroy()
{
  if (is_inited_) {
    stop();
    if (OB_NOT_NULL(palf_env_) && OB_NOT_NULL(palf_handle_) && palf_handle_->is_valid()) {
      palf_env_->close(palf_handle_);
    }
    palf_handle_ = nullptr;
    palf_env_ = nullptr;
    ack_service_ = nullptr;
    ls_id_.reset();
    is_inited_ = false;
  }
}

int StandbyAckStatus::stop()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    is_in_stop_state_ = true;
    if (OB_FAIL(unregister_file_size_cb())) {
      CLOG_LOG(WARN, "unregister_file_size_cb failed", K(ret), K(ls_id_));
    }
  }
  return ret;
}

int StandbyAckStatus::register_file_size_cb()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(palf_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "palf_handle_ is null", K(ret));
  } else {
    obrpc::ObLogTransportRpcProxy *rpc_proxy = ack_service_->get_rpc_proxy();
    if (OB_ISNULL(rpc_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "rpc_proxy is null", K(ret));
    } else if (OB_FAIL(fs_cb_.init(ls_id_, rpc_proxy, this))) {
      CLOG_LOG(WARN, "fs_cb_ init failed", K(ret), K(ls_id_));
    } else {
      fs_cb_.set_ack_service(ack_service_);
    //  palf::PalfHandle *palf_handle = static_cast<palf::PalfHandle*>(palf_handle_);
      if (OB_ISNULL(palf_handle_)) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "palf_handle is null", K(ret));
      } else if (OB_FAIL(palf_handle_->register_file_size_cb(&fs_cb_))) {
        CLOG_LOG(WARN, "register_file_size_cb failed", K(ret), K(ls_id_));
      } else {
        CLOG_LOG(INFO, "register_file_size_cb success", K(ls_id_));
      }
    }
  }
  return ret;
}

int StandbyAckStatus::unregister_file_size_cb()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "StandbyAckStatus not inited", K(ret));
  } else if (OB_ISNULL(palf_handle_)) {
  } else {
    if (OB_FAIL(palf_handle_->unregister_file_size_cb())) {
      CLOG_LOG(WARN, "unregister_file_size_cb failed", K(ret), K(ls_id_));
    } else {
      fs_cb_.destroy();
      CLOG_LOG(INFO, "unregister_file_size_cb success", K(ls_id_));
    }
  }
  return ret;
}

//---------------ObLogStandbyAckService---------------//
ObLogStandbyAckService::ObLogStandbyAckService()
  : is_inited_(false),
    is_running_(false),
    ack_task_queue_(),
    timer_tg_id_(-1),
    palf_env_(nullptr),
    ack_status_map_(),
    rpc_proxy_(nullptr),
    periodic_ack_task_(),
    cache_lock_(common::ObLatchIds::TRANSPORT_SERVICE_TASK_LOCK),
    primary_info_map_(),
    sync_mode_map_()
{
  periodic_ack_task_.set_ack_service(this);
}

ObLogStandbyAckService::~ObLogStandbyAckService()
{
  destroy();
}

int ObLogStandbyAckService::init(ipalf::IPalfEnv *palf_env, rpc::frame::ObReqTransport *transport)
{
  int ret = OB_SUCCESS;
  const uint64_t MAP_TENANT_ID = MTL_ID();
  const int64_t ACK_QUEUE_THREAD_NUM = 1;  // ACK 队列使用 1 个线程
  const int64_t ACK_QUEUE_SIZE = OB_MAX_LS_NUM_PER_TENANT_PER_SERVER_CAN_BE_SET;    // 队列大小

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogStandbyAckService init twice", K(ret));
  } else if (OB_ISNULL(palf_env_ = palf_env)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(palf_env));
  } else if (OB_FAIL(ack_task_queue_.init(this, ACK_QUEUE_THREAD_NUM, ACK_QUEUE_SIZE,
                                          "StandbyAckSrv", MAP_TENANT_ID))) {
    CLOG_LOG(WARN, "fail to init ack task queue", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::StandbyAckTimer, timer_tg_id_))) {
    CLOG_LOG(WARN, "fail to create timer thread group", K(ret));
  } else if (OB_FAIL(ack_status_map_.init("SYNC_ACK_STATUS", MAP_TENANT_ID))) {
    CLOG_LOG(WARN, "ack_status_map_ init error", K(ret));
  } else if (OB_FAIL(primary_info_map_.init("SYNC_PRI_INFO", MAP_TENANT_ID))) {
    CLOG_LOG(WARN, "primary_info_map_ init error", K(ret));
  } else if (OB_FAIL(sync_mode_map_.init("SYNC_MODE_MAP", MAP_TENANT_ID))) {
    CLOG_LOG(WARN, "sync_mode_map_ init error", K(ret));
  } else {
    // 初始化RPC代理
    rpc_proxy_ = OB_NEW(obrpc::ObLogTransportRpcProxy, "SyncAckRpcProxy");
    if (OB_ISNULL(rpc_proxy_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(ERROR, "allocate ObLogTransportRpcProxy failed", K(ret));
    } else if (OB_FAIL(rpc_proxy_->init(transport))) {
      CLOG_LOG(ERROR, "init ObLogTransportRpcProxy failed", K(ret), KP(transport));
      OB_DELETE(ObLogTransportRpcProxy, "SyncAckRpcProxy", rpc_proxy_);
      rpc_proxy_ = nullptr;
    } else {
      is_inited_ = true;
      CLOG_LOG(INFO, "ObLogStandbyAckService init success");
    }
  }

  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
    CLOG_LOG(WARN, "ObLogStandbyAckService init failed", K(ret));
  }
  return ret;
}

int ObLogStandbyAckService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogStandbyAckService not init", KR(ret));
  } else if (OB_FAIL(TG_START(timer_tg_id_))) {
     CLOG_LOG(ERROR, "start timer thread group failed", K(ret));
   } else if (OB_FAIL(TG_SCHEDULE(timer_tg_id_, periodic_ack_task_, PERIODIC_ACK_INTERVAL_US, true))) {
     CLOG_LOG(ERROR, "schedule periodic ack task failed", K(ret));
   } else {
     ATOMIC_STORE(&is_running_, true);
     CLOG_LOG(INFO, "start ObLogStandbyAckService success", K(ret), K(timer_tg_id_));
   }
   return ret;
}

void ObLogStandbyAckService::stop()
{
  CLOG_LOG(INFO, "ObLogStandbyAckService stop begin");
  ATOMIC_STORE(&is_running_, false);
  if (-1 != timer_tg_id_) {
    TG_STOP(timer_tg_id_);
  }
  ack_task_queue_.stop();
  CLOG_LOG(INFO, "stop ObLogStandbyAckService finish");
}

void ObLogStandbyAckService::wait()
{
  CLOG_LOG(INFO, "ObLogStandbyAckService wait begin");
  if (-1 != timer_tg_id_) {
    TG_STOP(timer_tg_id_);
    TG_WAIT(timer_tg_id_);
  }
  ack_task_queue_.stop();
  ack_task_queue_.wait();
  CLOG_LOG(INFO, "ObLogStandbyAckService wait finish");
}

void ObLogStandbyAckService::destroy()
{
  (void)remove_all_ls();
  is_inited_ = false;
  is_running_ = false;
  palf_env_ = nullptr;
  ack_status_map_.destroy();
  primary_info_map_.destroy();
  sync_mode_map_.destroy();
  if (nullptr != rpc_proxy_) {
    rpc_proxy_->destroy();
    OB_DELETE(ObLogTransportRpcProxy, "StandbyAckRpcProxy", rpc_proxy_);
    rpc_proxy_ = nullptr;
  }
  if (-1 != timer_tg_id_) {
    TG_DESTROY(timer_tg_id_);
    timer_tg_id_ = -1;
  }
  ack_task_queue_.destroy();
  CLOG_LOG(INFO, "ObLogStandbyAckService destroy finished");
}

int ObLogStandbyAckService::add_ls(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  StandbyAckStatus *ack_status = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogStandbyAckService not init", K(ret));
  } else if (!id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(id));
  } else {
    void *buf = mtl_malloc(sizeof(StandbyAckStatus), "SyncAckStatus");
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      ack_status = new(buf) StandbyAckStatus();
      if (OB_FAIL(ack_status->init(id, palf_env_, this))) {
        CLOG_LOG(WARN, "ack_status init failed", K(ret), K(id));
        ack_status->~StandbyAckStatus();
        mtl_free(ack_status);
        ack_status = nullptr;
      } else {
        ack_status->inc_ref();
        if (OB_FAIL(ack_status_map_.insert(id, ack_status))) {
          CLOG_LOG(WARN, "ack_status_map_ insert failed", K(ret), K(id));
          revert_ack_status_(ack_status);
          ack_status = nullptr;
        } else {
          CLOG_LOG(INFO, "ObLogStandbyAckService add_ls success", K(id));
        }
      }
    }
  }
  return ret;
}

int ObLogStandbyAckService::remove_ls(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  StandbyAckStatus *ack_status = nullptr;
  RemoveStandbyStatusFunctor functor;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogStandbyAckService not init", K(ret));
  } else if (OB_FAIL(ack_status_map_.get(id, ack_status))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      CLOG_LOG(INFO, "ls not exist in ack_status_map_", K(id));
    } else {
      CLOG_LOG(WARN, "ack_status_map_ get failed", K(ret), K(id));
    }
  } else if (OB_FAIL(unregister_file_size_cb_(id))) {
    CLOG_LOG(WARN, "unregister_file_size_cb_ failed", K(ret), K(id));
  } else if (OB_FAIL(ack_status_map_.erase_if(id, functor))) {
    CLOG_LOG(WARN, "ack_status_map_ erase failed", K(ret), K(id));
  } else {
    // 清理 sync_mode 缓存
    {
      common::ObSpinLockGuard guard(cache_lock_);
      (void)sync_mode_map_.erase(id);
    }

    CLOG_LOG(INFO, "remove_ls success", K(id));
  }

  return ret;
}

int ObLogStandbyAckService::remove_all_ls()
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;
  RemoveStandbyStatusFunctor functor;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogStandbyAckService not init", KR(ret));
  } else if (OB_FAIL(ack_status_map_.for_each(functor))) {
    CLOG_LOG(WARN, "failed to remove ack status", KR(ret));
  }
  return ret;
}

bool ObLogStandbyAckService::RemoveStandbyStatusFunctor::operator()(const share::ObLSID &id,
                                                                    StandbyAckStatus *ack_status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ack_status)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "ack_status is NULL", K(id), KR(ret));
  } else if (OB_SUCC(ret) && 0 == ack_status->dec_ref()) {
    CLOG_LOG(INFO, "free ack status", KPC(ack_status));
    ack_status->~StandbyAckStatus();
    mtl_free(ack_status);
  }
  ret_code_ = ret;
  return OB_SUCCESS == ret;
}

obrpc::ObLogTransportRpcProxy *ObLogStandbyAckService::get_rpc_proxy()
{
  return rpc_proxy_;
}

int ObLogStandbyAckService::revert_ack_status_(StandbyAckStatus *ack_status)
{
  int ret = OB_SUCCESS;
  if (NULL != ack_status) {
    if (0 == ack_status->dec_ref()) {
      CLOG_LOG(INFO, "free ack status", KPC(ack_status));
      ack_status->~StandbyAckStatus();
      mtl_free(ack_status);
    }
  }
  return ret;
}

int ObLogStandbyAckService::register_file_size_cb_(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  StandbyAckStatus *ack_status = nullptr;

  if (OB_FAIL(ack_status_map_.get(id, ack_status))) {
    CLOG_LOG(WARN, "ack_status_map_ get failed", K(ret), K(id));
  } else if (OB_FAIL(ack_status->register_file_size_cb())) {
    CLOG_LOG(WARN, "register_file_size_cb failed", K(ret), K(id));
  }

  return ret;
}

int ObLogStandbyAckService::unregister_file_size_cb_(const share::ObLSID &id)
{
  int ret = OB_SUCCESS;
  StandbyAckStatus *ack_status = nullptr;

  if (OB_FAIL(ack_status_map_.get(id, ack_status))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      CLOG_LOG(WARN, "ack_status_map_ get failed", K(ret), K(id));
    }
  } else if (OB_FAIL(ack_status->unregister_file_size_cb())) {
    CLOG_LOG(WARN, "unregister_file_size_cb failed", K(ret), K(id));
  }

  return ret;
}

int ObLogStandbyAckService::push_ack_task(const StandbyAckTask &task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogStandbyAckService not init", K(ret));
  } else if (!ATOMIC_LOAD(&is_running_)) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogStandbyAckService not running", K(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid task", K(ret), K(task));
  } else if (OB_FAIL(ack_task_queue_.add(task))) {
    if (OB_EAGAIN == ret) {
      // 任务已存在，这是正常情况，返回 OB_SUCCESS
      ret = OB_SUCCESS;
      CLOG_LOG(TRACE, "same task already exists in queue", K(task));
    } else if (OB_SIZE_OVERFLOW == ret) {
      CLOG_LOG(ERROR, "task queue is full", KR(ret), K(task));
    } else {
      CLOG_LOG(WARN, "add task to queue failed", KR(ret), K(task));
    }
  } else {
    CLOG_LOG(TRACE, "push ack task success", K(task));
  }
  return ret;
}

int ObLogStandbyAckService::get_current_end_lsn_scn_(const share::ObLSID &id, palf::LSN &lsn, share::SCN &scn)
{
  int ret = OB_SUCCESS;
  struct GetCurrentEndLsnScnFunctor
  {
    GetCurrentEndLsnScnFunctor(palf::LSN &lsn, share::SCN &scn) : lsn_(lsn), scn_(scn) {}
    bool operator()(const share::ObLSID &id, StandbyAckStatus *ack_status)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(ack_status)) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "ack_status is null", KR(ret), K(id));
      } else if (OB_FAIL(ack_status->get_current_end_lsn_scn(lsn_, scn_))) {
        CLOG_LOG(WARN, "get_current_end_lsn_scn failed", KR(ret), K(id));
      }
      return OB_SUCC(ret);
    }
    private:
      palf::LSN &lsn_;
      share::SCN &scn_;
  };
  lsn.reset();
  scn.reset();
  GetCurrentEndLsnScnFunctor functor(lsn, scn);
  if (!id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(id));
  } else if (OB_FAIL(ack_status_map_.operate(id, functor))) {
    CLOG_LOG(WARN, "for_each ack_status_map_ failed", K(ret));
  }
  return ret;
}

int ObLogStandbyAckService::batch_process_tasks(const common::ObIArray<StandbyAckTask> &tasks, bool &stopped)
{
  int ret = OB_SUCCESS;
  UNUSED(stopped);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogStandbyAckService not init", K(ret));
  } else if (tasks.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected task count", K(ret), "task_count", tasks.count());
  } else {
    struct GetAccessModeFunctor
    {
      explicit GetAccessModeFunctor(palf::AccessMode &access_mode) : access_mode_(access_mode) {}
      bool operator()(const share::ObLSID &id, StandbyAckStatus *ack_status)
      {
        int ret = OB_SUCCESS;
        if (OB_ISNULL(ack_status)) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "ack_status is null", KR(ret), K(id));
        } else if (OB_FAIL(ack_status->get_access_mode(access_mode_))) {
          CLOG_LOG(WARN, "get_access_mode failed", KR(ret), K(id));
        }
        return OB_SUCC(ret);
      }
    private:
      palf::AccessMode &access_mode_;
    };

    const StandbyAckTask &task = tasks.at(0);
    const uint64_t tenant_id = MTL_ID();
    const share::ObLSID ls_id = task.get_ls_id();
    palf::LSN end_lsn;
    share::SCN end_scn;
    palf::AccessMode access_mode;
    GetAccessModeFunctor get_access_mode_functor(access_mode);

    if (OB_FAIL(ack_status_map_.operate(ls_id, get_access_mode_functor))) {
      CLOG_LOG(WARN, "get access_mode from ack_status_map failed", K(ret), K(ls_id));
    } else if (palf::AccessMode::APPEND == access_mode) {
      CLOG_LOG(TRACE, "access mode is append, skip sending ack", K(ls_id), K(access_mode));
    } else if (OB_FAIL(get_current_end_lsn_scn_(ls_id, end_lsn, end_scn))) {
      CLOG_LOG(WARN, "get_current_end_lsn_scn failed", K(ret), K(ls_id));
    } else if (!end_lsn.is_valid() || !end_scn.is_valid()) {
      if (REACH_TIME_INTERVAL(1_s)) {
        CLOG_LOG(INFO, "end_lsn or end_scn is invalid, skip sending ack", K(ls_id), K(end_lsn), K(end_scn));
      } else {
        CLOG_LOG(TRACE, "end_lsn or end_scn is invalid, skip sending ack", K(ls_id), K(end_lsn), K(end_scn));
      }
    } else if (OB_FAIL(ack_status_map_.operate(ls_id, get_access_mode_functor))) {
      CLOG_LOG(WARN, "get access_mode from ack_status_map failed before send", K(ret), K(ls_id));
    } else if (palf::AccessMode::APPEND == access_mode) {
      CLOG_LOG(TRACE, "access mode changed to APPEND after get lsn, skip sending ack", K(ls_id), K(access_mode));
    } else {
      // 发送 ACK
      if (OB_FAIL(send_ack_to_primary_(ls_id, end_lsn, end_scn))) {
        CLOG_LOG(WARN, "send_ack_to_primary_ failed", K(ret), K(ls_id), K(end_lsn), K(end_scn));
        StandbyAckTask ack_task(ls_id);
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(push_ack_task(ack_task))) {
          LOG_WARN("failed to push ack task", KR(ret), K(tmp_ret), K(ls_id));
        }
      } else {
        CLOG_LOG(INFO, "send_ack_to_primary_ success", K(ls_id), K(end_lsn), K(end_scn));
      }
    }
  }
  return ret;
}

int ObLogStandbyAckService::process_barrier(const StandbyAckTask &task, bool &stopped)
{
  int ret = OB_SUCCESS;
  UNUSED(task);
  UNUSED(stopped);
  // barrier task 不需要特殊处理
  ret = OB_NOT_SUPPORTED;
  CLOG_LOG(WARN, "barrier task not supported", K(ret), K(task));
  return ret;
}

int ObLogStandbyAckService::check_sync_mode_(const share::ObLSID &ls_id, bool &is_sync_mode)
{
  int ret = OB_SUCCESS;
  is_sync_mode = false;
  const int64_t now = ObTimeUtility::current_time();
  bool need_update = false;
  SyncModeInfo cached_info;

  // 检查缓存是否过期
  {
    common::ObSpinLockGuard guard(cache_lock_);
    if (OB_FAIL(sync_mode_map_.get(ls_id, cached_info))) {
      need_update = true;
    } else if ((now - cached_info.cached_ts_) > SYNC_MODE_CACHE_EXPIRE_US) {
      need_update = true;
    } else {
      // 从缓存中获取
      is_sync_mode = cached_info.is_sync_mode_;
      CLOG_LOG(TRACE, "get sync_mode from cache", K(ls_id), K(is_sync_mode),
                K(cached_info.sync_mode_), K(cached_info.mode_version_));
    }
  }

  // 缓存未命中或过期，需要重新查询
  if (need_update) {
    ObLogService *log_service = MTL(ObLogService*);
    storage::ObLS *ls = nullptr;
    ObLSHandle ls_handle;
    ObLogHandler *log_handler = nullptr;

    if (OB_ISNULL(log_service)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "ObLogService is null", K(ret));
    } else if (OB_FAIL(MTL(storage::ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::LOG_MOD))) {
      CLOG_LOG(WARN, "get ls failed", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "ls is null", K(ret));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "log_handler is null", K(ret));
    } else {
      int64_t mode_version = 0;
      ipalf::SyncMode sync_mode = ipalf::SyncMode::INVALID_SYNC_MODE;
      if (OB_FAIL(log_handler->get_sync_mode(mode_version, sync_mode))) {
        CLOG_LOG(WARN, "get_sync_mode failed", K(ret), K(ls_id));
      } else {
        is_sync_mode = (sync_mode == ipalf::SyncMode::SYNC);

        // 更新缓存
        SyncModeInfo new_info;
        new_info.mode_version_ = mode_version;
        new_info.sync_mode_ = sync_mode;
        new_info.is_sync_mode_ = is_sync_mode;
        new_info.cached_ts_ = now;

        {
          common::ObSpinLockGuard guard(cache_lock_);
          if (OB_FAIL(sync_mode_map_.insert_or_update(ls_id, new_info))) {
            CLOG_LOG(WARN, "update sync_mode cache failed", K(ret), K(ls_id));
          } else {
            CLOG_LOG(TRACE, "update sync_mode cache success", K(ls_id), K(sync_mode),
                    K(is_sync_mode), K(mode_version));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogStandbyAckService::init_sync_mode_cache(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObLogService *log_service = MTL(ObLogService*);
  storage::ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  ObLogHandler *log_handler = nullptr;

  if (OB_ISNULL(log_service)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "ObLogService is null", K(ret));
  } else if (OB_FAIL(MTL(storage::ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::LOG_MOD))) {
    CLOG_LOG(WARN, "get ls failed", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "ls is null", K(ret));
  } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "log_handler is null", K(ret));
  } else {
    int64_t mode_version = 0;
    ipalf::SyncMode sync_mode = ipalf::SyncMode::INVALID_SYNC_MODE;
    if (OB_FAIL(log_handler->get_sync_mode(mode_version, sync_mode))) {
      CLOG_LOG(WARN, "get_sync_mode failed", K(ret), K(ls_id));
    } else {
      const int64_t now = ObTimeUtility::current_time();
      bool is_sync_mode = (sync_mode == ipalf::SyncMode::SYNC);

      SyncModeInfo new_info;
      new_info.mode_version_ = mode_version;
      new_info.sync_mode_ = sync_mode;
      new_info.is_sync_mode_ = is_sync_mode;
      new_info.cached_ts_ = now;

      common::ObSpinLockGuard guard(cache_lock_);
      if (OB_FAIL(sync_mode_map_.insert_or_update(ls_id, new_info))) {
        CLOG_LOG(WARN, "init sync_mode cache failed", K(ret), K(ls_id), K(sync_mode), K(is_sync_mode));
      } else {
        CLOG_LOG(INFO, "init sync_mode cache success", K(ls_id), K(sync_mode),
                 K(is_sync_mode), K(mode_version));
      }
    }
  }

  return ret;
}

int ObLogStandbyAckService::get_sync_mode_from_cache(const share::ObLSID &ls_id, bool &is_sync_mode)
{
  int ret = OB_SUCCESS;
  is_sync_mode = false;
  SyncModeInfo cached_info;

  if (OB_FAIL(sync_mode_map_.get(ls_id, cached_info))) {
    ret = OB_ENTRY_NOT_EXIST;
    CLOG_LOG(TRACE, "sync_mode cache not found", K(ls_id));
  } else {
    is_sync_mode = cached_info.is_sync_mode_;
    CLOG_LOG(TRACE, "get sync_mode from cache", K(ls_id), K(is_sync_mode),
             K(cached_info.sync_mode_), K(cached_info.mode_version_));
  }

  return ret;
}

int ObLogStandbyAckService::update_sync_mode_cache(const share::ObLSID &ls_id,
                                                     int64_t mode_version,
                                                     ipalf::SyncMode sync_mode,
                                                     bool is_sync_mode)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();

  SyncModeInfo new_info;
  new_info.mode_version_ = mode_version;
  new_info.sync_mode_ = sync_mode;
  new_info.is_sync_mode_ = is_sync_mode;
  new_info.cached_ts_ = now;

  common::ObSpinLockGuard guard(cache_lock_);
  if (OB_FAIL(sync_mode_map_.insert_or_update(ls_id, new_info))) {
    CLOG_LOG(WARN, "update sync_mode cache failed", K(ret), K(ls_id), K(sync_mode), K(is_sync_mode));
  } else {
    CLOG_LOG(TRACE, "update sync_mode cache success", K(ls_id), K(sync_mode),
             K(is_sync_mode), K(mode_version));
  }

  return ret;
}

int ObLogStandbyAckService::get_primary_info_(const share::ObLSID &ls_id,
                                                common::ObAddr &primary_addr,
                                                int64_t &primary_cluster_id,
                                                uint64_t &primary_tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  bool need_update = false;
  PrimaryInfo cached_info;

  // 检查缓存是否过期
  {
    common::ObSpinLockGuard guard(cache_lock_);
    if (OB_FAIL(primary_info_map_.get(ls_id, cached_info))) {
      need_update = true;
    } else if ((now - cached_info.cached_ts_) > PRIMARY_INFO_CACHE_EXPIRE_US) {
      need_update = true;
    } else {
      // 从缓存中获取
      primary_addr = cached_info.primary_addr_;
      primary_cluster_id = cached_info.primary_cluster_id_;
      primary_tenant_id = cached_info.primary_tenant_id_;
    }
  }

  if (need_update) {
    // 从 __all_virtual_log_restore_source 获取主库信息
    ObLogRestoreSourceMgr restore_source_mgr;
    ObLogRestoreSourceItem item;
    const uint64_t tenant_id = MTL_ID();

    if (OB_FAIL(restore_source_mgr.init(tenant_id, GCTX.sql_proxy_))) {
      CLOG_LOG(WARN, "restore_source_mgr init failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(restore_source_mgr.get_source(item))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        CLOG_LOG(INFO, "no log restore source found", K(ret), K(tenant_id));
        ret = OB_SUCCESS;  // 没有配置 restore source，不是错误
      } else {
        CLOG_LOG(WARN, "get_source failed", K(ret), K(tenant_id));
      }
    } else if (item.type_ != ObLogRestoreSourceType::SERVICE) {
      CLOG_LOG(TRACE, "restore source type is not SERVICE, skip", K(item.type_));
      ret = OB_SUCCESS;  // 不是 SERVICE 类型，不需要发送
    } else {
      // 解析 SERVICE 类型的 restore source
      RestoreServiceAttr service_attr;
      common::ObSqlString value;
      common::ObArray<common::ObAddr> addr_list;
      uint64_t primary_tenant_id_tmp = OB_INVALID_TENANT_ID;
      uint64_t primary_cluster_id_tmp = OB_INVALID_CLUSTER_ID;

      if (OB_FAIL(value.assign(item.value_))) {
        CLOG_LOG(WARN, "assign value failed", K(ret));
      } else if (OB_FAIL(service_attr.parse_service_attr_from_str(value))) {
        CLOG_LOG(WARN, "get_primary_server_addr failed", K(ret), K(value));
      } else if (service_attr.addr_.empty()) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "addr_list is empty", K(ret));
      } else {
        primary_cluster_id = service_attr.user_.cluster_id_;
        primary_tenant_id = service_attr.user_.tenant_id_;
        share::ObLogRestoreProxyUtil restore_proxy;

        // 使用 RPC 查询主库日志流的 leader 地址（主库从 location cache 获取）
        if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "srv_rpc_proxy_ is null", K(ret));
        } else if (OB_FAIL(restore_proxy.get_primary_ls_leader_addr_by_rpc(service_attr, GCTX.srv_rpc_proxy_, ls_id, primary_addr))) {
          CLOG_LOG(WARN, "get_primary_ls_leader_addr_by_rpc failed", K(ret), K(primary_tenant_id), K(ls_id));
        }

        if (OB_SUCC(ret)) {
          // 更新缓存
          PrimaryInfo info;
          info.primary_addr_ = primary_addr;
          info.primary_cluster_id_ = primary_cluster_id;
          info.primary_tenant_id_ = primary_tenant_id;
          info.cached_ts_ = now;

          common::ObSpinLockGuard guard(cache_lock_);
          (void)primary_info_map_.insert_or_update(ls_id, info);

          CLOG_LOG(INFO, "get_primary_info_ success", K(primary_addr), K(primary_cluster_id),
                  K(primary_tenant_id), K(ls_id));
        }
      }
    }
  }

  return ret;
}

int ObLogStandbyAckService::send_ack_to_primary_(const share::ObLSID &ls_id,
   const palf::LSN &committed_end_lsn, const share::SCN &committed_end_scn)
{
  int ret = OB_SUCCESS;
  common::ObAddr primary_addr;
  int64_t primary_cluster_id = OB_INVALID_CLUSTER_ID;
  uint64_t primary_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_FAIL(get_primary_info_(ls_id, primary_addr, primary_cluster_id, primary_tenant_id))) {
    CLOG_LOG(WARN, "get_primary_info_ failed", K(ret), K(ls_id));
  } else if (!primary_addr.is_valid() || primary_cluster_id == OB_INVALID_CLUSTER_ID ||
            primary_tenant_id == OB_INVALID_TENANT_ID) {
    // 没有配置主库信息，跳过发送
    CLOG_LOG(TRACE, "primary info not available, skip sending ack", K(ls_id));
  } else if (OB_ISNULL(rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "rpc_proxy_ is null", K(ret));
  } else {
    // 构造 RPC 响应，发送给主库
    ObLogSyncStandbyInfo resp;
    resp.standby_cluster_id_ = GCONF.cluster_id;
    resp.standby_tenant_id_ = MTL_ID();
    resp.ls_id_ = ls_id;
    resp.ret_code_ = OB_SUCCESS;
    resp.refresh_info_ret_code_ = OB_SUCCESS;
    resp.standby_committed_end_lsn_ = committed_end_lsn;
    resp.standby_committed_end_scn_ = committed_end_scn;

    const int64_t timeout_us = 3 * 1000 * 1000; // 3秒超时

    // 使用 post_log_transport_resp 发送响应给主库
    // 这是一个异步 RPC，但备库发送 ACK 不需要等待响应，所以使用空回调
    static StandbyAckCallback cb;
    if (OB_FAIL(rpc_proxy_->to(primary_addr)
                      .dst_cluster_id(primary_cluster_id)
                      .by(primary_tenant_id)
                      .timeout(timeout_us)
                      .post_log_transport_resp(resp, &cb))) {
      CLOG_LOG(WARN, "post_log_transport_resp failed", K(ret), K(primary_addr), K(resp));
    } else {
      if (REACH_TIME_INTERVAL_THREAD_LOCAL(1_s)) {
        CLOG_LOG(INFO, "send_ack_to_primary_ success", K(ls_id), K(committed_end_lsn),
                K(committed_end_scn), K(primary_addr));
      } else {
        CLOG_LOG(TRACE, "send_ack_to_primary_ success", K(ls_id), K(committed_end_lsn),
                K(committed_end_scn), K(primary_addr));
      }
    }
  }

  return ret;
}

int StandbyAckStatus::get_current_end_lsn_scn_from_palf(palf::LSN &end_lsn, share::SCN &end_scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(palf_handle_->get_end_scn(end_scn))) {
    CLOG_LOG(WARN, "get_end_scn failed", K(ret), K(ls_id_));
  } else if (OB_FAIL(palf_handle_->get_end_lsn(end_lsn))) {
    CLOG_LOG(WARN, "get_end_lsn failed", K(ret), K(ls_id_));
  }
  return ret;
}

int StandbyAckStatus::get_access_mode(palf::AccessMode &access_mode)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "StandbyAckStatus is not inited", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(palf_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "palf_handle_ is null", KR(ret), K(ls_id_));
  } else if (OB_FAIL(palf_handle_->get_access_mode(access_mode))) {
    CLOG_LOG(WARN, "get_access_mode failed", KR(ret), K(ls_id_));
  }
  return ret;
}

int StandbyAckStatus::get_current_end_lsn_scn(palf::LSN &end_lsn, share::SCN &end_scn)
{
  int ret = OB_SUCCESS;
  ObLatchRGuard guard(end_lsn_scn_lock_, ObLatchIds::STANDBY_ACK_STATUS_LOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("StandbyAckStatus is not inited", KR(ret), K(is_inited_));
  } else {
    end_lsn = committed_end_lsn_;
    end_scn = committed_end_scn_;
  }
  return ret;
}

int StandbyAckStatus::update_current_end_lsn_scn(const palf::LSN &end_lsn, const share::SCN &end_scn)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(end_lsn_scn_lock_, ObLatchIds::STANDBY_ACK_STATUS_LOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("StandbyAckStatus is not inited", KR(ret), K(is_inited_));
  } else if (!end_lsn.is_valid() || !end_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(end_lsn), K(end_scn));
  } else if ((committed_end_lsn_.is_valid() && end_lsn < committed_end_lsn_) ||
      (committed_end_scn_.is_valid() && end_scn < committed_end_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "end_lsn or end_scn is less than committed_end_lsn or committed_end_scn",
       K(ret), K(end_lsn), K(end_scn), K(committed_end_lsn_), K(committed_end_scn_));
  } else {
    committed_end_lsn_ = end_lsn;
    committed_end_scn_ = end_scn;
  }
  return ret;
}

//---------------StandbyPeriodicAckTask---------------//
void StandbyPeriodicAckTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ack_service_)) {
    CLOG_LOG(WARN, "ack_service_ is null");
  } else {
    ret = ack_service_->periodic_send_ack_();
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "periodic_send_ack_ failed", K(ret));
    }
  }
}

//---------------ObLogStandbyAckService::periodic_send_ack_---------------//
int ObLogStandbyAckService::periodic_send_ack_()
{
  int ret = OB_SUCCESS;
  bool is_standby = false;
  const uint64_t tenant_id = MTL_ID();

  // 检查当前租户是否是 standby 租户
  if (OB_FAIL(ObShareUtil::mtl_check_if_tenant_role_is_standby(tenant_id, is_standby))) {
    CLOG_LOG(WARN, "check if tenant role is standby failed", K(ret), K(tenant_id));
  } else if (!is_standby) {
    // 只有 standby 才需要发送 ack
    CLOG_LOG(TRACE, "current tenant is not standby, skip periodic ack", K(tenant_id));
  } else {
    // 遍历所有 LS，获取当前的 committed_end_lsn/scn 并发送 ACK
    class PeriodicAckFunctor
    {
    public:
      PeriodicAckFunctor(ObLogStandbyAckService *ack_service)
        : ack_service_(ack_service), ret_(OB_SUCCESS) {}
      bool operator()(const share::ObLSID &ls_id, StandbyAckStatus *ack_status)
      {
        int ret = OB_SUCCESS;
        palf::AccessMode access_mode;
        palf::LSN end_lsn;
        share::SCN end_scn;
        if (OB_ISNULL(ack_service_) || OB_ISNULL(ack_status)) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "ack_service_ or ack_status is null", K(ret), KP(ack_service_), KP(ack_status));
        } else if (OB_FAIL(ack_status->get_access_mode(access_mode))) {
          CLOG_LOG(WARN, "get_access_mode failed", K(ret), K(ls_id));
        } else if (palf::AccessMode::APPEND == access_mode) {
          CLOG_LOG(TRACE, "access mode is append, skip send ack", K(ls_id), K(access_mode));
        } else {
          // 获取当前的 committed_end_lsn 和 committed_end_scn
          // 重启或切主场景下，可能没有新日志导致回调函数一直没有回调过，因此缓存中的lsn和scn是非法的
          // 这里直接从palf中获取作为一个兜底手段
          if (OB_FAIL(ack_status->get_current_end_lsn_scn_from_palf(end_lsn, end_scn))) {
            CLOG_LOG(TRACE, "get_current_end_lsn_scn failed", K(ret), K(ls_id));
            ret = OB_SUCCESS;  // 忽略错误，继续处理下一个 LS
          } else if (!end_lsn.is_valid() || !end_scn.is_valid()) {
            CLOG_LOG(TRACE, "end_lsn or end_scn is invalid, skip this ls", K(ls_id), K(end_lsn), K(end_scn));
          } else if (OB_FAIL(ack_status->get_access_mode(access_mode))) {
            CLOG_LOG(WARN, "get_access_mode failed after get lsn", K(ret), K(ls_id));
          } else if (palf::AccessMode::APPEND == access_mode) {
            CLOG_LOG(TRACE, "access mode changed to APPEND after get lsn, skip send ack", K(ls_id), K(access_mode));
          } else {
            // 检查是否处于强同步模式
            bool is_sync_mode = true; //TODO by ziqi: 临时修改，后续删除
            if (OB_FAIL(ack_service_->check_sync_mode_(ls_id, is_sync_mode))) {
              CLOG_LOG(TRACE, "check_sync_mode_ failed", K(ret), K(ls_id));
              ret = OB_SUCCESS;
            // } else if (!is_sync_mode) {
            //   // 非强同步模式，不需要发送
            //   CLOG_LOG(TRACE, "not in sync mode, skip periodic ack", K(ls_id), K(end_lsn), K(end_scn));
            } else {
              // 强同步模式，发送 ACK
              if (OB_FAIL(ack_service_->send_ack_to_primary_(ls_id, end_lsn, end_scn))) {
                CLOG_LOG(TRACE, "send_ack_to_primary_ failed", K(ret), K(ls_id), K(end_lsn), K(end_scn));
                ret = OB_SUCCESS;
              }
              CLOG_LOG(INFO, "periodic send_ack_to_primary_", K(ret), K(ls_id), K(end_scn), K(end_lsn));
            }
          }
        }

        if (OB_FAIL(ret)) {
          ret_ = ret;
        }
        return true;
      }

      int get_ret() const { return ret_; }

    private:
      ObLogStandbyAckService *ack_service_;
      int ret_;
    };

    PeriodicAckFunctor functor(this);
    if (OB_FAIL(ack_status_map_.for_each(functor))) {
      CLOG_LOG(WARN, "for_each ack_status_map_ failed", K(ret));
    } else {
      ret = functor.get_ret();
    }
  }

  return ret;
}

} // namespace logservice
} // namespace oceanbase

