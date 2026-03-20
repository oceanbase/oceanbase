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

#include <utility>

#include "ob_log_restore_handler.h"
#include "logservice/ob_log_service.h"       // ObLogService
#include "ob_remote_log_source_allocator.h"  // ObResSrcAlloctor
#include "logservice/palf/log_entry.h"       // LogEntry
#include "logservice/palf/log_group_entry.h" // LogGroupEntry
#include "logservice/ob_log_base_header.h"   // ObLogBaseHeader
#include "logservice/palf/log_sync_mode_mgr.h" // ObSyncModeLog, ObSyncModeLogType
#include "logservice/palf/palf_env.h"
#include "logservice/transportservice/ob_log_standby_ack_service.h" // ObLogStandbyAckService
#include "storage/ls/ob_ls.h"                 // ObLS
#include "storage/ob_storage_struct.h"       // ObLSHandle, ObLSGetMod
#include "lib/utility/ob_sort.h"             // ob_sort
#include "lib/container/ob_se_array.h"       // ObSEArray
#include "share/rc/ob_tenant_base.h"         // MTL_ID, mtl_malloc, mtl_free
#include "rootserver/ob_tenant_info_loader.h" // ObTenantInfoLoader
#include "share/config/ob_server_config.h"   // GCONF

namespace oceanbase
{
using namespace palf;
namespace logservice
{
using namespace oceanbase::share;
using namespace oceanbase::archive;

const char *restore_comment_str[static_cast<int>(RestoreSyncStatus::MAX_RESTORE_SYNC_STATUS)] = {
  "Invalid restore status",
  " ",
  "There is a gap between the log source and standby",
  "Log conflicts, the standby with the same LSN is different from the log source when submit log",
  "Log conflicts, the standby with the same LSN is different from the log source when fetch log",
  "Log source can not be accessed, the replication account may be incorrect or the privelege is insufficient",
  "Log source is unreachable, the log source access point may be unavailable",
  "Fetch log time out",
  "Restore suspend, the log stream has synchronized to recovery until scn",
  "Standby binary version is lower than primary data version, standby need to upgrade",
  "Primary tenant has been dropped",
  "Waiting log stream created",
  "Query primary failed",
  "Restore handler has no leader",
  "Unexpected exceptions",
};

const char *restore_status_str[static_cast<int>(RestoreSyncStatus::MAX_RESTORE_SYNC_STATUS)] = {
  "INVALID RESTORE STATUS",
  "NORMAL",
  "SOURCE HAS A GAP",
  "STANDBY LOG NOT MATCH",
  "STANDBY LOG NOT MATCH",
  "CHECK USER OR PASSWORD",
  "CHECK NETWORK",
  "FETCH LOG TIMEOUT",
  "RESTORE SUSPEND",
  "STANDBY NEED UPGRADE",
  "PRIMARY TENANT DROPPED",
  "WAITING LS CREATED",
  "QUERY PRIMARY FAILED",
  "RESTORE HANDLER HAS NO LEADER",
  "NOT AVAILABLE",
};

RestoreSyncStatus str_to_restore_sync_status(const ObString &str)
{
  RestoreSyncStatus ret_status = RestoreSyncStatus::MAX_RESTORE_SYNC_STATUS;
  if (str.empty()) {
    ret_status = RestoreSyncStatus::MAX_RESTORE_SYNC_STATUS;
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(restore_status_str); i++) {
      if (0 == str.case_compare(restore_status_str[i])) {
        ret_status = static_cast<RestoreSyncStatus>(i);
        break;
      }
    }
  }
  return ret_status;
}

void ObRemoteFetchStat::update_fetch_stat(const ObRemoteFetchTaskStat &stat)
{
  fetch_task_count_++;
  fetch_log_size_ += stat.fetch_log_size_;
  gen_to_fetch_time_ += stat.start_fetch_ts_ - stat.gen_ts_;
  fetch_log_time_ += stat.finish_fetch_ts_ - stat.start_fetch_ts_;
  fetch_to_submit_time_ += stat.start_submit_ts_ - stat.finish_fetch_ts_;
  submit_log_time_ += stat.finish_submit_ts_ - stat.start_submit_ts_;
}

ObLogRestoreHandler::ObLogRestoreHandler() :
  parent_(NULL),
  context_(),
  restore_context_(),
  last_stat_ts_(OB_INVALID_TIMESTAMP),
  cur_delay_count_(0),
  last_delay_count_(0),
  cur_stat_info_(),
  last_stat_info_(),
  sync_mode_change_lock_(common::ObLatchIds::TRANSPORT_SERVICE_TASK_LOCK),
  pre_async_scn_()
{
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  ignore_restore_source_for_test_ = false;
#endif
}

ObLogRestoreHandler::~ObLogRestoreHandler()
{
  destroy();
}

int ObLogRestoreHandler::init(const int64_t id, ipalf::IPalfEnv *palf_env)
{
  int ret = OB_SUCCESS;
  ipalf::IPalfHandle *palf_handle = NULL;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (NULL == palf_env) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), KP(palf_env));
  } else if (OB_FAIL(palf_env->open(id, palf_handle))) {
    CLOG_LOG(WARN, "get palf_handle failed", K(ret), K(id));
  } else {
    common::ObILogAllocator *alloc_mgr = nullptr;
    id_ = id;
    palf_handle_ = palf_handle;
    palf_env_ = palf_env;
    role_ = FOLLOWER;
    enable_logservice_ = GCONF.enable_logservice;
    is_in_stop_state_ = false;
    palf::PalfEnv *palf_env_impl = static_cast<palf::PalfEnv*>(palf_env_);
    if (!enable_logservice_ && (OB_ISNULL(palf_env_impl)
        || OB_ISNULL(palf_env_impl->get_palf_env_impl())
        || OB_ISNULL(alloc_mgr = palf_env_impl->get_palf_env_impl()->get_log_allocator()))) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "get palf log allocator failed", K(ret), K(id), KP(palf_env_impl));
    } else if (!enable_logservice_ && OB_FAIL(transport_task_queue_.init(alloc_mgr,
                                                 this,
                                                 ObLogTransportTaskQueue::MAX_QUEUE_SIZE))) {
      CLOG_LOG(WARN, "init transport task queue failed", K(ret), K(id));
    } else {
      is_inited_ = true;
      CLOG_LOG(INFO, "ObLogRestoreHandler init success", K(id), K(role_), K(palf_handle));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(palf_env) && OB_NOT_NULL(palf_handle) && true == palf_handle->is_valid() && false == is_valid()) {
      palf_env->close(palf_handle);
    }
  }
  return ret;
}

int ObLogRestoreHandler::stop()
{
  int ret = OB_SUCCESS;
  // Stop queue admission first to block new push/process attempts.
  transport_task_queue_.stop();
  WLockGuard guard(lock_);
  if (IS_INIT) {
    is_in_stop_state_ = true;
    if (OB_NOT_NULL(palf_handle_) && palf_handle_->is_valid()) {
      palf_env_->close(palf_handle_);
    }
  }
  CLOG_LOG(INFO, "ObLogRestoreHandler stop", K(ret), KPC(this));
  return ret;
}

void ObLogRestoreHandler::destroy()
{
  // stop transport task queue first to block new push/process attempts.
  // destroy transport task queue after stop to ensure no new tasks are processed.
  transport_task_queue_.stop();
  transport_task_queue_.destroy();
  WLockGuard guard(lock_);
  if (IS_INIT) {
    is_in_stop_state_ = true;
    is_inited_ = false;
    if (OB_NOT_NULL(palf_env_) && OB_NOT_NULL(palf_handle_) && palf_handle_->is_valid()) {
      palf_env_->close(palf_handle_);
    }
    palf_env_ = NULL;
    id_ = -1;
    proposal_id_ = 0;
    last_stat_ts_ = OB_INVALID_TIMESTAMP;
    cur_delay_count_ = 0;
    last_delay_count_ = 0;
    cur_stat_info_.reset();
    last_stat_info_.reset();
    role_ = ObRole::INVALID_ROLE;
    context_.reset();
    pre_async_scn_.reset();
  }
  if (NULL != parent_) {
    ObResSrcAlloctor::free(parent_);
    parent_ = NULL;
  }
}

// To support log restore in parallel, the FetchLogTask is introduced and each task covers a range of logs, as [Min_LSN, Max_LSN),
// which means logs wiil be pulled from the log restore source  and submitted to Palf with this task.
//
// Two scenarios should be handled:
// 1) Log restore is done within the leader of restore handler, which changes with the Palf
// 2) The restore end_scn maybe set to control the log pull and restore.
//
// For follower, log restore should terminate while all tasks should be freed and the restore context can be reset.
// The same as restore to_end.
//
// To generate nonoverlapping and continuous FetchLogTask, a max_submit_lsn_ in context is hold, which is the Max_LSN of the previous task
// and the Min_LSN of the current task.
//
// When role change of the restore handler, the context is reset but which is not reset when restore to_end is set.
//
// The role change is easy to distinguish with the proposal_id while restore_scn change is not,
// so reset restore context and advance issue_version, to reset start_fetch_log_lsn if restore_scn is advanced
// and free issued tasks before restore to_end(paralleled)
//
void ObLogRestoreHandler::switch_role(const common::ObRole &role, const int64_t proposal_id)
{
  // follower to leader, clear transport task queue to reset begin sn from new leader commited end log id
  if (role_ != role && false == is_strong_leader(role_)) {
    clear_transport_task_queue_();
  }
  WLockGuard guard(lock_);
  role_ = role;
  proposal_id_ = proposal_id;
  if (! is_strong_leader(role_)) {
    ObResSrcAlloctor::free(parent_);
    parent_ = NULL;
    context_.reset();
  }
}

int ObLogRestoreHandler::get_role(common::ObRole &role, int64_t &proposal_id) const
{
  return ObLogHandlerBase::get_role(role, proposal_id);
}

bool ObLogRestoreHandler::is_valid() const
{
  return true == is_inited_
         && false == is_in_stop_state_
         && OB_NOT_NULL(palf_handle_)
         && true == palf_handle_->is_valid()
         && NULL != palf_env_;
}

int ObLogRestoreHandler::get_upper_limit_scn(SCN &scn) const
{
  RLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_)) {
    ret = OB_EAGAIN;
  } else {
    parent_->get_upper_limit_scn(scn);
  }
  return ret;
}

int ObLogRestoreHandler::get_max_restore_scn(SCN &scn) const
{
  RLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_) || ! restore_to_end_unlock_()) {
    ret = OB_EAGAIN;
  } else {
    parent_->get_end_scn(scn);
  }
  return ret;
}

int ObLogRestoreHandler::add_source(logservice::DirArray &array, const SCN &end_scn)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
  } else if (! is_strong_leader(role_)) {
    // not leader, just skip
  } else if (OB_UNLIKELY(array.empty() || !end_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(array), K(end_scn), KPC(this));
  } else if (FALSE_IT(alloc_source(share::ObLogRestoreSourceType::RAWPATH))) {
  } else if (NULL == parent_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObRemoteRawPathParent *source = static_cast<ObRemoteRawPathParent *>(parent_);
    const bool source_exist = source->is_valid();
    if (OB_FAIL(source->set(array, end_scn))) {
      CLOG_LOG(WARN, "ObRemoteRawPathParent set failed", K(ret), K(array), K(end_scn));
      ObResSrcAlloctor::free(parent_);
      parent_ = NULL;
    } else if (! source_exist) {
      context_.set_issue_version();
    }
  }
  return ret;;
}

int ObLogRestoreHandler::add_source(share::ObBackupDest &dest, const SCN &end_scn)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
  } else if (! is_strong_leader(role_)) {
    // not leader, just skip
  } else if (OB_UNLIKELY(! dest.is_valid() || !end_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
     CLOG_LOG(WARN, "invalid argument", K(ret), K(end_scn), K(dest), KPC(this));
  } else if (FALSE_IT(alloc_source(share::ObLogRestoreSourceType::LOCATION))) {
  } else if (NULL == parent_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObRemoteLocationParent *source = static_cast<ObRemoteLocationParent *>(parent_);
    const bool source_exist = source->is_valid();
    if (OB_FAIL(source->set(dest, end_scn))) {
      CLOG_LOG(WARN, "ObRemoteLocationParent set failed", K(ret), K(end_scn), K(dest));
      ObResSrcAlloctor::free(parent_);
      parent_ = NULL;
    } else if (! source_exist) {
      context_.set_issue_version();
    }
  }
  return ret;
}

int ObLogRestoreHandler::add_source(const share::ObRestoreSourceServiceAttr &service_attr, const SCN &end_scn)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
  } else if (! is_strong_leader(role_)) {
    // not leader, just skip
  } else if (OB_UNLIKELY(!service_attr.is_valid() || !end_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(end_scn), K(service_attr), KPC(this));
  } else if (FALSE_IT(alloc_source(ObLogRestoreSourceType::SERVICE))) {
  } else if (NULL == parent_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObRemoteSerivceParent *source = static_cast<ObRemoteSerivceParent *>(parent_);
    const bool source_exist = source->is_valid();
    if (OB_FAIL(source->set(service_attr, end_scn))) {
      CLOG_LOG(WARN, "ObRemoteSerivceParent set failed",
          K(ret), K(end_scn), K(service_attr), KPC(this));
      ObResSrcAlloctor::free(parent_);
      parent_ = NULL;
    } else if (! source_exist) {
      context_.reset();
    }
  }
  return ret;
}

int ObLogRestoreHandler::clean_source()
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
  } else if (NULL == parent_) {
    // just skip
  } else {
    CLOG_LOG(INFO, "log_restore_source is empty, clean source", KPC(parent_));
    ObResSrcAlloctor::free(parent_);
    parent_ = NULL;
    context_.reset();
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_SUBMIT_LOG_ERROR);

int ObLogRestoreHandler::validate_raw_write_params_(const int64_t proposal_id,
                                                     const palf::LSN &lsn,
                                                     const char *buf,
                                                     const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_in_stop_state_) {
    ret = OB_IN_STOP_STATE;
  } else if (LEADER != role_) {
    ret = OB_NOT_MASTER;
  } else if (OB_UNLIKELY(!lsn.is_valid()
        || NULL == buf
        || 0 >= buf_size
        || 0 >= proposal_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(proposal_id), K(lsn), K(buf), K(buf_size));
  } else if (proposal_id != proposal_id_) {
    ret = OB_NOT_MASTER;
    CLOG_LOG(INFO, "stale task, just skip", K(proposal_id), K(proposal_id_), K(lsn), K(id_));
  } else if ((NULL == parent_ || restore_to_end_unlock_())
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
             && !ignore_restore_source_for_test_
#endif
            ) {
    ret = OB_RESTORE_LOG_TO_END;
    CLOG_LOG(INFO, "submit log to end, just skip", K(ret), K(lsn), KPC(this));
  }
  return ret;
}

int ObLogRestoreHandler::parse_log_type_(const char *buf,
                                         const int64_t buf_size,
                                         ObLogBaseType &log_type,
                                         ObSyncModeLogType &sync_mode_log_type,
                                         ObLogBaseHeader &log_base_header,
                                         bool &is_standby_dest) const
{
  int ret = OB_SUCCESS;
  log_type = ObLogBaseType::INVALID_LOG_BASE_TYPE;
  sync_mode_log_type = ObSyncModeLogType::SYNC_MODE_UNKNOWN_TYPE;
  is_standby_dest = true;

  if (OB_ISNULL(buf) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid log data, skip parsing log type", K(ret), KP(buf), K(buf_size));
  } else {
    // 先反序列化 LogGroupEntry 获取 payload
    palf::LogGroupEntry group_entry;
    int64_t group_entry_pos = 0;
    if (OB_FAIL(group_entry.deserialize(buf, buf_size, group_entry_pos))) {
      CLOG_LOG(WARN, "deserialize LogGroupEntry failed", K(ret), K(id_), K(buf_size));
    } else {
      // 从 group entry 的 payload 中遍历所有 LogEntry
      const char *payload_buf = group_entry.get_data_buf();
      const int64_t payload_size = group_entry.get_data_len();
      if (OB_ISNULL(payload_buf) || payload_size <= 0) {
        ret = OB_INVALID_ARGUMENT;
        CLOG_LOG(WARN, "invalid payload, skip parsing log type", K(ret), KP(payload_buf), K(payload_size));
      } else {
        int64_t curr_pos = 0;
        int64_t log_entry_idx = 0;
        bool found_sync_mode_log = false;
        // 遍历 payload 中的所有 LogEntry
        while (OB_SUCCESS == ret && curr_pos < payload_size) {
          palf::LogEntry log_entry;
          int64_t log_entry_pos = 0;
          // 反序列化 LogEntry（包含 LogEntryHeader + data）
          if (OB_FAIL(log_entry.deserialize(payload_buf + curr_pos, payload_size - curr_pos, log_entry_pos))) {
            if (OB_ITER_END != ret) {
              CLOG_LOG(WARN, "deserialize LogEntry failed", K(ret), K(id_),
                       K(log_entry_idx), K(curr_pos), K(payload_size));
            }
          } else {
            // 从 LogEntry 的 data_buf 中解析 ObLogBaseHeader
            const char *log_entry_data_buf = log_entry.get_data_buf();
            const int64_t log_entry_data_len = log_entry.get_data_len();
            if (OB_NOT_NULL(log_entry_data_buf) && log_entry_data_len > 0) {
              ObLogBaseHeader entry_log_base_header;
              int64_t entry_header_pos = 0;
              if (OB_SUCCESS == (ret = entry_log_base_header.deserialize(log_entry_data_buf, log_entry_data_len, entry_header_pos))
                  && entry_log_base_header.is_valid()) {
                ObLogBaseType entry_log_type = entry_log_base_header.get_log_type();
                // 如果是强同步特殊日志，继续解析获取具体的类型（SYNC/ASYNC/PRE_ASYNC）
                // ObSyncModeLog 包含 ObLogBaseHeader header_，所以需要从 pos=0 开始反序列化整个 ObSyncModeLog
                if (ObLogBaseType::SYNC_MODE_LOG_BASE_TYPE == entry_log_type) {
                  ObSyncModeLog sync_mode_log;
                  int64_t sync_log_pos = 0;  // 从 0 开始，因为 ObSyncModeLog 包含 header_
                  if (OB_SUCCESS == (ret = sync_mode_log.deserialize(log_entry_data_buf, log_entry_data_len, sync_log_pos))) {
                    sync_mode_log_type = static_cast<ObSyncModeLogType>(sync_mode_log.get_log_type());
                    log_type = entry_log_type;
                    log_base_header = entry_log_base_header;
                    found_sync_mode_log = true;
                    // 检查当前备库是否为强同步下游
                    share::ObSyncStandbyStatusAttr protection_info = sync_mode_log.get_protection_info();
                    CLOG_LOG(INFO, "parsed sync mode log protection info", K(sync_mode_log));
                    if (protection_info.get_cluster_id() != GCONF.cluster_id || protection_info.get_tenant_id() != MTL_ID()) {
                      is_standby_dest = false;
                    }

                    CLOG_LOG(INFO, "parsed sync mode log type", K(sync_mode_log_type), K(id_),
                             K(log_entry_idx), K(log_entry.get_scn()), K(log_entry_data_len));
                  } else {
                    CLOG_LOG(WARN, "deserialize ObSyncModeLog failed", K(ret), K(id_),
                             K(log_entry_idx), K(log_entry_data_len), K(entry_header_pos));
                  }
                }
                // 记录第一条非特殊日志的类型
                if (!found_sync_mode_log && ObLogBaseType::INVALID_LOG_BASE_TYPE == log_type) {
                  log_type = entry_log_type;
                  log_base_header = entry_log_base_header;
                }
              }
            }
            // 移动到下一个 LogEntry
            curr_pos += log_entry.get_serialize_size();
            log_entry_idx++;
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;  // 正常结束
        }
        CLOG_LOG(INFO, "parsed log type from restore handler", K(log_type), K(sync_mode_log_type),
                 K(id_), K(log_base_header), K(payload_size), K(log_entry_idx), K(found_sync_mode_log));
      }
    }
  }
  return ret;
}

int ObLogRestoreHandler::handle_sync_mode_log_(const int64_t proposal_id,
                                                const palf::LSN &lsn,
                                                const share::SCN &scn,
                                                ObSyncModeLogType sync_mode_log_type,
                                                const bool is_standby_dest,
                                                int64_t &new_proposal_id)
{
  int ret = OB_SUCCESS;
  storage::ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  ObLogService *log_service = MTL(ObLogService*);
  int64_t mode_version = INVALID_PROPOSAL_ID;
  palf::SyncMode curr_sync_mode = palf::SyncMode::INVALID_SYNC_MODE;
  palf::SyncMode target_sync_mode = palf::SyncMode::INVALID_SYNC_MODE;
  palf::AccessMode curr_access_mode = palf::AccessMode::INVALID_ACCESS_MODE;

  if (OB_ISNULL(log_service)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "ObLogService is null, skip change sync mode", K(id_));
  } else if (OB_FAIL(MTL(storage::ObLSService*)->get_ls(share::ObLSID(id_), ls_handle, ObLSGetMod::LOG_MOD))) {
    CLOG_LOG(WARN, "get ls failed", K(ret), K(id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "ls is null", K(ret), K(id_));
  } else if (OB_FAIL(palf_handle_->get_sync_mode(mode_version, curr_sync_mode))) {
    CLOG_LOG(WARN, "get_sync_mode failed", K(ret), K(id_));
  } else if (OB_FAIL(palf_handle_->get_access_mode(curr_access_mode))) {
    CLOG_LOG(WARN, "get_access_mode failed", K(ret), K(id_));
  } else {
    // 根据日志类型确定target_sync_mode
    // 收到SYNC日志，并且强同步下游是自己，修改为SYNC
    if (is_standby_dest && ObSyncModeLogType::SYNC == sync_mode_log_type
        && curr_access_mode == palf::AccessMode::RAW_WRITE) {
      target_sync_mode = palf::SyncMode::SYNC;
      // 收到 SYNC 日志时，清空 pre_async_scn_，因为已经重新进入强同步模式
      pre_async_scn_.reset();
    // 收到PRE_ASYNC日志，不区分强同步备库还是第三方备库，直接修改为PRE_ASYNC
    } else if (ObSyncModeLogType::PRE_ASYNC == sync_mode_log_type
        && curr_access_mode == palf::AccessMode::RAW_WRITE) {
      target_sync_mode = palf::SyncMode::PRE_ASYNC;
      // 记录 PRE_ASYNC 日志的 SCN，用于后续判断 ASYNC 日志是否可以接受
      pre_async_scn_ = scn;
      CLOG_LOG(INFO, "recorded pre_async_scn for ASYNC log acceptance check",
               K(id_), K(scn), K(pre_async_scn_));
    // 第三方备库收到SYNC日志，修改为PRE_ASYNC
    } else if (!is_standby_dest && ObSyncModeLogType::SYNC == sync_mode_log_type
        && curr_access_mode == palf::AccessMode::RAW_WRITE) {
      target_sync_mode = palf::SyncMode::PRE_ASYNC;
    // 处理 ASYNC 日志：修改为 ASYNC
    } else if (ObSyncModeLogType::ASYNC == sync_mode_log_type
        && curr_access_mode == palf::AccessMode::RAW_WRITE) {
      target_sync_mode = palf::SyncMode::ASYNC;
      // ASYNC 日志处理完成后，清空 pre_async_scn_
      pre_async_scn_.reset();
      CLOG_LOG(INFO, "ASYNC log processed, cleared pre_async_scn", K(id_), K(scn));
    } else {
      CLOG_LOG(WARN, "invalid sync mode log type", K(sync_mode_log_type), K(curr_access_mode));
    }

    // 如果目标sync_mode与当前不同，则修改
    if (target_sync_mode != palf::SyncMode::INVALID_SYNC_MODE
        && curr_sync_mode != target_sync_mode) {
      int64_t new_mode_version = INVALID_PROPOSAL_ID;
      if (OB_FAIL(palf_handle_->change_sync_mode(proposal_id, mode_version, target_sync_mode,
                                                 new_mode_version, new_proposal_id))) {
        CLOG_LOG(WARN, "change_sync_mode failed", K(ret), K(id_), K(mode_version),
                 K(curr_sync_mode), K(target_sync_mode), K(scn), K(lsn));
      } else {
        CLOG_LOG(INFO, "change_sync_mode success for sync mode log",
                 K(id_), K(curr_sync_mode), K(target_sync_mode),
                 K(mode_version), K(new_mode_version), K(new_proposal_id), K(scn));

        // 等待备库上 palf proposal_id 和 restore_handler 的 proposal_id 一致（阻塞等待）
        // wait_proposal_id_consistent_() 内部会尝试获取读锁，所以必须在释放写锁后调用
        // TODO by qingxia: 暂时注释掉，等待 sync mode 变更不杀事务合入，后续再放开
        // TODO by qingxia: 在 sync mode 变更和选举并发的情况下，可能这里还是会卡住等30s
        // int wait_ret = wait_proposal_id_consistent_(new_proposal_id);
        // if (OB_FAIL(wait_ret)) {
        //   CLOG_LOG(WARN, "wait proposal_id consistent failed", K(id_), K(wait_ret));
        // }

        // 通知 ack service 更新 sync_mode 缓存
        ObLogStandbyAckService *ack_service = log_service->get_log_standby_ack_service();
        if (OB_ISNULL(ack_service)) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "ack_service is null", KR(ret), K(id_));
        } else {
          bool is_sync_mode = (target_sync_mode == palf::SyncMode::SYNC);
          int tmp_ret = ack_service->update_sync_mode_cache(share::ObLSID(id_),
                                                             new_mode_version,
                                                             target_sync_mode,
                                                             is_sync_mode);
          if (OB_SUCCESS != tmp_ret) {
            CLOG_LOG(WARN, "update_sync_mode_cache failed", K(tmp_ret),
                     K(id_), K(target_sync_mode), K(is_sync_mode));
          } else {
            CLOG_LOG(INFO, "update_sync_mode_cache success",
                     K(id_), K(target_sync_mode), K(is_sync_mode), K(new_mode_version));
          }
        }
      }
    } else {
      CLOG_LOG(TRACE, "no need to change sync mode",
               K(id_), K(curr_sync_mode), K(target_sync_mode));
    }
  }

  return ret;
}

int ObLogRestoreHandler::check_async_log_acceptable_(const share::SCN &async_log_scn) const
{
  int ret = OB_SUCCESS;
  share::SCN tenant_sync_scn;
  rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);

  if (OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "ObTenantInfoLoader is null", KR(ret), K(id_));
  } else if (OB_FAIL(tenant_info_loader->get_sync_scn(tenant_sync_scn))) {
    CLOG_LOG(WARN, "get_sync_scn failed", KR(ret), K(id_));
  } else if (!pre_async_scn_.is_valid()) {
    // 如果没有记录 pre_async_scn_，可能是：
    // 1. 重启后丢失
    // 2. 从未收到过 PRE_ASYNC 日志
    // 这种情况下，直接接受 ASYNC 日志
    CLOG_LOG(INFO, "pre_async_scn_ is invalid, accept ASYNC log directly",
             K(id_), K(async_log_scn), K(tenant_sync_scn));
  } else if (tenant_sync_scn < pre_async_scn_) {
    // 条件不满足：租户的 sync_scn 还没有超过 pre_async_scn_
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {  // 每秒打印一次日志
      CLOG_LOG(INFO, "ASYNC log cannot be accepted, tenant sync_scn not exceed pre_async_scn",
               K(id_), K(tenant_sync_scn), K(pre_async_scn_), K(async_log_scn));
    }
  } else {
    // 条件满足：租户的 sync_scn 已经超过 pre_async_scn_
    CLOG_LOG(INFO, "ASYNC log can be accepted, tenant sync_scn exceed pre_async_scn",
             K(id_), K(tenant_sync_scn), K(pre_async_scn_), K(async_log_scn));
  }

  return ret;
}

int ObLogRestoreHandler::wait_proposal_id_consistent_(const int64_t new_proposal_id) const
{
  int ret = OB_SUCCESS;
  const int64_t CHECK_INTERVAL_US = 500L; // 500us
  const int64_t MAX_WAIT_TIME_US = 30 * 1000 * 1000L; // 30秒超时 //TODO by ziqi: 改为可配置或传入timeout
  int64_t start_time_us = ObTimeUtility::current_time();
  bool is_consistent = false;

  // 阻塞等待直到 proposal_id 一致
  while (OB_SUCC(ret) && !is_consistent) {
    // 检查超时
    int64_t elapsed_time_us = ObTimeUtility::current_time() - start_time_us;
    if (elapsed_time_us > MAX_WAIT_TIME_US) {
      ret = OB_TIMEOUT;
      CLOG_LOG(WARN, "wait proposal_id consistent timeout", K(ret), K(id_), K(new_proposal_id),
               K(elapsed_time_us), K(MAX_WAIT_TIME_US));
      break;
    }

    int64_t restore_handler_proposal_id = palf::INVALID_PROPOSAL_ID;
    common::ObRole restore_handler_role = common::FOLLOWER;
    if (OB_FAIL(get_role(restore_handler_role, restore_handler_proposal_id))) {
      CLOG_LOG(WARN, "get role failed", KR(ret), K(id_));
      ob_usleep(CHECK_INTERVAL_US);
      continue;
    } else if (new_proposal_id < restore_handler_proposal_id) {
      ret = OB_NOT_MASTER;
      CLOG_LOG(WARN, "new_proposal_id is less than restore_handler_proposal_id",
               KR(ret), K(id_), K(new_proposal_id), K(restore_handler_proposal_id));
      break;
    } else if (new_proposal_id == restore_handler_proposal_id) {
      is_consistent = true;
      CLOG_LOG(INFO, "proposal_id consistent between palf and restore_handler",
               K(id_), K(restore_handler_proposal_id), K(new_proposal_id),
               K(restore_handler_role), K(elapsed_time_us));
    } else {
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
        CLOG_LOG(INFO, "waiting for proposal_id consistent",
                 K(id_), K(restore_handler_proposal_id), K(new_proposal_id),
                 K(restore_handler_role), K(elapsed_time_us));
      }
      ob_usleep(CHECK_INTERVAL_US);
    }
  }

  return ret;
}

int ObLogRestoreHandler::do_raw_write_(const int64_t proposal_id,
                                       const palf::LSN &lsn,
                                       const char *buf,
                                       const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  palf::PalfAppendOptions opts;
  opts.need_nonblock = true;
  opts.need_check_proposal_id = true;
  opts.proposal_id = proposal_id;

  // errsim fake error
  if (ERRSIM_SUBMIT_LOG_ERROR) {
    ret = ERRSIM_SUBMIT_LOG_ERROR;
    CLOG_LOG(TRACE, "errsim submit log error");
  } else {
    ret = palf_handle_->raw_write(opts, lsn, buf, buf_size);
    if (OB_SUCC(ret)) {
      uint64_t tenant_id = palf_env_->get_tenant_id();
      EVENT_TENANT_ADD(ObStatEventIds::RESTORE_WRITE_LOG_SIZE, buf_size, tenant_id);
    } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
      // 日志已存在（LSN 小于当前可写位置），可能是被其他来源（如 log fetcher）已写入
      // 当作成功处理，避免重复写入
      ret = OB_SUCCESS;
      CLOG_LOG(TRACE, "log already exists (OB_ERR_OUT_OF_LOWER_BOUND), treat as success",
               K(id_), K(lsn), K(buf_size));
    } else {
      CLOG_LOG(WARN, "raw write failed", K(ret), K(id_), K(lsn), K(buf_size));
    }
  }
  return ret;
}

int ObLogRestoreHandler::do_raw_write_with_retry_(const int64_t proposal_id,
                                                  const palf::LSN &lsn,
                                                  const char *buf,
                                                  const int64_t buf_size,
                                                  const bool need_sync_mode_lock)
{
  int ret = OB_SUCCESS;
  int64_t wait_times = 0;

  while (wait_times < MAX_RAW_WRITE_RETRY_TIMES) {
    ret = OB_SUCCESS;
    if (need_sync_mode_lock) {
      // 普通日志获取 sync_mode_change_lock_ 的读锁
      // 这样在 change_sync_mode 执行期间（持有写锁）会被阻塞
      SyncModeRLockGuard sync_mode_rguard(sync_mode_change_lock_);
      RLockGuard guard(lock_);
      if (OB_FAIL(validate_raw_write_params_(proposal_id, lsn, buf, buf_size))) {
        CLOG_LOG(WARN, "validate_raw_write_params_ failed", KR(ret), K(id_), K(proposal_id), K(lsn), K(buf_size));
      } else if (OB_FAIL(do_raw_write_(proposal_id, lsn, buf, buf_size))) {
        CLOG_LOG(WARN, "do_raw_write_ failed", KR(ret), K(id_), K(proposal_id), K(lsn), K(buf_size));
      }
    } else {
      // sync_mode_log 不需要获取 sync_mode_change_lock_，因为它已经持有写锁
      RLockGuard guard(lock_);
      if (OB_FAIL(validate_raw_write_params_(proposal_id, lsn, buf, buf_size))) {
        CLOG_LOG(WARN, "validate_raw_write_params_ failed", KR(ret), K(id_), K(proposal_id), K(lsn), K(buf_size));
      } else if (OB_FAIL(do_raw_write_(proposal_id, lsn, buf, buf_size))) {
        CLOG_LOG(WARN, "do_raw_write_ failed", KR(ret), K(id_), K(proposal_id), K(lsn), K(buf_size));
      }
    }

    if (OB_EAGAIN == ret) {
      ++wait_times;
      int64_t sleep_us = wait_times * 10;
      if (sleep_us > MAX_RETRY_SLEEP_US) {
        sleep_us = MAX_RETRY_SLEEP_US;
      }
      ob_usleep(sleep_us);
    } else {
      break;
    }
  }

  return ret;
}

int ObLogRestoreHandler::raw_write(const int64_t proposal_id,
                                   const palf::LSN &lsn,
                                   const SCN &scn,
                                   const char *buf,
                                   const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  // 解析日志类型
  ObLogBaseHeader log_base_header;
  ObLogBaseType log_type = ObLogBaseType::INVALID_LOG_BASE_TYPE;
  ObSyncModeLogType sync_mode_log_type = ObSyncModeLogType::SYNC_MODE_UNKNOWN_TYPE;
  bool is_standby_dest = false;
  if (OB_FAIL(parse_log_type_(buf, buf_size, log_type, sync_mode_log_type, log_base_header, is_standby_dest))) {
    CLOG_LOG(WARN, "parse log type from buffer failed", KR(ret), K(id_), K(lsn), K(buf_size));
  } else {
    // 如果是强同步升降级的特殊日志，需要修改sync_mode
    bool is_sync_mode_log = (ObLogBaseType::SYNC_MODE_LOG_BASE_TYPE == log_type
                            && (ObSyncModeLogType::SYNC == sync_mode_log_type
                            || ObSyncModeLogType::ASYNC == sync_mode_log_type
                            || ObSyncModeLogType::PRE_ASYNC == sync_mode_log_type));
    CLOG_LOG(INFO, "check sync mode log", K(is_sync_mode_log), K(log_type), K(sync_mode_log_type), K(id_), K(lsn));

    // 如果是 ASYNC 日志，需要检查租户的 sync_scn 是否已经超过 pre_async_scn_
    if (ObSyncModeLogType::ASYNC == sync_mode_log_type) {
      if (OB_FAIL(check_async_log_acceptable_(scn))) {
        if (OB_EAGAIN == ret) {
          CLOG_LOG(INFO, "ASYNC log not acceptable yet, will retry", K(id_), K(lsn), K(scn));
        } else {
          CLOG_LOG(WARN, "check_async_log_acceptable_ failed", KR(ret), K(id_), K(lsn), K(scn));
        }
        return ret;  // 条件不满足，直接返回让调用方重试
      }
    }

    // 如果是 sync_mode_log，先修改 sync_mode，再写日志
    if (is_sync_mode_log) {
      // 获取写锁，阻止新的日志提交，防止在 change_sync_mode 期间有新日志导致滑动窗口问题
      SyncModeWLockGuard sync_mode_wguard(sync_mode_change_lock_);
      CLOG_LOG(INFO, "acquired sync_mode_change_lock_ write lock before change_sync_mode",
              K(id_), K(lsn), K(sync_mode_log_type));

      // 先修改 sync_mode
      int64_t new_proposal_id = proposal_id;
      if (OB_FAIL(handle_sync_mode_log_(proposal_id, lsn, scn, sync_mode_log_type, is_standby_dest, new_proposal_id))) {
        CLOG_LOG(WARN, "handle_sync_mode_log_ failed", KR(ret), K(id_), K(lsn), K(scn), K(sync_mode_log_type));
      } else {
        CLOG_LOG(INFO, "handle_sync_mode_log_ success, now write log", K(id_), K(lsn), K(scn), K(sync_mode_log_type),
                 K(proposal_id), K(new_proposal_id));

        // 再写日志，不需要获取 sync_mode_change_lock_ 读锁（因为已经持有写锁）
        if (OB_FAIL(do_raw_write_with_retry_(new_proposal_id, lsn, buf, buf_size, false))) {
          CLOG_LOG(WARN, "do_raw_write_with_retry_ failed", KR(ret), K(id_), K(lsn), K(buf_size));
        } else {
          CLOG_LOG(INFO, "sync_mode_log write success", K(id_), K(lsn), K(scn), K(sync_mode_log_type));
        }
      }
      // sync_mode_wguard 析构时自动释放写锁
    } else {
      // 普通日志需要获取 sync_mode_change_lock_ 的读锁
      // 这样在 change_sync_mode 执行期间（持有写锁）会被阻塞
      if (OB_FAIL(do_raw_write_with_retry_(proposal_id, lsn, buf, buf_size, true))) {
        CLOG_LOG(WARN, "do_raw_write_with_retry_ failed", KR(ret), K(id_), K(lsn), K(buf_size));
      }
    }
  }

  return ret;
}

int ObLogRestoreHandler::update_max_fetch_info(const int64_t proposal_id,
                                                  const palf::LSN &lsn,
                                                  const SCN &scn,
                                                  const ObRemoteFetchTaskStat &stat)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_in_stop_state_) {
    ret = OB_IN_STOP_STATE;
  } else if (LEADER != role_) {
    ret = OB_NOT_MASTER;
  } else if (proposal_id != proposal_id_) {
    ret = OB_NOT_MASTER;
    CLOG_LOG(INFO, "stale task, just skip", K(proposal_id), K(proposal_id_), K(lsn), K(id_));
  } else if (OB_UNLIKELY(!lsn.is_valid() || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid lsn or scn", K(proposal_id), K(lsn), K(scn), KPC(this));
  } else if (context_.max_fetch_lsn_.is_valid() && context_.max_fetch_lsn_ >= lsn) {
    // do nothing
  } else {
    const int64_t max_fetch_ts = scn.convert_to_ts();
    const int64_t cur_delay = stat.finish_submit_ts_ - max_fetch_ts;
    const int64_t task_delay = stat.finish_submit_ts_ - stat.gen_ts_;
    const int64_t max_delay = task_delay + cur_delay;
    constexpr int64_t SECOND_US = 1000 * 1000;
    cur_stat_info_.update_fetch_stat(stat);
    context_.max_fetch_lsn_ = lsn;
    context_.max_fetch_scn_ = scn;
    context_.last_fetch_ts_ = ObTimeUtility::fast_current_time();

    if (REACH_TIME_INTERVAL(1 * SECOND_US)) {
      if (max_delay > 1 * SECOND_US && max_delay < 600 * SECOND_US) {
        CLOG_LOG(INFO, "cur delay larger than 1s, print detail", K(max_delay), K(proposal_id),
            K(id_), K(role_), K(task_delay), K(cur_delay), K(lsn), K(scn), K(stat));
      }
    }

    if (OB_NOT_NULL(parent_)) {
      if (parent_->set_to_end(scn)) {
        // To stop and clear all restore log tasks and restore context, reset context and advance issue version
        CLOG_LOG(INFO, "restore log to_end succ", KPC(this), KPC(parent_));
      }
    }
  }
  return ret;
}

void ObLogRestoreHandler::deep_copy_source(ObRemoteSourceGuard &source_guard)
{
  RLockGuard guard(lock_);
  deep_copy_source_(source_guard);
}

int ObLogRestoreHandler::schedule(const int64_t id,
    const int64_t proposal_id,
    const int64_t version,
    const LSN &lsn,
    bool &scheduled)
{
  int ret = OB_SUCCESS;
  scheduled = false;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (id != id_ || proposal_id != proposal_id_ || version != context_.issue_version_) {
    // stale task
  } else {
    scheduled = true;
    context_.max_submit_lsn_ = lsn;
    context_.issue_task_num_++;
  }
  return ret;
}

int ObLogRestoreHandler::try_retire_task(ObFetchLogTask &task, bool &done)
{
  done = false;
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!task.is_valid() || task.id_.id() != id_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(task));
  } else if (! is_strong_leader(role_) || NULL == parent_ || is_service_log_source_type(parent_->get_source_type())) {
    done = true;
    CLOG_LOG(INFO, "ls not leader or invalid source, stale task, just skip it", K(task), K(role_), KPC(parent_));
  } else if (OB_UNLIKELY(task.proposal_id_ != proposal_id_
        || task.version_ != context_.issue_version_)) {
    done = true;
    CLOG_LOG(INFO, "stale task, just skip it", K(task), KPC(this));
  } else if (context_.max_fetch_lsn_.is_valid() && context_.max_fetch_lsn_ >= task.end_lsn_) {
    CLOG_LOG(INFO, "restore max_lsn bigger than task end_lsn, just skip it", K(task), KPC(this));
    done = true;
    context_.issue_task_num_--;
  }
  return ret;
}

int ObLogRestoreHandler::need_schedule(bool &need_schedule,
    int64_t &proposal_id,
    ObRemoteFetchContext &context) const
{
  int ret = OB_SUCCESS;
  need_schedule = false;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(parent_)) {
    // do nothing
  } else {
    need_schedule = is_strong_leader(role_) && ! restore_to_end_unlock_();
    proposal_id = proposal_id_;
    context = context_;
  }
  if (! need_schedule) {
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000L)) {
      CLOG_LOG(INFO, "restore not schedule", KPC(this), KPC(parent_));
    }
  }
  return ret;
}

bool ObLogRestoreHandler::need_update_source() const
{
  RLockGuard guard(lock_);
  return is_strong_leader(role_);
}

void ObLogRestoreHandler::mark_error(share::ObTaskId &trace_id,
                                     const int ret_code,
                                     const palf::LSN &lsn,
                                     const ObLogRestoreErrorContext::ErrorType &error_type)
{
  int ret = OB_SUCCESS;
  palf::LSN end_lsn;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_in_stop_state_) {
    ret = OB_IN_STOP_STATE;
  } else if (! is_strong_leader(role_)) {
    CLOG_LOG(INFO, "not leader, no need record error", K(id_), K(ret_code));
  } else if (OB_FAIL(palf_handle_->get_end_lsn(end_lsn))) {
    CLOG_LOG(WARN, "get end_lsn failed", K(id_));
  } else if (end_lsn < lsn) {
    CLOG_LOG(WARN, "end_lsn smaller than error lsn, just skip", K(id_), K(end_lsn), K(lsn), KPC(parent_), KPC(this));
  } else if (OB_SUCCESS == context_.error_context_.ret_code_ || OB_TIMEOUT == context_.error_context_.ret_code_) {
    context_.error_context_.error_type_ = error_type;
    context_.error_context_.ret_code_ = ret_code;
    context_.error_context_.trace_id_.set(trace_id);
    context_.error_context_.err_lsn_ = lsn;
    if ((OB_TIMEOUT == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type)
      || (OB_TENANT_NOT_EXIST == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type)
      || (OB_TENANT_NOT_IN_SERVER == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type)
      || (OB_IN_STOP_STATE == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type)
      || (OB_SERVER_IS_INIT == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type)
      || (OB_ERR_OUT_OF_LOWER_BOUND == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type)
      || (OB_SIZE_OVERFLOW == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type)) {
      CLOG_LOG(WARN, "fetch log failed in restore", KPC(parent_), KPC(this));
    } else if (OB_SUCCESS != ret_code) {
      CLOG_LOG(ERROR, "fatal error occur in restore", KPC(parent_), KPC(this));
    }
  }
}

int ObLogRestoreHandler::get_restore_error(share::ObTaskId &trace_id, int &ret_code, bool &error_exist)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (OB_FAIL(get_restore_error_unlock_(trace_id, ret_code, error_exist))) {
    CLOG_LOG(WARN, "fail to get restore_error");
  }
  return ret;
}

int ObLogRestoreHandler::get_restore_error_unlock_(share::ObTaskId &trace_id, int &ret_code, bool &error_exist)
{
  int ret = OB_SUCCESS;
  error_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (! is_strong_leader(role_)) {
    // not leader, not report
  } else if (OB_SUCCESS == context_.error_context_.ret_code_) {
    // no error, not report
  } else {
    trace_id.set(context_.error_context_.trace_id_);
    ret_code = context_.error_context_.ret_code_;
    error_exist = true;
  }
  return ret;
}
int ObLogRestoreHandler::update_location_info(ObRemoteLogParent *source)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(source)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "source is NULL", K(ret), K(source));
  } else if (OB_ISNULL(parent_) || ! is_strong_leader(role_)) {
    // stale task, just skip
  } else if (source->get_source_type() != parent_->get_source_type()) {
    CLOG_LOG(INFO, "source type not consistent with parent, just skip", KPC(source), KPC(this));
  } else if (OB_FAIL(parent_->update_locate_info(*source))) {
    CLOG_LOG(WARN, "update location info failed", K(ret), KPC(source), KPC(this));
  }
  return ret;
}

void ObLogRestoreHandler::alloc_source(const ObLogRestoreSourceType &type)
{
  if (parent_ != NULL && parent_->get_source_type() != type) {
    ObResSrcAlloctor::free(parent_);
    parent_ = NULL;
  }

  if (NULL == parent_) {
    parent_ = ObResSrcAlloctor::alloc(type, share::ObLSID(id_));
  }
}

int ObLogRestoreHandler::check_restore_done(const SCN &recovery_end_scn, bool &done)
{
  int ret = OB_SUCCESS;
  palf::PalfGroupBufferIterator iter;
  SCN end_scn;
  palf::LogGroupEntry entry;
  SCN entry_scn;
  palf::LSN end_lsn;
  int64_t id = 0;
  done = false;
  ObLogService *logservice = MTL(ObLogService*);
  {
    RLockGuard guard(lock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
    } else if (is_in_stop_state_) {
      ret = OB_IN_STOP_STATE;
      CLOG_LOG(WARN, "ObLogRestoreHandler is stopped", K(ret), KPC(this));
    } else if (OB_UNLIKELY(!recovery_end_scn.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid argument", K(ret), K(recovery_end_scn));
    } else if (OB_ISNULL(logservice)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "unexpected error, ObLogService must not be NULL", K(ret), K(recovery_end_scn));
    } else if (restore_context_.seek_done_) {
      end_lsn = restore_context_.lsn_;
    } else if (OB_FAIL(palf_handle_->get_end_scn(end_scn))) {
      CLOG_LOG(WARN, "get end scn failed", K(ret), K_(id));
    } else if (end_scn < recovery_end_scn) {
      ret = OB_EAGAIN;
      CLOG_LOG(WARN, "log restore not finish", K(ret), K_(id), K(end_scn), K(recovery_end_scn));
    } else if (OB_FAIL(seek_log_iterator(ObLSID(id_), recovery_end_scn, iter))) {
      CLOG_LOG(WARN, "palf seek failed", K(ret), K_(id));
    } else if (OB_FAIL(iter.set_io_context(palf::LogIOContext(MTL_ID(), id_, palf::LogIOUser::RESTORE)))) {
      CLOG_LOG(WARN, "set_io_context failed", K(ret), K_(id));
    } else if (OB_FAIL(iter.next())) {
      CLOG_LOG(WARN, "next entry failed", K(ret));
    } else if (OB_FAIL(iter.get_entry(entry, end_lsn))) {
      CLOG_LOG(WARN, "gen entry failed", K(ret), K_(id), K(iter));
    } else if (entry.get_scn() == recovery_end_scn) {
      // if the max log scn equals to recovery_end_scn, the max log should be replayed
      // otherwise the max log should not be replayed
      end_lsn = end_lsn + entry.get_serialize_size();
    }
    id = ATOMIC_LOAD(&id_);
  }

  // update restore context
  {
    WLockGuard guard(lock_);
    if (OB_SUCC(ret) && ! restore_context_.seek_done_) {
      restore_context_.lsn_ = end_lsn;
      restore_context_.seek_done_ = true;
      CLOG_LOG(INFO, "update restore context", K(id), K_(restore_context));
    }
  }

  if (OB_SUCC(ret)) {
    ObLogReplayService *replayservice = logservice->get_log_replay_service();
    if (OB_ISNULL(replayservice)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "unexpected error, ObLogReplayService must not be NULL", K(ret), K(recovery_end_scn));
    } else if (OB_FAIL(replayservice->is_replay_done(ObLSID(id), end_lsn, done))) {
      CLOG_LOG(WARN, "is_replay_done failed", K(ret), K(id), K(end_lsn));
    } else if (! done) {
      ret = OB_EAGAIN;
      CLOG_LOG(WARN, "log restore finish, and log replay not finish",
          K(ret), K(id), K(end_lsn), K(end_scn), K(recovery_end_scn));
    } else {
      CLOG_LOG(TRACE, "check restore done succ", K(ret), K(id));
    }
  }
  return ret;
}

int ObLogRestoreHandler::check_restore_to_newest(share::SCN &end_scn, share::SCN &archive_scn)
{
  int ret = OB_SUCCESS;
  ObRemoteSourceGuard guard;
  ObRemoteLogParent *source = NULL;
  ObLogArchivePieceContext *piece_context = NULL;
  share::ObBackupDest *dest = NULL;
  palf::LSN end_lsn;
  end_scn = SCN::min_scn();
  archive_scn = SCN::min_scn();
  SCN restore_scn = SCN::min_scn();
  CLOG_LOG(INFO, "start check restore to newest", K(id_));
  {
    RLockGuard lock_guard(lock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      CLOG_LOG(WARN, "ObLogRestoreHandler not init", K(ret), KPC(this));
    } else if (is_in_stop_state_) {
      ret = OB_IN_STOP_STATE;
      CLOG_LOG(WARN, "ObLogRestoreHandler is in stop state", K(ret), KPC(this));
    } else if (! is_strong_leader(role_)) {
      ret = OB_NOT_MASTER;
      CLOG_LOG(WARN, "not leader", K(ret), KPC(this));
    } else if (OB_FAIL(palf_handle_->get_end_lsn(end_lsn))) {
      CLOG_LOG(WARN, "get end lsn failed", K(id_));
    } else if (OB_FAIL(palf_handle_->get_end_scn(end_scn))) {
      CLOG_LOG(WARN, "get end scn failed", K(id_));
    } else if (FALSE_IT(deep_copy_source_(guard))) {
    } else if (OB_ISNULL(source = guard.get_source())) {
      ret = OB_EAGAIN;
      CLOG_LOG(WARN, "invalid source", K(ret), KPC(this), KPC(source));
    } else if (! source->is_valid()) {
      ret = OB_EAGAIN;
      CLOG_LOG(WARN, "source is invalid", K(ret), KPC(this), KPC(source));
    }
  }

  if (OB_SUCC(ret) && NULL != source) {
    if (share::is_location_log_source_type(source->get_source_type())) {
      ObRemoteLocationParent *location_source = dynamic_cast<ObRemoteLocationParent *>(source);
      ret = check_restore_to_newest_from_archive_(*location_source, end_lsn, end_scn, archive_scn);
    } else if (share::is_service_log_source_type(source->get_source_type())) {
      ObRemoteSerivceParent *service_source = dynamic_cast<ObRemoteSerivceParent *>(source);
      share::ObRestoreSourceServiceAttr *service_attr = NULL;
      service_source->get(service_attr, restore_scn);
      ret = check_restore_to_newest_from_service_(*service_attr, end_scn, archive_scn);
    } else if (share::is_raw_path_log_source_type(source->get_source_type())) {
      ObRemoteRawPathParent *rawpath_source = dynamic_cast<ObRemoteRawPathParent *>(source);
      ObLogRawPathPieceContext *rawpath_ctx = NULL;
      rawpath_source->get(rawpath_ctx, restore_scn);
      ret = check_restore_to_newest_from_rawpath_(*rawpath_ctx, end_lsn, end_scn, archive_scn);
    } else {
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

int ObLogRestoreHandler::submit_sorted_task(ObFetchLogTask &task)
{
  WLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (! is_strong_leader(role_)) {
    ret = OB_NOT_MASTER;
    CLOG_LOG(WARN, "restore_handler not master", K(ret), KPC(this));
  } else if (OB_UNLIKELY(! task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(task));
  } else if (OB_UNLIKELY(task.iter_.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "task iterator is empty", K(ret), K(task));
  } else if (OB_FAIL(context_.submit_array_.push_back(&task))) {
    CLOG_LOG(WARN, "push back failed", K(ret), K(task));
  } else {
    lib::ob_sort(context_.submit_array_.begin(), context_.submit_array_.end(), FetchLogTaskCompare());
  }
  return ret;
}

int ObLogRestoreHandler::get_next_sorted_task(ObFetchLogTask *&task)
{
  int ret = OB_SUCCESS;
  palf::LSN max_lsn;
  ObFetchLogTask *first = NULL;
  task = NULL;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_in_stop_state_) {
    ret = OB_IN_STOP_STATE;
  } else if (! is_strong_leader(role_)) {
    ret = OB_NOT_MASTER;
  } else if (context_.submit_array_.empty()) {
    // sorted_array is empty, do nothing
  } else if (FALSE_IT(first = context_.submit_array_.at(0))) {
    // get the first one
  } else if (OB_FAIL(palf_handle_->get_max_lsn(max_lsn))) {
    CLOG_LOG(WARN, "get max lsn failed", K(ret), K_(id));
  } else if (max_lsn < first->start_lsn_) {
    // check the first task if is in turn, skip it if not
    CLOG_LOG(TRACE, "task not in turn", KPC(first), K(max_lsn));
  } else if (context_.submit_array_.count() == 1) {
    // only one task in array, just pop it
    context_.submit_array_.pop_back();
    task = first;
  } else {
    // more than one task, replace first and end, and sort again
    ObFetchLogTask *tmp_task = NULL;
    context_.submit_array_.pop_back(tmp_task);
    context_.submit_array_.at(0) = tmp_task;
    lib::ob_sort(context_.submit_array_.begin(), context_.submit_array_.end(), FetchLogTaskCompare());
    task = first;
  }
  return ret;
}

int ObLogRestoreHandler::diagnose(RestoreDiagnoseInfo &diagnose_info) const
{
  int ret = OB_SUCCESS;
  diagnose_info.restore_context_info_.reset();
  diagnose_info.restore_context_info_.reset();
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char trace_id[MAX_TRACE_ID_LENGTH];
  const bool need_ignore_invalid = true;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (FALSE_IT(diagnose_info.restore_role_ = role_)) {
  } else if (FALSE_IT(diagnose_info.restore_proposal_id_ = proposal_id_)) {
  } else if (OB_FAIL(diagnose_info.restore_context_info_.append_fmt("issue_task_num:%ld; "
                                                                    "last_fetch_ts:%ld; "
                                                                    "max_submit_lsn:%ld; "
                                                                    "max_fetch_lsn:%ld; "
                                                                    "max_fetch_scn:%ld; ",
                                                                    context_.issue_task_num_,
                                                                    context_.last_fetch_ts_,
                                                                    context_.max_submit_lsn_.val_,
                                                                    context_.max_fetch_lsn_.val_,
                                                                    context_.max_fetch_scn_.convert_to_ts(need_ignore_invalid)))) {
    CLOG_LOG(WARN, "append restore_context_info failed", K(ret), K(context_));
  } else if (FALSE_IT(context_.error_context_.trace_id_.to_string(trace_id, sizeof(trace_id)))) {
  } else if (OB_FAIL(diagnose_info.restore_err_context_info_.append_fmt("ret_code:%d; "
                                                                        "trace_id:%s; ",
                                                                        context_.error_context_.ret_code_,
                                                                        trace_id))) {
    CLOG_LOG(WARN, "append restore_context_info failed", K(ret), K(context_));
  }
  return ret;
}

int ObLogRestoreHandler::refresh_error_context()
{
  int ret = OB_SUCCESS;
  palf::LSN end_lsn;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_in_stop_state_) {
    ret = OB_IN_STOP_STATE;
  } else if (! is_strong_leader(role_)) {
    CLOG_LOG(TRACE, "not leader, no need refresh error context", K(id_));
  } else if (OB_FAIL(palf_handle_->get_end_lsn(end_lsn))) {
    CLOG_LOG(WARN, "get end_lsn failed", K(id_));
  } else if (end_lsn > context_.error_context_.err_lsn_ && OB_SUCCESS != context_.error_context_.ret_code_) {
    context_.error_context_.reset();
    CLOG_LOG(INFO, "flush error context to success", K(id_), K(context_), K(end_lsn), KPC(parent_), KPC(this));
  }
  return ret;
}

bool ObLogRestoreHandler::restore_to_end() const
{
  RLockGuard guard(lock_);
  return restore_to_end_unlock_();
}

void ObLogRestoreHandler::clear_transport_task_queue_()
{
  transport_task_queue_.clear();
}

int ObLogRestoreHandler::submit_transport_task(const ObLogTransportReq &req)
{
  int ret = OB_SUCCESS;
  const uint64_t early_drop_lsn_gap_limit = static_cast<uint64_t>(palf::FOLLOWER_DEFAULT_GROUP_BUFFER_SIZE);
  palf::LSN end_lsn;
  int64_t end_log_id = OB_INVALID_LOG_ID;
  ObLogTransportTaskHolder task_holder;

  {
    RLockGuard guard(lock_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      CLOG_LOG(WARN, "restore handler not init", K(ret), K_(id));
    } else if (is_in_stop_state_) {
      ret = OB_IN_STOP_STATE;
      CLOG_LOG(WARN, "restore handler is stopping", K(ret), K_(id));
    } else if (OB_UNLIKELY(!req.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid request", K(ret), K(req));
    } else if (OB_ISNULL(palf_handle_) || !palf_handle_->is_valid()) {
      ret = OB_NOT_INIT;
      CLOG_LOG(WARN, "palf handle is invalid", K(ret), K_(id), KP_(palf_handle));
    } else if (false == is_strong_leader(role_)) {
      ret = OB_NOT_MASTER;
      CLOG_LOG(WARN, "restore handler not master", K(ret), K_(id));
    } else if (OB_FAIL(palf_handle_->get_end_lsn(end_lsn))) {
      CLOG_LOG(WARN, "get end lsn failed", K(ret), K_(id));
    } else if (OB_FAIL(palf_handle_->get_end_log_id(end_log_id))) {
      CLOG_LOG(WARN, "get end log id failed", K(ret), K_(id));
    } else if (end_lsn.is_valid() && req.end_lsn_ <= end_lsn) {
      ret = OB_ERR_OUT_OF_LOWER_BOUND;
      CLOG_LOG(TRACE, "drop already committed transport task", KR(ret), K_(id), K(end_lsn), K(req));
    } else if (end_lsn.is_valid() && req.end_lsn_ > end_lsn
              && req.end_lsn_ - end_lsn > early_drop_lsn_gap_limit) {
      transport_task_queue_.inc_early_drop_far_lsn();
      ret = OB_ERR_OUT_OF_UPPER_BOUND;
      CLOG_LOG(WARN, "early drop far lsn transport task", KR(ret), K_(id), K(end_lsn), "task_end_lsn",
        req.end_lsn_, "lsn_gap", req.end_lsn_ - end_lsn,
        K(early_drop_lsn_gap_limit), K(req.log_size_));
    } else {
    }
  }

  // lock-free submit transport task
  if (OB_FAIL(ret)) { // skip if previous check failed
  } else if (OB_FAIL(ObLogTransportTaskHolder::borrowed(&req, task_holder))) {
    CLOG_LOG(WARN, "build borrowed transport task holder failed", K(ret), K(req));
  } else if (OB_FAIL(transport_task_queue_.push(std::move(task_holder), end_log_id))) {
    if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
      CLOG_LOG(TRACE, "drop stale transport task by queue", K_(id), K(end_log_id), K(req));
      ret = OB_SUCCESS;
    } else if (OB_ENTRY_EXIST == ret) {
      CLOG_LOG(TRACE, "drop duplicate transport task by queue", K_(id), K(end_log_id), K(req));
      ret = OB_SUCCESS;
    } else if (OB_ERR_OUT_OF_UPPER_BOUND == ret) {
      CLOG_LOG(WARN, "task out of window", K(ret), K_(id), K(req.start_lsn_), K(req.end_lsn_));
    } else if (OB_SIZE_OVERFLOW == ret) {
      CLOG_LOG(WARN, "transport task queue is full", K(ret), K_(id), K(req.start_lsn_), K(req.end_lsn_));
    } else {
      CLOG_LOG(WARN, "push transport task failed", K(ret), K_(id), K(req.start_lsn_), K(req.end_lsn_));
    }
  } else {
    CLOG_LOG(TRACE, "submit_transport_task succ", K(id_), K(req.start_lsn_), K(req.end_lsn_));
  }
  return ret;
}

int ObLogRestoreHandler::process_transport_tasks(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  common::ObRole role = ObRole::INVALID_ROLE;
  int64_t proposal_id = INVALID_PROPOSAL_ID;

  if (OB_FAIL(get_role(role, proposal_id))) {
    CLOG_LOG(WARN, "get role failed", K(ret), K(id_));
  } else if (!is_strong_leader(role)) {
    ret = OB_NOT_MASTER;
    CLOG_LOG(TRACE, "not leader, no need process transport tasks", K(ret), K(id_));
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
    CLOG_LOG(INFO, "not leader, no need process transport tasks",
             K(ret), K(id_), K(role), K(proposal_id), K(GCONF.self_addr_));
#endif
  } else {
    palf::LSN current_end_lsn;
    int64_t end_log_id = OB_INVALID_LOG_ID;
    if (OB_FAIL(palf_handle_->get_end_lsn(current_end_lsn))) {
      CLOG_LOG(WARN, "get end lsn failed", K(ret), K(id_));
    } else if (OB_FAIL(palf_handle_->get_end_log_id(end_log_id))) {
      CLOG_LOG(WARN, "get end log id failed", K(ret), K(id_));
    } else {
      int64_t processed_count = 0;
      ret = transport_task_queue_.process(proposal_id,
                                          end_log_id,
                                          processed_count,
                                          batch_size);
      CLOG_LOG(INFO, "process transport tasks",
               K(ret), K(id_), K(role), K(proposal_id),
               K(current_end_lsn), K(end_log_id), K(processed_count), K(batch_size), K(GCONF.self_addr_));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_LS_STATE_NOT_MATCH);
int ObLogRestoreHandler::check_restore_to_newest_from_service_(
    const share::ObRestoreSourceServiceAttr &service_attr,
    const share::SCN &end_scn,
    share::SCN &archive_scn)
{
  int ret = OB_SUCCESS;
  bool offline_log_exist = false;
  palf::AccessMode access_mode;
  SMART_VAR(share::ObLogRestoreProxyUtil, proxy_util) {
    if (OB_FAIL(proxy_util.init_with_service_attr(MTL_ID(), &service_attr))) {
      CLOG_LOG(WARN, "proxy_util init failed", K(id_), K(service_attr));
    } else if (OB_FAIL(proxy_util.get_max_log_info(share::ObLSID(id_), access_mode, archive_scn))) {
      // OB_ENTRY_NOT_EXIST, ls not exist in gv$ob_log_stat, a) ls has no leader; b) access virtual table failed; c) ls gc
      if (OB_ENTRY_NOT_EXIST == ret) {
        // get ls from dba_ob_ls
        // 1. OB_LS_NOT_EXIST, ls gc in log restore source tenant, check if offline_log already transported successfully
        // 2. OB_SUCCESS, ls still exist, check in next turn
        // 3. other error code
        if (OB_FAIL(proxy_util.is_ls_existing(share::ObLSID(id_))) && OB_LS_NOT_EXIST != ret) {
          CLOG_LOG(WARN, "get_ls failed", K(id_));
        } else if (OB_SUCCESS == ret) {
          ret = OB_EAGAIN;
          CLOG_LOG(WARN, "get_ls succ, while get ls max_log info failed, just retry", K(id_));
        } else if (OB_FAIL(check_if_ls_gc_(offline_log_exist))) {
          CLOG_LOG(WARN, "check ls_gc failed", K(id_));
        } else if (offline_log_exist) {
          archive_scn = end_scn;
          CLOG_LOG(INFO, "check ls_gc succ, set archive_scn equals to end_scn", K(id_), K(end_scn), K(service_attr));
        } else {
          ret = OB_ERR_OUT_OF_LOWER_BOUND;
          CLOG_LOG(ERROR, "ls not exist in primary while offline_log not exist in standby, check log gap",
              K(id_), K(end_scn), K(service_attr));
        }
      } else {
        CLOG_LOG(WARN, "get max_log info failed", K(id_));
      }
    } else if (!palf::is_valid_access_mode(access_mode) || palf::AccessMode::RAW_WRITE != access_mode) {
      ret = OB_SOURCE_LS_STATE_NOT_MATCH;
      CLOG_LOG(WARN, "access_mode not match, check if ls gc in log restore source tenant", K(id_), K(access_mode));
      // rewrite ret code, retry next time
      ret = OB_EAGAIN;
    } else if (end_scn < archive_scn) {
      CLOG_LOG(INFO, "end_scn smaller than archive_scn", K(id_), K(archive_scn), K(end_scn));
    } else {
      CLOG_LOG(INFO, "check_restore_to_newest succ", K(id_), K(archive_scn), K(end_scn));
    }
  }
  if (OB_UNLIKELY(ERRSIM_LS_STATE_NOT_MATCH)) {
    ret = OB_SUCC(ret) ? OB_SOURCE_LS_STATE_NOT_MATCH : ret;
    CLOG_LOG(WARN, "ERRSIM_LS_STATE_NOT_MATCH is on", KR(ret));
  }
  // if connect to source tenant denied, rewrite ret_code
  if (-ER_ACCESS_DENIED_ERROR == ret) {
    ret = OB_PASSWORD_WRONG;
  }
  return ret;
}

int ObLogRestoreHandler::check_restore_to_newest_from_archive_(
    ObRemoteLocationParent &location_parent,
    const palf::LSN &end_lsn,
    const share::SCN &end_scn,
    share::SCN &archive_next_scn)
{
  int ret = OB_SUCCESS;
  ObLogArchivePieceContext *piece_context = NULL;
  share::ObBackupDest *dest = NULL;
  share::SCN restore_scn;
  if (OB_FAIL(get_next_log_after_end_lsn_(location_parent, end_lsn, end_scn, archive_next_scn))) {
    CLOG_LOG(WARN, "get max archive log failed", K(id_));
  } else if (end_scn < archive_next_scn) {
    CLOG_LOG(INFO, "end_scn smaller than archive_scn", K(id_), K(archive_next_scn), K(end_scn));
  } else {
    CLOG_LOG(INFO, "check_restore_to_newest succ", K(id_), K(archive_next_scn), K(end_scn));
  }
  return ret;
}

int ObLogRestoreHandler::get_next_log_after_end_lsn_(ObRemoteLocationParent &location_parent,
  const palf::LSN &end_lsn, const share::SCN &end_scn, share::SCN &archive_scn)
{
  int ret = OB_SUCCESS;

  class GetSourceFunctor {
  public:
    GetSourceFunctor(ObRemoteLocationParent &location_parent):
        location_parent_(location_parent) {}
    int operator()(const share::ObLSID &id, ObRemoteSourceGuard &guard)
    {
      int ret = OB_SUCCESS;
      ObRemoteLocationParent *location_parent = static_cast<ObRemoteLocationParent*>(ObResSrcAlloctor::alloc(share::ObLogRestoreSourceType::LOCATION, id));
      if (OB_ISNULL(location_parent)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        CLOG_LOG(WARN, "failed to allocate location_parent", K(id));
      } else if (OB_FAIL(location_parent_.deep_copy_to(*location_parent))) {
        CLOG_LOG(WARN, "failed to deep copy to location_parent");
      } else if (OB_FAIL(guard.set_source(location_parent))) {
        CLOG_LOG(WARN, "failed to set location source");
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(location_parent)) {
        ObResSrcAlloctor::free(location_parent);
        location_parent = nullptr;
      }
      return ret;
    }
  private:
    ObRemoteLocationParent &location_parent_;
  };
  GetSourceFunctor get_source_func(location_parent);
  ObRemoteIGroupEntryIterator remote_iter(get_source_func);
  LargeBufferPool tmp_buffer_pool;
  ObLogExternalStorageHandler tmp_handler;
  ipalf::IGroupEntry tmp_entry;
  LSN tmp_lsn;
  const char *tmp_buf = NULL;
  int64_t tmp_buf_len = 0;
  constexpr int64_t DEFAULT_BUF_SIZE = 64L * 1024 * 1024;
  const bool enable_logservice = GCONF.enable_logservice;

  if (OB_FAIL(tmp_buffer_pool.init("TmpLargePool", 1024L * 1024 * 1024))) {
    CLOG_LOG(WARN, "failed to init tmp_buffer_pool");
  } else if (OB_FAIL(tmp_handler.init())) {
    CLOG_LOG(WARN, "failed to init tmp_handler");
  } else if (OB_FAIL(tmp_handler.start(0))) {
    CLOG_LOG(WARN, "failed to start tmp_handler");
  } else if (OB_FAIL(remote_iter.init(MTL_ID(), ObLSID(id_), end_scn, end_lsn, palf::LSN(palf::LOG_MAX_LSN_VAL), &tmp_buffer_pool, &tmp_handler, DEFAULT_BUF_SIZE, enable_logservice))) {
    CLOG_LOG(WARN, "failed to init remote_iter");
  } else if (OB_FAIL(remote_iter.next(tmp_entry, tmp_lsn, tmp_buf, tmp_buf_len))) {
    if (OB_ITER_END == ret) {
      archive_scn = end_scn;
      ret = OB_SUCCESS;
    } else if (OB_ARCHIVE_ROUND_NOT_CONTINUOUS == ret) {
      // return
      CLOG_LOG(WARN, "round not continuous, there could be newer archivelog");
    } else {
      CLOG_LOG(WARN, "failed to iterate remote_log");
    }
  } else {
    archive_scn = tmp_entry.get_scn();
  }
  remote_iter.reset();
  tmp_handler.stop();
  tmp_handler.wait();
  tmp_handler.destroy();
  tmp_buffer_pool.destroy();

  return ret;
}

int ObLogRestoreHandler::check_restore_to_newest_from_rawpath_(ObLogRawPathPieceContext &rawpath_ctx,
    const palf::LSN &end_lsn, const share::SCN &end_scn, share::SCN &archive_scn)
{
  int ret = OB_SUCCESS;
  const ObLSID ls_id(id_);

  if (! rawpath_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "rawpath ctx is invalid");
  } else {
    palf::LSN archive_lsn;
    if (OB_FAIL(rawpath_ctx.get_max_archive_log(archive_lsn, archive_scn))) {
      CLOG_LOG(WARN, "fail to get max archive log", K_(id));
    } else if (archive_lsn <= end_lsn && archive_scn == SCN::min_scn()) {
      archive_scn = end_scn;
      CLOG_LOG(INFO, "rewrite archive_scn while end_lsn equals to archive_lsn and archive_scn not got",
        K(id_), K(archive_lsn), K(archive_scn), K(end_lsn), K(end_scn));
    } else if (end_scn < archive_scn) {
      CLOG_LOG(INFO, "end_scn smaller than archive_scn", K_(id), K(end_scn));
    } else if (end_lsn < archive_lsn) {
      ret = OB_EAGAIN;
      CLOG_LOG(INFO, "end_lsn smaller than archive_lsn", K_(id), K(end_lsn));
    } else {
      CLOG_LOG(INFO, "check_restore_to_newest succ", K(id_), K(archive_scn), K(end_scn), K(end_lsn));
    }
  }
  return ret;
}

int ObLogRestoreHandler::check_if_ls_gc_(bool &done)
{
  int ret = OB_SUCCESS;
  share::SCN offline_scn;
  done = false;
  if (OB_FAIL(get_offline_scn_(offline_scn))) {
    CLOG_LOG(WARN, "get offline_scn failed", K(offline_scn), K(id_));
  } else if (offline_scn.is_valid()) {
    done = true;
    CLOG_LOG(INFO, "offline_scn is valid, ls gc", K(id_), K(offline_scn));
  } else if (OB_FAIL(check_offline_log_(done))) {
    CLOG_LOG(WARN, "check offline_log failed", K(id_), K(offline_scn));
  } else if (!done) {
    // if check offline_log failed, double check if offline_scn valid
    if (OB_FAIL(get_offline_scn_(offline_scn))) {
      CLOG_LOG(WARN, "get offline_scn failed", K(offline_scn), K(id_));
    } else if (offline_scn.is_valid()) {
      done = true;
      CLOG_LOG(INFO, "offline_scn is valid, ls gc", K(id_), K(offline_scn));
    }
  }
  return ret;
}

// function out of lock, use palf guard to open palf
int ObLogRestoreHandler::check_offline_log_(bool &done)
{
  int ret = OB_SUCCESS;
  share::SCN replayed_scn;
  ipalf::IPalfIterator<ipalf::ILogEntry> iter;
  done = false;
  ObLogService *logservice = MTL(ObLogService*);
  ObLogReplayService *replayservice = NULL;
  if (OB_ISNULL(logservice)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected error, ObLogService must not be NULL");
  } else if (FALSE_IT(replayservice = logservice->get_log_replay_service())) {
  } else if (OB_ISNULL(replayservice)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected error, ObLogReplayService must not be NULL");
  } else if (OB_FAIL(replayservice->get_max_replayed_scn(share::ObLSID(id_), replayed_scn))) {
    CLOG_LOG(WARN, "get replayed_lsn failed", K(id_));
  } else if (OB_FAIL(seek_log_iterator(ObLSID(id_), replayed_scn, iter))) {
    CLOG_LOG(WARN, "seek failed", K(id_));
  } else if (OB_FAIL(iter.set_io_context(palf::LogIOContext(MTL_ID(), id_, palf::LogIOUser::RESTORE)))) {
    CLOG_LOG(WARN, "set_io_context failed", K(id_));
  } else {
    ipalf::ILogEntry entry;
    palf::LSN lsn;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.next())) {
        CLOG_LOG(WARN, "next failed", K(id_), K(lsn));
      } else if (OB_FAIL(iter.get_entry(entry, lsn))) {
        CLOG_LOG(WARN, "get entry failed", K(id_), K(lsn), K(entry));
      } else {
        int64_t header_pos = 0;
        int64_t log_pos = 0;
        const char *log_buf = entry.get_data_buf();
        const int64_t log_length = entry.get_data_len();
        logservice::ObLogBaseHeader header;
        const int64_t header_size = header.get_serialize_size();
        if (OB_FAIL(header.deserialize(log_buf, header_size, header_pos))) {
          CLOG_LOG(ERROR, "ObLogBaseHeader deserialize failed", K(id_), K(lsn), K(entry));
        } else if (OB_UNLIKELY(!header.is_valid())) {
          ret = OB_INVALID_DATA;
          CLOG_LOG(ERROR, "log base header not valid", K(id_), K(lsn), K(entry), K(header));
        } else if (OB_UNLIKELY(header_pos >= log_length)) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "unexpected log pos", K(id_), K(header_pos), K(log_length), K(entry));
        } else if (logservice::GC_LS_LOG_BASE_TYPE == header.get_log_type()) {
          logservice::ObGCLSLog gc_log;
          if (OB_FAIL(gc_log.deserialize(log_buf, log_length, log_pos))) {
            CLOG_LOG(ERROR, "gc_log deserialize failed", K(id_), K(log_pos), K(log_length), K(entry));
          } else if (logservice::ObGCLSLOGType::OFFLINE_LS == gc_log.get_log_type()) {
            done = true;
            CLOG_LOG(INFO, "offline_log exist", K(id_), K(gc_log), K(lsn), K(entry));
            break;
          }
        }
      }
    } // while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

bool ObLogRestoreHandler::restore_to_end_unlock_() const
{
  int ret = OB_SUCCESS;
  bool bret = false;
  share::SCN scn;
  share::SCN recovery_end_scn;
  if (NULL == parent_) {
    bret = false;
  } else if (parent_->to_end()) {
    bret = true;
  } else if (OB_ISNULL(palf_handle_) || OB_FAIL(palf_handle_->get_end_scn(scn))) {
    CLOG_LOG(WARN, "get end scn failed", K(ret), K(id_), KP(palf_handle_));
  } else {
    parent_->get_upper_limit_scn(recovery_end_scn);
    bret = scn >= recovery_end_scn;
    CLOG_LOG(INFO, "check restore to end", K(recovery_end_scn), K(scn));
  }
  return bret;
}

int ObLogRestoreHandler::get_ls_restore_status_info(RestoreStatusInfo &restore_status_info)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  LSN lsn;
  share::SCN scn;
  share::ObTaskId trace_id;
  int ret_code = OB_SUCCESS;
  bool error_exist = false;
  bool is_leader = true;
  RestoreSyncStatus sync_status;
  ObRole palf_role = FOLLOWER;
  int64_t palf_proposal_id = -1;
  bool is_pending_state = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_in_stop_state_) {
    ret = OB_IN_STOP_STATE;
  } else if (OB_FAIL(palf_handle_->get_role(palf_role, palf_proposal_id, is_pending_state))) {
    CLOG_LOG(WARN, "fail to get palf role", K(ret), K_(id));
  } else if (LEADER != palf_role || true == is_pending_state) {
    CLOG_LOG(TRACE, "palf is not leader when get ls restore status info", K_(id), K(palf_role), K(is_pending_state));
  } else if (LEADER == palf_role && !is_strong_leader(role_)) {
    is_leader = false;
    CLOG_LOG(WARN, "restore handler not leader", K_(id), K(role_), K(palf_role));
    restore_status_info.ls_id_ = id_;
    restore_status_info.err_code_ = OB_NOT_MASTER;
    restore_status_info.sync_lsn_ = 0;
    restore_status_info.sync_scn_ = SCN::min_scn();
    if (OB_FAIL(get_restore_sync_status(restore_status_info.err_code_, context_.error_context_.error_type_, sync_status))) {
      CLOG_LOG(WARN, "fail to get restore sync status", K_(restore_status_info.err_code), K(sync_status));
    } else if (OB_FALSE_IT(restore_status_info.sync_status_ = sync_status)) { // set sync_status before get_restore_comment
    } else if (OB_FAIL(restore_status_info.get_restore_comment())) {
      CLOG_LOG(WARN, "fail to get restore comment", K(sync_status));
    } else {
      CLOG_LOG(TRACE, "success to get error code and message", K(restore_status_info));
    }
  } else if (OB_FAIL(palf_handle_->get_end_lsn(lsn))) {
    CLOG_LOG(WARN, "fail to get end lsn when get ls restore status info");
  } else if (OB_FAIL(palf_handle_->get_end_scn(scn))) {
    CLOG_LOG(WARN, "fail to get end scn");
  } else if (OB_FAIL(get_restore_error_unlock_(trace_id, ret_code, error_exist))) {
    CLOG_LOG(WARN, "fail to get restore error");
  } else if (error_exist) {
    CLOG_LOG(TRACE, "start to mark restore sync error", K(trace_id), K(ret_code), K(context_.error_context_.error_type_));
    if (OB_FAIL(get_restore_sync_status(ret_code, context_.error_context_.error_type_, sync_status))) {
      CLOG_LOG(WARN, "fail to get sync status", K(ret_code), K(context_.error_context_.error_type_), K(sync_status));
    } else {
      restore_status_info.sync_status_ = sync_status;
    }
  } else if (restore_to_end_unlock_()) {
    restore_status_info.sync_status_ = RestoreSyncStatus::RESTORE_SYNC_SUSPEND;
    CLOG_LOG(TRACE, "restore suspend", K(error_exist), K(restore_status_info.sync_status_));
  } else {
    restore_status_info.sync_status_ = RestoreSyncStatus::RESTORE_SYNC_NORMAL;
    CLOG_LOG(TRACE, "error is not exist, restore sync is normal", K(error_exist), K(restore_status_info.sync_status_));
  }
  if (is_leader && OB_SUCC(ret)) {
    restore_status_info.ls_id_ = id_;
    restore_status_info.err_code_ = ret_code;
    restore_status_info.sync_lsn_ = lsn.val_;
    restore_status_info.sync_scn_ = scn;
    if (OB_FAIL(restore_status_info.get_restore_comment())) {
      CLOG_LOG(WARN, "fail to get restore comment", K(sync_status));
    } else {
      CLOG_LOG(TRACE, "success to get error code and message", K(restore_status_info));
    }
  }
  return ret;
}

int ObLogRestoreHandler::get_restore_sync_status(int ret_code,
    const ObLogRestoreErrorContext::ErrorType error_type,
    RestoreSyncStatus &sync_status)
{
  int ret = OB_SUCCESS;

  // RESTORE_SYNC_RESTORE_HANDLER_HAS_NO_LEADER
  if (OB_NOT_MASTER == ret_code) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_RESTORE_HANDLER_HAS_NO_LEADER;
  }
  // RESTORE_SYNC_SOURCE_HAS_A_GAP
  else if ((OB_ERR_OUT_OF_LOWER_BOUND == ret_code
    || OB_ARCHIVE_ROUND_NOT_CONTINUOUS == ret_code
    || OB_ARCHIVE_LOG_RECYCLED == ret_code)
    && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_SOURCE_HAS_A_GAP;
  }
  // RESTORE_SYNC_SUBMIT_LOG_NOT_MATCH
  else if (OB_ERR_UNEXPECTED == ret_code && ObLogRestoreErrorContext::ErrorType::SUBMIT_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_SUBMIT_LOG_NOT_MATCH;
  }
  // RESTORE_SYNC_FETCH_LOG_NOT_MATCH
  else if ((OB_INVALID_DATA == ret_code || OB_CHECKSUM_ERROR == ret_code)
    && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_FETCH_LOG_NOT_MATCH;
  }
  // RESTORE_SYNC_CHECK_NETWORK
  else if (-ER_CONNECT_FAILED == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_CHECK_NETWORK;
  }
  // RESTORE_SYNC_CHECK_USER_OR_PASSWORD
  else if ((-ER_ACCESS_DENIED_ERROR == ret_code || -ER_TABLEACCESS_DENIED_ERROR == ret_code)
    && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_CHECK_USER_OR_PASSWORD;
  }
  // RESTORE_SYNC_FETCH_LOG_TIME_OUT
  else if (OB_TIMEOUT == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_FETCH_LOG_TIME_OUT;
  }
  // RESTORE_SYNC_STANDBY_NEED_UPGRADE
  else if (OB_ERR_RESTORE_STANDBY_VERSION_LAG == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_STANDBY_NEED_UPGRADE;
  }
  // RESTORE_SYNC_PRIMARY_IS_DROPPED
  else if (OB_ERR_RESTORE_PRIMARY_TENANT_DROPPED == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_PRIMARY_IS_DROPPED;
  }
  // RESTORE_SYNC_WAITING_LS_CREATED
  else if (OB_LS_NOT_EXIST == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_WAITING_LS_CREATED;
  }
  // RESTORE_SYNC_ACCESS_PRIMARY_FAILED
  else if (OB_SIZE_OVERFLOW == ret_code && ObLogRestoreErrorContext::ErrorType::FETCH_LOG == error_type) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_QUERY_PRIMARY_FAILED;
  }
  // RESTORE_SYNC_NOT_AVAILABLE
  else if (OB_SUCCESS != ret_code) {
    sync_status = RestoreSyncStatus::RESTORE_SYNC_NOT_AVAILABLE;
  }
  CLOG_LOG(TRACE, "get error code and message succ", K(sync_status));
  return ret;
}

void ObLogRestoreHandler::inc_delay_count()
{
  WLockGuard guard(lock_);
  cur_delay_count_++;
}

void ObLogRestoreHandler::print_stat()
{
  int ret = OB_SUCCESS;
  constexpr int64_t STAT_INTERVAL = 10 * 1000 * 1000;
  WLockGuard guard(lock_);
  if (OB_INVALID_TIMESTAMP == last_stat_ts_) {
  } else {
    ObRemoteFetchStat delta_stat = cur_stat_info_ - last_stat_info_;
    ObRemoteFetchStat mean_stat = delta_stat / delta_stat.fetch_task_count_;
    int64_t delay_count_delta = cur_delay_count_ - last_delay_count_;

    CLOG_LOG(INFO, "[RESTORE_HANDLER] [STAT]", K(id_), K(delay_count_delta),
        K(delta_stat), K(mean_stat), KPC(this), K(transport_task_queue_),
        "restore_delay", context_.max_fetch_scn_.is_valid() ?
        context_.max_fetch_scn_.convert_to_ts(true) - context_.last_fetch_ts_ : 0);
  }
  last_delay_count_ = cur_delay_count_;
  last_stat_info_ = cur_stat_info_;
  last_stat_ts_ = ObTimeUtility::current_time();
}

int ObLogRestoreHandler::get_offline_scn_(share::SCN &scn)
{
  int ret = OB_SUCCESS;
  storage::ObLSHandle handle;
  if (OB_FAIL(MTL(storage::ObLSService*)->get_ls(share::ObLSID(id_), handle, ObLSGetMod::LOG_MOD))) {
    CLOG_LOG(WARN, "get ls failed", K(id_));
  } else if (OB_FAIL(handle.get_ls()->get_offline_scn(scn))) {
    CLOG_LOG(WARN, "get offline_scn failed", K(id_));
  }
  return ret;
}

void ObLogRestoreHandler::deep_copy_source_(ObRemoteSourceGuard &source_guard)
{
  int ret = OB_SUCCESS;
  ObRemoteLogParent *source = NULL;
  if (OB_ISNULL(parent_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "parent_ is NULL", K(ret));
  } else if (OB_ISNULL(source = ObResSrcAlloctor::alloc(parent_->get_source_type(), share::ObLSID(id_)))) {
  } else if (FALSE_IT(parent_->deep_copy_to(*source))) {
  } else if (! source->is_valid()) {
    ObResSrcAlloctor::free(source); // free source when it is invalid
    source = nullptr;
    CLOG_LOG(WARN, "source is invalid");
  } else if (FALSE_IT(source_guard.set_source(source))) {
  }
}

RestoreStatusInfo::RestoreStatusInfo()
  : ls_id_(share::ObLSID::INVALID_LS_ID),
    sync_lsn_(LOG_INVALID_LSN_VAL),
    sync_status_(RestoreSyncStatus::INVALID_RESTORE_SYNC_STATUS),
    err_code_(OB_SUCCESS)
{
  sync_scn_.reset();
  comment_.reset();
}

int RestoreStatusInfo::set(const share::ObLSID &ls_id,
           const palf::LSN &lsn, const share::SCN &scn, int err_code,
           const RestoreSyncStatus sync_status)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid() || !lsn.is_valid() || !scn.is_valid() || OB_SUCCESS == err_code) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(ls_id), K(lsn), K(scn), K(err_code));
  } else {
    ls_id_ = ls_id.id();
    sync_scn_= scn;
    sync_lsn_ = lsn.val_;
    sync_status_ = sync_status;
    err_code_ = err_code;
    if (OB_FAIL(get_restore_comment())) {
      CLOG_LOG(WARN, "failed to assign comment", KR(ret), K(sync_status));
    }
  }
  return ret;
}

void RestoreStatusInfo::reset()
{
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  sync_lsn_ = LOG_INVALID_LSN_VAL;
  sync_scn_.reset();
  sync_status_ = RestoreSyncStatus::INVALID_RESTORE_SYNC_STATUS;
  err_code_ = OB_SUCCESS;
  comment_.reset();
}

int RestoreStatusInfo::restore_sync_status_to_string(char *str_buf, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_RESTORE_STATUS_STR_LEN = 32;
  if (OB_ISNULL(str_buf)
    || str_len < MAX_RESTORE_STATUS_STR_LEN
    || sync_status_ <= RestoreSyncStatus::INVALID_RESTORE_SYNC_STATUS
    || sync_status_ >= RestoreSyncStatus::MAX_RESTORE_SYNC_STATUS) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid restore status", K(sync_status_));
  } else if (OB_FAIL(databuff_printf(str_buf, str_len, "%s", restore_status_str[int(sync_status_)]))) {
    CLOG_LOG(WARN, "databuff printf restore status str failed", K(sync_status_));
  }
  return ret;
}

int RestoreStatusInfo::get_restore_comment()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(comment_.assign_fmt("%s", restore_comment_str[int(sync_status_)]))) {
    CLOG_LOG(WARN, "fail to assign comment", K_(sync_status));
  } else {
    CLOG_LOG(TRACE, "success to get restore status comment", K_(sync_status), K_(comment));
  }
  return ret;
}

int RestoreStatusInfo::assign(const RestoreStatusInfo &other)
{
  int ret = OB_SUCCESS;
  ls_id_ = other.ls_id_;
  sync_lsn_ = other.sync_lsn_;
  sync_scn_ = other.sync_scn_;
  sync_status_ = other.sync_status_;
  err_code_ = other.err_code_;
  if (OB_FAIL(comment_.assign(other.comment_))) {
    CLOG_LOG(WARN, "fail to assign comment");
  }
  return ret;
}

bool RestoreStatusInfo::is_valid() const
{
  return ls_id_ != share::ObLSID::INVALID_LS_ID
      && sync_lsn_ != LOG_INVALID_LSN_VAL
      && sync_scn_.is_valid()
      && sync_status_ != RestoreSyncStatus::INVALID_RESTORE_SYNC_STATUS;
}
} // namespace logservice
} // namespace oceanbase
