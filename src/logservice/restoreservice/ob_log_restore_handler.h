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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_HANDLER_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_HANDLER_H_

#include <cstdint>
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_tc_rwlock.h"         // RWLock
#include "lib/ob_define.h"
#include "common/ob_role.h"                // Role
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "logservice/palf/log_group_entry.h"
#include "logservice/palf/lsn.h"           // LSN
#include "ob_log_restore_define.h"         // Parent ObRemoteFetchContext
#include "logservice/palf/palf_handle.h"   // PalfHandle
#include "share/ob_define.h"
#include "share/restore/ob_log_restore_source_mgr.h"
#include "logservice/ob_log_handler_base.h"
#include "ob_remote_log_source.h"          // ObRemoteSource
#include "ob_remote_fetch_context.h"       // ObRemoteFetchContext
namespace oceanbase
{
namespace common
{
class ObAddr;
class ObString;
}

namespace share
{
class ObBackupDest;
}

namespace palf
{
class PalfEnv;
}

namespace logservice
{
class ObFetchLogTask;
using oceanbase::common::ObRole;
using oceanbase::palf::LSN;
using oceanbase::palf::PalfEnv;
using oceanbase::common::ObString;
using oceanbase::common::ObAddr;

struct RestoreDiagnoseInfo
{
  RestoreDiagnoseInfo() { reset(); }
  ~RestoreDiagnoseInfo() { reset(); }
  common::ObRole restore_role_;
  int64_t restore_proposal_id_;
  ObSqlString restore_context_info_;
  ObSqlString restore_err_context_info_;
  TO_STRING_KV(K(restore_role_),
               K(restore_proposal_id_));
  void reset() {
    restore_role_ = FOLLOWER;
    restore_proposal_id_ = palf::INVALID_PROPOSAL_ID;
    restore_context_info_.reset();
    restore_err_context_info_.reset();
  }
};

enum class RestoreSyncStatus {
  INVALID_RESTORE_SYNC_STATUS = 0,
  RESTORE_SYNC_NORMAL = 1,
  RESTORE_SYNC_SOURCE_HAS_A_GAP = 2,
  RESTORE_SYNC_SUBMIT_LOG_NOT_MATCH = 3,
  RESTORE_SYNC_FETCH_LOG_NOT_MATCH = 4,
  RESTORE_SYNC_CHECK_USER_OR_PASSWORD = 5,
  RESTORE_SYNC_CHECK_NETWORK = 6,
  RESTORE_SYNC_FETCH_LOG_TIME_OUT = 7,
  RESTORE_SYNC_SUSPEND = 8,
  RESTORE_SYNC_STANDBY_NEED_UPGRADE = 9,
  RESTORE_SYNC_PRIMARY_IS_DROPPED = 10,
  RESTORE_SYNC_WAITING_LS_CREATED = 11,
  RESTORE_SYNC_QUERY_PRIMARY_FAILED = 12,
  RESTORE_SYNC_RESTORE_HANDLER_HAS_NO_LEADER = 13,
  RESTORE_SYNC_NOT_AVAILABLE = 14,
  MAX_RESTORE_SYNC_STATUS
};

struct RestoreStatusInfo
{
public:
  RestoreStatusInfo();
  ~RestoreStatusInfo() { reset(); }
  bool is_valid() const;
  void reset();
  int restore_sync_status_to_string(char *str_buf, const int64_t str_len);
  int set(const share::ObLSID &ls_id,
           const palf::LSN &lsn, const share::SCN &scn, int err_code,
           const RestoreSyncStatus sync_status);
  int get_restore_comment();
  int assign(const RestoreStatusInfo &other);
  TO_STRING_KV(K_(ls_id), K_(sync_lsn), K_(sync_scn), K_(sync_status), K_(err_code), K_(comment));

public:
  int64_t ls_id_;
  int64_t sync_lsn_;
  share::SCN sync_scn_;
  RestoreSyncStatus sync_status_;
  int err_code_;
  ObSqlString comment_;
};

// The interface to submit log for physical restore and physical standby
class ObLogRestoreHandler : public ObLogHandlerBase
{
  static const int64_t MAX_RAW_WRITE_RETRY_TIMES = 10000;
  static const int64_t MAX_RETRY_SLEEP_US = 100;
public:
  ObLogRestoreHandler();
  ~ObLogRestoreHandler();

public:
  int init(const int64_t id, PalfEnv *palf_env);
  int stop();
  void destroy();
  // @brief switch log_restore_handle role, to LEADER or FOLLOWER
  // @param[in], role, LEADER or FOLLOWER
  // @param[in], proposal_id, global monotonically increasing id
  virtual void switch_role(const common::ObRole &role, const int64_t proposal_id) override;
  // @brief query role and proposal_id from ObLogRestoreHandler
  // @param[out], role:
  //    LEADER, if 'role_' of ObLogRestoreHandler is LEADER and 'proposal_id' is same with PalfHandle.
  //    FOLLOWER, otherwise.
  // @param[out], proposal_id, global monotonically increasing.
  // @retval
  //   OB_SUCCESS
  // NB: for primary, ObLogRestoreHandler is always FOLLOWER
  int get_role(common::ObRole &role, int64_t &proposal_id) const;
  // interface only for restore_and_standby branch, TODO should delete after no cut log by shuning.tsn
  int get_upper_limit_scn(share::SCN &scn) const;
  // get the log ts of the max restored log
  int get_max_restore_scn(share::SCN &scn) const;
  // @brief add log fetch source of phyiscal backups
  int add_source(logservice::DirArray &array, const share::SCN &end_scn);
  int add_source(share::ObBackupDest &dest, const share::SCN &end_scn);
  int add_source(const share::ObRestoreSourceServiceAttr &service_attr, const share::SCN &end_scn);
  // clean source if log_restore_source is empty
  int clean_source();
  // @brief As restore handler maybe destroyed, log source should be copied out
  void deep_copy_source(ObRemoteSourceGuard &guard);
  // @brief raw write logs to the pointed offset of palf
  // @param[in] const int64_t, proposal_id used to distinguish stale logs after flashback
  // @param[in] const palf::LSN, the pointed offset to be writen of the data buffer
  // @param[in] const int64_t, the scn
  // @param[in] const char *, the data buffer
  // @param[in], const int64_t, the size of the data buffer
  // @retval  OB_SUCCESS  raw write successfully
  //          OB_ERR_OUT_OF_LOWER_BOUND  log already exists in palf
  //          OB_LOG_OUTOF_DISK_SPACE  clog disk is full
  //          OB_RESTORE_LOG_TO_END  restore log already enough for recovery_end_ts
  int raw_write(const int64_t proposal_id,
      const palf::LSN &lsn,
      const share::SCN &scn,
      const char *buf,
      const int64_t buf_size);
  // @brief update max fetch info
  // @param[in] const int64_t, proposal_id used to distinguish stale logs after flashback
  // @param[in] const palf::LSN, the max_lsn submitted
  // @param[in] const int64_t, the max_scn submitted
  int update_max_fetch_info(const int64_t proposal_id,
      const palf::LSN &lsn,
      const share::SCN &scn);
  // @brief check if need update fetch log source,
  // ONLY return true if role of RestoreHandler is LEADER
  bool need_update_source() const;
  // @brief the big range of log is separated into small tasks, so as to do parallel,
  //        concise dispatch is needed, here is to check if new task is in turn
  int need_schedule(bool &need_schedule, int64_t &proposal_id, ObRemoteFetchContext &context) const;
  int schedule(const int64_t id,
      const int64_t proposal_id,
      const int64_t version,
      const LSN &lsn,
      bool &scheduled);
  // @brief try retire fetch log task
  // @param[in] ObFetchLogTask &, the remote fetch log task
  // @param[out] bool &, the remote fetch log task is to end or not, retire it if true
  int try_retire_task(ObFetchLogTask &task, bool &done);
  // @brief To avoid redundancy locating archive round and piece and reading active files,
  // save archive log consumption context in restore handler
  // @param[in] source, temory log source used to read archive files
  int update_location_info(ObRemoteLogParent *source);
  // @brief check if restore finish
  // return true only if in restore state and all replicas have restore and replay finish
  int check_restore_done(const share::SCN &recovery_end_scn, bool &done);
  // @brief set error if error occurs in log restore
  void mark_error(share::ObTaskId &trace_id,
                  const int ret_code,
                  const palf::LSN &lsn,
                  const ObLogRestoreErrorContext::ErrorType &error_type);
  // @brief get restore error for report
  int get_restore_error(share::ObTaskId &trace_id, int &ret_code, bool &error_exist);
  // @brief Before the standby tenant switchover to primary, check if all primary logs are restored in the standby
  // 1. for standby based on archive, check the max archived log is restored in the standby
  // 2. for standby based on net service, check the max log in primary is restored in the standby
  // When need to add other error codes, you need to add error code to need_fail_when_switch_to_primary()
  // that requires switch tenant to primary fail immediately
  // @param[out] end_scn, the end_scn of palf
  // @param[out] archive_scn, the max scn in archive logs
  // @ret_code OB_NOT_MASTER   the restore_handler is not master
  //           OB_EAGAIN       the restore source not valid
  //           OB_SOURCE_TENANT_STATE_NOT_MATCH     original tenant state not match to switchover
  //           OB_SOURCE_LS_STATE_NOT_MATCH         original ls state not match to switchover
  //           OB_PASSWORD_WRONG                    user password in log restore source is invalid
  //           other code      unexpected ret_code
  int check_restore_to_newest(share::SCN &end_scn, share::SCN &archive_scn);
  static bool need_fail_when_switch_to_primary(const int ret)
  {
    return OB_SOURCE_TENANT_STATE_NOT_MATCH == ret
          || OB_SOURCE_LS_STATE_NOT_MATCH == ret
          || OB_PASSWORD_WRONG == ret;
  }

  // @brief Remote Fetch Log Workers fetch log from remote source in parallel, but raw write to palf in series
  // This interface to to sort and cache logs
  // @ret_code  OB_NOT_MASTER   the restore_handler is not master
  int submit_sorted_task(ObFetchLogTask &task);
  // @brief Correspond to the submit_sorted_task interface,
  // this is to get the next serial task and to submit its logs to palf
  // If the parent is to_end, clear all cached tasks
  // @param[out] ObFetchLogTask *&, the task in turn to submit
  // @ret_code  OB_NOT_MASTER   the restore_handler is not master, and no task gotten
  //            OB_SUCCESS      get task successfully, but maybe no task in turn exists
  //            other code      unexpected ret_code
  int get_next_sorted_task(ObFetchLogTask *&task);
  bool restore_to_end() const;
  int get_restore_error_unlock_(share::ObTaskId &trace_id, int &ret_code, bool &error_exist);
  int diagnose(RestoreDiagnoseInfo &diagnose_info) const;
  int refresh_error_context();
  int get_ls_restore_status_info(RestoreStatusInfo &restore_status_info);
  int get_restore_sync_status(int ret_code, const ObLogRestoreErrorContext::ErrorType error_type, RestoreSyncStatus &sync_status);
  TO_STRING_KV(K_(is_inited), K_(is_in_stop_state), K_(id), K_(proposal_id), K_(role), KP_(parent), K_(context), K_(restore_context));

private:
  bool is_valid() const;
  void alloc_source(const share::ObLogRestoreSourceType &type);
  int check_replay_done_(const share::SCN &scn, bool &done);
  int check_replica_replay_done_(const share::SCN &scn, common::ObMemberList &member_list, bool &done);
  int check_member_list_change_(common::ObMemberList &member_list, bool &member_list_change);
  int check_restore_to_newest_from_service_(const share::ObRestoreSourceServiceAttr &attr,
      const share::SCN &end_scn, share::SCN &archive_scn);
  int check_restore_to_newest_from_archive_(ObLogArchivePieceContext &piece_context,
      const palf::LSN &end_lsn, const share::SCN &end_scn, share::SCN &archive_scn);
  int check_restore_to_newest_from_rawpath_(ObLogRawPathPieceContext &rawpath_ctx,
      const palf::LSN &end_lsn, const share::SCN &end_scn, share::SCN &archive_scn);
  bool restore_to_end_unlock_() const;
  int get_offline_scn_(share::SCN &scn);
  void deep_copy_source_(ObRemoteSourceGuard &guard);
  int check_if_ls_gc_(bool &done);
  int check_offline_log_(bool &done);

private:
  ObRemoteLogParent *parent_;
  ObRemoteFetchContext context_;
  ObRestoreLogContext restore_context_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreHandler);
};

}
}
#endif
