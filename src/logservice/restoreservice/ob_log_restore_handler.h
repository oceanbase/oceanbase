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

// The interface to submit log for physical restore and physical standby
class ObLogRestoreHandler : public ObLogHandlerBase
{
  static const int64_t MAX_RAW_WRITE_RETRY_TIMES = 1000;
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
  int add_source(const ObAddr &addr, const share::SCN &end_scn);
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
  void mark_error(share::ObTaskId &trace_id, const int ret_code);
  // @brief get restore error for report
  int get_restore_error(share::ObTaskId &trace_id, int &ret_code, bool &error_exist);
  // @brief Before the standby tenant switchover to primary, check if all primary logs are restored in the standby
  // 1. for standby based on archive, check the max archived log is restored in the standby
  // 2. for standby based on net service, check the max log in primary is restored in the standby
  // @param[out] end_scn, the end_scn of palf
  // @param[out] archive_scn, the max scn in archive logs
  int check_restore_to_newest(share::SCN &end_scn, share::SCN &archive_scn);
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

  TO_STRING_KV(K_(is_inited), K_(is_in_stop_state), K_(id), K_(proposal_id), K_(role), KP_(parent), K_(context), K_(restore_context));

private:
  bool is_valid() const;
  void alloc_source(const share::ObLogRestoreSourceType &type);
  int check_replay_done_(const share::SCN &scn, bool &done);
  int check_replica_replay_done_(const share::SCN &scn, common::ObMemberList &member_list, bool &done);
  int check_member_list_change_(common::ObMemberList &member_list, bool &member_list_change);

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
