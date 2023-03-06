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
#include "share/restore/ob_log_archive_source_mgr.h"
#include "logservice/ob_log_handler_base.h"
#include "ob_remote_log_source.h"          // ObRemoteSource
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
using oceanbase::common::ObRole;
using oceanbase::palf::LSN;
using oceanbase::palf::PalfEnv;
using oceanbase::common::ObString;
using oceanbase::common::ObAddr;

// The interface to submit log for physical restore and physical standby
class ObLogRestoreHandler : public ObLogHandlerBase
{
  static const int64_t MAX_RAW_WRITE_RETRY_TIMES = 1000;
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
  // interface only for restore_and_standby branch, TODO should delete after no cut log by shuning.tsn
  // get the restore end ts of the restore tenant
  int get_upper_limit_ts(int64_t &ts) const;
  // get the log ts of the max restored log
  int get_max_restore_log_ts(int64_t &ts) const;
  // @brief add log fetch source of phyiscal backups
  int add_source(share::DirArray &array, const int64_t end_log_ts);
  // for stale TODO delete after log archive source ready
  int add_source(logservice::DirArray &array, const int64_t end_log_ts);
  int add_source(share::ObBackupDest &dest, const int64_t end_log_ts);
  int add_source(const ObAddr &addr, const int64_t end_log_ts);
  // @brief As restore handler maybe destroyed, log source should be copied out
  void deep_copy_source(ObRemoteSourceGuard &guard);
  // @brief raw write logs to the pointed offset of palf
  // @param[in] const palf::LSN, the pointed offset to be writen of the data buffer
  // @param[in] const char *, the data buffer
  // @param[in], const int64_t, the size of the data buffer
  int raw_write(const palf::LSN &lsn, const char *buf, const int64_t buf_size);
  // @brief check if need update fetch log source,
  // ONLY return true if role of RestoreHandler is LEADER
  bool need_update_source() const;
  // @brief the big range of log is separated into small tasks, so as to do parallel,
  //        concise dispatch is needed, here is to check if new task is in turn
  int need_schedule(bool &need_schedule, int64_t &proposal_id, ObRemoteFetchContext &context) const;
  int schedule(const int64_t id, const int64_t proposal_id, const LSN &lsn, bool &scheduled);
  // @brief update log progress after fetch_log_task is handled
  // @param[in] is_finish, task is finish
  // @param[in] is_to_end, log fetch is to end, only for physical restore
  // @param[out] is_stale, fetch log task is stale due to cur_proposal_id is changed
  int update_fetch_log_progress(const int64_t id, const int64_t proposal_id, const LSN &max_fetch_lsn,
      const int64_t max_submit_log_ts, const bool is_finish, const bool is_to_end, bool &is_stale);
  int update_location_info(ObRemoteLogParent *source);
  // @brief check if restore finish
  // return true only if in restore state and all replicas have restore and replay finish
  int check_restore_done(bool &done);
  // only for physical restore to get the restore upper limit ts as sync_point if restore to end
  // param[in], const share::ObLSID, the identification of LS
  // param[in], int64_t, the commit ts of LS
  // @param[out], int64_t, the restore upper ts if restore to the end
  int get_restore_sync_ts(const share::ObLSID &id, int64_t &log_ts);
  void mark_error(share::ObTaskId &trace_id, const int ret_code);
  int get_restore_error(share::ObTaskId &trace_id, int &ret_code, bool &error_exist);
  TO_STRING_KV(K_(is_inited), K_(is_in_stop_state), K_(id), K_(proposal_id), K_(role), KP_(parent), K_(context));

private:
  bool is_valid() const;
  void alloc_source(const share::ObLogArchiveSourceType &type);
  int check_replay_done_(const int64_t timestamp, bool &done);
  int check_replica_replay_done_(const int64_t timestamp, common::ObMemberList &member_list, bool &done);
  int check_member_list_change_(common::ObMemberList &member_list, bool &member_list_change);

private:
  ObRemoteLogParent *parent_;
  ObRemoteFetchContext context_;
  bool is_in_stop_state_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreHandler);
};

}
}
#endif
