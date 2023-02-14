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

#ifndef OCEANBASE_LOGSERVICE_OB_REMOTE_FETCH_LOG_H_
#define OCEANBASE_LOGSERVICE_OB_REMOTE_FETCH_LOG_H_

#include <cstdint>
#include "common/ob_role.h"     // ObRole
#include "lib/utility/ob_macro_utils.h"
#include "ob_remote_fetch_log_worker.h"    // ObRemoteFetchWorker

namespace oceanbase
{
namespace share
{
class ObLSID;
class SCN;
}

namespace storage
{
class ObLS;
class ObLSService;
}

namespace palf
{
struct LSN;
}

namespace logservice
{
class ObLogService;
using oceanbase::share::ObLSID;
using oceanbase::storage::ObLS;
using oceanbase::storage::ObLSService;
using oceanbase::common::ObRole;
using oceanbase::palf::LSN;

// Generate Fetch Log Task for LS
class ObRemoteFetchLogImpl
{
  static const int64_t FETCH_LOG_AHEAD_THRESHOLD_US = 3 * 1000 * 1000L;  // 3s
public:
  ObRemoteFetchLogImpl();
  ~ObRemoteFetchLogImpl();

  int init(const uint64_t tenant_id, ObLSService *ls_svr, ObLogService *log_service, ObRemoteFetchWorker *worker);
  void destroy();
  int do_schedule();
private:
  int do_fetch_log_(ObLS &ls);
  int check_replica_status_(ObLS &ls, bool &can_fetch_log);
  int check_need_schedule_(ObLS &ls, bool &need_schedule, int64_t &proposal_id,
      int64_t &version, LSN &lsn, int64_t &last_fetch_ts, int64_t &task_count);
  int check_need_delay_(const ObLSID &id, bool &need_delay);
  int get_fetch_log_base_lsn_(ObLS &ls, const LSN &max_fetch_lsn, const int64_t last_fetch_ts, share::SCN &scn, LSN &lsn);
  int get_palf_base_lsn_scn_(ObLS &ls, LSN &lsn, share::SCN &scn);
  int submit_fetch_log_task_(ObLS &ls, const share::SCN &scn, const LSN &lsn,
      const int64_t task_count, const int64_t proposal_id, const int64_t version);
  int do_submit_fetch_log_task_(ObLS &ls, const share::SCN &scn, const LSN &lsn, const int64_t size,
      const int64_t proposal_id, const int64_t version, bool &scheduled);

private:
  bool inited_;
  uint64_t tenant_id_;
  ObLSService *ls_svr_;
  ObLogService *log_service_;
  ObRemoteFetchWorker *worker_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteFetchLogImpl);
};
} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_REMOTE_FETCH_LOG_H_ */
