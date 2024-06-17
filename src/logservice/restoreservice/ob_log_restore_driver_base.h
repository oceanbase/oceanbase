/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_DRIVER_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_DRIVER_H_

#include "share/scn.h"
#include "storage/ls/ob_ls.h"
#include <cstdint>

namespace oceanbase
{
namespace storage
{
class ObLS;
class ObLSService;
}
namespace logservice
{
class ObLogService;
class ObLogRestoreDriverBase
{
  const int64_t FETCH_LOG_AHEAD_THRESHOLD_NS = 6 * 1000 * 1000 *1000L;  // 6s
public:
  ObLogRestoreDriverBase();
  virtual ~ObLogRestoreDriverBase();

  int init(const uint64_t tenant_id, ObLSService *ls_svr, ObLogService *log_service);
  void destroy();
  int do_schedule();
  int set_global_recovery_scn(const share::SCN &recovery_scn);
protected:
  virtual int do_fetch_log_(ObLS &ls) = 0;
  int check_replica_status_(storage::ObLS &ls, bool &can_fetch_log);
  int get_upper_resotore_scn(share::SCN &scn);
protected:
  bool inited_;
  uint64_t tenant_id_;
  storage::ObLSService *ls_svr_;
  ObLogService *log_service_;
  share::SCN global_recovery_scn_;
};
} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_DRIVER_H_ */
