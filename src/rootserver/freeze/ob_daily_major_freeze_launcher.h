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

#ifndef OCEANBASE_ROOTSERVER_FREEZE_OB_DAILY_MAJOR_FREEZE_LAUNCHER_
#define OCEANBASE_ROOTSERVER_FREEZE_OB_DAILY_MAJOR_FREEZE_LAUNCHER_

#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "rootserver/freeze/ob_freeze_reentrant_thread.h"
#include "share/scn.h"

namespace oceanbase
{
namespace common
{
class ObServerConfig;
class ObMySQLProxy;
}
namespace obrpc
{
class ObCommonRpcProxy;
}
namespace rootserver
{
class ObMajorMergeInfoManager;
// primary cluster: sys tenant, meta tenant, user tenant all have this launcher
// standby cluster: only sys tenant, meta tenant have this launcher
class ObDailyMajorFreezeLauncher : public ObFreezeReentrantThread
{
public:
  ObDailyMajorFreezeLauncher(const uint64_t tenant_id);
  virtual ~ObDailyMajorFreezeLauncher() {}
  int init(common::ObServerConfig &config,
           common::ObMySQLProxy &proxy,
           ObMajorMergeInfoManager &merge_info_manager);

  virtual void run3() override;
  virtual int blocking_run() { BLOCKING_RUN_IMPLEMENT(); }
  virtual int start() override;
  virtual int64_t get_schedule_interval() const override;

private:
  int try_launch_major_freeze();
  int try_gc_freeze_info();
  int try_gc_tablet_checksum();

private:
  static const int64_t MAJOR_FREEZE_RETRY_LIMIT = 120;
  static const int64_t MAJOR_FREEZE_LAUNCHER_THREAD_CNT = 1;
  static const int64_t LAUNCHER_INTERVAL_US = 5 * 1000 * 1000; // 5s
  static const int64_t MAJOR_FREEZE_RETRY_INTERVAL_US = 1000 * 1000; // 1s
  static const int64_t MODIFY_GC_INTERVAL = 24 * 60 * 60 * 1000 * 1000L; // 1 day
  static const int64_t TABLET_CKM_CHECK_INTERVAL_US = 30 * 60 * 1000 * 1000L; // 30 min

  bool is_inited_;
  bool already_launch_;
  common::ObServerConfig *config_;
  int64_t gc_freeze_info_last_timestamp_;
  ObMajorMergeInfoManager *merge_info_mgr_;
  int64_t last_check_tablet_ckm_us_;
  share::SCN tablet_ckm_gc_compaction_scn_;

  DISALLOW_COPY_AND_ASSIGN(ObDailyMajorFreezeLauncher);
};

}//end namespace rootserver
}//end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_DAILY_MAJOR_FREEZE_LAUNCHER_
