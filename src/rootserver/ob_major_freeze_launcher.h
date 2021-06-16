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

#ifndef OCEANBASE_ROOTSERVER_OB_MAJOR_FREEZE_LAUNCHER_H_
#define OCEANBASE_ROOTSERVER_OB_MAJOR_FREEZE_LAUNCHER_H_

#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "ob_snapshot_info_manager.h"

namespace oceanbase {
namespace common {
class ObServerConfig;
}
namespace obrpc {
class ObCommonRpcProxy;
}
namespace rootserver {
class ObRootService;
class ObFreezeInfoManager;
class ObMajorFreezeLauncher : public ObRsReentrantThread {
public:
  ObMajorFreezeLauncher();
  virtual ~ObMajorFreezeLauncher();
  int init(ObRootService& root_service, obrpc::ObCommonRpcProxy& rpc_proxy, common::ObServerConfig& config,
      const common::ObAddr& self_addr, ObFreezeInfoManager& freeze_info_manager);
  virtual void run3() override;
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  int64_t get_schedule_interval() const;

private:
  static const int64_t TRY_LAUNCH_MAJOR_FREEZE_INTERVAL_US = 1000 * 1000;  // 1s
  // daily major freeze will retry when error is OB_MAJOR_FREEZE_NOT_ALLOW.
  static const int64_t MAJOR_FREEZE_RETRY_LIMIT = 300;
  static const int64_t MAJOR_FREEZE_RETRY_INTERVAL_US = 1000 * 1000;                  // 1s
  static const int64_t MODIFY_GC_FREEZE_INFO_INTERVAL = 24 * 60 * 60 * 1000 * 1000L;  // 1 day

  int try_launch_major_freeze();
  int try_gc_freeze_info();

private:
  bool inited_;
  ObRootService* root_service_;
  obrpc::ObCommonRpcProxy* rpc_proxy_;
  common::ObServerConfig* config_;
  common::ObAddr self_addr_;
  bool same_minute_flag_;
  int64_t gc_freeze_info_last_timestamp_;
  ObFreezeInfoManager* freeze_info_manager_;
};
}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_MAJOR_FREEZE_LAUNCHER_H_
