/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OB_ROOT_SERVICE_MONITOR_H_
#define OCEANBASE_OBSERVER_OB_ROOT_SERVICE_MONITOR_H_

#include "share/ob_define.h"
#include "share/ob_thread_pool.h"

namespace oceanbase
{
namespace rootserver
{
class ObRootService;
}
namespace share
{
class ObRsMgr;
}

namespace observer
{
class ObRootServiceMonitor : public share::ObThreadPool
{
public:
  ObRootServiceMonitor(rootserver::ObRootService &root_service,
                       share::ObRsMgr &rs_mgr);
  virtual ~ObRootServiceMonitor();
  int init();
  void run1() final;
  int start();
  void stop();
private:
  static const int64_t MONITOR_ROOT_SERVICE_INTERVAL_US = 10 * 1000;  //10ms

  int monitor_root_service();
  int try_start_root_service();
  int wait_rs_finish_start();
private:
  bool inited_;
  rootserver::ObRootService &root_service_;
  int64_t fail_count_;
  share::ObRsMgr &rs_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRootServiceMonitor);
};
}//end namespace observer
}//end namespace oceanbase
#endif //OCEANBASE_OBSERVER_OB_ROOT_SERVICE_MONITOR_H_
