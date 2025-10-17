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

#ifndef OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_SSLOG_SERVICE_H_
#define OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_SSLOG_SERVICE_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace rootserver
{

class ObDRCreateSSLOGTask : public common::ObTimerTask
{
public:
  ObDRCreateSSLOGTask();
  virtual ~ObDRCreateSSLOGTask();
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  int check_locality_for_add_sslog_(bool &locality_is_matched);
  int get_params_for_add_sslog_(int64_t &orig_paxos_replica_number,
                                common::ObAddr &leader_server);

  int check_tenant_schema_is_ready_(bool &tenant_schema_is_ready);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  static const int64_t SCHEDULE_INTERVAL_US = 10 * 1000 * 1000L; // 10s
  static const int INVALID_TG_ID = -1;
};

class ObDRSSLOGService
{
public:
  ObDRSSLOGService();
  virtual ~ObDRSSLOGService() {}
  static int mtl_init(ObDRSSLOGService *&sslog_service);
  int init();
  int start();
  void stop();
  int wait();
  void destroy();

private:
  int tg_id_;
  bool is_inited_;
  volatile bool is_stopped_;
  static const int INVALID_TG_ID = -1;
  ObDRCreateSSLOGTask create_sslog_task_;
};


} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_SSLOG_SERVICE_H_
