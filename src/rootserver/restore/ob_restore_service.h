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

#ifndef OCEANBASE_ROOTSERVER_RECOVER_TABLE_SERVICE_H
#define OCEANBASE_ROOTSERVER_RECOVER_TABLE_SERVICE_H
#include "ob_restore_scheduler.h"
#include "ob_recover_table_job_scheduler.h"
#include "ob_import_table_job_scheduler.h"
#include "rootserver/ob_tenant_thread_helper.h"//ObTenantThreadHelper
#include "share/ob_check_stop_provider.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/scn.h"

namespace oceanbase
{

namespace share
{
class ObLocationService;
namespace schema
{
class ObMultiVersionSchemaService;
}

struct ObRecoverTableJob;
}

namespace common
{
class ObMySQLProxy;
}

namespace rootserver
{

// Running in a single thread.
// schedule restore job, register to sys ls of meta tenant
class ObRestoreService : public ObTenantThreadHelper,
  public logservice::ObICheckpointSubHandler, public logservice::ObIReplaySubHandler,
  public share::ObCheckStopProvider
{
public:
  static const int64_t MAX_RESTORE_TASK_CNT = 10000;
public:
  ObRestoreService();
  virtual ~ObRestoreService();
  int init();
  virtual void do_work() override;
  void destroy();
  DEFINE_MTL_FUNC(ObRestoreService)
public:
  virtual share::SCN get_rec_scn() override { return share::SCN::max_scn();}
  virtual int flush(share::SCN &rec_scn) override { return OB_SUCCESS; }
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn) override
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }

public:
  int idle();
  int check_stop() const override;
  void wakeup() { ATOMIC_INC(&wakeup_cnt_);}

private:
  bool inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObCommonRpcProxy *rpc_proxy_;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy_;
  common::ObAddr self_addr_;
  uint64_t tenant_id_;
  int64_t idle_time_us_;
  int64_t wakeup_cnt_;
  ObRestoreScheduler restore_scheduler_;
  ObRecoverTableJobScheduler recover_table_scheduler_;
  ObImportTableJobScheduler import_table_scheduler_;
  DISALLOW_COPY_AND_ASSIGN(ObRestoreService);
};

}
}

#endif