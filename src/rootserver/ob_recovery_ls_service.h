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

#ifndef OCEANBASE_ROOTSERVER_OB_RECCOVERY_LS_SERVICE_H
#define OCEANBASE_ROOTSERVER_OB_RECCOVERY_LS_SERVICE_H
#include "lib/thread/ob_reentrant_thread.h"//ObRsReentrantThread
#include "logservice/ob_log_base_type.h"//ObIRoleChangeSubHandler ObICheckpointSubHandler ObIReplaySubHandler
#include "logservice/palf/lsn.h"//palf::LSN
#include "logservice/palf/palf_iterator.h"          //PalfBufferIterator
#include "ob_primary_ls_service.h" //ObTenantThreadHelper
#include "lib/lock/ob_spin_lock.h" //ObSpinLock

namespace oceanbase
{
namespace obrpc
{
class  ObSrvRpcProxy;
}
namespace common
{
class ObMySQLProxy;
class ObISQLClient;
class ObMySQLTransaction;
}
namespace share
{
class ObLSTableOperator;
struct ObLSAttr;
struct ObLSRecoveryStat;
namespace schema
{
class ObMultiVersionSchemaService;
class ObTenantSchema;
}
}
namespace logservice
{
class ObLogHandler;
class ObGCLSLog;
}
namespace transaction
{
class ObTxLogBlock;
}
namespace rootserver 
{
/*description:
 *Restores the status of each log stream according to the logs to which the
 *system log stream is synchronized, and updates the recovery progress of the
 *system log stream. This thread should only exist in the standby database or
 *the recovery process, and needs to be registered in the RestoreHandler.
 *This thread is only active on the leader of the system log stream under the user tenant*/
class ObRecoveryLSService : public ObTenantThreadHelper
{
public:
  ObRecoveryLSService() : inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID), proxy_(NULL) {}
  virtual ~ObRecoveryLSService() {}
  static int mtl_init(ObRecoveryLSService *&ka);
  int init();
  void destroy();
  virtual void do_work() override;
private:
 //get log iterator by start_scn
 int seek_log_iterator_(const int64_t sync_scn,
                        palf::PalfBufferIterator &iterator);
 int process_ls_log_(const int64_t start_scn ,palf::PalfBufferIterator &iterator);
 int process_gc_log_(logservice::ObGCLSLog &gc_log,
                     const int64_t sync_ts);
 int process_ls_tx_log_(transaction::ObTxLogBlock &tx_log,
                        const int64_t sync_ts);
 int process_ls_operator_(const share::ObLSAttr &ls_attr,
                          const int64_t sync_ts);
 int create_new_ls_(const share::ObLSAttr &ls_attr,
                    const int64_t sync_ts,
                    common::ObMySQLTransaction &trans);
 int process_recovery_ls_manager();
 int construct_ls_recovery_stat(const int64_t sync_ts,
                                share::ObLSRecoveryStat &ls_stat);
 //wait other ls is larger than sycn ts
 int check_valid_to_operator_ls_(const int64_t sync_scn);
 int check_can_do_recovery_();
 //check restore finish and update sync scn to recovery_unitl_scn
 int update_sys_ls_restore_finish_();
 //readable scn need report
 int report_sys_ls_recovery_stat_(const int64_t sync_scn);
private:
  bool inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *proxy_;

};
}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_RECCOVERY_LS_SERVICE_H */
