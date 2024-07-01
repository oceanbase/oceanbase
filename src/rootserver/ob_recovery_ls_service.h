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
#include "logservice/restoreservice/ob_log_restore_handler.h"//RestoreStatusInfo
#include "ob_primary_ls_service.h" //ObTenantThreadHelper
#include "lib/lock/ob_spin_lock.h" //ObSpinLock
#include "storage/tx/ob_multi_data_source.h" //ObTxBufferNode

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
namespace transaction
{
class ObTxBufferNode;
}
namespace share
{
class ObLSTableOperator;
struct ObLSAttr;
struct ObLSRecoveryStat;
class SCN;
class ObBalanceTaskHelper;
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
namespace palf
{
class PalfHandleGuard;
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
  ObRecoveryLSService() : inited_(false), tenant_id_(OB_INVALID_TENANT_ID), proxy_(NULL),
  last_report_ts_(OB_INVALID_TIMESTAMP), restore_status_() {}
  virtual ~ObRecoveryLSService() {}
  int init();
  void destroy();
  int get_sys_restore_status(logservice::RestoreStatusInfo &restore_status);
  virtual void do_work() override;
  DEFINE_MTL_FUNC(ObRecoveryLSService)
private:
 int process_thread0_(const ObAllTenantInfo &tenant_info);
 int process_thread1_(const ObAllTenantInfo &tenant_info,
     share::SCN &start_scn,
     palf::PalfBufferIterator &iterator);
 //get log iterator by start_scn
 //interface for thread0
 int init_palf_handle_guard_(palf::PalfHandleGuard &palf_handle_guard);
 int seek_log_iterator_(const share::SCN &syn_scn,
                       palf::PalfHandleGuard &palf_handle_guard,
                        palf::PalfBufferIterator &iterator);
 int process_ls_log_(const ObAllTenantInfo &tenant_info,
                     share::SCN &start_scn,
                     palf::PalfBufferIterator &iterator);
 int process_upgrade_log_(const share::SCN &sync_scn,
     const transaction::ObTxBufferNode &node);
 int process_upgrade_data_version_log_(const share::SCN &sync_scn,
                                       const transaction::ObTxBufferNode &node,
                                       common::ObMySQLTransaction &trans);
 int process_gc_log_(logservice::ObGCLSLog &gc_log,
                     const share::SCN &syn_scn);
 int process_ls_tx_log_(transaction::ObTxLogBlock &tx_log,
                        const share::SCN &syn_scn);
 int process_ls_table_in_trans_(const transaction::ObTxBufferNode &node,
                          const share::SCN &syn_scn,
                          common::ObMySQLTransaction &trans);
 int create_new_ls_(const share::ObLSAttr &ls_attr,
                    const share::SCN &syn_scn,
                    const ObTenantSwitchoverStatus &switchover_status,
                    common::ObMySQLTransaction &trans);
 int construct_sys_ls_recovery_stat_based_on_sync_scn_(
     const share::SCN &syn_scn,
     ObLSRecoveryStat &ls_stat,
     const ObAllTenantInfo &tenant_info);
 //wait other ls is larger than sycn ts
 int check_valid_to_operator_ls_(const share::SCN &syn_scn);
 //readable scn need report
 void try_tenant_upgrade_end_();
 int get_min_data_version_(uint64_t &compatible);
 int process_ls_operator_in_trans_(const share::ObLSAttr &ls_attr,
     const share::SCN &sync_scn, common::ObMySQLTransaction &trans);
 int porcess_alter_ls_group_(const share::ObLSAttr &ls_attr,
                            const share::SCN &sync_scn,
                            common::ObMySQLTransaction &trans);
 int report_sys_ls_recovery_stat_(const share::SCN &sync_scn, const bool only_update_readable_scn,
                                  const char* comment);
 int report_sys_ls_recovery_stat_in_trans_(const share::SCN &sync_scn,
                                           const bool only_update_readable_scn,
                                           common::ObMySQLTransaction &trans,
                                           const char* comment);

 int process_ls_transfer_task_in_trans_(const transaction::ObTxBufferNode &node,
     const share::SCN &sync_scn, common::ObMySQLTransaction &trans);
 int init_restore_status(const share::SCN &sync_scn, int err_code);
 //thread1
 int do_standby_balance_();
 int do_ls_balance_task_();
 int try_do_ls_balance_task_(const share::ObBalanceTaskHelper &ls_balance_task,
     const share::ObAllTenantInfo &tenant_info);
 int check_transfer_begin_can_remove_(const share::ObBalanceTaskHelper &ls_balance_task,
     const share::ObAllTenantInfo &tenant_info,
     bool &can_remove);
 int do_ls_balance_alter_task_(const share::ObBalanceTaskHelper &ls_balance_task,
                               common::ObMySQLTransaction &trans);
 int reset_restore_proxy_(ObRestoreSourceServiceAttr &service_attr);
 int get_ls_all_replica_readable_scn_(const share::ObLSID &ls_id, share::SCN &reabable_scn);
#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
 int decompress_log_payload_(const char *in_buf, const int64_t in_buf_len, char *&decompress_buf, int64_t &decompressed_len);
#endif
 // cancel clone job if clone job exists
 int try_cancel_clone_job_for_standby_tenant_(const transaction::ObTxBufferNode &node);
 int check_standby_tenant_not_in_cloning_(common::ObMySQLTransaction &trans);
 int log_type_conflict_with_clone_procedure_(const transaction::ObTxBufferNode &node, bool &is_conflict);
private:
  bool inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *proxy_;
  int64_t last_report_ts_;
  logservice::RestoreStatusInfo restore_status_;
};
}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_RECCOVERY_LS_SERVICE_H */
