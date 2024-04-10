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

#ifndef OCEABASE_STORAGE_HA_DIAG_SERVICE_
#define OCEABASE_STORAGE_HA_DIAG_SERVICE_

#include "share/ob_storage_ha_diagnose_struct.h"
#include "share/ob_storage_ha_diagnose_operator.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

class ObStorageHADiagService : public lib::ThreadPool
{
public:
  ObStorageHADiagService();
  virtual ~ObStorageHADiagService() {}

  int init(common::ObMySQLProxy *sql_proxy);
  void run1() final;
  void destroy();
  void wakeup();
  void stop();
  void wait();
  int start();

public:
  int add_task(const ObStorageHADiagTaskKey &key);
  static ObStorageHADiagService &instance();

private:
  typedef ObArray<ObStorageHADiagTaskKey> TaskKeyArray;
  typedef hash::ObHashMap<ObStorageHADiagTaskKey, int64_t, hash::NoPthreadDefendMode> TaskKeyMap;
  static const int64_t ONCE_REPORT_KEY_MAX_NUM = 100;
  static const int64_t REPORT_KEY_MAX_NUM = 1000;

private:
  int do_clean_history_(const ObStorageHADiagModule module, int64_t &end_timestamp);
  int do_report_();
  int get_info_from_type_(const ObStorageHADiagTaskKey &key, ObStorageHADiagInfo *&info,
                          ObTransferErrorDiagInfo &transfer_err_diag,
                          ObTransferPerfDiagInfo &transfer_perf_diag,
                          ObIAllocator &alloc);
  int insert_inner_table_(
      ObMySQLTransaction &trans,
      ObStorageHADiagMgr *mgr,
      const ObStorageHADiagTaskKey &task_key,
      ObStorageHADiagInfo &info);
  int deep_copy_keys_(TaskKeyArray &do_report_keys) const;
  int report_process_(const TaskKeyArray &task_keys);
  int add_keys_(
      const int64_t index,
      const TaskKeyArray &task_keys,
      TaskKeyArray &new_task_keys) const;
  int clean_task_key_without_lock_(const ObStorageHADiagTaskKey &task_key);
  int report_to_inner_table_(
      ObMySQLTransaction &trans,
      ObStorageHADiagMgr *mgr,
      const ObStorageHADiagTaskKey &task_key);
  int remove_oldest_without_lock_();
  int get_oldest_key_without_lock_(ObStorageHADiagTaskKey &key) const;
  int start_trans_(
      ObTimeoutCtx &timeout_ctx,
      ObMySQLTransaction &trans);
  int commit_trans_(
      const int32_t result,
      ObMySQLTransaction &trans);
  int report_batch_task_key_(ObMySQLTransaction &trans, const TaskKeyArray &task_keys);
  int batch_del_task_in_mgr_(const TaskKeyArray &task_keys);
  int get_ls_id_array_(ObLSService *ls_service, ObIArray<share::ObLSID> &ls_id_array);
  int do_clean_related_info_(ObLSService *ls_service, const share::ObLSID &ls_id, const uint64_t tenant_id);
  int scheduler_clean_related_info_(ObLSService *ls_service, const ObIArray<share::ObLSID> &ls_id_array, const uint64_t tenant_id);
  int do_clean_related_info_in_ls_(ObLSService *ls_service, const uint64_t tenant_id);
  int do_clean_transfer_related_info_();
  int del_task_in_mgr_(const ObStorageHADiagTaskKey &task_key) const;

private:
  bool is_inited_;
  common::ObThreadCond thread_cond_;
  int64_t wakeup_cnt_;
  ObStorageHADiagOperator op_;
  common::ObMySQLProxy *sql_proxy_;
  TaskKeyMap task_keys_;
  SpinRWLock lock_;
  int64_t err_diag_end_timestamp_;
  int64_t perf_diag_end_timestamp_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageHADiagService);
};

}
}
#endif
