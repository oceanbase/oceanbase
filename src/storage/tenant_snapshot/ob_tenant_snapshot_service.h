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

#ifndef OCEANBASE_STORAGE_OB_TENANT_SNAPSHOT_SERVICE_
#define OCEANBASE_STORAGE_OB_TENANT_SNAPSHOT_SERVICE_

#include "share/ob_rpc_struct.h"
#include "lib/lock/ob_rwlock.h"
#include "storage/slog_ckpt/ob_tenant_meta_snapshot_handler.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_mgr.h"
#include "storage/tenant_snapshot/ob_ls_snapshot_mgr.h"
#include "storage/tenant_snapshot/ob_tenant_clone_service.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_meta_table.h"

namespace oceanbase
{
namespace storage
{
class ObTenantSnapshot;

class ObTenantSnapshotService : public lib::TGRunnable
{
public:
  ObTenantSnapshotService();
  virtual ~ObTenantSnapshotService();

  static int mtl_init(ObTenantSnapshotService* &service);
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  void run1();
public:
  int get_all_ls_snapshot_keys(common::ObArray<ObLSSnapshotMapKey> &ls_snapshot_key_arr);
  int get_ls_snapshot_vt_info(const ObLSSnapshotMapKey &ls_snapshot_key, ObLSSnapshotVTInfo &ls_snapshot_vt_info);

  int create_tenant_snapshot(const obrpc::ObInnerCreateTenantSnapshotArg &arg);

  int drop_tenant_snapshot(const obrpc::ObInnerDropTenantSnapshotArg &arg);

  int get_ls_snapshot_tablet_meta_entry(const share::ObTenantSnapshotID &tenant_snapshot_id,
                                        const share::ObLSID &ls_id,
                                        blocksstable::MacroBlockId &tablet_meta_entry);

  int start_clone(const share::ObTenantSnapshotID &tenant_snapshot_id,
                  const share::ObLSID &ls_id,
                  blocksstable::MacroBlockId &tablet_meta_entry);

  int end_clone(const ObTenantSnapshotID &tenant_snapshot_id);

  int check_all_tenant_snapshot_released(bool& is_released);

  void dump_all_tenant_snapshot_info();

  void notify_unit_is_deleting();

  TO_STRING_KV(K(is_inited_), K(is_running_), K(running_mode_), K(meta_loaded_), K(tg_id_));
private:
  enum RUNNING_MODE
  {
    INVALID = 0,
    RESTORE = 1,
    CLONE   = 2,
    NORMAL  = 3,
    GC      = 4,
  };
private:
  int load_();

  int common_env_check_();
  int normal_running_env_check_();
  int clone_running_env_check_();
  int get_tenant_status_(share::schema::ObTenantStatus& tenant_status);
  int get_tenant_role_(share::ObTenantRole& tenant_role);
  int decide_running_mode_(enum RUNNING_MODE& running_mode);
  void run_in_normal_mode_();
  void run_in_clone_mode_();
  void run_in_gc_mode_();

  int wait_();
  int schedule_create_tenant_snapshot_dag_(const share::ObTenantSnapshotID& tenant_snapshot_id,
                                           const common::ObArray<share::ObLSID>& creating_ls_id_arr,
                                           const common::ObCurTraceId::TraceId& trace_id);
  int schedule_gc_tenant_snapshot_dag_(const share::ObTenantSnapshotID &tenant_snapshot_id,
                                       const common::ObArray<share::ObLSID> &gc_ls_id_arr,
                                       const bool gc_tenant_snapshot,
                                       const common::ObCurTraceId::TraceId& trace_id);
  int try_create_tenant_snapshot_(const share::ObTenantSnapshotID& tenant_snapshot_id);
  int try_create_tenant_snapshot_in_meta_table_();
  int try_gc_tenant_snapshot_();
  uint64_t calculate_idle_time_();
  int try_load_meta_();
  int check_if_tenant_has_been_dropped_(bool &has_dropped);

private:
  class TryGcTenantSnapshotFunctor {
  public:
    TryGcTenantSnapshotFunctor(bool tenant_has_been_dropped)
      : tenant_has_been_dropped_(tenant_has_been_dropped) {}

    ~TryGcTenantSnapshotFunctor() {}
    bool operator()(const share::ObTenantSnapshotID &tenant_snapshot_id, ObTenantSnapshot* tenant_snapshot);
  private:
    const bool tenant_has_been_dropped_;
  };

  class GetAllLSSnapshotMapKeyFunctor {
  public:
    GetAllLSSnapshotMapKeyFunctor(common::ObArray<ObLSSnapshotMapKey> *ls_snapshot_key_arr)
      : ls_snapshot_key_arr_(ls_snapshot_key_arr) {}
    bool operator()(const ObLSSnapshotMapKey &ls_snap_map_key, ObLSSnapshot *ls_snapshot);
  private:
    common::ObArray<ObLSSnapshotMapKey> *ls_snapshot_key_arr_;
  };

  class DumpTenantSnapInfoFunctor {
  public:
    bool operator()(const share::ObTenantSnapshotID &tenant_snapshot_id, ObTenantSnapshot* tenant_snapshot);
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantSnapshotService);

  bool is_inited_;
  bool is_running_;
  bool meta_loaded_;
  bool unit_is_deleting_;
  ObTenantSnapshotMgr tenant_snapshot_mgr_;
  ObLSSnapshotMgr ls_snapshot_mgr_;
  ObTenantMetaSnapshotHandler meta_handler_;

  common::ObThreadCond cond_;
  int tg_id_;


  RUNNING_MODE running_mode_;
  ObTenantCloneService clone_service_;
};

}
}
#endif
