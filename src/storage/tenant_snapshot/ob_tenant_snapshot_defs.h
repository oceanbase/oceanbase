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

#ifndef OCEANBASE_STORAGE_OB_TENANT_SNAPSHOT_DEFS_
#define OCEANBASE_STORAGE_OB_TENANT_SNAPSHOT_DEFS_

#include "lib/hash/ob_link_hashmap.h"
#include "share/ob_ls_id.h"
#include "share/tenant_snapshot/ob_tenant_snapshot_id.h"
#include "share/tenant_snapshot/ob_tenant_snapshot_table_operator.h"
#include "storage/tenant_snapshot/ob_ls_snapshot_defs.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

class ObLSSnapshotMgr;
class ObLSSnapshot;
class ObTenantMetaSnapshotHandler;
class ObTenantSnapshotVTInfo;

typedef common::LinkHashValue<share::ObTenantSnapshotID> ObTenantSnapshotValue;
class ObTenantSnapshot : public ObTenantSnapshotValue
{
public:
  ObTenantSnapshot() :
    is_inited_(false),
    is_running_(false),
    tenant_snapshot_id_(),
    has_unfinished_create_dag_(false),
    has_unfinished_gc_dag_(false),
    clone_ref_(0),
    meta_existed_(false),
    ls_snapshot_mgr_(nullptr),
    meta_handler_(nullptr),
    mutex_() {}

  ~ObTenantSnapshot() { reset(); }
  int destroy();

  int init(const share::ObTenantSnapshotID& tenant_snapshot_id,
           ObLSSnapshotMgr* ls_snapshot_manager,
           ObTenantMetaSnapshotHandler* meta_handler);

  void reset()
  {
    if (IS_INIT) {
      is_running_ = false;
      tenant_snapshot_id_.reset();
      has_unfinished_create_dag_ = false;
      has_unfinished_gc_dag_ = false;
      clone_ref_ = 0;
      meta_existed_ = false;
      ls_snapshot_mgr_ = nullptr;
      meta_handler_ = nullptr;
      is_inited_ = false;
    }
  }

public:
  int is_valid() const { return tenant_snapshot_id_.is_valid(); }
  share::ObTenantSnapshotID get_tenant_snapshot_id() const { return tenant_snapshot_id_; }

  int load();
  int try_start_create_tenant_snapshot_dag(common::ObArray<share::ObLSID>& creating_ls_id_arr,
                                           common::ObCurTraceId::TraceId& trace_id);

  int try_start_gc_tenant_snapshot_dag(const bool tenant_has_been_dropped,
                                       bool &gc_tenant_snapshot,
                                       common::ObArray<share::ObLSID> &gc_ls_id_arr,
                                       common::ObCurTraceId::TraceId& trace_id);
  int execute_create_tenant_snapshot_dag(const common::ObArray<share::ObLSID> &creating_ls_id_arr);
  int execute_gc_tenant_snapshot_dag(const bool gc_tenant_snapshot, const common::ObArray<share::ObLSID> &gc_ls_id_arr);
  int finish_create_tenant_snapshot_dag();
  int finish_gc_tenant_snapshot_dag();

  void stop();
  bool is_stopped();

  int get_tenant_snapshot_vt_info(ObTenantSnapshotVTInfo &info);
  int get_ls_snapshot_tablet_meta_entry(const share::ObLSID &ls_id,
                                        blocksstable::MacroBlockId &tablet_meta_entry);

  int inc_clone_ref();
  int dec_clone_ref();

  TO_STRING_KV(K(is_inited_),
               K(is_running_),
               K(tenant_snapshot_id_),
               K(has_unfinished_create_dag_),
               K(has_unfinished_gc_dag_),
               K(clone_ref_),
               K(meta_existed_),
               KP(ls_snapshot_mgr_),
               KP(meta_handler_));
private:
  template<class Fn> class ForEachFilterFunctor
  {
  public:
    explicit ForEachFilterFunctor(const share::ObTenantSnapshotID &tenant_snapshot_id, Fn& fn)
        : tenant_snapshot_id_(tenant_snapshot_id), fn_(fn) {}
    ~ForEachFilterFunctor() {}
    bool operator()(const ObLSSnapshotMapKey &snapshot_key, ObLSSnapshot* ls_snapshot);

  private:
    const share::ObTenantSnapshotID tenant_snapshot_id_;
    Fn &fn_;
  };

  template<class Fn> class RemoveIfFilterFunctor
  {
  public:
    explicit RemoveIfFilterFunctor(const share::ObTenantSnapshotID &tenant_snapshot_id, Fn& fn)
        : tenant_snapshot_id_(tenant_snapshot_id), fn_(fn) {}
    ~RemoveIfFilterFunctor() {}
    bool operator()(const ObLSSnapshotMapKey &snapshot_key, ObLSSnapshot* ls_snapshot);

  private:
    const share::ObTenantSnapshotID tenant_snapshot_id_;
    Fn &fn_;
  };
  template <typename Fn> int for_each_(Fn &fn);
  template <typename Fn> int remove_if_(Fn &fn);

private:
  int clear_meta_snapshot_();

  int create_dag_start_();
  int create_dag_finish_();
  int gc_dag_start_();
  int gc_dag_finish_();
  void build_all_snapshots_(const common::ObArray<share::ObLSID>& creating_ls_id_arr);
  int build_tenant_snapshot_meta_();
  void build_all_ls_snapshots_(const common::ObArray<share::ObLSID>& creating_ls_id_arr);
  int build_one_ls_snapshot_(const share::ObLSID& creating_ls_id);
  int build_one_ls_snapshot_meta_(ObLSSnapshot* ls_snapshot);

  void report_one_ls_snapshot_build_rlt_(ObLSSnapshot* ls_snapshot, const int ls_ret);
  int report_create_ls_snapshot_succ_rlt_(ObLSSnapshot* ls_snapshot);
  int report_create_ls_snapshot_fail_rlt_(const share::ObLSID& ls_id);

  bool has_unfinished_dag_() { return has_unfinished_create_dag_ || has_unfinished_gc_dag_; }
  int gc_tenant_snapshot_();
  void notify_ls_snapshots_tenant_gc_();
  int gc_ls_snapshots_(const common::ObArray<share::ObLSID> &gc_ls_id_arr);
  int destroy_all_ls_snapshots_();
  int get_need_gc_ls_snapshot_arr_(
    const common::ObArray<share::ObTenantSnapLSReplicaSimpleItem>& item_arr,
    common::ObArray<share::ObLSID>& gc_ls_id_arr);
private:
  bool is_inited_;
  bool is_running_;
  share::ObTenantSnapshotID tenant_snapshot_id_;

  bool has_unfinished_create_dag_;
  bool has_unfinished_gc_dag_;
  int64_t clone_ref_;
  bool meta_existed_;
  ObLSSnapshotMgr* ls_snapshot_mgr_;

  ObTenantMetaSnapshotHandler* meta_handler_;
  lib::ObMutex mutex_;
};

}
}
#endif
