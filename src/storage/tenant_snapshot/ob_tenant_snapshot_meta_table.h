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

#ifndef OCEANBASE_STORAGE_OB_TENANT_SNAPSHOT_META_TABLE_
#define OCEANBASE_STORAGE_OB_TENANT_SNAPSHOT_META_TABLE_

#include "lib/container/ob_iarray.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "storage/ls/ob_ls.h"
#include "lib/hash/ob_hashset.h"
#include "share/tenant_snapshot/ob_tenant_snapshot_table_operator.h"

namespace oceanbase
{
namespace storage
{

struct ObLSSnapshotBuildCtxVTInfo
{
public:
  ObLSSnapshotBuildCtxVTInfo() : build_status_(),
                                 rebuild_seq_start_(0),
                                 rebuild_seq_end_(0),
                                 ls_meta_package_(),
                                 end_interval_scn_() {}
  void reset();

  void set_build_status(const common::ObString &build_status) { build_status_ = build_status; }
  void set_rebuild_seq_start(int64_t rebuild_seq_start) { rebuild_seq_start_ = rebuild_seq_start; }
  void set_rebuild_seq_end(int64_t rebuild_seq_end) { rebuild_seq_end_ = rebuild_seq_end; }
  void set_ls_meta_package(const ObLSMetaPackage &ls_meta_package) { ls_meta_package_ = ls_meta_package; }
  void set_end_interval_scn(const share::SCN &end_interval_scn) { end_interval_scn_ = end_interval_scn_; }

  const common::ObString &get_build_status() { return build_status_; }
  int64_t get_rebuild_seq_start() const { return rebuild_seq_start_; }
  int64_t get_rebuild_seq_end() const { return rebuild_seq_end_; }

  ObLSMetaPackage &get_ls_meta_package() { return ls_meta_package_; }
  share::SCN get_end_interval_scn() const { return end_interval_scn_; }

  TO_STRING_KV(K(build_status_),K(rebuild_seq_start_), K(rebuild_seq_end_),
               K(ls_meta_package_),K(end_interval_scn_));
private:
  common::ObString build_status_;
  int64_t rebuild_seq_start_;
  int64_t rebuild_seq_end_;
  ObLSMetaPackage ls_meta_package_;  // copy ls_meta_package
  share::SCN end_interval_scn_;
};

struct ObTenantSnapshotVTInfo
{
public:
  ObTenantSnapshotVTInfo() : tsnap_is_running_(false),
                             tsnap_has_unfinished_create_dag_(false),
                             tsnap_clone_ref_(0),
                             tsnap_meta_existed_(false) {}
  void reset();

  void set_tsnap_is_running(bool tsnap_is_running) { tsnap_is_running_ = tsnap_is_running; }
  void set_tsnap_has_unfinished_create_dag(bool tsnap_has_unfinished_create_dag)
  {
    tsnap_has_unfinished_create_dag_ = tsnap_has_unfinished_create_dag;
  }
  void set_tsnap_has_unfinished_gc_dag(bool tsnap_has_unfinished_gc_dag)
  {
    tsnap_has_unfinished_gc_dag_ = tsnap_has_unfinished_gc_dag;
  }
  void set_tsnap_clone_ref(int64_t tsnap_clone_ref) { tsnap_clone_ref_ = tsnap_clone_ref; }
  void set_tsnap_meta_existed(bool tsnap_meta_existed) { tsnap_meta_existed_ = tsnap_meta_existed; }

  bool get_tsnap_is_running() const { return tsnap_is_running_; }
  bool get_tsnap_has_unfinished_create_dag() const { return tsnap_has_unfinished_create_dag_; }
  bool get_tsnap_has_unfinished_gc_dag() const { return tsnap_has_unfinished_gc_dag_; }
  int64_t get_tsnap_clone_ref() const { return tsnap_clone_ref_; }
  bool get_tsnap_meta_existed() const { return tsnap_meta_existed_; }

  TO_STRING_KV(K(tsnap_is_running_), K(tsnap_has_unfinished_create_dag_),
               K(tsnap_has_unfinished_gc_dag_), K(tsnap_clone_ref_), K(tsnap_meta_existed_));
private:
  bool tsnap_is_running_;
  bool tsnap_has_unfinished_create_dag_;
  bool tsnap_has_unfinished_gc_dag_;
  int64_t tsnap_clone_ref_;
  bool tsnap_meta_existed_;
};

// info collection of ObLSSnapshot(for vtable)
struct ObLSSnapshotVTInfo
{
public:
  ObLSSnapshotVTInfo () : ls_id_(),
                          tenant_snapshot_id_(),
                          meta_existed_(false),
                          has_build_ctx_(false),
                          has_tsnap_info_(false) {}
  void reset();

  void set_ls_id(share::ObLSID ls_id) { ls_id_ = ls_id; }
  void set_tenant_snapshot_id(const share::ObTenantSnapshotID &tenant_snapshot_id)
  {
    tenant_snapshot_id_ = tenant_snapshot_id;
  }
  void set_meta_existed(bool meta_existed) { meta_existed_ = meta_existed; }
  void set_has_build_ctx(bool has_build_ctx) { has_build_ctx_ = has_build_ctx; }
  void set_build_ctx_info(ObLSSnapshotBuildCtxVTInfo &build_ctx_info) { build_ctx_info = build_ctx_info; }
  void set_has_tsnap_info(bool has_tsnap_info) { has_tsnap_info_ = has_tsnap_info; }
  void set_tsnap_info(ObTenantSnapshotVTInfo &tsnap_info) { tsnap_info_ = tsnap_info; }

  share::ObLSID get_ls_id() const { return ls_id_; }
  share::ObTenantSnapshotID get_tenant_snapshot_id() const { return tenant_snapshot_id_; }
  bool get_meta_existed() const { return meta_existed_; }
  bool get_has_build_ctx() const { return has_build_ctx_; }
  ObLSSnapshotBuildCtxVTInfo &get_build_ctx_info() { return build_ctx_info_; }
  bool get_has_tsnap_info() const { return has_tsnap_info_; }
  ObTenantSnapshotVTInfo &get_tsnap_info() { return tsnap_info_; }

  TO_STRING_KV(K(ls_id_), K(tenant_snapshot_id_), K(meta_existed_),
               K(has_build_ctx_), K(build_ctx_info_),
               K(has_tsnap_info_), K(tsnap_info_));
private:
  share::ObLSID ls_id_;
  share::ObTenantSnapshotID tenant_snapshot_id_;
  bool meta_existed_;

  bool has_build_ctx_; // flag to indicate whether carries build_ctx info, not show in vtable
  ObLSSnapshotBuildCtxVTInfo build_ctx_info_;

  bool has_tsnap_info_; // flag to indicate whether carries tenant snapshot info, not show in vtable
  ObTenantSnapshotVTInfo tsnap_info_;
};

class ObLSSnapshotReportInfo
{
public:
  ObLSSnapshotReportInfo(const share::ObLSID& ls_id): ls_id_(ls_id),
                                               is_creating_succ_(false),
                                               begin_interval_scn_(),
                                               end_interval_scn_(),
                                               ls_meta_package_(nullptr) {}
  bool is_valid() const;
  void to_failed();
  void to_success(const share::SCN& begin_interval_scn,
                  const share::SCN& end_interval_scn,
                  const ObLSMetaPackage* ls_meta_package);
  bool scn_range_is_valid(const ObTenantSnapItem &tenant_snap_item) const;

public:
  TO_STRING_KV(K(ls_id_),
               K(is_creating_succ_),
               K(begin_interval_scn_),
               K(end_interval_scn_),
               KPC(ls_meta_package_));

public:
  share::ObLSID get_ls_id() const { return ls_id_; }
  bool is_creating_succ() const { return is_creating_succ_; }
  share::SCN get_begin_interval_scn() const { return begin_interval_scn_; }
  share::SCN get_end_interval_scn() const { return end_interval_scn_; }
  const ObLSMetaPackage* get_ls_meta_package() const { return ls_meta_package_; }
private:
  share::ObLSID ls_id_;
  bool is_creating_succ_;
  share::SCN begin_interval_scn_;
  share::SCN end_interval_scn_;
  const ObLSMetaPackage* ls_meta_package_;
};

class ObTenantSnapshotSvrInfo
{
public:
  ObTenantSnapshotSvrInfo() : tenant_snap_item_(),
                              ls_snap_item_arr_() {}

  void reset();
  int get_creating_ls_id_arr(common::ObArray<share::ObLSID>& creating_ls_id_arr);
public:
  TO_STRING_KV(K(tenant_snap_item_), K(ls_snap_item_arr_));

public:
  share::ObTenantSnapItem& get_tenant_snap_item() { return tenant_snap_item_; }
  const share::ObTenantSnapItem& get_tenant_snap_item_const() const { return tenant_snap_item_; }

  common::ObArray<share::ObTenantSnapLSReplicaSimpleItem>& get_ls_snap_item_arr() {
    return ls_snap_item_arr_;
  }
  const common::ObArray<share::ObTenantSnapLSReplicaSimpleItem>& get_ls_snap_item_arr_const() const {
    return ls_snap_item_arr_;
  }
private:
  share::ObTenantSnapItem tenant_snap_item_;
  common::ObArray<share::ObTenantSnapLSReplicaSimpleItem> ls_snap_item_arr_;
};

class ObTenantSnapshotMetaTable
{
public:
  static int report_create_ls_snapshot_rlt(const share::ObTenantSnapshotID& tenant_snapshot_id,
                                           ObLSSnapshotReportInfo& info);

  static int acquire_tenant_snapshot_svr_info(const share::ObTenantSnapshotID& tenant_snapshot_id,
                                              ObTenantSnapshotSvrInfo& svr_info);

  static int acquire_all_tenant_snapshots(common::ObArray<ObTenantSnapItem>& item_arr);

  static int acquire_clone_ls_meta_package(const share::ObTenantSnapshotID &tenant_snapshot_id,
                                           const share::ObLSID &ls_id,
                                           ObLSMetaPackage& ls_meta_package);

  static void acquire_tenant_snapshot_trace_id(const share::ObTenantSnapshotID &tenant_snapshot_id,
                                               const share::ObTenantSnapOperation operation,
                                               common::ObCurTraceId::TraceId& trace_id);
private:
  static int update_ls_snap_replica_item_(const ObLSSnapshotReportInfo& info,
                                          share::ObTenantSnapLSReplicaSimpleItem& item);

};

}
}
#endif
