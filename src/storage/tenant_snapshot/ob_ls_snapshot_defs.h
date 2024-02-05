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

#ifndef OCEANBASE_STORAGE_OB_LS_SNAPSHOT_DEFS_
#define OCEANBASE_STORAGE_OB_LS_SNAPSHOT_DEFS_

#include "lib/hash/ob_link_hashmap.h"
#include "share/ob_ls_id.h"
#include "storage/ls/ob_ls_meta_package.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
class ObTenantSnapshot;
class ObLSSnapshot;
class ObLSSnapshotReportInfo;
class ObTenantMetaSnapshotHandler;
class ObLSSnapshotVTInfo;

class ObLSSnapshotMapKey final
{
public:
  ObLSSnapshotMapKey() : tenant_snapshot_id_(),
                         ls_id_(ObLSID::INVALID_LS_ID) {}
  ObLSSnapshotMapKey(const ObLSSnapshotMapKey &other) :
                         tenant_snapshot_id_(other.tenant_snapshot_id_),
                         ls_id_(other.ls_id_) {}
  explicit ObLSSnapshotMapKey(const int64_t tenant_snapshot_id, const int64_t ls_id) :
                         tenant_snapshot_id_(tenant_snapshot_id),
                         ls_id_(ls_id) {}
  explicit ObLSSnapshotMapKey(const share::ObTenantSnapshotID& tenant_snapshot_id, const share::ObLSID& ls_id) :
                         tenant_snapshot_id_(tenant_snapshot_id),
                         ls_id_(ls_id) {}
  ~ObLSSnapshotMapKey() { reset(); }

public:
  void reset()
  {
    tenant_snapshot_id_.reset();
    ls_id_.reset();
  }
  bool is_valid() const { return tenant_snapshot_id_.is_valid() && ls_id_.is_valid(); }
  // assignment
  ObLSSnapshotMapKey &operator=(const ObLSSnapshotMapKey &other)
  {
    tenant_snapshot_id_ = other.tenant_snapshot_id_;
    ls_id_ = other.ls_id_;
    return *this;
  }

  // compare operator
  bool operator == (const ObLSSnapshotMapKey &other) const
  { return tenant_snapshot_id_ == other.tenant_snapshot_id_ && ls_id_ == other.ls_id_; }

  bool operator != (const ObLSSnapshotMapKey &other) const
  { return tenant_snapshot_id_ != other.tenant_snapshot_id_ || ls_id_ != other.ls_id_; }

  int compare(const ObLSSnapshotMapKey &other) const
  {
    int compare_ret = tenant_snapshot_id_.compare(other.tenant_snapshot_id_);
    if (0 == compare_ret) {
      compare_ret = ls_id_.compare(other.ls_id_);
    }
    return compare_ret;
  }

  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = murmurhash(&tenant_snapshot_id_, sizeof(tenant_snapshot_id_), hash_val);
    hash_val = murmurhash(&ls_id_, sizeof(ls_id_), hash_val);
    return hash_val;
  }

  TO_STRING_KV(K_(tenant_snapshot_id), K_(ls_id));

public:
  share::ObTenantSnapshotID tenant_snapshot_id_;
  share::ObLSID ls_id_;
};

typedef common::LinkHashValue<ObLSSnapshotMapKey> ObLSSnapshotValue;
class ObLSSnapshot : public ObLSSnapshotValue
{
public:
  ObLSSnapshot()
    : is_inited_(false),
      tenant_snapshot_id_(),
      ls_id_(),
      meta_existed_(false),
      build_ctx_(nullptr),
      build_ctx_allocator_(nullptr),
      meta_handler_(nullptr),
      build_ctx_mutex_() {};

  ~ObLSSnapshot() { reset(); };

  int init(const share::ObTenantSnapshotID& tenant_snapshot_id,
           const ObLSID& ls_id,
           common::ObConcurrentFIFOAllocator* build_ctx_allocator,
           ObTenantMetaSnapshotHandler* meta_handler);
  void destroy();

  void reset()
  {
    if (IS_INIT) {
      tenant_snapshot_id_.reset();
      ls_id_.reset();
      meta_existed_ = false;
      try_free_build_ctx_();
      build_ctx_allocator_ = nullptr;
      meta_handler_ = nullptr;
      is_inited_ = false;
    }
  }

  ObLSSnapshot &operator=(const ObLSSnapshot &other) = delete;

public:
  share::ObTenantSnapshotID get_tenant_snapshot_id() const { return tenant_snapshot_id_; }
  share::ObLSID get_ls_id() const { return ls_id_; }

  int load();

  int gc_ls_snapshot();
  void notify_tenant_gc();

  bool is_build_finished() const;
  bool is_valid_for_reporting_succ() const;
  int get_report_info(ObLSSnapshotReportInfo& info);
  int get_ls_snapshot_vt_info(ObLSSnapshotVTInfo &info);
  void try_free_build_ctx();

  void try_determine_final_rlt();
  void try_set_failed();

  int build_ls_snapshot(ObLS* ls);

  int get_tablet_meta_entry(blocksstable::MacroBlockId &tablet_meta_entry);
  TO_STRING_KV(K(is_inited_),
               K(tenant_snapshot_id_),
               K(ls_id_),
               K(meta_existed_),
               KPC(build_ctx_),
               KP(build_ctx_allocator_),
               KP(meta_handler_));
private:
  bool is_build_ctx_lost_() const;
  int build_meta_snapshot_(share::SCN& max_sstable_range_scn);
  int clear_meta_snapshot_();
  int try_alloc_build_ctx_();
  void try_free_build_ctx_();

private:
  class ObLSSnapshotBuildCtx
  {
    enum BuildStatus
    {
      BUILDING      = 0,
      FAILED        = 1,
      SUCCESSFUL    = 2,
      MAX           = 3,
    };

  public:
    ObLSSnapshotBuildCtx () : build_status_(BUILDING),
                              rebuild_seq_start_(-1),
                              rebuild_seq_end_(-1),
                              ls_meta_package_(),
                              end_interval_scn_() {}

    ~ObLSSnapshotBuildCtx () {}

    TO_STRING_KV(K(build_status_),
                 K(rebuild_seq_start_),
                 K(rebuild_seq_end_),
                 K(ls_meta_package_),
                 K(end_interval_scn_));

    ObLSSnapshotBuildCtx &operator=(const ObLSSnapshotBuildCtx &other) = delete;

  public:
    bool is_valid_for_reporting_succ() const;
    bool is_finished() const;
    bool is_succ() const;
    bool is_failed() const;
    void set_failed();
    void determine_final_rlt();
    share::SCN get_begin_interval_scn() const;

  public:
    static const char * status_to_str(BuildStatus status);
    BuildStatus get_build_status() const { return build_status_; }
    int64_t get_rebuild_seq_start() const { return rebuild_seq_start_; }
    int64_t get_rebuild_seq_end() const { return rebuild_seq_end_; }
    share::SCN get_end_interval_scn() const { return end_interval_scn_; }

    void set_build_status(BuildStatus build_status) { build_status_ = build_status; }
    void set_rebuild_seq_start(int64_t rebuild_seq_start) { rebuild_seq_start_ = rebuild_seq_start; }
    void set_rebuild_seq_end(int64_t rebuild_seq_end) { rebuild_seq_end_ = rebuild_seq_end; }
    void set_end_interval_scn(const share::SCN& end_interval_scn) { end_interval_scn_ = end_interval_scn; }

    int set_ls_meta_package(ObLS* ls);
    void get_ls_meta_package(ObLSMetaPackage& out);
    const ObLSMetaPackage* get_ls_meta_package_const_ptr() const { return &ls_meta_package_; }

  private:
    static const char *LS_SNAPSHOT_BUILD_STATUS_ARRAY[];

  private:
    BuildStatus build_status_;
    int64_t rebuild_seq_start_;
    int64_t rebuild_seq_end_;

    lib::ObMutex ls_meta_package_mutex_;
    ObLSMetaPackage ls_meta_package_;

    share::SCN end_interval_scn_;
  };

private:
  bool is_inited_;
  share::ObTenantSnapshotID tenant_snapshot_id_;
  share::ObLSID ls_id_;
  bool meta_existed_;
  ObLSSnapshotBuildCtx *build_ctx_;
  common::ObConcurrentFIFOAllocator* build_ctx_allocator_;
  ObTenantMetaSnapshotHandler* meta_handler_;
  lib::ObMutex build_ctx_mutex_;
};

}
}
#endif
