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

#ifndef OCEANBASE_STORAGE_OB_TABLET_POINTER
#define OCEANBASE_STORAGE_OB_TABLET_POINTER

#include "lib/lock/ob_mutex.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_tablet_attr.h"
#include "storage/multi_data_source/runtime_utility/mds_lock.h"
#include "storage/tablet/ob_tablet_ddl_info.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/multi_data_source/mds_table_handler.h"
#include "storage/ob_i_memtable_mgr.h"
#include "storage/ob_protected_memtable_mgr_handle.h"
#include "ob_tablet_mds_truncate_lock.h"

namespace oceanbase
{
namespace storage
{
class ObTablet;
class ObTabletDDLKvMgr;
class ObTabletPointer;
class ObTabletResidentInfo;
typedef ObMetaObjGuard<ObTabletDDLKvMgr> ObDDLKvMgrHandle;

class ObTabletBasePointer
{
  friend class ObTablet;
  friend class ObLSTabletService;
  friend class ObTenantMetaMemMgr;
  friend class ObTabletPointerMap;
  friend class ObFlyingTabletPointerMap;
private:
  #define ONLY_LOG(INFO) int ret = OB_SUCCESS; STORAGE_LOG(ERROR, INFO)
  #define RET_NOT_MY_DUTY { ONLY_LOG("NOT MY DUTY"); return OB_ERR_SYS; }
  #define LOG_NOT_MY_DUTY_VOID { ONLY_LOG("NOT MY DUTY"); }
public: //  function shared with ObTabletPointer
  ObTabletBasePointer();
  virtual ~ObTabletBasePointer();
  OB_INLINE const ObMetaDiskAddr &get_addr() const { return phy_addr_; }
  bool is_in_memory() const;
  void set_addr_without_reset_obj(const ObMetaDiskAddr &addr);
  void set_addr_with_reset_obj(const ObMetaDiskAddr &addr);

public: // for_virtual_function, base_pointer return ret != OB_SUCCESS;
  virtual int get_in_memory_obj(ObTabletHandle &guard) RET_NOT_MY_DUTY;
  virtual int get_attr_for_obj(ObTablet *t) RET_NOT_MY_DUTY;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObTabletPointer *&value) const RET_NOT_MY_DUTY;
  virtual int release_mds_nodes_redo_scn_below(const ObTabletID &tablet_id, const share::SCN &mds_ckpt_scn) RET_NOT_MY_DUTY;
  virtual int try_release_mds_nodes_below(const share::SCN &scn) RET_NOT_MY_DUTY;
  virtual int try_gc_mds_table() RET_NOT_MY_DUTY;
  virtual int release_memtable_and_mds_table_for_ls_offline(const ObTabletID &tablet_id) RET_NOT_MY_DUTY;
  virtual int acquire_obj(ObTablet *&t) RET_NOT_MY_DUTY;
  virtual int read_from_disk(const bool is_full_load,
      common::ObArenaAllocator &allocator, char *&r_buf, int64_t &r_len, ObMetaDiskAddr &addr) RET_NOT_MY_DUTY;
  virtual int deserialize(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t buf_len,
      ObTablet *t) RET_NOT_MY_DUTY;
  virtual int deserialize(
      const char *buf,
      const int64_t buf_len,
      ObTablet *t) RET_NOT_MY_DUTY;
  virtual int release_obj(ObTablet *&t) RET_NOT_MY_DUTY;
  virtual int dump_meta_obj(ObTabletHandle &guard, void *&free_obj) RET_NOT_MY_DUTY;
  virtual int create_ddl_kv_mgr(const share::ObLSID &ls_id, const ObTabletID &tablet_id, ObDDLKvMgrHandle &ddl_kv_mgr_handle) RET_NOT_MY_DUTY;
  virtual int set_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle) RET_NOT_MY_DUTY;
  virtual int remove_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle) RET_NOT_MY_DUTY;


public: // no_ret, but need log_error
  virtual void set_obj_pool(ObITenantMetaObjPool &obj_pool) LOG_NOT_MY_DUTY_VOID;
  virtual void set_auto_part_size(const int64_t auto_part_size) LOG_NOT_MY_DUTY_VOID;
  virtual bool is_old_version_chain_empty() const RET_NOT_MY_DUTY;
  virtual int64_t get_deep_copy_size() const RET_NOT_MY_DUTY;
  virtual int64_t get_auto_part_size() const RET_NOT_MY_DUTY;
  virtual void mark_mds_table_deleted() LOG_NOT_MY_DUTY_VOID;
  virtual void set_tablet_status_written() LOG_NOT_MY_DUTY_VOID;
  virtual void reset_tablet_status_written() LOG_NOT_MY_DUTY_VOID;
  virtual bool is_tablet_status_written() const RET_NOT_MY_DUTY;
  virtual share::SCN get_tablet_max_checkpoint_scn() const { LOG_NOT_MY_DUTY_VOID; return share::SCN::invalid_scn(); }
  virtual void set_tablet_max_checkpoint_scn(const share::SCN &scn) LOG_NOT_MY_DUTY_VOID;
  virtual share::SCN get_ss_change_version() const { LOG_NOT_MY_DUTY_VOID; return share::SCN::invalid_scn(); }
  virtual share::SCN get_notify_ss_change_version() const { LOG_NOT_MY_DUTY_VOID; return share::SCN::invalid_scn(); }
  virtual void set_ss_change_version(const share::SCN &scn) LOG_NOT_MY_DUTY_VOID;
  virtual void set_notify_ss_change_version(const share::SCN &scn) LOG_NOT_MY_DUTY_VOID;
  virtual void get_ddl_kv_mgr(ObDDLKvMgrHandle &ddl_kv_mgr_handle) LOG_NOT_MY_DUTY_VOID;

public: // virtual func_for TabletPointer
  virtual ObLS *get_ls() const { ONLY_LOG("NOT MY DUTY"); return nullptr; }
  virtual void set_obj(const ObTabletHandle &guard);
  virtual void reset_obj();
  virtual void reset();
  virtual void get_obj(ObTabletHandle &guard);
  ObTabletMDSTruncateLock &get_mds_truncate_lock() const { return mds_lock_; }

  virtual bool get_initial_state() const { ONLY_LOG("NOT MY DUTY"); return true; }
  // return -4018 (OB_ENTRY_NOT_EXIST))
  virtual int get_mds_table(const ObTabletID &tablet_id, mds::MdsTableHandle &handle, bool not_exist_create = false);

  VIRTUAL_TO_STRING_KV(K_(phy_addr), K_(obj));

protected: // for_virtual_function, base_pointer return ret != OB_SUCCESS;
  virtual int scan_all_tablets_on_chain(const ObFunction<int(ObTablet &)> &op) RET_NOT_MY_DUTY;
  // virtual int wash_obj() RET_NOT_MY_DUTY;
  virtual int add_tablet_to_old_version_chain(ObTablet *tablet) RET_NOT_MY_DUTY;
  virtual int remove_tablet_from_old_version_chain(ObTablet *tablet) RET_NOT_MY_DUTY;
protected: // no_ret, but need log_error
  virtual void set_flying(const bool status) LOG_NOT_MY_DUTY_VOID;
  virtual bool is_flying() const RET_NOT_MY_DUTY;
  virtual bool need_push_to_flying_() const RET_NOT_MY_DUTY;
  virtual bool need_remove_from_flying_() const RET_NOT_MY_DUTY;
protected: // customized
  virtual ObProtectedMemtableMgrHandle* get_protected_memtable_mgr_handle() { ONLY_LOG("NOT MY DUTY"); return nullptr; }
  // do not rely on thess interface
  virtual ObTablet* get_old_version_chain_() const { ONLY_LOG("NOT MY DUTY"); return nullptr; }
  virtual ObTabletDDLInfo* get_ddl_info_() { ONLY_LOG("NOT MY DUTY"); return nullptr; }

protected:
  ObMetaDiskAddr phy_addr_; // 48B
  ObMetaObj<ObTablet> obj_; // 40B
  mutable ObTabletMDSTruncateLock mds_lock_;// 8B
};

class ObTabletPointer final : public ObTabletBasePointer
{
  friend class ObTablet;
  friend class ObLSTabletService;
  friend class ObTenantMetaMemMgr;
  friend class ObTabletResidentInfo;
  friend class ObTabletPointerMap;
  friend class ObFlyingTabletPointerMap;
public: // self_function
  ObTabletPointer();
  ObTabletPointer(const ObLSHandle &ls_handle,
      const ObMemtableMgrHandle &memtable_mgr_handle);
  virtual ~ObTabletPointer();

public: // for_virtual_function, base_pointer return ret != OB_SUCCESS;
  virtual int get_in_memory_obj(ObTabletHandle &guard) override;
  virtual int get_attr_for_obj(ObTablet *t) override;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObTabletPointer *&value) const override;
			  // NOTICE1: input arg mds_ckpt_scn must be very carefully picked,
  // this scn should be calculated by mds table when flush,
  // and dumped to mds sstable, recorded on tablet
  // ohterwise mds data will lost
  // NOTICE2: this call must be protected by TabletMdsWLockGuard
  virtual int release_mds_nodes_redo_scn_below(const ObTabletID &tablet_id, const share::SCN &mds_ckpt_scn) override;
  virtual int try_release_mds_nodes_below(const share::SCN &scn) override;
  virtual int try_gc_mds_table() override;
  virtual int release_memtable_and_mds_table_for_ls_offline(const ObTabletID &tablet_id) override;
  // load and dump interface
  virtual int acquire_obj(ObTablet *&t) override;
  virtual int read_from_disk(const bool is_full_load,
      common::ObArenaAllocator &allocator, char *&r_buf, int64_t &r_len, ObMetaDiskAddr &addr) override;
  virtual int deserialize(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t buf_len,
      ObTablet *t) override;
  virtual int deserialize(
      const char *buf,
      const int64_t buf_len,
      ObTablet *t) override;
  int hook_obj(ObTablet *&t, ObTabletHandle &guard);
  virtual int release_obj(ObTablet *&t) override;
  virtual int dump_meta_obj(ObTabletHandle &guard, void *&free_obj) override;
  virtual int create_ddl_kv_mgr(const share::ObLSID &ls_id, const ObTabletID &tablet_id, ObDDLKvMgrHandle &ddl_kv_mgr_handle) override;
  virtual int set_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle) override;
  virtual int remove_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle) override;

  ObTabletResidentInfo get_tablet_resident_info(const ObTabletMapKey &key) const;
  int set_tablet_attr(const ObTabletAttr &attr);
  bool is_attr_valid() const { return attr_.is_valid(); }

public: // no_ret, but need log_error
  virtual void set_obj_pool(ObITenantMetaObjPool &obj_pool) override;
  virtual void set_auto_part_size(const int64_t auto_part_size) override;
  virtual bool is_old_version_chain_empty() const override { return OB_ISNULL(old_version_chain_); }
  virtual int64_t get_deep_copy_size() const override;
  virtual int64_t get_auto_part_size() const override;
  // interfaces forward to mds_table_handler_
  virtual void mark_mds_table_deleted() override;
  virtual void set_tablet_status_written() override;
  virtual void reset_tablet_status_written() override;
  virtual bool is_tablet_status_written() const override;
  virtual share::SCN get_tablet_max_checkpoint_scn() const override { return attr_.tablet_max_checkpoint_scn_; }
  virtual void set_tablet_max_checkpoint_scn(const share::SCN &scn) override { attr_.tablet_max_checkpoint_scn_ = scn; }
  virtual share::SCN get_ss_change_version() const override { return attr_.ss_change_version_; }
  virtual share::SCN get_notify_ss_change_version() const override { return attr_.notify_ss_change_version_; }
  virtual void set_ss_change_version(const share::SCN &scn) override { attr_.ss_change_version_ = scn; };
  virtual void set_notify_ss_change_version(const share::SCN &scn) override { attr_.notify_ss_change_version_ = scn; }
  virtual void get_ddl_kv_mgr(ObDDLKvMgrHandle &ddl_kv_mgr_handle) override;

public: // for virtual_function, base_pointer return OB_SUCCESS but empty_result_set or nullptr;
  virtual ObLS *get_ls() const override;

public: // for_virtual_function, base_pointer do customize logical
  virtual void reset_obj() override;
  virtual void reset() override;
  virtual void set_obj(const ObTabletHandle &guard) override;
  virtual void get_obj(ObTabletHandle &guard) override;
  virtual bool get_initial_state() const override;
  virtual int get_mds_table(const ObTabletID &tablet_id, mds::MdsTableHandle &handle, bool not_exist_create = false) override;
  // do not KPC memtable_mgr, may dead lock
  VIRTUAL_TO_STRING_KV(K_(phy_addr), K_(obj), K_(ls_handle), K_(ddl_kv_mgr_handle), K_(attr),
      K_(protected_memtable_mgr_handle), K_(ddl_info), KP_(old_version_chain), K_(flying));

protected: // for_virtual_function, base_pointer return ret != OB_SUCCESS;
  virtual int scan_all_tablets_on_chain(const ObFunction<int(ObTablet &)> &op) override;// must be called under t3m bucket lock's protection
  // virtual int wash_obj() override;
  virtual int add_tablet_to_old_version_chain(ObTablet *tablet) override;
  virtual int remove_tablet_from_old_version_chain(ObTablet *tablet) override;
protected: // no_ret, but need log_error
  virtual void set_flying(const bool status) override { flying_ = status; }
  virtual bool is_flying() const override { return flying_; }
  virtual bool need_push_to_flying_() const override;
  virtual bool need_remove_from_flying_() const override;
protected: // customized
  virtual ObProtectedMemtableMgrHandle* get_protected_memtable_mgr_handle() override { return &protected_memtable_mgr_handle_; }
  // do not rely on thess interface
  virtual ObTablet* get_old_version_chain_() const override { return old_version_chain_; }
  virtual ObTabletDDLInfo* get_ddl_info_() override { return &ddl_info_; }

private:
  ObLSHandle ls_handle_; // 24B
  ObDDLKvMgrHandle ddl_kv_mgr_handle_; // 48B
  ObProtectedMemtableMgrHandle protected_memtable_mgr_handle_; // 32B
  ObTabletDDLInfo ddl_info_; // 32B
  bool flying_; // 1B
  ObByteLock ddl_kv_mgr_lock_; // 1B
  mds::ObMdsTableHandler mds_table_handler_;// 48B
  ObTablet *old_version_chain_; // 8B
  // the RW operations of tablet_attr are protected by lock guard of tablet_map_
  ObTabletAttr attr_; // 104B
  DISALLOW_COPY_AND_ASSIGN(ObTabletPointer); // 416B
};


class ObITabletFilterOp
{
public:
  ObITabletFilterOp()
    :skip_cnt_(0), total_cnt_(0), not_in_mem_cnt_(0), invalid_attr_cnt_(0)
  {}
  virtual ~ObITabletFilterOp();
  int operator()(const ObTabletResidentInfo &info, bool &is_skipped);
  virtual int do_filter(const ObTabletResidentInfo &info, bool &is_skipped) = 0;
  void inc_not_in_memory_cnt() { ++total_cnt_; ++not_in_mem_cnt_; }
  void inc_invalid_attr_cnt() { ++total_cnt_; ++invalid_attr_cnt_; }
private:
  int64_t skip_cnt_;
  int64_t total_cnt_;
  int64_t not_in_mem_cnt_;
  int64_t invalid_attr_cnt_;
  static int64_t total_skip_cnt_;
  static int64_t total_tablet_cnt_;
  static int64_t not_in_mem_tablet_cnt_;
  static int64_t invalid_attr_tablet_cnt_;
  DISALLOW_COPY_AND_ASSIGN(ObITabletFilterOp);
};

struct ScanAllVersionTabletsOp
{
  struct GetMinMdsCkptScnOp
  {
    explicit GetMinMdsCkptScnOp(share::SCN &min_mds_ckpt_scn);
    int operator()(ObTablet &);
    share::SCN &min_mds_ckpt_scn_;
  };
  struct GetMaxMdsCkptScnOp
  {
    explicit GetMaxMdsCkptScnOp(share::SCN &max_mds_ckpt_scn);
    int operator()(ObTablet &);
    share::SCN &max_mds_ckpt_scn_;
  };
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_POINTER
