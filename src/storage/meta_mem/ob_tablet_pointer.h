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
struct ObTabletResidentInfo;
typedef ObMetaObjGuard<ObTabletDDLKvMgr> ObDDLKvMgrHandle;

struct ObTabletAttr final
{
public:
  ObTabletAttr()
    :v_(0),
     ha_status_(0),
     all_sstable_data_occupy_size_(0),
     all_sstable_data_required_size_(0),
     tablet_meta_size_(0),
     ss_public_sstable_occupy_size_(0),
     backup_bytes_(0)
    {}
  ~ObTabletAttr() { reset(); }
  void reset() 
  { 
    v_ = 0; 
    ha_status_ = 0; 
    all_sstable_data_occupy_size_ = 0; 
    all_sstable_data_required_size_ = 0; 
    tablet_meta_size_ = 0; 
    ss_public_sstable_occupy_size_ = 0;
    backup_bytes_ = 0;
  }
  bool is_valid() const { return valid_; }
  TO_STRING_KV(K_(valid), K_(is_empty_shell), K_(has_transfer_table),
      K_(has_next_tablet), K_(has_nested_table), K_(ha_status), 
      K_(all_sstable_data_occupy_size), K_(all_sstable_data_required_size), K_(tablet_meta_size),
      K_(ss_public_sstable_occupy_size), K_(backup_bytes)
      );
public:
  union {
    int64_t v_;
    struct {
        bool valid_ : 1; // valid_ = true means attr is filled
        bool is_empty_shell_ : 1;
        bool has_transfer_table_ : 1;
        bool has_next_tablet_ : 1;
        bool has_nested_table_: 1;
    };
  };

  int64_t ha_status_;
  // all sstable data occupy_size, include major sstable
  // <data_block real_size> + <small_sstable_nest_size (in share_nothing)>
  int64_t all_sstable_data_occupy_size_;
  // all sstable data requred_size, data_block_count * 2MB, include major sstable
  int64_t all_sstable_data_required_size_;
  // meta_size in shared_nothing, meta_block_count * 2MB
  int64_t tablet_meta_size_;
  // major sstable data occupy_size
  // which is same as major_sstable_required_size_; 
  // because the alignment size is 1B in object_storage.
  int64_t ss_public_sstable_occupy_size_; 
  int64_t backup_bytes_;
};

class ObTabletPointer final
{
  friend class ObTablet;
  friend class ObLSTabletService;
  friend class ObTenantMetaMemMgr;
  friend class ObTabletResidentInfo;
  friend class ObTabletPointerMap;
  friend class ObFlyingTabletPointerMap;
public:
  ObTabletPointer();
  ObTabletPointer(const ObLSHandle &ls_handle,
      const ObMemtableMgrHandle &memtable_mgr_handle);
  ~ObTabletPointer();
  int get_in_memory_obj(ObMetaObjGuard<ObTablet> &guard);
  void get_obj(ObMetaObjGuard<ObTablet> &guard);
  void set_obj_pool(ObITenantMetaObjPool &obj_pool);
  void set_obj(const ObMetaObjGuard<ObTablet> &guard);
  void set_addr_without_reset_obj(const ObMetaDiskAddr &addr);
  void set_addr_with_reset_obj(const ObMetaDiskAddr &addr);
  OB_INLINE const ObMetaDiskAddr &get_addr() const { return phy_addr_; }

  int get_attr_for_obj(ObTablet *t);

  int deep_copy(char *buf, const int64_t buf_len, ObTabletPointer *&value) const;
  int64_t get_deep_copy_size() const;
  bool is_in_memory() const;

  void reset_obj();
  void reset();

  // load and dump interface
  int acquire_obj(ObTablet *&t);
  int read_from_disk(const bool is_full_load,
      common::ObArenaAllocator &allocator, char *&r_buf, int64_t &r_len, ObMetaDiskAddr &addr);
  int deserialize(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t buf_len,
      ObTablet *t);
  int deserialize(
      const char *buf,
      const int64_t buf_len,
      ObTablet *t);
  int hook_obj(const ObTabletAttr &attr, ObTablet *&t, ObMetaObjGuard<ObTablet> &guard);
  int release_obj(ObTablet *&t);
  int dump_meta_obj(ObMetaObjGuard<ObTablet> &guard, void *&free_obj);

  // do not KPC memtable_mgr, may dead lock
  TO_STRING_KV(K_(phy_addr), K_(obj), K_(ls_handle), K_(ddl_kv_mgr_handle), K_(attr),
      K_(protected_memtable_mgr_handle), K_(ddl_info), K_(initial_state), KP_(old_version_chain), K_(flying));
public:
  bool get_initial_state() const;
  ObTabletResidentInfo get_tablet_resident_info(const ObTabletMapKey &key) const;
  ObTabletMDSTruncateLock &get_mds_truncate_lock() const { return mds_lock_; }
  void set_initial_state(const bool initial_state);
  int create_ddl_kv_mgr(const share::ObLSID &ls_id, const ObTabletID &tablet_id, ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  void get_ddl_kv_mgr(ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int set_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int remove_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int get_mds_table(const ObTabletID &tablet_id, mds::MdsTableHandle &handle, bool not_exist_create = false);
  // interfaces forward to mds_table_handler_
  void mark_mds_table_deleted();
  void set_tablet_status_written();
  void reset_tablet_status_written();
  bool is_tablet_status_written() const;
  int try_release_mds_nodes_below(const share::SCN &scn);
  int try_gc_mds_table();
  int release_memtable_and_mds_table_for_ls_offline(const ObTabletID &tablet_id);
  // NOTICE1: input arg mds_ckpt_scn must be very carefully picked,
  // this scn should be calculated by mds table when flush,
  // and dumped to mds sstable, recorded on tablet
  // otherwise mds data will lost
  // NOTICE2: this call must be protected by ObTabletMdsSharedLockGuard EXCLUSIVE mode
  int release_mds_nodes_redo_scn_below(const ObTabletID &tablet_id, const share::SCN &mds_ckpt_scn);
  ObLS *get_ls() const;
  // the RW operations of tablet_attr are protected by lock guard of tablet_map_
  int set_tablet_attr(const ObTabletAttr &attr);
  bool is_old_version_chain_empty() const { return OB_ISNULL(old_version_chain_); }
  bool is_attr_valid() const { return attr_.is_valid(); }
  int64_t get_auto_part_size() const;
  void set_auto_part_size(const int64_t auto_part_size);
private:
  int scan_all_tablets_on_chain(const ObFunction<int(ObTablet &)> &op);// must be called under t3m bucket lock's protection
  int wash_obj();
  int add_tablet_to_old_version_chain(ObTablet *tablet);
  int remove_tablet_from_old_version_chain(ObTablet *tablet);
  void set_flying() { flying_ = true; }
  bool is_flying() const { return flying_; }
  bool need_push_to_flying_() const;
  bool need_remove_from_flying_() const;
private:
  ObMetaDiskAddr phy_addr_; // 48B
  ObMetaObj<ObTablet> obj_; // 40B
  ObLSHandle ls_handle_; // 24B
  ObDDLKvMgrHandle ddl_kv_mgr_handle_; // 48B
  ObProtectedMemtableMgrHandle protected_memtable_mgr_handle_; // 32B
  ObTabletDDLInfo ddl_info_; // 32B
  bool initial_state_; // 1B
  bool flying_; // 1B
  ObByteLock ddl_kv_mgr_lock_; // 1B
  mutable ObTabletMDSTruncateLock mds_lock_;// 8B
  mds::ObMdsTableHandler mds_table_handler_;// 48B
  ObTablet *old_version_chain_; // 8B
  ObTabletAttr attr_; // 32B // protected by rw lock of tablet_map_
  int64_t auto_part_size_; // 8B
  DISALLOW_COPY_AND_ASSIGN(ObTabletPointer); // 376B
};

struct ObTabletResidentInfo final
{
public:
  ObTabletResidentInfo() { reset(); }
  ObTabletResidentInfo(
    const ObTabletAttr &attr, 
    const ObMetaDiskAddr &tablet_addr, 
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id)
  : attr_(attr), tablet_addr_(tablet_addr), ls_id_(ls_id), tablet_id_(tablet_id)
    {}
  ~ObTabletResidentInfo() { reset(); };
  bool is_valid() const { return attr_.valid_ && tablet_id_.is_valid() && tablet_addr_.is_valid(); }
  bool has_transfer_table() const { return attr_.has_transfer_table_; }
  bool is_empty_shell() const { return attr_.is_empty_shell_; }
  bool has_next_tablet() const { return attr_.has_next_tablet_; }
  bool has_nested_table() const { return attr_.has_nested_table_; }
  int64_t get_required_size() const { return attr_.all_sstable_data_required_size_; }
  int64_t get_occupy_size() const { return attr_.all_sstable_data_occupy_size_; }
  uint64_t get_tablet_meta_size() const { return attr_.tablet_meta_size_; }
  int64_t get_ss_public_sstable_occupy_size() const { return attr_.ss_public_sstable_occupy_size_; }
  int64_t get_backup_size() const { return attr_.backup_bytes_; }
  void reset() 
  { 
    attr_.reset();
    tablet_addr_.set_none_addr();
    tablet_id_ = ObTabletID::INVALID_TABLET_ID;
    ls_id_ = ObLSID::INVALID_LS_ID;
  }
  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(tablet_addr), K_(attr));
public:
  ObTabletAttr attr_;
  ObMetaDiskAddr tablet_addr_; // used to identify one tablet
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
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

