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

#ifndef OCEANBASE_STORAGE_OB_TABLET_HANDLE
#define OCEANBASE_STORAGE_OB_TABLET_HANDLE

#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "share/leak_checker/obj_leak_checker.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/tablet/ob_tablet_table_store_iterator.h"

namespace oceanbase
{
namespace storage
{
enum class WashTabletPriority : int8_t
{
  WTP_HIGH = 0,
  WTP_LOW  = 1,
  WTP_MAX
};
class ObTablet;

class ObTabletHandle final
{
public:
  ObTabletHandle(const char *file = __builtin_FILE(),
                 const int line = __builtin_LINE(),
                 const char *func = __builtin_FUNCTION());
  ObTabletHandle(const ObTabletHandle& other) = delete;
  ObTabletHandle &operator= (const ObTabletHandle& other) = delete;
  ~ObTabletHandle();
  int assign(const ObTabletHandle &other);
  OB_INLINE ObTablet *get_obj() const { return obj_; }
  void reset();
  bool is_valid() const;
  int64_t get_buf_len() const { return get_buf_header().buf_len_; }
  DECLARE_TO_STRING;
private:
  friend class ObTenantMetaMemMgr;
  friend class ObSSMetaService;
  friend class ObSSTabletCreateHelper;
  friend class ObTabletPointer;
  friend class ObSSTabletDummyPointer;
  friend class ObTabletPointerMap;
  friend class ObTabletPersister;
  enum class ObTabletHdlType : int8_t
  {
    FROM_T3M       = 0, // get_tablet from t3m, memory may from full_allocator or pool of t3m;
    COPY_FROM_T3M  = 1, // get_tablet by external allocator of t3m;
    STANDALONE     = 2, // for acuqired/create_tmp_tablet and ss_tablet in local
    MAX            = 3
  };
  void set_obj(const ObTabletHdlType type, ObMetaObj<ObTablet> &obj);
  void set_obj(const ObTabletHdlType type, ObTablet *obj, common::ObIAllocator *allocator, ObTenantMetaMemMgr *t3m);
  void set_wash_priority(const WashTabletPriority priority) { wash_priority_ = priority; }
  common::ObArenaAllocator *get_allocator() { return static_cast<common::ObArenaAllocator *>(allocator_); }
  char *get_buf() { return reinterpret_cast<char *>(obj_); } // for ObTabletPersister
  bool need_hold_time_check() const { return true; }
  OB_INLINE void get_obj(ObMetaObj<ObTablet> &obj) const {obj.pool_ = obj_pool_; obj.allocator_ = allocator_; obj.ptr_ = obj_; obj.t3m_ = t3m_;} // for ObTabletPointer
  int register_into_leak_checker(const char *file, const int line, const char *func);
  int inc_ref_in_leak_checker(ObTenantMetaMemMgr *t3m);
  int dec_ref_in_leak_checker(ObTenantMetaMemMgr *t3m);
  int64_t calc_wash_score(const WashTabletPriority priority) const;
  ObMetaObjBufferHeader &get_buf_header() const
  {
    return ObMetaObjBufferHelper::get_buffer_header(reinterpret_cast<char *>(obj_));
  }
private:
  static const int64_t HOLD_OBJ_MAX_TIME = 2 * 60 * 60 * 1000 * 1000L; // 2h

  ObTabletHdlType type_;                // 1B
  WashTabletPriority wash_priority_;    // 1B
  int32_t index_;                       // 4B  initialize as -1
  ObTablet *obj_;                       // 8B
  ObITenantMetaObjPool *obj_pool_;      // 8B
  common::ObIAllocator *allocator_;     // 8B
  ObTenantMetaMemMgr *t3m_;             // 8B
  int64_t hold_start_time_;             // 8B

  DEFINE_OBJ_LEAK_DEBUG_NODE(node_);
};

class ObTabletTableIterator final
{
  friend class ObTablet;
  friend class ObLSTabletService;
  typedef ObSEArray<ObTabletHandle, 1> SplitExtraTabletHandleArray;
public:
  ObTabletTableIterator() : tablet_handle_(), table_store_iter_(), transfer_src_handle_(nullptr), split_extra_tablet_handles_() {}
  explicit ObTabletTableIterator(const bool is_reverse) : tablet_handle_(), table_store_iter_(is_reverse), transfer_src_handle_(nullptr), split_extra_tablet_handles_() {}
  int assign(const ObTabletTableIterator& other);
  ~ObTabletTableIterator() { reset(); }
  void reset()
  {
    table_store_iter_.reset();
    tablet_handle_.reset();
    if (nullptr != transfer_src_handle_) {
      transfer_src_handle_->~ObTabletHandle();
      ob_free(transfer_src_handle_);
      transfer_src_handle_ = nullptr;
    }
    split_extra_tablet_handles_.reset();
  }
  bool is_valid() const { return tablet_handle_.is_valid() || table_store_iter_.is_valid(); }
  ObTableStoreIterator *table_iter();
  const ObTableStoreIterator *table_iter() const;
  const ObTablet *get_tablet() const { return tablet_handle_.get_obj(); }
  ObTablet *get_tablet() { return tablet_handle_.get_obj(); }
  const ObTabletHandle &get_tablet_handle() { return tablet_handle_; }
  const ObTabletHandle *get_tablet_handle_ptr() const { return &tablet_handle_; }
  const ObIArray<ObTabletHandle> *get_split_extra_tablet_handles_ptr() const { return split_extra_tablet_handles_.empty() ? nullptr : &split_extra_tablet_handles_; }
  int set_tablet_handle(const ObTabletHandle &tablet_handle);
  int set_transfer_src_tablet_handle(const ObTabletHandle &tablet_handle);
  int add_split_extra_tablet_handle(const ObTabletHandle &tablet_handle);
  int refresh_read_tables_from_tablet(
      const int64_t snapshot_version,
      const bool allow_no_ready_read,
      const bool major_sstable_only,
      const bool need_split_src_table,
      const bool need_split_dst_table);
  int get_mds_sstables_from_tablet(const int64_t snapshot_version);
  int get_read_tables_from_tablet(
      const int64_t snapshot_version,
      const bool allow_no_ready_read,
      const bool major_sstable_only,
      const bool need_split_src_table,
      const bool need_split_dst_table,
      ObIArray<ObITable *> &tables);
  TO_STRING_KV(K_(tablet_handle), K_(transfer_src_handle), K_(table_store_iter), K_(split_extra_tablet_handles));
private:
  ObTabletHandle tablet_handle_;
  ObTableStoreIterator table_store_iter_;
  ObTabletHandle *transfer_src_handle_;
  SplitExtraTabletHandleArray split_extra_tablet_handles_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletTableIterator);
};

struct ObGetTableParam final
{
public:
  ObGetTableParam() : frozen_version_(-1), sample_info_(), tablet_iter_(), refreshed_merge_(nullptr), need_split_dst_table_(true) {}
  ~ObGetTableParam() { reset(); }
  bool is_valid() const { return tablet_iter_.is_valid(); }
  void reset()
  {
    frozen_version_ = -1;
    sample_info_.reset();
    tablet_iter_.reset();
    refreshed_merge_ = nullptr;
    need_split_dst_table_ = true;
  }
  TO_STRING_KV(K_(frozen_version), K_(sample_info), K_(tablet_iter), K_(need_split_dst_table));
public:
  int64_t frozen_version_;
  common::SampleInfo sample_info_;
  ObTabletTableIterator tablet_iter_;

  // when tablet has been refreshed, to notify other ObMultipleMerge in ObTableScanIterator re-inited
  // before rescan.
  void *refreshed_merge_;

  // true means maybe need split dst table, which is always safe because get_read_tables will check by mds again;
  // false means no need split dst table, which is for optimization and UNSAFE
  bool need_split_dst_table_;
  DISALLOW_COPY_AND_ASSIGN(ObGetTableParam);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_HANDLE
