/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_PERSIST_H_
#define OCEANBASE_STORAGE_OB_TABLET_PERSIST_H_

#include "lib/allocator/page_arena.h"
#include "storage/compaction/ob_extra_medium_info.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blockstore/ob_shared_block_reader_writer.h"
#include "storage/tablet/ob_tablet_complex_addr.h"
#include "storage/tablet/ob_tablet_dumped_medium_info.h"

namespace oceanbase
{
namespace storage
{
struct ObBlockInfoSet;
class ObTabletMacroInfo;
class ObCOSSTableV2;

struct ObSharedBlockIndex final
{
public:
  ObSharedBlockIndex()
    : shared_macro_id_(), nested_offset_(0)
  {
  }
  ObSharedBlockIndex(const blocksstable::MacroBlockId &shared_macro_id, const int64_t nested_offset)
    : shared_macro_id_(shared_macro_id), nested_offset_(nested_offset)
  {
  }
  ~ObSharedBlockIndex()
  {
    reset();
  }
  void reset()
  {
    shared_macro_id_.reset();
    nested_offset_ = 0;
  }
  int hash(uint64_t &hash_val) const;
  bool operator ==(const ObSharedBlockIndex &other) const;

  TO_STRING_KV(K_(shared_macro_id), K_(nested_offset));
public:
  blocksstable::MacroBlockId shared_macro_id_;
  int64_t nested_offset_;
};

class ObTabletTransformArg final
{
public:
  ObTabletTransformArg();
  ~ObTabletTransformArg();
  ObTabletTransformArg(const ObTabletTransformArg &) = delete;
  ObTabletTransformArg &operator=(const ObTabletTransformArg &) = delete;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(KP_(rowkey_read_info_ptr),
               K_(tablet_meta),
               K_(table_store_addr),
               K_(storage_schema_addr),
               K_(tablet_macro_info_addr),
               KP_(tablet_macro_info_ptr),
               K_(is_row_store));
public:
  const ObRowkeyReadInfo *rowkey_read_info_ptr_;
  const ObTabletMacroInfo *tablet_macro_info_ptr_;
  ObTabletMeta tablet_meta_;
  ObMetaDiskAddr table_store_addr_;
  ObMetaDiskAddr storage_schema_addr_;
  ObMetaDiskAddr tablet_macro_info_addr_;
  bool is_row_store_;
  ObDDLKV **ddl_kvs_;
  int64_t ddl_kv_count_;
  ObIMemtable *memtables_[MAX_MEMSTORE_CNT];
  int64_t memtable_count_;
  // If you want to add new member, make sure all member is assigned in 2 convert function.
  // ObTabletPersister::convert_tablet_to_mem_arg
  // ObTabletPersister::convert_tablet_to_disk_arg
};

class ObSSTablePersistWrapper final
{
public:
  explicit ObSSTablePersistWrapper(blocksstable::ObSSTable *sstable) : sstable_(sstable) { }
  ~ObSSTablePersistWrapper() { reset(); }
  void reset() { sstable_ = nullptr; }
  bool is_valid() const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int64_t get_serialize_size() const;
  TO_STRING_KV(KPC_(sstable));
private:
  const blocksstable::ObSSTable *sstable_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTablePersistWrapper);
};

class ObITabletMetaModifier
{
public:
  ObITabletMetaModifier() = default;
  virtual ~ObITabletMetaModifier() = default;
  virtual int modify_tablet_meta(ObTabletMeta &meta) = 0;
};


class ObMultiTimeStats
{
public:
  class TimeStats
  {
  public:
    explicit TimeStats(const char *owner);
    void click(const char *step_name);
    ~TimeStats() {}
    int64_t to_string(char *buf, const int64_t buf_len) const;
    int set_extra_info(const char *fmt, ...);
    int64_t get_total_time() const { return last_ts_ - start_ts_; }

  private:
    static const int64_t MAX_CLICK_COUNT = 16;
    static const int64_t MAX_EXTRA_INFO_LENGTH = 128;
    const char *owner_;
    int64_t start_ts_;
    int64_t last_ts_;
    const char *click_str_[MAX_CLICK_COUNT];
    int32_t click_[MAX_CLICK_COUNT];
    int32_t click_count_;
    char extra_info_[MAX_EXTRA_INFO_LENGTH];
    bool has_extra_info_;

    DISALLOW_COPY_AND_ASSIGN(TimeStats);
  };

  explicit ObMultiTimeStats(ObArenaAllocator *allocator);
  ~ObMultiTimeStats();
  int acquire_stats(const char *owner, TimeStats *&stats);
  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  static const int64_t MAX_STATS_CNT = 16;
  ObArenaAllocator *allocator_;
  TimeStats *stats_;
  int32_t stats_count_;

  DISALLOW_COPY_AND_ASSIGN(ObMultiTimeStats);
};

class ObTabletPersister final
{
public:
  static const int64_t MAP_EXTEND_RATIO = 2;
  typedef typename common::hash::ObHashMap<ObSharedBlockIndex,
                                          int64_t,
                                          common::hash::NoPthreadDefendMode,
                                          common::hash::hash_func<ObSharedBlockIndex>,
                                          common::hash::equal_to<ObSharedBlockIndex>,
                                          common::hash::SimpleAllocer<common::hash::HashMapTypes<ObSharedBlockIndex, int64_t>::AllocType>,
                                          common::hash::NormalPointer,
                                          oceanbase::common::ObMalloc,
                                          MAP_EXTEND_RATIO> SharedMacroMap;
  typedef typename SharedMacroMap::iterator SharedMacroIterator;
public:
  explicit ObTabletPersister(const int64_t ctx_id = 0);
  ~ObTabletPersister();
  // Persist the old tablet itself and all internal members, and transform it into a new tablet
  // from object pool. The old tablet can be allocated by allocator or from object pool.
  static int persist_and_transform_tablet(
      const ObTablet &old_tablet,
      ObTabletHandle &new_handle);
  static int persist_and_transform_only_tablet_meta(
      const ObTablet &old_tablet,
      ObITabletMetaModifier &modifier,
      ObTabletHandle &new_tablet);
  // copy from old tablet
  static int copy_from_old_tablet(
      const ObTablet &old_tablet,
      ObTabletHandle &new_handle);
  // change tablet memory footprint
  //  - degrade larger tablet objects to relatively smaller tablet objects, reducing the memory footprint.
  //  - upgrade smaller tablet objects to relatively larger tablet objects, achieving more performance.
  static int transform_tablet_memory_footprint(
      const ObTablet &old_tablet,
      char *buf,
      const int64_t len);
  static int convert_macro_info_map(SharedMacroMap &shared_macro_map, ObBlockInfoSet::TabletMacroMap &aggregated_info_map);
  static int copy_sstable_macro_info(
      const blocksstable::ObSSTable &sstable,
      SharedMacroMap &shared_macro_map,
      ObBlockInfoSet &block_info_set);
  static int copy_shared_macro_info(
      const blocksstable::ObSSTableMacroInfo &macro_info,
      SharedMacroMap &shared_macro_map,
      ObBlockInfoSet::TabletMacroSet &meta_id_set);
  static int copy_data_macro_ids(
      const blocksstable::ObSSTableMacroInfo &macro_info,
      ObBlockInfoSet &block_info_set);
  static int transform_empty_shell(const ObTablet &old_tablet, ObTabletHandle &new_handle);
private:
  static int inc_ref_with_macro_iter(ObTablet &tablet, ObMacroInfoIterator &macro_iter);
  static int do_copy_ids(
      blocksstable::ObMacroIdIterator &iter,
      ObBlockInfoSet::TabletMacroSet &id_set);
  static int check_tablet_meta_ids(
      const ObIArray<blocksstable::MacroBlockId> &shared_meta_id_arr,
      const ObTablet &tablet);
  static int acquire_tablet(
      const ObTabletPoolType &type,
      const ObTabletMapKey &key,
      const bool try_smaller_pool,
      ObTabletHandle &new_handle);
  static int convert_tablet_to_mem_arg(
      const ObTablet &tablet,
      ObTabletTransformArg &arg);
  int convert_tablet_to_disk_arg(
      const ObTablet &tablet,
      common::ObIArray<ObSharedBlocksWriteCtx> &total_write_ctxs,
      ObTabletPoolType &type,
      ObTabletTransformArg &arg,
      int64_t &total_tablet_meta_size,
      ObBlockInfoSet &block_info_set);
  static int convert_arg_to_tablet(
      const ObTabletTransformArg &arg,
      ObTablet &tablet);
  int transform(
      const ObTabletTransformArg &arg,
      char *buf,
      const int64_t len);
  int persist_and_fill_tablet(
      const ObTablet &old_tablet,
      ObLinkedMacroBlockItemWriter &linked_writer,
      common::ObIArray<ObSharedBlocksWriteCtx> &total_write_ctxs,
      ObTabletHandle &new_handle,
      ObTabletSpaceUsage &space_usage,
      ObTabletMacroInfo &macro_info,
      ObIArray<blocksstable::MacroBlockId> &shared_meta_id_arr);
  int modify_and_fill_tablet(
      const ObTablet &old_tablet,
      ObITabletMetaModifier &modifier,
      ObTabletHandle &new_handle);
  int fetch_and_persist_sstable(
      ObTableStoreIterator &table_iter,
      ObTabletTableStore &new_table_store,
      common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs,
      int64_t &total_tablet_meta_size,
      ObBlockInfoSet &block_info_set);
  static int fetch_and_persist_co_sstable(
    common::ObArenaAllocator &allocator,
    storage::ObCOSSTableV2 *co_sstable,
    common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs,
    common::ObIArray<ObMetaDiskAddr> &cg_addrs,
    int64_t &total_tablet_meta_size,
    ObBlockInfoSet &block_info_set,
    SharedMacroMap &shared_macro_map);
  static int batch_write_sstable_info(
    common::ObIArray<ObSharedBlockWriteInfo> &write_infos,
    common::ObIArray<ObSharedBlocksWriteCtx> &write_ctxs,
    common::ObIArray<ObMetaDiskAddr> &addrs,
    common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs,
    ObBlockInfoSet &block_info_set);
  int fetch_table_store_and_write_info(
      const ObTablet &tablet,
      ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
      common::ObIArray<ObSharedBlockWriteInfo> &write_infos,
      common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs,
      int64_t &total_tablet_meta_size,
      ObBlockInfoSet &block_info_set);
  static int load_storage_schema_and_fill_write_info(
      const ObTablet &tablet,
      common::ObArenaAllocator &allocator,
      common::ObIArray<ObSharedBlockWriteInfo> &write_infos);
  template <typename T>
  static int fill_write_info(
      common::ObArenaAllocator &allocator,
      const T *t,
      common::ObIArray<ObSharedBlockWriteInfo> &write_infos);
  static int write_and_fill_args(
      const common::ObIArray<ObSharedBlockWriteInfo> &write_infos,
      ObTabletTransformArg &arg,
      common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs,
      int64_t &total_tablet_meta_size,
      ObBlockInfoSet::TabletMacroSet &meta_block_id_set);
  int persist_aggregated_meta(
      const ObTabletMacroInfo &tablet_macro_info,
      ObTabletHandle &new_handle,
      ObTabletSpaceUsage &space_usage);
  static int fill_tablet_write_info(
      common::ObArenaAllocator &allocator,
      const ObTablet *tablet,
      const ObTabletMacroInfo &tablet_macro_info,
      ObSharedBlockWriteInfo &write_info);
  int load_table_store(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObMetaDiskAddr &addr,
      ObTabletTableStore *&table_store);
  void print_time_stats(
      const ObMultiTimeStats::TimeStats &time_stats,
      const int64_t stats_warn_threshold,
      const int64_t print_interval);

public:
  static const int64_t SSTABLE_MAX_SERIALIZE_SIZE = 1966080L;  // 1.875MB

private:
  ObArenaAllocator allocator_;
  ObMultiTimeStats multi_stats_;

  DISALLOW_COPY_AND_ASSIGN(ObTabletPersister);
};

template <typename T>
int ObTabletPersister::fill_write_info(
    common::ObArenaAllocator &allocator,
    const T *t,
    common::ObIArray<ObSharedBlockWriteInfo> &write_infos)
{
  int ret = common::OB_SUCCESS;
  int64_t size = 0;

  if (OB_ISNULL(t)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, tablet member is nullptr", K(ret), KP(t));
  } else if (FALSE_IT(size = t->get_serialize_size())) {
  } else if (OB_UNLIKELY(size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get serialize size", K(ret), KPC(t));
  } else {
    char *buf = static_cast<char *>(allocator.alloc(size));
    int64_t pos = 0;
    if (OB_ISNULL(buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory for serialize", K(ret), K(size));
    } else if (OB_FAIL(t->serialize(buf, size, pos))) {
      STORAGE_LOG(WARN, "fail to serialize member", K(ret), KP(buf), K(size), K(pos));
    } else {
      ObSharedBlockWriteInfo write_info;
      write_info.buffer_ = buf;
      write_info.offset_ = 0;
      write_info.size_ = size;
      write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
      if (OB_FAIL(write_infos.push_back(write_info))) {
        STORAGE_LOG(WARN, "fail to push back write info", K(ret));
      }
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_TABLET_PERSIST_H_ */
