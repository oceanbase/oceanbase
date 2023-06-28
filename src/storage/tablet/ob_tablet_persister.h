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
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blockstore/ob_shared_block_reader_writer.h"
#include "storage/tablet/ob_tablet_complex_addr.h"
#include "storage/tablet/ob_tablet_dumped_medium_info.h"

namespace oceanbase
{
namespace storage
{
class ObTabletTransformArg final
{
public:
  ObTabletTransformArg();
  ~ObTabletTransformArg();
  ObTabletTransformArg(const ObTabletTransformArg &) = delete;
  ObTabletTransformArg &operator=(const ObTabletTransformArg &) = delete;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(KP_(auto_inc_seq_ptr),
               KP_(rowkey_read_info_ptr),
               K_(tablet_meta),
               K_(table_store_addr),
               K_(storage_schema_addr),
               K_(tablet_status_uncommitted_kv_addr),
               K_(tablet_status_committed_kv_addr),
               K_(aux_tablet_info_uncommitted_kv_addr),
               K_(aux_tablet_info_committed_kv_addr),
               K_(extra_medium_info),
               K_(medium_info_list_addr),
               K_(auto_inc_seq_addr),
               K_(tablet_status_cache),
               K_(aux_tablet_info_cache));
public:
  const share::ObTabletAutoincSeq *auto_inc_seq_ptr_;
  const ObRowkeyReadInfo *rowkey_read_info_ptr_;
  ObTabletMeta tablet_meta_;
  ObMetaDiskAddr table_store_addr_;
  ObMetaDiskAddr storage_schema_addr_;
  ObMetaDiskAddr tablet_status_uncommitted_kv_addr_;
  ObMetaDiskAddr tablet_status_committed_kv_addr_;
  ObMetaDiskAddr aux_tablet_info_uncommitted_kv_addr_;
  ObMetaDiskAddr aux_tablet_info_committed_kv_addr_;
  ObTaletExtraMediumInfo extra_medium_info_;
  ObMetaDiskAddr medium_info_list_addr_;
  ObMetaDiskAddr auto_inc_seq_addr_;
  ObTabletCreateDeleteMdsUserData tablet_status_cache_;
  ObTabletBindingMdsUserData aux_tablet_info_cache_;
  // If you want to add new member, make sure all member is assigned in 2 convert function.
  // ObTabletPersister::convert_tablet_to_mem_arg
  // ObTabletPersister::convert_tablet_to_disk_arg
};

class ObTabletPersister final
{
public:
  ObTabletPersister() = default;
  ~ObTabletPersister() = default;

  // Persist the old tablet itself and all internal members, and transform it into a new tablet
  // from object pool. The old tablet can be allocated by allocator or from object pool.
  static int persist_and_transform_tablet(
      const ObTablet &old_tablet,
      ObTabletHandle &new_handle);
  // change tablet memory footprint
  //  - degrade larger tablet objects to relatively smaller tablet objects, reducing the memory footprint.
  //  - upgrade smaller tablet objects to relatively larger tablet objects, achieving more performance.
  static int transform_tablet_memory_footprint(
      const ObTablet &old_tablet,
      char *buf,
      const int64_t len);
private:
  using FetchTableStore = std::function<int(ObTabletMemberWrapper<ObTabletTableStore> &)>;
  using FetchAutoincSeq = std::function<int(ObTabletMemberWrapper<share::ObTabletAutoincSeq> &)>;
  using LoadStorageSchema = std::function<int(common::ObArenaAllocator &, const ObStorageSchema *&)>;
  using LoadMediumInfoList = std::function<int(common::ObArenaAllocator &, const compaction::ObMediumCompactionInfoList *&)>;
  using LoadMdsDumpKV = std::function<int(common::ObArenaAllocator &, const mds::MdsDumpKV *&)>;
private:
  static int check_tablet_meta_ids(
      const common::ObIArray<ObSharedBlocksWriteCtx> &tablet_meta_write_ctxs,
      const ObTablet &tablet);
  static int acquire_tablet(
      const ObTabletPoolType &type,
      const ObTabletMapKey &key,
      const bool try_smaller_pool,
      ObTabletHandle &new_handle);
  static int convert_tablet_to_mem_arg(
      const ObTablet &tablet,
      ObTabletMemberWrapper<share::ObTabletAutoincSeq> &auto_inc_seq,
      ObTabletTransformArg &arg);
  static int convert_tablet_to_disk_arg(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      common::ObIArray<ObSharedBlocksWriteCtx> &tablet_meta_write_ctxs,
      common::ObIArray<ObSharedBlocksWriteCtx> &sstable_meta_write_ctxs,
      ObTabletPoolType &type,
      ObTabletMemberWrapper<share::ObTabletAutoincSeq> &auto_inc_seq,
      ObTabletTransformArg &arg);
  static int convert_arg_to_tablet(
      const ObTabletTransformArg &arg,
      ObTablet &tablet);
  static int transform(
      const ObTabletTransformArg &arg,
      char *buf,
      const int64_t len);
  static int recursively_persist(
      const ObTablet &old_tablet,
      common::ObArenaAllocator &allocator,
      common::ObIArray<ObSharedBlocksWriteCtx> &tablet_meta_write_ctxs,
      common::ObIArray<ObSharedBlocksWriteCtx> &sstable_meta_write_ctxs,
      ObTabletHandle &new_handle);
  static int persist_and_fill_tablet(
      const ObTablet &old_tablet,
      common::ObArenaAllocator &allocator,
      common::ObIArray<ObSharedBlocksWriteCtx> &tablet_meta_write_ctxs,
      common::ObIArray<ObSharedBlocksWriteCtx> &sstable_meta_write_ctxs,
      ObTabletHandle &new_handle);
  static int fetch_and_persist_sstable(
      common::ObArenaAllocator &allocator,
      ObTableStoreIterator &table_iter,
      ObTabletTableStore &new_table_store,
      common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs);
  template <typename Fetch, typename T>
  static int fetch_wrapper_and_write_info(
      common::ObArenaAllocator &allocator,
      Fetch &fetch,
      ObTabletMemberWrapper<T> &wrapper,
      common::ObIArray<ObSharedBlockWriteInfo> &write_infos,
      common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs);
  template <typename Load, typename T>
  static int load_member_and_write_info(
      common::ObArenaAllocator &allocator,
      Load &load,
      T *&t,
      common::ObIArray<ObSharedBlockWriteInfo> &write_infos);
  static int load_dump_kv_and_fill_write_info(
      common::ObArenaAllocator &allocator,
      const ObTabletComplexAddr<mds::MdsDumpKV> &complex_addr,
      common::ObIArray<ObSharedBlockWriteInfo> &write_infos);
  static int load_medium_info_list_and_write(
      common::ObArenaAllocator &allocator,
      const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &complex_addr,
      ObMetaDiskAddr &addr,
      common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs);
  static int link_write_medium_info_list(
      const ObTabletDumpedMediumInfo &medium_info_list,
      ObMetaDiskAddr &addr,
      common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs);
  template <typename T>
  static int fill_write_info(
      common::ObArenaAllocator &allocator,
      const T *t,
      common::ObIArray<ObSharedBlockWriteInfo> &write_infos);
  static int fill_write_info(
      common::ObArenaAllocator &allocator,
      const common::ObIArray<compaction::ObMediumCompactionInfo> &medium_info_list,
      common::ObIArray<ObSharedBlockWriteInfo> &write_infos);
  static int write_and_fill_args(
      const common::ObIArray<ObSharedBlockWriteInfo> &write_infos,
      ObTabletTransformArg &arg,
      common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs);
  static int persist_4k_tablet(
      common::ObArenaAllocator &allocator,
      ObTabletHandle &new_handle);
  static int load_table_store(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObMetaDiskAddr &addr,
      ObTabletTableStore *&table_store);
};

template <typename Fetch, typename T>
int ObTabletPersister::fetch_wrapper_and_write_info(
    common::ObArenaAllocator &allocator,
    Fetch &fetch,
    ObTabletMemberWrapper<T> &wrapper,
    common::ObIArray<ObSharedBlockWriteInfo> &write_infos,
    common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs)
{
  UNUSED(meta_write_ctxs);
  int ret = common::OB_SUCCESS;
  const T *member = nullptr;
  if (OB_FAIL(fetch(wrapper))) {
    STORAGE_LOG(WARN, "fail to fetch tablet wrapper", K(ret));
  } else if (OB_FAIL(wrapper.get_member(member))) {
    STORAGE_LOG(WARN, "fail to get tablet member", K(ret), K(wrapper));
  } else if (OB_ISNULL(member)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, member is nullptr", K(ret), KP(member));
  } else if (OB_FAIL(fill_write_info(allocator, member, write_infos))) {
    STORAGE_LOG(WARN, "fail to fill write info", K(ret), KP(member));
  }
  return ret;
};

template <>
inline int ObTabletPersister::fetch_wrapper_and_write_info<ObTabletPersister::FetchTableStore, ObTabletTableStore>(
    common::ObArenaAllocator &allocator,
    FetchTableStore &fetch,
    ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
    common::ObIArray<ObSharedBlockWriteInfo> &write_infos,
    common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs)
{
  int ret = common::OB_SUCCESS;
  ObTabletTableStore new_table_store;
  const ObTabletTableStore *table_store = nullptr;
  ObTableStoreIterator table_iter;
  if (OB_FAIL(fetch(wrapper))) {
    STORAGE_LOG(WARN, "fail to fetch table store", K(ret));
  } else if (OB_FAIL(wrapper.get_member(table_store))) {
    STORAGE_LOG(WARN, "fail to get table store from wrapper", K(ret), K(wrapper));
  } else if (OB_ISNULL(table_store)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, table store is nullptr", K(ret), KP(table_store));
  } else if (OB_FAIL(table_store->get_all_sstable(table_iter))) {
    STORAGE_LOG(WARN, "fail to get all sstable iterator", K(ret), KPC(table_store));
  } else if (OB_FAIL(fetch_and_persist_sstable(allocator, table_iter, new_table_store, meta_write_ctxs))) {
    STORAGE_LOG(WARN, "fail to fetch and persist sstable", K(ret), K(table_iter));
  } else if (OB_FAIL(fill_write_info(allocator, &new_table_store, write_infos))) {
    STORAGE_LOG(WARN, "fail to fill table store write info", K(ret), K(new_table_store));
  }
  return ret;
}

template <typename Load, typename T>
int ObTabletPersister::load_member_and_write_info(
    common::ObArenaAllocator &allocator,
    Load &load,
    T *&t,
    common::ObIArray<ObSharedBlockWriteInfo> &write_infos)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(load(allocator, t))) {
    STORAGE_LOG(WARN, "fail to load tablet member", K(ret));
  } else if (OB_ISNULL(t)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, t is nullptr", K(ret), KP(t));
  } else if (OB_FAIL(fill_write_info(allocator, t, write_infos))) {
    STORAGE_LOG(WARN, "fail to fill write info", K(ret), KP(t));
  }
  return ret;
}

template <typename T>
int ObTabletPersister::fill_write_info(
    common::ObArenaAllocator &allocator,
    const T *t,
    common::ObIArray<ObSharedBlockWriteInfo> &write_infos)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(t)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, tablet member is nullptr", K(ret), KP(t));
  } else {
    const int64_t size = t->get_serialize_size();
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
