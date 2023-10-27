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
  compaction::ObExtraMediumInfo extra_medium_info_;
  ObMetaDiskAddr medium_info_list_addr_;
  ObMetaDiskAddr auto_inc_seq_addr_;
  ObTabletCreateDeleteMdsUserData tablet_status_cache_;
  ObTabletBindingMdsUserData aux_tablet_info_cache_;
  ObITable **ddl_kvs_;
  int64_t ddl_kv_count_;
  // memtable::ObIMemtable **memtables_;
  memtable::ObIMemtable *memtables_[MAX_MEMSTORE_CNT];
  int64_t memtable_count_;
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
  static int transform_empty_shell(const ObTablet &old_tablet, ObTabletHandle &new_handle);
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
      ObTabletTransformArg &arg);
  static int convert_arg_to_tablet(
      const ObTabletTransformArg &arg,
      ObTablet &tablet,
      ObArenaAllocator &allocator);
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
  static int load_auto_inc_seq_and_write_info(
      common::ObArenaAllocator &allocator,
      const ObTabletComplexAddr<share::ObTabletAutoincSeq> &complex_addr,
      const share::ObTabletAutoincSeq *&auto_inc_seq,
      common::ObIArray<ObSharedBlockWriteInfo> &write_infos,
      ObMetaDiskAddr &addr);
  static int fetch_table_store_and_write_info(
      const ObTablet &tablet,
      common::ObArenaAllocator &allocator,
      ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
      common::ObIArray<ObSharedBlockWriteInfo> &write_infos,
      common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs);
  static int load_storage_schema_and_fill_write_info(
      const ObTablet &tablet,
      common::ObArenaAllocator &allocator,
      common::ObIArray<ObSharedBlockWriteInfo> &write_infos);
  static int load_dump_kv_and_fill_write_info(
      common::ObArenaAllocator &allocator,
      const ObTabletComplexAddr<mds::MdsDumpKV> &complex_addr,
      common::ObIArray<ObSharedBlockWriteInfo> &write_infos,
      ObMetaDiskAddr &addr);
  static int load_medium_info_list_and_write(
      common::ObArenaAllocator &allocator,
      const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &complex_addr,
      common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs,
      ObMetaDiskAddr &addr);
  static int link_write_medium_info_list(
      const ObTabletDumpedMediumInfo *medium_info_list,
      common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs,
      ObMetaDiskAddr &addr);
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

template <typename T>
int ObTabletPersister::fill_write_info(
    common::ObArenaAllocator &allocator,
    const T *t,
    common::ObIArray<ObSharedBlockWriteInfo> &write_infos)
{
  int ret = common::OB_SUCCESS;

  if (OB_ISNULL(t)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(t));
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
