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
#include "storage/blockstore/ob_shared_object_reader_writer.h"
#include "storage/tablet/ob_tablet_complex_addr.h"
#include "storage/tablet/ob_tablet_dumped_medium_info.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/tablet/ob_tablet_persist_common.h"



namespace oceanbase
{
namespace blocksstable
{
class ObMajorChecksumInfo;
}
namespace storage
{
struct ObBlockInfoSet;
class ObTabletMacroInfo;
class ObCOSSTableV2;

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
private:
  struct ObSSTableWriteOperator final : public ObSSTableMetaPersistHelper::IWriteOperator
  {
  public:
    /// @param[out] out_write_ctxs: recording written blocks, used for garbage collection
    /// if some errs happened during tablet persisting
    /// @param[out] block_info_set: updated by do_write(), used for building tablet macro info and calculating
    /// tablet space usage
    ObSSTableWriteOperator(
      const ObTabletPersisterParam &persist_param,
      common::ObIArray<ObSharedObjectsWriteCtx> &out_write_ctxs,
      ObBlockInfoSet &block_info_set,
      int64_t &start_macro_seq)
      : persist_param_(persist_param),
        out_write_ctxs_(out_write_ctxs),
        out_block_info_set_(block_info_set),
        start_macro_seq_(start_macro_seq)
    {
    }
    ~ObSSTableWriteOperator() = default;
    int do_write(const ObIArray<ObSharedObjectWriteInfo> &write_infos) override;
    bool is_valid() const override
    {
      return start_macro_seq_ >= 0
             && persist_param_.is_valid();
    }
    INHERIT_TO_STRING_KV(
      "IWriteOperator",
      IWriteOperator,
      K_(persist_param));

  private:
    DISALLOW_COPY_AND_ASSIGN(ObSSTableWriteOperator);

  private:
    const ObTabletPersisterParam &persist_param_;
    common::ObIArray<ObSharedObjectsWriteCtx> &out_write_ctxs_;
    ObBlockInfoSet &out_block_info_set_;
    int64_t &start_macro_seq_;
  };

private: // hide constructor
  ObTabletPersister(const ObTabletPersisterParam &param, const int64_t mem_ctx_id);
  ~ObTabletPersister();
  // Persist the old tablet itself and all internal members, and transform it into a new tablet
  // from object pool. The old tablet can be allocated by allocator or from object pool.
public:
  static int persist_and_transform_tablet(
      const ObTabletPersisterParam &param,
      const ObTablet &old_tablet,
      ObTabletHandle &new_handle);
  static int persist_and_transform_only_tablet_meta(
      const share::SCN &reorg_scn,
      const ObTabletPersisterParam &param,
      const ObTablet &old_tablet,
      ObITabletMetaModifier &modifier,
      ObTabletHandle &new_tablet);
  /// @brief Persist sstable's some fields into linked blocks if it is too large to
  /// fit one LogEntry. the fields include MacroIds and column groups(used in full
  /// direct load)
  static int persist_major_sstable_linked_block_if_large(
    ObArenaAllocator &allocator,
    const ObTabletPersisterParam &param,
    blocksstable::ObSSTable &sstable,
    ObCOSSTableV2 *&out_co_sstable,
    int64_t &out_macro_seq);
  // copy from old tablet
  static int copy_from_old_tablet(
      const ObTabletPersisterParam &param,
      const ObTablet &old_tablet,
      ObTabletHandle &new_handle);
  // change tablet memory footprint
  //  - degrade larger tablet objects to relatively smaller tablet objects, reducing the memory footprint.
  //  - upgrade smaller tablet objects to relatively larger tablet objects, achieving more performance.
  static int transform_tablet_memory_footprint(
      const ObTabletPersisterParam &param,
      const ObTablet &old_tablet,
      char *buf,
      const int64_t len);
  static int transform_empty_shell(const ObTabletPersisterParam &param, const ObTablet &old_tablet, ObTabletHandle &new_handle);
private:
#ifdef OB_BUILD_SHARED_STORAGE
  int delete_blocks_(
    const common::ObIArray<ObSharedObjectsWriteCtx> &total_write_ctxs);
  int check_shared_root_macro_seq_(
    const blocksstable::ObStorageObjectOpt& shared_tablet_opt,
    const ObTabletHandle &tablet_hdl);
#endif
  static int inner_persist_and_transform(
    const ObTabletPersisterParam &param,
    const ObTablet &old_tablet,
    ObTabletHandle &new_handle);
  static int inc_ref_with_macro_iter(ObTablet &tablet, ObMacroInfoIterator &macro_iter);
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
      common::ObIArray<ObSharedObjectsWriteCtx> &total_write_ctxs,
      ObTabletPoolType &type,
      ObTabletTableStore &new_table_store,
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
      common::ObIArray<ObSharedObjectsWriteCtx> &total_write_ctxs,
      ObTabletHandle &new_handle,
      ObTabletSpaceUsage &space_usage,
      ObTabletMacroInfo &macro_info,
      ObIArray<blocksstable::MacroBlockId> &shared_meta_id_arr);
  int calc_tablet_space_usage_(
      const ObBlockInfoSet &block_info_set,
      ObTabletHandle &new_tablet_hdl,
      ObIArray<MacroBlockId> &shared_meta_id_arr,
      ObTabletSpaceUsage &space_usage);
  int modify_and_fill_tablet(
      const ObTablet &old_tablet,
      ObITabletMetaModifier &modifier,
      ObTabletHandle &new_handle);
  int fetch_table_store_and_write_info(
      const ObTablet &tablet,
      ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
      common::ObIArray<ObSharedObjectWriteInfo> &write_infos,
      common::ObIArray<ObSharedObjectsWriteCtx> &meta_write_ctxs,
      ObTabletTableStore *new_table_store,
      int64_t &total_tablet_meta_size,
      ObBlockInfoSet &block_info_set);
  template <typename T>
  int fill_write_info_with_data_version(
      common::ObArenaAllocator &allocator,
      const int64_t data_version,
      const T *t,
      common::ObIArray<ObSharedObjectWriteInfo> &write_infos);
  int write_and_fill_args(
      const common::ObIArray<ObSharedObjectWriteInfo> &write_infos,
      ObTabletTransformArg &arg,
      common::ObIArray<ObSharedObjectsWriteCtx> &meta_write_ctxs,
      int64_t &total_tablet_meta_size,
      ObBlockInfoSet::TabletMacroSet &meta_block_id_set);
  int persist_aggregated_meta(
      const ObTabletMacroInfo &macro_info,
      ObTabletHandle &new_handle,
      ObTabletSpaceUsage &space_usage);
  int load_table_store(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObMetaDiskAddr &addr,
      ObTabletTableStore *&table_store);
  void print_time_stats(
      const ObMultiTimeStats::TimeStats &time_stats,
      const int64_t stats_warn_threshold,
      const int64_t print_interval);
  static int build_tablet_meta_opt(
      const ObTabletPersisterParam &persist_param,
      const storage::ObMetaDiskAddr &old_tablet_addr,
      blocksstable::ObStorageObjectOpt &opt);

public:
  static const int64_t SSTABLE_MAX_SERIALIZE_SIZE = 1966080L;  // 1.875MB
  static const int64_t DEFAULT_CTX_ID = 0;

private:
  ObArenaAllocator allocator_;
  ObMultiTimeStats multi_stats_;
  const ObTabletPersisterParam &param_;
  int64_t cur_macro_seq_;

  DISALLOW_COPY_AND_ASSIGN(ObTabletPersister);
};

template <typename T>
int ObTabletPersister::fill_write_info_with_data_version(
    common::ObArenaAllocator &allocator,
    const int64_t data_version,
    const T *t,
    common::ObIArray<ObSharedObjectWriteInfo> &write_infos)
{
  int ret = common::OB_SUCCESS;

  if (OB_ISNULL(t)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid null member", K(ret), KP(t));
  } else {
    ObSharedObjectWriteInfo write_info;
    ObTabletPersistCommon::PersistWithDataVersionWrapper<T> member_wrapper(data_version, t);

    if (OB_FAIL(ObTabletPersistCommon::fill_write_info(param_,
                                                       &member_wrapper,
                                                       allocator,
                                                       write_info))) {
      STORAGE_LOG(WARN, "fail to fill write info", K(ret), KPC(t), K(param_));
    } else if (OB_FAIL(write_infos.push_back(write_info))) {
      STORAGE_LOG(WARN, "fail to push back write info", K(ret), K(write_info));
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_TABLET_PERSIST_H_ */
