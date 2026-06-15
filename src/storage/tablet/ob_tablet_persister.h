/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

  struct FullTabletPersistCtx final
  {
  public:
    FullTabletPersistCtx(
      const ObTabletPersisterParam &param,
      const ObTablet &tablet,
      /*ref*/ObTabletHandle &new_tablet_handle);
    ~FullTabletPersistCtx() { reset(); }
    bool is_valid() const { return is_inited_ && param_.is_valid() && tablet_.is_valid(); }
    int init();
    void reset();
    TO_STRING_KV(K_(is_inited),
                 K_(param),
                 K_(tablet),
                 K_(new_space_usage),
                 K_(new_tablet_handle),
                 K_(tablet_clustered_meta_size_not_aligned),
                 K_(pool_type));

  public:
    bool is_inited_;
    const ObTabletPersisterParam &param_;
    const ObTablet &tablet_;
    common::ObSEArray<ObSharedObjectsWriteCtx, 16> total_write_ctxs_;
    common::ObSEArray<MacroBlockId, 16> shared_meta_id_arr_;
    ObTabletTransformArg transform_arg_;
    ObTabletSpaceUsage new_space_usage_;
    ObBlockInfoSet new_block_info_set_;
    ObTabletMacroInfo new_tablet_macro_info_;
    ObTabletHandle &new_tablet_handle_;
    ObTabletTableStore new_table_store_;
    int64_t tablet_clustered_meta_size_not_aligned_;
    ObTabletPoolType pool_type_;

  private:
    DISALLOW_COPY_AND_ASSIGN(FullTabletPersistCtx);
  };

  class WriteResultApplier final
  {
  public:
    enum MetaType:uint8_t
    {
      AGGREGATED_META = 0,
      TABLET_TABLE_STORE = 1,
      STORAGE_SCHEMA = 2,
      MAX = 3
    };
    WriteResultApplier()
      : type_(MetaType::MAX),
        ctx_(nullptr)
    {
    }
    WriteResultApplier(
      const MetaType type,
      FullTabletPersistCtx &ctx)
      : type_(type),
        ctx_(&ctx)
    {
    }
    bool is_valid() const
    {
      return  type_ >= AGGREGATED_META
              && type_ < MAX
              && nullptr != ctx_
              && ctx_->is_valid();
    }
    int do_apply(const ObSharedObjectsWriteCtx &write_ctx) const;
    TO_STRING_KV(K_(type), KP_(ctx));

  private:
    int apply_aggregated_meta_res_(const ObSharedObjectsWriteCtx &ctx) const;
    int apply_secondary_meta_res_(const ObSharedObjectsWriteCtx &ctx) const;

  private:
    MetaType type_;
    FullTabletPersistCtx *ctx_;
  };

  class WriteBatch final
  {
  public:
    WriteBatch(const bool is_sub_meta);
    ~WriteBatch()
    {
      pending_write_infos_.reset();
      pending_res_appliers_.reset();
    }
    int append(const ObSharedObjectWriteInfo &write_info, const WriteResultApplier &res_applier);
    int sync_and_apply_write_res(/*out*/ObStorageObjectOpt &cur_opt);
    void reuse()
    {
      pending_write_infos_.reuse();
      pending_res_appliers_.reuse();
    }

  private:
    typedef common::hash::ObHashSet<
              uintptr_t,
              common::hash::NoPthreadDefendMode> CallbackSet;

  private:
    int get_shared_obj_reader_writer_(ObSharedObjectReaderWriter *&writer) const;
    int wait_write_info_callback_() const;
    DISALLOW_COPY_AND_ASSIGN(WriteBatch);

  private:
    bool is_sub_meta_;
    common::ObSEArray<ObSharedObjectWriteInfo, 16> pending_write_infos_;
    common::ObSEArray<WriteResultApplier, 16> pending_res_appliers_;
  };

private: // hide constructor
  ObTabletPersister(const int64_t start_macro_seq, const int64_t mem_ctx_id);
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
  /// @brief Batch persist and transform tablets.
  /// @note  This function is ONLY intended for use in the LS migration
  ///        scenario.
  /// @param[in]  params            persister parameters, one per tablet(params.count() == tablet_handles.count())
  /// @param[in]  tablet_handles    source tablet handles to be persisted
  /// @param[out] new_tablet_handles result tablet handles after persist and transform
  ///       NOTE: Both @param tablet_handles and @param new_tablet_handles are raw ObTabletHandle array.
  //              The caller must ensure the array capacity is >= total_tablet_cnt to avoid
  ///             out-of-bounds access.
  /// @return OB_SUCCESS on success, other error codes on failure
  static int batch_persist_and_transform_tablets(
      const ObUncopyableItemArray<ObTabletPersisterParam> &params,
      const ObTabletHandle* tablet_handles,
      const int64_t tablet_hdl_cnt,
      /*out*/ObTabletHandle *new_tablet_handles);
private:
#ifdef OB_BUILD_SHARED_STORAGE
  static int delete_blocks_(
    const common::ObIArray<ObSharedObjectsWriteCtx> &total_write_ctxs);
  int check_shared_root_macro_seq_(
    const blocksstable::ObStorageObjectOpt& shared_tablet_opt,
    const ObTabletHandle &tablet_hdl);
#endif
  int batch_make_persist_ctxs(
    const ObUncopyableItemArray<ObTabletPersisterParam> &params,
    const ObTabletHandle *tablet_handles,
    const int64_t tablet_hdl_cnt,
      /*out*/ObTabletHandle *new_tablet_handles,
      /*out*/ObUncopyableItemArray<FullTabletPersistCtx> &ctxs);
  int inner_batch_persist_and_transform_tablets(
    const ObUncopyableItemArray<ObTabletPersisterParam> &params,
    const ObTabletHandle *tablet_handles,
    const int64_t tablet_hdl_cnt,
    /*out*/ObTabletHandle *new_tablet_handles);
  int batch_acquire_and_transform_tablets(
    /*ref*/ObUncopyableItemArray<FullTabletPersistCtx> &ctxs,
    /*ref*/ObMultiTimeStats::TimeStats &time_stats);
  int batch_persist_secondary_meta(
    const ObUncopyableItemArray<ObTabletPersisterParam> &params,
    /*ref*/ObUncopyableItemArray<FullTabletPersistCtx> &ctxs,
    /*ref*/ObMultiTimeStats::TimeStats &time_stats);
  int batch_persist_aggregated_meta(
    const ObUncopyableItemArray<ObTabletPersisterParam> &params,
    /*ref*/ObUncopyableItemArray<FullTabletPersistCtx> &ctxs,
    /*ref*/ObMultiTimeStats::TimeStats &time_stats);
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
  int convert_secondary_meta_to_write_batch(
      FullTabletPersistCtx &ctx,
      ObMultiTimeStats *multi_stats,
      WriteBatch &write_batch);
  static int convert_arg_to_tablet(
      const ObTabletTransformArg &arg,
      ObTablet &tablet);
  int transform(
      const ObTabletPersisterParam &param,
      const ObTabletTransformArg &arg,
      ObMultiTimeStats *multi_stats,
      char *buf,
      const int64_t len);
  int persist_and_fill_tablet(
      FullTabletPersistCtx &ctx,
      ObMultiTimeStats *multi_stats,
      ObLinkedMacroBlockItemWriter &linked_writer);
  int calc_tablet_space_usage_(
      const ObBlockInfoSet &block_info_set,
      ObTabletHandle &new_tablet_hdl,
      ObIArray<MacroBlockId> &shared_meta_id_arr,
      ObTabletSpaceUsage &space_usage);
  int modify_and_fill_tablet(
      const ObTabletPersisterParam &param,
      const ObTablet &old_tablet,
      ObMultiTimeStats *multi_stats,
      ObITabletMetaModifier &modifier,
      ObTabletHandle &new_handle);
  int fetch_table_store_and_write_info(
      const ObTabletPersisterParam &param,
      const ObTablet &tablet,
      ObMultiTimeStats *multi_stats,
      ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
      ObSharedObjectWriteInfo &write_info,
      common::ObIArray<ObSharedObjectsWriteCtx> &meta_write_ctxs,
      ObTabletTableStore *new_table_store,
      int64_t &total_tablet_meta_size,
      ObBlockInfoSet &block_info_set);
  template <typename T>
  int fill_write_info_with_data_version(
      const ObTabletPersisterParam &param,
      common::ObArenaAllocator &allocator,
      const int64_t data_version,
      const T *t,
      ObSharedObjectWriteInfo &write_info);
  int persist_aggregated_meta(
      const ObTabletPersisterParam &param,
      const ObTabletMacroInfo &macro_info,
      ObTabletHandle &new_handle,
      ObTabletSpaceUsage &space_usage);
  static int apply_aggregated_meta_res(
      const ObTabletPersisterParam &param,
      const ObTabletMacroInfo &macro_info,
      const ObSharedObjectsWriteCtx &write_ctx,
      ObTabletHandle &new_handle,
      ObTabletSpaceUsage &space_usage);
  static void print_time_stats(
      const ObMultiTimeStats &multi_stats,
      const ObMultiTimeStats::TimeStats &time_stats,
      const int64_t stats_warn_threshold,
      const int64_t print_interval);
  static int build_tablet_meta_opt(
      const ObTabletPersisterParam &persist_param,
      blocksstable::ObStorageObjectOpt &opt);

public:
  static const int64_t SSTABLE_MAX_SERIALIZE_SIZE = 1966080L;  // 1.875MB
  static const int64_t DEFAULT_CTX_ID = 0;

private:
  ObArenaAllocator allocator_;
  int64_t cur_macro_seq_;

  DISALLOW_COPY_AND_ASSIGN(ObTabletPersister);
};

template <typename T>
int ObTabletPersister::fill_write_info_with_data_version(
    const ObTabletPersisterParam &param,
    common::ObArenaAllocator &allocator,
    const int64_t data_version,
    const T *t,
    ObSharedObjectWriteInfo &write_info)
{
  int ret = common::OB_SUCCESS;
  write_info.reset();
  if (OB_ISNULL(t)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid null member", K(ret), KP(t));
  } else {
    ObTabletPersistCommon::PersistWithDataVersionWrapper<T> member_wrapper(data_version, t);

    if (OB_FAIL(ObTabletPersistCommon::fill_write_info(param,
                                                       &member_wrapper,
                                                       allocator,
                                                       write_info))) {
      STORAGE_LOG(WARN, "fail to fill write info", K(ret), KPC(t), K(param));
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_TABLET_PERSIST_H_ */
