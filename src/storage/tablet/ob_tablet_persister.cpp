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

#define USING_LOG_PREFIX STORAGE

#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "src/storage/ls/ob_ls.h"
#include "storage/slog_ckpt/ob_linked_macro_block_writer.h"
#include "storage/tablet/ob_tablet_block_aggregated_info.h"
#include "storage/tablet/ob_tablet_block_header.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/ob_upgrade_utils.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "storage/incremental/atomic_protocol/ob_atomic_tablet_meta_define.h"
#include "storage/incremental/atomic_protocol/ob_atomic_define.h"
#endif


using namespace std::placeholders;
using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
ObTabletPersister::ObTabletPersister(
    const ObTabletPersisterParam &param, const int64_t mem_ctx_id)
  : allocator_("TblPersist", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), mem_ctx_id),
    multi_stats_(&allocator_), param_(param), cur_macro_seq_(param.start_macro_seq_)
{
}

ObTabletPersister::~ObTabletPersister()
{
}

void ObTabletPersister::print_time_stats(
    const ObMultiTimeStats::TimeStats &time_stats,
    const int64_t stats_warn_threshold,
    const int64_t print_interval)
{
  int ret = OB_SUCCESS;
  if (time_stats.get_total_time() > stats_warn_threshold) {
    if (REACH_TIME_INTERVAL(100_ms)) {
      LOG_WARN("[TABLET PERSISTER TIME STATS] cost too much time\n", K_(multi_stats));
    }
  } else if (REACH_TIME_INTERVAL(print_interval)) {
    FLOG_INFO("[TABLET PERSISTER TIME STATS]\n", K_(multi_stats));
  }
}

/*static*/ int ObTabletPersister::build_tablet_meta_opt(
    const ObTabletPersisterParam &persist_param,
    const ObMetaDiskAddr &old_tablet_addr,
    ObStorageObjectOpt &opt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!persist_param.is_valid() || !old_tablet_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("persist param is invalid", K(ret), K(persist_param), K(old_tablet_addr));
  } else if (GCTX.is_shared_storage_mode()) {
#ifdef OB_BUILD_SHARED_STORAGE
    if (persist_param.for_ss_persist()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected call", K(ret));
    } else if (OB_UNLIKELY(!ObTabletTransferInfo::is_private_transfer_epoch_valid(persist_param.private_transfer_epoch_))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid private transfer epoch for private tablet persistence", K(ret), K(persist_param));
    } else { // private
      const ObLSID &ls_id = persist_param.ls_id_;
      const ObTabletID &tablet_id = persist_param.tablet_id_;
      ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
      ObTabletMapKey tablet_key(ls_id, tablet_id);
      // persist a tmp tablet or full mds tablet
      opt.set_ss_private_tablet_meta_object_opt(ls_id.id(), tablet_id.id(), persist_param.meta_version_, persist_param.private_transfer_epoch_);
    }
#endif
  } else {
    opt.set_private_meta_macro_object_opt(persist_param.tablet_id_.id(), persist_param.private_transfer_epoch_);
  }
  return ret;
}

int ObTabletPersister::persist_and_transform_tablet(
    const ObTabletPersisterParam &param,
    const ObTablet &old_tablet,
    ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid persister param", K(ret), K(param));
  } else if (OB_UNLIKELY(param.for_ss_persist())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("shared tablet meta persistence should not call this method", K(ret), K(lbt()));
  } else if (OB_UNLIKELY(new_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new handle should not be valid", K(ret), K(new_handle));
  } else if (OB_FAIL(inner_persist_and_transform(param, old_tablet, new_handle))) {
    LOG_WARN("persist and transform fail", K(ret), K(param));
  }
  return ret;
}

int ObTabletPersister::inner_persist_and_transform(
    const ObTabletPersisterParam &param,
    const ObTablet &old_tablet,
    ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;
  const int64_t ctx_id = share::is_reserve_mode()
                       ? ObCtxIds::MERGE_RESERVE_CTX_ID
                       : ObCtxIds::DEFAULT_CTX_ID;
  ObTabletPersister persister(param, ctx_id);
  ObMultiTimeStats::TimeStats *time_stats = nullptr;
  common::ObSEArray<ObSharedObjectsWriteCtx, 16> total_write_ctxs;
  ObLinkedMacroBlockItemWriter linked_writer;
  ObTabletSpaceUsage space_usage;
  int64_t total_tablet_meta_size = 0;
  ObTabletMacroInfo tablet_macro_info;
  total_write_ctxs.set_attr(lib::ObMemAttr(MTL_ID(), "TblMetaWriCtx", ctx_id));
  ObSArray<MacroBlockId> shared_meta_id_arr;

  if (OB_UNLIKELY(param.for_ss_persist())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected call", K(ret), K(param));
  } else if (OB_UNLIKELY(!old_tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid old tablet to persist", K(ret), K(old_tablet));
  } else if (OB_FAIL(persister.multi_stats_.acquire_stats("persist_and_transform_tablet", time_stats))) {
    LOG_WARN("fail to acquire time stats", K(ret));
  } else if (OB_FAIL(persister.persist_and_fill_tablet(
      old_tablet, linked_writer, total_write_ctxs, new_handle, space_usage, tablet_macro_info, shared_meta_id_arr))) {
    LOG_WARN("fail to persist and fill tablet", K(ret), K(old_tablet));
  } else if (FALSE_IT(time_stats->click("persist_and_fill_tablet"))) {
  } else if (OB_FAIL(check_tablet_meta_ids(shared_meta_id_arr, *(new_handle.get_obj())))) {
    LOG_WARN("fail to check whether tablet meta's macro ids match", K(ret), K(shared_meta_id_arr), KPC(new_handle.get_obj()));
  } else if (FALSE_IT(time_stats->click("check_tablet_meta_ids"))) {
  } else if (OB_FAIL(persister.persist_aggregated_meta(tablet_macro_info, new_handle, space_usage))) {
      LOG_WARN("fail to persist aggregated meta", K(ret), KPC(new_handle.get_obj()), K(space_usage));
  }

  if (OB_SUCC(ret)) {
    persister.print_time_stats(*time_stats, 20_ms, 1_s);
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObTabletPersister::delete_blocks_(
    const common::ObIArray<ObSharedObjectsWriteCtx> &total_write_ctxs)
{
  int ret = OB_SUCCESS;
  ObBlockInfoSet::TabletMacroSet deleting_block_set;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_FAIL(deleting_block_set.create(32/*bucket_num*/, "PersistDelBlk", "ObBlockSetNode", MTL_ID()))) {
    LOG_WARN("fail to create deleting block id set", K(ret), K(deleting_block_set));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < total_write_ctxs.count(); i++) {
    const ObSharedObjectsWriteCtx &write_ctx = total_write_ctxs.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < write_ctx.block_ids_.count(); j++) {
      if (write_ctx.block_ids_.at(j).is_private_data_or_meta() &&
          OB_FAIL(deleting_block_set.set_refactored(write_ctx.block_ids_.at(j)))) {
        LOG_WARN("fail to record the block waitting for delete", K(ret), K(total_write_ctxs));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get file manager", K(ret), K(MTL_ID()), KP(file_manager));
  }
  for (ObBlockInfoSet::SetIterator iter = deleting_block_set.begin();
      OB_SUCC(ret) && iter != deleting_block_set.end();
      ++iter) {
    if (OB_FAIL(file_manager->delete_file(iter->first, param_.ls_id_.id()))) {
      if (OB_OBJECT_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("this block has not been written_down when tablet_persist", K(iter->first));
      } else {
        LOG_WARN("fail to delete file", K(ret), K(iter->first), K(param_.ls_id_), KP(file_manager));
      }
    } else {
      LOG_INFO("succ delete block when tablet_persist failed", K(iter->first));
    }
  }
  return ret;
}
#endif

// !!!attention shouldn't be called by empty shell
/*static*/ int ObTabletPersister::persist_and_transform_only_tablet_meta(
    const share::SCN &reorg_scn,
    const ObTabletPersisterParam &param,
    const ObTablet &old_tablet,
    ObITabletMetaModifier &modifier,
    ObTabletHandle &new_tablet)
{
  int ret = OB_SUCCESS;
  ObTabletPersister persister(param, DEFAULT_CTX_ID);
  ObMultiTimeStats::TimeStats *time_stats = nullptr;
  ObTabletMacroInfo *macro_info = nullptr;
  common::ObSEArray<ObSharedObjectsWriteCtx, 16> total_write_ctxs;
  ObLinkedMacroBlockItemWriter linked_writer;
  ObTabletSpaceUsage space_usage;
  ObSArray<MacroBlockId> shared_meta_id_arr;
  total_write_ctxs.set_attr(lib::ObMemAttr(MTL_ID(), "TblMetaWriCtx", DEFAULT_CTX_ID));

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid persister param", K(ret), K(param));
  } else if (OB_UNLIKELY(param.for_ss_persist())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected call", K(ret), K(param));
  } else if (OB_UNLIKELY(!old_tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid old tablet", K(ret), K(old_tablet));
  } else if (OB_UNLIKELY(old_tablet.is_empty_shell())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this isn't supported for the empty shell tablet", K(ret), K(old_tablet));
  } else if (OB_UNLIKELY(!old_tablet.hold_ref_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old tablet doesn't hold ref cnt", K(ret), K(old_tablet));
  } else if (reorg_scn != old_tablet.get_reorganization_scn()) {
    ret = OB_TABLET_REORG_SCN_NOT_MATCH;
    LOG_WARN("tablet reorg scn is not same, cannot update", K(ret), K(reorg_scn), K(old_tablet));
  } else if (OB_NOT_NULL(old_tablet.allocator_)) {
#ifdef ERRSIM
  LOG_ERROR("this tablet has not been persisted before, which needs to be persisted before update restore_status", K(old_tablet));
#endif
    void *buf = nullptr;
    if (OB_ISNULL(buf = persister.allocator_.alloc(sizeof(ObTabletMacroInfo)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate tablet macro info memory", K(ret));
    } else if (FALSE_IT(macro_info = new (buf) ObTabletMacroInfo())) {
    } else if (OB_FAIL(persister.multi_stats_.acquire_stats("persist_and_transform_tablet", time_stats))) {
     LOG_WARN("fail to acquire time stats", K(ret));
    } else if (OB_FAIL(persister.persist_and_fill_tablet(old_tablet, linked_writer, total_write_ctxs, new_tablet,
            space_usage, *macro_info, shared_meta_id_arr))) {
      LOG_WARN("fail to persist and fill tablet", K(ret), K(old_tablet));
    } else if (OB_FAIL(check_tablet_meta_ids(shared_meta_id_arr, *(new_tablet.get_obj())))) {
      LOG_WARN("fail to check whether tablet meta's macro ids match", K(ret), K(shared_meta_id_arr), KPC(new_tablet.get_obj()));
    } else if (FALSE_IT(time_stats->click("check_tablet_meta_ids"))) {
    } else if (OB_FAIL(modifier.modify_tablet_meta(new_tablet.get_obj()->tablet_meta_))) {
      LOG_WARN("fail to modify tablet meta", K(ret), KPC(new_tablet.get_obj()));
    } else if (OB_FAIL(new_tablet.get_obj()->check_ready_for_read_if_need(old_tablet))) {
      LOG_WARN("fail to check ready for read if need", K(ret), K(old_tablet), K(new_tablet));
    } else {
      time_stats->click("transform_and_modify");
    }
  } else if (OB_FAIL(persister.multi_stats_.acquire_stats("persist_and_transform_only_tablet_meta", time_stats))) {
    LOG_WARN("fail to acquire time stats", K(ret));
  } else if (OB_FAIL(old_tablet.load_macro_info(param.ls_epoch_, persister.allocator_, macro_info))) {
    LOG_WARN("fail to fetch macro info", K(ret));
  } else if (FALSE_IT(time_stats->click("load_macro_info"))) {
  } else if (OB_FAIL(persister.modify_and_fill_tablet(old_tablet, modifier, new_tablet))) {
    LOG_WARN("fail to modify and fill tablet", K(ret), K(old_tablet));
  } else {
    time_stats->click("modify_and_fill_tablet");
    space_usage = old_tablet.get_tablet_meta().space_usage_;
    int64_t size = 0;
    if (OB_FAIL(old_tablet.get_tablet_addr().get_size_for_tablet_space_usage(size))) {
      LOG_WARN("fail to get size for tablet space usage", K(ret), K(old_tablet.get_tablet_addr()));
    } else {
      space_usage.tablet_clustered_meta_size_ -= upper_align(size, DIO_READ_ALIGN_SIZE);
    }

  }

  if (FAILEDx(persister.persist_aggregated_meta(*macro_info, new_tablet, space_usage))) {
    LOG_WARN("fail to persist aggregated meta", K(ret), KPC(macro_info), KPC(new_tablet.get_obj()), K(space_usage));
  } else {
    time_stats->click("persist_aggregated_meta");
    persister.print_time_stats(*time_stats, 20_ms, 1_s);
  }

  if (OB_NOT_NULL(macro_info)) {
    macro_info->reset();
  }
  return ret;
}

/*static*/ int ObTabletPersister::persist_major_sstable_linked_block_if_large(
    ObArenaAllocator &allocator,
    const ObTabletPersisterParam &param,
    blocksstable::ObSSTable &sstable,
    ObCOSSTableV2 *&out_co_sstable,
    int64_t &out_macro_seq)
{
  int ret = OB_SUCCESS;
  out_co_sstable = nullptr;
  out_macro_seq = 0;

#ifdef ERRSIM
  const int64_t large_co_sstable_threshold_config = GCONF.errsim_large_co_sstable_threshold;
  const int64_t large_co_sstable_threshold = 0 == large_co_sstable_threshold_config ? ObSSTableMetaPersistHelper::SSTABLE_MAX_SERIALIZE_SIZE : large_co_sstable_threshold_config;
#else
  const int64_t large_co_sstable_threshold = ObSSTableMetaPersistHelper::SSTABLE_MAX_SERIALIZE_SIZE;
#endif
  const int64_t ctx_id = share::is_reserve_mode()
    ? ObCtxIds::MERGE_RESERVE_CTX_ID
    : ObCtxIds::DEFAULT_CTX_ID;

  ObSEArray<ObSharedObjectsWriteCtx, 1> meta_write_ctxs;
  meta_write_ctxs.set_attr(lib::ObMemAttr(MTL_ID(), "PerstWriteCtxs", ctx_id));
  ObBlockInfoSet block_info_set;
  ObSSTableMetaPersistCtx sstable_persist_ctx(block_info_set, meta_write_ctxs, allocator);
  int64_t cur_macro_seq = param.start_macro_seq_;

  if (!sstable.is_major_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is not major", K(ret), K(sstable));
  } else if (OB_UNLIKELY(!param.is_valid()
                         || OB_ISNULL(param.ddl_redo_callback_)
                         || !param.is_major_shared_object())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid persister param", K(ret), K(param));
  } else if (OB_UNLIKELY(!param.is_major_shared_object())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("persist_major_sstable_linked_block_if_large only supports shared major", K(ret), K(param));
  } else if (OB_FAIL(block_info_set.init())) {
    LOG_WARN("fail to init block_info_set", K(ret));
  } else if (OB_FAIL(sstable_persist_ctx.init(ctx_id, 100))) {
    LOG_WARN("fail to init sstable_persist_ctx", K(ret), K(ctx_id), K(sstable_persist_ctx));
  }

  ObTabletPersister::ObSSTableWriteOperator cgsst_write_op(param, meta_write_ctxs, block_info_set, cur_macro_seq);
  ObSSTableMetaPersistHelper sst_persist_helper(param, sstable_persist_ctx, large_co_sstable_threshold, cur_macro_seq);

  if (FAILEDx(sst_persist_helper.register_write_op(&cgsst_write_op))) {
    LOG_WARN("failed to register sstable write op", K(ret), K(cgsst_write_op), K(sst_persist_helper));
  } else if (OB_FAIL(sst_persist_helper.do_persist_for_full_direct_load(&sstable,
                                                                        out_co_sstable))) {
    LOG_WARN("failed to persist major sstable", K(ret), K(sstable), K(cgsst_write_op), KP(out_co_sstable));
  }
  out_macro_seq = cur_macro_seq;
  return ret;
}

int ObTabletPersister::modify_and_fill_tablet(
    const ObTablet &old_tablet,
    ObITabletMetaModifier &modifier,
    ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;
  const ObTabletMeta &tablet_meta = old_tablet.get_tablet_meta();
  const ObTabletMapKey key(tablet_meta.ls_id_, tablet_meta.tablet_id_);
  const char* buf = reinterpret_cast<const char *>(&old_tablet);
  const bool try_smaller_pool = old_tablet.get_try_cache_size() > ObTenantMetaMemMgr::NORMAL_TABLET_POOL_SIZE
                                ? false : true;
  ObMetaObjBufferHeader &buf_header = ObMetaObjBufferHelper::get_buffer_header(const_cast<char *>(buf));
  ObTabletTransformArg arg;
  ObTabletPoolType type;
  ObMultiTimeStats::TimeStats *time_stats = nullptr;
  if (OB_FAIL(multi_stats_.acquire_stats("persist_and_transform_only_tablet_meta", time_stats))) {
    LOG_WARN("fail to acquire time stats", K(ret));
  } else if (OB_FAIL(ObTenantMetaMemMgr::get_tablet_pool_type(buf_header.buf_len_, type))) {
    LOG_WARN("fail to get tablet pool type", K(ret), K(buf_header));
  } else if (OB_FAIL(acquire_tablet(type, key, try_smaller_pool, new_handle))) {
    LOG_WARN("fail to acqurie tablet", K(ret), K(type), K(new_handle));
  } else if (OB_FAIL(convert_tablet_to_mem_arg(old_tablet, arg))) {
    LOG_WARN("fail to convert tablet to mem arg", K(ret), K(arg), K(old_tablet));
  } else if (FALSE_IT(time_stats->click("convert_tablet_to_mem_arg"))) {
  } else if (OB_FAIL(transform(arg, new_handle.get_buf(), new_handle.get_buf_len()))) {
    LOG_WARN("fail to transform tablet", K(ret), K(arg),
        KP(new_handle.get_buf()), K(new_handle.get_buf_len()), K(old_tablet));
  } else if (OB_FAIL(modifier.modify_tablet_meta(new_handle.get_obj()->tablet_meta_))) {
    LOG_WARN("fail to modify tablet meta", K(ret), KPC(new_handle.get_obj()));
  } else if (OB_FAIL(new_handle.get_obj()->check_ready_for_read_if_need(old_tablet))) {
    LOG_WARN("fail to check ready for read if need", K(ret), K(old_tablet), K(new_handle));
  } else {
    time_stats->click("transform_and_modify");
  }
  return ret;
}

/*static*/ int ObTabletPersister::copy_from_old_tablet(
    const ObTabletPersisterParam &param,
    const ObTablet &old_tablet,
    ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid persister param", K(ret), K(param));
  } else if (OB_UNLIKELY(param.for_ss_persist())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected call", K(ret), K(param));
  } else if (OB_NOT_NULL(old_tablet.allocator_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this isn't supported for the tablet from allocator", K(ret), K(old_tablet));
  } else if (OB_UNLIKELY(!old_tablet.hold_ref_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old tablet doesn't hold ref cnt", K(ret), K(old_tablet));
  } else {
    const ObTabletMeta &tablet_meta = old_tablet.get_tablet_meta();
    const ObTabletMapKey key(tablet_meta.ls_id_, tablet_meta.tablet_id_);
    const char* buf = reinterpret_cast<const char *>(&old_tablet);
    const bool try_smaller_pool = old_tablet.get_try_cache_size() > ObTenantMetaMemMgr::NORMAL_TABLET_POOL_SIZE
                                  ? false : true;
    ObMetaObjBufferHeader &buf_header = ObMetaObjBufferHelper::get_buffer_header(const_cast<char *>(buf));
    ObTabletTransformArg arg;
    ObTabletPoolType type;
    ObTabletPersister persister(param, DEFAULT_CTX_ID);
    ObMultiTimeStats::TimeStats *time_stats = nullptr;

    if (OB_FAIL(persister.multi_stats_.acquire_stats("copy_from_old_tablet", time_stats))) {
      LOG_WARN("fail to acquire time stats", K(ret));
    } else if (OB_FAIL(ObTenantMetaMemMgr::get_tablet_pool_type(buf_header.buf_len_, type))) {
      LOG_WARN("fail to get tablet pool type", K(ret), K(buf_header));
    } else if (OB_FAIL(acquire_tablet(type, key, try_smaller_pool, new_handle))) {
      LOG_WARN("fail to acqurie tablet", K(ret), K(type), K(new_handle));
    } else if (OB_FAIL(convert_tablet_to_mem_arg(old_tablet, arg))) {
      LOG_WARN("fail to convert tablet to mem arg", K(ret), K(arg), K(old_tablet));
    } else if (FALSE_IT(time_stats->click("convert_tablet_to_mem_arg"))) {
    } else if (OB_FAIL(persister.transform(arg, new_handle.get_buf(), new_handle.get_buf_len()))) {
      LOG_WARN("fail to transform tablet", K(ret), K(arg), KP(new_handle.get_buf()),
          K(new_handle.get_buf_len()), K(old_tablet));
    } else {
      time_stats->click("transform");
      persister.print_time_stats(*time_stats, 20_ms, 1_s);
      new_handle.get_obj()->set_tablet_addr(old_tablet.get_tablet_addr());
      if (OB_FAIL(new_handle.get_obj()->inc_macro_ref_cnt())) {
        LOG_WARN("fail to increase macro ref cnt for new tablet", K(ret), K(new_handle));
      }
    }
  }
  return ret;
}

int ObTabletPersister::convert_tablet_to_mem_arg(
    const ObTablet &tablet,
    ObTabletTransformArg &arg)
{
  int ret = OB_SUCCESS;
  arg.reset();
  if (OB_UNLIKELY(!tablet.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("old tablet isn't valid, don't allow to degrade tablet memory", K(ret), K(tablet));
  } else if (OB_FAIL(arg.tablet_meta_.assign(tablet.tablet_meta_))) {
    LOG_WARN("fail to copy tablet meta", K(ret), K(tablet));
  } else {
    arg.tablet_macro_info_addr_ = tablet.macro_info_addr_.addr_;
    arg.tablet_macro_info_ptr_ = tablet.macro_info_addr_.ptr_;
    arg.rowkey_read_info_ptr_ = tablet.rowkey_read_info_;
    arg.table_store_addr_ = tablet.table_store_addr_.addr_;
    arg.storage_schema_addr_ = tablet.storage_schema_addr_.addr_;
    arg.ddl_kvs_ = tablet.ddl_kvs_;
    arg.ddl_kv_count_ = tablet.ddl_kv_count_;
    MEMCPY(arg.memtables_, tablet.memtables_, sizeof(ObIMemtable*) * MAX_MEMSTORE_CNT);
    arg.memtable_count_ = tablet.memtable_count_;
    arg.new_table_store_ptr_ = tablet.table_store_addr_.ptr_;
    arg.table_store_cache_.assign(tablet.table_store_cache_);
  }
  return ret;
}

int ObTabletPersister::convert_tablet_to_disk_arg(
      const ObTablet &tablet,
      common::ObIArray<ObSharedObjectsWriteCtx> &total_write_ctxs,
      ObTabletPoolType &type,
      ObTabletTableStore &new_table_store,
      ObTabletTransformArg &arg,
      int64_t &total_tablet_meta_size,
      ObBlockInfoSet &block_info_set)
{
  int ret = OB_SUCCESS;
  ObMultiTimeStats::TimeStats *time_stats = nullptr;
  arg.reset();

  common::ObSEArray<ObSharedObjectWriteInfo, 2> write_infos;
  const int64_t ctx_id = share::is_reserve_mode()
                       ? ObCtxIds::MERGE_RESERVE_CTX_ID
                       : ObCtxIds::DEFAULT_CTX_ID;
  write_infos.set_attr(lib::ObMemAttr(MTL_ID(), "WriteInfos", ctx_id));
  // fetch member wrapper
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObStorageSchema *storage_schema = nullptr;

  if (OB_FAIL(multi_stats_.acquire_stats("convert_tablet_to_disk_arg", time_stats))) {
    LOG_WARN("fail to acquire time stats", K(ret));
  } else if (OB_FAIL(tablet.load_storage_schema(allocator_, storage_schema))) {
    LOG_WARN("fail to load storage schema", K(ret));
  } else if (OB_ISNULL(storage_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage schema is null", K(ret), KP(storage_schema));
  } else if (FALSE_IT(time_stats->click("load_storage_schema"))) {
  } else if (OB_FAIL(arg.tablet_meta_.assign(tablet.tablet_meta_))) {
    LOG_WARN("fail to copy tablet meta", K(ret), K(tablet));
  } else if (FALSE_IT(arg.rowkey_read_info_ptr_ = tablet.rowkey_read_info_)) {
  // } else if (FALSE_IT(arg.extra_medium_info_ = tablet.mds_data_.extra_medium_info_)) {
  // TODO: @baichangmin.bcm after mds_mvs joint debugging completed
  } else if (OB_FAIL(fetch_table_store_and_write_info(tablet, table_store_wrapper,
      write_infos, total_write_ctxs, &new_table_store, total_tablet_meta_size, block_info_set))) {
    LOG_WARN("fail to fetch table store and write info", K(ret));
  } else if (OB_FAIL(arg.table_store_cache_.init(new_table_store,
                                                 *storage_schema,
                                                 tablet.get_max_stored_sync_medium_scn()))) {
    LOG_WARN("fail to init table store cache", K(ret), K(tablet));
  } else {
    time_stats->click("fetch_table_store_and_write_info");
    arg.ddl_kvs_ = tablet.ddl_kvs_;
    arg.ddl_kv_count_ = tablet.ddl_kv_count_;
    arg.memtable_count_ = tablet.memtable_count_;
    arg.new_table_store_ptr_ = &new_table_store;
    MEMCPY(arg.memtables_, tablet.memtables_, sizeof(arg.memtables_));
  }

  ObSharedObjectWriteInfo write_info;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletPersistCommon::fill_write_info(param_, storage_schema, allocator_, write_info))) {
    LOG_WARN("fail to fill write info", K(ret), KP(storage_schema));
  } else if (OB_FAIL(write_infos.push_back(write_info))) {
    LOG_WARN("fail to push back write info", K(ret), K(write_info));
  } else if (FALSE_IT(time_stats->click("fill_storage_schema"))) {
  } else if (OB_FAIL(write_and_fill_args(write_infos, arg, total_write_ctxs, total_tablet_meta_size, block_info_set.shared_meta_block_info_set_))) {
    LOG_WARN("fail to write and fill address", K(ret), K(write_infos));
  } else if (FALSE_IT(time_stats->click("write_and_fill_args"))) {
  } else if (OB_FAIL(MTL(ObTenantMetaMemMgr *)->choose_tablet_pool_type(tablet.is_user_tablet(),
      tablet.get_try_cache_size(), table_store_wrapper.get_member()->get_try_cache_size(), type))) {
    LOG_WARN("fail to choose tablet pool type", K(ret), K(tablet));
  }
  ObTabletObjLoadHelper::free(allocator_, storage_schema);

  return ret;
}

int ObTabletPersister::persist_and_fill_tablet(
    const ObTablet &old_tablet,
    ObLinkedMacroBlockItemWriter &linked_writer,
    common::ObIArray<ObSharedObjectsWriteCtx> &total_write_ctxs,
    ObTabletHandle &new_handle,
    ObTabletSpaceUsage &space_usage,
    ObTabletMacroInfo &tablet_macro_info,
    ObIArray<MacroBlockId> &shared_meta_id_arr)
{
  int ret = OB_SUCCESS;
  ObTabletTableStore new_table_store;
  ObTabletTransformArg arg;
  ObBlockInfoSet block_info_set;
  ObMultiTimeStats::TimeStats *time_stats = nullptr;

  const ObTabletMeta &tablet_meta = old_tablet.get_tablet_meta();
  const ObTabletMapKey key(tablet_meta.ls_id_, tablet_meta.tablet_id_);
  ObTabletPoolType type = ObTabletPoolType::TP_NORMAL;
  bool try_smaller_pool = true;

  if (OB_UNLIKELY(param_.for_ss_persist())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected call", K(ret), K(param_));
  } else if (OB_FAIL(multi_stats_.acquire_stats("persist_and_fill_tablet", time_stats))) {
    LOG_WARN("fail to acquire time stats", K(ret));
  } else if (OB_FAIL(block_info_set.init())) {
    LOG_WARN("fail to init macro id set", K(ret));
  } else if (old_tablet.is_empty_shell()) {
    if (OB_FAIL(convert_tablet_to_mem_arg(old_tablet, arg))) {
      LOG_WARN("fail to conver tablet to mem arg", K(ret), K(old_tablet));
    } else {
      time_stats->click("convert_tablet_to_mem_arg");
    }
  } else if (OB_FAIL(convert_tablet_to_disk_arg(
      old_tablet, total_write_ctxs, type, new_table_store, arg, space_usage.tablet_clustered_meta_size_, block_info_set))) {
    LOG_WARN("fail to conver tablet to disk arg", K(ret), K(old_tablet));
  } else {
    time_stats->click("convert_tablet_to_disk_arg");
    if (old_tablet.get_try_cache_size() > ObTenantMetaMemMgr::NORMAL_TABLET_POOL_SIZE) {
      try_smaller_pool = false;
    }
  }

  if (FAILEDx(ObTabletPersistCommon::make_tablet_macro_info(param_,
                                                            cur_macro_seq_,
                                                            allocator_,
                                                            /* out */ block_info_set,
                                                            /* out */ linked_writer,
                                                            /* out */ tablet_macro_info))) {
    LOG_WARN("fail to make tablet macro info", K(ret), K(param_), K(old_tablet));
  } else {
    arg.tablet_macro_info_addr_.set_none_addr();
    arg.tablet_macro_info_ptr_ = &tablet_macro_info;
    time_stats->click("init_tabelt_macro_info");
  }

  if (OB_FAIL(ret)) {
  } else if (!new_handle.is_valid() && OB_FAIL(acquire_tablet(type, key, try_smaller_pool, new_handle))) {
    LOG_WARN("fail to acquire tablet", K(ret), K(key), K(type));
  } else if (OB_FAIL(transform(arg, new_handle.get_buf(), new_handle.get_buf_len()))) {
    LOG_WARN("fail to transform old tablet", K(ret), K(arg), K(new_handle), K(type));
  } else if (FALSE_IT(time_stats->click("transform"))) {
  } else if (OB_FAIL(ObTabletPersistCommon::calc_and_set_tablet_space_usage(block_info_set,
                                                                            shared_meta_id_arr,
                                                                            new_handle,
                                                                            space_usage))) {
    LOG_WARN("fail to calc tablet_space_usage");
  } else {
    time_stats->click("calc tablet space_usage");
  }

#ifdef OB_BUILD_SHARED_STORAGE
  if (OB_FAIL(ret) && GCTX.is_shared_storage_mode()) {
    // error process
    int tmp_ret = OB_SUCCESS;
    ObSharedObjectsWriteCtx tablet_linked_block_write_ctx;
    ObMetaDiskAddr addr;
    addr.set_block_addr(ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK,
                        0, /*offset*/
                        1, /*size*/
                        ObMetaDiskAddr::DiskType::BLOCK); // unused;
    tablet_linked_block_write_ctx.set_addr(addr);
    if (!linked_writer.is_closed() && OB_TMP_FAIL(linked_writer.close())) {
      LOG_WARN("fail to close block_writer", K(tmp_ret));
    }
    for (int64_t i = 0; i < linked_writer.get_meta_block_list().count(); ++i) {
      if (OB_TMP_FAIL(tablet_linked_block_write_ctx.add_object_id(linked_writer.get_meta_block_list().at(i)))) {
        LOG_WARN("fail to push_back macro_block", K(tmp_ret), K(i));
      }
    }
    if (OB_TMP_FAIL(total_write_ctxs.push_back(tablet_linked_block_write_ctx))) {
        LOG_WARN("fail to push_back tablet_linked_blocks", K(tmp_ret));
    }
    if (OB_TMP_FAIL(delete_blocks_(total_write_ctxs))) {
      LOG_WARN("fail to delete blocks", K(tmp_ret), K(total_write_ctxs));
    }
  }
#endif

  return ret;
}

int ObTabletPersister::calc_tablet_space_usage_(
    const ObBlockInfoSet &block_info_set,
    ObTabletHandle &new_tablet_hdl,
    ObIArray<MacroBlockId> &shared_meta_id_arr,
    ObTabletSpaceUsage &space_usage)
{
  int ret = OB_SUCCESS;
  backup::ObBackupDeviceMacroBlockId tmp_back_block_id;
  int64_t clustered_sstable_size = 0;
  int64_t backup_block_size = 0; // for sstable has backup_block and local_block;
  int64_t pure_backup_sstable_size = 0; // for sstable has no local_block
  for (ObBlockInfoSet::SetIterator iter = block_info_set.backup_block_info_set_.begin();
      OB_SUCC(ret) && iter != block_info_set.backup_block_info_set_.end();
      ++iter) {
    if (OB_FAIL(tmp_back_block_id.set(iter->first))) {
      LOG_WARN("failed to get backup block_id");
    } else {
      backup_block_size += tmp_back_block_id.get_length();
    }
  }
  for (ObBlockInfoSet::MapIterator iter = block_info_set.clustered_data_block_info_map_.begin();
      iter != block_info_set.clustered_data_block_info_map_.end();
      ++iter) {
    clustered_sstable_size += iter->second;
  }
  for (ObBlockInfoSet::SetIterator iter = block_info_set.shared_meta_block_info_set_.begin();
      OB_SUCC(ret) && iter != block_info_set.shared_meta_block_info_set_.end();
      ++iter) {
    if (OB_FAIL(shared_meta_id_arr.push_back(iter->first))) {
      LOG_WARN("fail to push back macro id", K(ret), K(iter->first));
    }
  }

  int64_t ss_public_sstable_occupy_size = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(new_tablet_hdl.get_obj()->calc_sstable_occupy_size(space_usage.all_sstable_data_occupy_size_,
                                                                        ss_public_sstable_occupy_size,
                                                                        pure_backup_sstable_size))) {
    LOG_WARN("failed to calc tablet occupy_size", K(ret), KPC(new_tablet_hdl.get_obj()));
  } else {
    if (GCTX.is_shared_storage_mode()) {
      space_usage.all_sstable_data_required_size_ = space_usage.all_sstable_data_occupy_size_;
      // TODO @zs475329, all_sstable_meta_size_ should be the sum of all the sstable.meta_occupy_size_ (Both SS and SN)
      space_usage.all_sstable_meta_size_ = block_info_set.meta_block_info_set_.size() * 16 * 1024; // 16KB;
    } else {
      space_usage.all_sstable_data_required_size_ = block_info_set.data_block_info_set_.size() * DEFAULT_MACRO_BLOCK_SIZE;
      space_usage.all_sstable_meta_size_ = block_info_set.meta_block_info_set_.size() * DEFAULT_MACRO_BLOCK_SIZE;
    }
    space_usage.backup_bytes_ = backup_block_size + pure_backup_sstable_size;
    space_usage.tablet_clustered_sstable_data_size_ = clustered_sstable_size;
    // major_sstable_sizes are only used for shared_storage
    space_usage.ss_public_sstable_occupy_size_ = ss_public_sstable_occupy_size;
    new_tablet_hdl.get_obj()->set_space_usage_(space_usage);
  }
  return ret;
}

int ObTabletPersister::transform_empty_shell(
    const ObTabletPersisterParam &param, const ObTablet &old_tablet, ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;

  ObLinkedMacroBlockItemWriter linked_writer;
  common::ObArray<ObSharedObjectsWriteCtx> total_write_ctxs;
  ObTabletSpaceUsage space_usage;
  ObTabletMacroInfo tablet_macro_info;
  ObTabletPersister persister(param, DEFAULT_CTX_ID);
  ObSArray<MacroBlockId> shared_meta_id_arr;

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid persister param", K(ret), K(param));
  } else if (OB_UNLIKELY(param.for_ss_persist())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected call", K(ret), K(param));
  } else if (OB_UNLIKELY(!old_tablet.is_empty_shell())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only support transform empty shell", K(ret), K(old_tablet));
  } else if (OB_FAIL(persister.persist_and_fill_tablet(old_tablet, linked_writer,
      total_write_ctxs, new_handle, space_usage, tablet_macro_info, shared_meta_id_arr))) {
    LOG_WARN("fail to persist old empty shell", K(ret), K(old_tablet));
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(check_tablet_meta_ids(shared_meta_id_arr, *(new_handle.get_obj())))) {
      LOG_WARN("fail to check whether tablet meta's macro ids match", K(ret), K(shared_meta_id_arr), KPC(new_handle.get_obj()));
    } else if (OB_FAIL(persister.persist_aggregated_meta(tablet_macro_info, new_handle, space_usage))) {
      LOG_WARN("fail to persist aggregated meta", K(ret), KPC(new_handle.get_obj()), K(space_usage));
    }
  }
  if (OB_SUCC(ret)) {
    new_handle.get_obj()->tablet_meta_.space_usage_ = space_usage;
  }
  return ret;
}

int ObTabletPersister::check_tablet_meta_ids(
    const ObIArray<blocksstable::MacroBlockId> &shared_meta_id_arr,
    const ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  ObSArray<MacroBlockId> meta_ids;
  if (OB_FAIL(tablet.get_tablet_first_second_level_meta_ids(meta_ids))) {
    LOG_WARN("fail to get tablet meta ids", K(ret), K(tablet));
  } else if (OB_UNLIKELY(meta_ids.count() > shared_meta_id_arr.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num of macro blocks doesn't match", K(ret), K(meta_ids.count()), K(shared_meta_id_arr));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_ids.count(); i++) {
      for (int64_t j = 0; !found && j < shared_meta_id_arr.count(); j++) {
        if (meta_ids.at(i) == shared_meta_id_arr.at(j)) {
          found = true;
        }
      }
      if (OB_UNLIKELY(!found)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet meta macro block doesn't match", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletPersister::acquire_tablet(
    const ObTabletPoolType &type,
    const ObTabletMapKey &key,
    const bool try_smaller_pool,
    ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_FAIL(t3m->acquire_tablet_from_pool(type, WashTabletPriority::WTP_HIGH, key, new_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
    } else if (ObTabletPoolType::TP_LARGE == type
        && try_smaller_pool
        && OB_SUCC(t3m->acquire_tablet_from_pool(ObTabletPoolType::TP_NORMAL, WashTabletPriority::WTP_HIGH, key, new_handle))) {
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to acquire tablet from pool", K(ret), K(key), K(type));
    }
  }

  if (OB_SUCC(ret) && OB_ISNULL(new_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new tablet is null", K(ret), K(new_handle));
  }
  return ret;
}

int ObTabletPersister::persist_aggregated_meta(
    const ObTabletMacroInfo &macro_info,
    ObTabletHandle &new_handle,
    ObTabletSpaceUsage &space_usage)
{
  int ret = OB_SUCCESS;
  ObMacroInfoIterator macro_iter;
  bool inc_success = false;
  ObTablet *new_tablet = nullptr;
  ObTenantStorageMetaService *meta_service = MTL(ObTenantStorageMetaService*);
  ObSharedObjectWriteInfo write_info;
  ObSharedObjectWriteHandle handle;
  ObSharedObjectsWriteCtx write_ctx;
  blocksstable::ObStorageObjectOpt curr_opt;
  int64_t inline_meta_size = 0;
  MacroBlockId macro_id;
  int64_t offset = 0;
  int64_t size = 0;
  if (OB_ISNULL(new_tablet = new_handle.get_obj()) || OB_ISNULL(meta_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_handle), KP(meta_service), KPC(new_tablet));
  } else if (FALSE_IT(inline_meta_size = macro_info.get_serialize_size())) {
  } else if (OB_FAIL(ObTabletPersistCommon::fill_tablet_write_info(param_, allocator_, macro_info, new_tablet, write_info))) {
    LOG_WARN("fail to fill write info", K(ret), KPC(new_tablet));
  } else if (FALSE_IT(write_info.write_callback_ = param_.ddl_finish_callback_)) {
  } else if (OB_FAIL(build_tablet_meta_opt(param_,
                                           new_tablet->get_pointer_handle().get_resource_ptr()->get_addr(),
                                           curr_opt))) {
    LOG_WARN("fail to build tablet meta opt", K(ret), K(param_), KPC(new_tablet), K(curr_opt));
  } else if (OB_FAIL(meta_service->get_shared_object_raw_reader_writer().async_write(write_info, curr_opt, handle))) {
    LOG_WARN("fail to async write", K(ret), K(write_info));
  } else if (OB_FAIL(handle.get_write_ctx(write_ctx))) {
    LOG_WARN("fail to batch get address", K(ret), K(handle));
  } else if (FALSE_IT(cur_macro_seq_++)) {
  } else if (FALSE_IT(new_tablet->set_tablet_addr(write_ctx.addr_))) {
  } else if (OB_FAIL(write_ctx.addr_.get_block_addr(macro_id, offset, size))) {
    LOG_WARN("fail to get block addr", K(ret), K(write_ctx));
  } else if (OB_FAIL(new_tablet->set_macro_info_addr(macro_id, offset + (size - inline_meta_size), inline_meta_size, ObMetaDiskAddr::DiskType::RAW_BLOCK))) {
    LOG_WARN("fail to set macro info addr", K(ret), K(macro_id), K(offset), K(size), K(inline_meta_size));
  } else if (OB_FAIL(macro_iter.init(ObTabletMacroType::MAX, macro_info))) {
    LOG_WARN("fail to init macro info iter", K(ret), K(macro_info));
  } else if (OB_FAIL(inc_ref_with_macro_iter(*new_tablet, macro_iter))) {
    LOG_WARN("fail to increase macro ref cnt", K(ret));
  } else {
    int64_t size = 0;

    if (OB_FAIL(write_ctx.addr_.get_size_for_tablet_space_usage(size))) {
      LOG_WARN("fail to get size for tablet space usage", K(ret), K(write_ctx.addr_));
    } else {
      space_usage.tablet_clustered_meta_size_ += upper_align(size, DIO_READ_ALIGN_SIZE);
      new_tablet->tablet_meta_.space_usage_ = space_usage;
    }
  }
  return ret;
}

int ObTabletPersister::inc_ref_with_macro_iter(ObTablet &tablet, ObMacroInfoIterator &macro_iter)
{
  int ret = OB_SUCCESS;
  bool inc_tablet_macro_ref_success = false;
  bool inc_other_macro_ref_success = false;
  const ObMetaDiskAddr &tablet_addr = tablet.tablet_addr_;

  if (OB_FAIL(ObTablet::inc_addr_ref_cnt(tablet_addr, inc_tablet_macro_ref_success))) {
    LOG_WARN("fail to increase tablet macro ref", K(ret), K(tablet_addr));
  } else if (OB_FAIL(tablet.inc_ref_with_macro_iter(macro_iter, inc_other_macro_ref_success))) {
    LOG_WARN("fail to increase ref cnt from macro iter", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (inc_tablet_macro_ref_success) {
      ObTablet::dec_addr_ref_cnt(tablet_addr);
    }
    if (inc_other_macro_ref_success) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(macro_iter.reuse())) {
        LOG_WARN("fail to reuse macro info iterator", K(tmp_ret));
      } else {
        tablet.dec_ref_with_macro_iter(macro_iter);
      }
    }
  } else {
    tablet.hold_ref_cnt_ = true;
  }
  return ret;
}

int ObTabletPersister::convert_arg_to_tablet(const ObTabletTransformArg &arg, ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_FAIL(tablet.tablet_meta_.assign(arg.tablet_meta_))) {
    LOG_WARN("fail to copy tablet meta", K(ret), K(arg.tablet_meta_));
  // TODO: @baichangmin.bcm after mds_mvs joint debugging completed, delete mds_data_.tablet_status_cache_
  } else if (OB_FAIL(tablet.assign_memtables(arg.memtables_, arg.memtable_count_))) {
    LOG_WARN("fail to assign memtables", K(ret), KP(arg.memtables_), K(arg.memtable_count_));
  } else {
    tablet.table_store_addr_.addr_ = arg.table_store_addr_;
    tablet.storage_schema_addr_.addr_ = arg.storage_schema_addr_;
    tablet.macro_info_addr_.addr_ = arg.tablet_macro_info_addr_;
  }
  return ret;
}

int ObTabletPersister::transform(
    const ObTabletTransformArg &arg,
    char *buf,
    const int64_t len)
{
  int ret = OB_SUCCESS;
  ObMetaObjBufferHeader &buf_header = ObMetaObjBufferHelper::get_buffer_header(buf);
  ObTabletPoolType pool_type = ObTabletPoolType::TP_MAX;
  ObTablet *tiny_tablet = reinterpret_cast<ObTablet *>(buf);
  ObMultiTimeStats::TimeStats *time_stats = nullptr;

  if (len <= sizeof(ObTablet) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(len));
  } else if (OB_FAIL(multi_stats_.acquire_stats("transform", time_stats))) {
    LOG_WARN("fail to acquire time stats", K(ret));
  } else if (OB_FAIL(ObTenantMetaMemMgr::get_tablet_pool_type(buf_header.buf_len_, pool_type))) {
    LOG_WARN("fail to get tablet pool type", K(ret), K(buf_header));
  } else if (OB_FAIL(convert_arg_to_tablet(arg, *tiny_tablet))) {
    LOG_WARN("fail to convert arg to tablet", K(ret), K(arg.tablet_meta_));
  } else {
    // buf related
    int64_t start_pos = sizeof(ObTablet);
    int64_t remain = len - start_pos;
    common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "Transform"));

    LOG_DEBUG("TINY TABLET: tablet", KP(buf), K(start_pos), K(remain));
    // rowkey read info related
    int64_t rowkey_read_info_size = 0;
    if (OB_SUCC(ret) && OB_NOT_NULL(arg.rowkey_read_info_ptr_)) {
      rowkey_read_info_size = arg.rowkey_read_info_ptr_->get_deep_copy_size();
      if (OB_UNLIKELY(remain < rowkey_read_info_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet memory buffer not enough for rowkey read info", K(ret), K(remain), K(rowkey_read_info_size));
      } else if (OB_FAIL(arg.rowkey_read_info_ptr_->deep_copy(
          buf + start_pos, remain, tiny_tablet->rowkey_read_info_))) {
        LOG_WARN("fail to deep copy rowkey read info to tablet", K(ret), KPC(arg.rowkey_read_info_ptr_));
      } else if (OB_ISNULL(tiny_tablet->rowkey_read_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr for rowkey read info deep copy", K(ret));
      } else {
        remain -= rowkey_read_info_size;
        start_pos += rowkey_read_info_size;
      }
      LOG_DEBUG("TINY TABLET: tablet + rowkey_read_info", KP(buf), K(start_pos), K(remain));
    }

    // ddl_kvs_ related
    if (OB_SUCC(ret) && (arg.ddl_kv_count_ > 0)) {
      const int ddl_kvs_size = sizeof(ObITable*) * ObTablet::DDL_KV_ARRAY_SIZE;
      if (OB_UNLIKELY(remain < ddl_kvs_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet memory buffer not enough for ddl kvs", K(ret), K(remain), K(ddl_kvs_size));
      } else {
        tiny_tablet->ddl_kvs_ = reinterpret_cast<ObDDLKV**>(buf + start_pos);
        if (OB_FAIL(tiny_tablet->assign_ddl_kvs(arg.ddl_kvs_, arg.ddl_kv_count_))) {
          LOG_WARN("fail to assign ddl_kvs_", K(ret), KP(arg.ddl_kvs_), K(arg.ddl_kv_count_), KP(buf), K(start_pos));
        } else {
          remain -= ddl_kvs_size;
          start_pos += ddl_kvs_size;
        }
      }
      LOG_DEBUG("TINY TABLET: tablet + ddl_kvs", KP(buf), K(start_pos), K(remain), K(tiny_tablet->ddl_kv_count_));
    }

    // table store related
    ObTabletTableStore *table_store = nullptr;
    void *ptr = nullptr;
    if (OB_SUCC(ret)) {
      time_stats->click("before_load_table_store");
      if (OB_ISNULL(arg.new_table_store_ptr_)) {
        table_store = nullptr;
      } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObTabletTableStore)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate a buffer", K(ret), "sizeof", sizeof(ObTabletTableStore));
      } else if (FALSE_IT(table_store = new (ptr) ObTabletTableStore())) {
      } else if (arg.table_store_addr_.is_none()) {
        if (OB_FAIL(table_store->init(allocator, *tiny_tablet))) {
          LOG_WARN("fail to init table store", K(ret), K(*tiny_tablet));
        } else {
          time_stats->click("init_table_store");
        }
      } else {
        char *table_store_buf = nullptr;
        int64_t table_store_buf_len = arg.new_table_store_ptr_->get_serialize_size(param_.data_version_);
        int64_t pos = 0;
        // The reason for using serialize then deserialize approach here is that arg.new_table_store may
        // have already cached some sstables, which would affect the table store cached sstables. After deserialize,
        // the table store will be reset, and the cached sstables will be cleared.
        if (OB_ISNULL(table_store_buf = static_cast<char *>(allocator.alloc(table_store_buf_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate a buffer", K(ret), K(table_store_buf_len));
        } else if (OB_FAIL(arg.new_table_store_ptr_->serialize(param_.data_version_, table_store_buf, table_store_buf_len, pos))) {
          LOG_WARN("fail to serialize table store", K(ret), KPC(arg.new_table_store_ptr_));
        } else if (FALSE_IT(pos = 0)) {
        } else if (OB_FAIL(table_store->deserialize(allocator, *tiny_tablet, table_store_buf, table_store_buf_len, pos))) {
          LOG_WARN("fail to deserialize table store", K(ret), KPC(arg.new_table_store_ptr_), KPC(tiny_tablet));
        } else {
          time_stats->click("deserialize_table_store");
        }
      }
    }

    int64_t remain_size_before_cache_table_store = 0;
    int64_t table_store_size = 0;
    if (OB_SUCC(ret) && OB_NOT_NULL(table_store)) {
      remain_size_before_cache_table_store = remain;
      table_store_size = table_store->get_deep_copy_size();
      if (OB_LIKELY(remain - table_store_size >= 0)) {
        if (pool_type == ObTabletPoolType::TP_LARGE && OB_FAIL(ObCacheSSTableHelper::batch_cache_sstable_meta(allocator, remain - table_store_size, table_store))) {
          LOG_WARN("fail to batch cache sstable meta", K(ret), K(remain), K(table_store_size));
        } else {
          ObIStorageMetaObj *table_store_obj = nullptr;
          table_store_size = table_store->get_deep_copy_size();
          if (OB_FAIL(table_store->deep_copy(buf + start_pos, remain, table_store_obj))) {
            LOG_WARN("fail to deep copy table store v2", K(ret), K(table_store), K(remain), K(start_pos), K(table_store_size), K(pool_type));
          } else if (OB_ISNULL(table_store_obj)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr for rowkey table store deep copy", K(ret), K(table_store_obj));
          } else {
            time_stats->click("cache_table_store");
            tiny_tablet->table_store_addr_.ptr_ = static_cast<ObTabletTableStore *>(table_store_obj);
            remain -= table_store_size;
            start_pos += table_store_size;
          }
        }
      } else {
        LOG_DEBUG("TINY TABLET: no enough memory for tablet store", K(rowkey_read_info_size), K(remain),
            K(table_store_size));
      }
    }

    // id_array related
    if (OB_SUCC(ret)) {
      LOG_INFO("TINY TABLET: tablet + rowkey_read_info + tablet store + auto_inc_seq", KP(buf), K(start_pos), K(remain));
      ObTabletMacroInfo *tablet_macro_info_obj = nullptr;
      if (OB_ISNULL(arg.tablet_macro_info_ptr_)) {
        // no need to prefetch id_array, since we only need it when recycling tablet
      } else {
        int64_t tablet_macro_info_size = arg.tablet_macro_info_ptr_->get_deep_copy_size();
        if (remain >= tablet_macro_info_size) {
          if (OB_FAIL(arg.tablet_macro_info_ptr_->deep_copy(buf + start_pos, remain, tablet_macro_info_obj))) {
            LOG_WARN("fail to deep copy block id array", K(ret));
          } else {
            time_stats->click("cache_macro_info");
            tiny_tablet->macro_info_addr_.ptr_ = tablet_macro_info_obj;
            remain -= tablet_macro_info_size;
            start_pos += tablet_macro_info_size;
          }
        } else {
          LOG_DEBUG("TINY TABLET: no enough memory for tablet macro info", K(rowkey_read_info_size), K(remain), K(tablet_macro_info_size));
        }
      }
    }

    if (OB_SUCC(ret)) {
      tiny_tablet->table_store_cache_.assign(arg.table_store_cache_);
      tiny_tablet->is_inited_ = true;
      LOG_DEBUG("succeed to transform", "tablet_id", tiny_tablet->tablet_meta_.tablet_id_,
        KPC(tiny_tablet->table_store_addr_.ptr_), K(tiny_tablet->macro_info_addr_),
        "tablet_buf_len", len, K(remain_size_before_cache_table_store), K(table_store_size), KPC(arg.tablet_macro_info_ptr_));
    }
    if (OB_NOT_NULL(table_store)) {
      table_store->~ObTabletTableStore();
    }
  }
  return ret;
}

int ObTabletPersister::ObSSTableWriteOperator::do_write(const ObIArray<ObSharedObjectWriteInfo> &write_infos)
{
  int ret = OB_SUCCESS;
  ObSharedObjectBatchHandle handle;
  ObTenantStorageMetaService *meta_service = MTL(ObTenantStorageMetaService*);
  blocksstable::ObStorageObjectOpt curr_opt;
  common::ObSEArray<ObSharedObjectsWriteCtx, 16> write_ctxs;
  const int64_t ctx_id = share::is_reserve_mode()
                        ? ObCtxIds::MERGE_RESERVE_CTX_ID
                        : ObCtxIds::DEFAULT_CTX_ID;
  written_addrs_.set_attr(lib::ObMemAttr(MTL_ID(), "SSTWriteAddrs", ctx_id));
  write_ctxs.set_attr(lib::ObMemAttr(MTL_ID(), "SSTWriteCtxs", ctx_id));

  if (OB_FAIL(ObSSTableMetaPersistHelper::IWriteOperator::do_write(write_infos))) {
    LOG_WARN("failed to do write", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpecte invalid write operator", K(ret), KPC(this));
  } else if (OB_UNLIKELY(write_infos.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty write infos", K(ret), K(write_infos));
  } else if (OB_FAIL(ObTabletPersistCommon::build_async_write_start_opt(persist_param_, start_macro_seq_, curr_opt))) {
    LOG_WARN("fail to build async write start opt", K(ret), K(persist_param_), K(start_macro_seq_));
  } else if (OB_FAIL(meta_service->get_shared_object_reader_writer().async_batch_write(write_infos, handle, curr_opt/*OUTPUT*/))) {
    LOG_WARN("fail to batch async write", K(ret), K(write_infos));
  } else if (OB_FAIL(ObTabletPersistCommon::sync_cur_macro_seq_from_opt(persist_param_, curr_opt, start_macro_seq_))) {
    LOG_WARN("fail to sync cur macro seq from opt", K(ret), K(persist_param_), K(curr_opt), K(start_macro_seq_));
  } else if (OB_FAIL(handle.batch_get_write_ctx(write_ctxs))) {
    LOG_WARN("fail to batch get addr", K(ret), K(handle));
  } else if (OB_UNLIKELY(write_ctxs.count() != write_infos.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of write ctxs mismatch", K(ret), K(write_ctxs.count()), K(write_infos.count()));
  } else if (OB_FAIL(ObTabletPersistCommon::wait_write_info_callback(write_infos))) {
    LOG_WARN("fail to wait redo callback", K(ret));
  } else if (OB_FAIL(written_addrs_.reserve(write_ctxs.count()))) {
    LOG_WARN("fail to reserve written addrs", K(ret), K(write_ctxs.count()));
  } else {
    MacroBlockId block_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < write_ctxs.count(); ++i) {
      ObSharedObjectsWriteCtx &write_ctx = write_ctxs.at(i);
      block_id.reset();
      if (OB_UNLIKELY(!write_ctx.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid addr", K(ret), K(i), K(write_ctx));
      } else if (OB_FAIL(written_addrs_.push_back(write_ctx.addr_))) {
        LOG_WARN("fail to push sstable addr to array", K(ret), K(i), K(write_ctx));
      } else if (OB_FAIL(out_write_ctxs_.push_back(write_ctx))) {
        LOG_WARN("fail to push write ctxs to array", K(ret), K(i), K(write_ctx));
      } else if (OB_FAIL(write_ctx.addr_.get_macro_block_id(block_id))) {
        LOG_WARN("fail to get macro id from meta disk addr", K(ret), K(write_ctx.addr_), K(block_id));
      } else if (OB_FAIL(out_block_info_set_.shared_meta_block_info_set_.set_refactored(block_id, 0 /*whether to overwrite*/))) {
        if (OB_HASH_EXIST != ret) {
          LOG_WARN("fail to push macro id into set", K(ret), K(i), K(write_ctx));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }

  if (OB_FAIL(ret) && GCTX.is_shared_storage_mode()) {
    // if persist failed, we have to wait the I/O request, record the block_id and delete them
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(handle.batch_get_write_ctx(write_ctxs))) {
      LOG_WARN("fail to batch get addr", K(tmp_ret), K(handle));
    } else if (OB_TMP_FAIL(ObTabletPersistCommon::sync_write_ctx_to_total_ctx_if_failed(write_ctxs, out_write_ctxs_))) {
      LOG_WARN("fail to sync write_ctx to total_ctx", K(tmp_ret), K(write_ctxs), K(out_write_ctxs_));
    }
  }
  return ret;
}

int ObTabletPersister::write_and_fill_args(
    const common::ObIArray<ObSharedObjectWriteInfo> &write_infos,
    ObTabletTransformArg &arg,
    common::ObIArray<ObSharedObjectsWriteCtx> &total_write_ctxs,
    int64_t &total_tablet_meta_size,
    ObBlockInfoSet::TabletMacroSet &meta_block_id_set)
{
  int ret = OB_SUCCESS;
  ObTenantStorageMetaService *meta_service = MTL(ObTenantStorageMetaService*);
  ObSharedObjectReaderWriter &reader_writer = meta_service->get_shared_object_reader_writer();
  ObSharedObjectBatchHandle handle;
  ObMetaDiskAddr* addr[] = { // NOTE: The order must be the same as the batch async write.
    &arg.table_store_addr_,
    &arg.storage_schema_addr_,
  };
  constexpr int64_t total_addr_cnt = sizeof(addr) / sizeof(addr[0]);
  int64_t none_addr_cnt = 0;
  for (int64_t i = 0; i < total_addr_cnt; ++i) {
    if (addr[i]->is_none()) {
      ++none_addr_cnt;
    }
  }

  common::ObSEArray<ObSharedObjectsWriteCtx, sizeof(addr)/sizeof(addr[0])> write_ctxs;
  const int64_t ctx_id = share::is_reserve_mode()
                       ? ObCtxIds::MERGE_RESERVE_CTX_ID
                       : ObCtxIds::DEFAULT_CTX_ID;
  write_ctxs.set_attr(lib::ObMemAttr(MTL_ID(), "WriteCtxs", ctx_id));

  blocksstable::ObStorageObjectOpt curr_opt;
  if (OB_UNLIKELY(total_addr_cnt != write_infos.count() + none_addr_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(total_addr_cnt), "write_info_count", write_infos.count(), K(none_addr_cnt));
  } else if (OB_FAIL(ObTabletPersistCommon::build_async_write_start_opt(param_, cur_macro_seq_, curr_opt))) {
    LOG_WARN("fail to build async write start opt", K(ret), K(param_), K(cur_macro_seq_));
  } else if (OB_FAIL(reader_writer.async_batch_write(write_infos, handle, curr_opt/*OUTPUT*/))) {
    LOG_WARN("fail to batch async write", K(ret));
  } else if (OB_FAIL(ObTabletPersistCommon::sync_cur_macro_seq_from_opt(param_, curr_opt, cur_macro_seq_))) {
    LOG_WARN("fail to sync cur macro seq from opt", K(ret), K(param_), K(curr_opt), K(cur_macro_seq_));
  } else if (OB_FAIL(handle.batch_get_write_ctx(write_ctxs))) {
    LOG_WARN("fail to batch get addr", K(ret), K(handle));
  } else if (OB_FAIL(ObTabletPersistCommon::wait_write_info_callback(write_infos))) {
    LOG_WARN("fail to wait write callback", K(ret));
  } else if (OB_UNLIKELY(write_infos.count() != write_ctxs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write ctx count does not equal to write info count", K(ret),
        "write_info_count", write_infos.count(),
        "write_ctx_count", write_ctxs.count(),
        K(write_ctxs), K(handle));
  } else {
    int64_t pos = 0;
    MacroBlockId block_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < total_addr_cnt; ++i) {
      if (addr[i]->is_none()) {
        // skip none addr
      } else {
        const ObSharedObjectsWriteCtx &write_ctx = write_ctxs.at(pos++);
        if (OB_UNLIKELY(!write_ctx.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected write ctx", K(ret), K(i), K(write_ctx), K(handle));
        } else if (OB_FAIL(total_write_ctxs.push_back(write_ctx))) {
          LOG_WARN("fail to push write ctx to array", K(ret), K(i), K(write_ctx));
        } else if (OB_FAIL(write_ctx.addr_.get_macro_block_id(block_id))) {
          LOG_WARN("failed to get macro block id", K(ret), K(write_ctx));
        } else if (OB_FAIL(meta_block_id_set.set_refactored(block_id, 0 /*whether to overwrite*/))) {
          if (OB_HASH_EXIST != ret) {
            LOG_WARN("fail to push macro id into set", K(ret), K(write_ctx.addr_));
          } else {
            ret = OB_SUCCESS;
          }
        }
        if (OB_SUCC(ret)) {
          *addr[i] = write_ctx.addr_;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t tmp_meta_size = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < total_addr_cnt; i++) {
      if (!addr[i]->is_none()) {
        int64_t size = 0;
        if (OB_FAIL(addr[i]->get_size_for_tablet_space_usage(size))) {
          LOG_WARN("fail to get size for tablet space usage", K(ret), KPC(addr[i]));
        } else {
          tmp_meta_size += size;
        }
      }
    }
    total_tablet_meta_size += upper_align(tmp_meta_size, DIO_READ_ALIGN_SIZE);
  } else if (OB_FAIL(ret) && GCTX.is_shared_storage_mode()) {
    // if persist failed, we have to wait the I/O request, record the block_id and delete them
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(handle.batch_get_write_ctx(write_ctxs))) {
      LOG_WARN("fail to batch get addr", K(tmp_ret), K(handle));
    } else if (OB_TMP_FAIL(ObTabletPersistCommon::sync_write_ctx_to_total_ctx_if_failed(write_ctxs, total_write_ctxs))) {
      LOG_WARN("fail to sync write_ctx to total_ctx", K(tmp_ret), K(write_ctxs), K(total_write_ctxs));
    }
  }

  return ret;
}

int ObTabletPersister::load_table_store(
    common::ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const ObMetaDiskAddr &addr,
    ObTabletTableStore *&table_store)
{
  int ret = OB_SUCCESS;
  void *ptr = nullptr;
  ObTabletTableStore *tmp_store = nullptr;
  ObArenaAllocator io_allocator(common::ObMemAttr(MTL_ID(), "PersisterTmpIO"));
  if (OB_UNLIKELY(!addr.is_block())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("address type isn't disk", K(ret), K(addr));
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObTabletTableStore)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate a buffer", K(ret), "sizeof", sizeof(ObTabletTableStore));
  } else {
    tmp_store = new (ptr) ObTabletTableStore();
    char *io_buf = nullptr;
    int64_t buf_len = -1;
    int64_t io_pos = 0;
    ObSharedObjectReadInfo read_info;
    ObSharedObjectReadHandle io_handle(io_allocator);
    ObMultiTimeStats::TimeStats *time_stats = nullptr;

    read_info.addr_ = addr;
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.ls_epoch_ = 0; /* ls_epoch for share storage */
    read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000;
    if (OB_FAIL(multi_stats_.acquire_stats("load_table_store", time_stats))) {
      LOG_WARN("fail to acquire stats", K(ret));
    } else if (OB_FAIL(ObSharedObjectReaderWriter::async_read(read_info, io_handle))) {
      LOG_WARN("fail to async read", K(ret), K(read_info));
    } else if (OB_FAIL(io_handle.wait())) {
      LOG_WARN("fail to wait io_hanlde", K(ret), K(read_info));
    } else if (FALSE_IT(time_stats->click("read_io"))) {
    } else if (OB_FAIL(io_handle.get_data(io_allocator, io_buf, buf_len))) {
      LOG_WARN("fail to get data", K(ret), K(read_info));
    } else if (OB_FAIL(tmp_store->deserialize(allocator, tablet, io_buf, buf_len, io_pos))) {
      LOG_WARN("fail to deserialize table store", K(ret), K(tablet), KP(io_buf), K(buf_len));
    } else {
      time_stats->click("deserialize_table_store");
      table_store = tmp_store;
      LOG_DEBUG("succeed to load table store", K(ret), K(addr), KPC(table_store), K(tablet));
    }
  }
  if (OB_FAIL(ret)) {
    table_store = nullptr;
    if (OB_NOT_NULL(tmp_store)) {
      // avoid memory leak, like: ObMajorChecksumInfo::column_checksums_
      tmp_store->~ObTabletTableStore();
    }
    if (OB_NOT_NULL(ptr)) {
      // ObArenaAllocator has no effect, but is a safety measure
      allocator.free(ptr);
    }
  }
  return ret;
}

int ObTabletPersister::transform_tablet_memory_footprint(
    const ObTabletPersisterParam &param,
    const ObTablet &old_tablet,
    char *buf,
    const int64_t len)
{
  int ret = OB_SUCCESS;
  ObTabletTransformArg arg;
  ObTabletPersister persister(param, DEFAULT_CTX_ID);
  ObMultiTimeStats::TimeStats *time_stats = nullptr;

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid persister param", K(ret), K(param));
  } else if (OB_UNLIKELY(param.for_ss_persist())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected call", K(ret), K(param));
  } else if (OB_FAIL(persister.multi_stats_.acquire_stats("transform_tablet_memory_footprint", time_stats))) {
    LOG_WARN("fail to acquire stats", K(ret));
  } else if (OB_UNLIKELY(!old_tablet.hold_ref_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old tablet doesn't hold ref cnt", K(ret), K(old_tablet));
  } else if (OB_FAIL(convert_tablet_to_mem_arg(old_tablet, arg))) {
    LOG_WARN("fail to convert tablet to mem arg", K(ret), K(arg), KP(buf), K(len), K(old_tablet));
  } else if (FALSE_IT(time_stats->click("convert_tablet_to_mem_arg"))) {
  } else if (OB_FAIL(persister.transform(arg, buf, len))) {
    LOG_WARN("fail to transform tablet", K(ret), K(arg), KP(buf), K(len), K(old_tablet));
  } else {
    time_stats->click("transform");
    ObTablet *tablet = reinterpret_cast<ObTablet *>(buf);
    tablet->set_tablet_addr(old_tablet.get_tablet_addr());
    tablet->hold_ref_cnt_ = old_tablet.hold_ref_cnt_;
    persister.print_time_stats(*time_stats, 20_ms, 1_s);
  }
  return ret;
}

int ObTabletPersister::fetch_table_store_and_write_info(
    const ObTablet &tablet,
    ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
    common::ObIArray<ObSharedObjectWriteInfo> &write_infos,
    common::ObIArray<ObSharedObjectsWriteCtx> &sstable_meta_write_ctxs,
    ObTabletTableStore *new_table_store,
    int64_t &total_tablet_meta_size,
    ObBlockInfoSet &block_info_set)
{
  int ret = OB_SUCCESS;
  const int64_t ctx_id = share::is_reserve_mode()
                       ? ObCtxIds::MERGE_RESERVE_CTX_ID
                       : ObCtxIds::DEFAULT_CTX_ID;
#ifdef ERRSIM
  const int64_t large_co_sstable_threshold_config = GCONF.errsim_large_co_sstable_threshold;
  const int64_t large_co_sstable_threshold = 0 == large_co_sstable_threshold_config ? ObSSTableMetaPersistHelper::SSTABLE_MAX_SERIALIZE_SIZE : large_co_sstable_threshold_config;
#else
  const int64_t large_co_sstable_threshold = ObSSTableMetaPersistHelper::SSTABLE_MAX_SERIALIZE_SIZE;
#endif
  ObArenaAllocator tmp_allocator("PersistSSTable", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ctx_id);
  ObSSTableMetaPersistCtx sstable_persist_ctx(block_info_set, sstable_meta_write_ctxs, tmp_allocator);

  ObMultiTimeStats::TimeStats *time_stats = nullptr;
  const ObTabletTableStore *table_store = nullptr;
  int64_t sstable_meta_size_aligned = 0; // aligned by 4K
  ObTableStoreIterator table_iter;

  if (OB_ISNULL(new_table_store)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new table store is null", K(ret), KP(new_table_store));
  } else if (OB_FAIL(multi_stats_.acquire_stats("fetch_table_store_and_write_info", time_stats))) {
    LOG_WARN("fail to acquire time stats", K(ret));
  } else if (OB_FAIL(tablet.fetch_table_store(wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(wrapper.get_member(table_store))) {
    LOG_WARN("fail to get table store from wrapper", K(ret), K(wrapper));
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null table store", K(ret), K(wrapper), KP(table_store));
  } else if (FALSE_IT(time_stats->click("fetch_table_store"))) {
  } else if (OB_FAIL(sstable_persist_ctx.init(ctx_id, ObTablet::SHARED_MACRO_BUCKET_CNT))) {
    LOG_WARN("fail to init sstable_persist_ctx", K(ret), K(ctx_id), K(sstable_persist_ctx));
  }

  ObSSTableWriteOperator sst_write_op(param_, sstable_meta_write_ctxs, block_info_set, cur_macro_seq_);
  ObSSTableMetaPersistHelper sst_persist_helper(param_, sstable_persist_ctx, large_co_sstable_threshold, cur_macro_seq_);

  if (FAILEDx(sst_persist_helper.register_write_op(&sst_write_op))) {
    LOG_WARN("fail to register sstable write op", K(ret), K(sst_write_op), K(sst_persist_helper));
  } else if (OB_FAIL(sst_persist_helper.do_persist_all_sstables(*table_store,
                                                                allocator_,
                                                                multi_stats_,
                                                                *new_table_store,
                                                                sstable_meta_size_aligned))) {
    LOG_WARN("fail to persist all sstable from table store", K(ret), KPC(table_store), KPC(new_table_store),
      K(sstable_meta_size_aligned), K(sst_persist_helper));
  } else if (FALSE_IT(time_stats->click("fetch_and_persist_sstable"))) {
  } else if (OB_FAIL(fill_write_info_with_data_version(allocator_, param_.data_version_, new_table_store, write_infos))) {
    LOG_WARN("fail to fill table store write info", K(ret), KPC(new_table_store));
  } else {
    time_stats->click("fill_write_info");
    total_tablet_meta_size += sstable_meta_size_aligned;
    total_tablet_meta_size += sstable_persist_ctx.total_tablet_meta_size_;
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
