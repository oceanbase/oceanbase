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

#include "storage/tablet/ob_tablet_persister.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"

using namespace std::placeholders;
using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

ObTabletTransformArg::ObTabletTransformArg()
  : auto_inc_seq_ptr_(nullptr),
    rowkey_read_info_ptr_(nullptr),
    tablet_meta_(),
    table_store_addr_(),
    storage_schema_addr_(),
    tablet_status_uncommitted_kv_addr_(),
    tablet_status_committed_kv_addr_(),
    aux_tablet_info_uncommitted_kv_addr_(),
    aux_tablet_info_committed_kv_addr_(),
    extra_medium_info_(),
    medium_info_list_addr_(),
    auto_inc_seq_addr_(),
    tablet_status_cache_(),
    aux_tablet_info_cache_()
{
}

ObTabletTransformArg::~ObTabletTransformArg()
{
  reset();
}

void ObTabletTransformArg::reset()
{
  auto_inc_seq_ptr_ = nullptr;
  rowkey_read_info_ptr_ = nullptr;
  tablet_meta_.reset();
  table_store_addr_.reset();
  storage_schema_addr_.reset();
  tablet_status_uncommitted_kv_addr_.reset();
  tablet_status_committed_kv_addr_.reset();
  aux_tablet_info_uncommitted_kv_addr_.reset();
  aux_tablet_info_committed_kv_addr_.reset();
  extra_medium_info_.reset();
  medium_info_list_addr_.reset();
  auto_inc_seq_addr_.reset();
  tablet_status_cache_.reset();
  aux_tablet_info_cache_.reset();
}

bool ObTabletTransformArg::is_valid() const
{
  return nullptr != auto_inc_seq_ptr_
      && table_store_addr_.is_none() ^ (nullptr != rowkey_read_info_ptr_)
      && tablet_meta_.is_valid()
      && table_store_addr_.is_valid()
      && storage_schema_addr_.is_valid()
      && tablet_status_uncommitted_kv_addr_.is_valid()
      && tablet_status_committed_kv_addr_.is_valid()
      && aux_tablet_info_uncommitted_kv_addr_.is_valid()
      && aux_tablet_info_committed_kv_addr_.is_valid()
      && auto_inc_seq_addr_.is_valid()
      && medium_info_list_addr_.is_valid();
}

int ObTabletPersister::persist_and_transform_tablet(
    const ObTablet &old_tablet,
    ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator;
  common::ObSEArray<ObSharedBlocksWriteCtx, 16> tablet_meta_write_ctxs;
  common::ObSEArray<ObSharedBlocksWriteCtx, 16> sstable_meta_write_ctxs;

  if (OB_UNLIKELY(!old_tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid old tablet to persist", K(ret), K(old_tablet));
  } else if (OB_FAIL(recursively_persist(
      old_tablet, allocator, tablet_meta_write_ctxs, sstable_meta_write_ctxs, new_handle))) {
    LOG_WARN("fail to recursively persist and fill tablet", K(ret), K(old_tablet));
  } else if (OB_FAIL(check_tablet_meta_ids(tablet_meta_write_ctxs, *(new_handle.get_obj())))) {
    LOG_WARN("fail to check whether tablet meta's macro ids match", K(ret), K(tablet_meta_write_ctxs), KPC(new_handle.get_obj()));
  } else if (OB_FAIL(persist_4k_tablet(allocator, new_handle))) {
    LOG_WARN("fail to persist 4k tablet", K(ret), K(new_handle), KPC(new_handle.get_obj()));
  } else {
    FLOG_INFO("succeed to persist 4k tablet", K(&old_tablet), K(new_handle.get_obj()));
  }
  return ret;
}

int ObTabletPersister::recursively_persist(
    const ObTablet &old_tablet,
    common::ObArenaAllocator &allocator,
    common::ObIArray<ObSharedBlocksWriteCtx> &tablet_meta_write_ctxs,
    common::ObIArray<ObSharedBlocksWriteCtx> &sstable_meta_write_ctxs,
    ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(persist_and_fill_tablet(
      old_tablet, allocator, tablet_meta_write_ctxs, sstable_meta_write_ctxs, new_handle))) {
    LOG_WARN("fail to persist and fill tablet", K(ret), K(old_tablet));
  } else if (old_tablet.get_tablet_meta().has_next_tablet_) {
    ObTabletHandle new_next_handle;
    const ObTablet &old_next_tablet = *(old_tablet.get_next_tablet_guard().get_obj());
    if (OB_FAIL(recursively_persist(
        old_next_tablet, allocator, tablet_meta_write_ctxs, sstable_meta_write_ctxs, new_next_handle))) {
      LOG_WARN("fail to recursively persist and fill next tablet",
          K(ret), K(old_next_tablet), K(tablet_meta_write_ctxs), K(sstable_meta_write_ctxs));
    } else {
      new_handle.get_obj()->set_next_tablet_guard(new_next_handle);
    }
  }
  return ret;
}

int ObTabletPersister::convert_tablet_to_mem_arg(
      const ObTablet &tablet,
      ObTabletMemberWrapper<share::ObTabletAutoincSeq> &auto_inc_seq,
      ObTabletTransformArg &arg)
{
  int ret = OB_SUCCESS;
  arg.reset();
  if (OB_UNLIKELY(!tablet.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("old tablet isn't valid, don't allow to degrade tablet memory", K(ret), K(tablet));
  } else if (OB_FAIL(arg.tablet_status_cache_.assign(tablet.mds_data_.tablet_status_cache_))) {
    LOG_WARN("fail to assign tablet status cache", K(ret), K(tablet));
  } else if (OB_FAIL(arg.aux_tablet_info_cache_.assign(tablet.mds_data_.aux_tablet_info_cache_))) {
    LOG_WARN("fail to assign aux tablet info cache", K(ret), K(tablet));
  } else if (OB_FAIL(arg.tablet_meta_.assign(tablet.tablet_meta_))) {
    LOG_WARN("fail to assign tablet meta", K(ret), K(tablet));
  } else if (OB_FAIL(tablet.fetch_autoinc_seq(auto_inc_seq))) {
    LOG_WARN("fail to fetch autoinc seq", K(ret), K(tablet));
  } else {
    arg.auto_inc_seq_ptr_ = auto_inc_seq.get_member();
    arg.rowkey_read_info_ptr_ = tablet.rowkey_read_info_;
    arg.table_store_addr_ = tablet.table_store_addr_.addr_;
    arg.storage_schema_addr_ = tablet.storage_schema_addr_.addr_;
    arg.tablet_status_uncommitted_kv_addr_ = tablet.mds_data_.tablet_status_.uncommitted_kv_.addr_;
    arg.tablet_status_committed_kv_addr_ = tablet.mds_data_.tablet_status_.committed_kv_.addr_;
    arg.aux_tablet_info_uncommitted_kv_addr_ = tablet.mds_data_.aux_tablet_info_.uncommitted_kv_.addr_;
    arg.aux_tablet_info_committed_kv_addr_ = tablet.mds_data_.aux_tablet_info_.committed_kv_.addr_;
    arg.extra_medium_info_ = tablet.mds_data_.extra_medium_info_;
    arg.medium_info_list_addr_ = tablet.mds_data_.medium_info_list_.addr_;
    arg.auto_inc_seq_addr_ = tablet.mds_data_.auto_inc_seq_.addr_;
  }
  return ret;
}
int ObTabletPersister::convert_tablet_to_disk_arg(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      common::ObIArray<ObSharedBlocksWriteCtx> &tablet_meta_write_ctxs,
      common::ObIArray<ObSharedBlocksWriteCtx> &sstable_meta_write_ctxs,
      ObTabletPoolType &type,
      ObTabletMemberWrapper<share::ObTabletAutoincSeq> &auto_inc_seq,
      ObTabletTransformArg &arg)
{
  int ret = OB_SUCCESS;
  arg.reset();

  common::ObSEArray<ObSharedBlockWriteInfo, 8> write_infos;
  // fetch member wrapper
  ObTabletMemberWrapper<ObTabletTableStore> table_store;
  // fetch member function
  FetchTableStore fetch_table_store =
      std::bind(&ObTablet::fetch_table_store, &tablet, _1);
  FetchAutoincSeq fetch_auto_inc_seq =
      std::bind(&ObTabletMdsData::fetch_auto_inc_seq, std::cref(tablet.mds_data_.auto_inc_seq_), _1);

  // load member
  const ObStorageSchema *storage_schema = nullptr;
  // load member function
  LoadStorageSchema load_storage_schema =
     std::bind(&ObTablet::load_storage_schema, &tablet, _1, _2);

  // load new mds data
  const ObTabletComplexAddr<mds::MdsDumpKV> &uncommitted_tablet_status_addr = tablet.mds_data_.tablet_status_.uncommitted_kv_;
  const ObTabletComplexAddr<mds::MdsDumpKV> &committed_tablet_status_addr = tablet.mds_data_.tablet_status_.committed_kv_;
  const ObTabletComplexAddr<mds::MdsDumpKV> &uncommitted_aux_tablet_info_addr = tablet.mds_data_.aux_tablet_info_.uncommitted_kv_;
  const ObTabletComplexAddr<mds::MdsDumpKV> &committed_aux_tablet_info_addr = tablet.mds_data_.aux_tablet_info_.committed_kv_;
  const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &medium_info_list_addr = tablet.mds_data_.medium_info_list_;

  if (OB_FAIL(arg.tablet_status_cache_.assign(tablet.mds_data_.tablet_status_cache_))) {
    LOG_WARN("fail to assign tablet status cache", K(ret), K(tablet));
  } else if (OB_FAIL(arg.aux_tablet_info_cache_.assign(tablet.mds_data_.aux_tablet_info_cache_))) {
    LOG_WARN("fail to assign aux tablet info cache", K(ret), K(tablet));
  } else if (OB_FAIL(arg.tablet_meta_.assign(tablet.tablet_meta_))) {
    LOG_WARN("fail to assign tablet meta", K(ret), K(tablet));
  } else if (FALSE_IT(arg.rowkey_read_info_ptr_ = tablet.rowkey_read_info_)) {
  } else if (FALSE_IT(arg.extra_medium_info_ = tablet.mds_data_.extra_medium_info_)) {
  } else if (OB_FAIL(fetch_wrapper_and_write_info(allocator, fetch_table_store, table_store, write_infos, sstable_meta_write_ctxs))) {
    LOG_WARN("fail to fetch table store wrapper and write info", K(ret));
  } else if (OB_FAIL(fetch_wrapper_and_write_info(
        allocator, fetch_auto_inc_seq, auto_inc_seq, write_infos, tablet_meta_write_ctxs))) {
    LOG_WARN("fail to fetch auto inc seq wrapper and write info", K(ret));
  } else if (FALSE_IT(arg.auto_inc_seq_ptr_ = auto_inc_seq.get_member())) {
  } else if (OB_FAIL(load_member_and_write_info(
        allocator, load_storage_schema, storage_schema, write_infos))) {
    LOG_WARN("fail to load storage schema and write info", K(ret));
  } else if (OB_FAIL(load_dump_kv_and_fill_write_info(allocator, uncommitted_tablet_status_addr, write_infos))) {
    LOG_WARN("fail to load tablet status uncommitted kv", K(ret), K(uncommitted_tablet_status_addr));
  } else if (OB_FAIL(load_dump_kv_and_fill_write_info(allocator, committed_tablet_status_addr, write_infos))) {
    LOG_WARN("fail to load tablet status committed kv", K(ret), K(committed_tablet_status_addr));
  } else if (OB_FAIL(load_dump_kv_and_fill_write_info(allocator, uncommitted_aux_tablet_info_addr, write_infos))) {
    LOG_WARN("fail to load aux tablet info uncommitted kv", K(ret), K(uncommitted_aux_tablet_info_addr));
  } else if (OB_FAIL(load_dump_kv_and_fill_write_info(allocator, committed_aux_tablet_info_addr, write_infos))) {
    LOG_WARN("fail to load aux tablet info committed kv", K(ret), K(committed_aux_tablet_info_addr));
  } else if (OB_FAIL(write_and_fill_args(write_infos, arg, tablet_meta_write_ctxs))) {
    LOG_WARN("fail to write and fill address", K(ret), K(write_infos), K(auto_inc_seq));
  } else if (OB_FAIL(load_medium_info_list_and_write(allocator, medium_info_list_addr, arg.medium_info_list_addr_, tablet_meta_write_ctxs))) {
    LOG_WARN("fail to load medium info list and write", K(ret), K(medium_info_list_addr));
  } else {
    const int64_t try_cache_size = sizeof(ObTablet)
                                 + tablet.rowkey_read_info_->get_deep_copy_size()
                                 + table_store.get_member()->get_deep_copy_size();
    if (try_cache_size > ObTenantMetaMemMgr::NORMAL_TABLET_POOL_SIZE) {
      type = ObTabletPoolType::TP_LARGE;
    }
  }
  ObTablet::free_storage_schema(allocator, storage_schema);

  return ret;
}

int ObTabletPersister::persist_and_fill_tablet(
    const ObTablet &old_tablet,
    common::ObArenaAllocator &allocator,
    common::ObIArray<ObSharedBlocksWriteCtx> &tablet_meta_write_ctxs,
    common::ObIArray<ObSharedBlocksWriteCtx> &sstable_meta_write_ctxs,
    ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObSharedBlockWriteInfo, 8> write_infos;
  ObTabletTransformArg arg;

  const ObTabletMeta &tablet_meta = old_tablet.get_tablet_meta();
  const ObTabletMapKey key(tablet_meta.ls_id_, tablet_meta.tablet_id_);
  ObTabletPoolType type = ObTabletPoolType::TP_NORMAL;
  ObTabletMemberWrapper<share::ObTabletAutoincSeq> auto_inc_seq;
  bool try_smaller_pool = true;

  if (old_tablet.is_empty_shell()) {
    if (OB_FAIL(convert_tablet_to_mem_arg(old_tablet, auto_inc_seq, arg))) {
      LOG_WARN("fail to conver tablet to mem arg", K(ret), K(old_tablet));
    }
  } else if (OB_FAIL(convert_tablet_to_disk_arg(
      allocator, old_tablet, tablet_meta_write_ctxs, sstable_meta_write_ctxs, type, auto_inc_seq, arg))) {
    LOG_WARN("fail to conver tablet to disk arg", K(ret), K(old_tablet));
  } else if (sizeof(old_tablet) + old_tablet.rowkey_read_info_->get_deep_copy_size() > ObTenantMetaMemMgr::NORMAL_TABLET_POOL_SIZE) {
    try_smaller_pool = false;
  }

  if (FAILEDx(acquire_tablet(type, key, try_smaller_pool, new_handle))) {
    LOG_WARN("fail to acquire tablet", K(ret), K(key), K(type));
  } else if (OB_FAIL(transform(arg, new_handle.get_buf(), new_handle.get_buf_len()))) {
    LOG_WARN("fail to transform old tablet", K(ret), K(arg), K(new_handle), K(type));
  }

  return ret;
}

int ObTabletPersister::check_tablet_meta_ids(
    const common::ObIArray<ObSharedBlocksWriteCtx> &tablet_meta_write_ctxs,
    const ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  ObSArray<MacroBlockId> meta_ids;
  ObSArray<MacroBlockId> ctx_ids;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_meta_write_ctxs.count(); i++) {
    if (OB_FAIL(ObTablet::parse_meta_addr(tablet_meta_write_ctxs.at(i).addr_, ctx_ids))) {
      LOG_WARN("fail to parse meta addr", K(ret), K(tablet_meta_write_ctxs.at(i).addr_));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(tablet.get_tablet_meta_ids(meta_ids))) {
    LOG_WARN("fail to get tablet meta ids", K(ret), K(tablet));
  } else if (meta_ids.count() != ctx_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet meta's macro ids don't match", K(ret), K(meta_ids.count()), K(ctx_ids.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_ids.count(); i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < meta_ids.count(); j++) {
        if (meta_ids.at(j) == ctx_ids.at(i)) {
          if (OB_FAIL(meta_ids.remove(j))) {
            LOG_WARN("fail to remove id from array", K(ret), K(ctx_ids.at(i)));
          } else {
            break;
          }
        }
        if (OB_SUCC(ret) && j == meta_ids.count() - 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet meta's macro ids don't match", K(ret), K(ctx_ids.at(i)));
        }
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

int ObTabletPersister::persist_4k_tablet(common::ObArenaAllocator &allocator, ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;
  ObTablet *new_tablet = new_handle.get_obj();
  ObTenantCheckpointSlogHandler *ckpt_slog_hanlder = MTL(ObTenantCheckpointSlogHandler*);
  common::ObSEArray<ObSharedBlockWriteInfo, 1> write_infos;
  ObSharedBlockWriteHandle handle;
  ObSharedBlocksWriteCtx write_ctx;
  if (OB_FAIL(fill_write_info(allocator, new_tablet, write_infos))) {
    LOG_WARN("fail to fill write info", K(ret), KPC(new_tablet));
  } else if (OB_ISNULL(ckpt_slog_hanlder)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, ckpt slog handler is nullptr", K(ret), KP(ckpt_slog_hanlder));
  } else if (OB_FAIL(ckpt_slog_hanlder->get_shared_block_reader_writer().async_write(write_infos.at(0), handle))) {
    LOG_WARN("fail to async write", K(ret), K(write_infos));
  } else if (OB_FAIL(handle.get_write_ctx(write_ctx))) {
    LOG_WARN("fail to batch get address", K(ret), K(handle));
  } else if (FALSE_IT(new_tablet->set_tablet_addr(write_ctx.addr_))) {
  } else if (OB_FAIL(new_tablet->inc_macro_ref_cnt())) {
    LOG_WARN("fail to increase macro ref cnt for new tablet", K(ret), KPC(new_tablet));
  }
  return ret;
}

int ObTabletPersister::convert_arg_to_tablet(
    const ObTabletTransformArg &arg,
    ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_FAIL(tablet.tablet_meta_.assign(arg.tablet_meta_))) {
    LOG_WARN("fail to assign tablet meta", K(ret), K(arg.tablet_meta_));
  } else if (OB_FAIL(tablet.pull_memtables())) {
    LOG_WARN("fail to build memtables", K(ret), K(tablet));
  } else if (OB_FAIL(tablet.mds_data_.tablet_status_cache_.assign(arg.tablet_status_cache_))) {
    LOG_WARN("fail to assign tablet status cache", K(ret), K(arg.aux_tablet_info_cache_));
  } else if (OB_FAIL(tablet.mds_data_.aux_tablet_info_cache_.assign(arg.aux_tablet_info_cache_))) {
    LOG_WARN("fail to assign aux tablet info cache", K(ret), K(arg.aux_tablet_info_cache_));
  } else {
    tablet.table_store_addr_.addr_ = arg.table_store_addr_;
    tablet.storage_schema_addr_.addr_ = arg.storage_schema_addr_;
    tablet.mds_data_.tablet_status_.uncommitted_kv_.addr_ = arg.tablet_status_uncommitted_kv_addr_;
    tablet.mds_data_.tablet_status_.committed_kv_.addr_ = arg.tablet_status_committed_kv_addr_;
    tablet.mds_data_.aux_tablet_info_.uncommitted_kv_.addr_ = arg.aux_tablet_info_uncommitted_kv_addr_;
    tablet.mds_data_.aux_tablet_info_.committed_kv_.addr_ = arg.aux_tablet_info_committed_kv_addr_;
    tablet.mds_data_.extra_medium_info_.info_ = arg.extra_medium_info_.info_;
    tablet.mds_data_.extra_medium_info_.last_medium_scn_ = arg.extra_medium_info_.last_medium_scn_;
    tablet.mds_data_.medium_info_list_.addr_ = arg.medium_info_list_addr_;
    tablet.mds_data_.auto_inc_seq_.addr_ = arg.auto_inc_seq_addr_;
    tablet.mds_data_.is_inited_ = true;
  }
  return ret;
}

int ObTabletPersister::transform(
    const ObTabletTransformArg &arg,
    char *buf,
    const int64_t len)
{
  int ret = OB_SUCCESS;
  ObTablet *tiny_tablet = reinterpret_cast<ObTablet *>(buf);
  if (len <= sizeof(ObTablet) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(len));
  } else if (OB_FAIL(convert_arg_to_tablet(arg, *tiny_tablet))) {
    LOG_WARN("fail to convert arg to tablet", K(ret), K(arg.tablet_meta_));
  } else {
    // buf related
    int64_t start_pos = sizeof(ObTablet);
    int64_t remain = len - start_pos;
    common::ObArenaAllocator allocator("Transform");

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

    // table store related
    ObTabletTableStore *table_store = nullptr;
    if (OB_SUCC(ret)) {
      if (arg.table_store_addr_.is_none()) {
        void *ptr = nullptr;
        if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObTabletTableStore)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate a buffer", K(ret), "sizeof", sizeof(ObTabletTableStore));
        } else {
          table_store = new (ptr) ObTabletTableStore();
          if (OB_FAIL(table_store->init(allocator, *tiny_tablet))) {
            LOG_WARN("fail to init table store", K(ret), K(*tiny_tablet));
          }
        }
      } else if (OB_FAIL(load_table_store(allocator, *tiny_tablet, arg.table_store_addr_, table_store))) {
        LOG_WARN("fail to load table store", K(ret), KPC(tiny_tablet), K(arg.table_store_addr_));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t table_store_size = table_store->get_deep_copy_size();
      if (OB_LIKELY((remain - table_store_size) >= 0)) {
        if (OB_FAIL(table_store->batch_cache_sstable_meta(allocator, remain - table_store_size))) {
          LOG_WARN("fail to batch cache sstable meta", K(ret), K(remain), K(table_store_size));
        } else {
          ObIStorageMetaObj *table_store_obj = nullptr;
          table_store_size = table_store->get_deep_copy_size();
          if (OB_FAIL(table_store->deep_copy(buf + start_pos, remain, table_store_obj))) {
            LOG_WARN("fail to deep copy table store v2", K(ret), K(table_store));
          } else if (OB_ISNULL(table_store_obj)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr for rowkey table store deep copy", K(ret), K(table_store_obj));
          } else {
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

    // auto_inc_seq related
    if (OB_SUCC(ret)) {
      LOG_DEBUG("TINY TABLET: tablet + rowkey_read_info + tablet store", KP(buf), K(start_pos), K(remain));
      ObIStorageMetaObj *auto_inc_obj = nullptr;
      const int auto_inc_seq_size = arg.auto_inc_seq_ptr_->get_deep_copy_size();
      if (OB_LIKELY((remain - auto_inc_seq_size) > 0)) {
        if(OB_FAIL(arg.auto_inc_seq_ptr_->deep_copy(buf + start_pos, remain, auto_inc_obj))) {
          LOG_WARN("fail to deep copy auto inc seq", K(ret), K(arg.auto_inc_seq_ptr_));
        } else {
          tiny_tablet->mds_data_.auto_inc_seq_.ptr_ = static_cast<share::ObTabletAutoincSeq *>(auto_inc_obj);
          remain -= auto_inc_seq_size;
          start_pos += auto_inc_seq_size;
        }
      } else {
        LOG_DEBUG("TINY TABLET: no enough memory for auto inc seq", K(rowkey_read_info_size), K(remain),
            K(auto_inc_seq_size));
      }
    }
    if (OB_SUCC(ret)) {
      tiny_tablet->is_inited_ = true;
    }
  }
  return ret;
}

int ObTabletPersister::fetch_and_persist_sstable(
    common::ObArenaAllocator &allocator,
    ObTableStoreIterator &table_iter,
    ObTabletTableStore &new_table_store,
    common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObITable *, 16> tables;
  common::ObSEArray<ObMetaDiskAddr, 16> addrs;
  common::ObSEArray<ObSharedBlocksWriteCtx, 16> write_ctxs;
  common::ObSEArray<ObSharedBlockWriteInfo, 16> write_infos;
  ObSharedBlockBatchHandle handle;
  ObITable *table = nullptr;
  while (OB_SUCC(ret) && OB_SUCC(table_iter.get_next(table))) {
    if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, table is nullptr", K(ret), KPC(table));
    } else {
      ObMetaDiskAddr addr;
      ObSSTable *sstable = nullptr;
      ObArenaAllocator tmp_allocator("PersistSSTable");
      // The sstable by cache in table store, the address is also valid. But, here we hope that all
      // members of the sstable are serialized. So, we deep copy the sstable and set mem address.
      addr.set_mem_addr(0, sizeof(ObSSTable));
      if (OB_FAIL(static_cast<ObSSTable *>(table)->deep_copy(tmp_allocator, sstable))) {
        LOG_WARN("fail to deep copy sstable", K(ret), KPC(table));
      } else if (OB_FAIL(sstable->set_addr(addr))) {
        LOG_WARN("fail to set sstable address", K(ret), K(addr));
      } else if (OB_FAIL(fill_write_info(allocator, sstable, write_infos))) {
        LOG_WARN("fail to fill sstable write info", K(ret), KPC(table));
      } else if (OB_FAIL(tables.push_back(table))) {
        LOG_WARN("fail to push back sstable address", K(ret), K(tables));
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret) && write_infos.count() > 0) {
    ObTenantCheckpointSlogHandler *ckpt_slog_hanlder = MTL(ObTenantCheckpointSlogHandler*);
    if (OB_ISNULL(ckpt_slog_hanlder)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, ckpt slog handler is nullptr", K(ret), KP(ckpt_slog_hanlder));
    } else if (OB_FAIL(ckpt_slog_hanlder->get_shared_block_reader_writer().async_batch_write(write_infos, handle))) {
      LOG_WARN("fail to batch async write", K(ret), K(write_infos));
    } else if (OB_FAIL(handle.batch_get_write_ctx(write_ctxs))) {
      LOG_WARN("fail to batch get addr", K(ret), K(handle));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < write_ctxs.count(); ++i) {
        if (OB_UNLIKELY(!write_ctxs.at(i).is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected invalid addr", K(ret), K(i), K(write_ctxs.at(i)));
        } else if (OB_FAIL(addrs.push_back(write_ctxs.at(i).addr_))) {
          LOG_WARN("fail to push sstable addr to array", K(ret), K(i), K(write_ctxs.at(i)));
        } else if (OB_FAIL(meta_write_ctxs.push_back(write_ctxs.at(i)))) {
          LOG_WARN("fail to push write ctxs to array", K(ret), K(i), K(write_ctxs.at(i)));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(new_table_store.init(allocator, tables, addrs))) {
    LOG_WARN("fail to init new table store", K(ret), K(tables), K(addrs));
  }
  return ret;
}

int ObTabletPersister::write_and_fill_args(
    const common::ObIArray<ObSharedBlockWriteInfo> &write_infos,
    ObTabletTransformArg &arg,
    common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs)
{
  int ret = OB_SUCCESS;
  ObTenantCheckpointSlogHandler *ckpt_slog_hanlder = MTL(ObTenantCheckpointSlogHandler*);
  ObSharedBlockReaderWriter &reader_writer = ckpt_slog_hanlder->get_shared_block_reader_writer();
  ObSharedBlockBatchHandle handle;
  ObMetaDiskAddr* addr[] = { // NOTE: The order must be the same as the batch async write.
    &arg.table_store_addr_,
    &arg.auto_inc_seq_addr_,
    &arg.storage_schema_addr_,
    &arg.tablet_status_uncommitted_kv_addr_,
    &arg.tablet_status_committed_kv_addr_,
    &arg.aux_tablet_info_uncommitted_kv_addr_,
    &arg.aux_tablet_info_committed_kv_addr_,
  };
  common::ObSEArray<ObSharedBlocksWriteCtx, sizeof(addr)/sizeof(addr[0])> write_ctxs;
  if (OB_UNLIKELY(sizeof(addr)/sizeof(addr[0]) != write_infos.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), "write info count", write_infos.count(), K(write_infos));
  } else if (OB_ISNULL(ckpt_slog_hanlder)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, ckpt slog handler is nullptr", K(ret), KP(ckpt_slog_hanlder));
  } else if (OB_FAIL(ckpt_slog_hanlder->get_shared_block_reader_writer().async_batch_write(write_infos, handle))) {
    LOG_WARN("fail to batch async write", K(ret), K(write_infos));
  } else if (OB_FAIL(handle.batch_get_write_ctx(write_ctxs))) {
    LOG_WARN("fail to batch get addr", K(ret), K(handle));
  } else if (OB_UNLIKELY(sizeof(addr)/sizeof(addr[0]) != write_ctxs.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), "write ctx count", write_ctxs.count(), K(write_ctxs), K(handle));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < write_ctxs.count(); ++i) {
      if (OB_UNLIKELY(!write_ctxs.at(i).is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid addr", K(ret), K(i), K(write_ctxs.at(i)), K(handle));
      } else if (OB_FAIL(meta_write_ctxs.push_back(write_ctxs.at(i)))) {
        LOG_WARN("fail to push write ctx to array", K(ret), K(i), K(write_ctxs.at(i)));
      } else {
        *addr[i] = write_ctxs.at(i).addr_;
      }
    }
  }

  return ret;
}

int ObTabletPersister::load_dump_kv_and_fill_write_info(
    common::ObArenaAllocator &allocator,
    const ObTabletComplexAddr<mds::MdsDumpKV> &complex_addr,
    common::ObIArray<ObSharedBlockWriteInfo> &write_infos)
{
  int ret = OB_SUCCESS;
  const mds::MdsDumpKV *kv = nullptr;

  if (complex_addr.addr_.is_none()) {
    // do nothing
  } else if (OB_FAIL(ObTabletMdsData::load_mds_dump_kv(allocator, complex_addr, kv))) {
    LOG_WARN("fail to load mds dump kv", K(ret), K(complex_addr));
  } else if (OB_ISNULL(kv)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, kv is null", K(ret), KP(kv));
  } else if (OB_FAIL(fill_write_info(allocator, kv, write_infos))) {
    LOG_WARN("fail to fill write info", K(ret), KPC(kv));
  }

  ObTabletMdsData::free_mds_dump_kv(allocator, kv);

  return ret;
}

int ObTabletPersister::load_medium_info_list_and_write(
    common::ObArenaAllocator &allocator,
    const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &complex_addr,
    ObMetaDiskAddr &addr,
    common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs)
{
  int ret = OB_SUCCESS;
  const ObTabletDumpedMediumInfo *medium_info_list = nullptr;

  if (OB_FAIL(ObTabletMdsData::load_medium_info_list(allocator, complex_addr, medium_info_list))) {
    LOG_WARN("fail to load medium info list", K(ret), K(complex_addr));
  } else if (OB_ISNULL(medium_info_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, medium info list is null", K(ret), KP(medium_info_list));
  } else if (OB_FAIL(link_write_medium_info_list(*medium_info_list, addr, meta_write_ctxs))) {
    LOG_WARN("failed to link write medium info list", K(ret));
  }

  ObTabletMdsData::free_medium_info_list(allocator, medium_info_list);

  return ret;
}

int ObTabletPersister::link_write_medium_info_list(
    const ObTabletDumpedMediumInfo &medium_info_list,
    ObMetaDiskAddr &addr,
    common::ObIArray<ObSharedBlocksWriteCtx> &meta_write_ctxs)
{
  int ret = OB_SUCCESS;
  ObTenantCheckpointSlogHandler *ckpt_slog_hanlder = MTL(ObTenantCheckpointSlogHandler*);
  ObSharedBlockReaderWriter &reader_writer = ckpt_slog_hanlder->get_shared_block_reader_writer();
  common::ObArenaAllocator arena_allocator("serializer");
  ObSharedBlockWriteInfo write_info;
  ObSharedBlockLinkHandle write_handle;

  const common::ObIArray<compaction::ObMediumCompactionInfo*> &array = medium_info_list.medium_info_list_;
  for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); ++i) {
    const compaction::ObMediumCompactionInfo *medium_info = array.at(i);
    if (OB_ISNULL(medium_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, medium info is null", K(ret), K(i), KP(medium_info));
    } else {
      const int64_t size = medium_info->get_serialize_size();

      if (0 == size) {
        LOG_INFO("medium info serialize size is 0, just skip", K(ret));
      } else {
        int64_t pos = 0;
        char *buffer = static_cast<char*>(arena_allocator.alloc(size));
        if (OB_ISNULL(buffer)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret), K(size));
        } else if (OB_FAIL(medium_info->serialize(buffer, size, pos))) {
          LOG_WARN("failed to serialize medium info", K(ret));
        } else {
          write_info.reset();
          write_info.buffer_ = buffer;
          write_info.offset_ = 0;
          write_info.size_ = size;
          write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
          if (OB_FAIL(reader_writer.async_link_write(write_info, write_handle))) {
            LOG_WARN("failed to do async link write", K(ret), K(write_info));
          } else if (OB_UNLIKELY(!write_handle.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, write handle is invalid", K(ret), K(write_handle));
          }
        }

        if (nullptr != buffer) {
          arena_allocator.free(buffer);
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (array.empty()) {
    addr.set_none_addr();
  } else {
    ObSharedBlocksWriteCtx write_ctx;
    if (OB_FAIL(write_handle.get_write_ctx(write_ctx))) {
      LOG_WARN("failed to get write ctx", K(ret), K(write_handle));
    } else if (OB_FAIL(meta_write_ctxs.push_back(write_ctx))) {
      LOG_WARN("failed to push back write ctx", K(ret), K(write_ctx));
    } else {
      addr = write_ctx.addr_;
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
  ObArenaAllocator io_allocator("PersisterTmpIO");
  if (OB_UNLIKELY(!addr.is_block())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("address type isn't disk", K(ret), K(addr));
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObTabletTableStore)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate a buffer", K(ret), "sizeof", sizeof(ObTabletTableStore));
  } else {
    ObTabletTableStore *tmp_store = new (ptr) ObTabletTableStore();
    char *io_buf = nullptr;
    int64_t buf_len = -1;
    int64_t io_pos = 0;
    ObSharedBlockReadInfo read_info;
    ObSharedBlockReadHandle io_handle;
    read_info.addr_ = addr;
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    if (OB_FAIL(ObSharedBlockReaderWriter::async_read(read_info, io_handle))) {
      LOG_WARN("fail to async read", K(ret), K(read_info));
    } else if (OB_FAIL(io_handle.wait())) {
      LOG_WARN("fail to wait io_hanlde", K(ret), K(read_info));
    } else if (OB_FAIL(io_handle.get_data(io_allocator, io_buf, buf_len))) {
      LOG_WARN("fail to get data", K(ret), K(read_info));
    } else if (OB_FAIL(tmp_store->deserialize(allocator, tablet, io_buf, buf_len, io_pos))) {
      LOG_WARN("fail to deserialize table store", K(ret), K(tablet), KP(io_buf), K(buf_len));
    } else {
      table_store = tmp_store;
      LOG_DEBUG("succeed to load table store", K(ret), K(addr), KPC(table_store), K(tablet));
    }
  }
  return ret;
}

int ObTabletPersister::transform_tablet_memory_footprint(
    const ObTablet &old_tablet,
    char *buf,
    const int64_t len)
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<share::ObTabletAutoincSeq> auto_inc_seq;
  ObTabletTransformArg arg;
  if (OB_UNLIKELY(!old_tablet.hold_ref_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old tablet doesn't hold ref cnt", K(ret), K(old_tablet));
  } else if (OB_FAIL(convert_tablet_to_mem_arg(old_tablet, auto_inc_seq, arg))) {
    LOG_WARN("fail to convert tablet to mem arg", K(ret), K(arg), KP(buf), K(len), K(old_tablet));
  } else if (OB_FAIL(transform(arg, buf, len))) {
    LOG_WARN("fail to transform tablet", K(ret), K(arg), KP(buf), K(len), K(old_tablet));
  } else {
    ObTablet *tablet = reinterpret_cast<ObTablet *>(buf);
    tablet->set_next_tablet_guard(old_tablet.next_tablet_guard_);
    tablet->set_tablet_addr(old_tablet.get_tablet_addr());
    tablet->hold_ref_cnt_ = old_tablet.hold_ref_cnt_;
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
