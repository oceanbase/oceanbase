/**
 * Copyright (c) 2023 OceanBase
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

#include "ob_tenant_storage_meta_service.h"
#include "storage/meta_store/ob_storage_meta_io_util.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"
#include "observer/omt/ob_tenant.h"
#include "storage/ls/ob_ls.h"
#ifdef OB_BUILD_SHARED_STORAGE
  #include "meta_store/ob_shared_storage_obj_meta.h"
  #include "storage/compaction/ob_major_task_checkpoint_mgr.h"
  #include "storage/compaction/ob_tablet_id_obj.h"
#endif

namespace oceanbase
{
using namespace compaction;
namespace storage
{

ObTenantStorageMetaService::ObTenantStorageMetaService()
  : is_inited_(false),
    is_started_(false),
    is_shared_storage_(false),
    ckpt_slog_handler_(),
    slogger_(),
    persister_(),
    replayer_(),
    shared_object_rwriter_(),
    shared_object_raw_rwriter_()
{}

int ObTenantStorageMetaService::mtl_init(ObTenantStorageMetaService *&meta_service)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(meta_service->init())) {
    LOG_WARN("fail to init ObTenantStorageMetaService", K(ret));
  }
  return ret;
}

int ObTenantStorageMetaService::init()
{
  int ret = OB_SUCCESS;
  const bool is_shared_storage = GCTX.is_shared_storage_mode();

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", K(ret));
  } else if (!is_shared_storage &&
      OB_FAIL(slogger_.init(SERVER_STORAGE_META_SERVICE.get_slogger_manager(), MTL_ID()))) {
    LOG_WARN("failed to init slogger", K(ret));
  } else if (!is_shared_storage && OB_FAIL(ckpt_slog_handler_.init(slogger_))) {
    LOG_WARN("fail to init tenant checkpoint slog hander", K(ret));
  } else if (OB_FAIL(persister_.init(is_shared_storage, slogger_, ckpt_slog_handler_))) {
    LOG_WARN("fail to init persister", K(ret));
  } else if (OB_FAIL(replayer_.init(is_shared_storage, persister_, ckpt_slog_handler_))) {
    LOG_WARN("fail to init replayer", K(ret));
  } else if (OB_FAIL(shared_object_rwriter_.init())) {
    LOG_WARN("fail to init shared block rwriter", K(ret));
  } else if (OB_FAIL(shared_object_raw_rwriter_.init())) {
    LOG_WARN("fail to init shared block raw rwriter", K(ret));
  } else {
    is_shared_storage_ = is_shared_storage;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantStorageMetaService::start()
{
  int ret = OB_SUCCESS;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  const ObTenantSuperBlock super_block = tenant->get_super_block();
  uint64_t macro_block_id = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_ && OB_FAIL(slogger_.start())) {
    LOG_WARN("fail to start slogger", K(ret));
  } else if (!is_shared_storage_ && OB_FAIL(ckpt_slog_handler_.start())) {
    LOG_WARN("fail to start tenant checkpoint slog handler", K(ret));
  } else if (OB_FAIL(replayer_.start_replay(super_block))) {
    LOG_WARN("fail to start replayer", K(ret));
  } else if (OB_FAIL(seq_generator_.init(is_shared_storage_, persister_))) {
    LOG_WARN("fail to seq generator", K(ret));
  } else if (OB_FAIL(seq_generator_.start())) {
    LOG_WARN("fail to seq generator", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (is_shared_storage_ && OB_FAIL(seq_generator_.get_private_object_seq(macro_block_id))) {
    LOG_WARN("failed to get_private_object_seq", KR(ret));
  } else if (is_shared_storage_) {
    // for macro check in observer start
    MTL(checkpoint::ObTabletGCService*)->set_mtl_start_max_block_id(macro_block_id);
#endif
  }
  if (OB_SUCC(ret)) {
    is_started_ = true;
  }
  FLOG_INFO("finish start ObTenantStorageMetaService", K(ret));
  return ret;
}

void ObTenantStorageMetaService::stop()
{
  if (IS_INIT) {
    if (!is_shared_storage_) {
      slogger_.stop();
      ckpt_slog_handler_.stop();
    }
    seq_generator_.stop();
  }
}

void ObTenantStorageMetaService::wait()
{
  if (IS_INIT) {
    if (!is_shared_storage_) {
      slogger_.wait();
      ckpt_slog_handler_.wait();
    }
    seq_generator_.stop();
  }
}

void ObTenantStorageMetaService::destroy()
{
  slogger_.destroy();
  ckpt_slog_handler_.destroy();
  persister_.destroy();
  replayer_.destroy();
  seq_generator_.destroy();
  shared_object_rwriter_.reset();
  shared_object_raw_rwriter_.reset();
  is_shared_storage_ = false;
  is_started_ = false;
  is_inited_ = false;
}

int ObTenantStorageMetaService::get_active_cursor(common::ObLogCursor &log_cursor)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(slogger_.get_active_cursor(log_cursor))) {
    LOG_WARN("fail to get active cursor", K(ret));
  }
  return ret;
}

int ObTenantStorageMetaService::get_meta_block_list(
    ObIArray<blocksstable::MacroBlockId> &meta_block_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_shared_storage_) {
    if (OB_FAIL(ckpt_slog_handler_.get_meta_block_list(meta_block_list))) {
      LOG_WARN("fail to get meta block list", K(ret));
    }
  }
  return ret;
}

int ObTenantStorageMetaService::write_checkpoint(bool is_force)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_shared_storage_) {
    // do nothing
  } else if (OB_FAIL(ckpt_slog_handler_.write_checkpoint(is_force))) {
    LOG_WARN("fail to write checkpoint", K(ret));
  }
  return ret;
}

int ObTenantStorageMetaService::add_snapshot(const ObTenantSnapshotMeta &tenant_snapshot)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_shared_storage_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support for shared-storage", K(ret));
  } else if (OB_FAIL(ckpt_slog_handler_.add_snapshot(tenant_snapshot))) {
    LOG_WARN("fail to get meta block list", K(ret));
  }
  return ret;
}

int ObTenantStorageMetaService::delete_snapshot(const share::ObTenantSnapshotID &snapshot_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_shared_storage_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support for shared-storage", K(ret));
  } else if (OB_FAIL(ckpt_slog_handler_.delete_snapshot(snapshot_id))) {
    LOG_WARN("fail to get meta block list", K(ret));
  }
  return ret;
}

int ObTenantStorageMetaService::swap_snapshot(const ObTenantSnapshotMeta &tenant_snapshot)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_shared_storage_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support for shared-storage", K(ret));
  } else if (OB_FAIL(ckpt_slog_handler_.swap_snapshot(tenant_snapshot))) {
    LOG_WARN("fail to get meta block list", K(ret));
  }
  return ret;
}

int ObTenantStorageMetaService::clone_ls(
    observer::ObStartupAccelTaskHandler* startup_accel_handler,
    const blocksstable::MacroBlockId &tablet_meta_entry)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_shared_storage_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support for shared-storage", K(ret));
  } else if (OB_FAIL(ckpt_slog_handler_.clone_ls(startup_accel_handler, tablet_meta_entry))) {
    LOG_WARN("fail to get meta block list", K(ret));
  }
  return ret;
}

int ObTenantStorageMetaService::read_from_disk(
    const ObMetaDiskAddr &addr,
    const int64_t ls_epoch,
    common::ObArenaAllocator &allocator,
    char *&buf,
    int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  char *read_buf = nullptr;
  const int64_t read_buf_len = addr.size();
  if (ObMetaDiskAddr::DiskType::FILE == addr.type()) {
    if (OB_UNLIKELY(is_shared_storage_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shared-storage not support DiskType::FILE", K(ret));
    } else if (OB_FAIL(ckpt_slog_handler_.read_empty_shell_file(addr, allocator, buf, buf_len))) {
      LOG_WARN("fail to read empty shell", K(ret), K(addr), K(buf), K(buf_len));
    }
  } else {
    if (OB_FAIL(read_from_share_blk(addr, ls_epoch, allocator, buf, buf_len))) {
      LOG_WARN("fail to read from share block", K(ret), K(addr), K(ls_epoch), K(buf), K(buf_len));
    }
  }
  return ret;
}

int ObTenantStorageMetaService::read_from_share_blk(
    const ObMetaDiskAddr &addr,
    const int64_t ls_epoch,
    common::ObArenaAllocator &allocator,
    char *&buf,
    int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  ObSharedObjectReadHandle read_handle(allocator);
  ObSharedObjectReadInfo read_info;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000;
  read_info.addr_ = addr;
  read_info.ls_epoch_ = ls_epoch; /* ls_epoch for share storage */
  if (OB_FAIL(ObSharedObjectReaderWriter::async_read(read_info, read_handle))) {
    LOG_WARN("fail to read tablet from macro block", K(ret), K(read_info));
  } else if (OB_FAIL(read_handle.wait())) {
    LOG_WARN("fail to wait for read handle", K(ret));
  } else if (OB_FAIL(read_handle.get_data(allocator, buf, buf_len))) {
    LOG_WARN("fail to get data from read handle", K(ret), KP(buf), K(buf_len));
  }
  return ret;
}

int ObTenantStorageMetaService::ObLSItemIterator::get_next_ls_item(
      storage::ObLSItem &item)
{
  int ret = OB_SUCCESS;
  if (idx_ == tenant_super_block_.ls_cnt_) {
    ret = OB_ITER_END;
  } else {
    item = tenant_super_block_.ls_item_arr_[idx_++];
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE

int ObTenantStorageMetaService::inner_get_blocks_for_tablet_(
    const ObMetaDiskAddr &tablet_addr,
    const int64_t ls_epoch,
    const bool is_shared,
    ObIArray<blocksstable::MacroBlockId> &block_ids/*OUT*/) const
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "GetTabletBLocks"));
  storage::ObTablet tablet;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  // TODO (gaishun.gs): remove debug log tmp arr once get tablet block ids is stable
  const int64_t max_print_id_count = 30;
  ObSEArray<blocksstable::MacroBlockId, max_print_id_count> tmp_print_arr;

  if (OB_FAIL(MTL(ObTenantStorageMetaService*)->read_from_disk(tablet_addr, ls_epoch, allocator, buf, buf_len))) {
    LOG_WARN("fail to read tablet buf from disk", K(ret), K(tablet_addr), K(ls_epoch));
  } else if (FALSE_IT(tablet.set_tablet_addr(tablet_addr))) {
  } else if (OB_FAIL(tablet.deserialize_for_replay(allocator, buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize tablet", K(ret), KP(buf), K(buf_len));
  } else if (!tablet.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("talbet is invalid", K(ret), K(tablet), K(tablet_addr));
  } else {
    bool in_memory = true;
    ObTabletMacroInfo *macro_info = nullptr;
    storage::ObMacroInfoIterator macro_iter;
    storage::ObTabletBlockInfo block_info;

    if (OB_FAIL(tablet.load_macro_info(ls_epoch, allocator, macro_info, in_memory))) {
      LOG_WARN("fail to load macro info", K(ret));
    } else if (OB_FAIL(macro_iter.init(ObTabletMacroType::MAX, *macro_info))) {
      LOG_WARN("fail to init macro iterator", K(ret), KPC(macro_info));
    } else {
      while (OB_SUCC(ret)) {
        block_info.reset();
        if (OB_FAIL(macro_iter.get_next(block_info))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next block info", KR(ret), K(block_info));
          } else {
            ret = OB_SUCCESS;
            if (0 != tmp_print_arr.count()) {
#ifndef OB_BUILD_PACKAGE
              FLOG_INFO("iter get blocks", K(ret), K(ls_epoch), K(tablet_addr), K(is_shared), K(tmp_print_arr));
#endif
              tmp_print_arr.reuse();
            }
            break;
          }
        } else if (!block_info.macro_id_.is_private_data_or_meta() &&
                   !block_info.macro_id_.is_shared_data_or_meta()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected storage_object_type detected", K(ret), K(block_info));
        } else if (!is_shared &&
            block_info.macro_id_.is_private_data_or_meta() &&
            OB_FAIL(block_ids.push_back(block_info.macro_id_))) {
          LOG_WARN("fail to push private macro id", K(ret), K(block_info));
        } else if (is_shared &&
            block_info.macro_id_.is_shared_data_or_meta() &&
            OB_FAIL(block_ids.push_back(block_info.macro_id_))) {
          LOG_WARN("fail to push shared macro id", K(ret), K(block_info));
        } else if (OB_FAIL(tmp_print_arr.push_back(block_info.macro_id_))) {
          LOG_WARN("fail to push shared macro id", K(ret), K(block_info));
        } else if (max_print_id_count == tmp_print_arr.count()) {
#ifndef OB_BUILD_PACKAGE
          FLOG_INFO("iter get blocks", K(ret), K(ls_epoch), K(tablet_addr), K(is_shared), K(tmp_print_arr));
#endif
          tmp_print_arr.reuse();
        }
      }
      if (OB_NOT_NULL(macro_info) && !in_memory) {
        macro_info->reset();
      }
    }
  }
  return ret;
}

int ObTenantStorageMetaService::get_private_blocks_for_tablet(
    const share::ObLSID &ls_id,
    const int64_t ls_epoch,
    const ObTabletID &tablet_id,
    const int64_t tablet_version,
    const int64_t tablet_transfer_seq,
    ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  ObMetaDiskAddr tablet_addr;
  blocksstable::MacroBlockId object_id;
  blocksstable::ObStorageObjectOpt opt;
  const int64_t object_size = OB_DEFAULT_MACRO_BLOCK_SIZE;

  opt.set_ss_private_tablet_meta_object_opt(ls_id.id(), tablet_id.id(), tablet_version, tablet_transfer_seq);
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, object_id))) {
    LOG_WARN("fail to get object id", K(ret), K(opt));
  } else if (OB_FAIL(tablet_addr.set_block_addr(object_id, 0/*offset*/, object_size, ObMetaDiskAddr::DiskType::RAW_BLOCK))) {
    LOG_WARN("fail to set initial tablet meta addr", K(ret), K(tablet_addr));
  } else if (OB_FAIL(inner_get_blocks_for_tablet_(tablet_addr, ls_epoch, false/*is_shared*/, block_ids))) {
    LOG_WARN("fail to deserialize tablet", K(ret), K(tablet_addr), K(ls_epoch), K(block_ids));
  }
  return ret;
}
int ObTenantStorageMetaService::get_shared_blocks_for_tablet(
    const ObTabletID &tablet_id,
    const int64_t tablet_version,
    ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  ObMetaDiskAddr addr;
  blocksstable::MacroBlockId object_id;
  blocksstable::ObStorageObjectOpt opt;
  const int64_t object_size = OB_DEFAULT_MACRO_BLOCK_SIZE;

  opt.set_ss_share_tablet_meta_object_opt(tablet_id.id(), tablet_version);
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, object_id))) {
    LOG_WARN("fail to get object id", K(ret), K(opt));
  } else if (OB_FAIL(addr.set_block_addr(object_id, 0/*offset*/, object_size, ObMetaDiskAddr::DiskType::RAW_BLOCK))) {
    LOG_WARN("fail to set initial tablet meta addr", K(ret), K(addr));
  } else if (OB_FAIL(inner_get_blocks_for_tablet_(addr, 0 /*ls_epoch*/, true/*is_shared*/, block_ids))) {
    LOG_WARN("fail to deserialize tablet", K(ret), K(addr), K(block_ids));
  }
  return ret;
}

int ObTenantStorageMetaService::get_next_major_shared_blocks_for_tablet(
      const ObTabletID &tablet_id,
      const int64_t last_tablet_version,
      ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "OldTabletMeta"));
  int64_t root_macro_seq = 0;
  if (OB_UNLIKELY(!tablet_id.is_valid() || last_tablet_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(last_tablet_version));
  } else {
    ObTabletIDObj tablet_id_obj(tablet_id);
    ObCompactionObjBuffer obj_buf;
    if (OB_FAIL(obj_buf.init())) {
      LOG_WARN("failed to init obj", KR(ret), K(tablet_id), K(obj_buf));
    } else if (OB_FAIL(tablet_id_obj.read_object(obj_buf))) {
      if (OB_OBJECT_NOT_EXIST == ret) {
        LOG_DEBUG("tablet id have not output macro on shared storage", KR(ret), K(tablet_id), K(last_tablet_version));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to read object", KR(ret), K(tablet_id));
      }
    } else if (OB_UNLIKELY(tablet_id_obj.get_compaction_scn() <= last_tablet_version)) {
      LOG_INFO("not output macro after last tablet version, no need to gc", KR(ret), K(last_tablet_version), K(tablet_id_obj));
    } else {
      ObSimpleTaskCheckpointMgr mgr;
      if (OB_FAIL(mgr.get_all_macro_start_seq(
          tablet_id,
          tablet_id_obj.get_parallel_cnt(),
          tablet_id_obj.get_cg_dir_cnt(),
          tablet_id_obj.get_last_major_root_macro_seq(),
          block_ids))) {
        LOG_WARN("failed to get all macro seq", KR(ret), K(root_macro_seq));
      } else {
        LOG_INFO("success to get macro seq", KR(ret), K(root_macro_seq), K(block_ids));
      }
    }
  }
  return ret;
}

int ObTenantStorageMetaService::inner_get_gc_tablet_scn_arr_(
    const blocksstable::ObStorageObjectOpt &opt,
    ObGCTabletMetaInfoList &gc_tablet_scn_arr) const
{
  int ret = OB_SUCCESS;
  gc_tablet_scn_arr.tablet_version_arr_.reuse();
  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "InnerGetGCScn"));
  bool is_exist = false;

  if (OB_FAIL(ObStorageMetaIOUtil::check_meta_existence(opt, 0/*do not need ls_epoch*/, is_exist))) {
    LOG_WARN("fail to check existence", K(ret), K(opt));
  } else if (!is_exist) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_TRACE("entry not exist", K(ret), K(opt));
  } else if (OB_FAIL(ObStorageMetaIOUtil::read_storage_meta_object(
      opt, allocator, MTL_ID(), 0/*do not need ls_epoch*/, gc_tablet_scn_arr))) {
    LOG_WARN("failed to get object: gc_tablet_sc_arr", K(ret), K(opt), K(MTL_ID()), K(gc_tablet_scn_arr));
  }
  return ret;
}
int ObTenantStorageMetaService::get_gc_tablet_scn_arr(
    const ObTabletID &tablet_id,
    const blocksstable::ObStorageObjectType obj_type,
    ObGCTabletMetaInfoList &tablet_scn_arr)
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageObjectOpt opt;
  if ((blocksstable::ObStorageObjectType::SHARED_MAJOR_GC_INFO != obj_type && blocksstable::ObStorageObjectType::SHARED_MAJOR_META_LIST != obj_type) ||
        !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(obj_type));
  } else if (blocksstable::ObStorageObjectType::SHARED_MAJOR_GC_INFO == obj_type) {
    opt.set_ss_gc_info_object_opt(tablet_id.id());
  } else if (blocksstable::ObStorageObjectType::SHARED_MAJOR_META_LIST == obj_type) {
    opt.set_ss_meta_list_object_opt(tablet_id.id());
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(inner_get_gc_tablet_scn_arr_(opt, tablet_scn_arr))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get gc_tablet_scn_arr", K(ret), K(opt), K(tablet_scn_arr));
    }
  }
  return ret;
}
int ObTenantStorageMetaService::write_gc_tablet_scn_arr(
    const ObTabletID &tablet_id,
    const blocksstable::ObStorageObjectType obj_type,
    const ObGCTabletMetaInfoList &tablet_scn_arr)
{
  int ret = OB_SUCCESS;
  if ((blocksstable::ObStorageObjectType::SHARED_MAJOR_GC_INFO != obj_type && blocksstable::ObStorageObjectType::SHARED_MAJOR_META_LIST != obj_type) ||
      !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(obj_type));
  } else if (blocksstable::ObStorageObjectType::SHARED_MAJOR_GC_INFO == obj_type &&
      OB_FAIL(ss_write_gc_info_(tablet_id, tablet_scn_arr))) {
    LOG_WARN("failed to write gc_info", K(ret), K(tablet_id), K(tablet_scn_arr));
  } else if (blocksstable::ObStorageObjectType::SHARED_MAJOR_META_LIST == obj_type &&
      OB_FAIL(ss_write_meta_list_(tablet_id, tablet_scn_arr))) {
    LOG_WARN("failed to write meta_list", K(ret), K(tablet_id), K(tablet_scn_arr));
  }
  return ret;
}

int ObTenantStorageMetaService::force_write_gc_tablet_scn_arr_(
    const ObTabletID &tablet_id,
    const blocksstable::ObStorageObjectType obj_type,
    const ObGCTabletMetaInfoList &tablet_scn_arr)
{
  int ret = OB_SUCCESS;
  if ((blocksstable::ObStorageObjectType::SHARED_MAJOR_GC_INFO != obj_type && blocksstable::ObStorageObjectType::SHARED_MAJOR_META_LIST != obj_type) ||
      !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(obj_type));
  } else {
    do {
      if (OB_FAIL(write_gc_tablet_scn_arr(tablet_id, obj_type, tablet_scn_arr))) {
        LOG_WARN("failed to write_gc_tablet_scn_arr", K(ret), K(tablet_id), K(obj_type), K(tablet_scn_arr));
        usleep(100 * 1000);
      }
    } while (OB_FAIL(ret));
  }
  return ret;
}

int ObTenantStorageMetaService::update_shared_tablet_meta_list(
    const ObTabletID &tablet_id,
    const int64_t new_tablet_meta_version)
{
  int ret = OB_SUCCESS;
  ObGCTabletMetaInfoList new_tablet_meta_version_list;
  ObGCTabletMetaInfoList old_tablet_meta_version_list;
  ObGCTabletMetaInfoList tablet_meta_gc_version;
  SCN new_tablet_meta_scn;
  SCN gc_scn;
  const int64_t new_tablet_meta_create_ts = ObTimeUtility::fast_current_time();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(new_tablet_meta_scn.convert_for_inner_table_field(new_tablet_meta_version))) {
    LOG_WARN("failed to convert_for_inner_table_field", K(ret), K(tablet_id), K(new_tablet_meta_version));
  } else if (!tablet_id.is_valid() || !new_tablet_meta_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(new_tablet_meta_version));
  } else if (OB_FAIL(get_gc_tablet_scn_arr(tablet_id, blocksstable::ObStorageObjectType::SHARED_MAJOR_META_LIST, old_tablet_meta_version_list))) {
    LOG_WARN("failed to get tablet_meta_versions", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_gc_tablet_scn_arr(tablet_id, blocksstable::ObStorageObjectType::SHARED_MAJOR_GC_INFO, tablet_meta_gc_version))) {
    LOG_WARN("failed to get gc info", K(ret), K(tablet_id));
  } else if (tablet_meta_gc_version.tablet_version_arr_.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gc info is invalid", K(ret), K(tablet_meta_gc_version.tablet_version_arr_.count()));
  } else if (0 == tablet_meta_gc_version.tablet_version_arr_.count()) {
    gc_scn.set_min();
  } else if (FALSE_IT(gc_scn = tablet_meta_gc_version.tablet_version_arr_.at(0).scn_)) {
  } else if (!gc_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gc scn is invalid", K(ret));
  } else if (gc_scn >= new_tablet_meta_scn) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_meta_version has been gc", K(ret), K(gc_scn), K(new_tablet_meta_scn));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < old_tablet_meta_version_list.tablet_version_arr_.count(); i++) {
    const SCN tablet_meta_scn = old_tablet_meta_version_list.tablet_version_arr_.at(i).scn_;
    const int64_t tablet_meta_create_ts = old_tablet_meta_version_list.tablet_version_arr_.at(i).tablet_meta_create_ts_;
    if (!tablet_meta_scn.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet meta scn is invalid", K(ret), K(i), K(new_tablet_meta_version_list));
    } else if (gc_scn < tablet_meta_scn) {
      if (tablet_meta_scn >= new_tablet_meta_scn) {
        ret = OB_ENTRY_EXIST;
        LOG_INFO("new tablet_meta_version is not more than old tablet_meta_version", K(ret), K(new_tablet_meta_scn), K(tablet_meta_scn));
      } else if (OB_FAIL(new_tablet_meta_version_list.tablet_version_arr_.push_back(ObGCTabletMetaInfo(tablet_meta_scn, tablet_meta_create_ts)))) {
        LOG_WARN("failed to push back", K(ret), K(i), K(new_tablet_meta_version_list));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(new_tablet_meta_version_list.tablet_version_arr_.push_back(ObGCTabletMetaInfo(new_tablet_meta_scn, new_tablet_meta_create_ts)))) {
    LOG_WARN("failed to push back", K(ret), K(new_tablet_meta_version_list), K(new_tablet_meta_scn));
  } else if (OB_FAIL(force_write_gc_tablet_scn_arr_(tablet_id, blocksstable::ObStorageObjectType::SHARED_MAJOR_META_LIST, new_tablet_meta_version_list))) {
    LOG_WARN("failed to write_gc_tablet_scn_arr", K(ret), K(new_tablet_meta_scn), K(new_tablet_meta_version_list));
  }
  FLOG_INFO("finish update_shared_tablet_meta_list", K(ret), K(tablet_id), K(new_tablet_meta_scn), K(new_tablet_meta_version_list));
  return ret;
}
int ObTenantStorageMetaService::ss_write_gc_info_(
    const ObTabletID tablet_id, const ObGCTabletMetaInfoList &gc_info_scn_arr)
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageObjectOpt opt;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "WriteGCSCN"));
  opt.set_ss_gc_info_object_opt(tablet_id.id());
  if (OB_FAIL(ObStorageMetaIOUtil::write_storage_meta_object(
      opt, gc_info_scn_arr, allocator, MTL_ID(), 0/*ls_epoch, unused*/))) {
    LOG_WARN("fail to write gc tablet scn arr", K(ret), K(opt), K(gc_info_scn_arr));
  }
  return ret;
}
int ObTenantStorageMetaService::ss_write_meta_list_(
    const ObTabletID tablet_id, const ObGCTabletMetaInfoList &meta_list_scn_arr)
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageObjectOpt opt;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "WriteMetaList"));
  opt.set_ss_meta_list_object_opt(tablet_id.id());
  if (OB_FAIL(ObStorageMetaIOUtil::write_storage_meta_object(
      opt, meta_list_scn_arr, allocator, MTL_ID(), 0/*ls_epoch, unused*/))) {
    LOG_WARN("fail to write gc tablet scn arr", K(ret), K(opt), K(meta_list_scn_arr));
  }
  return ret;
}
int ObTenantStorageMetaService::ss_is_meta_list_exist(const ObTabletID tablet_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageObjectOpt opt;
  opt.set_ss_meta_list_object_opt(tablet_id.id());
  if (OB_FAIL(ObStorageMetaIOUtil::check_meta_existence(opt, 0/*do not need ls_epoch*/, is_exist))) {
    LOG_WARN("fail to check existence", K(ret), K(opt));
  }
  return ret;
}

#endif
int ObTenantStorageMetaService::get_ls_items_by_status(
    const storage::ObLSItemStatus status,
    ObIArray<storage::ObLSItem> &ls_items)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_shared_storage_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only available for shared-storage");
  } else {
    ls_items.reuse();
    omt::ObTenant *tenant = static_cast<omt::ObTenant*>(MTL_CTX());
    HEAP_VAR(ObLSItemIterator, ls_item_iter, tenant->get_super_block()) {
      ObLSItem ls_item;
      while (OB_SUCC(ls_item_iter.get_next_ls_item(ls_item))) {
        if (status == ls_item.status_ &&
            OB_FAIL(ls_items.push_back(ls_item))) {
          LOG_WARN("failed to push back tenant_item", K(ret), K(ls_item), K(ls_items), K(ls_item_iter));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get tenant items by status", K(ret), K(ls_item), K(ls_items), K(ls_item_iter));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
