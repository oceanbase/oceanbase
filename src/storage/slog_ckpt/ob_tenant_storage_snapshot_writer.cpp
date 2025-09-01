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

#define USING_LOG_PREFIX STORAGE

#include "storage/slog_ckpt/ob_tenant_storage_snapshot_writer.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tablet/ob_tablet_mds_table_mini_merger.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "observer/omt/ob_tenant.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObTenantStorageSnapshotWriter::ObTenantStorageSnapshotWriter()
  : is_inited_(false),
    meta_type_(ObTenantStorageMetaType::INVALID_TYPE),
    ckpt_slog_handler_(nullptr),
    tablet_item_addr_info_arr_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("TabletCkptArr", MTL_ID())),
    ls_item_writer_(),
    tablet_item_writer_(),
    wait_gc_tablet_item_writer_()
{
}

int ObTenantStorageSnapshotWriter::init(
    const ObTenantStorageMetaType meta_type,
    ObTenantCheckpointSlogHandler *ckpt_slog_handler)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(MTL_ID(), ObModIds::OB_CHECKPOINT);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantStorageSnapshotWriter init twice", K(ret));
  } else if (OB_UNLIKELY(ObTenantStorageMetaType::INVALID_TYPE == meta_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(meta_type));
  } else {
    meta_type_ = meta_type;
    ckpt_slog_handler_ = ckpt_slog_handler;
    is_inited_ = true;
  }
  return ret;
}

void ObTenantStorageSnapshotWriter::reset()
{
  is_inited_ = false;
  tablet_item_addr_info_arr_.reset();
  ls_item_writer_.reset();
  tablet_item_writer_.reset();
  wait_gc_tablet_item_writer_.reset();
  meta_type_ = ObTenantStorageMetaType::INVALID_TYPE;
  ckpt_slog_handler_ = nullptr;
}

int ObTenantStorageSnapshotWriter::record_single_ls_meta(
    const MacroBlockId &orig_ls_meta_entry,
    const ObLSID &ls_id,
    ObIArray<blocksstable::MacroBlockId> &orig_linked_block_list,
    blocksstable::MacroBlockId &ls_meta_entry,
    share::SCN &clog_max_scn)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(MTL_ID(), ObModIds::OB_CHECKPOINT);
  ObLSHandle ls_handle;
  ObTenantStorageCheckpointReader ls_ckpt_reader;
  ObTenantStorageCheckpointReader::ObStorageMetaOp copy_ls_meta_op = std::bind(
      &ObTenantStorageSnapshotWriter::copy_ls_meta_for_creating,
      this,
      std::placeholders::_1,
      std::placeholders::_2,
      std::placeholders::_3);

  if (OB_UNLIKELY(!orig_ls_meta_entry.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(orig_ls_meta_entry), K(ls_id));
  } else if (OB_FAIL(ls_item_writer_.init_for_slog_ckpt(MTL_ID(), MTL_EPOCH_ID(), mem_attr, nullptr/*fd_dispenser*/))) {
    LOG_WARN("failed to init log stream item writer", K(ret));
  } else if (OB_FAIL(ls_ckpt_reader.iter_read_meta_item(orig_ls_meta_entry, copy_ls_meta_op, orig_linked_block_list))) {
    LOG_WARN("fail to iter read and write ls snapshot", K(ret), K(orig_ls_meta_entry));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", K(ret));
  } else if (OB_FAIL(do_record_ls_meta(*(ls_handle.get_ls()), clog_max_scn, nullptr/*slog checkpoint fd dispenser*/))) {
    LOG_WARN("fail to record single ls meta", K(ret), K(ls_handle));
  } else if (OB_FAIL(close(ls_meta_entry))) {
    LOG_WARN("fail to close tenant storage checkpoint writer", K(ret));
  }
  return ret;
}

int ObTenantStorageSnapshotWriter::delete_single_ls_meta(
    const MacroBlockId &orig_ls_meta_entry,
    const ObLSID &ls_id,
    ObIArray<blocksstable::MacroBlockId> &orig_linked_block_list,
    blocksstable::MacroBlockId &ls_meta_entry)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(MTL_ID(), ObModIds::OB_CHECKPOINT);
  ObTenantStorageCheckpointReader ls_ckpt_reader;
  ObTenantStorageCheckpointReader::ObStorageMetaOp copy_ls_meta_op = std::bind(
      &ObTenantStorageSnapshotWriter::copy_ls_meta_for_deleting,
      this,
      std::placeholders::_1,
      std::placeholders::_2,
      std::placeholders::_3,
      ls_id);

  if (OB_UNLIKELY(!orig_ls_meta_entry.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(orig_ls_meta_entry), K(ls_id));
  } else if (OB_FAIL(ls_item_writer_.init_for_slog_ckpt(MTL_ID(), MTL_EPOCH_ID(), mem_attr, nullptr/*fd_dispenser*/))) {
    LOG_WARN("failed to init log stream item writer", K(ret));
  } else if (OB_FAIL(ls_ckpt_reader.iter_read_meta_item(orig_ls_meta_entry, copy_ls_meta_op, orig_linked_block_list))) {
    LOG_WARN("fail to iter read and write ls snapshot", K(ret), K(orig_ls_meta_entry));
  } else if (OB_FAIL(close(ls_meta_entry))) {
    LOG_WARN("fail to close tenant storage checkpoint writer", K(ret));
  }
  return ret;
}

int ObTenantStorageSnapshotWriter::record_wait_gc_tablet(
    blocksstable::MacroBlockId &wait_gc_tablet_entry,
    ObSlogCheckpointFdDispenser *fd_dispenser)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(MTL_ID(), ObModIds::OB_CHECKPOINT);
  if (!GCTX.is_shared_storage_mode()) {
    // nothing to do
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (OB_FAIL(wait_gc_tablet_item_writer_.init_for_slog_ckpt(MTL_ID(), MTL_EPOCH_ID(), mem_attr, fd_dispenser))) {
    LOG_WARN("failed to init log stream item writer", K(ret));
  } else {
    omt::ObTenant *tenant = static_cast<omt::ObTenant*>(MTL_CTX());
    HEAP_VAR(ObTenantSuperBlock, tenant_super_block, tenant->get_super_block()) {
      common::ObSArray<ObPendingFreeTabletItem> items;
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_super_block.ls_cnt_; ++i) {
        ObLSPendingFreeTabletArray ls_wait_gc_tablet_array;
        const ObLSItem &ls_item = tenant_super_block.ls_item_arr_[i];
        items.reuse();
        ls_wait_gc_tablet_array.ls_id_ = ls_item.ls_id_;
        ls_wait_gc_tablet_array.ls_epoch_ = ls_item.epoch_;
        if (OB_FAIL(TENANT_STORAGE_META_SERVICE.get_wait_gc_tablet_items(ls_item.ls_id_, ls_item.epoch_, items))) {
          LOG_WARN("fail to get wait gc tablet items", K(ret), K(ls_item));
        } else if (OB_FAIL(ls_wait_gc_tablet_array.items_.assign(items))) {
          LOG_WARN("fail to assign wait gc tablet items", K(ret), K(ls_item), K(items));
        } else {
          const int64_t buf_len = ls_wait_gc_tablet_array.get_serialize_size();
          int64_t pos = 0;
          char *buf = nullptr;
          if (OB_UNLIKELY(!ls_wait_gc_tablet_array.is_valid())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid wait gc tablet array", K(ret), K(ls_wait_gc_tablet_array));
          } else if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(buf_len, mem_attr)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory", K(ret));
          } else if (OB_FAIL(ls_wait_gc_tablet_array.serialize(buf, buf_len, pos))) {
            LOG_WARN("fail to serialize ls wait gc tablet array", K(ret), KP(buf), K(buf_len), K(pos));
          } else if (OB_FAIL(wait_gc_tablet_item_writer_.write_item(buf, buf_len))) {
            LOG_WARN("fail to write ls wait gc tablet array", K(ret), KP(buf), K(buf_len));
          }
          if (OB_NOT_NULL(buf)) {
            ob_free(buf);
          }
        }
      }
      if (FAILEDx(wait_gc_tablet_item_writer_.close())) {
        LOG_WARN("fail to close ls wait gc tablet writer", K(ret));
      } else if (OB_FAIL(wait_gc_tablet_item_writer_.get_entry_block(wait_gc_tablet_entry))) {
        LOG_WARN("fail to get entry block", K(ret));
      }
    } // HEAP_VAR
#endif
  }
  return ret;
}

int ObTenantStorageSnapshotWriter::record_ls_meta(
    MacroBlockId &ls_entry_block,
    ObSlogCheckpointFdDispenser *fd_dispenser)
{
  int ret = OB_SUCCESS;
  common::ObSharedGuard<ObLSIterator> ls_iter;
  ObLS *ls = nullptr;

  ls_item_writer_.reset();
  tablet_item_writer_.reset();
  ObMemAttr mem_attr(MTL_ID(), ObModIds::OB_CHECKPOINT);
  if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream iter", K(ret));
  } else if (OB_FAIL(ls_item_writer_.init_for_slog_ckpt(MTL_ID(), MTL_EPOCH_ID(), mem_attr, fd_dispenser))) {
    LOG_WARN("failed to init log stream item writer", K(ret));
  } else {
    share::SCN unused_scn;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next log stream", K(ret));
        }
      }

      if (OB_SUCC(ret) && OB_FAIL(do_record_ls_meta(*ls, unused_scn, fd_dispenser))) {
        LOG_WARN("fail to do record storage meta", K(ret), KPC(ls));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(close(ls_entry_block))) {
      LOG_WARN("fail to close ls meta writer", K(ret));
    }
  }

  LOG_INFO("write ls checkpoint finish", K(ret), K(ls_entry_block));
  return ret;
}

int ObTenantStorageSnapshotWriter::do_record_ls_meta(
    ObLS &ls,
    share::SCN &clog_max_scn,
    ObSlogCheckpointFdDispenser *fd_dispenser)
{
  int ret = OB_SUCCESS;
  ObLSCkptMember ls_ckpt_member;
  {
    ObLSLockGuard lock_ls(&ls);
    if (OB_FAIL(ls.get_ls_meta(ls_ckpt_member.ls_meta_))) {
      LOG_WARN("fail to get ls meta", K(ret));
    } else if (OB_FAIL(ls.get_dup_table_ls_meta(ls_ckpt_member.dup_ls_meta_))) {
      LOG_WARN("fail to get dup ls meta", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(record_tablet_meta(ls, ls_ckpt_member.tablet_meta_entry_, clog_max_scn, fd_dispenser))) {
    LOG_WARN("fail to write tablet checkpoint for this ls", K(ret), K(ls));
  } else if (OB_FAIL(write_item(ls_ckpt_member))) {
    LOG_WARN("fail to write ls item", K(ret), K(ls_ckpt_member));
  }
  return ret;
}

int ObTenantStorageSnapshotWriter::write_item(const ObLSCkptMember &ls_ckpt_member)
{
  int ret = OB_SUCCESS;
  int64_t buf_len = ls_ckpt_member.get_serialize_size();
  int64_t pos = 0;
  char *buf = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta writer hasn't been inited", K(ret));
  } else if (OB_UNLIKELY(!ls_ckpt_member.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_ckpt_member));
  } else if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(buf_len, "MetaWriter")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else if (OB_FAIL(ls_ckpt_member.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize ls ckpt member", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(ls_item_writer_.write_item(buf, buf_len, nullptr /*item idx*/))) {
    LOG_WARN("fail to write ls ckpt item", K(ret), KP(buf), K(buf_len));
  } else {
  }
  if (OB_LIKELY(nullptr != buf)) {
    ob_free(buf);
  }
  return ret;
}

int ObTenantStorageSnapshotWriter::copy_ls_meta_for_deleting(
    const ObMetaDiskAddr &addr,
    const char *buf,
    const int64_t buf_len,
    const ObLSID &ls_id)
{
  UNUSED(addr);
  int ret = OB_SUCCESS;
  ObLSCkptMember ls_ckpt_member;
  int64_t pos = 0;
  if (OB_FAIL(ls_ckpt_member.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize ls_ckpt_member", K(ret), KP(buf), K(buf_len));
  } else if (ls_id != ls_ckpt_member.ls_meta_.ls_id_ && OB_FAIL(write_item(ls_ckpt_member))) {
    LOG_WARN("fail to write ls snapshot", K(ret), K(ls_id), K(ls_ckpt_member));
  }
  return ret;
}

int ObTenantStorageSnapshotWriter::copy_ls_meta_for_creating(
    const ObMetaDiskAddr &addr,
    const char *buf,
    const int64_t buf_len)
{
  UNUSED(addr);
  int ret = OB_SUCCESS;
  ObLSCkptMember ls_ckpt_member;
  int64_t pos = 0;
  if (OB_FAIL(ls_ckpt_member.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize ls_ckpt_member", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(write_item(ls_ckpt_member))) {
    LOG_WARN("fail to write ls snapshot", K(ret), K(ls_ckpt_member));
  }
  return ret;
}

int ObTenantStorageSnapshotWriter::close(blocksstable::MacroBlockId &ls_meta_entry)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("meta writer hasn't been inited", K(ret));
  } else if (OB_FAIL(ls_item_writer_.close())) {
    LOG_WARN("fail to close ls item writer", K(ret));
  } else if (OB_FAIL(ls_item_writer_.get_entry_block(ls_meta_entry))) {
    LOG_WARN("fail to get ls entry block", K(ret));
  }
  return ret;
}

int ObTenantStorageSnapshotWriter::record_tablet_meta(
    ObLS &ls,
    MacroBlockId &tablet_meta_entry,
    share::SCN &clog_max_scn,
    ObSlogCheckpointFdDispenser *fd_dispenser)
{
  int ret = OB_SUCCESS;
  const int64_t total_tablet_cnt = ls.get_tablet_svr()->get_tablet_count();
  int64_t processed_cnt = 0;
  ObMetaDiskAddr addr;
  ObLSTabletAddrIterator tablet_iter;
  ObTabletMapKey tablet_key;
  char slog_buf[sizeof(ObUpdateTabletLog)];

  uint64_t data_version = 0;
  tablet_item_writer_.reuse_for_next_round();
  ObMemAttr mem_attr(MTL_ID(), ObModIds::OB_CHECKPOINT);
  if (OB_FAIL(tablet_item_writer_.init_for_slog_ckpt(MTL_ID(), MTL_EPOCH_ID(), mem_attr, fd_dispenser))) {
    LOG_WARN("failed to init tablet item writer", K(ret));
  } else if (OB_FAIL(ls.get_tablet_svr()->build_tablet_iter(tablet_iter))) {
    LOG_WARN("fail to build ls tablet iter", K(ret), K(ls));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  }

  while (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_iter.get_next_tablet_addr(tablet_key, addr))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to get next tablet", K(ret));
      }
    } else if (OB_UNLIKELY(!tablet_key.is_valid() || !addr.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet key or addr is invalid", K(ret), K(tablet_key), K(addr));
    } else if (addr.is_memory()) {
      FLOG_INFO("skip MEM type", K(ret), K(tablet_key), K(addr));
    } else if (addr.is_none()) {
      ret = OB_NEED_RETRY;  // tablet slog has been written, but the addr hasn't been updated
      LOG_WARN("addr is none", K(ret));
    } else if (ObTenantStorageMetaType::SNAPSHOT == meta_type_ && OB_FAIL(copy_tablet(ls, data_version, tablet_key, ls.get_ls_epoch(), slog_buf, clog_max_scn))) {
      LOG_WARN("fail to copy tablet", K(ret), K(tablet_key), K(data_version));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(tablet_item_writer_.close())) {
    LOG_WARN("fail to close tablet item writer", K(ret));
  } else if (OB_FAIL(tablet_item_writer_.get_entry_block(tablet_meta_entry))) {
    LOG_WARN("fail to get tablet meta entry", K(ret));
  }

  FLOG_INFO("write tablet checkpoint finish", K(ret), K(tablet_item_addr_info_arr_.count()), K(tablet_meta_entry));
  return ret;
}

int ObTenantStorageSnapshotWriter::copy_tablet(
    ObLS &ls,
    const uint64_t data_version,
    const ObTabletMapKey &tablet_key,
    const int64_t ls_epoch,
    char (&slog_buf)[sizeof(ObUpdateTabletLog)],
    share::SCN &clog_max_scn)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("MetaSnapshot");
  ObTabletHandle tablet_handle;
  ObTabletHandle new_empty_shell_handle;
  ObTablet *tablet = nullptr;
  int64_t slog_buf_pos = 0;
  MEMSET(slog_buf, 0, sizeof(ObUpdateTabletLog));
  ObUpdateTabletPointerParam update_pointer_param;
  ObMetaDiskAddr old_addr;

  if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->get_tablet_with_allocator(WashTabletPriority::WTP_LOW, tablet_key, allocator, tablet_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("skip writing snapshot for this tablet", K(tablet_key));
    } else {
      LOG_WARN("fail to get tablet with allocator", K(ret), K(tablet_key));
    }
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (tablet->get_tablet_addr().is_file()) {
    int64_t tablet_meta_version = 0;
    if (GCTX.is_shared_storage_mode() && OB_FAIL(ls.get_tablet_svr()->alloc_private_tablet_meta_version_with_lock(tablet_key, tablet_meta_version))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("skip writing snapshot for this tablet", K(tablet_key));
      } else {
        LOG_WARN("failed to alloc tablet meta version", K(ret), K(tablet_key));
      }
    }
    const ObTabletPersisterParam param(data_version, tablet_key.ls_id_, ls_epoch, tablet_key.tablet_id_, tablet->get_transfer_seq(), tablet_meta_version);
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!tablet->is_empty_shell())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("addr format normal tablet's shouldn't be file", K(ret), KPC(tablet));
    } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(param, *tablet, new_empty_shell_handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("skip writing snapshot for this tablet", K(tablet_key));
      } else {
        LOG_WARN("fail to persist and transform tablet", K(ret), K(tablet_key), KPC(tablet));
      }
    } else {
      old_addr = tablet->get_tablet_addr();
      tablet = new_empty_shell_handle.get_obj();
    }
  } else {
    old_addr = tablet->get_tablet_addr();
  }

  if (OB_FAIL(ret)) {
    // do nothing
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(tablet->get_updating_tablet_pointer_param(update_pointer_param))) {
    LOG_WARN("fail to get updating tablet pointer param", K(ret), KPC(tablet));
  } else {
    ObUpdateTabletLog slog(tablet_key.ls_id_, tablet_key.tablet_id_, update_pointer_param, ls_epoch);
    if (OB_UNLIKELY(!slog.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid slog entry", K(ret), K(slog), K(tablet_key), K(ls_epoch), K(update_pointer_param));
    } else if (OB_FAIL(slog.serialize(slog_buf, sizeof(ObUpdateTabletLog), slog_buf_pos))) {
      LOG_WARN("fail to serialize update tablet slog", K(ret), K(slog_buf_pos));
    } else if (OB_FAIL(tablet_item_writer_.write_item(slog_buf, slog.get_serialize_size()))) {
      LOG_WARN("fail to write update tablet slog into ckpt", K(ret));
    } else if (OB_FAIL(tablet->inc_macro_ref_cnt())) {
      LOG_WARN("fail to increase meta and data macro blocks' ref cnt", K(ret));
    } else {
      share::SCN tmp_scn = tablet->get_tablet_meta().clog_checkpoint_scn_;
      clog_max_scn = tmp_scn > clog_max_scn ? tmp_scn : clog_max_scn;
      TabletItemAddrInfo addr_info;
      addr_info.tablet_key_ = tablet_key;
      addr_info.old_addr_ = old_addr;
      addr_info.new_addr_ = slog.disk_addr_;
      addr_info.need_rollback_ = true;
      addr_info.tablet_pool_type_ = ObTabletPoolType::TP_MAX; // only used by checkpoint, so we set it to TP_MAX here
      if (OB_FAIL(tablet_item_addr_info_arr_.push_back(addr_info))) {
        LOG_WARN("fail to push back addr info", K(ret), K(addr_info));
      }
    }
  }
  return ret;
}

int ObTenantStorageSnapshotWriter::get_ls_block_list(common::ObIArray<MacroBlockId> *&block_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantStorageSnapshotWriter not inited", K(ret));
  } else {
    block_list = &(ls_item_writer_.get_meta_block_list());
  }
  return ret;
}

int ObTenantStorageSnapshotWriter::get_tablet_block_list(
  common::ObIArray<MacroBlockId> *&block_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantStorageSnapshotWriter not inited", K(ret));
  } else {
    ObIArray<MacroBlockId> &tablet_block_list = tablet_item_writer_.get_meta_block_list();
    block_list = &tablet_block_list;
  }
  return ret;
}

int ObTenantStorageSnapshotWriter::get_wait_gc_tablet_block_list(
    common::ObIArray<blocksstable::MacroBlockId> *&block_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantStorageSnapshotWriter not inited", K(ret));
  } else {
    ObIArray<MacroBlockId> &tablet_block_list = wait_gc_tablet_item_writer_.get_meta_block_list();
    block_list = &tablet_block_list;
  }
  return ret;
}

bool ObTenantStorageSnapshotWriter::ignore_ret(int ret)
{
  return OB_ALLOCATE_MEMORY_FAILED == ret || OB_DISK_HUNG == ret || OB_TIMEOUT == ret || OB_BUF_NOT_ENOUGH == ret;
}

int ObTenantStorageSnapshotWriter::rollback()
{
  int ret = OB_SUCCESS;
  int64_t rollback_cnt = 0;
  if (!is_inited_ || 0 == tablet_item_addr_info_arr_.count()) {
    // there's no new tablet, no need to rollback
  } else {
    ObArenaAllocator allocator("CkptRollback", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTablet tablet;
    for (int64_t i = 0; i < tablet_item_addr_info_arr_.count(); i++) {
      tablet.reset();
      allocator.reuse();
      int64_t buf_len = 0;
      char *buf = nullptr;
      int64_t pos = 0;
      const TabletItemAddrInfo &addr_info = tablet_item_addr_info_arr_.at(i);
      if (addr_info.need_rollback_ && !GCTX.is_shared_storage_mode()) {
        rollback_cnt++;
        do {
          allocator.reuse();
          if (OB_FAIL(MTL(ObTenantStorageMetaService*)->read_from_disk(
              addr_info.new_addr_,
              0, /* ls_epoch for share storage */
              allocator,
              buf,
              buf_len))) {
            LOG_WARN("fail to read from disk", K(ret), K(addr_info));
          }
        } while (ignore_ret(ret));
        if (OB_SUCC(ret)) {
          tablet.set_tablet_addr(addr_info.new_addr_);
          if (OB_FAIL(tablet.release_ref_cnt(allocator, buf, buf_len, pos))) {
            LOG_ERROR("fail to dec macro ref for tablet, macro block may leak", K(ret), K(tablet));
          }
        }
      }
    }
  }
  FLOG_INFO("finsh checkpoint rollback", K(ret), K(tablet_item_addr_info_arr_.count()), K(rollback_cnt));
  return ret;
}

int ObTenantStorageSnapshotWriter::get_tablet_with_addr(
    const int64_t ls_epoch,
    const TabletItemAddrInfo &addr_info,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObSharedObjectReadInfo read_info;
  int64_t buf_len;
  char *buf = nullptr;
  read_info.addr_ = addr_info.new_addr_;
  read_info.ls_epoch_ = ls_epoch;
  read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000;
  ObTabletPoolType tablet_pool_type = addr_info.tablet_pool_type_;
  // only need load first-level meta
  if (addr_info.new_addr_.is_raw_block()) {
    if (addr_info.new_addr_.size() > ObTabletCommon::MAX_TABLET_FIRST_LEVEL_META_SIZE) {
      read_info.addr_.set_size(ObTabletCommon::MAX_TABLET_FIRST_LEVEL_META_SIZE);
    }
  }
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  do {
    ObArenaAllocator allocator("SlogCkptWriter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObSharedObjectReadHandle shared_read_handle(allocator);
    int64_t pos = 0;
    if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->acquire_tablet_from_pool(
        tablet_pool_type,
        WashTabletPriority::WTP_LOW,
        addr_info.tablet_key_,
        tablet_handle))) {
      LOG_WARN("fail to acquire 4k tablet", K(ret), K(addr_info));
    } else if (OB_FAIL(ObSharedObjectReaderWriter::async_read(read_info, shared_read_handle))) {
      LOG_WARN("fail to read tablet buf from macro block", K(ret), K(read_info));
    } else if (OB_FAIL(shared_read_handle.wait())) {
      LOG_WARN("fail to wait async read", K(ret));
    } else if (OB_FAIL(shared_read_handle.get_data(allocator, buf, buf_len))) {
      LOG_WARN("fail to get tablet buf and buf_len", K(ret), K(shared_read_handle));
    } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data of block handle is invalid", K(ret), K(shared_read_handle));
    } else if (FALSE_IT(tablet_handle.get_obj()->set_tablet_addr(addr_info.new_addr_))) {
    } else if (OB_FAIL(tablet_handle.get_obj()->deserialize(buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize tiny tablet", K(ret), K(shared_read_handle), K(addr_info), K(pos));
    }

    if (OB_FAIL(ret)) {
      if ((OB_BUF_NOT_ENOUGH == ret) && (ObTabletPoolType::TP_NORMAL == tablet_pool_type)) {
        tablet_pool_type = ObTabletPoolType::TP_LARGE;
      } else if ((OB_BUF_NOT_ENOUGH == ret) && (ObTabletPoolType::TP_NORMAL != tablet_pool_type)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        // do nothing
      }
    }

  } while (ignore_ret(ret));

  return ret;
}

}  // namespace storage
}  // end namespace oceanbase
