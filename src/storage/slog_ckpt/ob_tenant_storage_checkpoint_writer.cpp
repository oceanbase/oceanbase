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

#include "storage/slog_ckpt/ob_tenant_storage_checkpoint_writer.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/slog/ob_storage_log_reader.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_mds_table_mini_merger.h"
#include "storage/tx/ob_timestamp_service.h"
#include "storage/tx/ob_trans_id_service.h"
#include "storage/tx/ob_dup_table_base.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "sql/das/ob_das_id_service.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "storage/blockstore/ob_shared_block_reader_writer.h"
#include "observer/omt/ob_tenant.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObTenantStorageCheckpointWriter::ObTenantStorageCheckpointWriter()
  : is_inited_(false),
    meta_type_(ObTenantStorageMetaType::INVALID_TYPE),
    tablet_item_addr_info_arr_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("TabletCkptArr", MTL_ID())),
    ls_item_writer_(),
    tablet_item_writer_()
{
}

int ObTenantStorageCheckpointWriter::init(const ObTenantStorageMetaType meta_type)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(MTL_ID(), ObModIds::OB_CHECKPOINT);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantStorageCheckpointWriter init twice", K(ret));
  } else if (OB_UNLIKELY(ObTenantStorageMetaType::INVALID_TYPE == meta_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(meta_type));
  } else if (OB_FAIL(ls_item_writer_.init(false /*whether need addr*/, mem_attr))) {
    LOG_WARN("fail to init ls item writer", K(ret));
  } else if (OB_FAIL(tablet_item_writer_.init(false /*whether need addr*/, mem_attr))) {
    LOG_WARN("fail to init tablet item writer", K(ret));
  } else {
    meta_type_ = meta_type;
    is_inited_ = true;
  }
  return ret;
}

void ObTenantStorageCheckpointWriter::reset()
{
  is_inited_ = false;
  tablet_item_addr_info_arr_.reset();
  ls_item_writer_.reset();
  tablet_item_writer_.reset();
  meta_type_ = ObTenantStorageMetaType::INVALID_TYPE;
}

int ObTenantStorageCheckpointWriter::record_meta(MacroBlockId &ls_meta_entry)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantStorageCheckpointWriter not inited", K(ret));
  } else if (OB_FAIL(record_ls_meta(ls_meta_entry))) {
    LOG_WARN("fail to construct ls ckpt linked list", K(ret));
  } else if (OB_FAIL(THE_IO_DEVICE->fsync_block())) {
    LOG_WARN("fail to fsync_block", K(ret));
  }
  return ret;
}

int ObTenantStorageCheckpointWriter::record_single_ls_meta(
    const MacroBlockId &orig_ls_meta_entry,
    const ObLSID &ls_id,
    ObIArray<blocksstable::MacroBlockId> &orig_linked_block_list,
    blocksstable::MacroBlockId &ls_meta_entry,
    share::SCN &clog_max_scn)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTenantStorageCheckpointReader ls_ckpt_reader;
  ObTenantStorageCheckpointReader::ObStorageMetaOp copy_ls_meta_op = std::bind(
      &ObTenantStorageCheckpointWriter::copy_ls_meta_for_creating,
      this,
      std::placeholders::_1,
      std::placeholders::_2,
      std::placeholders::_3);

  if (OB_UNLIKELY(!orig_ls_meta_entry.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(orig_ls_meta_entry), K(ls_id));
  } else if (OB_FAIL(ls_ckpt_reader.iter_read_meta_item(orig_ls_meta_entry, copy_ls_meta_op, orig_linked_block_list))) {
    LOG_WARN("fail to iter read and write ls snapshot", K(ret), K(orig_ls_meta_entry));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", K(ret));
  } else if (OB_FAIL(do_record_ls_meta(*(ls_handle.get_ls()), clog_max_scn))) {
    LOG_WARN("fail to record single ls meta", K(ret), K(ls_handle));
  } else if (OB_FAIL(close(ls_meta_entry))) {
    LOG_WARN("fail to close tenant storage checkpoint writer", K(ret));
  }
  return ret;
}

int ObTenantStorageCheckpointWriter::delete_single_ls_meta(
    const MacroBlockId &orig_ls_meta_entry,
    const ObLSID &ls_id,
    ObIArray<blocksstable::MacroBlockId> &orig_linked_block_list,
    blocksstable::MacroBlockId &ls_meta_entry)
{
  int ret = OB_SUCCESS;
  ObTenantStorageCheckpointReader ls_ckpt_reader;
  ObTenantStorageCheckpointReader::ObStorageMetaOp copy_ls_meta_op = std::bind(
      &ObTenantStorageCheckpointWriter::copy_ls_meta_for_deleting,
      this,
      std::placeholders::_1,
      std::placeholders::_2,
      std::placeholders::_3,
      ls_id);

  if (OB_UNLIKELY(!orig_ls_meta_entry.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(orig_ls_meta_entry), K(ls_id));
  } else if (OB_FAIL(ls_ckpt_reader.iter_read_meta_item(orig_ls_meta_entry, copy_ls_meta_op, orig_linked_block_list))) {
    LOG_WARN("fail to iter read and write ls snapshot", K(ret), K(orig_ls_meta_entry));
  } else if (OB_FAIL(close(ls_meta_entry))) {
    LOG_WARN("fail to close tenant storage checkpoint writer", K(ret));
  }
  return ret;
}

int ObTenantStorageCheckpointWriter::record_ls_meta(MacroBlockId &ls_entry_block)
{
  int ret = OB_SUCCESS;
  common::ObSharedGuard<ObLSIterator> ls_iter;
  ObLS *ls = nullptr;

  ls_item_writer_.reset();
  tablet_item_writer_.reset();
  ObMemAttr mem_attr(MTL_ID(), ObModIds::OB_CHECKPOINT);
  if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream iter", K(ret));
  } else if (OB_FAIL(ls_item_writer_.init(false /*whether need addr*/, mem_attr))) {
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

      if (OB_SUCC(ret) && OB_FAIL(do_record_ls_meta(*ls, unused_scn))) {
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

int ObTenantStorageCheckpointWriter::do_record_ls_meta(ObLS &ls, share::SCN &clog_max_scn)
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
  } else if (OB_FAIL(record_tablet_meta(ls, ls_ckpt_member.tablet_meta_entry_, clog_max_scn))) {
    LOG_WARN("fail to write tablet checkpoint for this ls", K(ret), K(ls));
  } else if (OB_FAIL(write_item(ls_ckpt_member))) {
    LOG_WARN("fail to write ls item", K(ret), K(ls_ckpt_member));
  }
  return ret;
}

int ObTenantStorageCheckpointWriter::write_item(const ObLSCkptMember &ls_ckpt_member)
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

int ObTenantStorageCheckpointWriter::copy_ls_meta_for_deleting(
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

int ObTenantStorageCheckpointWriter::copy_ls_meta_for_creating(
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

int ObTenantStorageCheckpointWriter::close(blocksstable::MacroBlockId &ls_meta_entry)
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

int ObTenantStorageCheckpointWriter::record_tablet_meta(ObLS &ls, MacroBlockId &tablet_meta_entry, share::SCN &clog_max_scn)
{
  int ret = OB_SUCCESS;
  ObMetaDiskAddr addr;
  ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_READABLE_COMMITED);
  ObTabletMapKey tablet_key;
  char slog_buf[sizeof(ObUpdateTabletLog)];

  tablet_item_writer_.reuse_for_next_round();
  ObMemAttr mem_attr(MTL_ID(), ObModIds::OB_CHECKPOINT);
  if (OB_FAIL(tablet_item_writer_.init(false /*whether need addr*/, mem_attr))) {
    LOG_WARN("failed to init tablet item writer", K(ret));
  } else if (OB_FAIL(ls.get_tablet_svr()->build_tablet_iter(tablet_iter))) {
    LOG_WARN("fail to build ls tablet iter", K(ret), K(ls));
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
    } else if (ObTenantStorageMetaType::CKPT == meta_type_) {
      do {
        if (OB_FAIL(persist_and_copy_tablet(tablet_key, addr, slog_buf))) {
          LOG_WARN("fail to persist and copy tablet", K(ret), K(tablet_key), K(addr));
        }
      } while (OB_SERVER_OUTOF_DISK_SPACE == ret);
    } else if (ObTenantStorageMetaType::SNAPSHOT == meta_type_ && OB_FAIL(copy_tablet(tablet_key, slog_buf, clog_max_scn))) {
      LOG_WARN("fail to copy tablet", K(ret), K(tablet_key));
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

int ObTenantStorageCheckpointWriter::persist_and_copy_tablet(
    const ObTabletMapKey &tablet_key,
    const ObMetaDiskAddr &old_addr,
    char (&slog_buf)[sizeof(ObUpdateTabletLog)])
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("SlogCkptWriter");
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObLSService *ls_service = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObTabletHandle old_tablet_handle;
  ObTabletHandle tmp_tablet_handle;
  ObTabletHandle new_tablet_handle;
  ObTablet *old_tablet = nullptr;
  ObTablet *new_tablet = nullptr;
  int64_t slog_buf_pos = 0;
  MEMSET(slog_buf, 0, sizeof(ObUpdateTabletLog));
  ObUpdateTabletLog slog;
  slog.ls_id_ = tablet_key.ls_id_;
  slog.tablet_id_ = tablet_key.tablet_id_;
  bool has_slog = false;

  if (OB_FAIL(OB_E(EventTable::EN_SLOG_CKPT_ERROR) OB_SUCCESS)) {
  } else if (OB_FAIL(MTL(ObTenantCheckpointSlogHandler*)->check_slog(tablet_key, has_slog))) {
    LOG_WARN("fail to check whether tablet has been written slog", K(ret), K(tablet_key));
  } else if (has_slog) {
    // tablet has been updated, skip
  } else if (OB_FAIL(t3m->get_tablet_with_allocator(WashTabletPriority::WTP_LOW, tablet_key, allocator, old_tablet_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // skip write this tablet's checkpoint
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tablet with allocator", K(ret), K(tablet_key));
    }
  } else {
    old_tablet = old_tablet_handle.get_obj();
    ObTablet *src_tablet = nullptr;
    const bool need_compat = old_tablet->get_version() < ObTablet::VERSION_V4;
    if (!need_compat) {
      src_tablet = old_tablet;
    } else if (OB_FAIL(handle_old_version_tablet_for_compat(allocator, tablet_key, *old_tablet, tmp_tablet_handle))) {
      LOG_WARN("fail to handle old version tablet for compat", K(ret), K(tablet_key), KPC(old_tablet));
    } else {
      src_tablet = tmp_tablet_handle.get_obj();
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*src_tablet, new_tablet_handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("skip writing checkpoint for this tablet", K(ret), K(tablet_key));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to persist and transform tablet", K(ret), K(tablet_key), K(need_compat), KPC(src_tablet));
      }
    } else if (FALSE_IT(new_tablet = new_tablet_handle.get_obj())) {
    } else if (FALSE_IT(slog.disk_addr_ = new_tablet->get_tablet_addr())) {
    } else if (OB_FAIL(slog.serialize(slog_buf, sizeof(ObUpdateTabletLog), slog_buf_pos))) {
      LOG_WARN("fail to serialize update tablet slog", K(ret), K(slog_buf_pos));
    } else if (OB_FAIL(tablet_item_writer_.write_item(slog_buf, slog.get_serialize_size()))) {
      LOG_WARN("fail to write update tablet slog into ckpt", K(ret));
    } else if (OB_FAIL(new_tablet->inc_macro_ref_cnt())) {
      LOG_WARN("fail to increase meta and data macro blocks' ref cnt", K(ret));
    } else {
      TabletItemAddrInfo addr_info;
      addr_info.tablet_key_ = tablet_key;
      addr_info.old_addr_ = old_addr;
      addr_info.new_addr_ = slog.disk_addr_;
      addr_info.need_rollback_ = true;
      if (OB_FAIL(ObTenantMetaMemMgr::get_tablet_pool_type(new_tablet_handle.get_buf_len(), addr_info.tablet_pool_type_))) {
        LOG_WARN("fail to get tablet pool type", K(ret), K(addr_info));
      } else if (OB_FAIL(tablet_item_addr_info_arr_.push_back(addr_info))) {
        LOG_WARN("fail to push back addr info", K(ret), K(addr_info));
      }
    }
  }

  return ret;
}

int ObTenantStorageCheckpointWriter::copy_tablet(
    const ObTabletMapKey &tablet_key,
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
  ObUpdateTabletLog slog;
  slog.ls_id_ = tablet_key.ls_id_;
  slog.tablet_id_ = tablet_key.tablet_id_;
  ObMetaDiskAddr old_addr;

  if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->get_tablet_with_allocator(WashTabletPriority::WTP_LOW, tablet_key, allocator, tablet_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("skip writing snapshot for this tablet", K(tablet_key));
    } else {
      LOG_WARN("fail to get tablet with allocator", K(ret), K(tablet_key));
    }
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (tablet->get_tablet_addr().is_file()) {
    if (OB_UNLIKELY(!tablet->is_empty_shell())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("addr format normal tablet's shouldn't be file", K(ret), KPC(tablet));
    } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*tablet, new_empty_shell_handle))) {
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
  } else if (FALSE_IT(slog.disk_addr_ = tablet->get_tablet_addr())) {
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
  return ret;
}

int ObTenantStorageCheckpointWriter::handle_old_version_tablet_for_compat(
    common::ObArenaAllocator &allocator,
    const ObTabletMapKey &tablet_key,
    const ObTablet &old_tablet,
    ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTablet *new_tablet = nullptr;
  ObTableHandleV2 mds_mini_sstable;

  if (OB_FAIL(ObMdsDataCompatHelper::generate_mds_mini_sstable(old_tablet, allocator, mds_mini_sstable))) {
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    } else if (OB_EMPTY_RESULT == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("empty mds data in old tablet, no need to generate mds mini sstable", K(ret));
    } else {
      LOG_WARN("fail to generate mds mini sstable", K(ret), K(tablet_key));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(tablet_key, allocator, new_tablet_handle))) {
    LOG_WARN("fail to create tmp tablet", K(ret), K(tablet_key));
  } else if (FALSE_IT(new_tablet = new_tablet_handle.get_obj())) {
  } else if (OB_FAIL(new_tablet->init_for_compat(allocator, old_tablet, mds_mini_sstable))) {
    LOG_WARN("fail to init tablet", K(ret), K(tablet_key));
  } else {
    LOG_INFO("succeed to handle mds data for tablet", K(ret), K(tablet_key), K(mds_mini_sstable));
  }

  return ret;
}

int ObTenantStorageCheckpointWriter::get_ls_block_list(common::ObIArray<MacroBlockId> *&block_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantStorageCheckpointWriter not inited", K(ret));
  } else {
    block_list = &(ls_item_writer_.get_meta_block_list());
  }
  return ret;
}

int ObTenantStorageCheckpointWriter::get_tablet_block_list(
  common::ObIArray<MacroBlockId> *&block_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantStorageCheckpointWriter not inited", K(ret));
  } else {
    ObIArray<MacroBlockId> &tablet_block_list = tablet_item_writer_.get_meta_block_list();
    block_list = &tablet_block_list;
  }
  return ret;
}

int ObTenantStorageCheckpointWriter::batch_compare_and_swap_tablet(const bool is_replay_old)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantStorageCheckpointWriter not init", K(ret));
  }
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ObTabletHandle new_tablet_handle;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = nullptr;

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_item_addr_info_arr_.count(); i++) {
    TabletItemAddrInfo &addr_info = tablet_item_addr_info_arr_.at(i);
    ObMetaDiskAddr tablet_addr;
    if (OB_FAIL(t3m->get_tablet_addr(addr_info.tablet_key_, tablet_addr))) {
      // OB_ENTRY_NOT_EXIST is not allowed during upgrade
      if (OB_ENTRY_NOT_EXIST != ret || is_replay_old) {
        LOG_WARN("fail to get tablet addr", K(ret), K(addr_info));
      } else {
        ret = OB_SUCCESS;
        LOG_INFO("this tablet has been deleted, skip the swap", K(addr_info));
      }
    } else if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is null", K(ret));
    } else if (OB_FAIL(ls_svr->get_ls(addr_info.tablet_key_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get ls", K(ret), K(addr_info));
    } else if (!is_replay_old) {
      if (OB_FAIL(get_tablet_with_addr(addr_info, new_tablet_handle))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to load tablet", K(ret), K(addr_info));
        } else {
          ret = OB_SUCCESS;
          LOG_INFO("this tablet has been deleted, skip the swap", K(addr_info));
        }
      } else if (FALSE_IT(addr_info.need_rollback_ = false)) {
      } else if (!tablet_addr.is_equal_for_persistence(addr_info.old_addr_)) { // ignore the change of memtable seq
        // we must check the addr after loading tablet, otherwise the macro ref cnt won't be decreased
        LOG_INFO("the tablet has changed, skip the swap", K(tablet_addr), K(addr_info));
      } else {
        do {
          if (OB_FAIL(ls_handle.get_ls()->update_tablet_checkpoint(
              addr_info.tablet_key_,
              addr_info.old_addr_,
              addr_info.new_addr_,
              is_replay_old,
              new_tablet_handle))) {
            if (OB_NOT_THE_OBJECT == ret) {
              ret = OB_SUCCESS;
              LOG_INFO("tablet has changed, no need to swap", K(ret), K(addr_info));
            } else if (OB_TABLET_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              LOG_INFO("tablet has been deleted, no need to swap", K(ret), K(addr_info));
            } else {
              LOG_WARN("fail to compare and swap tablet with seq check", K(ret), K(addr_info));
            }
          }
        } while (ignore_ret(ret));
      }
    } else {
      addr_info.need_rollback_ = false;
      ObArenaAllocator allocator("CompatLoad", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObTabletHandle old_tablet_handle;
      do {
        old_tablet_handle.reset();
        allocator.reuse();
        if (OB_FAIL(t3m->get_tablet_with_allocator(
            WashTabletPriority::WTP_LOW, addr_info.tablet_key_, allocator, old_tablet_handle))) {
          LOG_WARN("fail to get tablet with allocator", K(ret), K(addr_info));
        } else if (OB_FAIL(ls_handle.get_ls()->update_tablet_checkpoint(
            addr_info.tablet_key_,
            addr_info.old_addr_,
            addr_info.new_addr_,
            is_replay_old,
            new_tablet_handle))) {
          LOG_WARN("fail to compare and swap tablet with seq check", K(ret), K(addr_info));
        }
      } while (ignore_ret(ret));
      if (OB_SUCC(ret)) {
        old_tablet_handle.get_obj()->dec_macro_ref_cnt();
      }
    }
  }

  return ret;
}

bool ObTenantStorageCheckpointWriter::ignore_ret(int ret)
{
  return OB_ALLOCATE_MEMORY_FAILED == ret || OB_DISK_HUNG == ret || OB_TIMEOUT == ret || OB_BUF_NOT_ENOUGH == ret;
}

int ObTenantStorageCheckpointWriter::rollback()
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
      if (addr_info.need_rollback_) {
        rollback_cnt++;
        do {
          allocator.reuse();
          if (OB_FAIL(MTL(ObTenantCheckpointSlogHandler*)->read_from_disk(
              addr_info.new_addr_,
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

int ObTenantStorageCheckpointWriter::get_tablet_with_addr(
    const TabletItemAddrInfo &addr_info,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObSharedBlockReadInfo read_info;
  int64_t buf_len;
  char *buf = nullptr;
  read_info.addr_ = addr_info.new_addr_;
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
    ObSharedBlockReadHandle block_handle(allocator);
    int64_t pos = 0;
    if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->acquire_tablet_from_pool(
        tablet_pool_type,
        WashTabletPriority::WTP_LOW,
        addr_info.tablet_key_,
        tablet_handle))) {
      LOG_WARN("fail to acquire 4k tablet", K(ret), K(addr_info));
    } else if (OB_FAIL(ObSharedBlockReaderWriter::async_read(read_info, block_handle))) {
      LOG_WARN("fail to read tablet buf from macro block", K(ret), K(read_info));
    } else if (OB_FAIL(block_handle.wait())) {
      LOG_WARN("fail to wait async read", K(ret));
    } else if (OB_FAIL(block_handle.get_data(allocator, buf, buf_len))) {
      LOG_WARN("fail to get tablet buf and buf_len", K(ret), K(block_handle));
    } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data of block handle is invalid", K(ret), K(block_handle));
    } else if (FALSE_IT(tablet_handle.get_obj()->set_tablet_addr(addr_info.new_addr_))) {
    } else if (OB_FAIL(tablet_handle.get_obj()->deserialize(buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize tiny tablet", K(ret), K(block_handle), K(addr_info), K(pos));
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
