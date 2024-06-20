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

#include "ob_tenant_meta_snapshot_handler.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/ob_super_block_struct.h"
#include "storage/tx/ob_timestamp_service.h"
#include "observer/omt/ob_tenant.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_slog_helper.h"
#include "storage/slog/ob_storage_logger.h"
#include "observer/ob_startup_accel_task_handler.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{
int ObTenantMetaSnapshotHandler::create_tenant_snapshot(const ObTenantSnapshotID &snapshot_id)
{
  int ret = OB_SUCCESS;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  const ObTenantSuperBlock last_super_block = tenant->get_super_block();
  ObTenantSnapshotMeta snapshot;
  snapshot.snapshot_id_ = snapshot_id;

  if (OB_UNLIKELY(!snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(snapshot_id));
  } else if (OB_FAIL(last_super_block.check_new_snapshot(snapshot_id))) {
    LOG_WARN("fail to check snapshot version", K(ret));
  } else if (OB_UNLIKELY(tenant->is_hidden())) {
    ret = OB_NOT_SUPPORTED;
    LOG_INFO("shouldn't create snapshot for hidden tenant", K(ret));
  } else if (OB_UNLIKELY(!last_super_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tenant super block", K(ret), K(last_super_block));
  } else if (OB_FAIL(MTL(ObTenantCheckpointSlogHandler*)->add_snapshot(snapshot))) {
    LOG_WARN("fail to add snapshot", K(ret), K(snapshot));
  }

  FLOG_INFO("finish creating tenant snapshot", K(ret), K(last_super_block));
  return ret;
}

int ObTenantMetaSnapshotHandler::create_single_ls_snapshot(const ObTenantSnapshotID &snapshot_id,
                                                           const ObLSID &ls_id,
                                                           share::SCN &clog_max_scn)
{
  int ret = OB_SUCCESS;
  ObTenantStorageCheckpointWriter tenant_storage_meta_writer;
  MacroBlockId orig_ls_meta_entry;
  ObTenantSnapshotMeta snapshot;
  ObSArray<MacroBlockId> ls_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("CreateSnapLS", MTL_ID()));
  bool inc_ls_blocks_ref_succ = false;
  bool inc_tablet_blocks_ref_succ = false;

  if (OB_UNLIKELY(!snapshot_id.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(snapshot_id), K(ls_id));
  } else if (OB_FAIL(get_ls_meta_entry(snapshot_id, orig_ls_meta_entry))) {
    LOG_WARN("fail to get ls meta entry", K(ret), K(snapshot_id));
  } else if (OB_FAIL(tenant_storage_meta_writer.init(ObTenantStorageMetaType::SNAPSHOT))) {
    LOG_WARN("fail to init tenant storage checkpoint writer", K(ret), K(snapshot_id), K(ls_id));
  } else if (OB_FAIL(tenant_storage_meta_writer.record_single_ls_meta(orig_ls_meta_entry,
                                                                      ls_id,
                                                                      ls_block_list,
                                                                      snapshot.ls_meta_entry_,
                                                                      clog_max_scn))) {
    LOG_WARN("fail to record_single_ls_meta", K(ret), K(orig_ls_meta_entry), K(snapshot_id), K(ls_id));
  } else if (FALSE_IT(snapshot.snapshot_id_ = snapshot_id)) {
  } else if (OB_FAIL(inc_all_linked_block_ref(tenant_storage_meta_writer,
                                              inc_ls_blocks_ref_succ,
                                              inc_tablet_blocks_ref_succ))) {
    LOG_WARN("fail to increase ref cnt for all linked blocks", K(ret), K(snapshot_id), K(ls_id));
  } else if (OB_FAIL(MTL(ObTenantCheckpointSlogHandler*)->swap_snapshot(snapshot))) {
    LOG_WARN("fail to swap snapshot", K(ret), K(snapshot_id), K(ls_id), K(snapshot));
  }

  if (OB_FAIL(ret)) {
    rollback_ref_cnt(inc_ls_blocks_ref_succ, inc_tablet_blocks_ref_succ, tenant_storage_meta_writer);
  } else {
    dec_meta_block_ref(ls_block_list);
  }

  FLOG_INFO("finish create ls snapshot", K(ret), K(snapshot_id), K(ls_id), K(snapshot));
  return ret;
}

int ObTenantMetaSnapshotHandler::delete_single_ls_snapshot(const ObTenantSnapshotID &snapshot_id,
                                                           const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObTenantStorageCheckpointWriter tenant_storage_meta_writer;
  MacroBlockId orig_ls_meta_entry;
  MacroBlockId tablet_meta_entry;
  ObTenantSnapshotMeta snapshot;
  ObSArray<MacroBlockId> ls_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("CreateSnapLS", MTL_ID()));
  ObSArray<ObMetaDiskAddr> deleted_tablet_addrs(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("DelSnapLS", MTL_ID()));
  ObSArray<MacroBlockId> tablet_meta_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("DelSnapLS", MTL_ID()));
  bool inc_ls_blocks_ref_succ = false;
  bool inc_tablet_blocks_ref_succ = false;

  if (OB_UNLIKELY(!snapshot_id.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(snapshot_id), K(ls_id));
  } else if (OB_FAIL(get_ls_meta_entry(snapshot_id, orig_ls_meta_entry))) {
    LOG_WARN("fail to get ls meta entry", K(ret), K(snapshot_id));
  } else if (OB_FAIL(find_tablet_meta_entry(orig_ls_meta_entry, ls_id, tablet_meta_entry))) {
    LOG_WARN("fail to get tablet meta entry", K(ret), K(snapshot_id), K(ls_id));
  } else if (OB_FAIL(inner_delete_ls_snapshot(tablet_meta_entry,
                                              deleted_tablet_addrs,
                                              tablet_meta_block_list))) {
    LOG_WARN("fail to exec inner_delete_ls_snapshot", K(ret), K(tablet_meta_entry), K(snapshot_id), K(ls_id));
  } else if (OB_FAIL(tenant_storage_meta_writer.init(ObTenantStorageMetaType::SNAPSHOT))) {
    LOG_WARN("fail to init tenant storage checkpoint writer", K(ret), K(snapshot_id), K(ls_id));
  } else if (OB_FAIL(tenant_storage_meta_writer.delete_single_ls_meta(orig_ls_meta_entry, ls_id, ls_block_list, snapshot.ls_meta_entry_))) {
    LOG_WARN("fail to delete_single_ls_meta", K(ret), K(orig_ls_meta_entry), K(snapshot_id), K(ls_id));
  } else if (FALSE_IT(snapshot.snapshot_id_ = snapshot_id)) {
  } else if (OB_FAIL(inc_all_linked_block_ref(tenant_storage_meta_writer, inc_ls_blocks_ref_succ, inc_tablet_blocks_ref_succ))) {
    LOG_WARN("fail to increase ref cnt for all linked blocks", K(ret), K(snapshot_id), K(ls_id));
  } else if (OB_FAIL(MTL(ObTenantCheckpointSlogHandler*)->swap_snapshot(snapshot))) {
    LOG_WARN("fail to swap snapshot", K(ret), K(snapshot_id), K(ls_id), K(snapshot));
  }

  if (OB_FAIL(ret)) {
    rollback_ref_cnt(inc_ls_blocks_ref_succ, inc_tablet_blocks_ref_succ, tenant_storage_meta_writer);
  } else {
    dec_meta_block_ref(ls_block_list);
    dec_meta_block_ref(tablet_meta_block_list);
    if (OB_FAIL(inner_delete_tablet_by_addrs(deleted_tablet_addrs))) {
      LOG_WARN("fail to inner_delete_tablet_by_addrs", K(ret), K(snapshot_id), K(ls_id));
    }
  }

  FLOG_INFO("finish delete ls snapshot", K(ret), K(snapshot_id), K(ls_id), K(snapshot));
  return ret;
}

int ObTenantMetaSnapshotHandler::inc_all_linked_block_ref(
    ObTenantStorageCheckpointWriter &tenant_storage_meta_writer,
    bool &inc_ls_blocks_ref_succ,
    bool &inc_tablet_blocks_ref_succ)
{
  int ret = OB_SUCCESS;
  ObIArray<MacroBlockId> *meta_block_list = nullptr;
  if (OB_FAIL(tenant_storage_meta_writer.get_ls_block_list(meta_block_list))) {
    LOG_WARN("fail to get ls block list", K(ret));
  } else if (OB_FAIL(inc_linked_block_ref(*meta_block_list, inc_ls_blocks_ref_succ))) {
    LOG_WARN("fail to increase macro block ref for ls block", K(ret));
  } else if (OB_FAIL(tenant_storage_meta_writer.get_tablet_block_list(meta_block_list))) {
    LOG_WARN("fail to get tablet block list", K(ret));
  } else if (OB_FAIL(inc_linked_block_ref(*meta_block_list, inc_tablet_blocks_ref_succ))) {
    LOG_WARN("fail to increase macro block ref for tablet block", K(ret));
  }
  return ret;
}

void ObTenantMetaSnapshotHandler::rollback_ref_cnt(
    const bool inc_ls_blocks_ref_succ,
    const bool inc_tablet_blocks_ref_succ,
    ObTenantStorageCheckpointWriter &tenant_storage_meta_writer)
{
  int ret = OB_SUCCESS;
  ObIArray<MacroBlockId> *meta_block_list = nullptr;
  // ignore all ret, because we need to rollback the ref cnt as much as possible
  if (OB_FAIL(tenant_storage_meta_writer.rollback())) {
    LOG_ERROR("fail to rollback checkpoint, macro blocks may leak", K(ret));
  }
  if (inc_ls_blocks_ref_succ) {
    if (OB_FAIL(tenant_storage_meta_writer.get_ls_block_list(meta_block_list))) {
      LOG_ERROR("fail to get ls block list, macro blocks may leak", K(ret));
    } else {
      dec_meta_block_ref(*meta_block_list);
    }
  }
  if (inc_tablet_blocks_ref_succ) {
    if (OB_FAIL(tenant_storage_meta_writer.get_tablet_block_list(meta_block_list))) {
      LOG_ERROR("fail to get tablet block list, macro blocks may leak", K(ret));
    } else {
      dec_meta_block_ref(*meta_block_list);
    }
  }
}

int ObTenantMetaSnapshotHandler::get_ls_meta_entry(
    const ObTenantSnapshotID &snapshot_id,
    blocksstable::MacroBlockId &ls_meta_entry)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotMeta snapshot;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  const ObTenantSuperBlock super_block = tenant->get_super_block();
  if (OB_UNLIKELY(!snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(snapshot_id));
  } else if (OB_UNLIKELY(tenant->is_hidden())) {
    ret = OB_NOT_SUPPORTED;
    LOG_INFO("shouldn't get snapshot from hidden tenant", K(ret));
  } else if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tenant super block", K(ret), K(super_block));
  } else if (OB_FAIL(super_block.get_snapshot(snapshot_id, snapshot))) {
    LOG_WARN("fail to get snapshot", K(ret), K(snapshot_id), K(super_block));
  } else {
    ls_meta_entry = snapshot.ls_meta_entry_;
  }
  return ret;
}

int ObTenantMetaSnapshotHandler::inc_linked_block_ref(
    const ObIArray<blocksstable::MacroBlockId> &meta_block_list,
    bool &inc_success)
{
  int ret = OB_SUCCESS;
  inc_success = false;
  int64_t meta_block_num = 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < meta_block_list.count(); i++) {
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(meta_block_list.at(i)))) {
      LOG_WARN("fail to increase meta block ref", K(ret), K(meta_block_list.at(i)));
    } else {
      meta_block_num++;
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < meta_block_num; i++) {
      if (OB_TMP_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(meta_block_list.at(i)))) {
        LOG_WARN("fail to decrease meta block ref, macro block may leak", K(tmp_ret), K(meta_block_list.at(i)));
      }
    }
  } else {
    inc_success = true;
  }
  return ret;
}

void ObTenantMetaSnapshotHandler::dec_meta_block_ref(const ObIArray<blocksstable::MacroBlockId> &meta_block_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < meta_block_list.count(); i++) {
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(meta_block_list.at(i)))) {
      LOG_WARN("fail to decrease meta block ref, macro block may leak", K(ret), K(meta_block_list.at(i)));
    }
  }
}

int ObTenantMetaSnapshotHandler::delete_tenant_snapshot(const ObTenantSnapshotID &snapshot_id)
{
  int ret = OB_SUCCESS;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  const ObTenantSuperBlock last_super_block = tenant->get_super_block();
  ObTenantStorageCheckpointReader ls_snapshot_reader;
  ObTenantSnapshotMeta snapshot;
  ObSArray<MacroBlockId> ls_meta_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("DelSnap", MTL_ID()));
  ObSArray<ObMetaDiskAddr> deleted_tablet_addrs(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("DelSnap", MTL_ID()));
  ObSArray<MacroBlockId> tablet_meta_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("DelSnap", MTL_ID()));
  ObTenantStorageCheckpointReader::ObStorageMetaOp del_ls_snapshot_op = std::bind(
      &ObTenantMetaSnapshotHandler::delete_ls_snapshot,
      std::placeholders::_1,
      std::placeholders::_2,
      std::placeholders::_3,
      std::ref(deleted_tablet_addrs),
      std::ref(tablet_meta_block_list));

  if (OB_UNLIKELY(!snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(snapshot_id));
  } else if (OB_UNLIKELY(tenant->is_hidden())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't delete snapshot for hidden tenant", K(ret));
  } else if (OB_UNLIKELY(!last_super_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("super block is invalid", K(ret), K(last_super_block));
  } else if (OB_FAIL(last_super_block.get_snapshot(snapshot_id, snapshot))) {
    LOG_WARN("fail to get snapshot", K(ret), K(snapshot_id), K(last_super_block));
  } else if (OB_FAIL(ls_snapshot_reader.iter_read_meta_item(
      snapshot.ls_meta_entry_, del_ls_snapshot_op, ls_meta_block_list))) {
    LOG_WARN("fail to delete ls snapshot", K(ret), K(snapshot));
  } else if (OB_FAIL(MTL(ObTenantCheckpointSlogHandler*)->delete_snapshot(snapshot_id))) {
    LOG_WARN("fail to delete snapshot", K(ret), K(snapshot_id));
  } else {
    dec_meta_block_ref(ls_meta_block_list);
    dec_meta_block_ref(tablet_meta_block_list);
    if (OB_FAIL(inner_delete_tablet_by_addrs(deleted_tablet_addrs))) {
      LOG_WARN("fail to inner_delete_tablet_by_addrs", K(ret), K(snapshot_id));
    }
  }

  FLOG_INFO("finish deleting tenant snapshot", K(ret), K(last_super_block));
  return ret;
}

int ObTenantMetaSnapshotHandler::inner_delete_ls_snapshot(
    const blocksstable::MacroBlockId& tablet_meta_entry,
    ObIArray<ObMetaDiskAddr> &deleted_tablet_addrs,
    ObIArray<MacroBlockId> &tablet_meta_block_list)
{
  int ret = OB_SUCCESS;
  ObTenantStorageCheckpointReader tablet_snapshot_reader;
  ObSArray<MacroBlockId> meta_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("SnapTablet", MTL_ID()));
  ObTenantStorageCheckpointReader::ObStorageMetaOp del_tablet_snapshot_op = std::bind(
      &ObTenantMetaSnapshotHandler::delete_tablet_snapshot,
      std::placeholders::_1,
      std::placeholders::_2,
      std::placeholders::_3,
      std::ref(deleted_tablet_addrs));

  if (OB_FAIL(tablet_snapshot_reader.iter_read_meta_item(
      tablet_meta_entry, del_tablet_snapshot_op, meta_block_list))) {
    LOG_WARN("fail to delete tablet snapshot", K(ret), K(tablet_meta_entry));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_block_list.count(); i++) {
      if (OB_FAIL(tablet_meta_block_list.push_back(meta_block_list.at(i)))) {
        LOG_WARN("fail to push back meta block id", K(ret), K(i), K(meta_block_list.at(i)));
      }
    }
  }
  return ret;
}

int ObTenantMetaSnapshotHandler::delete_ls_snapshot(
    const ObMetaDiskAddr &addr,
    const char *buf,
    const int64_t buf_len,
    ObIArray<ObMetaDiskAddr> &deleted_tablet_addrs,
    ObIArray<MacroBlockId> &tablet_meta_block_list)
{
  UNUSED(addr);
  int ret = OB_SUCCESS;
  ObLSCkptMember ls_ckpt_member;
  int64_t pos = 0;

  if (OB_FAIL(ls_ckpt_member.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize ls_ckpt_member", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(inner_delete_ls_snapshot(ls_ckpt_member.tablet_meta_entry_,
                                              deleted_tablet_addrs,
                                              tablet_meta_block_list))) {
    LOG_WARN("fail to exec inner_delete_ls_snapshot", K(ret), K(ls_ckpt_member));
  }

  return ret;
}

int ObTenantMetaSnapshotHandler::inner_delete_tablet_by_addrs(
    const ObIArray<ObMetaDiskAddr> &deleted_tablet_addrs)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator arena_allocator("DelSnapTablet", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTablet tablet;
  for (int64_t i = 0; i < deleted_tablet_addrs.count(); i++) {
    tablet.reset();
    arena_allocator.reuse();
    int64_t buf_len = 0;
    char *buf = nullptr;
    int64_t pos = 0;
    do {
      if (OB_FAIL(MTL(ObTenantCheckpointSlogHandler*)->read_from_disk(
          deleted_tablet_addrs.at(i),
          arena_allocator,
          buf,
          buf_len))) {
        LOG_WARN("fail to read from disk", K(ret), K(deleted_tablet_addrs.at(i)));
      }
    } while (ObTenantStorageCheckpointWriter::ignore_ret(ret));
    if (OB_SUCC(ret)) {
      tablet.set_tablet_addr(deleted_tablet_addrs.at(i));
      if (OB_FAIL(tablet.release_ref_cnt(arena_allocator, buf, buf_len, pos))) {
        LOG_ERROR("fail to decrease macro ref cnt, macro block may leak", K(ret), K(tablet));
      }
    }
  }
  return ret;
}

int ObTenantMetaSnapshotHandler::delete_tablet_snapshot(
    const ObMetaDiskAddr &addr,
    const char *buf,
    const int64_t buf_len,
    ObIArray<ObMetaDiskAddr> &deleted_tablet_addrs)
{
  UNUSED(addr);
  int ret = OB_SUCCESS;
  ObUpdateTabletLog slog;
  int64_t pos = 0;
  if (OB_FAIL(slog.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize update tablet slog", K(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(!slog.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("slog is invalid", K(ret), K(slog));
  } else if (OB_FAIL(deleted_tablet_addrs.push_back(slog.disk_addr_))) {
    LOG_WARN("fail to push back tablet's disk addr", K(ret), K(slog));
  }
  return ret;
}

int ObTenantMetaSnapshotHandler::get_all_tenant_snapshot(ObIArray<ObTenantSnapshotID> &snapshot_ids)
{
  int ret = OB_SUCCESS;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  const ObTenantSuperBlock super_block = tenant->get_super_block();

  if (OB_UNLIKELY(tenant->is_hidden())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't get snapshot from hidden tenant", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < super_block.snapshot_cnt_; i++) {
      const ObTenantSnapshotMeta &snapshot = super_block.tenant_snapshots_[i];
      if (OB_UNLIKELY(!snapshot.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("snapshot meta is invalid", K(ret), K(snapshot));
      } else if (OB_FAIL(snapshot_ids.push_back(snapshot.snapshot_id_))) {
        LOG_WARN("fail to push back to snapshot ids", K(ret), K(snapshot), K(i));
      }
    }
  }
  return ret;
}

int ObTenantMetaSnapshotHandler::get_all_ls_snapshot(
    const ObTenantSnapshotID &snapshot_id,
    ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  const ObTenantSuperBlock super_block = tenant->get_super_block();
  ObTenantSnapshotMeta snapshot;
  ObTenantStorageCheckpointReader ls_ckpt_reader;
  ObSArray<MacroBlockId> meta_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("GetAllLS", MTL_ID()));
  ObTenantStorageCheckpointReader::ObStorageMetaOp push_ls_op = std::bind(
      &ObTenantMetaSnapshotHandler::push_ls_snapshot,
      std::placeholders::_1,
      std::placeholders::_2,
      std::placeholders::_3,
      std::ref(ls_ids));

  if (OB_UNLIKELY(!snapshot_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(snapshot_id));
  } else if (OB_UNLIKELY(tenant->is_hidden())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't get snapshot from hidden tenant", K(ret), K(snapshot_id));
  } else if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("super block is invalid", K(ret), K(super_block), K(snapshot_id));
  } else if (OB_FAIL(super_block.get_snapshot(snapshot_id, snapshot))) {
    LOG_WARN("fail to get snapshot", K(ret), K(snapshot_id));
  } else if (OB_FAIL(ls_ckpt_reader.iter_read_meta_item(snapshot.ls_meta_entry_, push_ls_op, meta_block_list))) {
    LOG_WARN("fail to iter push ls", K(ret), K(snapshot));
  }
  return ret;
}

int ObTenantMetaSnapshotHandler::push_ls_snapshot(
    const ObMetaDiskAddr &addr,
    const char *buf,
    const int64_t buf_len,
    ObIArray<ObLSID> &ls_ids)
{
  UNUSED(addr);
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObLSCkptMember ls_ckpt_member;
  if (OB_FAIL(ls_ckpt_member.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize ls ckpt member", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(ls_ids.push_back(ls_ckpt_member.ls_meta_.ls_id_))) {
    LOG_WARN("fail to push back ls id", K(ret), K(ls_ckpt_member));
  }
  return ret;
}

int ObTenantMetaSnapshotHandler::get_ls_snapshot(
    const ObTenantSnapshotID &snapshot_id,
    const ObLSID &ls_id,
    blocksstable::MacroBlockId &tablet_meta_entry)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotMeta snapshot;
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  const ObTenantSuperBlock super_block = tenant->get_super_block();

  if (OB_UNLIKELY(!snapshot_id.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(snapshot_id), K(ls_id));
  } else if (OB_UNLIKELY(tenant->is_hidden())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't get ls snapshot from hidden tenant", K(ret), K(snapshot_id), K(ls_id));
  } else if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("super block is invalid", K(ret), K(super_block));
  } else if (OB_FAIL(super_block.get_snapshot(snapshot_id, snapshot))) {
    LOG_WARN("fail to get snapshot", K(ret), K(snapshot_id), K(super_block));
  } else if (OB_FAIL(find_tablet_meta_entry(snapshot.ls_meta_entry_, ls_id, tablet_meta_entry))) {
    LOG_WARN("fail to get tablet meta entry", K(ret), K(snapshot), K(ls_id));
  }
  return ret;
}

int ObTenantMetaSnapshotHandler::find_tablet_meta_entry(
    const blocksstable::MacroBlockId &ls_meta_entry,
    const ObLSID &ls_id,
    blocksstable::MacroBlockId &tablet_meta_entry)
{
  int ret = OB_SUCCESS;
  ObLinkedMacroBlockItemReader ls_ckpt_reader;
  ObMemAttr mem_attr(MTL_ID(), "Snapshot");

  if (OB_UNLIKELY(!ls_meta_entry.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_meta_entry), K(ls_id));
  } else if (OB_UNLIKELY(IS_EMPTY_BLOCK_LIST(ls_meta_entry))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("ls snapshot doesn't exist", K(ret), K(ls_meta_entry));
  } else if (OB_FAIL(ls_ckpt_reader.init(ls_meta_entry, mem_attr))) {
    LOG_WARN("fail to init log stream item reader", K(ret), K(ls_meta_entry));
  } else {
    char *item_buf = nullptr;
    int64_t item_buf_len = 0;
    ObLSCkptMember ls_ckpt_member;
    int64_t pos = 0;
    ObMetaDiskAddr addr;
    while (OB_SUCC(ret)) {
      item_buf = nullptr;
      item_buf_len = 0;
      pos = 0;
      ls_ckpt_member.reset();
      if (OB_FAIL(ls_ckpt_reader.get_next_item(item_buf, item_buf_len, addr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next log stream item", K(ret));
        } else {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("can't find target ls snapshot", K(ret));
        }
      } else if (OB_FAIL(ls_ckpt_member.deserialize(item_buf, item_buf_len, pos))) {
        LOG_WARN("fail to deserialize ls ckpt member", K(ret), KP(item_buf), K(item_buf_len), K(pos));
      } else if (ls_ckpt_member.ls_meta_.ls_id_ == ls_id) {
        tablet_meta_entry = ls_ckpt_member.tablet_meta_entry_;
        break;
      }
    }
  }
  return ret;
}

int ObTenantMetaSnapshotHandler::create_all_tablet(observer::ObStartupAccelTaskHandler* startup_accel_handler,
                                                   const blocksstable::MacroBlockId &tablet_meta_entry)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!tablet_meta_entry.is_valid() || IS_EMPTY_BLOCK_LIST(tablet_meta_entry))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tablet_meta_entry));
  }

  if (OB_SUCC(ret)) {
    ObTenantStorageCheckpointReader tablet_snapshot_reader;
    ObSArray<MacroBlockId> meta_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("SnapCreate", MTL_ID()));
    ObSArray<ObUpdateTabletLog> slog_arr;
    slog_arr.set_attr(ObMemAttr(MTL_ID(), "SnapRecovery"));

    ObTenantStorageCheckpointReader::ObStorageMetaOp write_slog_op = std::bind(
        &ObTenantMetaSnapshotHandler::batch_write_slog,
        std::placeholders::_1,
        std::placeholders::_2,
        std::placeholders::_3,
        std::ref(slog_arr));

    if (OB_FAIL(tablet_snapshot_reader.iter_read_meta_item(tablet_meta_entry, write_slog_op, meta_block_list))) {
      LOG_WARN("fail to iter write slog", K(ret), K(tablet_meta_entry));
    } else if (0 != slog_arr.count() && OB_FAIL(do_write_slog(slog_arr))) {
      LOG_WARN("fail to write and report slogs", K(ret), K(slog_arr));
    } else {
      FLOG_INFO("write all tablet slog done");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(MTL(ObTenantCheckpointSlogHandler*)->clone_ls(startup_accel_handler, tablet_meta_entry))) {
      LOG_WARN("fail to clone one ls", K(ret));
    }
  }
  return ret;
}

int ObTenantMetaSnapshotHandler::batch_write_slog(
    const ObMetaDiskAddr &addr,
    const char *buf,
    const int64_t buf_len,
    ObIArray<ObUpdateTabletLog> &slog_arr)
{
  UNUSED(addr);
  int ret = OB_SUCCESS;
  ObUpdateTabletLog slog;
  ObStorageLogParam log_param;
  int64_t pos = 0;

  if (MAX_SLOG_BATCH_NUM <= slog_arr.count()) {
    if (OB_FAIL(do_write_slog(slog_arr))) {
      LOG_WARN("fail to write and report slogs", K(ret), K(slog_arr));
    } else {
      slog_arr.reuse();
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(slog.deserialize(buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize update tablet slog", K(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(!slog.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("slog is invalid", K(ret), K(slog));
  } else if (OB_FAIL(slog_arr.push_back(slog))) {
    LOG_WARN("fail to push back slog entry", K(ret), K(slog));
  }
  return ret;
}

int ObTenantMetaSnapshotHandler::do_write_slog(ObIArray<ObUpdateTabletLog> &slog_arr)
{
  int ret = OB_SUCCESS;
  ObSArray<ObStorageLogParam> param_arr;
  param_arr.set_attr(ObMemAttr(MTL_ID(), "SnapRecovery"));
  ObStorageLogParam log_param;
  log_param.cmd_ = ObIRedoModule::gen_cmd(
      ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE,
      ObRedoLogSubType::OB_REDO_LOG_UPDATE_TABLET);
  if (OB_FAIL(param_arr.reserve(slog_arr.count()))) {
    LOG_WARN("fail to reserve memory for slog param arr", K(ret), K(slog_arr.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < slog_arr.count(); i++) {
    log_param.data_ = &(slog_arr.at(i));
    if (OB_FAIL(param_arr.push_back(log_param))) {
      LOG_WARN("fail to push back slog param", K(ret), K(log_param));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(MTL(ObStorageLogger*)->write_log(param_arr))) {
    LOG_WARN("fail to batch write slog", K(ret), K(param_arr.count()));
  } else if (OB_FAIL(batch_report_slog(param_arr))) {
    LOG_WARN("fail to batch report slog", K(ret), K(param_arr.count()));
  }
  return ret;
}

int ObTenantMetaSnapshotHandler::batch_report_slog(const ObIArray<ObStorageLogParam> &param_arr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_arr.count(); i++) {
    const ObStorageLogParam &log_param = param_arr.at(i);
    const ObUpdateTabletLog *slog = reinterpret_cast<const ObUpdateTabletLog*>(log_param.data_);
    const ObTabletMapKey tablet_key(slog->ls_id_, slog->tablet_id_);
    do {
      if (OB_FAIL(MTL(ObTenantCheckpointSlogHandler*)->report_slog(tablet_key, log_param.disk_addr_))) {
        if (OB_ALLOCATE_MEMORY_FAILED != ret) {
          LOG_WARN("fail to report slog", K(ret), K(tablet_key), K(log_param));
        } else if (REACH_TIME_INTERVAL(1000 * 1000L)) { // 1s
          LOG_WARN("fail to report slog due to memory limit", K(ret), K(tablet_key), K(log_param));
        }
      }
    } while (OB_ALLOCATE_MEMORY_FAILED == ret);
  }
  return ret;
}

}
}
