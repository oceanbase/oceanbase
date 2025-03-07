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

#include "ob_tenant_storage_meta_replayer.h"
#include "storage/meta_store/ob_storage_meta_io_util.h"
#include "storage/tx_storage/ob_ls_service.h"
#ifdef OB_BUILD_SHARED_STORAGE
#endif

namespace oceanbase
{
using namespace omt;
namespace storage
{
int ObTenantStorageMetaReplayer::init(
    const bool is_shared_storage,
    ObTenantStorageMetaPersister &persister,
    ObTenantCheckpointSlogHandler &ckpt_slog_handler)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", K(ret));
  } else {
    is_shared_storage_ = is_shared_storage;
    persister_ = &persister;
    ckpt_slog_handler_ = &ckpt_slog_handler;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantStorageMetaReplayer::start_replay(const ObTenantSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant super block invalid", K(ret), K(super_block));
  } else if (!is_shared_storage_) {
    if (OB_FAIL(ckpt_slog_handler_->start_replay(super_block))) {
      LOG_WARN("fail to start replay", K(ret));
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ss_start_replay_(super_block))) {
      LOG_WARN("fail to start replay", K(ret));
    }
#endif
  }
  return ret;
}

void ObTenantStorageMetaReplayer::destroy()
{
  is_shared_storage_ = false;
  persister_ = nullptr;
  ckpt_slog_handler_ = nullptr;
  is_inited_ = false;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObTenantStorageMetaReplayer::ss_start_replay_(const ObTenantSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("TntMetaReplay", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

  for (int64_t i = 0; OB_SUCC(ret) && i < super_block.ls_cnt_; i++) {
    const ObLSItem &item = super_block.ls_item_arr_[i];
    if (OB_FAIL(ss_replay_create_ls_(allocator, item))) {
      LOG_WARN("fail to replay create ls", K(ret), K(item));
    } else if (OB_FAIL(ss_recover_ls_pending_free_list_(allocator, item))) {
      LOG_WARN("fail to recover pending_free", );
    } else if (OB_FAIL(ss_replay_ls_dup_table_meta_(allocator, item))) {
      LOG_WARN("fail to replay ls dup table meta", K(ret), K(item));
    } else if (OB_FAIL(ss_replay_ls_tablets_for_trans_info_tmp_(allocator, item))) {
      LOG_WARN("fail to replay ls tablets", K(ret), K(item));
    }
    allocator.reuse();
  }
  return ret;
}

int ObTenantStorageMetaReplayer::ss_recover_ls_pending_free_list_(
    ObArenaAllocator &allocator, const ObLSItem &item)
{
  int ret = OB_SUCCESS;

  ObStorageObjectOpt deleting_opt;
  bool is_deleting_tablets_exist = false;
  deleting_opt.set_ss_ls_level_meta_object_opt(ObStorageObjectType::LS_PENDING_FREE_TABLET_ARRAY, item.ls_id_.id());

  if (OB_FAIL(ObStorageMetaIOUtil::check_meta_existence(deleting_opt, item.epoch_, is_deleting_tablets_exist))) {
    LOG_WARN("fail to check meta existence", K(ret), K(deleting_opt), K(item));
  } else if (is_deleting_tablets_exist) {
    if (OB_FAIL(TENANT_STORAGE_META_PERSISTER.ss_replay_ls_pending_free_arr(allocator, item.ls_id_, item.epoch_))) {
      LOG_WARN("fail to replay ls_pending_free_tablet_arr", K(ret), K(item));
    }
  }

  return ret;
}

int ObTenantStorageMetaReplayer::ss_replay_create_ls_(
    ObArenaAllocator &allocator, const ObLSItem &item)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  opt.set_ss_ls_level_meta_object_opt(ObStorageObjectType::LS_META, item.ls_id_.id());
  ObLSMeta ls_meta;

  switch(item.status_) {
    case ObLSItemStatus::CREATED: {
      if (OB_FAIL(ObStorageMetaIOUtil::read_storage_meta_object(
          opt, allocator, MTL_ID(), item.epoch_, ls_meta))) {
        LOG_WARN("fail to read ls meta", K(ret), K(item));
      } else if (OB_FAIL(MTL(ObLSService *)->replay_create_ls(item.epoch_, ls_meta))) {
        LOG_WARN("fail to replay create ls", K(ret), K(ls_meta));
      } else if (OB_FAIL(MTL(ObLSService *)->replay_create_ls_commit(item.ls_id_))) {
        LOG_WARN("fail to replay create ls commit", K(ret), K(item));
      } else {
        LOG_INFO("successfully replay create ls commit", K(ls_meta));
      }
      break;
    }
    case ObLSItemStatus::CREATING: {
      bool is_ls_meta_exist = false;
      if (OB_FAIL(ObStorageMetaIOUtil::check_meta_existence(opt, item.epoch_, is_ls_meta_exist))) {
        LOG_WARN("fail to check existence", K(ret), K(opt));
      } else if (!is_ls_meta_exist) {
        if (OB_FAIL(persister_->abort_create_ls(item.ls_id_, item.epoch_))) {
          LOG_ERROR("fail to abort creat ls", K(ret), K(item));
        } else {
          LOG_INFO("abort create ls when replay", K(ret), K(item));
        }
      } else if (OB_FAIL(ObStorageMetaIOUtil::read_storage_meta_object(
          opt, allocator, MTL_ID(), item.epoch_, ls_meta))) {
      } else if (OB_FAIL(MTL(ObLSService *)->replay_create_ls(item.epoch_, ls_meta))) {
        LOG_WARN("fail to replay create ls", K(ret), K(ls_meta));
      } else {
        LOG_INFO("replay a creating ls", K(ret), K(item));
      }
      break;
    }
    case ObLSItemStatus::CREATE_ABORT:
    case ObLSItemStatus::DELETED: {
      LOG_INFO("skip invalid ls item", K(item));
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknow item status", K(item));
    }
  }
  return ret;
}

int ObTenantStorageMetaReplayer::ss_replay_ls_dup_table_meta_(
    ObArenaAllocator &allocator, const ObLSItem &item)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  opt.set_ss_ls_level_meta_object_opt(ObStorageObjectType::LS_DUP_TABLE_META, item.ls_id_.id());
  bool is_dup_table_meta_exist = false;

  if (item.status_ != ObLSItemStatus::CREATED) {
    // ls is not create commit, skip repaly dup table meta
  } else if (OB_FAIL(ObStorageMetaIOUtil::check_meta_existence(opt, item.epoch_, is_dup_table_meta_exist))) {
     LOG_WARN("fail to check existence", K(ret), K(opt));
  } else if (!is_dup_table_meta_exist) {
    // do nothing
  } else {
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    transaction::ObDupTableLSCheckpoint::ObLSDupTableMeta ls_dup_table_meta;
    if (OB_FAIL(ObStorageMetaIOUtil::read_storage_meta_object(
        opt, allocator, MTL_ID(), item.epoch_, ls_dup_table_meta))) {
      LOG_WARN("fail to read ls dup table meta", K(item));
    } else if (OB_FAIL(MTL(ObLSService *)->get_ls(
        ls_dup_table_meta.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get ls", K(ret), K(ls_dup_table_meta));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ls_dup_table_meta));
    } else if (OB_FAIL(ls->set_dup_table_ls_meta(ls_dup_table_meta))) {
      LOG_WARN("fail to set ls dup table meta", K(ret), K(item), K(ls_dup_table_meta));
    } else {
      LOG_INFO("successfully replay ls dup table meta", K(ret), K(item));
    }
  }
  return ret;
}

int ObTenantStorageMetaReplayer::ss_replay_ls_tablets_(
    ObArenaAllocator &allocator, const ObLSItem &item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid item", K(ret), K(item));
  } else if (ObLSItemStatus::CREATED == item.status_) {
    ObStorageObjectOpt active_opt;
    active_opt.set_ss_ls_level_meta_object_opt(ObStorageObjectType::LS_ACTIVE_TABLET_ARRAY, item.ls_id_.id());
    bool is_active_tablets_exist = false;
    ObLSActiveTabletArray active_tablets;
    ObArray<ObPendingFreeTabletItem> deleting_tablets;
    ObLSTabletService *ls_tablet_svr;
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    const ObLSID &ls_id = item.ls_id_;

    // 1. load active_tablet_arr and pending_free_tablet_arr;
    if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get ls handle", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls_tablet_svr = ls->get_tablet_svr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet service is null", K(ret), K(ls_id));
    } else if (OB_FAIL(ObStorageMetaIOUtil::check_meta_existence(active_opt, item.epoch_, is_active_tablets_exist))) {
      LOG_WARN("fail to check meta existence", K(ret), K(active_opt), K(item));
    } else if (is_active_tablets_exist && OB_FAIL(ObStorageMetaIOUtil::read_storage_meta_object(
        active_opt, allocator, MTL_ID(), item.epoch_, active_tablets))) {
      LOG_WARN("fail to get active tablets", K(ret), K(active_opt), K(item));
    } else if (OB_FAIL(TENANT_STORAGE_META_PERSISTER.get_items_from_pending_free_tablet_array(ls_id, item.epoch_, deleting_tablets))) {
      LOG_WARN("failed to get_items_from_pending_free_tablet_array", K(ret), K(item));
    }

    // 2. check and delete the un-deleted current_version file for tablets recorded in pending_free_tablet_arr
    for (int64_t i = 0; OB_SUCC(ret) && i < deleting_tablets.count(); i++) {
      const ObPendingFreeTabletItem &deleting_item = deleting_tablets.at(i);
      if (OB_FAIL(TENANT_STORAGE_META_PERSISTER.ss_check_and_delete_tablet_current_version(deleting_item.tablet_id_,
                                                                                           ls->get_ls_id(),
                                                                                           ls->get_ls_epoch(),
                                                                                           deleting_item.tablet_meta_version_,
                                                                                           deleting_item.tablet_transfer_seq_,
                                                                                           allocator))) {
        LOG_WARN("failed to check and delete the current_version file of the tablet", K(ret), K(deleting_item), KPC(ls));
      }
    }

    // 3. replay create tablets recorded in active_tablets (except had deleted in pending_free_arr);
    //    Even though in different transfer_seq, the tablet_version is unique.
    //    Therefore the compare is safe.
    for (int64_t i = 0; OB_SUCC(ret) && i < active_tablets.items_.count(); i++) {
      const ObActiveTabletItem &active_item = active_tablets.items_.at(i);
      bool has_deleted = false;
      for (int64_t j = 0; !has_deleted && OB_SUCC(ret) && j < deleting_tablets.count(); j++) {
        const ObPendingFreeTabletItem &deleting_item = deleting_tablets.at(j);
        if (active_item.tablet_id_ == deleting_item.tablet_id_ && active_item.get_transfer_seq() == deleting_item.tablet_transfer_seq_) {
          if (active_item.get_tablet_meta_version() <= deleting_item.tablet_meta_version_) {
            has_deleted = true;
          }
        }
      }

      if (!has_deleted) {
        ObMetaDiskAddr inaccurate_addr;
        blocksstable::MacroBlockId object_id;
        ObStorageObjectOpt opt;
        const int64_t object_size = OB_DEFAULT_MACRO_BLOCK_SIZE;
        opt.set_ss_private_tablet_meta_object_opt(ls_id.id(), active_item.tablet_id_.id(), ObStorageObjectOpt::INVALID_TABLET_VERSION, ObStorageObjectOpt::INVALID_TABLET_TRANSFER_SEQ);
        if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, object_id))) {
          LOG_WARN("fail to get object id", K(ret), K(opt));
        } else if (OB_FAIL(inaccurate_addr.set_block_addr(object_id, 0/*offset*/, object_size, ObMetaDiskAddr::DiskType::RAW_BLOCK))) {
          LOG_WARN("fail to set initial tablet meta addr", K(ret), K(inaccurate_addr));
        } else if (OB_FAIL(ls_tablet_svr->ss_replay_create_tablet(inaccurate_addr, active_item.tablet_id_))) {
          LOG_WARN("fail to replay create tablet", K(ret), K(active_item), K(inaccurate_addr));
        }
      }
    }
  } else {
    LOG_INFO("item status need not replay", K(ret), K(item));
  }

  return ret;
}


int ObTenantStorageMetaReplayer::ss_replay_ls_tablets_for_trans_info_tmp_(
    ObArenaAllocator &allocator, const ObLSItem &item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid item", K(ret), K(item));
  } else if (ObLSItemStatus::CREATED == item.status_) {
    ObSArray<int64_t> ls_tablets;
    ObArray<ObPendingFreeTabletItem> deleting_tablets;
    ObLSTabletService *ls_tablet_svr;
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    const ObLSID &ls_id = item.ls_id_;

    ObTenantFileManager *file_manager = nullptr;

    // 1. list all tablet and load pending_free_tablet_arr
    if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get ls handle", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls_tablet_svr = ls->get_tablet_svr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet service is null", K(ret), K(ls_id));
    } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to check meta existence", K(ret), K(item));
    } else if (OB_FAIL(file_manager->list_tablet_meta_dir(item.ls_id_.id(), item.epoch_, ls_tablets))) {
      LOG_WARN("failed to list all tablets under ls", K(ret), K(MTL_ID()), K(item), K(ls_tablets));
    } else if (OB_FAIL(TENANT_STORAGE_META_PERSISTER.get_items_from_pending_free_tablet_array(ls_id, item.epoch_, deleting_tablets))) {
      LOG_WARN("failed to get_items_from_pending_free_tablet_array", K(ret), K(item.epoch_));
    }

    // 2. check and delete the un-deleted current_version file for tablets recorded in pending_free_tablet_arr
    for (int64_t i = 0; OB_SUCC(ret) && i < deleting_tablets.count(); i++) {
      const ObPendingFreeTabletItem &deleting_item = deleting_tablets.at(i);
      if (OB_FAIL(TENANT_STORAGE_META_PERSISTER.ss_check_and_delete_tablet_current_version(deleting_item.tablet_id_,
                                                                                           ls->get_ls_id(),
                                                                                           ls->get_ls_epoch(),
                                                                                           deleting_item.tablet_meta_version_,
                                                                                           deleting_item.tablet_transfer_seq_,
                                                                                           allocator))) {
        LOG_WARN("failed to check and delete the current_version file of the tablet", K(ret), K(deleting_item), KPC(ls));
      }
    }

    // 3. replay each tablet;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablets.count(); i++) {
      const uint64_t &tablet_id = ls_tablets.at(i);
      int64_t deleted_tablet_meta_version = ObStorageObjectOpt::INVALID_TABLET_VERSION;
      for (int64_t j = 0; OB_SUCC(ret) && j < deleting_tablets.count(); j++) {
        const ObPendingFreeTabletItem &deleting_item = deleting_tablets.at(j);
        if (tablet_id == deleting_item.tablet_id_.id()) {
            deleted_tablet_meta_version = deleting_item.tablet_meta_version_;
            break;
        }
      }

      ObPrivateTabletCurrentVersion latest_addr;
      ObStorageObjectOpt current_version_opt;
      current_version_opt.set_ss_private_tablet_meta_current_verison_object_opt(item.ls_id_.id(), tablet_id);

      if (OB_FAIL(ObStorageMetaIOUtil::read_storage_meta_object(current_version_opt, allocator, MTL_ID(), item.epoch_, latest_addr))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("this tablet has been deleted and current_version has been deleted", K(ret), K(item), K(current_version_opt));
        } else {
          LOG_WARN("fail to read cur version", K(ret), K(item), K(current_version_opt));
        }
      } else if (ObStorageObjectOpt::INVALID_TABLET_VERSION != deleted_tablet_meta_version
              && latest_addr.tablet_addr_.block_id().meta_version_id() <= deleted_tablet_meta_version) {
        ret = OB_ERR_UNEXPECTED;
        LOG_INFO("this tablet has been deleted, but current_version has not been deleted", K(ret), K(item), K(current_version_opt), K(latest_addr), K(deleted_tablet_meta_version));
      } else if (OB_FAIL(ls_tablet_svr->ss_replay_create_tablet_for_trans_info_tmp(latest_addr.tablet_addr_, ls_handle, ObTabletID(tablet_id)))) {
        LOG_WARN("fail to replay create tablet", K(ret), K(tablet_id), K(latest_addr.tablet_addr_));
      }
    }
  } else {
    LOG_INFO("item status need not replay", K(ret), K(item));
  }

  return ret;
}


#endif


} // namespace storage
} // namespace oceanbase
