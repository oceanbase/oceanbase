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
#include "ob_storage_restore_struct.h"
#include "storage/restore/ob_ls_restore_args.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
namespace oceanbase
{
namespace storage
{
/******************ObRestoreBaseInfo*********************/
ObRestoreBaseInfo::ObRestoreBaseInfo()
  : restore_scn_(),
    backup_cluster_version_(0),
    backup_data_version_(0),
    backup_dest_(),
    backup_set_list_()
{
}

void ObRestoreBaseInfo::reset()
{
  restore_scn_.reset();
  backup_cluster_version_ = 0;
  backup_data_version_ = 0;
  backup_dest_.reset();
  backup_set_list_.reset();
}

bool ObRestoreBaseInfo::is_valid() const
{
  return restore_scn_.is_valid()
      && backup_cluster_version_ > 0
      && backup_data_version_ > 0
      && backup_dest_.is_valid()
      && !backup_set_list_.empty();
  //backup piece list can be empty
}

int ObRestoreBaseInfo::assign(const ObRestoreBaseInfo &restore_base_info)
{
  int ret = OB_SUCCESS;
  backup_dest_.reset();
  if (!restore_base_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign restore info get invalid argument", K(ret), K(restore_base_info));
  } else {
    restore_scn_ = restore_base_info.restore_scn_;
    backup_cluster_version_ = restore_base_info.backup_cluster_version_;
    backup_data_version_ = restore_base_info.backup_data_version_;
    if (OB_FAIL(backup_dest_.deep_copy(restore_base_info.backup_dest_))) {
      LOG_WARN("failed to set backup dest", K(ret), K(restore_base_info));
    } else if (OB_FAIL(backup_set_list_.assign(restore_base_info.backup_set_list_))) {
      LOG_WARN("failed to assign backup set list", K(ret), K(restore_base_info));
    }
  }
  return ret;
}

int ObRestoreBaseInfo::copy_from(const ObTenantRestoreCtx &restore_arg)
{
  int ret = OB_SUCCESS;
  int idx = -1;
  if (!restore_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("restore_arg get invalid argument", K(ret), K(restore_arg));
  } else {
    idx = restore_arg.get_backup_set_list().count() - 1;
    restore_scn_ = restore_arg.get_restore_scn();
    backup_cluster_version_ = restore_arg.get_backup_cluster_version();
    backup_data_version_ = restore_arg.get_backup_data_version();
    backup_dest_.reset();
    backup_set_list_.reset();
    if (OB_FAIL(backup_dest_.set(restore_arg.get_backup_set_list().at(idx).backup_set_path_))) {
      LOG_WARN("fail to get backup set dest", K(ret));
    } else if (OB_FAIL(backup_set_list_.assign(restore_arg.get_backup_set_list()))) {
      LOG_WARN("failed to assign backup set list", K(ret), K(restore_arg));
    }
  }
  return ret;
}

int ObRestoreBaseInfo::get_restore_backup_set_dest(const int64_t backup_set_id,
    share::ObRestoreBackupSetBriefInfo &backup_set_dest) const
{
  int ret = OB_SUCCESS;
  bool find_target = false;
  backup_set_dest.reset();
  ARRAY_FOREACH_X(backup_set_list_, i, cnt, OB_SUCC(ret)) {
    if (backup_set_id == backup_set_list_.at(i).backup_set_desc_.backup_set_id_) {
      if (OB_FAIL(backup_set_dest.assign(backup_set_list_.at(i)))) {
        LOG_WARN("fail to assign", K(ret), K(backup_set_list_));
      } else {
        find_target = true;
        break;
      }
    }
  }
  if (OB_SUCC(ret) && !find_target) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    LOG_WARN("backup set not exist", K(ret), K(backup_set_id), K(backup_set_list_));
  }
  return ret;
}

/******************ObTabletRestoreAction*********************/

const char *ObTabletRestoreAction::get_action_str(const ACTION &action)
{
  const char *str = "UNKNOWN";
  const char *action_strs[] = {
      "RESTORE_ALL",
      "RESTORE_TABLET_META",
      "RESTORE_MINOR",
      "RESTORE_MAJOR",
      "RESTORE_NONE",
  };
  STATIC_ASSERT(MAX == ARRAYSIZEOF(action_strs), "action count mismatch");
  if (action < 0 || action >= MAX) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid action", K(action));
  } else {
    str = action_strs[action];
  }
  return str;
}

bool ObTabletRestoreAction::is_valid(const ACTION &action)
{
  return action >= ACTION::RESTORE_ALL && action < ACTION::MAX;
}

bool ObTabletRestoreAction::is_restore_minor(const ACTION &action)
{
  bool bool_ret = false;
  if (!is_valid(action)) {
    bool_ret = false;
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "restore action is unexpected", K(action));
  } else if (ACTION::RESTORE_MINOR != action) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

bool ObTabletRestoreAction::is_restore_major(const ACTION &action)
{
  bool bool_ret = false;
  if (!is_valid(action)) {
    bool_ret = false;
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "restore action is unexpected", K(action));
  } else if (ACTION::RESTORE_MAJOR != action) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

bool ObTabletRestoreAction::is_restore_none(const ACTION &action)
{
  bool bool_ret = false;
  if (!is_valid(action)) {
    bool_ret = false;
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "restore action is unexpected", K(action));
  } else if (ACTION::RESTORE_NONE != action) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

bool ObTabletRestoreAction::is_restore_all(const ACTION &action)
{
  bool bool_ret = false;
  if (!is_valid(action)) {
    bool_ret = false;
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "restore action is unexpected", K(action));
  } else if (ACTION::RESTORE_ALL != action) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

bool ObTabletRestoreAction::is_restore_tablet_meta(const ACTION &action)
{
  bool bool_ret = false;
  if (!is_valid(action)) {
    bool_ret = false;
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "restore action is unexpected", K(action));
  } else if (ACTION::RESTORE_TABLET_META != action) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

int ObTabletRestoreAction::trans_restore_action_to_restore_status(
    const ACTION &action, ObTabletRestoreStatus::STATUS &status)
{
  int ret = OB_SUCCESS;
  status = ObTabletRestoreStatus::RESTORE_STATUS_MAX;
  if (!is_valid(action)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans restore action to restore status get invalid argument", K(ret), K(action));
  } else if (is_restore_all(action)) {
    status = ObTabletRestoreStatus::FULL;
  } else if (is_restore_minor(action)) {
    status = ObTabletRestoreStatus::MINOR_AND_MAJOR_META;
  } else if (is_restore_major(action)) {
    status = ObTabletRestoreStatus::FULL;
  } else if (is_restore_tablet_meta(action)) {
    status = ObTabletRestoreStatus::EMPTY;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not trans restore action to restore status", K(ret), K(action), K(status));
  }
  return ret;
}

/******************ObRestoreUtils*********************/

int ObRestoreUtils::get_backup_data_type(
    const ObITable::TableKey &table_key,
    share::ObBackupDataType &data_type)
{
  int ret = OB_SUCCESS;
  if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup data type get invalid argument", K(ret), K(table_key));
  } else if (table_key.get_tablet_id().is_ls_inner_tablet()) {
    data_type.set_sys_data_backup();
  } else if (table_key.is_minor_sstable()) {
    data_type.set_minor_data_backup();
  } else if (table_key.is_major_sstable()) {
    data_type.set_major_data_backup();
  } else if (table_key.is_ddl_dump_sstable()) {
    data_type.set_minor_data_backup();
  } else if (table_key.is_mds_sstable()) {
    data_type.set_minor_data_backup();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table key", K(ret), K(table_key));
  }
  return ret;
}

/******************ObTabletGroupRestoreArg*********************/
ObTabletGroupRestoreArg::ObTabletGroupRestoreArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    is_leader_(false),
    tablet_id_array_(),
    src_(),
    dst_(),
    restore_base_info_(),
    action_(ObTabletRestoreAction::MAX)
{
}

void ObTabletGroupRestoreArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  is_leader_ = false;
  tablet_id_array_.reset();
  src_.reset();
  dst_.reset();
  restore_base_info_.reset();
  action_ = ObTabletRestoreAction::MAX;
}

bool ObTabletGroupRestoreArg::is_valid() const
{
  bool bool_ret = false;
  bool_ret = tenant_id_ != OB_INVALID_ID
      && ls_id_.is_valid()
      && !tablet_id_array_.empty()
      && ObTabletRestoreAction::is_valid(action_);
  if (bool_ret) {
    if (!is_leader_ && (!src_.is_valid() || !dst_.is_valid())) {
      bool_ret = false;
    } else {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int ObTabletGroupRestoreArg::assign(const ObTabletGroupRestoreArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign restore tablet group arg get invalid argument", K(ret), K(arg));
  } else {
    tenant_id_ = arg.tenant_id_;
    ls_id_ = arg.ls_id_;
    is_leader_ = arg.is_leader_;
    src_ = arg.src_;
    dst_ = arg.dst_;
    action_ = arg.action_;
    if (OB_FAIL(tablet_id_array_.assign(arg.tablet_id_array_))) {
      LOG_WARN("failed to assign tablet id array", K(ret), K(arg));
    } else if (OB_FAIL(restore_base_info_.assign(arg.restore_base_info_))) {
      LOG_WARN("failed to assign restore base info", K(ret), K(arg));
    }
  }
  return ret;
}

/******************ObLSRestoreArg*********************/
ObLSRestoreArg::ObLSRestoreArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    is_leader_(false),
    src_(),
    dst_(),
    restore_base_info_()
{
}

void ObLSRestoreArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  is_leader_ = false;
  src_.reset();
  dst_.reset();
  restore_base_info_.reset();
}

bool ObLSRestoreArg::is_valid() const
{
  bool bool_ret = false;
  bool_ret = tenant_id_ != OB_INVALID_ID
      && ls_id_.is_valid();
  if (bool_ret) {
    if (!is_leader_ && (!src_.is_valid() || !dst_.is_valid())) {
      bool_ret = false;
    } else {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int ObLSRestoreArg::assign(const ObLSRestoreArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign restore tablet group arg get invalid argument", K(ret), K(arg));
  } else {
    tenant_id_ = arg.tenant_id_;
    ls_id_ = arg.ls_id_;
    is_leader_ = arg.is_leader_;
    src_ = arg.src_;
    dst_ = arg.dst_;
    if (OB_FAIL(restore_base_info_.assign(arg.restore_base_info_))) {
      LOG_WARN("failed to assign restore base info", K(ret), K(arg));
    }
  }
  return ret;
}

/******************ObIRestoreDagNetCtx*********************/
ObIRestoreDagNetCtx::ObIRestoreDagNetCtx()
  : task_id_(),
    src_(),
    start_ts_(0),
    finish_ts_(0)
{
}

ObIRestoreDagNetCtx::~ObIRestoreDagNetCtx()
{
}

/******************ObRestoreMacroBlockId*********************/
ObRestoreMacroBlockId::ObRestoreMacroBlockId()
  : logic_block_id_(),
    backup_physic_block_id_()
{
}

void ObRestoreMacroBlockId::reset()
{
  logic_block_id_.reset();
  backup_physic_block_id_.reset();
}

bool ObRestoreMacroBlockId::is_valid() const
{
  return logic_block_id_.is_valid() && backup_physic_block_id_.is_valid();
}

/******************ObRestoreMacroBlockIdMgr*********************/
ObRestoreMacroBlockIdMgr::ObRestoreMacroBlockIdMgr()
  : is_inited_(false),
    table_key_(),
    block_id_array_()
{
}

ObRestoreMacroBlockIdMgr::~ObRestoreMacroBlockIdMgr()
{
}

int ObRestoreMacroBlockIdMgr::init(
    const common::ObTabletID &tablet_id,
    const ObITable::TableKey &table_key,
    const ObRestoreBaseInfo &restore_base_info,
    backup::ObBackupMetaIndexStoreWrapper &meta_index_store,
    backup::ObBackupMetaIndexStoreWrapper &second_meta_index_store)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("restore macro block id mgr init tiwce", K(ret));
  } else if (!tablet_id.is_valid() || !table_key.is_valid() || !restore_base_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("restore macro block id mgr init get invalid argument", K(ret), K(tablet_id),
        K(table_key), K(restore_base_info));
  } else if (OB_FAIL(inner_init_(tablet_id, table_key, restore_base_info, meta_index_store, second_meta_index_store))) {
    LOG_WARN("failed to inner init restore macro block id mar", K(ret), K(tablet_id), K(table_key));
  } else {
    table_key_ = table_key;
    is_inited_ = true;
  }
  return ret;
}

int ObRestoreMacroBlockIdMgr::inner_init_(
    const common::ObTabletID &tablet_id,
    const ObITable::TableKey &table_key,
    const ObRestoreBaseInfo &restore_base_info,
    backup::ObBackupMetaIndexStoreWrapper &meta_index_store,
    backup::ObBackupMetaIndexStoreWrapper &second_meta_index_store)
{
  int ret = OB_SUCCESS;
  backup::ObBackupMetaIndex sstable_meta_index;
  backup::ObBackupMetaIndex second_meta_index;
  const backup::ObBackupMetaType meta_type =
      backup::ObBackupMetaType::BACKUP_MACRO_BLOCK_ID_MAPPING_META;
  share::ObBackupDataType data_type;
  bool found = false;
  ObRestoreMacroBlockId block_id;
  share::ObBackupStorageInfo storage_info;
  ObArray<backup::ObBackupSSTableMeta> backup_sstable_meta_array;

  if (OB_FAIL(ObRestoreUtils::get_backup_data_type(table_key, data_type))) {
    LOG_WARN("failed to get backup data type", K(ret), K(table_key));
  } else if (OB_FAIL(meta_index_store.get_backup_meta_index(data_type, tablet_id, backup::ObBackupMetaType::BACKUP_SSTABLE_META, sstable_meta_index))) {
      LOG_WARN("failed to get backup meta index", K(ret), K(tablet_id), K(restore_base_info));
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
  } else if (OB_FAIL(second_meta_index_store.get_backup_meta_index(data_type, tablet_id, meta_type, second_meta_index))) {
    LOG_WARN("failed to get backup meta index", K(ret), K(tablet_id), K(table_key), K(meta_type));
  } else {
    SMART_VARS_3((share::ObBackupPath, sstable_meta_backup_path), (share::ObBackupPath, second_meta_backup_path), (backup::ObBackupMacroBlockIDMappingsMeta, macro_block_id_map)) {
      if (OB_FAIL(share::ObBackupPathUtil::get_macro_block_backup_path(restore_base_info.backup_dest_,
          sstable_meta_index.ls_id_, data_type, sstable_meta_index.turn_id_, sstable_meta_index.retry_id_, sstable_meta_index.file_id_, sstable_meta_backup_path))) {
        LOG_WARN("failed to get sstable meta index", K(ret), K(table_key), K(tablet_id), K(restore_base_info), K(sstable_meta_index));
      } else if (OB_FAIL(share::ObBackupPathUtil::get_macro_block_backup_path(restore_base_info.backup_dest_,
          second_meta_index.ls_id_, data_type, second_meta_index.turn_id_, second_meta_index.retry_id_, second_meta_index.file_id_, second_meta_backup_path))) {
        LOG_WARN("failed to get macro block index", K(ret), K(table_key), K(tablet_id), K(restore_base_info), K(second_meta_index));
      } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_sstable_metas(sstable_meta_backup_path.get_obstr(),
          restore_base_info.backup_dest_.get_storage_info(), sstable_meta_index, backup_sstable_meta_array))) {
        LOG_WARN("failed to read sstable metas", K(ret), K(table_key), K(tablet_id), K(restore_base_info));
      } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_macro_block_id_mapping_metas(second_meta_backup_path.get_obstr(),
          restore_base_info.backup_dest_.get_storage_info(), second_meta_index, macro_block_id_map))) {
        LOG_WARN("failed to read macro block data", K(ret), K(table_key), K(tablet_id), K(restore_base_info));
      } else {
        backup::ObBackupSSTableMeta *backup_sstable_meta = nullptr;
        for (int64_t i = 0; OB_SUCC(ret) && i < backup_sstable_meta_array.count(); ++i) {
          if (table_key == backup_sstable_meta_array.at(i).sstable_meta_.table_key_) {
            backup_sstable_meta = &backup_sstable_meta_array.at(i);
            break;
          }
        }

        if (OB_ISNULL(backup_sstable_meta)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not found the backup sstable meta from meta array", K(ret), K(table_key));
        }

        for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_id_map.sstable_count_; ++i) {
          const backup::ObBackupMacroBlockIDMapping *id_mapping = macro_block_id_map.id_map_list_[i];
          if (OB_ISNULL(id_mapping)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("id mapping should not be null", K(ret), K(i));
          } else if (table_key == id_mapping->table_key_ ) {
            for (int64_t j = 0; OB_SUCC(ret) && j < id_mapping->id_pair_list_.count(); ++j) {
              const backup::ObBackupMacroBlockIDPair &pair = id_mapping->id_pair_list_.at(j);
              block_id.reset();
              block_id.logic_block_id_ = pair.logic_id_;
              block_id.backup_physic_block_id_ = pair.physical_id_;
              if (OB_FAIL(block_id_array_.push_back(block_id))) {
                LOG_WARN("failed to push block id into array", K(ret), K(block_id));
              } else {
                LOG_DEBUG("push back block id", K(tablet_id), K(table_key), K(pair), K(block_id));
              }
            }

            if (OB_SUCC(ret)) {
              found = true;
              if (OB_FAIL(sort_block_id_array(backup_sstable_meta->logic_id_list_))) {
                LOG_WARN("failed to sort block id array", K(ret));
              }
              break;
            }
          }
        }

        if (OB_SUCC(ret) && !found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not find table key from meta", K(ret), K(table_key));
        }
      }
    }
  }
  return ret;
}

// TODO() remove relate logic later.
int ObRestoreMacroBlockIdMgr::sort_block_id_array(
    common::ObIArray<blocksstable::ObLogicMacroBlockId> &logic_id_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(block_id_array_.count() != logic_id_list.count())) {
    ret = OB_ERR_UNEXPECTED;
  }

  ObSEArray<ObRestoreMacroBlockId, 16> tmp_sort_array;
  for (int64_t i = 0; OB_SUCC(ret) && i < logic_id_list.count(); ++i) {
    const blocksstable::ObLogicMacroBlockId &cur_id = logic_id_list.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < block_id_array_.count(); ++j) {
      if (cur_id != block_id_array_.at(j).logic_block_id_) {
        continue;
      } else if (OB_FAIL(tmp_sort_array.push_back(block_id_array_.at(j)))) {
        LOG_WARN("failed to add block id", K(ret), K(j));
      } else {
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    block_id_array_.reuse();
    if (OB_FAIL(block_id_array_.assign(tmp_sort_array))) {
      LOG_WARN("failed to assign block id array", K(ret), K(tmp_sort_array));
    }
  }
  return ret;
}

int ObRestoreMacroBlockIdMgr::get_macro_block_id(
    const int64_t block_id_index,
    blocksstable::ObLogicMacroBlockId &logic_block_id,
    backup::ObBackupPhysicalID &physic_block_id)
{
  int ret = OB_SUCCESS;
  logic_block_id.reset();
  physic_block_id.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore macro block id mgr do not init", K(ret));
  } else if (block_id_index < 0 || block_id_index >= block_id_array_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get physical id get invalid argument", K(ret), K(block_id_index), K(block_id_array_));
  } else {
    logic_block_id = block_id_array_.at(block_id_index).logic_block_id_;
    physic_block_id = block_id_array_.at(block_id_index).backup_physic_block_id_;
  }
  return ret;
}

int ObRestoreMacroBlockIdMgr::get_block_id_index(
    const blocksstable::ObLogicMacroBlockId &logic_block_id,
    int64_t &block_id_index)
{
  int ret = OB_SUCCESS;
  block_id_index = 0;
  bool found = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore macro block id mgr do not init", K(ret));
  } else if (!logic_block_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get block id index get invalid argument", K(ret), K(logic_block_id));
  } else {
    for (int64_t i = 0; i < block_id_array_.count(); ++i) {
      const ObRestoreMacroBlockId &restore_block_id = block_id_array_.at(i);
      if (logic_block_id == restore_block_id.logic_block_id_) {
        block_id_index = i;
        found = true;
        break;
      }
    }

    if (!found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("logic block id is not exist, unexpected error",
          K(ret), K(logic_block_id), K(block_id_array_));
    }
  }
  return ret;
}

int ObRestoreMacroBlockIdMgr::get_restore_macro_block_id_array(
    common::ObIArray<ObRestoreMacroBlockId> &block_id_array)
{
  int ret = OB_SUCCESS;
  block_id_array.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore macro block id mgr do not init", K(ret));
  } else if (OB_FAIL(block_id_array.assign(block_id_array_))) {
    LOG_WARN("failed to assign block id array", K(ret), K(table_key_));
  }
  return ret;
}

}
}
