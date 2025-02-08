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
#include "share/backup/ob_backup_connectivity.h"
#include "storage/backup/ob_backup_factory.h"
#include "storage/backup/ob_backup_meta_cache.h"
namespace oceanbase
{
namespace storage
{
/******************ObRestoreBaseInfo*********************/
ObRestoreBaseInfo::ObRestoreBaseInfo()
  : job_id_(0),
    restore_scn_(),
    backup_cluster_version_(0),
    backup_data_version_(0),
    backup_compatible_(ObBackupSetFileDesc::MAX_COMPATIBLE_VERSION),
    backup_dest_(),
    backup_set_list_()
{
}

void ObRestoreBaseInfo::reset()
{
  job_id_ = 0;
  restore_scn_.reset();
  backup_cluster_version_ = 0;
  backup_data_version_ = 0;
  backup_compatible_ = ObBackupSetFileDesc::MAX_COMPATIBLE_VERSION;
  backup_dest_.reset();
  backup_set_list_.reset();
}

bool ObRestoreBaseInfo::is_valid() const
{
  return job_id_ > 0
      && restore_scn_.is_valid()
      && backup_cluster_version_ > 0
      && backup_data_version_ > 0
      && backup_dest_.is_valid()
      && !backup_set_list_.empty()
      && backup_compatible_ >= ObBackupSetFileDesc::COMPATIBLE_VERSION_1
      && backup_compatible_ < ObBackupSetFileDesc::MAX_COMPATIBLE_VERSION;
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
    job_id_ = restore_base_info.job_id_;
    restore_scn_ = restore_base_info.restore_scn_;
    backup_cluster_version_ = restore_base_info.backup_cluster_version_;
    backup_data_version_ = restore_base_info.backup_data_version_;
    backup_compatible_ = restore_base_info.backup_compatible_;
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
    job_id_ = restore_arg.get_job_id();
    restore_scn_ = restore_arg.get_restore_scn();
    backup_cluster_version_ = restore_arg.get_backup_cluster_version();
    backup_data_version_ = restore_arg.get_backup_data_version();
    backup_compatible_ = restore_arg.backup_compatible_;
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
    ret = OB_OBJECT_NOT_EXIST;
    LOG_WARN("backup set not exist", K(ret), K(backup_set_id), K(backup_set_list_));
  }
  return ret;
}

int ObRestoreBaseInfo::get_restore_data_dest_id(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    int64_t &dest_id) const
{
  int ret = OB_SUCCESS;
  const ObBackupDestType::TYPE backup_dest_type = ObBackupDestType::DEST_TYPE_RESTORE_DATA;
  if (OB_FAIL(share::ObBackupStorageInfoOperator::get_restore_dest_id(
       proxy, tenant_id, backup_dest_type, dest_id))) {
    LOG_WARN("failed to get restore dest id", K(ret));
  }
  return ret;
}

int ObRestoreBaseInfo::get_restore_archive_dest_id(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    int64_t &dest_id) const
{
  int ret = OB_SUCCESS;
  const ObBackupDestType::TYPE backup_dest_type = ObBackupDestType::DEST_TYPE_RESTORE_LOG;
  if (OB_FAIL(share::ObBackupStorageInfoOperator::get_restore_dest_id(
       proxy, tenant_id, backup_dest_type, dest_id))) {
    LOG_WARN("failed to get restore dest id", K(ret));
  }
  return ret;
}

int ObRestoreBaseInfo::get_last_backup_set_desc(share::ObBackupSetDesc &backup_set_desc) const
{
  int ret = OB_SUCCESS;
  if (backup_set_list_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup set list should not be empty", K(ret));
  } else {
    backup_set_desc = backup_set_list_.at(backup_set_list_.count() - 1).backup_set_desc_;
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
      "RESTORE_REMOTE_SSTABLE",
      "RESTORE_REPLACE_REMOTE_SSTABLE",
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

bool ObTabletRestoreAction::is_restore_remote_sstable(const ACTION &action)
{
  bool bool_ret = false;
  if (!is_valid(action)) {
    bool_ret = false;
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "restore action is unexpected", K(action));
  } else if (ACTION::RESTORE_REMOTE_SSTABLE != action) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

bool ObTabletRestoreAction::is_restore_replace_remote_sstable(const ACTION &action)
{
  bool bool_ret = false;
  if (!is_valid(action)) {
    bool_ret = false;
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "restore action is unexpected", K(action));
  } else if (ACTION::RESTORE_REPLACE_REMOTE_SSTABLE != action) {
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
  } else if (is_restore_remote_sstable(action)) {
    status = ObTabletRestoreStatus::REMOTE;
  } else if (is_restore_replace_remote_sstable(action)) {
    status = ObTabletRestoreStatus::FULL;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not trans restore action to restore status", K(ret), K(action), K(status));
  }
  return ret;
}

bool ObTabletRestoreAction::is_restore_status_match(
      const ACTION &action, const ObTabletRestoreStatus::STATUS &status)
{
  bool b_ret = false;
  if (!is_valid(action) || !ObTabletRestoreStatus::is_valid(status)) {
    b_ret = false;
  } else if (is_restore_all(action)) {
    b_ret = status == ObTabletRestoreStatus::EMPTY;
  } else if (is_restore_minor(action)) {
    b_ret = status == ObTabletRestoreStatus::EMPTY;
  } else if (is_restore_major(action)) {
    b_ret = status == ObTabletRestoreStatus::MINOR_AND_MAJOR_META;
  } else if (is_restore_tablet_meta(action)) {
    b_ret =  status == ObTabletRestoreStatus::PENDING;
  } else if (is_restore_remote_sstable(action)) {
    b_ret = status == ObTabletRestoreStatus::EMPTY;
  } else if (is_restore_replace_remote_sstable(action)) {
    b_ret = status == ObTabletRestoreStatus::REMOTE;
  }
  return b_ret;
}

bool ObTabletRestoreAction::need_restore_mds_sstable(const ACTION &action)
{
  return ACTION::RESTORE_MINOR == action
         || ACTION::RESTORE_ALL == action
         || ACTION::RESTORE_REMOTE_SSTABLE == action
         || ACTION::RESTORE_REPLACE_REMOTE_SSTABLE == action;
}

bool ObTabletRestoreAction::need_restore_minor_sstable(const ACTION &action)
{
  return ACTION::RESTORE_MINOR == action
         || ACTION::RESTORE_ALL == action
         || ACTION::RESTORE_REMOTE_SSTABLE == action
         || ACTION::RESTORE_REPLACE_REMOTE_SSTABLE == action;
}

bool ObTabletRestoreAction::need_restore_ddl_sstable(const ACTION &action)
{
  return ACTION::RESTORE_MINOR == action
         || ACTION::RESTORE_ALL == action
         || ACTION::RESTORE_REMOTE_SSTABLE == action
         || ACTION::RESTORE_REPLACE_REMOTE_SSTABLE == action;
}

bool ObTabletRestoreAction::need_restore_major_sstable(const ACTION &action)
{
  return ACTION::RESTORE_MAJOR == action
         || ACTION::RESTORE_ALL == action
         || ACTION::RESTORE_REMOTE_SSTABLE == action
         || ACTION::RESTORE_REPLACE_REMOTE_SSTABLE == action;
}

bool ObTabletRestoreAction::need_verify_table_store(const ACTION &action)
{
  return need_restore_major_sstable(action);
}

bool ObTabletRestoreAction::disallow_remote_table_exist(const ACTION &action)
{
  return ACTION::RESTORE_MAJOR == action
         || ACTION::RESTORE_ALL == action
         || ACTION::RESTORE_REPLACE_REMOTE_SSTABLE == action;
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

int ObRestoreUtils::create_backup_sstable_sec_meta_iterator(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    const storage::ObTabletHandle &tablet_handle,
    const ObITable::TableKey &table_key,
    const blocksstable::ObDatumRange &query_range,
    const ObRestoreBaseInfo &restore_base_info,
    backup::ObBackupMetaIndexStoreWrapper &meta_index_store,
    backup::ObBackupSSTableSecMetaIterator *&sstable_sec_meta_iterator)
{
  int ret = OB_SUCCESS;
  share::ObBackupSetDesc backup_set_desc;
  ObBackupDataType backup_data_type;
  backup::ObRestoreMetaIndexStore *meta_index_store_ptr = nullptr;
  backup::ObBackupSSTableSecMetaIterator *iterator = nullptr;
  const share::ObBackupDest &backup_dest = restore_base_info.backup_dest_;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  common::ObStorageIdMod mod;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
  int64_t dest_id = 0;

  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy should not be null", K(ret));
  } else if (OB_ISNULL(iterator = backup::ObLSBackupFactory::get_backup_sstable_sec_meta_iterator(tenant_id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to get backup sstable sec meta iterator", K(ret));
  } else if (OB_FAIL(restore_base_info.get_last_backup_set_desc(backup_set_desc))) {
    LOG_WARN("failed to get last backup set desc", K(ret));
  } else if (OB_FAIL(restore_base_info.get_restore_data_dest_id(*sql_proxy, tenant_id, dest_id))) {
    LOG_WARN("failed to get restore data dest id", K(ret), K(tenant_id));
  } else if (OB_FALSE_IT(mod.storage_id_ = static_cast<uint64_t>(dest_id))) {
  } else if (OB_FAIL(ObRestoreUtils::get_backup_data_type(table_key, backup_data_type))) {
    LOG_WARN("failed to get backup data type", K(ret), K(table_key));
  } else if (OB_FAIL(meta_index_store.get_backup_meta_index_store(backup_data_type, meta_index_store_ptr))) {
    LOG_WARN("failed to get backup meta index store", K(ret), K(backup_data_type));
  } else if (OB_FAIL(iterator->init(table_key.get_tablet_id(),
                                    tablet_handle,
                                    table_key,
                                    query_range,
                                    backup_dest,
                                    backup_set_desc,
                                    mod,
                                    *meta_index_store_ptr))) {
    LOG_WARN("failed to init sstable sec meta iterator", K(ret), K(table_key),
        K(query_range), K(backup_dest), K(backup_set_desc), K(restore_base_info));
  } else {
    sstable_sec_meta_iterator = iterator;
    iterator = nullptr;
  }

  if (OB_NOT_NULL(iterator)) {
    backup::ObLSBackupFactory::free(iterator);
  }

  return ret;
}

int ObRestoreUtils::create_backup_sstable_sec_meta_iterator(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    const storage::ObTabletHandle &tablet_handle,
    const ObITable::TableKey &table_key,
    const ObRestoreBaseInfo &restore_base_info,
    backup::ObBackupMetaIndexStoreWrapper &meta_index_store,
    backup::ObBackupSSTableSecMetaIterator *&sstable_sec_meta_iterator)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRange query_range;

  query_range.set_whole_range();
  if (OB_FAIL(create_backup_sstable_sec_meta_iterator(tenant_id,
                                                      tablet_id,
                                                      tablet_handle,
                                                      table_key,
                                                      query_range,
                                                      restore_base_info,
                                                      meta_index_store,
                                                      sstable_sec_meta_iterator))) {
    LOG_WARN("failed to create backup sstable sec meta iterator", K(ret));
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
    backup_physic_block_id_(),
    macro_id_()
{
}

void ObRestoreMacroBlockId::reset()
{
  logic_block_id_.reset();
  backup_physic_block_id_.reset();
  macro_id_.reset();
}

bool ObRestoreMacroBlockId::is_valid() const
{
  return logic_block_id_.is_valid()
    && (backup_physic_block_id_.is_valid() || macro_id_.is_valid());
}

int ObRestoreMacroBlockId::set(const blocksstable::ObLogicMacroBlockId &logic_id,
    const backup::ObBackupDeviceMacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (!logic_id.is_valid() || !macro_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(logic_id), K(macro_id));
  } else {
    logic_block_id_ = logic_id;
    macro_id_ = macro_id;
  }
  return ret;
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
    const ObTabletHandle &tablet_handle,
    const ObITable::TableKey &table_key,
    const ObRestoreBaseInfo &restore_base_info,
    backup::ObBackupMetaIndexStoreWrapper &meta_index_store,
    backup::ObBackupMetaIndexStoreWrapper &second_meta_index_store)
{
  int ret = OB_SUCCESS;
  const ObBackupSetFileDesc::Compatible compatible = restore_base_info.backup_compatible_;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("restore macro block id mgr init tiwce", K(ret));
  } else if (!tablet_id.is_valid() || !table_key.is_valid() || !restore_base_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("restore macro block id mgr init get invalid argument", K(ret), K(tablet_id),
        K(table_key), K(restore_base_info));
  } else {
    if (compatible <= ObBackupSetFileDesc::COMPATIBLE_VERSION_3) {
      if (OB_FAIL(inner_init_v1_(tablet_id, table_key, restore_base_info, meta_index_store, second_meta_index_store))) {
        LOG_WARN("failed to inner init restore macro block id mgr", K(ret), K(tablet_id), K(table_key));
      }
    } else {
      if (OB_FAIL(inner_init_v2_(tablet_id, tablet_handle, table_key, restore_base_info, meta_index_store))) {
        LOG_WARN("failed to inner init", K(ret), K(tablet_id), K(table_key));
      }
    }
    if (OB_SUCC(ret)) {
      table_key_ = table_key;
      backup_compatible_ = compatible;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObRestoreMacroBlockIdMgr::inner_init_v1_(
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
  ObStorageIdMod mod;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
  int64_t dest_id = 0;

  if (OB_FAIL(ObRestoreUtils::get_backup_data_type(table_key, data_type))) {
    LOG_WARN("failed to get backup data type", K(ret), K(table_key));
  } else if (OB_FAIL(restore_base_info.get_restore_data_dest_id(*GCTX.sql_proxy_, MTL_ID(), dest_id))) {
      LOG_WARN("fail to get restore data dest id", K(ret));
  } else if (OB_FALSE_IT(mod.storage_id_ = static_cast<uint64_t>(dest_id))) {
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
          restore_base_info.backup_dest_.get_storage_info(), mod, sstable_meta_index, &OB_BACKUP_META_CACHE, backup_sstable_meta_array))) {
        LOG_WARN("failed to read sstable metas", K(ret), K(table_key), K(tablet_id), K(restore_base_info));
      } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_macro_block_id_mapping_metas(second_meta_backup_path.get_obstr(),
          restore_base_info.backup_dest_.get_storage_info(), mod, second_meta_index, macro_block_id_map))) {
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
              const backup::ObCompatBackupMacroBlockIDPair &pair = id_mapping->id_pair_list_.at(j);
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

int ObRestoreMacroBlockIdMgr::inner_init_v2_(
    const common::ObTabletID &tablet_id,
    const storage::ObTabletHandle &tablet_handle,
    const ObITable::TableKey &table_key,
    const ObRestoreBaseInfo &restore_base_info,
    backup::ObBackupMetaIndexStoreWrapper &meta_index_store)
{
  int ret = OB_SUCCESS;
  backup::ObBackupMetaIndex meta_index;
  share::ObBackupDataType data_type;
  share::ObBackupPath backup_path;
  common::ObArray<backup::ObBackupSSTableMeta> sstable_metas;
  const backup::ObBackupMetaType meta_type = backup::ObBackupMetaType::BACKUP_SSTABLE_META;
  ObStorageIdMod mod;
  int64_t dest_id = 0;

  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
  if (OB_FAIL(ObRestoreUtils::get_backup_data_type(table_key, data_type))) {
    LOG_WARN("failed to get backup data type", K(ret), K(table_key));
  } else if (OB_FAIL(restore_base_info.get_restore_data_dest_id(*GCTX.sql_proxy_, MTL_ID(), dest_id))) {
    LOG_WARN("fail to get restore data dest id", K(ret));
  } else if (OB_FALSE_IT(mod.storage_id_ = static_cast<uint64_t>(dest_id))) {
  } else if (OB_FAIL(meta_index_store.get_backup_meta_index(data_type, tablet_id, meta_type, meta_index))) {
    LOG_WARN("failed to get backup meta index", K(ret), K(data_type), K(tablet_id), K(meta_type));
  } else if (OB_FAIL(share::ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(restore_base_info.backup_dest_,
      meta_index.ls_id_, data_type, meta_index.turn_id_, meta_index.retry_id_, meta_index.file_id_, backup_path))) {
    LOG_WARN("failed to get macro block backup path", K(ret), K(restore_base_info), K(meta_index), K(data_type));
  } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_sstable_metas(backup_path.get_obstr(),
      restore_base_info.backup_dest_.get_storage_info(), mod, meta_index, &OB_BACKUP_META_CACHE, sstable_metas))) {
    LOG_WARN("failed to read sstable meta", K(ret), K(backup_path), K(restore_base_info), K(meta_index));
  } else {
    int64_t index = -1;
    ARRAY_FOREACH_X(sstable_metas, idx, cnt, OB_SUCC(ret)) {
      const backup::ObBackupSSTableMeta &backup_sstable_meta = sstable_metas.at(idx);
      const ObITable::TableKey &cur_table_key = backup_sstable_meta.sstable_meta_.table_key_;
      if (cur_table_key == table_key) {
        index = idx;
        break;
      }
    }
    if (OB_SUCC(ret) && -1 == index) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("can not find table key", K(sstable_metas));
    }
    if (OB_SUCC(ret)) {
      const uint64_t tenant_id = MTL_ID();
      const ObITable::TableKey &table_key = sstable_metas.at(index).sstable_meta_.table_key_;
      ObArenaAllocator allocator;
      backup::ObBackupSSTableSecMetaIterator *iterator = NULL;
      if (OB_FAIL(ObRestoreUtils::create_backup_sstable_sec_meta_iterator(tenant_id,
                                                                          tablet_id,
                                                                          tablet_handle,
                                                                          table_key,
                                                                          restore_base_info,
                                                                          meta_index_store,
                                                                          iterator))) {
        LOG_WARN("failed to create backup sstable sec meta iterator", K(ret), K(tablet_id), K(table_key));
      } else if (OB_FAIL(get_macro_block_index_list_from_iter_(*iterator, block_id_array_))) {
        LOG_WARN("failed to get macro block index list from iter", K(ret), K(tablet_id), K(table_key), K(restore_base_info));
      } else {
        LOG_INFO("get macro block index list", K(block_id_array_));
      }
      if (OB_NOT_NULL(iterator)) {
        backup::ObLSBackupFactory::free(iterator);
      }
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
  } else if (backup_compatible_ > ObBackupSetFileDesc::COMPATIBLE_VERSION_3) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup not compatible", K(ret), K_(backup_compatible));
  } else {
    logic_block_id = block_id_array_.at(block_id_index).logic_block_id_;
    physic_block_id = block_id_array_.at(block_id_index).backup_physic_block_id_;
  }
  return ret;
}

int ObRestoreMacroBlockIdMgr::get_macro_block_id(
    const int64_t block_id_index,
    blocksstable::ObLogicMacroBlockId &logic_block_id,
    backup::ObBackupDeviceMacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  logic_block_id.reset();
  macro_id.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore macro block id mgr do not init", K(ret));
  } else if (block_id_index < 0 || block_id_index >= block_id_array_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get physical id get invalid argument", K(ret), K(block_id_index), K(block_id_array_));
  } else {
    logic_block_id = block_id_array_.at(block_id_index).logic_block_id_;
    macro_id = block_id_array_.at(block_id_index).macro_id_;
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

int ObRestoreMacroBlockIdMgr::get_macro_block_index_list_from_iter_(
    backup::ObBackupSSTableSecMetaIterator &meta_iter,
    common::ObIArray<ObRestoreMacroBlockId> &macro_id_list)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDataMacroBlockMeta data_macro_block_meta;
  blocksstable::ObLogicMacroBlockId logic_id;
  backup::ObBackupDeviceMacroBlockId macro_id;
  ObRestoreMacroBlockId restore_macro_id;
  while (OB_SUCC(ret)) {
    data_macro_block_meta.reset();
    logic_id.reset();
    macro_id.reset();
    restore_macro_id.reset();
    if (OB_FAIL(meta_iter.get_next(data_macro_block_meta))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next", K(ret));
      }
    } else {
      logic_id = data_macro_block_meta.get_logic_id();
      if (OB_FAIL(macro_id.set(data_macro_block_meta.get_macro_id()))) {
        LOG_WARN("failed to set macro id", K(ret), K(data_macro_block_meta.get_macro_id()));
      } else if (macro_id.block_type_ != backup::ObBackupDeviceMacroBlockId::DATA_BLOCK) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, invalid block type", K(ret), K(data_macro_block_meta), K(macro_id));
      } else if (OB_FAIL(restore_macro_id.set(logic_id, macro_id))) {
        LOG_WARN("failed to set restore macro id", K(ret), K(logic_id), K(macro_id));
      } else if (OB_FAIL(macro_id_list.push_back(restore_macro_id))) {
        LOG_WARN("failed to push back", K(ret), K(restore_macro_id));
      } else {
        LOG_DEBUG("get macro id", K(restore_macro_id));
      }
    }
  }
  return ret;
}

}
}
