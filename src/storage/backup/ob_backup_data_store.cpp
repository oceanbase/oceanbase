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

#define USING_LOG_PREFIX SHARE
#include "storage/backup/ob_backup_data_store.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "lib/restore/ob_storage.h"
#include "lib/oblog/ob_log_module.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif

using namespace oceanbase;
using namespace common;
using namespace share;
/*
 *--------------------------------ObBackupLSInfo-----------------------------
 */
OB_SERIALIZE_MEMBER(ObBackupDataLSAttrDesc, backup_scn_, ls_attr_array_);

bool ObBackupDataLSAttrDesc::is_valid() const
{
  return backup_scn_.is_valid() && !ls_attr_array_.empty();
}

/*
 *------------------------------ObBackupDataTabletToLSInfo----------------------------
 */
OB_SERIALIZE_MEMBER(ObBackupDataTabletToLSInfo, ls_id_, tablet_id_list_);

int ObBackupDataTabletToLSInfo::assign(const ObBackupDataTabletToLSInfo &that)
{
  int ret = OB_SUCCESS;
  if (!that.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(that));
  } else if (OB_FAIL(tablet_id_list_.assign(that.tablet_id_list_))) {
    LOG_WARN("fail to assign tablet id", K(ret), K(that));
  } else {
    ls_id_ = that.ls_id_;
  }
  return ret;
}

bool ObBackupDataTabletToLSInfo::is_valid() const
{
  return ls_id_.is_valid();
}

/*
 *------------------------------ObBackupDataTabletToLSDesc----------------------------
 */
OB_SERIALIZE_MEMBER(ObBackupDataTabletToLSDesc, backup_scn_, tablet_to_ls_);

bool ObBackupDataTabletToLSDesc::is_valid() const
{
  return true;
}

/*
 *------------------------------ObBackupDeletedTabletToLSDesc----------------------------
 */
OB_SERIALIZE_MEMBER(ObBackupDeletedTabletToLSDesc, deleted_tablet_to_ls_);

bool ObBackupDeletedTabletToLSDesc::is_valid() const
{
  return true;
}

/*
 *------------------------------ObExternTenantLocalityInfo----------------------------
 */
OB_SERIALIZE_MEMBER(ObExternTenantLocalityInfoDesc, tenant_id_, backup_set_id_, cluster_id_, compat_mode_,
    tenant_name_, cluster_name_, locality_, primary_zone_, sys_time_zone_, sys_time_zone_wrap_);

bool ObExternTenantLocalityInfoDesc::is_valid() const
{
  return is_valid_tenant_id(tenant_id_)
      && backup_set_id_ > 0
      && is_valid_cluster_id(cluster_id_) > 0
      && compat_mode_ != lib::Worker::CompatMode::INVALID
      && !tenant_name_.is_empty()
      && !cluster_name_.is_empty()
      && !locality_.is_empty()
      && !primary_zone_.is_empty();
}

int ObExternTenantLocalityInfoDesc::assign(const ObExternTenantLocalityInfoDesc &that)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sys_time_zone_wrap_.deep_copy(that.sys_time_zone_wrap_))) {
    LOG_WARN("failed to deep copy", K(ret));
  } else {
    tenant_id_ = that.tenant_id_;
    backup_set_id_ = that.backup_set_id_;
    cluster_id_ = that.cluster_id_;
    compat_mode_ = that.compat_mode_;
    tenant_name_ = that.tenant_name_;
    cluster_name_ = that.cluster_name_;
    locality_ = that.locality_;
    primary_zone_ = that.primary_zone_;
    sys_time_zone_ = that.sys_time_zone_;
  }
  return ret;
}

/*
 *------------------------------ObExternBackupSetInfo----------------------------
 */
OB_SERIALIZE_MEMBER(ObExternBackupSetInfoDesc, backup_set_file_);

bool ObExternBackupSetInfoDesc::is_valid() const
{
  return backup_set_file_.is_valid();
}

/*
 *------------------------------ObExternTenantDiagnoseInfo----------------------------
 */

OB_SERIALIZE_MEMBER(ObExternTenantDiagnoseInfoDesc, tenant_id_, tenant_locality_info_, backup_set_file_);

bool ObExternTenantDiagnoseInfoDesc::is_valid() const
{

  return tenant_id_ != OB_INVALID_TENANT_ID 
      && tenant_locality_info_.is_valid()
      && backup_set_file_.is_valid();
}

/*
 *------------------------------ObExternBackupSetPlaceholderDesc----------------------------
 */

OB_SERIALIZE_MEMBER(ObExternBackupSetPlaceholderDesc);

/*
 *-----------------------------ObTenantBackupSetInfosDesc-----------------------
 */

OB_SERIALIZE_MEMBER(ObTenantBackupSetInfosDesc, backup_set_infos_);

bool ObTenantBackupSetInfosDesc::is_valid() const
{
  return !backup_set_infos_.empty();
}


/*
 *-----------------------------ObBackupLSMetaInfosDesc-----------------------
 */

OB_SERIALIZE_MEMBER(ObBackupLSMetaInfosDesc, ls_meta_packages_);

bool ObBackupLSMetaInfosDesc::is_valid() const
{
  return !ls_meta_packages_.empty();
}


int ObBackupSetFilter::get_backup_set_array(ObIArray<share::ObBackupSetDesc> &backup_set_array) const
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(backup_set_name_array_, i, cnt, OB_SUCC(ret)) {
    share::ObBackupSetDesc backup_desc;
    const BackupSetDirName &backup_set_name = backup_set_name_array_.at(i);
    if (OB_FAIL(ObBackupDataStore::parse_backup_set_name(backup_set_name, backup_desc))) {
      LOG_WARN("fail to parse backup set name", K(ret));
    } else if (OB_FAIL(backup_set_array.push_back(backup_desc))) {
      LOG_WARN("fail to assign backup set name array", K(ret), K(backup_set_name_array_));
    }
  }
  return ret;
}

int ObBackupSetFilter::func(const dirent *entry)
{
  // parser backup_set_1_inc_end_success_20220601T010101 to backup_set_1_inc
  int ret = OB_SUCCESS;
  char tmp_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
  ObString bs_placeholder_name(entry->d_name);
  const char *end_success_str = "end_success";
  const char *backup_set_str = "backup_set";
  const char *find_pos = nullptr;
  if (backup_set_name_array_.count() >= OB_MAX_BACKUP_SET_NUM) { // list upper limit //TODO(chongrong.th) add new error code
  } else if (!bs_placeholder_name.prefix_match(backup_set_str)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid backup set dier prefix", K(ret), K(bs_placeholder_name));
  } else if (OB_ISNULL(find_pos = STRSTR(bs_placeholder_name.ptr(), end_success_str))) {
  } else if (find_pos == bs_placeholder_name.ptr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("find invalid pos", K(ret), K(find_pos));
  } else if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%.*s", static_cast<int>(bs_placeholder_name.length()), bs_placeholder_name.ptr()))) {
    LOG_WARN("fail to set config value", K(ret));
  } else {
    char *token = nullptr;
    char *saveptr = nullptr;
    char *p_end = nullptr;
    token = tmp_str;
    BackupSetDirName backup_set_dir_name;
    int64_t pos = 0;
    for (char *str = token; OB_SUCC(ret); str = nullptr) {
      token = ::STRTOK_R(str, "_", &saveptr);
      if (nullptr == token) {
        break;
      } else if (0 == STRCMP(OB_STR_INC_BACKUP, token) || 0 == STRCMP(OB_STR_FULL_BACKUP, token)) {
        if (OB_FAIL(databuff_printf(backup_set_dir_name.ptr(), backup_set_dir_name.capacity(), pos, "%s", token))) {
          LOG_WARN("fail to databuff printf backup set dir name", K(ret));
        } else {
          break;
        }
      } else if (OB_FAIL(databuff_printf(backup_set_dir_name.ptr(), backup_set_dir_name.capacity(), pos, "%s_", token))) {
        LOG_WARN("fail to databuff printf backup set dir name", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(backup_set_name_array_.push_back(backup_set_dir_name))) {
      LOG_WARN("fail to push backup set name", K(ret));
    } else {
      LOG_INFO("backup set dir name", K(ret), K(backup_set_dir_name));
    }
  }
  return ret;
}

/*
 *------------------------------ObBackupDataStore----------------------------
 */

ObBackupDataStore::ObBackupDataStore()
 : backup_desc_()
{
}

ObBackupDataStore::~ObBackupDataStore()
{
}

int ObBackupDataStore::init(const share::ObBackupDest &backup_set_dest)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest dest;
  if (is_init()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("extern backup data mgr init twice.", K(ret));
  } else if (OB_FAIL(backup_set_dest_.deep_copy(backup_set_dest))) {
    LOG_WARN("fail to set backup set dest", K(ret));
  } else if (OB_FAIL(ObBackupStore::init(backup_set_dest_))) {
    LOG_WARN("failed to set backup dest.", K(ret), K(backup_set_dest));
  }
  return ret;
}

int ObBackupDataStore::init(const char *backup_set_dest)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest dest;
  if (is_init()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("extern backup data mgr init twice.", K(ret));
  } else if (OB_FAIL(backup_set_dest_.set(backup_set_dest))) {
    LOG_WARN("fail to set backup set dest", K(ret));
  } else if (OB_FAIL(ObBackupStore::init(backup_set_dest_))) {
    LOG_WARN("failed to set backup dest.", K(ret), K(backup_set_dest));
  }
  return ret;
}

int ObBackupDataStore::init(
    const ObBackupDest &backup_dest,
    const share::ObBackupSetDesc &backup_desc)

{
  int ret = OB_SUCCESS;
  if (is_init()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("extern backup data mgr init twice.", K(ret));
  } else if (OB_FAIL(share::ObBackupPathUtil::construct_backup_set_dest(backup_dest, backup_desc, backup_set_dest_))) {
    LOG_WARN("fail to construct backup set dest", K(backup_dest), K(backup_desc));
  } else if (OB_FAIL(ObBackupStore::init(backup_dest))) {
    LOG_WARN("failed to set backup dest.", K(ret), K(backup_dest));
  } else {
    backup_desc_ = backup_desc;
  }
  return ret;
}

int ObBackupDataStore::write_ls_attr(const int64_t turn_id, const ObBackupDataLSAttrDesc &ls_info)
{
  int ret = OB_SUCCESS;
  ObBackupPathString full_path;
  share::ObBackupPath path;

  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data extern mgr not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_backup_ls_attr_info_path(backup_set_dest_, turn_id, path))) {
    LOG_WARN("fail to get path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(write_single_file(full_path, ls_info))) {
    LOG_WARN("fail to write single file", K(ret));
  } 

  return ret;
}

int ObBackupDataStore::read_ls_attr_info(ObBackupDataLSAttrDesc &ls_info)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath path;
  ObBackupPathString full_path;
  storage::ObExternBackupSetInfoDesc backup_set_info;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataStore not init", K(ret));
  } else if (OB_FAIL(read_backup_set_info(backup_set_info))) {
    LOG_WARN("fail to read backup set info", K(ret));
  } else if (!backup_set_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid backup set info", K(ret), K(backup_set_info));
  } else if (OB_FAIL(read_ls_attr_info(backup_set_info.backup_set_file_.meta_turn_id_, ls_info))) {
    LOG_WARN("fail to read ls attr info", K(ret));
  }
  return ret;
}

int ObBackupDataStore::read_ls_attr_info(const int64_t turn_id, ObBackupDataLSAttrDesc &ls_info)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath path;
  ObBackupPathString full_path;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataStore not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_backup_ls_attr_info_path(backup_set_dest_, turn_id, path))) {
    LOG_WARN("fail to get tenant ls attr info path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(read_single_file(full_path, ls_info))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path));
  }
  return ret;
}

int ObBackupDataStore::write_ls_meta_infos(const ObBackupLSMetaInfosDesc &ls_meta_infos)
{
  int ret = OB_SUCCESS;
  ObBackupPathString full_path;
  share::ObBackupPath path;

  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data extern mgr not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_ls_meta_infos_path(backup_set_dest_, path))) {
    LOG_WARN("fail to get path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(write_single_file(full_path, ls_meta_infos))) {
    LOG_WARN("fail to write single file", K(ret));
  } 

  return ret;
}

int ObBackupDataStore::read_ls_meta_infos(const ObLSID &ls_id, storage::ObLSMetaPackage &ls_meta_package)
{
  int ret = OB_SUCCESS;
  storage::ObBackupLSMetaInfosDesc ls_meta_infos;
  bool ls_exist = false;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataStore not init", K(ret));
  } else if (OB_FAIL(read_ls_meta_infos(ls_meta_infos))) {
    LOG_WARN("fail to read ls meta infos", K(ret));
  } else {
    ARRAY_FOREACH_X(ls_meta_infos.ls_meta_packages_, i, cnt, OB_SUCC(ret)) {
      const storage::ObLSMetaPackage &tmp_ls_meta_package = ls_meta_infos.ls_meta_packages_.at(i);
      if (ls_id == tmp_ls_meta_package.ls_meta_.ls_id_) {
        ls_exist = true;
        ls_meta_package = tmp_ls_meta_package;
        break;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!ls_exist) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("ls meta package is not existed", K(ret), K(ls_id));
    }
  }
  return ret;
}

int ObBackupDataStore::read_ls_meta_infos(ObBackupLSMetaInfosDesc &ls_meta_infos)
{
  int ret = OB_SUCCESS;
  ObBackupPathString full_path;
  share::ObBackupPath path;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataStore not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_ls_meta_infos_path(backup_set_dest_, path))) {
    LOG_WARN("fail to get tenant ls attr info path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(read_single_file(full_path, ls_meta_infos))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path));
  } else if (!ls_meta_infos.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls meta infos", K(ret), K(ls_meta_infos));
  } 
  return ret;
}

int ObBackupDataStore::write_tablet_to_ls_info(const ObBackupDataTabletToLSDesc &tablet_to_ls_info,
    const int64_t turn_id, const share::ObBackupDataType &type)
{
  int ret = OB_SUCCESS;
  ObBackupPathString full_path;
  share::ObBackupPath path;

  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data extern mgr not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_backup_data_tablet_ls_info_path(backup_set_dest_, type, turn_id, path))) {
    LOG_WARN("fail to get path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(write_single_file(full_path, tablet_to_ls_info))) {
    LOG_WARN("fail to write single file", K(ret));
  } 

  return ret;
}

int ObBackupDataStore::read_tablet_to_ls_info(const int64_t turn_id, const share::ObBackupDataType &type,
    ObBackupDataTabletToLSDesc &tablet_to_ls_info)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath path;
  ObBackupPathString full_path;
  share::ObBackupDataType tmp_type;
  type.is_major_backup() ? tmp_type.set_major_data_backup() : tmp_type.set_minor_data_backup();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataStore not init", K(ret));
  } else if (type.is_sys_backup() && turn_id != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(turn_id), K(type));
  } else if (OB_FAIL(ObBackupPathUtil::get_backup_data_tablet_ls_info_path(backup_set_dest_, tmp_type, turn_id, path))) {
    LOG_WARN("fail to get tenant ls attr info path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(read_single_file(full_path, tablet_to_ls_info))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path));
  }
  return ret;
}

int ObBackupDataStore::read_tablet_list(const share::ObBackupDataType &type, const int64_t turn_id, const ObLSID &ls_id,
    ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath path;
  ObBackupPathString full_path;
  ObBackupDataTabletToLSDesc tablet_to_ls_info;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataStore not init", K(ret));
  } else if (OB_FAIL(read_tablet_to_ls_info(turn_id, type, tablet_to_ls_info))) {
    LOG_WARN("failed to read tablet to ls info", K(ret), K(turn_id), K(type));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < tablet_to_ls_info.tablet_to_ls_.count(); ++i) {
      const ObBackupDataTabletToLSInfo &info = tablet_to_ls_info.tablet_to_ls_.at(i);
      if (!info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet to ls info", K(ret), K(info));
      } else if (info.ls_id_ == ls_id) {
        if (OB_FAIL(append(tablet_ids, info.tablet_id_list_))) {
          LOG_WARN("failed to append tablet ids", K(ret), K(info));
        } else {
          break;
        }
      }
    }
  }
  return ret;
}

int ObBackupDataStore::write_tenant_backup_set_infos(const ObTenantBackupSetInfosDesc &tenant_backup_set_infos)
{
  int ret = OB_SUCCESS;

  ObBackupPathString full_path;
  share::ObBackupPath path;

  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data extern mgr not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_backup_set_infos_path(backup_set_dest_, path))) {
    LOG_WARN("fail to get path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(write_single_file(full_path, tenant_backup_set_infos))) {
    LOG_WARN("fail to write single file", K(ret));
  }

  return ret;
}

int ObBackupDataStore::read_deleted_tablet_info(const ObLSID &ls_id, ObIArray<ObTabletID> &deleted_tablet_ids)
{
  int ret = OB_SUCCESS;
  deleted_tablet_ids.reset();
  share::ObBackupPath path;
  ObBackupPathString full_path;
  ObBackupDeletedTabletToLSDesc deleted_tablet_info;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataStore not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_deleted_tablet_info_path(backup_set_dest_, path))) {
    LOG_WARN("fail to get tenant ls attr info path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(read_single_file(full_path, deleted_tablet_info))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("backup deleted file not exist", K(ret));
    } else {
      LOG_WARN("failed to read single file", K(ret), K(full_path));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < deleted_tablet_info.deleted_tablet_to_ls_.count(); ++i) {
      const ObBackupDataTabletToLSInfo &info = deleted_tablet_info.deleted_tablet_to_ls_.at(i);
      if (info.ls_id_ == ls_id) {
        if (OB_FAIL(deleted_tablet_ids.assign(info.tablet_id_list_))) {
          LOG_WARN("failed to assign", K(ret), K(info));
        }
        break;
      }
    }
  }
  return ret;
}

int ObBackupDataStore::write_backup_set_placeholder(
    const bool is_inner,
    const bool is_start, 
    const bool is_succeed,
    const SCN &replay_scn,
    const SCN &min_restore_scn)
{
  int ret = OB_SUCCESS;
  ObExternBackupSetPlaceholderDesc placeholder;
  ObBackupPathString full_path;
  share::ObBackupPath path;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data extern mgr not init", K(ret));
  } else if (OB_FAIL(get_backup_set_placeholder_path_(
        is_inner, is_start, is_succeed, replay_scn, min_restore_scn, path))) {
    LOG_WARN("failed to get backup placeholder path.", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(write_single_file(full_path, placeholder))) {
    LOG_WARN("fail to write single file", K(ret));
  } 
  return ret;
}


int ObBackupDataStore::write_tenant_locality_info(const ObExternTenantLocalityInfoDesc &locality_info)
{
  int ret = OB_SUCCESS;
  
  ObBackupPathString full_path;
  share::ObBackupPath path;

  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data extern mgr not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_locality_info_path(backup_set_dest_, path))) {
    LOG_WARN("fail to get path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(write_single_file(full_path, locality_info))) {
    LOG_WARN("fail to write single file", K(ret));
  } 

  return ret;
}

int ObBackupDataStore::read_tenant_locality_info(ObExternTenantLocalityInfoDesc &locality_info)
{
  int ret = OB_SUCCESS;
  
  share::ObBackupPath path;
  ObBackupPathString full_path;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataStore not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_locality_info_path(backup_set_dest_, path))) {
    LOG_WARN("fail to get tenant ls attr info path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(read_single_file(full_path, locality_info))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path));
  }
  return ret;
}

int ObBackupDataStore::write_tenant_diagnose_info(const ObExternTenantDiagnoseInfoDesc &diagnose_info)
{
  int ret = OB_SUCCESS;
  
  ObBackupPathString full_path;
  share::ObBackupPath path;

  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data extern mgr not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_diagnose_info_path(backup_set_dest_, path))) {
    LOG_WARN("fail to get path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(write_single_file(full_path, diagnose_info))) {
    LOG_WARN("fail to write single file", K(ret));
  } 

  return ret;
}

int ObBackupDataStore::read_tenant_diagnose_info(ObExternTenantDiagnoseInfoDesc &diagnose_info)
{
  int ret = OB_SUCCESS;
  
  share::ObBackupPath path;
  ObBackupPathString full_path;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataStore not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_diagnose_info_path(backup_set_dest_, path))) {
    LOG_WARN("fail to get tenant ls attr info path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(read_single_file(full_path, diagnose_info))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path));
  }
  return ret;
}

int ObBackupDataStore::write_backup_set_info(const ObExternBackupSetInfoDesc &backup_set_info)
{
  int ret = OB_SUCCESS;
  
  ObBackupPathString full_path;
  share::ObBackupPath path;

  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data extern mgr not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_backup_set_info_path(backup_set_dest_, path))) {
    LOG_WARN("fail to get path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(write_single_file(full_path, backup_set_info))) {
    LOG_WARN("fail to write single file", K(ret));
  } 

  return ret;
}

int ObBackupDataStore::read_backup_set_info(ObExternBackupSetInfoDesc &backup_set_info)
{
  int ret = OB_SUCCESS;
  
  share::ObBackupPath path;
  ObBackupPathString full_path;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataStore not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_backup_set_info_path(backup_set_dest_, path))) {
    LOG_WARN("fail to get tenant ls attr info path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(read_single_file(full_path, backup_set_info))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path));
  }
  return ret;
}

int ObBackupDataStore::write_root_key_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  share::ObBackupPath path;
  ObString empty_str;
  // TODO(sean.yyj): support use user specified encrypt key to encrypt rook key backup
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data extern mgr not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_backup_root_key_path(backup_set_dest_, path))) {
    LOG_WARN("fail to get path", K(ret));
  } else if (OB_FAIL(ObMasterKeyUtil::backup_root_key(tenant_id, path.get_obstr(),
                                              backup_set_dest_.get_storage_info(), empty_str))) {
    LOG_WARN("fail to backup root key", K(ret));
  }
#endif
  return ret;
}

int ObBackupDataStore::read_root_key_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  share::ObBackupPath path;
  ObString empty_str;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup data extern mgr not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_backup_root_key_path(backup_set_dest_, path))) {
    LOG_WARN("fail to get path", K(ret));
  } else if (OB_FAIL(ObMasterKeyUtil::restore_root_key(tenant_id, path.get_obstr(),
                                              backup_set_dest_.get_storage_info(), empty_str))) {
    LOG_WARN("fail to backup root key", K(ret));
  }
#endif
  return ret;
}

int ObBackupDataStore::get_backup_set_placeholder_path_(
    const bool is_inner, 
    const bool is_start, 
    const bool is_succeed, 
    const SCN &replay_scn,
    const SCN &min_restore_scn,
    share::ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  
  if (is_inner) {
    if (OB_FAIL(share::ObBackupPathUtil::get_backup_set_inner_placeholder(
         backup_set_dest_, backup_desc_, replay_scn, min_restore_scn, path))) {
      LOG_WARN("failed to get backup set inner placeholder path", K(ret), K(*this));
    }
  } else if (is_start) {
    if (OB_FAIL(share::ObBackupPathUtil::get_backup_set_placeholder_start_path(
          get_backup_dest(), backup_desc_, path))) {
      LOG_WARN("failed to get backup set start placeholder path", K(ret), K(*this));
    } 
  } else if (is_succeed) {
    if (OB_FAIL(share::ObBackupPathUtil::get_backup_set_placeholder_end_success_path(
          get_backup_dest(), backup_desc_, min_restore_scn, path))) {
      LOG_WARN("failed to get backup set end placeholder path", K(ret), K(*this));
    } 
  } else {
    if (OB_FAIL(share::ObBackupPathUtil::get_backup_set_placeholder_end_failed_path(
          get_backup_dest(), backup_desc_, min_restore_scn, path))) {
      LOG_WARN("failed to get backup set end placeholder path", K(ret), K(*this));
    } 
  }
  return ret;
}

int ObBackupDataStore::get_backup_set_array(
    const common::ObString &passwd_array,
    const SCN &restore_scn,
    SCN &restore_start_scn,
    common::ObIArray<share::ObRestoreBackupSetBriefInfo> &backup_set_list)
{
  int ret = OB_SUCCESS;
  backup_set_list.reset();
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataStore not init", K(ret));
  } else {
    int64_t global_max_backup_set_id = -1;
    ObBackupIoAdapter util;
    ObBackupSetFilter op;
    share::ObBackupPath tenant_backup_placeholder_dir_path;
    const ObBackupStorageInfo *storage_info = get_storage_info();
    op.reset();
    if (OB_FAIL(share::ObBackupPathUtil::get_backup_sets_dir_path(get_backup_dest(), 
        tenant_backup_placeholder_dir_path))) {
      LOG_WARN("fail to get simple backup placeholder dir", K(ret));
    } else if (OB_FAIL(util.list_files(tenant_backup_placeholder_dir_path.get_obstr(), storage_info, op))) {
      LOG_WARN("fail to list files", K(ret), K(tenant_backup_placeholder_dir_path));
    } else if (OB_FAIL(do_get_backup_set_array_(passwd_array, restore_scn, op, backup_set_list,
        global_max_backup_set_id, restore_start_scn))) {
      LOG_WARN("fail to do get backup set array", K(ret), K(op));
    }
  }
  return ret;
}

int ObBackupDataStore::get_max_backup_set_file_info(const common::ObString &passwd_array, ObBackupSetFileDesc &output_desc)
{
  int ret = OB_SUCCESS;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataStore not init", K(ret));
  } else {
    ObBackupIoAdapter util;
    ObBackupSetFilter op;
    share::ObBackupPath tenant_backup_placeholder_dir_path;
    const ObBackupStorageInfo *storage_info = get_storage_info();
    op.reset();
    ObSArray<share::ObBackupSetDesc> backup_set_desc_array;
    if (OB_FAIL(share::ObBackupPathUtil::get_backup_sets_dir_path(get_backup_dest(),
        tenant_backup_placeholder_dir_path))) {
      LOG_WARN("fail to get simple backup placeholder dir", K(ret));
    } else if (OB_FAIL(util.list_files(tenant_backup_placeholder_dir_path.get_obstr(), storage_info, op))) {
      LOG_WARN("fail to list files", K(ret), K(tenant_backup_placeholder_dir_path));
    } else if (OB_FAIL(op.get_backup_set_array(backup_set_desc_array))) {
      LOG_WARN("fail to get backup set name array", K(ret), K(op));
    } else {
      ObBackupSetDescComparator cmp;
      storage::ObExternBackupSetInfoDesc backup_set_info;
      std::sort(backup_set_desc_array.begin(), backup_set_desc_array.end(), cmp);
      for (int64_t i = backup_set_desc_array.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
        const share::ObBackupSetDesc &backup_set_desc = backup_set_desc_array.at(i);
        backup_desc_.backup_set_id_ = backup_set_desc.backup_set_id_;
        backup_desc_.backup_type_.type_ = backup_set_desc.backup_type_.type_;
        backup_set_dest_.reset();
        if (OB_FAIL(ObBackupPathUtil::construct_backup_set_dest(
            get_backup_dest(), backup_desc_, backup_set_dest_))) {
          LOG_WARN("fail to construct backup set dest", K(ret));
        } else if (OB_FAIL(read_backup_set_info(backup_set_info))) {
          if (OB_BACKUP_FILE_NOT_EXIST == ret) {
            LOG_WARN("backup set info not exist", K(ret), K(backup_set_desc));
            ret = OB_SUCCESS;
            continue;
          } else {
            LOG_WARN("fail to read backup set info", K(ret), K(backup_set_desc));
          }
        } else {
          const share::ObBackupSetFileDesc &backup_set_file = backup_set_info.backup_set_file_;
          if (OB_FAIL(backup_set_file.check_passwd(passwd_array.ptr()))) {
            LOG_WARN("fail to check passwd", K(ret));
          } else if (share::ObBackupSetFileDesc::BackupSetStatus::SUCCESS != backup_set_file.status_
              || share::ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE != backup_set_file.file_status_) {
            LOG_WARN("invalid status backup set can not be used to restore", K(backup_set_file));
          } else {
            output_desc = backup_set_file;
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupDataStore::get_backup_sys_time_zone_wrap(common::ObTimeZoneInfoWrap & time_zone_wrap)
{
  int ret = OB_SUCCESS;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataStore not init", K(ret));
  } else {
    ObBackupIoAdapter util;
    ObBackupSetFilter op;
    share::ObBackupPath tenant_backup_placeholder_dir_path;
    const ObBackupStorageInfo *storage_info = get_storage_info();
    op.reset();
    ObSArray<share::ObBackupSetDesc> backup_set_desc_array;
    if (OB_FAIL(share::ObBackupPathUtil::get_backup_sets_dir_path(get_backup_dest(),
        tenant_backup_placeholder_dir_path))) {
      LOG_WARN("fail to get simple backup placeholder dir", K(ret));
    } else if (OB_FAIL(util.list_files(tenant_backup_placeholder_dir_path.get_obstr(), storage_info, op))) {
      LOG_WARN("fail to list files", K(ret), K(tenant_backup_placeholder_dir_path));
    } else if (OB_FAIL(op.get_backup_set_array(backup_set_desc_array))) {
      LOG_WARN("fail to get backup set name array", K(ret), K(op));
    } else {
      ObBackupSetDescComparator cmp;
      HEAP_VARS_2((storage::ObExternTenantLocalityInfoDesc, locality_info),
                  (storage::ObExternBackupSetInfoDesc, backup_set_info)) {
        std::sort(backup_set_desc_array.begin(), backup_set_desc_array.end(), cmp);
        for (int64_t i = backup_set_desc_array.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
          const share::ObBackupSetDesc &backup_set_desc = backup_set_desc_array.at(i);
          backup_desc_.backup_set_id_ = backup_set_desc.backup_set_id_;
          backup_desc_.backup_type_.type_ = backup_set_desc.backup_type_.type_;
          backup_set_dest_.reset();
          if (OB_FAIL(ObBackupPathUtil::construct_backup_set_dest(
              get_backup_dest(), backup_desc_, backup_set_dest_))) {
            LOG_WARN("fail to construct backup set dest", K(ret));
          } else if (OB_FAIL(read_tenant_locality_info(locality_info))) {
            if (OB_BACKUP_FILE_NOT_EXIST == ret) {
              LOG_WARN("backup set info not exist", K(ret), K(backup_set_desc));
              ret = OB_SUCCESS;
              continue;
            } else {
              LOG_WARN("fail to read backup set info", K(ret), K(backup_set_desc));
            }
          } else if (OB_FAIL(read_backup_set_info(backup_set_info))) {
            if (OB_BACKUP_FILE_NOT_EXIST == ret) {
              LOG_WARN("backup set info not exist", K(ret), K(backup_set_desc));
              ret = OB_SUCCESS;
              continue;
            } else {
              LOG_WARN("fail to read backup set info", K(ret), K(backup_set_desc));
            }
          } else if (backup_set_info.backup_set_file_.tenant_compatible_ < DATA_VERSION_4_2_0_0) {
            const char *time_zone = "+08:00";
            int32_t offset = 0;
            int ret_more = OB_SUCCESS;
            bool is_oracle_mode = locality_info.compat_mode_ == lib::Worker::CompatMode::ORACLE;
            if (OB_FAIL(ObTimeConverter::str_to_offset(time_zone,
                                                       offset,
                                                       ret_more,
                                                       is_oracle_mode))) {
              LOG_WARN("invalid time zone offset", K(ret), K(time_zone), K(offset), K(is_oracle_mode));
            } else {
              time_zone_wrap.set_tz_info_offset(offset);
            }
          } else if (OB_FAIL(time_zone_wrap.deep_copy(locality_info.sys_time_zone_wrap_))) {
            LOG_WARN("failed to deep copy time zone wrap", K(ret), K(locality_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupDataStore::do_get_backup_set_array_(const common::ObString &passwd_array, 
    const SCN &restore_scn, const ObBackupSetFilter &op,
    common::ObIArray<share::ObRestoreBackupSetBriefInfo> &tmp_backup_set_list, 
    int64_t &cur_max_backup_set_id, SCN &restore_start_scn)
{
  int ret = OB_SUCCESS;
  const int64_t OB_BACKUP_MAX_BACKUP_SET_ID = 5000;
  ObBackupSetFilter::BackupSetMap backup_set_map;
  ObSArray<share::ObBackupSetDesc> backup_set_desc_array;
  storage::ObExternBackupSetInfoDesc backup_set_info;
  if (OB_FAIL(backup_set_map.create(OB_BACKUP_MAX_BACKUP_SET_ID, "backupSetMap"))) {
    LOG_WARN("fail to create backup set map", K(ret));
  } else if (OB_FAIL(op.get_backup_set_array(backup_set_desc_array))) {
    LOG_WARN("fail to get backup set name array", K(ret), K(op));
  } else {
    ObBackupSetDescComparator cmp;
    std::sort(backup_set_desc_array.begin(), backup_set_desc_array.end(), cmp);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_desc_array.count(); ++i) {
    share::ObBackupSetDesc &backup_set_desc = backup_set_desc_array.at(i);
    backup_desc_.backup_set_id_ = backup_set_desc.backup_set_id_;
    backup_desc_.backup_type_.type_ = backup_set_desc.backup_type_.type_;
    backup_set_dest_.reset();
    if (OB_FAIL(ObBackupPathUtil::construct_backup_set_dest(
        get_backup_dest(), backup_desc_, backup_set_dest_))) {
      LOG_WARN("fail to construct backup set dest", K(ret));
    } else if (OB_FAIL(read_backup_set_info(backup_set_info))) {
      if (OB_BACKUP_FILE_NOT_EXIST == ret) {
        LOG_WARN("backup set info not exist", K(ret), K(backup_set_desc));
        ret = OB_SUCCESS;
        continue;
      } else {
        LOG_WARN("fail to read backup set info", K(ret), K(backup_set_desc));
      }
    } else {
      const share::ObBackupSetFileDesc &backup_set_file = backup_set_info.backup_set_file_;
      if (OB_FAIL(backup_set_file.check_passwd(passwd_array.ptr()))) {
        LOG_WARN("fail to check passwd", K(ret));
      } else if (backup_set_file.min_restore_scn_ > restore_scn) {
        // backup set file's min restore log ts > restore end log ts, can not be used to restore
        LOG_INFO("min restore scn of backup set file is greater than restore scn. can't use to restore.", 
            K(ret), K(backup_set_file), K(restore_scn));
        break;
      } else if (share::ObBackupSetFileDesc::BackupSetStatus::SUCCESS != backup_set_file.status_
          || share::ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE != backup_set_file.file_status_) {
        LOG_WARN("invalid status backup set can not be used to restore", K(backup_set_file));
      } else if (backup_set_file.backup_type_.is_full_backup()) {
        backup_set_desc.min_restore_scn_ = backup_set_file.min_restore_scn_;
        backup_set_desc.total_bytes_ = backup_set_file.stats_.output_bytes_;
        if (OB_FAIL(backup_set_map.clear())) {
          LOG_WARN("fail to clear backup set map", K(ret));
        } else if (OB_FAIL(backup_set_map.set_refactored(backup_set_file.backup_set_id_, backup_set_desc))) {
          LOG_WARN("fail to set refactored backup set map", K(ret), "backup set id", backup_set_file.backup_set_id_, K(backup_set_file));
        } else {
          restore_start_scn = backup_set_file.start_replay_scn_;
          LOG_INFO("find one full backup set", K(backup_set_file));
        }
      } else if (backup_set_file.backup_type_.is_inc_backup()) {
        share::ObBackupSetDesc value;
        value.backup_set_id_ = backup_set_file.prev_full_backup_set_id_;
        backup_set_desc.min_restore_scn_ = backup_set_file.min_restore_scn_;
        backup_set_desc.total_bytes_ = backup_set_file.stats_.output_bytes_;
        if (OB_FAIL(backup_set_map.get_refactored(backup_set_file.prev_full_backup_set_id_, value))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get refactored", K(ret), K(backup_set_file));
          }
        } else if (OB_FAIL(backup_set_map.get_refactored(backup_set_file.prev_inc_backup_set_id_, value))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get refactored", K(ret), K(backup_set_file));
          }
        } else if (OB_FAIL(backup_set_map.set_refactored(backup_set_file.backup_set_id_, backup_set_desc))) {
          LOG_WARN("fail to set refactored backup set map", K(ret), "backup set id", backup_set_file.backup_set_id_, 
              K(backup_set_desc));
        } else {
          restore_start_scn = backup_set_file.start_replay_scn_;
          LOG_INFO("find one inc backup set", K(backup_set_file));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid backup set type", K(ret), K(backup_set_file));
      }
    }
  }

  share::ObBackupDest backup_set_dest;
  ObRestoreBackupSetBriefInfo tmp_backup_set_brief_info;
  ObBackupSetFilter::BackupSetMap::iterator iter = backup_set_map.begin();
  for ( ; OB_SUCC(ret) && iter != backup_set_map.end(); ++iter) {
    // construct path which is include root_path, host and storage info
    const share::ObBackupSetDesc &desc = iter->second;
    tmp_backup_set_brief_info.reset();
    tmp_backup_set_brief_info.backup_set_desc_ = desc;
    if (OB_FAIL(ObBackupPathUtil::construct_backup_set_dest(get_backup_dest(), desc, backup_set_dest))) {
      LOG_WARN("fail to get backup set path", K(ret), "backup_dest", get_backup_dest(), K(desc));
    } else if (OB_FAIL(backup_set_dest.get_backup_dest_str(tmp_backup_set_brief_info.backup_set_path_.ptr(), 
        tmp_backup_set_brief_info.backup_set_path_.capacity()))) {
      LOG_WARN("fail to get backup set path", K(backup_set_dest));
    } else if (OB_FAIL(tmp_backup_set_list.push_back(tmp_backup_set_brief_info))) {
      LOG_WARN("fail to push path", K(ret), K(tmp_backup_set_brief_info));
    } else {
      cur_max_backup_set_id = OB_MAX(cur_max_backup_set_id, iter->first);
    }
  }

  FLOG_INFO("find all backup set for restore", K(tmp_backup_set_list));
  return ret;
}

int ObBackupDataStore::parse_backup_set_name(
    const ObBackupSetFilter::BackupSetDirName &backup_up_set_name,
    share::ObBackupSetDesc &backup_desc)
{
  int ret = OB_SUCCESS;
  char buf[ObBackupSetFilter::OB_BACKUP_SET_DIR_NAME_LEN] = { 0 };
  if (2 != sscanf(backup_up_set_name.ptr(), "backup_set_%ld_%5s", &backup_desc.backup_set_id_, buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid backup set name", K(ret), K(backup_up_set_name));
  } else if (0 == STRCMP(buf, OB_STR_FULL_BACKUP)) {
    backup_desc.backup_type_.type_ = ObBackupType::BackupType::FULL_BACKUP;
  } else if (0 == STRCMP(buf, OB_STR_INC_BACKUP)) {
    backup_desc.backup_type_.type_ = ObBackupType::BackupType::INCREMENTAL_BACKUP;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid backup set name", K(ret), K(backup_up_set_name));
  }
  return ret;
}

int ObBackupDataStore::get_max_sys_ls_retry_id(const share::ObBackupPath &backup_path,
    const ObLSID &ls_id, const int64_t turn_id,  int64_t &retry_id)
{
  int ret = OB_SUCCESS;
  retry_id = -1;
  ObBackupIoAdapter util;
  ObArray<ObIODirentEntry> d_entrys;
  char sys_data_prefix[OB_BACKUP_DIR_PREFIX_LENGTH] = { 0 };
  ObDirPrefixEntryNameFilter prefix_op(d_entrys);
  if (backup_path.is_empty() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_path), K(ls_id));
  } else if (OB_FAIL(databuff_printf(sys_data_prefix, OB_BACKUP_DIR_PREFIX_LENGTH, "sys_data_turn_%ld_retry_", turn_id))) {
    LOG_WARN("failed to printf sys data prefix", K(ret));
  } else if (OB_FAIL(prefix_op.init(sys_data_prefix, static_cast<int32_t>(strlen(sys_data_prefix))))) {
    LOG_WARN("failed to init dir prefix", K(ret), K(sys_data_prefix));
  } else if (OB_FAIL(util.list_directories(backup_path.get_obstr(), backup_set_dest_.get_storage_info(), prefix_op))) {
    LOG_WARN("failed to list files", K(ret), K(backup_path), K(backup_set_dest_));
  } else {
    ObIODirentEntry tmp_entry;
    int64_t max_retry_id = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < d_entrys.count(); ++i) {
      int64_t id_val = 0;
      tmp_entry = d_entrys.at(i);
      bool match = false;
      if (OB_ISNULL(tmp_entry.name_)) {
        ret = OB_ERR_UNEXPECTED; 
        LOG_WARN("file name is null", K(ret));
      } else if (OB_FAIL(extract_id_from_str(tmp_entry.name_, sys_data_prefix, id_val, match))) {
        LOG_WARN("failed to parse ls id", K(ret));
      } else if (!match) {
        LOG_INFO("not match", K(tmp_entry.name_), K(sys_data_prefix));
        continue;
      } else {
        if (id_val > max_retry_id) {
          max_retry_id = id_val;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (-1 == max_retry_id) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no retry exist", K(ret), K(backup_path), K(ls_id), K(d_entrys));
      } else {
        retry_id = max_retry_id;
      }
    }
  }
  return ret;
}

int ObBackupDataStore::extract_id_from_str(const common::ObString &file_name, const common::ObString &prefix, int64_t &file_id, bool &match)
{
  int ret = OB_SUCCESS;
  file_id = -1;
  const char *str = file_name.ptr();
  int64_t tmp_file_id = -1;
  match = false;
  if (OB_UNLIKELY(file_name.empty()) || OB_ISNULL(str) || OB_UNLIKELY('\0' == (*str))) {
    match = false;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(file_name));
  } else if (!file_name.prefix_match(prefix)) {
    LOG_INFO("prefix not match", K(file_name), K(prefix));
    match = false;
  } else if (file_name.length() <= prefix.length()) {
    LOG_INFO("file length not match", K(file_name), K(prefix));
    match = false;
  } else {
    const int64_t suffix_length = file_name.length() - prefix.length();
    const char *suffix_ptr = file_name.ptr() + prefix.length();
    ObString suffix_str = ObString(suffix_length, suffix_ptr);
    char *end_ptr = NULL;
    if (OB_FAIL(ob_strtoll(suffix_ptr, end_ptr, tmp_file_id))) {
      LOG_WARN("failed to convert str to ll", K(suffix_ptr), K(tmp_file_id));
    } else {
      file_id = tmp_file_id;
      match = true;
    }
    LOG_INFO("extract backup id", K(match), K(file_name), K(prefix), K(tmp_file_id), K(file_id), K(suffix_ptr), K(suffix_length));
  }
  return ret;
  
}

int ObBackupDataStore::read_base_tablet_list(const share::ObLSID &ls_id, ObIArray<common::ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  backup::ObExternTabletMetaReader reader;
  if (OB_FAIL(reader.init(backup_set_dest_, ls_id))) {
    LOG_WARN("fail to init reader", K(ret), K(backup_set_dest_), K(ls_id));
  } else {
    storage::ObMigrationTabletParam tablet_meta;
    while (OB_SUCC(ret)) {
      tablet_meta.reset();
      if (OB_FAIL(reader.get_next(tablet_meta))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next tablet meta", K(ret));
        }
      } else if (OB_FAIL(tablet_id_array.push_back(tablet_meta.tablet_id_))) {
        LOG_WARN("failed to push backup tablet id", K(ret), K(tablet_meta));
      }
    }
  }
  return ret;
}

int ObBackupDataStore::read_tablet_to_ls_info_v_4_1_x(
    const int64_t turn_id, const ObLSID &ls_id, ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath path;
  ObBackupPathString full_path;
  ObBackupDataTabletToLSDesc tablet_to_ls_info;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataStore not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtilV_4_1::get_backup_data_tablet_ls_info_path(backup_set_dest_, turn_id, path))) {
    LOG_WARN("fail to get tenant ls attr info path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(read_single_file(full_path, tablet_to_ls_info))) {
    LOG_WARN("failed to read single file", K(ret), K(full_path));
  } else if (!tablet_to_ls_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet to ls info", K(ret), K(tablet_to_ls_info));
  }
  for (int i = 0; OB_SUCC(ret) && i < tablet_to_ls_info.tablet_to_ls_.count(); ++i) {
    const ObBackupDataTabletToLSInfo &info = tablet_to_ls_info.tablet_to_ls_.at(i);
    if (!info.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid info", K(ret), K(info));
    } else if (info.ls_id_ == ls_id) {
      if (OB_FAIL(append(tablet_ids, info.tablet_id_list_))) {
        LOG_WARN("failed to append tablet ids", K(ret), K(info));
      } else {
        break;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (tablet_ids.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("read no tablet", K(ret), K(turn_id), K(ls_id), K(full_path));
  }
  return ret;
}

int ObBackupDataStore::read_deleted_tablet_info_v_4_1_x(
    const ObLSID &ls_id, ObIArray<ObTabletID> &deleted_tablet_ids)
{
  int ret = OB_SUCCESS;
  deleted_tablet_ids.reset();
  share::ObBackupPath path;
  ObBackupPathString full_path;
  ObBackupDeletedTabletToLSDesc deleted_tablet_info;
  if (!is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupDataStore not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_deleted_tablet_info_path(backup_set_dest_, path))) {
    LOG_WARN("fail to get tenant ls attr info path", K(ret));
  } else if (OB_FAIL(full_path.assign(path.get_obstr()))) {
    LOG_WARN("fail to assign full path", K(ret));
  } else if (OB_FAIL(read_single_file(full_path, deleted_tablet_info))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("backup deleted file not exist", K(ret));
    } else {
      LOG_WARN("failed to read single file", K(ret), K(full_path));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < deleted_tablet_info.deleted_tablet_to_ls_.count(); ++i) {
      const ObBackupDataTabletToLSInfo &info = deleted_tablet_info.deleted_tablet_to_ls_.at(i);
      if (info.ls_id_ == ls_id) {
        if (OB_FAIL(deleted_tablet_ids.assign(info.tablet_id_list_))) {
          LOG_WARN("failed to assign", K(ret), K(info));
        }
        break;
      }
    }
  }
  return ret;
}
