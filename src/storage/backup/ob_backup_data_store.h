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

#ifndef OCEANBASE_SHARE_OB_BACKUP_DATA_STORE_H_
#define OCEANBASE_SHARE_OB_BACKUP_DATA_STORE_H_

#include "share/ls/ob_ls_operator.h"
#include "share/backup/ob_backup_path.h"
#include "storage/backup/ob_backup_extern_info_mgr.h"
#include "storage/ls/ob_ls_meta_package.h"
#include "storage/tablet/ob_tablet_meta.h"

namespace oceanbase
{
namespace storage
{

/*
------------------------backup ls info-------------------------
*/

struct ObBackupDataLSAttrDesc final : public ObExternBackupDataDesc
{
public:
  static const uint8_t FILE_VERSION = 1; 
  OB_UNIS_VERSION(1);
public:
  ObBackupDataLSAttrDesc() 
    : ObExternBackupDataDesc(share::ObBackupFileType::BACKUP_LS_INFO, FILE_VERSION),
      backup_scn_(),
      ls_attr_array_() {}
  virtual ~ObBackupDataLSAttrDesc() {}

  bool is_valid() const override;
  INHERIT_TO_STRING_KV("ObExternBackupDataDesc", ObExternBackupDataDesc, K(backup_scn_), K_(ls_attr_array));
public:
  share::SCN backup_scn_;
  ObSArray<share::ObLSAttr> ls_attr_array_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataLSAttrDesc);
};

struct ObBackupDataTabletToLSInfo 
{
public:
  OB_UNIS_VERSION(1);
public:
  ObBackupDataTabletToLSInfo()
    : ls_id_(0),
      tablet_id_list_() {}
  virtual ~ObBackupDataTabletToLSInfo() {};

  bool is_valid() const;
  int assign(const ObBackupDataTabletToLSInfo &that);
  TO_STRING_KV(K_(ls_id), K_(tablet_id_list));
  share::ObLSID ls_id_;
  ObSArray<common::ObTabletID> tablet_id_list_;
};

struct ObBackupDataTabletToLSDesc final : public ObExternBackupDataDesc
{
public:
  static const uint8_t FILE_VERSION = 1; 
  OB_UNIS_VERSION(1);
public:
  ObBackupDataTabletToLSDesc()
    : ObExternBackupDataDesc(share::ObBackupFileType::BACKUP_TABLET_TO_LS_INFO, FILE_VERSION),
      backup_scn_(share::SCN::min_scn()),
      tablet_to_ls_() {}
  virtual ~ObBackupDataTabletToLSDesc() {}

  bool is_valid() const override;
  INHERIT_TO_STRING_KV("ObExternBackupDataDesc", ObExternBackupDataDesc, K_(backup_scn), K_(tablet_to_ls));
public:
  share::SCN backup_scn_;
  ObSArray<ObBackupDataTabletToLSInfo> tablet_to_ls_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataTabletToLSDesc);
};

struct ObBackupDeletedTabletToLSDesc final : public ObExternBackupDataDesc
{
public:
  static const uint8_t FILE_VERSION = 1;
  OB_UNIS_VERSION(FILE_VERSION);
public:
  ObBackupDeletedTabletToLSDesc()
    : ObExternBackupDataDesc(ObBackupFileType::BACKUP_DELETED_TABLET_INFO, FILE_VERSION),
      deleted_tablet_to_ls_() {}
  virtual ~ObBackupDeletedTabletToLSDesc() {}

  bool is_valid() const override;
  INHERIT_TO_STRING_KV("ObExternBackupDataDesc", ObExternBackupDataDesc, K_(deleted_tablet_to_ls));
public:
  ObSArray<ObBackupDataTabletToLSInfo> deleted_tablet_to_ls_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupDeletedTabletToLSDesc);
};

struct ObExternBackupSetPlaceholderDesc final : public ObExternBackupDataDesc
{
public:
  static const uint8_t FILE_VERSION = 1; 
  OB_UNIS_VERSION(1);
public:
  ObExternBackupSetPlaceholderDesc() 
    : ObExternBackupDataDesc(share::ObBackupFileType::BACKUP_TABLET_TO_LS_INFO, FILE_VERSION) {}
  virtual ~ObExternBackupSetPlaceholderDesc() {}

  bool is_valid() const override { return true; }
};

struct ObExternTenantLocalityInfoDesc final : public ObExternBackupDataDesc
{
public:
  using TenantName = common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH>;
  using ClusterName = common::ObFixedLengthString<common::OB_MAX_CLUSTER_NAME_LENGTH>;
  using Locality = common::ObFixedLengthString<common::MAX_LOCALITY_LENGTH>;
  using PrimaryZone = common::ObFixedLengthString<common::MAX_ZONE_LIST_LENGTH>;
  using TimeZone = common::ObFixedLengthString<common::OB_MAX_TIMESTAMP_TZ_LENGTH>;
  static const uint8_t FILE_VERSION = 1; 
  OB_UNIS_VERSION(1);
public:
  ObExternTenantLocalityInfoDesc()
    : ObExternBackupDataDesc(share::ObBackupFileType::BACKUP_TENANT_LOCALITY_INFO, FILE_VERSION),
      tenant_id_(OB_INVALID_TENANT_ID),
      backup_set_id_(0),
      cluster_id_(OB_INVALID_CLUSTER_ID),
      compat_mode_(),
      tenant_name_(),
      cluster_name_(),
      locality_(),
      primary_zone_(),
      sys_time_zone_(),
      sys_time_zone_wrap_() {}
  virtual ~ObExternTenantLocalityInfoDesc() {}
  int assign(const ObExternTenantLocalityInfoDesc &that);
  bool is_valid() const override;
  INHERIT_TO_STRING_KV("ObExternBackupDataDesc", ObExternBackupDataDesc, K_(tenant_id), K_(backup_set_id), K_(cluster_id), 
      K_(compat_mode), K_(tenant_name), K_(cluster_name), K_(locality), K_(primary_zone), K_(sys_time_zone),
      K_(sys_time_zone_wrap));
public:
  uint64_t tenant_id_;
  int64_t backup_set_id_;
  int64_t cluster_id_;
  lib::Worker::CompatMode compat_mode_;
  TenantName tenant_name_;
  ClusterName cluster_name_;
  Locality locality_;
  PrimaryZone primary_zone_;
  TimeZone sys_time_zone_;
  ObTimeZoneInfoWrap sys_time_zone_wrap_;
};

struct ObExternBackupSetInfoDesc final : public ObExternBackupDataDesc
{
public:
  static const uint8_t FILE_VERSION = 1; 
  OB_UNIS_VERSION(1);
public:
  ObExternBackupSetInfoDesc()
    : ObExternBackupDataDesc(share::ObBackupFileType::BACKUP_SET_INFO, FILE_VERSION),
      backup_set_file_() {}
  virtual ~ObExternBackupSetInfoDesc() {}

  bool is_valid() const override;
  INHERIT_TO_STRING_KV("ObExternBackupDataDesc", ObExternBackupDataDesc, K_(backup_set_file));
public:
  share::ObBackupSetFileDesc backup_set_file_;
};

struct ObExternTenantDiagnoseInfoDesc final : public ObExternBackupDataDesc
{
public:
  static const uint8_t FILE_VERSION = 1; 
  OB_UNIS_VERSION(1);
public:
  ObExternTenantDiagnoseInfoDesc()
    : ObExternBackupDataDesc(share::ObBackupFileType::BACKUP_TENANT_DIAGNOSE_INFO, FILE_VERSION),
      tenant_id_(OB_INVALID_TENANT_ID), 
      tenant_locality_info_(), 
      backup_set_file_() {}
  virtual ~ObExternTenantDiagnoseInfoDesc() {}

  bool is_valid() const override;
  INHERIT_TO_STRING_KV("ObExternBackupDataDesc", ObExternBackupDataDesc, K_(tenant_id), K_(tenant_locality_info), K_(backup_set_file));
public:
  uint64_t tenant_id_;
  ObExternTenantLocalityInfoDesc tenant_locality_info_;
  share::ObBackupSetFileDesc backup_set_file_;
};

struct ObTenantBackupSetInfosDesc final : public ObExternBackupDataDesc
{
public:
  static const uint8_t FILE_VERSION = 1; 
  OB_UNIS_VERSION(1);
public:
  ObTenantBackupSetInfosDesc()
    : ObExternBackupDataDesc(share::ObBackupFileType::BACKUP_TENANT_SET_INFOS, FILE_VERSION),
      backup_set_infos_() {}
  virtual ~ObTenantBackupSetInfosDesc() {}

  bool is_valid() const override;
  TO_STRING_KV(K_(backup_set_infos));
public:
  ObSArray<share::ObBackupSetFileDesc> backup_set_infos_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantBackupSetInfosDesc);
};

struct ObBackupLSMetaInfosDesc final: public ObExternBackupDataDesc
{
public:
  static const uint8_t FILE_VERSION = 1; 
  OB_UNIS_VERSION(1);
public:
  ObBackupLSMetaInfosDesc()
    : ObExternBackupDataDesc(share::ObBackupFileType::BACKUP_LS_META_INFOS_FILE, FILE_VERSION),
      ls_meta_packages_() {}
  virtual ~ObBackupLSMetaInfosDesc() {}
  bool is_valid() const override;
  TO_STRING_KV(K_(ls_meta_packages));
public:
  ObSArray<storage::ObLSMetaPackage> ls_meta_packages_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupLSMetaInfosDesc);
};

class ObBackupSetFilter : public ObBaseDirEntryOperator
{
public:
  static const int64_t OB_BACKUP_SET_DIR_NAME_LEN = 64;
  using BackupSetDirName = common::ObFixedLengthString<OB_BACKUP_SET_DIR_NAME_LEN>;
  using BackupSetMap = common::hash::ObHashMap<int64_t, share::ObBackupSetDesc, common::hash::NoPthreadDefendMode>;
public:
  ObBackupSetFilter()
   : backup_set_name_array_() {}
  ~ObBackupSetFilter() {}
  void reset() { backup_set_name_array_.reuse(); }
  int get_backup_set_array(ObIArray<share::ObBackupSetDesc> &backup_set_array) const;
  int func(const dirent *entry) override;
  TO_STRING_KV(K_(backup_set_name_array));
private:
  ObSArray<BackupSetDirName> backup_set_name_array_;
};

class ObBackupDataStore final : public share::ObBackupStore
{
public:
  ObBackupDataStore();
  virtual ~ObBackupDataStore();

  int init(const char *backup_set_dest);
  int init(const share::ObBackupDest &backup_set_dest);
  int init(const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_desc);
  const share::ObBackupSetDesc &get_backup_set_desc() const { return backup_desc_; }
  const share::ObBackupDest &get_backup_set_dest() const { return backup_set_dest_; }

  int write_ls_attr(const int64_t turn_id, const ObBackupDataLSAttrDesc &ls_info); 
  int read_ls_attr_info(ObBackupDataLSAttrDesc &ls_info);
  int read_ls_attr_info(const int64_t turn_id, ObBackupDataLSAttrDesc &ls_info);

  int write_ls_meta_infos(const ObBackupLSMetaInfosDesc &ls_meta_infos);
  // get the ObBackupLSMetaInfosDesc of target turn_id
  int read_ls_meta_infos(ObBackupLSMetaInfosDesc &ls_meta_infos);
  // get the ls_meta_infos of target turn_id and ls_id
  int read_ls_meta_infos(const share::ObLSID &ls_id, storage::ObLSMetaPackage &ls_meta_package);
  // write and read tablet_to_ls_info
  int write_tablet_to_ls_info(const ObBackupDataTabletToLSDesc &tablet_to_ls_info,
      const int64_t turn_id, const share::ObBackupDataType &type);
  int read_tablet_to_ls_info(const int64_t turn_id, const share::ObBackupDataType &type,
      ObBackupDataTabletToLSDesc &tablet_to_ls_info);
  int read_tablet_list(const share::ObBackupDataType &type, const int64_t turn_id, const share::ObLSID &ls_id,
      ObIArray<common::ObTabletID> &tablet_ids);
  int read_deleted_tablet_info(const ObLSID &ls_id, ObIArray<ObTabletID> &deleted_tablet_ids);
  
  // write tenant backup set infos
  int write_tenant_backup_set_infos(const ObTenantBackupSetInfosDesc &tenant_backup_set_infos);

  // write backup set place holder
  int write_backup_set_placeholder(const bool is_inner, const bool is_start, const bool is_succeed, 
      const share::SCN &replay_scn, const share::SCN &min_restore_scn);

  // write and read tenant locality info
  int write_tenant_locality_info(const ObExternTenantLocalityInfoDesc &locality_info);
  int read_tenant_locality_info(ObExternTenantLocalityInfoDesc &locality_info);

  // write and read tenant dignose info
  int write_tenant_diagnose_info(const ObExternTenantDiagnoseInfoDesc &diagnose_info);
  int read_tenant_diagnose_info(ObExternTenantDiagnoseInfoDesc &diagnose_info);

  // write and read backup set info
  int write_backup_set_info(const ObExternBackupSetInfoDesc &backup_set_info);
  int read_backup_set_info(ObExternBackupSetInfoDesc &backup_set_info);

  int get_backup_set_array(const common::ObString &passwd_array, const share::SCN &restore_scn,
      share::SCN &restore_start_scn, common::ObIArray<share::ObRestoreBackupSetBriefInfo> &backup_set_list);
  int get_max_backup_set_file_info(const common::ObString &passwd_array, ObBackupSetFileDesc &output_desc);
  int get_backup_sys_time_zone_wrap(common::ObTimeZoneInfoWrap & time_zone_wrap);
  int get_max_sys_ls_retry_id(
      const share::ObBackupPath &backup_path, const share::ObLSID &ls_id, const int64_t turn_id, int64_t &retry_id);
  int write_root_key_info(const uint64_t tenant_id);
  int read_root_key_info(const uint64_t tenant_id);

  int read_base_tablet_list(const share::ObLSID &ls_id, ObIArray<common::ObTabletID> &tablet_id_array);
// 4.1 interface to get tablet to ls
  int read_tablet_to_ls_info_v_4_1_x(const int64_t turn_id, const ObLSID &ls_id, ObIArray<ObTabletID> &tablet_ids);
  int read_deleted_tablet_info_v_4_1_x(const ObLSID &ls_id, ObIArray<ObTabletID> &deleted_tablet_ids);

  TO_STRING_KV(K_(backup_desc));

public:
  static int parse_backup_set_name(const ObBackupSetFilter::BackupSetDirName &backup_up_set_name,
      share::ObBackupSetDesc &backup_desc);
  static int extract_id_from_str(const common::ObString &file_name, const common::ObString &prefix, int64_t &file_id, bool &match);
      
  struct ObBackupSetDescComparator
  {
    bool operator()(const share::ObBackupSetDesc &left, const share::ObBackupSetDesc &right) const
    {
      return left.backup_set_id_ < right.backup_set_id_;
    }
  };

    struct ObBackupSetFileDescComparator
  {
    bool operator()(const share::ObBackupSetFileDesc &left, const share::ObBackupSetFileDesc &right) const
    {
      return left.backup_set_id_ < right.backup_set_id_;
    }
  };
private:
  int do_get_backup_set_array_(const common::ObString &passwd_array, const share::SCN &restore_scn,
      const ObBackupSetFilter &op, common::ObIArray<share::ObRestoreBackupSetBriefInfo> &tmp_backup_set_list, 
      int64_t &cur_max_backup_set_id, share::SCN &restore_start_scn);
  int get_backup_set_placeholder_path_(const bool is_inner, const bool is_start, const bool is_succeed, 
      const share::SCN &replay_scn, const share::SCN &min_restore_scn, share::ObBackupPath &path);
private:
  share::ObBackupSetDesc backup_desc_;
  share::ObBackupDest backup_set_dest_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataStore);
};

}
}
#endif  // OCEANBASE_SHARE_OB_BACKUP_DATA_STORE_H_
