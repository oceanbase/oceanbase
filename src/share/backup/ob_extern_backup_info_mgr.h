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

#ifndef OCEANBASE_SHARE_BACKUP_OB_EXTERN_BACKUP_INFO_MGR_H_
#define OCEANBASE_SHARE_BACKUP_OB_EXTERN_BACKUP_INFO_MGR_H_
#include "ob_backup_info_mgr.h"
#include "ob_backup_struct.h"
#include "ob_backup_path.h"
#include "rootserver/ob_freeze_info_manager.h"
#include "ob_backup_file_lock_mgr.h"
#include "ob_backup_lease_info_mgr.h"

namespace oceanbase {
namespace share {

class ObIExternInfos {
public:
  ObIExternInfos()
  {}
  virtual ~ObIExternInfos()
  {}
  virtual int write_buf(char* buf, const int64_t buf_len, int64_t& pos) const = 0;
  virtual int read_buf(const char* buf, const int64_t buf_len) = 0;
  virtual int64_t get_write_buf_size() const = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIExternInfos);
};

class ObExternBackupInfos : public ObIExternInfos {
  static const uint8_t VERSION = 1;
  OB_UNIS_VERSION(VERSION);

public:
  ObExternBackupInfos();
  virtual ~ObExternBackupInfos();

  void reset();
  bool is_valid() const;
  virtual int64_t get_write_buf_size() const;
  virtual int write_buf(char* buf, const int64_t buf_len, int64_t& pos) const;
  virtual int read_buf(const char* buf, const int64_t buf_len);
  int update(const ObExternBackupInfo& extern_backup_info);
  int get_last(ObExternBackupInfo& extern_backup_info);
  int get_last_succeed_info(ObExternBackupInfo& extern_backup_info);
  int add(const ObExternBackupInfo& extern_backup_info);
  int get_extern_backup_infos(common::ObIArray<ObExternBackupInfo>& extern_backup_infos);
  int get_extern_full_backup_infos(common::ObIArray<ObExternBackupInfo>& extern_backup_infos);
  int find_backup_info(
      const int64_t restore_snapshot_version, const char* passwd_array, ObExternBackupInfo& backup_info);
  int mark_backup_info_deleted(const int64_t backup_set_id);
  int delete_marked_backup_info();
  int get_extern_full_backup_info(const int64_t full_backup_set_id, ObExternBackupInfo& extern_backup_info);
  int check_passwd(const char* passwd_array, const char* passwd);

  int64_t get_extern_backup_info_count()
  {
    return extern_backup_info_array_.count();
  }
  int try_finish_extern_backup_info(const int64_t backup_set_id);
  bool is_extern_backup_infos_modified() const
  {
    return is_modified_;
  }
  bool is_empty() const
  {
    return extern_backup_info_array_.empty();
  }

  TO_STRING_KV(K_(extern_backup_info_array));

private:
  common::ObSArray<ObExternBackupInfo> extern_backup_info_array_;
  // no need serialize
  bool is_modified_;
  DISALLOW_COPY_AND_ASSIGN(ObExternBackupInfos);
};

class ObExternBackupInfoMgr {
public:
  ObExternBackupInfoMgr();
  virtual ~ObExternBackupInfoMgr();
  int init(const uint64_t tenant_id, const ObClusterBackupDest& backup_dest,
      share::ObIBackupLeaseService& backup_lease_service);
  int check_can_backup(const ObExternBackupInfo& extern_backup_info);
  int upload_backup_info(const ObExternBackupInfo& extern_backup_info);
  int get_extern_backup_info(const ObBaseBackupInfoStruct& info, rootserver::ObFreezeInfoManager& freeze_info_mgr,
      ObExternBackupInfo& extern_backup_info);
  int find_backup_info(
      const int64_t restore_snapshot_version, const char* passwd_array, ObExternBackupInfo& extern_backup_info);
  int get_extern_backup_infos(common::ObIArray<ObExternBackupInfo>& extern_backup_infos);
  int get_extern_full_backup_infos(common::ObIArray<ObExternBackupInfo>& extern_backup_infos);
  int mark_backup_info_deleted(const common::ObIArray<int64_t>& backup_set_ids);
  int delete_marked_backup_info();
  int get_extern_full_backup_info(const int64_t full_backup_set_id, ObExternBackupInfo& extern_backup_info);
  bool is_empty() const
  {
    return extern_backup_infos_.is_empty();
  }
  bool is_extern_backup_infos_modified() const
  {
    return extern_backup_infos_.is_extern_backup_infos_modified();
  }
  int upload_backup_info();
  int get_last_info(ObExternBackupInfo& last_backup_info);

private:
  int get_extern_backup_infos(const uint64_t tenant_id, const ObClusterBackupDest& backup_dest);
  int get_last_succeed_info();
  int get_backup_path(const uint64_t tenant_id, const ObClusterBackupDest& backup_dest, ObBackupPath& path);
  int try_finish_extern_backup_info(const int64_t backup_set_id);
  int get_lastest_incremental_backup_count(int32_t& incremental_backup_count);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObClusterBackupDest backup_dest_;
  ObExternBackupInfos extern_backup_infos_;
  ObExternBackupInfo last_succeed_info_;
  ObBackupFileSpinLock lock_;
  share::ObIBackupLeaseService* backup_lease_service_;
  DISALLOW_COPY_AND_ASSIGN(ObExternBackupInfoMgr);
};

class ObExternBackupSetInfos : public ObIExternInfos {
  static const uint8_t VERSION = 1;
  OB_UNIS_VERSION(VERSION);

public:
  ObExternBackupSetInfos();
  virtual ~ObExternBackupSetInfos();

  void reset();
  bool is_valid() const;
  virtual int64_t get_write_buf_size() const;
  virtual int write_buf(char* buf, const int64_t buf_len, int64_t& pos) const;
  virtual int read_buf(const char* buf, const int64_t buf_len);
  int add(const ObExternBackupSetInfo& extern_backup_set_info);
  int get_extern_backup_set_infos(common::ObIArray<ObExternBackupSetInfo>& extern_backup_set_infos);

  TO_STRING_KV(K_(extern_backup_set_info_array));

private:
  common::ObSArray<ObExternBackupSetInfo> extern_backup_set_info_array_;
  DISALLOW_COPY_AND_ASSIGN(ObExternBackupSetInfos);
};

class ObExternBackupSetInfoMgr {
public:
  ObExternBackupSetInfoMgr();
  virtual ~ObExternBackupSetInfoMgr();
  int init(const uint64_t tenant_id, const int64_t full_backup_set_id, const ObClusterBackupDest& backup_dest,
      share::ObIBackupLeaseService& backup_lease_service);
  int upload_backup_set_info(const ObExternBackupSetInfo& extern_backup_set_info);
  int get_extern_backup_set_infos(common::ObIArray<ObExternBackupSetInfo>& extern_backup_set_infos);
  int touch_extern_backup_set_info();
  int delete_extern_backup_set_info();

private:
  int get_extern_backup_set_infos(
      const uint64_t tenant_id, const int64_t full_backup_set_id, const ObClusterBackupDest& backup_dest);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t full_backup_set_id_;
  ObClusterBackupDest backup_dest_;
  ObExternBackupSetInfos extern_backup_set_infos_;
  ObBackupFileSpinLock lock_;
  share::ObIBackupLeaseService* backup_lease_service_;
  DISALLOW_COPY_AND_ASSIGN(ObExternBackupSetInfoMgr);
};

class ObExternPGList : public ObIExternInfos {
  static const uint8_t VERSION = 1;
  OB_UNIS_VERSION(VERSION);

public:
  ObExternPGList();
  virtual ~ObExternPGList() = default;
  void reset();
  bool is_valid() const;
  virtual int64_t get_write_buf_size() const;
  virtual int write_buf(char* buf, const int64_t buf_len, int64_t& pos) const;
  virtual int read_buf(const char* buf, const int64_t buf_len);
  int add(const common::ObPGKey& pg_key);
  int get(common::ObIArray<common::ObPGKey>& pg_keys);
  TO_STRING_KV(K_(pkeys));
  common::ObSArray<common::ObPGKey> pkeys_;
};

class ObExternPGListMgr {
public:
  ObExternPGListMgr();
  virtual ~ObExternPGListMgr();
  int init(const uint64_t tenant_id, const int64_t full_backup_set_id, const int64_t inc_backup_set_id,
      const ObClusterBackupDest& backup_dest, share::ObIBackupLeaseService& backup_lease_service);
  int add_pg_key(const common::ObPGKey& pg_key);
  int upload_pg_list();
  int get_sys_pg_list(common::ObIArray<common::ObPGKey>& pg_keys);
  int get_normal_pg_list(common::ObIArray<common::ObPGKey>& pg_keys);

private:
  int upload_sys_pg_list();
  int upload_normal_pg_list();
  int get_extern_sys_pg_list();
  int get_extern_normal_pg_list();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t full_backup_set_id_;
  int64_t inc_backup_set_id_;
  ObClusterBackupDest backup_dest_;
  ObExternPGList extern_sys_pg_list_;
  ObExternPGList extern_normal_pg_list_;
  share::ObIBackupLeaseService* backup_lease_service_;
  DISALLOW_COPY_AND_ASSIGN(ObExternPGListMgr);
};

class ObExternTenantInfos : public ObIExternInfos {
  static const uint8_t VERSION = 1;
  OB_UNIS_VERSION(VERSION);

public:
  ObExternTenantInfos();
  virtual ~ObExternTenantInfos();

  void reset();
  bool is_valid() const;
  virtual int64_t get_write_buf_size() const;
  virtual int write_buf(char* buf, const int64_t buf_len, int64_t& pos) const;
  virtual int read_buf(const char* buf, const int64_t buf_len);
  int add(const ObExternTenantInfo& extern_tenant_info);
  int get_extern_tenant_infos(common::ObIArray<ObExternTenantInfo>& extern_tenant_infos);
  int find_tenant_info(const uint64_t tenant_id, ObExternTenantInfo& tenant_info);
  int delete_tenant_info(const uint64_t tenant_id);
  bool is_empty() const
  {
    return extern_tenant_info_array_.empty();
  }
  bool is_extern_tenant_infos_modified() const
  {
    return is_modified_;
  }

  TO_STRING_KV(K_(extern_tenant_info_array));

private:
  common::ObSArray<ObExternTenantInfo> extern_tenant_info_array_;
  // no need serialize
  bool is_modified_;
  DISALLOW_COPY_AND_ASSIGN(ObExternTenantInfos);
};

class ObExternTenantInfoMgr {
public:
  ObExternTenantInfoMgr();
  virtual ~ObExternTenantInfoMgr();
  int init(const ObClusterBackupDest& backup_dest, share::ObIBackupLeaseService& backup_lease_service);
  int add_tenant_info(const ObExternTenantInfo& tenant_info);
  int upload_tenant_infos();
  int get_extern_tenant_infos(common::ObIArray<ObExternTenantInfo>& tenant_infos);
  int find_tenant_info(const uint64_t tenant_id, ObExternTenantInfo& tenant_info);
  int delete_tenant_info(const uint64_t tenant_id);
  bool is_empty() const;
  bool is_extern_tenant_infos_modified() const
  {
    return extern_tenant_infos_.is_extern_tenant_infos_modified();
  }

private:
  int get_extern_tenant_infos(const ObClusterBackupDest& backup_dest);

private:
  bool is_inited_;
  ObClusterBackupDest backup_dest_;
  ObExternTenantInfos extern_tenant_infos_;
  ObBackupFileSpinLock lock_;
  share::ObIBackupLeaseService* backup_lease_service_;
  DISALLOW_COPY_AND_ASSIGN(ObExternTenantInfoMgr);
};

class ObExternTenantLocality : public ObIExternInfos {
  static const uint8_t VERSION = 1;
  OB_UNIS_VERSION(VERSION);

public:
  ObExternTenantLocality();
  virtual ~ObExternTenantLocality();

  void reset();
  bool is_valid() const;
  virtual int64_t get_write_buf_size() const;
  virtual int write_buf(char* buf, const int64_t buf_len, int64_t& pos) const;
  virtual int read_buf(const char* buf, const int64_t buf_len);
  ObExternTenantLocalityInfo& get_tenant_locality_info()
  {
    return extern_tenant_locality_info_;
  }
  int set_tenant_locality_info(const ObExternTenantLocalityInfo& tenant_locality_info);
  TO_STRING_KV(K_(extern_tenant_locality_info));

private:
  ObExternTenantLocalityInfo extern_tenant_locality_info_;
  DISALLOW_COPY_AND_ASSIGN(ObExternTenantLocality);
};

class ObExternTenantLocalityInfoMgr {
public:
  ObExternTenantLocalityInfoMgr();
  virtual ~ObExternTenantLocalityInfoMgr();
  int init(const uint64_t tenant_id, const int64_t full_backup_set_id, const int64_t inc_backup_set_id,
      const ObClusterBackupDest& backup_dest, share::ObIBackupLeaseService& backup_lease_service);
  int upload_tenant_locality_info(const ObExternTenantLocalityInfo& tenant_locality_info);
  int get_extern_tenant_locality_info(ObExternTenantLocalityInfo& tenant_locality_info);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t full_backup_set_id_;
  int64_t inc_backup_set_id_;
  ObClusterBackupDest backup_dest_;
  ObExternTenantLocality extern_tenant_locality_;
  share::ObIBackupLeaseService* backup_lease_service_;
  DISALLOW_COPY_AND_ASSIGN(ObExternTenantLocalityInfoMgr);
};

class ObExternTenantBackupDiagnoseMgr {
public:
  ObExternTenantBackupDiagnoseMgr();
  virtual ~ObExternTenantBackupDiagnoseMgr();
  int init(const uint64_t tenant_id, const int64_t full_backup_set_id, const int64_t inc_backup_set_id,
      const ObClusterBackupDest& backup_dest, share::ObIBackupLeaseService& backup_lease_service);
  int upload_tenant_backup_diagnose_info(const ObExternBackupDiagnoseInfo& diagnose_info);

private:
  int64_t get_write_buf_size() const;
  int write_buf(const ObExternBackupDiagnoseInfo& diagnose_info, char* buf, const int64_t buf_len, int64_t& pos) const;

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t full_backup_set_id_;
  int64_t inc_backup_set_id_;
  ObClusterBackupDest backup_dest_;
  share::ObIBackupLeaseService* backup_lease_service_;
  DISALLOW_COPY_AND_ASSIGN(ObExternTenantBackupDiagnoseMgr);
};

}  // namespace share
}  // namespace oceanbase

#endif /* OCEANBASE_SHARE_BACKUP_OB_EXTERN_BACKUP_INFO_MGR_H_ */
