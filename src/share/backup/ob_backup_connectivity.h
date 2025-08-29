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

#ifndef OCEANBASE_SHARE_BACKUP_OB_BACKUP_CONNECTIVITY_H_
#define OCEANBASE_SHARE_BACKUP_OB_BACKUP_CONNECTIVITY_H_

#include "ob_backup_struct.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/backup/ob_backup_store.h"
#include "share/backup/ob_backup_path.h"
namespace oceanbase
{
namespace share
{
class ObBackupConnectivityCheckManager final
{
public:
  ObBackupConnectivityCheckManager();
  ~ObBackupConnectivityCheckManager();
  int init(
      const uint64_t tenant_id,
      obrpc::ObSrvRpcProxy &rpc_proxy,
      common::ObISQLClient &sql_proxy);
  int check_backup_dest_connectivity(const share::ObBackupDest &backup_dests);
private:
  int prepare_connectivity_check_file_(const share::ObBackupDest &backup_dest);
  int check_io_permission_(const share::ObBackupDest &backup_dest);
  int set_connectivity_check_path_(
      const share::ObBackupDest &backup_dest,
      share::ObBackupPath &path);
  int schedule_check_read_write_consistency_(const share::ObBackupDest &backup_dest);
  int check_server_rw_consistency_(
      const common::ObArray<ObAddr> &server_list,
      const ObString &file_path,
      const int64_t file_len,
      const uint64_t data_checksum);
  int set_last_check_time_(const share::ObBackupDest &backup_dest);
  bool is_inited_;
  uint64_t tenant_id_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  common::ObISQLClient *sql_proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupConnectivityCheckManager);
};

class ObBackupCheckFile final
{
public:
  ObBackupCheckFile();
  ~ObBackupCheckFile();
  int init(const uint64_t tenant_id, common::ObISQLClient &sql_proxy);
  int create_connectivity_check_file(
      const share::ObBackupDest &backup_dest,
      bool &is_new_create);
  int check_io_permission(const share::ObBackupDest &backup_dest);
  int get_check_file_path(
      const share::ObBackupDest &backup_dest,
      share::ObBackupPath &path);
  int delete_permission_check_file(const share::ObBackupDest &backup_dest);
  const char *get_connectivity_file_name() const { return connectivity_file_name_;}

private:
  int get_permission_check_file_path_(
      const share::ObBackupDest &backup_dest,
      const ObStorageAccessType access_type,
      share::ObBackupPath &path);
  int check_appender_permission_(const share::ObBackupDest &backup_dest);
  int check_multipart_upload_permission_(const share::ObBackupDest &backup_dest);
  int set_connectivity_check_name_();
  int create_check_file_dir_(
      const share::ObBackupDest &backup_dest,
      share::ObBackupPath &path);
  int compare_check_file_name_(
      const share::ObBackupDest &backup_dest,
      const share::ObBackupPath &path,
      bool &is_match);
  bool is_permission_error_(const int32_t result);
  int generate_format_desc_(const share::ObBackupDest &dest, share::ObBackupCheckDesc &check_desc);
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObISQLClient *sql_proxy_;
  char connectivity_file_name_[OB_MAX_BACKUP_CHECK_FILE_NAME_LENGTH];
  char permission_file_name_[OB_MAX_BACKUP_CHECK_FILE_NAME_LENGTH];
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupCheckFile);
};

class ObBackupConsistencyCheckFile final
{
public:
  ObBackupConsistencyCheckFile();
  ~ObBackupConsistencyCheckFile();
  int init(const uint64_t tenant_id, const ObBackupDest &backup_dest);
  int write_check_file(int64_t &file_len, uint64_t &data_checksum);
  int overwrite_check_file(int64_t &file_len, uint64_t &data_checksum);
  int append_check_file(int64_t &file_len, uint64_t &data_checksum);
  // file:///backup_data/backup_set_1_full/check_file/1002_nfs_connectivity_check_1741844011000000.obbak
  int get_file_dest_str(ObBackupPathString &file_dest_str);
  TO_STRING_KV(K_(backup_dest), K_(check_desc), K_(path));
private:
  int init_file_path_(const int64_t curr_time_us);
  int generate_append_data_(char *&append_data, int64_t &append_len);
  int get_data_checksum_(uint64_t &data_checksum);
  bool is_inited_;
  ObBackupDest backup_dest_;
  ObBackupPath path_;
  ObBackupConsistencyCheckDesc check_desc_;
  ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupConsistencyCheckFile);
};

class ObBackupDestCheck final
{
public:
  ObBackupDestCheck() {}
  ~ObBackupDestCheck() {}
  int check_backup_dest_connectivity(
      const uint64_t tenant_id,
      const char *backup_path,
      const share::ObBackupPath &check_path);
private:
  int check_check_file_exist_(
      const share::ObBackupDest &backup_dest,
      const share::ObBackupPath &path,
      bool &is_exist);
  DISALLOW_COPY_AND_ASSIGN(ObBackupDestCheck);
};

class ObBackupStorageInfoOperator
{
public:
  ObBackupStorageInfoOperator() = default;
  virtual ~ObBackupStorageInfoOperator() = default;
  static int insert_backup_storage_info(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest,
      const char *check_file_name);
  static int insert_backup_storage_info(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest,
      const ObBackupDestType::TYPE &dest_type,
      const int64_t dest_id,
      const int64_t max_iops,
      const int64_t max_bandwidth);
  static int remove_backup_storage_info(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest);
  static int remove_backup_storage_info(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const ObBackupDestType::TYPE &dest_type);
  static int update_backup_authorization(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest);
  static int update_backup_dest_attribute(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest,
      const int64_t max_iops,
      const int64_t max_bandwidth);
  static int update_last_check_time(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest,
      const int64_t last_check_time);
  static int update_backup_dest_extension(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest,
      const char *extension);
  static int get_check_file_name(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest,
      char *check_file_name);
  static int get_backup_dest(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const ObBackupPathString &backup_path,
      share::ObBackupDest &backup_dest);
  static int get_dest_id(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest,
      int64_t &dest_id);
  static int get_restore_dest_id(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const ObBackupDestType::TYPE &backup_dest_type,
      int64_t &dest_id);
  static int get_dest_type(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest,
      ObBackupDestType::TYPE &dest_type);
  static int get_restore_shared_storage_limit(
      const common::ObTrafficControl::ObStorageKey &storage_key,
      int64_t &max_iops,
      int64_t &max_bandwidth);

  static int get_backup_dest_extension(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest,
      char *extension,
      const int64_t buffer_len);
  static int get_backup_dest_extensions(
      const uint64_t tenant_id,
      const ObIArray<int64_t> &dest_ids,
      common::ObIAllocator &allocator,
      ObIArray<std::pair<int64_t, ObString>> &extensions);
private:
  static int parse_backup_path(
      const char *backup_path,
      char *path,
      int64_t path_len,
      char *endpoint,
      int64_t endpoint_len);
};

class ObBackupDestIOPermissionMgr final
{
public:
  ~ObBackupDestIOPermissionMgr();
  ObBackupDestIOPermissionMgr();

public:
  static int mtl_init(ObBackupDestIOPermissionMgr* &backup_dest_io_permission_mgr);
  int init(const uint64_t tenant_id);
  int start();
  void stop();
  void wait();
  void destroy();
public:
  static int check_backup_src_info_valid(const char *backup_src_info, const ObBackupSrcType &backup_src_type);
  static int check_zone_valid(const char *src_info);
  static int check_region_valid(const char *src_info);
  static int check_idc_valid(const char *src_info);
  static int delete_locality_info_in_backup_dest_str(char *backup_dest_str);
  static int get_src_info_from_extension(
      const ObString &extension,
      char *src_locality,
      const int64_t src_locality_length,
      share::ObBackupSrcType &src_type);
  static int separate_locality_info_from_dest_string(
      char *dest_string,
      const int64_t dest_string_length,
      char *locality_info,
      const int64_t localit_info_max_length);
public:
  int refresh_io_permission();
  int is_io_prohibited(const common::ObObjectStorageInfo *storage_info, bool &is_io_prohibited);
  int get_backup_path_src_info(
    char *src_locality,
    const int64_t src_locality_length,
    ObBackupSrcType &backup_src_type) const;
private:
  int update_last_access_time_(const int64_t dest_id, const bool is_io_prohibited);
  int get_backup_and_archive_path_dest_id_(int64_t &backup_dest_id, int64_t &archive_dest_id) const;
  int refresh_and_get_dest_ids_in_map_(ObIArray<int64_t> &dest_ids);
  int get_server_locality_info_(common::ObRegion &region, common::ObIDC &idc, common::ObZone &zone) const;
  int check_zone_in_src_info_(
      const char *src_info,
      const ObZone &zone,
      bool &io_prohibited) const;
  int check_region_in_src_info_(
      const char *src_info,
      const ObRegion &region,
      bool &io_prohibited) const;
  int check_idc_in_src_info_(
      const char *src_info,
      const ObIDC &idc,
      bool &io_prohibited) const;
  class ObRefreshIOPermissionTask : public common::ObTimerTask
  {
  public:
    ObRefreshIOPermissionTask(ObBackupDestIOPermissionMgr &mgr)
    : is_inited_(false),
      tenant_id_(common::OB_INVALID_ID),
      mgr_(mgr) {}
    virtual ~ObRefreshIOPermissionTask() {}
    int init(uint64_t tenant_id);
    void runTimerTask() override;
  public:
    bool is_inited_;
  private:
    uint64_t tenant_id_;
    ObBackupDestIOPermissionMgr &mgr_;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObRefreshIOPermissionTask);
  };

private:
  typedef int64_t ObDestId;
  typedef common::hash::ObHashMap<ObDestId, ObDestIOProhibitedInfo, hash::NoPthreadDefendMode> ObDestIOPermissionMap;
  static const int64_t OB_BACKUP_IO_PERMISSION_MAP_BUCKET_NUM = 1024;
  static const int64_t REFRESH_TIMEOUT = 10_min;
  static const int64_t PERMISSION_UPDATE_INTERVAL = 10_s;
  static const int64_t PERMISSION_EXPIRED_TIME = 24_hour;
  bool is_inited_;
  TCRWLock lock_;
  uint64_t tenant_id_;
  ObDestIOPermissionMap dest_io_permission_map_;
  int64_t last_refresh_time_;
  ObZone zone_;
  ObRegion region_;
  ObIDC idc_;
  ObRefreshIOPermissionTask refresh_io_permission_task_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupDestIOPermissionMgr);
};

class ObBackupChangeExternalStorageDestUtil
{
public:
  static int change_src_info(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      share::ObBackupDestAttribute &option,
      const share::ObBackupDest &backup_dest);
  static int process_src_info_in_extension_before_update(
      const char *src_info,
      char *extension,
      const int64_t extension_length,
      bool &do_not_need_update);
};

}//share
}//oceanbase

#endif /* OCEANBASE_SHARE_BACKUP_OB_BACKUP_CONNECTIVITY_H_ */