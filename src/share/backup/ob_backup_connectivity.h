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
  int schedule_connectivity_check_(
      const share::ObBackupDest &backup_dest,
      const share::ObBackupPath &path);
  int prepare_connectivity_check_file_(const share::ObBackupDest &backup_dest);
  int check_io_permission_(const share::ObBackupDest &backup_dest);
  int set_connectivity_check_path_(
      const share::ObBackupDest &backup_dest,
      share::ObBackupPath &path);
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
      const int64_t dest_id);
  static int remove_backup_storage_info(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest);
  static int update_backup_authorization(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest);
  static int update_last_check_time(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest,
      const int64_t last_check_time);
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
  static int get_dest_type(
      common::ObISQLClient &proxy,
      const uint64_t tenant_id,
      const share::ObBackupDest &backup_dest,
      ObBackupDestType::TYPE &dest_type);
private:
  static int parse_backup_path(
      const char *backup_path,
      char *path,
      int64_t path_len,
      char *endpoint,
      int64_t endpoint_len);
};

}//share
}//oceanbase

#endif /* OCEANBASE_SHARE_BACKUP_OB_BACKUP_CONNECTIVITY_H_ */