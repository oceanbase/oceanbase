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

#ifndef OCEANBASE_SHARE_BACKUP_OB_LOG_RESTORE_STRUCT_H_
#define OCEANBASE_SHARE_BACKUP_OB_LOG_RESTORE_STRUCT_H_

#include "lib/container/ob_array.h"
#include "share/scn.h"
#include "ob_backup_struct.h"

namespace oceanbase
{
namespace share
{

struct ObRestoreSourceServiceUser final
{
  ObRestoreSourceServiceUser();
  ~ObRestoreSourceServiceUser() {}
  void reset();
  bool is_valid() const;
  int assign(const ObRestoreSourceServiceUser &user);
  char user_name_[OB_MAX_USER_NAME_LENGTH];
  char tenant_name_[OB_MAX_ORIGINAL_NANE_LENGTH];
  ObCompatibilityMode mode_;
  uint64_t tenant_id_;
  int64_t cluster_id_;
  bool operator == (const ObRestoreSourceServiceUser &other) const;
  TO_STRING_KV(K_(user_name), K_(tenant_name), K_(tenant_id), K_(cluster_id));
};

struct ObRestoreSourceServiceAttr final
{
  ObRestoreSourceServiceAttr();
  ~ObRestoreSourceServiceAttr() {}
  void reset();
  int parse_service_attr_from_str(ObSqlString &str);
  int do_parse_sub_service_attr(const char *sub_value);
  int set_service_user_config(const char *user_tenant);
  int set_service_user(const char *user, const char *tenant);
  int set_service_tenant_id(const char *tenant_id);
  int set_service_cluster_id(const char *cluster_id);
  int set_service_compatibility_mode(const char *compatibility_mode);
  // It need to convert password to encrypted password when pasre from log_restore_source config.
  int set_service_passwd_to_encrypt(const char *passwd);
  // There's no need to convert password to encrypted password when parse from __all_log_restore_source record.
  int set_service_passwd_no_encrypt(const char *passwd);
  int parse_ip_port_from_str(const char *buf, const char *delimiter);
  bool is_valid() const;
  bool service_user_is_valid() const;
  bool service_host_is_valid() const;
  bool service_password_is_valid() const;
  int gen_config_items(common::ObIArray<BackupConfigItemPair> &items) const;
  int gen_service_attr_str(char *buf, const int64_t buf_size) const;
  int gen_service_attr_str(ObSqlString &str) const;
  int get_ip_list_str_(char *buf, const int64_t buf_size) const;
  int get_ip_list_str_(ObSqlString &str) const;
  int get_user_str_(char *buf, const int64_t buf_size) const;
  int get_user_str_(ObSqlString &str) const;
  int get_password_str_(char *buf, const int64_t buf_size) const;
  int get_password_str_(ObSqlString &str) const;
  int get_tenant_id_str_(char *buf ,const int64_t buf_size) const;
  int get_tenant_id_str_(ObSqlString &str) const;
  int get_cluster_id_str_(char *buf, const int64_t buf_size) const;
  int get_cluster_id_str_(ObSqlString &str) const;
  int get_compatibility_mode_str_(char *buf, const int64_t buf_size) const;
  int get_compatibility_mode_str_(ObSqlString &str) const;
  int get_is_encrypted_str_(char *buf, const int64_t buf_size) const;
  int get_is_encrypted_str_(ObSqlString &str) const;
  int set_encrypt_password_key_(const char *encrypt_key);
  int get_decrypt_password_key_(char *unencrypt_key, const int64_t buf_size) const;
  // return the origion password
  int get_password(char *passwd, const int64_t buf_size) const;
  bool compare_addr_(common::ObArray<common::ObAddr> addr) const;
  int check_restore_source_is_self_(bool &is_self, uint64_t tenant_id) const;
  bool operator ==(const ObRestoreSourceServiceAttr &other) const;
  int assign(const ObRestoreSourceServiceAttr &attr);
  TO_STRING_KV(K_(addr), K_(user));
  common::ObArray<common::ObAddr> addr_;
  ObRestoreSourceServiceUser user_;
  char encrypt_passwd_[OB_MAX_BACKUP_SERIALIZEKEY_LENGTH];
};

struct ObRestoreSourceLocationPrimaryAttr final
{
  ObRestoreSourceLocationPrimaryAttr();
  ~ObRestoreSourceLocationPrimaryAttr() {}
  void reset();
  uint64_t tenant_id_;
  int64_t cluster_id_;
  ObSqlString location_;
  int parse_location_attr_from_str(const common::ObString &value);
  int do_parse_sub_config_(const char *sub_value);
  int set_location_path_(const char *path);
  int set_location_cluster_id_(const char *cluster_id_str);
  int set_location_tenant_id_(const char *tenant_id_str);
  bool is_valid();
  TO_STRING_KV(K_(tenant_id), K_(cluster_id), K_(location));
};

}//share
}//oceanbase

#endif /* OCEANBASE_SHARE_BACKUP_OB_LOG_RESTORE_STRUCT_H_ */