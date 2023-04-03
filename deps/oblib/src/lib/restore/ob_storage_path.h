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

#include "lib/alloc/alloc_assist.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"

#ifndef OCEANBASE_COMMON_STORAGE_PATH_H_
#define OCEANBASE_COMMON_STORAGE_PATH_H_

namespace oceanbase
{
namespace common
{

static const char *const BACKUP_INFO_TENANT_ID = "tenant_id";
static const char *const BACKUP_BASE_DATA = "base_data";
static const char *const BACKUP_ALL_TENANT_ID_LIST = "all_tenant_id_list";

class ObStoragePath
{
public:
  ObStoragePath();
  virtual ~ObStoragePath();
  int init(const common::ObString &uri);
  int join_hash(const int64_t tenant_id, const int64_t table_id, const int64_t partition_id,
      const int64_t index_id, const int64_t macro_data_version, const int64_t macro_data_seq);
  int join(const common::ObString &path);
  int join(const int64_t tmp_var);
  int join_tablegroup_definition(const uint64_t tablegroup_id);
  int join_simple_object_definition(uint64_t object_id);
  int join_tablespace_definition(const uint64_t tablespace_id);
  int join_table_definition(const int64_t table_id);
  int join_data_seq(const int64_t data_version, const int64_t data_seq);
  int join_base_data_version(const int64_t data_version);
  int join_common_meta_variables(const int64_t data_version, const int64_t tenant_id,
      const int64_t table_id, const int64_t partition_id);
  int join_common_data_variables(const int64_t data_version, const int64_t tenant_id,
      const int64_t table_id, const int64_t partition_id);

  int64_t to_string(char *buf, const int64_t buf_len) const;

  const char *get_string() const { return path_; }
  int64_t get_length() const { return cur_pos_; }
  common::ObString get_obstring() const { return common::ObString(cur_pos_, path_); }
  void reset() { cur_pos_ = 0; }
  bool is_valid() const { return cur_pos_ > 0; }

private:
  int trim_right_delim();
  int trim_left_delim(const common::ObString &path, int64_t &delim_pos);
  char path_[common::OB_MAX_URI_LENGTH];
  int64_t cur_pos_;
};


class ObStoragePathUtil
{
public:
  //example: oss://runiu1/ob1.haipeng.zhp/all_tenant_id_list
  static int generate_all_tenant_id_list_file_path(const common::ObString &header_uri, const common::ObString &cluster_name,
      ObStoragePath &path);
  //The data_version on the path of the logical backup task will only be the latest, and the data_version of the macro data on the physical backup task is the data_version of the full backup task
  //The path on the physical backup task macro meta is consistent with the logical backup task, and both are the latest version
  //Logical backup tasks will back up every time without multiplexing; physical backup tasks will multiplex macroblocks according to conditions.
  //path example "file:///mnt/test_nfs_runiu/ob1.haipeng.zhp/2/1"
  static int generate_logic_backup_uri_dir_path(const common::ObString &header_uri, const common::ObString &cluster_name,
      const int64_t data_version, const int64_t tenant_id, ObStoragePath &path);
  //marco data dir path example:
  //"oss://runiu1/ob1.haipeng.zhp/base_data_3/1001/1100611139453834/3"
  //"oss://runiu1/ob1.haipeng.zhp/base_data_3/1001/1100611139453834/3/1100611139453834"
  static int generate_physic_backup_data_dir_path(const common::ObString &header_uri, const common::ObString &cluster_name,
      const int64_t data_version, const int64_t tenant_id, const int64_t table_id,
      const int64_t partition_id, ObStoragePath &path, const int64_t index_id = 0);
  //macro data full path, for backup, example:
  // "oss://runiu1/ob1.haipeng.zhp/base_data_5/1001/1100611139453836/1152921522055151616/1100611139453836/3_0"
  //macro data full path, for restore
  static int generate_physic_macro_data_file_path(const common::ObString &header_uri, const common::ObString &cluster_name,
      const int64_t data_version, const int64_t tenant_id,
      const int64_t table_id, const int64_t partition_id, const int64_t index_id,
      const int64_t macro_data_version, const int64_t macro_data_seq, ObStoragePath &path);
  //backup_info file path "oss://runiu1/ob1.haipeng.zhp/tenant_id/1001/backup_info"
  static int generate_backup_info_file_path(const common::ObString &header_uri, const common::ObString &cluster_name,
      const int64_t tenant_id, ObStoragePath &path);
  static int generate_routine_definition_file_path(const common::ObString &backup_uri,
      const uint64_t routine_id, ObStoragePath &path);
  static int generate_package_definition_file_path(const common::ObString &backup_uri,
      const uint64_t package_id, ObStoragePath &path);
  static int generate_udt_definition_file_path(const common::ObString &backup_uri,
      const uint64_t udt_id, ObStoragePath &path);
  //table definition example: "oss://runiu1/ob1.haipeng.zhp/3/1001/1100611139453822/inc_table_2_definition"
  static int generate_tablegroup_definition_file_path(const common::ObString &backup_uri,
      const uint64_t tablegroup_id, ObStoragePath &path);
  static int generate_foreign_key_definition_file_path(const common::ObString &backup_uri,
      const uint64_t foreign_key_id, ObStoragePath &path);
  static int generate_trigger_definition_file_path(const common::ObString &backup_uri,
      const uint64_t trigger_id, ObStoragePath &path);
  static int generate_table_definition_file_path(const common::ObString &backup_uri,
      const int64_t data_table_id, const int64_t table_id, ObStoragePath &path);
  static int generate_simple_object_file_path(const common::ObString &backup_uri,
                                              const common::ObString &object_prefix,
                                              uint64_t object_id,
                                              ObStoragePath &path);
  //sstable_meta example: "oss://runiu1/ob1.haipeng.zhp/2/1001/1100611139453799/1152921513465217024/sstable_meta"
  static int generate_sstable_meta_file_path(const common::ObString &header_uri, const common::ObString &cluster_name,
      const int64_t data_version, const int64_t tenant_id,
      const int64_t table_id, const int64_t partition_id, ObStoragePath &path);
  //table_keys example: "oss://runiu1/ob1.haipeng.zhp/2/1001/1100611139453799/1152921513465217024/table_keys"
  static int generate_table_keys_file_path(const common::ObString &header_uri, const common::ObString &cluster_name,
      const int64_t data_version, const int64_t tenant_id,
      const int64_t table_id, const int64_t partition_id, ObStoragePath &path);
  //partition_meta example: "oss://runiu1/ob1.haipeng.zhp/2/1001/1100611139453805/1/partition_meta"
  static int generate_partition_meta_file_path(const common::ObString &header_uri, const common::ObString &cluster_name,
      const int64_t data_version, const int64_t tenant_id,
      const int64_t table_id, const int64_t partition_id, ObStoragePath &path);
  //part_list example: "oss://runiu1/ob1.haipeng.zhp/3/1/1099511677788/0/1099511677788/part_list"
  static int generate_part_list_file_path(const common::ObString &header_uri, const common::ObString &cluster_name,
       const int64_t data_version, const int64_t tenant_id, const int64_t table_id,
       const int64_t partition_id, const int64_t index_id, ObStoragePath &path);
  static int generate_tablespace_definition_file_path(const common::ObString &backup_uri,
     const uint64_t tablespace_id, ObStoragePath &path);
  static int generate_partition_group_meta_file_path(const common::ObString &heder_uri, const common::ObString &cluster_name,
      const int64_t data_version, const int64_t tenant_id,
      const int64_t table_id, const int64_t partition_id, ObStoragePath &path);
};

}//common
}//oceanbase

#endif /* OCEANBASE_COMMON_STORAGE_PATH_H_ */
