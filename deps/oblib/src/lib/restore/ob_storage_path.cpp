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

#include "lib/utility/ob_print_utils.h"
#include "ob_storage_path.h"

namespace oceanbase
{

namespace common
{

ObStoragePath::ObStoragePath()
  :cur_pos_(0)
{
  MEMSET(path_, 0, sizeof(path_));
}

ObStoragePath::~ObStoragePath()
{
  cur_pos_ = 0;
  MEMSET(path_, 0, sizeof(path_));
}

int ObStoragePath::trim_right_delim()
{
  int ret = OB_SUCCESS;
  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else {
    while ('/' == path_[cur_pos_] && cur_pos_ > 0) {
      --cur_pos_;
    }
  }
  return ret;
}

int ObStoragePath::trim_left_delim(const common::ObString &path, int64_t &delim_pos)
{
  int ret = OB_SUCCESS;
  delim_pos = 0;
  if (path.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret), K(path));
  } else {
    while ('/' == *(path.ptr() + delim_pos) && delim_pos < path.length()) {
      ++delim_pos;
    }
  }
  return ret;
}

int ObStoragePath::init(const common::ObString &uri)
{
  int ret = OB_SUCCESS;

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret), K(uri));
  } else if (0 != cur_pos_) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "already inited", K(ret));
  } else if (OB_FAIL(databuff_printf(path_, common::OB_MAX_URI_LENGTH, cur_pos_, "%.*s",
      uri.length(), uri.ptr()))) {
    OB_LOG(WARN, "fail to save header uri", K(ret), K(uri));
  } else if (OB_FAIL(trim_right_delim())) {
    OB_LOG(WARN, "fail to trim right delim", K(ret));
  }
  OB_LOG(DEBUG, "", KCSTRING(path_), K(cur_pos_));
  return ret;
}

int ObStoragePath::join_hash(const int64_t tenant_id,
    const int64_t table_id, const int64_t partition_id, const int64_t index_id,
    const int64_t macro_data_version, const int64_t macro_data_seq)
{
  int ret = OB_SUCCESS;
  uint64_t hash  = 0;

  if (0 == cur_pos_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret), K(cur_pos_));
  } else {
    hash = common::murmurhash(&tenant_id, sizeof(tenant_id), hash);
    hash = common::murmurhash(&table_id, sizeof(table_id), hash);
    hash = common::murmurhash(&partition_id, sizeof(partition_id), hash);
    hash = common::murmurhash(&index_id, sizeof(index_id), hash);
    hash = common::murmurhash(&macro_data_version, sizeof(macro_data_version), hash);
    hash = common::murmurhash(&macro_data_seq, sizeof(macro_data_seq), hash);

    if (OB_FAIL(databuff_printf(path_, common::OB_MAX_URI_LENGTH, cur_pos_, "/%lu", hash))) {
      OB_LOG(WARN, "fail to save hash", K(ret), KCSTRING(path_), K(hash));
    }
  }

  OB_LOG(DEBUG, "hash path", KCSTRING(path_), K(cur_pos_));
  return ret;
}

int ObStoragePath::join(const common::ObString &path)
{
  int ret = OB_SUCCESS;
  int64_t left_delim_pos = 0;

  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (path.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret), K(path));
  } else if (OB_FAIL(trim_left_delim(path, left_delim_pos))) {
    OB_LOG(WARN, "fail to trim left delim", K(ret), K(path));
  } else if (OB_FAIL(databuff_printf(path_, common::OB_MAX_CONFIG_URL_LENGTH, cur_pos_, "/%.*s",
      static_cast<int>(path.length() - left_delim_pos), path.ptr() + left_delim_pos))) {
    OB_LOG(WARN, "fail to join path", K(ret), K(path));
  } else if (OB_FAIL(trim_right_delim())) {
    OB_LOG(WARN, "fail to trim right delim", K(ret));
  }
  OB_LOG(DEBUG, "", KCSTRING(path_), K(cur_pos_));
  return ret;
}

int ObStoragePath::join(const int64_t tmp_var)
{
  int ret = OB_SUCCESS;

  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (tmp_var < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret), K(tmp_var));
  } else if (OB_FAIL(databuff_printf(path_, common::OB_MAX_URI_LENGTH, cur_pos_, "/%ld", tmp_var))) {
    OB_LOG(WARN, "fail to save tmp_var", K(ret), KCSTRING(path_), K(tmp_var));
  }
  return ret;
}

int ObStoragePath::join_tablegroup_definition(const uint64_t tablegroup_id)
{
  int ret = OB_SUCCESS;
  if (cur_pos_ < 0) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (tablegroup_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret), K(tablegroup_id));
  } else if (OB_FAIL(databuff_printf(path_, common::OB_MAX_URI_LENGTH, cur_pos_, "/%ld_definition",
      tablegroup_id))) {
    OB_LOG(WARN, "fail to join table definition", K(ret), K(tablegroup_id), KCSTRING(path_));
  }
  return ret;
}

int ObStoragePath::join_simple_object_definition(uint64_t object_id)
{
  int ret = OB_SUCCESS;
  if (cur_pos_ < 0) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (object_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret), K(object_id));
  } else if (OB_FAIL(databuff_printf(path_, common::OB_MAX_URI_LENGTH, cur_pos_,
                                     "/%ld_definition", object_id))) {
    OB_LOG(WARN, "fail to join simple object definition", K(ret), KCSTRING(path_), K(object_id));
  }
  return ret;
}
int ObStoragePath::join_tablespace_definition(const uint64_t tablespace_id)
{
  int ret = OB_SUCCESS;
  if (cur_pos_ < 0) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (tablespace_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret), K(tablespace_id));
  } else if (OB_FAIL(databuff_printf(path_, common::OB_MAX_URI_LENGTH, cur_pos_, "/%ld_definition",
      tablespace_id))) {
    OB_LOG(WARN, "fail to join tablespace definition", K(ret), K(tablespace_id), KCSTRING(path_));
  }
  return ret;
}

int ObStoragePath::join_table_definition(const int64_t table_id)
{
  int ret = OB_SUCCESS;
  if (cur_pos_ < 0) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (table_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret), K(table_id));
  } else if (OB_FAIL(databuff_printf(path_, common::OB_MAX_URI_LENGTH, cur_pos_, "/%ld_definition",
      table_id))) {
    OB_LOG(WARN, "fail to join table definition", K(ret), K(table_id), KCSTRING(path_));
  }
  return ret;
}

int ObStoragePath::join_data_seq(const int64_t data_version, const int64_t data_seq)
{
  int ret = OB_SUCCESS;
   if (cur_pos_ <= 0) {
     ret = OB_NOT_INIT;
     OB_LOG(WARN, "please init first", K(ret));
   } else if (data_version <= 0 || data_seq < 0) {
     ret = OB_INVALID_ARGUMENT;
     OB_LOG(WARN, "argument is invalid", K(ret), K(data_version), K(data_seq));
   } else if (OB_FAIL(databuff_printf(path_, common::OB_MAX_URI_LENGTH, cur_pos_, "/%ld_%ld",
       data_version, data_seq))) {
     OB_LOG(WARN, "fail to save name and tmp_var", K(ret), K(data_version), K(data_seq));
   }
   return ret;
}

int ObStoragePath::join_base_data_version(const int64_t data_version)
{
  int ret = OB_SUCCESS;
   if (cur_pos_ <= 0) {
     ret = OB_NOT_INIT;
     OB_LOG(WARN, "please init first", K(ret));
   } else if (data_version < 0) {
     ret = OB_INVALID_ARGUMENT;
     OB_LOG(WARN, "argument is invalid", K(ret), K(data_version));
   } else if (OB_FAIL(databuff_printf(path_, common::OB_MAX_URI_LENGTH, cur_pos_, "/%s_%ld",
       BACKUP_BASE_DATA, data_version))) {
     OB_LOG(WARN, "fail to save name and tmp_var", K(ret), K(data_version));
   }
   return ret;
}

int ObStoragePath::join_common_meta_variables(const int64_t data_version, const int64_t tenant_id,
    const int64_t table_id, const int64_t partition_id)
{
  int ret = OB_SUCCESS;
   if (cur_pos_ <= 0) {
     ret = OB_NOT_INIT;
     OB_LOG(WARN, "please init first", K(ret));
   } else if (data_version < 0 || tenant_id < 0 || table_id < 0 || partition_id < 0) {
     ret = OB_INVALID_ARGUMENT;
     OB_LOG(WARN, "argument is invalid", K(ret), K(data_version), K(tenant_id), K(table_id), K(partition_id));
   } else if (OB_FAIL(databuff_printf(path_, common::OB_MAX_URI_LENGTH, cur_pos_, "/%ld/%ld/%ld/%ld",
       data_version, tenant_id, table_id, partition_id))) {
     OB_LOG(WARN, "fail to save name and tmp_var", K(ret), K(data_version), K(tenant_id), K(table_id), K(partition_id));
   }
   return ret;
}

int ObStoragePath::join_common_data_variables(const int64_t data_version, const int64_t tenant_id,
    const int64_t table_id, const int64_t partition_id)
{
  int ret = OB_SUCCESS;

  if (cur_pos_ <= 0) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "please init first", K(ret));
  } else if (data_version < 0 || tenant_id < 0 || table_id < 0 || partition_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret), K(data_version), K(tenant_id), K(table_id), K(partition_id));
  } else if (OB_FAIL(databuff_printf(path_, common::OB_MAX_URI_LENGTH, cur_pos_, "/%s_%ld/%ld/%ld/%ld",
      BACKUP_BASE_DATA, data_version, tenant_id, table_id, partition_id))) {
    OB_LOG(WARN, "fail to save name and tmp_var", K(ret), K(data_version), K(tenant_id), K(table_id), K(partition_id));
  }
  return ret;
}

int64_t ObStoragePath::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "%.*s",static_cast<int>(cur_pos_), path_);
  return pos;
}

int ObStoragePathUtil::generate_all_tenant_id_list_file_path(const common::ObString &header_uri,
    const common::ObString &cluster_name, ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(header_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(header_uri));
  } else if (OB_FAIL(path.join(cluster_name))) {
    OB_LOG(WARN, "fail to join cluster name to path", K(ret), K(path), K(cluster_name));
  } else if (OB_FAIL(path.join(ObString::make_string(BACKUP_ALL_TENANT_ID_LIST)))) {
    OB_LOG(WARN, "fail to join tennat id to path", K(ret), KCSTRING(BACKUP_ALL_TENANT_ID_LIST), K(path));
  }
  return ret;
}

int ObStoragePathUtil::generate_logic_backup_uri_dir_path(const common::ObString &header_uri,
    const common::ObString &cluster_name, const int64_t data_version, const int64_t tenant_id, ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(header_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(header_uri));
  } else if (OB_FAIL(path.join(cluster_name))) {
    OB_LOG(WARN, "fail to join cluster name to path", K(ret), K(path), K(cluster_name));
  } else if (OB_FAIL(path.join(data_version))) {
    OB_LOG(WARN, "fail to join data_version", K(ret), K(data_version));
  } else if (OB_FAIL(path.join(tenant_id))) {
    OB_LOG(WARN, "fail to join tenant_id", K(ret), K(tenant_id));
  }
  return ret;
}

int ObStoragePathUtil::generate_physic_backup_data_dir_path(const common::ObString &header_uri,
    const common::ObString &cluster_name, const int64_t data_version, const int64_t tenant_id, const int64_t table_id,
    const int64_t partition_id, ObStoragePath &path, const int64_t index_id /*default 0*/)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(header_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(header_uri));
  } else if (OB_FAIL(path.join(cluster_name))) {
    OB_LOG(WARN, "fail to join cluster name to path", K(ret), K(path), K(cluster_name));
  } else if (OB_FAIL(path.join_common_data_variables(data_version, tenant_id, table_id, partition_id))) {
    OB_LOG(WARN, "fail to join variable to data path", K(ret), K(data_version), K(tenant_id),
        K(table_id), K(partition_id));
  } else if (0 != index_id) {
    if (OB_FAIL(path.join(index_id))) {
      OB_LOG(WARN, "fail to join index_id ", K(ret), K(index_id));
    }
  }

  OB_LOG(DEBUG, " ", K(path));
  return ret;
}

int ObStoragePathUtil::generate_physic_macro_data_file_path(const common::ObString &header_uri,
    const common::ObString &cluster_name, const int64_t data_version, const int64_t tenant_id,
    const int64_t table_id, const int64_t partition_id, const int64_t index_id,
    const int64_t macro_data_version, const int64_t macro_data_seq, ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(header_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(header_uri));
  } else if (OB_FAIL(path.join(cluster_name))) {
    OB_LOG(WARN, "fail to join cluster name to path", K(ret), K(path), K(cluster_name));
  } else if (OB_FAIL(path.join_base_data_version(data_version))) {
    OB_LOG(WARN, "fail to join base data version", K(ret), K(data_version));
//  } else if (OB_FAIL(path.join_hash(tenant_id, table_id, partition_id, index_id, //TODO: haipeng.zhp hash path will add later
//      macro_data_version, macro_data_seq))) {
//    OB_LOG(WARN, "fail to join hash to path", K(ret));
  } else if (OB_FAIL(path.join(tenant_id))) {
    OB_LOG(WARN, "fail to join tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(path.join(table_id))) {
    OB_LOG(WARN, "fail to join table_id", K(ret), K(table_id));
  } else if (OB_FAIL(path.join(partition_id))) {
    OB_LOG(WARN, "fail to join partition_id", K(ret), K(partition_id));
  } else if (OB_FAIL(path.join(index_id))) {
    OB_LOG(WARN, "fail to join index_id ", K(ret), K(index_id));
  } else if (OB_FAIL(path.join_data_seq(macro_data_version, macro_data_seq))) {
    OB_LOG(WARN, "fail to join data_version and data_seq to path",
        K(ret), K(path), K(macro_data_version), K(macro_data_seq));
  }

  OB_LOG(DEBUG, " hash path", K(path));
  return ret;
}

int ObStoragePathUtil::generate_backup_info_file_path(const common::ObString &header_uri, const common::ObString &cluster_name,
      const int64_t tenant_id, ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(header_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(header_uri));
  } else if (OB_FAIL(path.join(cluster_name))) {
    OB_LOG(WARN, "fail to join cluster name to path", K(ret), K(path), K(cluster_name));
  } else if (OB_FAIL(path.join(ObString::make_string(BACKUP_INFO_TENANT_ID)))) {
    OB_LOG(WARN, "fail to join tenant_id", K(ret), K(path), KCSTRING(BACKUP_INFO_TENANT_ID));
  } else if (OB_FAIL(path.join(tenant_id))) {
    OB_LOG(WARN, "fail to join tenant_id to path", K(ret), K(tenant_id));
  } else if (OB_FAIL(path.join(ObString::make_string("backup_info")))) {
    OB_LOG(WARN, "fail to join backup_info to path", K(ret), K(path));
  }
  return ret;
}

int ObStoragePathUtil::generate_tablegroup_definition_file_path(const common::ObString &backup_uri,
     const uint64_t tablegroup_id, ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(backup_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(backup_uri), K(path));
  } else if (OB_FAIL(path.join(ObString::make_string(OB_CREATE_TABLEGROUP_DEFINITON)))) {
    OB_LOG(WARN, "fail to join tablegroup_id to path", K(ret), K(tablegroup_id), K(path));
  } else if (OB_FAIL(path.join_tablegroup_definition(tablegroup_id))) {
    OB_LOG(WARN, "fail to join tablegroup_id definition", K(ret), K(tablegroup_id), K(path));
  }
  return ret;
}

int ObStoragePathUtil::generate_foreign_key_definition_file_path(const common::ObString &backup_uri,
    const uint64_t foreign_key_id, ObStoragePath &path)
{
  return generate_simple_object_file_path(backup_uri,
                                          ObString::make_string(OB_FOREIGN_KEY_DEFINITION),
                                          foreign_key_id, path);
}
int ObStoragePathUtil::generate_tablespace_definition_file_path(const common::ObString &backup_uri,
     const uint64_t tablespace_id, ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(backup_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(backup_uri), K(path));
  } else if (OB_FAIL(path.join(ObString::make_string(OB_CREATE_TABLESPACE_DEFINITION)))) {
    OB_LOG(WARN, "fail to join tablespace_id to path", K(ret), K(tablespace_id), K(path));
  } else if (OB_FAIL(path.join_tablespace_definition(tablespace_id))) {
    OB_LOG(WARN, "fail to join tablespace_id definition", K(ret), K(tablespace_id), K(path));
  }
  return ret;
}

int ObStoragePathUtil::generate_routine_definition_file_path(const common::ObString &backup_uri,
    const uint64_t routine_id, ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(backup_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(backup_uri), K(path));
  } else if (OB_FAIL(path.join(ObString::make_string(OB_CREATE_ROUTINE_DEFINITION)))) {
    OB_LOG(WARN, "fail to join routine_id to path", K(ret), K(routine_id), K(path));
  } else if (OB_FAIL(path.join_tablegroup_definition(routine_id))) {
    OB_LOG(WARN, "fail to join routine_id definition", K(ret), K(routine_id), K(path));
  }
  return ret;
}

int ObStoragePathUtil::generate_package_definition_file_path(const common::ObString &backup_uri,
    const uint64_t package_id, ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(backup_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(backup_uri), K(path));
  } else if (OB_FAIL(path.join(ObString::make_string(OB_CREATE_PACKAGE_DEFINITION)))) {
    OB_LOG(WARN, "fail to join package_id to path", K(ret), K(package_id), K(path));
  } else if (OB_FAIL(path.join_tablegroup_definition(package_id))) {
    OB_LOG(WARN, "fail to join package_id definition", K(ret), K(package_id), K(path));
  }
  return ret;
}

int ObStoragePathUtil::generate_udt_definition_file_path(const common::ObString &backup_uri,
    const uint64_t udt_id, ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(backup_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(backup_uri), K(path));
  } else if (OB_FAIL(path.join(ObString::make_string(OB_CREATE_UDT_DEFINITION)))) {
    OB_LOG(WARN, "fail to join udt_id to path", K(ret), K(udt_id), K(path));
  } else if (OB_FAIL(path.join_tablegroup_definition(udt_id))) {
    OB_LOG(WARN, "fail to join udt_id definition", K(ret), K(udt_id), K(path));
  }
  return ret;
}

int ObStoragePathUtil::generate_trigger_definition_file_path(const common::ObString &backup_uri,
     const uint64_t trigger_id, ObStoragePath &path)
{
  return generate_simple_object_file_path(backup_uri,
                                          ObString::make_string(OB_TRIGGER_DEFINITION),
                                          trigger_id, path);
}

int ObStoragePathUtil::generate_table_definition_file_path(const common::ObString &backup_uri,
     const int64_t data_table_id, const int64_t table_id, ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(backup_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(backup_uri), K(path));
  } else if (OB_FAIL(path.join(data_table_id))) {
    OB_LOG(WARN, "fail to join data_table_id to path", K(ret), K(data_table_id), K(path));
  } else if (OB_FAIL(path.join_table_definition(table_id))) {
    OB_LOG(WARN, "fail to join table definition", K(ret), K(table_id), K(path));
  }
  return ret;
}

int ObStoragePathUtil::generate_simple_object_file_path(const ObString &backup_uri,
                                                        const ObString &object_prefix,
                                                        uint64_t object_id,
                                                        ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_ISNULL(object_prefix)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "prefix is null", K(ret));
  } else if (OB_FAIL(path.init(backup_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(backup_uri), K(path));
  } else if (OB_FAIL(path.join(object_prefix))) {
    OB_LOG(WARN, "fail to join object_prefix to path", K(ret), K(path), K(object_prefix));
  } else if (OB_FAIL(path.join_simple_object_definition(object_id))) {
    OB_LOG(WARN, "fail to join object_id definition", K(ret), K(path), K(object_id));
  }
  return ret;
}

int ObStoragePathUtil::generate_sstable_meta_file_path(const common::ObString &header_uri,
    const common::ObString &cluster_name, const int64_t data_version, const int64_t tenant_id,
    const int64_t table_id, const int64_t partition_id, ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(header_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(header_uri), K(path));
  } else if (OB_FAIL(path.join(cluster_name))) {
    OB_LOG(WARN, "fail to join cluster name to path", K(ret), K(path), K(cluster_name));
  } else if (OB_FAIL(path.join_common_meta_variables(data_version, tenant_id, table_id, partition_id))) {
    OB_LOG(WARN, "fail to join common meta variables", K(ret), K(path), K(data_version),
        K(tenant_id), K(table_id), K(partition_id));
  } else if (OB_FAIL(path.join(ObString::make_string(OB_STRING_SSTABLE_META)))) {
    OB_LOG(WARN, "fail to join sstable meta", K(ret), KCSTRING(OB_STRING_SSTABLE_META));
  }
  return ret;
}

int ObStoragePathUtil::generate_table_keys_file_path(const common::ObString &header_uri,
    const common::ObString &cluster_name, const int64_t data_version, const int64_t tenant_id,
    const int64_t table_id, const int64_t partition_id, ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(header_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(header_uri), K(path));
  } else if (OB_FAIL(path.join(cluster_name))) {
    OB_LOG(WARN, "fail to join cluster name to path", K(ret), K(path), K(cluster_name));
  } else if (OB_FAIL(path.join_common_meta_variables(data_version, tenant_id, table_id, partition_id))) {
    OB_LOG(WARN, "fail to join common meta variables", K(ret), K(path), K(data_version),
        K(tenant_id), K(table_id), K(partition_id));
  } else if (OB_FAIL(path.join(ObString::make_string(OB_STRING_TABLE_KEYS)))) {
    OB_LOG(WARN, "fail to join sstable meta", K(ret), KCSTRING(OB_STRING_TABLE_KEYS));
  }
  return ret;
}

int ObStoragePathUtil::generate_partition_meta_file_path(const common::ObString &header_uri,
    const common::ObString &cluster_name, const int64_t data_version, const int64_t tenant_id,
    const int64_t table_id, const int64_t partition_id, ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(header_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(header_uri), K(path));
  } else if (OB_FAIL(path.join(cluster_name))) {
    OB_LOG(WARN, "fail to join cluster name to path", K(ret), K(path), K(cluster_name));
  } else if (OB_FAIL(path.join_common_meta_variables(data_version, tenant_id, table_id, partition_id))) {
    OB_LOG(WARN, "fail to join common meta variables", K(ret), K(path), K(data_version),
        K(tenant_id), K(table_id), K(partition_id));
  } else if (OB_FAIL(path.join(ObString::make_string(OB_STRING_PARTITION_META)))) {
    OB_LOG(WARN, "fail to join sstable meta", K(ret), KCSTRING(OB_STRING_PARTITION_META));
  }
  return ret;
}

int ObStoragePathUtil::generate_part_list_file_path(const common::ObString &header_uri,
    const common::ObString &cluster_name, const int64_t data_version, const int64_t tenant_id, const int64_t table_id,
    const int64_t partition_id, const int64_t index_id, ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(header_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(header_uri), K(path));
  } else if (OB_FAIL(path.join(cluster_name))) {
    OB_LOG(WARN, "fail to join cluster name to path", K(ret), K(path), K(cluster_name));
  } else if (index_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argumen is invalid", K(ret), K(path));
  } else if (OB_FAIL(path.join_common_meta_variables(data_version, tenant_id, table_id, partition_id))) {
    OB_LOG(WARN, "fail to join common meta variables", K(ret), K(path), K(data_version),
        K(tenant_id), K(table_id), K(partition_id));
  } else if (OB_FAIL(path.join(index_id))) {
    OB_LOG(WARN, "fail to join index id", K(ret), K(path), K(index_id));
  } else if (OB_FAIL(path.join(ObString::make_string(OB_STRING_PART_LIST)))) {
    OB_LOG(WARN, "fail to join sstable meta", K(ret), KCSTRING(OB_STRING_PART_LIST));
  }
  return ret;
}

int ObStoragePathUtil::generate_partition_group_meta_file_path(const common::ObString &header_uri,
    const common::ObString &cluster_name, const int64_t data_version, const int64_t tenant_id,
    const int64_t table_id, const int64_t partition_id, ObStoragePath &path)
{
  int ret = OB_SUCCESS;
  path.reset();

  if (OB_FAIL(path.init(header_uri))) {
    OB_LOG(WARN, "fail to init path", K(ret), K(header_uri), K(path));
  } else if (OB_FAIL(path.join(cluster_name))) {
    OB_LOG(WARN, "fail to join cluster name to path", K(ret), K(path), K(cluster_name));
  } else if (OB_FAIL(path.join(data_version))) {
    OB_LOG(WARN, "fail to join data version to path", K(ret), K(path), K(data_version));
  } else if (OB_FAIL(path.join(tenant_id))) {
    OB_LOG(WARN, "fail to join tenant id to path", K(ret), K(path), K(tenant_id));
  } else if (OB_FAIL(path.join(ObString::make_string(OB_TABLEGROUP)))) {
    OB_LOG(WARN, "fail to join table group to path", K(ret), K(path), KCSTRING(OB_TABLEGROUP));
  } else if (OB_FAIL(path.join(table_id))) {
    OB_LOG(WARN, "fail to join table id to path", K(ret), K(path), K(table_id));
  } else if (OB_FAIL(path.join(partition_id))) {
    OB_LOG(WARN, "fail to join partition id to path", K(ret), K(path), K(partition_id));
  } else if (OB_FAIL(path.join(ObString::make_string(OB_STRING_PARTITION_GROUP_META)))) {
    OB_LOG(WARN, "fail to join partition group meta to path", K(ret), KCSTRING(OB_STRING_PARTITION_GROUP_META));
  }
  return ret;
}

}//common
}//oceanbase
