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

#ifndef __OB_SHARE_RESTORE_OB_RESTORE_ARGS_H__
#define __OB_SHARE_RESTORE_OB_RESTORE_ARGS_H__

#include "lib/string/ob_string.h"
#include "common/ob_partition_key.h"
#include "common/data_buffer.h"
#include "lib/allocator/page_arena.h"
#include "share/ob_define.h"

namespace oceanbase {
namespace share {

struct ObSchemaIdPair {
  ObSchemaIdPair() : schema_id_(common::OB_INVALID_ID), backup_schema_id_(common::OB_INVALID_ID)
  {}
  ObSchemaIdPair(uint64_t schema_id, uint64_t backup_schema_id)
      : schema_id_(schema_id), backup_schema_id_(backup_schema_id)
  {}
  bool is_valid() const
  {
    return common::OB_INVALID_ID != schema_id_ && common::OB_INVALID_ID != backup_schema_id_;
  }
  uint64_t schema_id_;
  uint64_t backup_schema_id_;  // The schema_id of the remote backup
  TO_STRING_KV(K_(schema_id), K_(backup_schema_id));
  OB_UNIS_VERSION(1);
};

class ObRestoreArgs {
public:
  OB_UNIS_VERSION(1);

public:
  struct OrignalSQLInfo {
    OrignalSQLInfo()
        : backup_table_id_(common::OB_INVALID_ID),
          backup_index_id_(common::OB_INVALID_ID),
          backup_tablegroup_id_(common::OB_INVALID_ID),
          backup_table_name_(),
          backup_index_name_(),
          backup_tablegroup_name_(),
          backup_database_name_()
    {
      MEMSET(backup_table_name_buf_, 0, common::OB_MAX_TABLE_NAME_BUF_LENGTH);
      MEMSET(backup_database_name_buf_, 0, common::OB_MAX_DATABASE_NAME_BUF_LENGTH);
      MEMSET(backup_index_name_buf_, 0, common::OB_MAX_TABLE_NAME_BUF_LENGTH);
      MEMSET(backup_tablegroup_name_buf_, 0, common::OB_MAX_TABLEGROUP_NAME_LENGTH);
    }
    void set_backup_table_id(const uint64_t table_id)
    {
      backup_table_id_ = table_id;
    }
    void set_backup_index_id(const uint64_t table_id)
    {
      backup_index_id_ = table_id;
    }
    void set_backup_tablegroup_id(const uint64_t tablegroup_id)
    {
      backup_tablegroup_id_ = tablegroup_id;
    }
    int set_backup_table_name(const common::ObString& tb_name)
    {
      common::ObDataBuffer allocator(backup_table_name_buf_, common::OB_MAX_TABLE_NAME_BUF_LENGTH);
      return ob_write_string(allocator, tb_name, backup_table_name_);
    }
    int set_backup_index_name(const common::ObString& tb_name)
    {
      common::ObDataBuffer allocator(backup_index_name_buf_, common::OB_MAX_TABLE_NAME_BUF_LENGTH);
      return ob_write_string(allocator, tb_name, backup_index_name_);
    }
    int add_backup_index_name_prefix(const uint64_t table_id)
    {
      int ret = common::OB_SUCCESS;
      if (backup_index_name_.prefix_match(common::OB_MYSQL_RECYCLE_PREFIX) ||
          backup_index_name_.prefix_match(common::OB_ORACLE_RECYCLE_PREFIX)) {
        // do nothing
      } else {
        char tmp_buf[common::OB_MAX_TABLE_NAME_LENGTH];
        int pos = snprintf(tmp_buf,
            common::OB_MAX_TABLE_NAME_LENGTH,
            "%s%lu_%.*s",
            common::OB_INDEX_PREFIX,
            table_id,
            backup_index_name_.length(),
            backup_index_name_.ptr());
        if (pos < 0 || pos >= common::OB_MAX_TABLE_NAME_LENGTH) {
          ret = common::OB_BUF_NOT_ENOUGH;
        } else {
          MEMCPY(backup_index_name_buf_, tmp_buf, pos);
          backup_index_name_.assign(backup_index_name_buf_, pos);
        }
      }
      return ret;
    }
    int set_backup_db_name(const common::ObString& db_name)
    {
      common::ObDataBuffer allocator(backup_database_name_buf_, common::OB_MAX_DATABASE_NAME_BUF_LENGTH);
      return ob_write_string(allocator, db_name, backup_database_name_);
    }
    int set_backup_tablegroup_name(const common::ObString& tg_name)
    {
      common::ObDataBuffer allocator(backup_tablegroup_name_buf_, common::OB_MAX_TABLEGROUP_NAME_LENGTH);
      return ob_write_string(allocator, tg_name, backup_tablegroup_name_);
    }
    uint64_t backup_table_id_;
    uint64_t backup_index_id_;
    uint64_t backup_tablegroup_id_;
    common::ObString backup_table_name_;
    common::ObString backup_index_name_;
    common::ObString backup_tablegroup_name_;
    common::ObString backup_database_name_;
    // buffer
    char backup_table_name_buf_[common::OB_MAX_TABLE_NAME_BUF_LENGTH];
    char backup_index_name_buf_[common::OB_MAX_TABLE_NAME_BUF_LENGTH];
    char backup_tablegroup_name_buf_[common::OB_MAX_TABLEGROUP_NAME_LENGTH];
    char backup_database_name_buf_[common::OB_MAX_DATABASE_NAME_BUF_LENGTH];
    TO_STRING_KV(K_(backup_table_id), K_(backup_index_id), K_(backup_tablegroup_id), K_(backup_table_name),
        K_(backup_index_name), K_(backup_database_name), K_(backup_tablegroup_name));
  };

public:
  static constexpr int64_t OB_MAX_URI_LENGTH = 2048;
  static constexpr int64_t OB_MAX_URI_HOST_LENGTH = 128;
  static constexpr int64_t OB_MAX_URI_ACCESS_ID_LENGTH = 128;
  static constexpr int64_t OB_MAX_URI_ACCESS_KEY_LENGTH = 128;
  static constexpr int64_t OB_MAX_URI_PROTOCOL_LENGTH = 16;
  static constexpr int64_t OB_MAX_URI_BUCKET_LENGTH = 512;
  static constexpr int64_t OB_MAX_CLUSTER_NAME_LENGTH = 128;
  static constexpr int64_t OB_MAX_POOL_LIST_LENGTH = 256;
  static constexpr int64_t OB_MAX_URI_BUCKET_DIR_DEPTH = 10;
  static constexpr int64_t OB_MAX_TCP_INVITED_NODES_LENGTH = common::MAX_IP_ADDR_LENGTH * 100;  // 3,200

public:
  ObRestoreArgs();
  ~ObRestoreArgs() = default;
  void reset();
  bool is_valid() const;
  bool is_parse_ok() const;
  bool is_inplace_restore() const;
  int set_schema_id_pairs(const common::ObIArray<ObSchemaIdPair>& pairs);
  int trans_to_backup_schema_id(const uint64_t schema_id, uint64_t& backup_schema_id) const;
  int trans_from_backup_schema_id(const uint64_t backup_schema_id, uint64_t& schema_id) const;

  const char* get_uri_header() const;
  const char* get_cluster_name() const;
  const char* get_storage_info() const;
  // oss://071092/obfin001/1001?host=xxxx&access_id=xxx&&access_key=xxx&timestamp=xxxx&restore_user=xxx&restore_pass=xxx
  char uri_header_[common::OB_MAX_URI_HEADER_LENGTH];      // oss://071092
  char cluster_name_[common::OB_MAX_CLUSTER_NAME_LENGTH];  // obfin001
  char storage_info_[common::OB_MAX_URI_LENGTH];

  char primary_zone_[common::MAX_ZONE_LENGTH];
  char locality_[common::MAX_LOCALITY_LENGTH];
  char pool_list_[OB_MAX_POOL_LIST_LENGTH];
  char restore_user_[common::OB_MAX_USERNAME_LENGTH];
  char restore_pass_[common::OB_MAX_PASSWORD_LENGTH];
  char tcp_invited_nodes_[OB_MAX_TCP_INVITED_NODES_LENGTH];
  int64_t base_data_version_;  // 32
  int64_t curr_data_version_;  // 33
  int64_t tenant_id_;          // 1001
  int64_t restore_timeu_;      // timestamp xxxx
  uint64_t backup_schema_id_;  // tablegroup_id or table_id
  int64_t partition_id_;
  // Above RS: In the case of pg, it contains the id pair of the main table and the index table; in the case of a
  // standalone partition, it only contains the id pair of the index table STORAGE above: tandalone partition, including
  // the id pair of the main table and the index table
  common::ObSEArray<ObSchemaIdPair, common::OB_MAX_SSTABLE_PER_TABLE> schema_id_list_;
  OrignalSQLInfo sql_info_;  // Some information in the original SQL, such as original table_id, etc.
  int64_t schema_version_;
  uint64_t cluster_version_;       // Backup cluster version number
  ObSchemaIdPair schema_id_pair_;  // tablegroup_id:backup_tablegroup_id or table_id:backup_table_id
  // TODO(): fix it after find out to_string core
  // TO_STRING_KV(K(tenant_id_));
  TO_STRING_KV(K(tenant_id_), K_(locality), K_(primary_zone), K_(pool_list), K_(tcp_invited_nodes),
      K_(base_data_version), K_(curr_data_version), K_(tenant_id), K_(restore_timeu), K_(backup_schema_id),
      K_(partition_id), K_(schema_id_list), K_(sql_info), K_(schema_version), K_(uri_header), K_(cluster_name),
      K_(storage_info), K_(schema_id_list));

private:
  /* functions */
  int add_schema_id_pair(const uint64_t schema_id, const uint64_t backup_schema_id);
  /* variables */
};

}  // namespace share
}  // namespace oceanbase
#endif /* __OB_SHARE_RESTORE_OB_RESTORE_ARGS_H__ */
//// end of header file
