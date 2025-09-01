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

#ifndef OCEANBASE_STORAGE_CACHE_DDL_UTIL_H_
#define OCEANBASE_STORAGE_CACHE_DDL_UTIL_H_
#include "share/ob_define.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_rpc_struct.h"
namespace oceanbase
{
namespace sql
{
class ObStorageCacheUtil
{
public:
  static int print_storage_cache_policy_element(const ObBasePartition *partition, char* buf, const int64_t &buf_len, int64_t &pos);
  static int print_table_storage_cache_policy(const ObTableSchema &table_schema, char* buf, const int64_t &buf_len, int64_t &pos);
  static int check_alter_column_validation(const AlterColumnSchema *alter_column_schema, const ObTableSchema &orig_table_schema);
  static int get_tenant_table_ids(const uint64_t tenant_id, common::ObIArray<uint64_t> &table_id_array);
  static bool is_type_change_allow(const ObObjType &src_type, const ObObjType &dst_type);
  static int check_alter_partiton_storage_cache_policy(const share::schema::ObTableSchema &orig_table_schema,
                                                       const obrpc::ObAlterTableArg &alter_table_arg);
  static int check_alter_subpartiton_storage_cache_policy(const share::schema::ObTableSchema &orig_table_schema,
                                                          const obrpc::ObAlterTableArg &alter_table_arg);
  static int check_column_is_first_part_key(const ObPartitionKeyInfo &part_key_info, const uint64_t column_id);
  static int get_range_part_level(const share::schema::ObTableSchema &tbl_schema, const ObStorageCachePolicy &storage_cache_policy,
                                  int32_t &part_level);
  DISALLOW_COPY_AND_ASSIGN(ObStorageCacheUtil);
};
} // namespace sql
} // namespace oceanbase
#endif