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

#ifndef _OCEANBASE_SQL_OB_SCHEMA_UTILS_H
#define _OCEANBASE_SQL_OB_SCHEMA_UTILS_H

#include <stdint.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "share/schema/ob_schema_struct.h"
#include "share/system_variable/ob_sys_var_class_type.h"
#include "common/sql_mode/ob_sql_mode.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
}

namespace share
{
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
class ObServerSchemaService;
struct SchemaKey;
class ObSchemaUtils
{
public:
  static uint64_t get_exec_tenant_id(const uint64_t tenant_id);
  static uint64_t get_extract_tenant_id(const uint64_t exec_tenant_id, const uint64_t tenant_id);
  static uint64_t get_extract_schema_id(const uint64_t exec_tenant_id, const uint64_t schema_id);
  static uint64_t get_real_table_mappings_tid(const uint64_t ref_table_id);
  static int get_all_table_name(
             const uint64_t tenant_id,
             const char* &table_name,
             const ObServerSchemaService *schema_service = NULL);
  static int get_all_table_history_name(
             const uint64_t tenant_id,
             const char* &table_name,
             const ObServerSchemaService *schema_service = NULL);

  template <class T>
  static int alloc_schema(common::ObIAllocator &allocator,
                          const T &schema,
                          T * &allocated_schema);
  template <class T>
  static int alloc_schema(common::ObIAllocator &allocator,
                          T * &allocated_schema);
  template <class T>
  static int deep_copy_schema(char *buf, const T &old_var, T *&new_var);
  static int cascaded_generated_column(ObTableSchema &table_schema,
                                       ObColumnSchemaV2 &column,
                                       const bool resolve_dependencies);
  static bool is_virtual_generated_column(uint64_t flag);
  static bool is_stored_generated_column(uint64_t flag);
  static bool is_always_identity_column(uint64_t flag);
  static bool is_default_identity_column(uint64_t flag);
  static bool is_default_on_null_identity_column(uint64_t flag);
  static bool is_invisible_column(uint64_t flag);
  static bool is_cte_generated_column(uint64_t flag);
  static bool is_default_expr_v2_column(uint64_t flag);
  static bool is_fulltext_column(uint64_t flag);
  static bool is_spatial_generated_column(uint64_t flag);
  static bool is_generated_column(uint64_t flag) { return is_virtual_generated_column(flag) || is_stored_generated_column(flag); }
  static bool is_identity_column(uint64_t flag) { return is_always_identity_column(flag) || is_default_identity_column(flag) || is_default_on_null_identity_column(flag); }
  static bool is_label_se_column(uint64_t flag);
  static int add_column_to_table_schema(ObColumnSchemaV2 &column, ObTableSchema &table_schema);
  static int convert_sys_param_to_sysvar_schema(const ObSysParam &sysparam, ObSysVarSchema &sysvar_schema);
  static int get_tenant_int_variable(
      uint64_t tenant_id,
      share::ObSysVarClassType var_id,
      int64_t &v);
  static int get_tenant_varchar_variable(
      uint64_t tenant_id,
      share::ObSysVarClassType var_id,
      common::ObIAllocator &allocator,
      common::ObString &v);
  static int str_to_int(const common::ObString &str, int64_t &value);
  static int str_to_uint(const common::ObString &str, uint64_t &value);

  template<class T>
  static int serialize_partition_array(
      T **partition_array,
      const int64_t partition_num,
      char *buf, const int64_t buf_len, int64_t &pos);
  template<class T>
  static int64_t get_partition_array_serialize_size(
          T **partition_array,
          const int64_t partition_num);
  template<class T>
  static int64_t get_partition_array_convert_size(
          T **partition_array,
          const int64_t partition_num);
  static int construct_tenant_space_simple_table(
             const uint64_t tenant_id,
             share::schema::ObSimpleTableSchemaV2 &table);
  static int construct_tenant_space_full_table(
             const uint64_t tenant_id,
             share::schema::ObTableSchema &table);
  static int construct_inner_table_schemas(
             const uint64_t tenant_id,
             common::ObIArray<share::schema::ObTableSchema> &tables);
  static int add_sys_table_lob_aux_table(
             uint64_t tenant_id,
             uint64_t data_table_id,
             ObIArray<ObTableSchema> &table_schemas);

  // Optimized method to batch get latest table schemas from cache or inner_table automatically.
  //
  // @param[in] sql_client: ObISQLClient
  // @param[in] allocator:  allocator to manage memory of table schemas
  // @param[in] tenant_id:  target tenant_id
  // @param[in] table_ids:   target table_id array
  // @param[out] table_schemas: array of ObSimpleTableSchemaV2 pointers
  //                           (it's count may be smaller than table_ids when some tables not exist or been deleted)
  // @return: OB_SUCCESS if success
  static int batch_get_latest_table_schemas(
      common::ObISQLClient &sql_client,
      common::ObIAllocator &allocator,
      const uint64_t tenant_id,
      const common::ObIArray<ObObjectID> &table_ids,
      common::ObIArray<ObSimpleTableSchemaV2 *> &table_schemas);

  // Optimized method to get latest table schema from cache or inner_table automatically.
  //
  // @param[in] sql_client: ObISQLClient
  // @param[in] allocator:  allocator to manage memory of table schema
  // @param[in] tenant_id:  target tenant_id
  // @param[in] table_id:   target table_id
  // @param[out] table_schema: pointer of ObSimpleTableSchemaV2 (not null)
  // @return: OB_SUCCESS if success
  //          OB_TABLE_NOT_EXIST if table not exist
  static int get_latest_table_schema(
      common::ObISQLClient &sql_client,
      common::ObIAllocator &allocator,
      const uint64_t tenant_id,
      const ObObjectID &table_id,
      ObSimpleTableSchemaV2 *&table_schema);

  static int try_check_parallel_ddl_schema_in_sync(
             const ObTimeoutCtx &ctx,
             const uint64_t tenant_id,
             const int64_t schema_version);
private:
  static int get_tenant_variable(schema::ObSchemaGetterGuard &schema_guard,
                                 uint64_t tenant_id,
                                 share::ObSysVarClassType var_id,
                                 common::ObObj &value);

  static int batch_get_table_schemas_from_cache_(
      common::ObIAllocator &allocator,
      const uint64_t tenant_id,
      const ObIArray<ObTableLatestSchemaVersion> &table_schema_versions,
      common::ObIArray<SchemaKey> &need_refresh_table_schema_keys,
      common::ObIArray<ObSimpleTableSchemaV2 *> &table_schemas);
  static int batch_get_table_schemas_from_inner_table_(
      common::ObISQLClient &sql_client,
      common::ObIAllocator &allocator,
      const uint64_t tenant_id,
      common::ObArray<SchemaKey> &need_refresh_table_schema_keys,
      common::ObIArray<ObSimpleTableSchemaV2 *> &table_schemas);

  // disallow construct
  ObSchemaUtils() {}
  ~ObSchemaUtils() {}
};

template <class T>
int ObSchemaUtils::alloc_schema(common::ObIAllocator &allocator,
                                const T &schema,
                                T * &allocated_schema)
{
  int ret = common::OB_SUCCESS;
  allocated_schema = NULL;
  void *buf = NULL;
  if (NULL == (buf = allocator.alloc(sizeof(T)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SHARE_SCHEMA_LOG(ERROR, "alloc schema failed", K(ret));
  } else if (FALSE_IT(allocated_schema = new (buf) T(&allocator))) {
    // will not reach here
  } else {
    if (OB_FAIL(copy_assign(*allocated_schema, schema))) {
      SHARE_SCHEMA_LOG(WARN,"fail to assign schema", K(ret));
    }
  }
  return ret;
}

template <class T>
int ObSchemaUtils::alloc_schema(common::ObIAllocator &allocator,
                                T * &allocated_schema)
{
  int ret = common::OB_SUCCESS;
  allocated_schema = NULL;
  void *buf = NULL;
  if (NULL == (buf = allocator.alloc(sizeof(T)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SHARE_SCHEMA_LOG(ERROR, "alloc schema failed", K(ret));
  } else if (FALSE_IT(allocated_schema = new (buf) T(&allocator))) {
    // will not reach here
  } else if (OB_ISNULL(allocated_schema)) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "invalid allocated schema", KR(ret));
  }
  return ret;
}

template <class T>
int ObSchemaUtils::deep_copy_schema(char *buf, const T &old_var, T *&new_var)
{
  int ret = common::OB_SUCCESS;
  new_var = NULL;

  if (NULL == buf) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "invalid argument", K(ret), K(buf));
  } else {
    int64_t size = old_var.get_convert_size() + sizeof(common::ObDataBuffer);
    common::ObDataBuffer *databuf = new (buf + sizeof(old_var))
        common::ObDataBuffer(buf + sizeof(old_var) + sizeof(common::ObDataBuffer),
                             size - sizeof(old_var) - sizeof(common::ObDataBuffer));
    new_var = new (buf) T(databuf);
    if (OB_FAIL(copy_assign(*new_var, old_var))) {
      SHARE_SCHEMA_LOG(WARN, "fail to assign schema", K(ret));
    }
  }

  return ret;
}

template<class T>
int ObSchemaUtils::serialize_partition_array(
    T **partition_array, const int64_t partition_num,
    char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, partition_num))) {
    SHARE_SCHEMA_LOG(WARN, "Fail to encode partition count", KR(ret));
  }
  if (OB_NOT_NULL(partition_array)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
      if (OB_ISNULL(partition_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "partition_array_ element is null", KR(ret));
      } else if (OB_FAIL(partition_array[i]->serialize(buf, buf_len, pos))) {
        SHARE_SCHEMA_LOG(WARN, "Fail to serialize partition", KR(ret));
      }
    }
  }
  return ret;
}

template<class T>
int64_t ObSchemaUtils::get_partition_array_serialize_size(
  T **partition_array,
  const int64_t partition_num)
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(partition_num);
  if (OB_NOT_NULL(partition_array)) {
    for (int64_t i = 0; i < partition_num; i++) {
      if (OB_NOT_NULL(partition_array[i])) {
        len += partition_array[i]->get_serialize_size();
      }
    }
  }
  return len;
}

template<class T>
int64_t ObSchemaUtils::get_partition_array_convert_size(
  T **partition_array,
  const int64_t partition_num)
{
  int64_t convert_size = 0;
  if (OB_NOT_NULL(partition_array)) {
    for (int64_t i = 0; i < partition_num && OB_NOT_NULL(partition_array[i]); ++i) {
      convert_size += partition_array[i]->get_convert_size();
    }
    convert_size += partition_num * sizeof(T*);
  }
  return convert_size;
}

} // end schema
} // end share
} // end oceanbase

#endif
