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

namespace oceanbase {
namespace share {
namespace schema {
class ObTableSchema;
class ObColumnSchemaV2;
class ObServerSchemaService;
class ObSchemaUtils {
public:
#define UNDER_SYS_TENANT(id) ({ OB_SYS_TENANT_ID == extract_tenant_id(id); })

public:
  static uint64_t get_exec_tenant_id(const uint64_t tenant_id);
  static uint64_t get_extract_tenant_id(const uint64_t exec_tenant_id, const uint64_t tenant_id);
  static uint64_t get_extract_schema_id(const uint64_t exec_tenant_id, const uint64_t schema_id);
  static uint64_t get_real_table_mappings_tid(const uint64_t ref_table_id);
  static int get_all_table_id(
      const uint64_t tenant_id, uint64_t& table_id, const ObServerSchemaService* schema_service = NULL);
  static int get_all_table_history_id(
      const uint64_t tenant_id, uint64_t& table_id, const ObServerSchemaService* schema_service = NULL);
  static int get_all_table_name(
      const uint64_t tenant_id, const char*& table_name, const ObServerSchemaService* schema_service = NULL);
  static int get_all_table_history_name(
      const uint64_t tenant_id, const char*& table_name, const ObServerSchemaService* schema_service = NULL);

  template <class T>
  static int alloc_schema(common::ObIAllocator& allocator, const T& schema, T*& allocated_schema);
  template <class T>
  static int alloc_schema(common::ObIAllocator& allocator, T*& allocated_schema);
  template <class T>
  static int deep_copy_schema(char* buf, const T& old_var, T*& new_var);
  static int cascaded_generated_column(ObTableSchema& table_schema, ObColumnSchemaV2& column);
  static bool is_virtual_generated_column(uint64_t flag);
  static bool is_stored_generated_column(uint64_t flag);
  static bool is_invisible_column(uint64_t flag);
  static bool is_cte_generated_column(uint64_t flag);
  static bool is_default_expr_v2_column(uint64_t flag);
  static bool is_fulltext_column(uint64_t flag);
  static bool is_generated_column(uint64_t flag)
  {
    return is_virtual_generated_column(flag) || is_stored_generated_column(flag);
  }
  static int add_column_to_table_schema(ObColumnSchemaV2& column, ObTableSchema& table_schema);
  static int convert_sys_param_to_sysvar_schema(const ObSysParam& sysparam, ObSysVarSchema& sysvar_schema);
  static int get_primary_zone_array(common::PageArena<>& alloc, const ObTableSchema& table_schema,
      ObSchemaGetterGuard& schema_guard, common::ObIArray<ObZoneScore>& primary_zone_array);
  static int get_primary_zone_array(common::PageArena<>& alloc, const ObTablegroupSchema& tg_schema,
      ObSchemaGetterGuard& schema_guard, common::ObIArray<ObZoneScore>& primary_zone_array);
  template <class T>
  static int get_schema_primary_zone_array(
      common::PageArena<>& alloc, const T* schema, common::ObIArray<ObZoneScore>& primary_zone_array);
  static int get_tenant_int_variable(uint64_t tenant_id, share::ObSysVarClassType var_id, int64_t& v);
  static int get_tenant_varchar_variable(
      uint64_t tenant_id, share::ObSysVarClassType var_id, common::ObIAllocator& allocator, common::ObString& v);
  static int str_to_int(const common::ObString& str, int64_t& value);
  static bool is_public_database(const common::ObString& db_name, bool is_oracle_mode);
  static bool is_public_database_case_cmp(const common::ObString& db_name, bool is_oracle_mode);

private:
  static int get_tenant_variable(schema::ObSchemaGetterGuard& schema_guard, uint64_t tenant_id,
      share::ObSysVarClassType var_id, common::ObObj& value);
  // disallow construct
  ObSchemaUtils()
  {}
  ~ObSchemaUtils()
  {}
};

template <class T>
int ObSchemaUtils::alloc_schema(common::ObIAllocator& allocator, const T& schema, T*& allocated_schema)
{
  int ret = common::OB_SUCCESS;
  allocated_schema = NULL;
  void* buf = NULL;
  if (NULL == (buf = allocator.alloc(sizeof(T)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SHARE_SCHEMA_LOG(ERROR, "alloc schema failed", K(ret));
  } else if (FALSE_IT(allocated_schema = new (buf) T(&allocator))) {
    // will not reach here
  } else {
    if (OB_FAIL(copy_assign(*allocated_schema, schema))) {
      SHARE_SCHEMA_LOG(WARN, "fail to assign schema", K(ret));
    }
  }
  return ret;
}

template <class T>
int ObSchemaUtils::alloc_schema(common::ObIAllocator& allocator, T*& allocated_schema)
{
  int ret = common::OB_SUCCESS;
  allocated_schema = NULL;
  void* buf = NULL;
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
int ObSchemaUtils::deep_copy_schema(char* buf, const T& old_var, T*& new_var)
{
  int ret = common::OB_SUCCESS;
  new_var = NULL;

  if (NULL == buf) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "invalid argument", K(ret), K(buf));
  } else {
    int64_t size = old_var.get_convert_size() + sizeof(common::ObDataBuffer);
    common::ObDataBuffer* databuf = new (buf + sizeof(old_var)) common::ObDataBuffer(
        buf + sizeof(old_var) + sizeof(common::ObDataBuffer), size - sizeof(old_var) - sizeof(common::ObDataBuffer));
    new_var = new (buf) T(databuf);
    if (OB_FAIL(copy_assign(*new_var, old_var))) {
      SHARE_SCHEMA_LOG(WARN, "fail to assign schema", K(ret));
    }
  }

  return ret;
}

template <class T>
int ObSchemaUtils::get_schema_primary_zone_array(
    common::PageArena<>& alloc, const T* schema, common::ObIArray<ObZoneScore>& primary_zone_array)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(schema)) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema is null", K(ret));
  } else {
    const common::ObIArray<ObZoneScore>& schema_zone_array = schema->get_primary_zone_array();
    primary_zone_array.reset();
    ObZoneScore zone_score;
    for (int64_t i = 0; i < schema_zone_array.count() && OB_SUCC(ret); i++) {
      const ObZoneScore& score = schema_zone_array.at(i);
      zone_score.reset();
      zone_score.score_ = score.score_;
      if (OB_FAIL(common::ob_write_string(alloc, score.zone_, zone_score.zone_))) {
        SHARE_SCHEMA_LOG(WARN, "fail to write string", K(ret));
      } else if (OB_FAIL(primary_zone_array.push_back(zone_score))) {
        SHARE_SCHEMA_LOG(WARN, "fail to push back", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace schema
}  // namespace share
}  // namespace oceanbase

#endif
