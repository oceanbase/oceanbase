/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifdef  ARCH_DEF
ARCH_DEF(OB_X86_ARCH_TYPE, "X86")
ARCH_DEF(OB_ARM_ARCH_TYPE, "ARM")
#endif


#ifndef OCEANBASE_PL_ROUTINE_STORAGE_H_
#define OCEANBASE_PL_ROUTINE_STORAGE_H_

#include "share/ob_define.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "ob_pl_stmt.h"
#include "pl/ob_pl_allocator.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/ob_stmt_resolver.h"
namespace oceanbase
{

namespace common
{
class ObIAllocator;
class ObISQLClient;
class ObMySQLTransaction;
}

namespace share
{
class ObDMLSqlSplicer;
}

namespace pl
{

enum ObPLArchType
{
  OB_INVALID_ARCH_TYPE = -1,
#define ARCH_DEF(type, name) type,
#include "pl/ob_pl_persistent.h"
#undef ARCH_DEF
  ARCH_TYPE_IDX_NUM
};

static constexpr const char* ARCH_TYPE_DEF[ARCH_TYPE_IDX_NUM] =
{
#define ARCH_DEF(type, name) name,
#include "pl/ob_pl_persistent.h"
#undef ARCH_DEF
};

static constexpr const int64_t dep_obj_ids_md5_len = common::OB_MAX_SQL_ID_LENGTH + 1;

struct ObPLExtraInfo {
  ObPLExtraInfo() {
    MEMSET(dep_obj_ids_md5_, '\0', dep_obj_ids_md5_len);
  }

  ObPLExtraInfo(const char* dep_obj_ids_md5)
  {
    if (OB_NOT_NULL(dep_obj_ids_md5)) {
      MEMCPY(dep_obj_ids_md5_, dep_obj_ids_md5, dep_obj_ids_md5_len);
    }
  }

  static int64_t get_extra_info_length() {
    return dep_obj_ids_md5_len;
  }

  char dep_obj_ids_md5_[dep_obj_ids_md5_len];
};

class ObRoutinePersistentInfo
{
public:
  enum ObPLOperation
  {
    NONE = 0,
    INSERT,
    UPDATE,
    SUCC,
  };

  ObRoutinePersistentInfo()
  : tenant_id_(OB_INVALID_ID),
    database_id_(OB_INVALID_ID),
    compile_db_id_(OB_INVALID_ID),
    key_id_(OB_INVALID_ID),
#if defined(__aarch64__)
    arch_type_(ARCH_TYPE_DEF[ObPLArchType::OB_ARM_ARCH_TYPE]),
#else
    arch_type_(ARCH_TYPE_DEF[ObPLArchType::OB_X86_ARCH_TYPE]),
#endif
    allocator_(ObMemAttr(MTL_ID() == OB_INVALID_TENANT_ID ? OB_SYS_TENANT_ID : MTL_ID(), GET_PL_MOD_STRING(OB_PL_JIT))),
    tenant_id_belongs_(OB_INVALID_ID),
    special_compile_mode_(0)
  {}
  ObRoutinePersistentInfo(uint64_t tenant_id,
                      uint64_t database_id,
                      uint64_t compile_db_id,
                      uint64_t key_id,
                      uint64_t tenant_id_belongs)
  : tenant_id_(tenant_id),
    database_id_(database_id),
    compile_db_id_(compile_db_id),
    key_id_(key_id),
#if defined(__aarch64__)
    arch_type_(ARCH_TYPE_DEF[ObPLArchType::OB_ARM_ARCH_TYPE]),
#else
    arch_type_(ARCH_TYPE_DEF[ObPLArchType::OB_X86_ARCH_TYPE]),
#endif
    allocator_(ObMemAttr(tenant_id_, GET_PL_MOD_STRING(OB_PL_JIT))),
    tenant_id_belongs_(tenant_id_belongs),
    special_compile_mode_(0)
  {}

  int64_t get_head_size() { return 1 + 1 + 2 + 2;/* 8bit flags + 8bit level + 8bit id + 8bit nums*/ }
  int get_total_size(ObPLCompileUnit &unit, int32_t &total_len);

  int encode_header(char *buf, const int64_t len, int64_t &pos,
                        int8_t flags, const int8_t level, const int16_t id, const int16_t nums);

  int encode_payload(char *buf, int64_t len, int64_t &pos, const ObString &binary);

  int encode_dll(ObPLCompileUnit &unit, ObString &dll, int64_t &pos,
                    const int8_t level, const int16_t id);

  int decode_header(char *buf, const int64_t len, int64_t &pos,
                        int8_t &flags, int8_t &level, int16_t &id, int16_t &nums);

  int decode_payload(char *buf, int64_t len, int64_t &pos, ObString &binary);

  int decode_dll(ObSQLSessionInfo &session_info,
                  schema::ObSchemaGetterGuard &schema_guard,
                  ObExecEnv &exec_env,
                  ObPLCompileUnitAST &unit_ast, ObPLCompileUnit &unit,
                  char *buf, const int64_t len, int64_t &pos,
                  int8_t &level, int16_t &id);

  int gen_routine_storage_dml(const uint64_t exec_tenant_id,
                              ObDMLSqlSplicer &dml,
                              int64_t merge_version,
                              const ObString &binary,
                              const ObString &stack_sizes,
                              const ObString &extra_info_str);

  template<typename DependencyTable>
  static int check_dep_schema(ObSchemaGetterGuard &schema_guard,
                              const DependencyTable &dep_schema_objs,
                              int64_t merge_version,
                              bool &match,
                              bool is_check_package_state = false);

  int read_dll_from_disk(ObSQLSessionInfo *session_info,
                          schema::ObSchemaGetterGuard &schema_guard,
                          ObExecEnv &exec_env,
                          ObPLCompileUnitAST &unit_ast,
                          ObPLCompileUnit &unit,
                          ObRoutinePersistentInfo::ObPLOperation &op);

  int insert_or_update_dll_to_disk(schema::ObSchemaGetterGuard &schema_guard,
                                   const ObString &binary,
                                   const ObString &stack_sizes,
                                   const ObString &extra_info_str,
                                   const ObRoutinePersistentInfo::ObPLOperation op);

  int process_storage_dll(ObIAllocator &alloc,
                            schema::ObSchemaGetterGuard &schema_guard,
                            ObPLCompileUnit &unit,
                            const ObRoutinePersistentInfo::ObPLOperation op);

  int mask_special_compile_mode(ObSQLSessionInfo &session_info);

  static int has_same_name_dependency_with_public_synonym(
                            schema::ObSchemaGetterGuard &schema_guard,
                            const ObPLDependencyTable &dep_schema_objs,
                            bool& exist,
                            ObSQLSessionInfo &session_info);

  static int delete_dll_from_disk(common::ObISQLClient &trans,
                                  uint64_t tenant_id,
                                  uint64_t key_id,
                                  uint64_t database_id);

  static int get_stack_size_length(const ObPLCompileUnit &unit, int64_t &stack_size_length);
  static int encode_stack_sizes(char *buf, const int64_t len, int64_t &pos, const ObPLCompileUnit &unit);
  static int decode_stack_sizes(ObPLCompileUnit &unit, char *buf, const int64_t len, int64_t &pos);

  static int encode_pl_extra_info(char *buf,
                               const int64_t len,
                               int64_t &pos,
                               const sql::DependenyTableStore &dep_table,
                               schema::ObSchemaGetterGuard &schema_guard,
                               uint64_t key_id);

  static int decode_and_check_extra_info(char *buf,
                                         const int64_t len,
                                         const ObPLDependencyTable &dep_table,
                                         bool &match,
                                         schema::ObSchemaGetterGuard &schema_guard,
                                         uint64_t key_id);

  template<typename DependencyTable>
  static int get_pl_extra_info(const DependencyTable &dep_table,
                               ObPLExtraInfo& extra_info,
                               schema::ObSchemaGetterGuard &schema_guard,
                               uint64_t key_id);
  static bool is_extra_info_column_exist(const uint64_t data_version);
  static bool is_stack_size_column_exist(const uint64_t data_version);

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t compile_db_id_;
  uint64_t key_id_;
  ObString arch_type_;

  ObArenaAllocator allocator_;
  uint64_t tenant_id_belongs_;
  uint64_t special_compile_mode_;
};

}

}
#endif