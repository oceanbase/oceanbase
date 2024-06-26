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

#define USING_LOG_PREFIX PL_STORAGEROUTINE
#include "ob_pl_persistent.h"
#include "lib/oblog/ob_log_module.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "observer/ob_inner_sql_connection.h"
#include "observer/ob_inner_sql_result.h"
#include "ob_pl_code_generator.h"
#include "ob_pl_compile.h"

namespace oceanbase
{
namespace pl
{

int ObRoutinePersistentInfo::get_total_size(ObPLCompileUnit &unit, int32_t &total_len)
{
  int ret = OB_SUCCESS;

  total_len += get_head_size();
  total_len += 4;
  if (unit.is_pkg()) {
    // do nothing
  } else {
    total_len += static_cast<ObPLFunction &>(unit).get_helper().get_compiled_object().length();
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < unit.get_routine_table().count(); ++i) {
    if (OB_NOT_NULL(unit.get_routine_table().at(i))) {
      OZ (SMART_CALL(get_total_size(*unit.get_routine_table().at(i), total_len)));
    }
  }

  return ret;
}

int ObRoutinePersistentInfo::encode_header(char *buf, const int64_t len, int64_t &pos,
                                          int8_t flags, const int8_t level, const int16_t id, const int16_t nums)
{
  int ret = OB_SUCCESS;
  flags = 0xff;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else {
    if (OB_LIKELY(6 < len - pos)) {
      MEMCPY(buf + pos, &flags, 1);
      ++pos;
      MEMCPY(buf + pos, &level, 1);
      ++pos;
      MEMCPY(buf + pos, &id, 2);
      pos += 2;
      MEMCPY(buf + pos, &nums, 2);
      pos += 2;
    } else {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("size overflow", K(ret), K(len), K(pos), KP(buf));
    }
  }

  return ret;
}

int ObRoutinePersistentInfo::encode_payload(char *buf, int64_t len, int64_t &pos, const ObString &binary)
{
  int ret = OB_SUCCESS;
  int32_t length = binary.length();

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else if (OB_LIKELY(4 < len - pos)) {
    if (0 != length) {
      MEMCPY(buf + pos, &length, 4);
      pos += 4;
      if (OB_LIKELY(length < len - pos)) {
        MEMCPY(buf + pos, binary.ptr(), length);
        pos += length;
      } else {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("size overflow", K(ret), K(len), K(pos), KP(buf));
      }
    } else {
      length = 0;
      MEMCPY(buf + pos, &length, 4);
      pos += 4;
    }
  } else {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(len), K(pos), KP(buf));
  }

  return ret;
}

int ObRoutinePersistentInfo::encode_dll(ObPLCompileUnit &unit, ObString &dll, int64_t &pos,
                                       const int8_t level, const int16_t id)
{
  int ret = OB_SUCCESS;
  int16_t nums = unit.get_routine_table().count();
  ObString binary;
  if (unit.is_pkg()) {
    binary.reset();
  } else {
    binary = static_cast<ObPLFunction &>(unit).get_helper().get_compiled_object();
  }
  OZ (encode_header(dll.ptr(), dll.length(), pos, 0, level, id, nums));
  OZ (encode_payload(dll.ptr(), dll.length(), pos, binary));
  for (int64_t i = 0; OB_SUCC(ret) && i < nums; ++i) {
    if (OB_NOT_NULL(unit.get_routine_table().at(i))) {
      OZ (SMART_CALL(encode_dll(*unit.get_routine_table().at(i), dll, pos, level + 1, i + 1)));
    }
  }

  return ret;
}

int ObRoutinePersistentInfo::decode_header(char *buf, const int64_t len, int64_t &pos,
                        int8_t &flags, int8_t &level, int16_t &id, int16_t &nums)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else {
    if (OB_LIKELY(6 < len - pos)) {
      MEMCPY(&flags, buf + pos, 1);
      ++pos;
      MEMCPY(&level, buf + pos, 1);
      ++pos;
      MEMCPY(&id, buf + pos, 2);
      pos += 2;
      MEMCPY(&nums, buf + pos, 2);
      pos += 2;
    } else {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("size overflow", K(ret), K(len), K(pos), KP(buf));
    }
  }

  return ret;
}

int ObRoutinePersistentInfo::decode_payload(char *buf, int64_t len, int64_t &pos, ObString &binary)
{
  int ret = OB_SUCCESS;
  int32_t length = 0;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), KP(buf));
  } else if (OB_LIKELY(4 < len - pos)) {
    MEMCPY(&length, buf + pos, 4);
    pos += 4;
    if (0 != length) {
      if (OB_LIKELY(length < len - pos)) {
        binary.assign_ptr(buf + pos, length);
        pos += length;
      } else {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("size overflow", K(ret), K(len), K(pos), KP(buf));
      }
    } else {
      binary.reset();
    }
  } else {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(len), K(pos), KP(buf));
  }

  return ret;
}

int ObRoutinePersistentInfo::decode_dll(ObSQLSessionInfo &session_info,
                                    schema::ObSchemaGetterGuard &schema_guard,
                                    ObExecEnv &exec_env,
                                    ObPLCompileUnitAST &unit_ast, ObPLCompileUnit &unit,
                                    char *buf, const int64_t len, int64_t &pos,
                                    int8_t &level, int16_t &id)
{
  int ret = OB_SUCCESS;
  int16_t nums = 0;
  int8_t flags = 0;
  ObString binary;

  OZ (decode_header(buf, len, pos, flags, level, id, nums));
  CK (nums == unit_ast.get_routine_table().get_count());
  OZ (decode_payload(buf, len, pos, binary));
  if (OB_FAIL(ret)) {
  } else if (0 == binary.length() &&
             ObPLCompileUnitAST::UnitType::ROUTINE_TYPE == unit_ast.get_type()) {
    CK (0 == nums);
  } else {
    // generate action pointer from binary
    // set action
    if (ObPLCompileUnitAST::UnitType::ROUTINE_TYPE == unit_ast.get_type() &&
        0 != binary.length()) {
      int64_t length = binary.length();
      char *copy_buf = static_cast<char*>(allocator_.alloc_aligned(length, 16));

      if (OB_ISNULL(copy_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for deep copy compiled binary", K(ret));
      } else {
        MEMCPY(copy_buf, binary.ptr(), length);
      }

      OZ (static_cast<ObPLFunction &>(unit).gen_action_from_precompiled(static_cast<ObPLCompileUnitAST &>(unit_ast).get_name(), length, copy_buf));
    }
    if (OB_SUCC(ret) && nums > 0) {
      ObPLRoutineTable &routine_table = unit_ast.get_routine_table();
      unit.init_routine_table(nums);
      for(int64_t routine_idx = 0; OB_SUCC(ret) && routine_idx < nums; routine_idx++) {
        ObPLFunctionAST *routine_ast = NULL;
        const ObPLRoutineInfo *routine_info = NULL;
        if (OB_FAIL(routine_table.get_routine_info(routine_idx, routine_info))) {
          LOG_WARN("get routine info failed", K(ret), K(routine_idx));
        } else if (OB_ISNULL(routine_info)) {
          if (OB_FAIL(unit.add_routine(NULL))) {
            LOG_WARN("package add routine failed", K(ret), K(routine_idx));
          }
        } else if (OB_FAIL(routine_table.get_routine_ast(routine_idx, routine_ast))) {
          LOG_WARN("get routine ast failed", K(routine_idx), K(ret));
        } else if (OB_ISNULL(routine_ast)) {
          if (OB_FAIL(unit.add_routine(NULL))) {
            LOG_WARN("add routine failed", K(ret), K(routine_idx));
          }
        } else {
          ObPLFunction *routine = NULL;
          if (OB_ISNULL(routine
            = static_cast<ObPLFunction *>(unit.get_allocator().alloc(sizeof(ObPLFunction))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else {
            new (routine) ObPLFunction(unit.get_mem_context());
            OZ (ObPLCompiler::init_function(schema_guard, exec_env, *routine_info, *routine));
            if (OB_SUCC(ret)) {

    #ifdef USE_MCJIT
              HEAP_VAR(ObPLCodeGenerator, cg ,unit.get_allocator(), session_info) {
    #else
              HEAP_VAR(ObPLCodeGenerator, cg, unit.get_allocator(),
                      session_info,
                      schema_guard,
                      *routine_ast,
                      routine->get_expressions(),
                      routine->get_helper(),
                      routine->get_di_helper(),
                      lib::is_oracle_mode()) {
    #endif
                int8_t cur_level = 0;
                int16_t sub_id = 0;
                OZ (cg.init());
                if (cg.get_debug_mode()
                    || !routine_ast->get_is_all_sql_stmt()
                    || !routine_ast->get_obj_access_exprs().empty()) {
                  OZ (SMART_CALL(decode_dll(session_info, schema_guard, routine->get_exec_env(), *routine_ast, *routine, buf, len, pos, cur_level, sub_id)));
                  CK (sub_id == routine_idx + 1);
                  OZ (cg.prepare_expression(*routine));
                  OZ (cg.final_expression(*routine));
                  OZ (routine->set_variables(routine_ast->get_symbol_table()));
                  OZ (routine->get_dependency_table().assign(routine_ast->get_dependency_table()));
                  OZ (routine->add_members(routine_ast->get_flag()));
                  OX (routine->set_pipelined(routine_ast->get_pipelined()));
                  OX (routine->set_can_cached(routine_ast->get_can_cached()));
                  OX (routine->set_is_all_sql_stmt(routine_ast->get_is_all_sql_stmt()));
                  OX (routine->set_has_parallel_affect_factor(routine_ast->has_parallel_affect_factor()));
                  OX (routine->set_ret_type(routine_ast->get_ret_type()));
                  OZ (routine->set_types(routine_ast->get_user_type_table()));
                } else {
                  // simple routine(generate by generate_simpile interface), skip encode header byte
                  OZ (SMART_CALL(decode_dll(session_info, schema_guard, routine->get_exec_env(), *routine_ast, *routine, buf, len, pos, cur_level, sub_id)));
                  CK (sub_id == routine_idx + 1);
                  CK (0 == routine->get_action());
                  OZ (cg.generate_simple(*routine));
                  OX (routine->set_ret_type(routine_ast->get_ret_type()));
                }
                OZ (unit.add_routine(routine));
              } // end of HEAP_VAR
            }
          }
          if (OB_FAIL(ret) && OB_NOT_NULL(routine)) {
            routine->~ObPLFunction();
            unit.get_allocator().free(routine);
            routine = NULL;
          }
        }
      }
    }
  }

  return ret;
}

int ObRoutinePersistentInfo::gen_routine_storage_dml(const uint64_t exec_tenant_id,
                                                ObDMLSqlSplicer &dml,
                                                int64_t merge_version,
                                                const ObString &binary)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dml.add_pk_column("database_id", database_id_))
    || OB_FAIL(dml.add_pk_column("key_id", key_id_))
    || OB_FAIL(dml.add_pk_column("compile_db_id", compile_db_id_))
    || OB_FAIL(dml.add_pk_column("arch_type", arch_type_))
    || OB_FAIL(dml.add_column("merge_version", merge_version))
    || OB_FAIL(dml.add_column("dll", ObHexEscapeSqlStr(binary)))) {
    LOG_WARN("add column failed", K(ret));
  }
  return ret;
}

int ObRoutinePersistentInfo::check_dep_schema(ObSchemaGetterGuard &schema_guard,
                                          const ObPLDependencyTable &dep_schema_objs,
                                          int64_t merge_version,
                                          bool &match)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  match = true;
  for (int64_t i = 0; OB_SUCC(ret) && match && i < dep_schema_objs.count(); ++i) {
    tenant_id = MTL_ID();
    if (TABLE_SCHEMA != dep_schema_objs.at(i).get_schema_type()) {
      int64_t new_version = 0;
      if (PACKAGE_SCHEMA == dep_schema_objs.at(i).get_schema_type()
          || UDT_SCHEMA == dep_schema_objs.at(i).get_schema_type()
          || ROUTINE_SCHEMA == dep_schema_objs.at(i).get_schema_type()) {
        tenant_id = pl::get_tenant_id_by_object_id(dep_schema_objs.at(i).object_id_);
      }
      if (OB_FAIL(schema_guard.get_schema_version(dep_schema_objs.at(i).get_schema_type(),
                                                  tenant_id,
                                                  dep_schema_objs.at(i).object_id_,
                                                  new_version))) {
        LOG_WARN("failed to get schema version",
                  K(ret), K(tenant_id), K(dep_schema_objs.at(i)));
      } else if (new_version <= merge_version) {
        match = true;
      } else {
        match = false;
      }
    } else {
      const ObSimpleTableSchemaV2 *table_schema = nullptr;
      if (OB_FAIL(schema_guard.get_simple_table_schema(MTL_ID(),
                                                      dep_schema_objs.at(i).object_id_,
                                                      table_schema))) {
        LOG_WARN("failed to get table schema", K(ret), K(dep_schema_objs.at(i)));
      } else if (nullptr == table_schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get an unexpected null table schema", K(dep_schema_objs.at(i).object_id_));
      } else if (table_schema->is_index_table()) {
        // do nothing
      } else if (table_schema->get_schema_version() <= merge_version) {
        match = true;
      } else {
        match = false;
      }
    }
  }
  if (OB_SUCC(ret) && !match) {
    LOG_INFO("not match schema", K(merge_version), K(dep_schema_objs));
  }

  return ret;
}

int ObRoutinePersistentInfo::read_dll_from_disk(ObSQLSessionInfo *session_info,
                                            schema::ObSchemaGetterGuard &schema_guard,
                                            ObExecEnv &exec_env,
                                            ObPLCompileUnitAST &unit_ast,
                                            ObPLCompileUnit &unit,
                                            ObRoutinePersistentInfo::ObPLOperation &op)
{
  int ret = OB_SUCCESS;

  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (!((GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_2_0 && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0) || GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_1_0) ||
             !((data_version >= DATA_VERSION_4_2_2_0 && data_version < DATA_VERSION_4_3_0_0) || data_version >= DATA_VERSION_4_3_1_0)) {
    // do nothing
  } else {
    uint64_t action = 0;

    ObMySQLProxy *sql_proxy = NULL;
    ObSqlString query_inner_sql;
    ObString binary;
    int64_t merge_version;

    if (OB_ISNULL(session_info)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(ret), KP(session_info));
    } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy must not null", K(ret), KP(GCTX.sql_proxy_));
    } else if (OB_FAIL(query_inner_sql.assign_fmt(
      "select merge_version, dll from OCEANBASE.%s where database_id = %ld and key_id = %ld and compile_db_id = %ld and arch_type = %d",
                        OB_ALL_NCOMP_DLL_TNAME, database_id_, key_id_, compile_db_id_, arch_type_))) {
      LOG_WARN("assign format failed", K(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, result) {
        if (OB_FAIL(sql_proxy->read(result, tenant_id_, query_inner_sql.ptr()))) {
          LOG_WARN("execute query failed", K(ret), K(query_inner_sql), K(tenant_id_));
        } else if (OB_NOT_NULL(result.get_result())) {
          if (OB_FAIL(result.get_result()->next())) {
            if (OB_ITER_END == ret) {
              op = ObRoutinePersistentInfo::ObPLOperation::INSERT;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to get next", K(ret), K(tenant_id_));
            }
          } else {
            EXTRACT_INT_FIELD_MYSQL(*(result.get_result()), "merge_version", merge_version, int64_t);
            EXTRACT_VARCHAR_FIELD_MYSQL(*(result.get_result()), "dll", binary);
            if (OB_SUCC(ret)) {
              bool match = false;
              int64_t tenant_schema_version = OB_INVALID_VERSION;
              if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, tenant_schema_version))) {
                LOG_WARN("fail to get schema version");
              } else if (merge_version == tenant_schema_version) {
                match = true;
                op = ObRoutinePersistentInfo::ObPLOperation::NONE;
              } else if (merge_version < tenant_schema_version) {
                if (OB_FAIL(check_dep_schema(schema_guard, unit_ast.get_dependency_table(), merge_version, match))) {
                  LOG_WARN("fail to check dep schema", K(ret));
                } else if (!match) {
                  op = ObRoutinePersistentInfo::ObPLOperation::UPDATE;
                } else {
                  op = ObRoutinePersistentInfo::ObPLOperation::NONE;
                }
              } else {
                match = false;
                op = ObRoutinePersistentInfo::ObPLOperation::NONE;
              }
              if (OB_SUCC(ret) && match) {
                // gen action using dll column
                int64_t pos = 0;
                int8_t level = 0;
                int16_t id = 0;
                if (OB_FAIL(decode_dll(*session_info, schema_guard, exec_env, unit_ast, unit, binary.ptr(), binary.length(), pos, level, id))) {
                  LOG_WARN("fail to decode dll", K(ret), K(level), K(id));
                } else if (0 != level || 0 != id) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("fail to decode dll", K(ret), K(level), K(id));
                } else {
                  op = ObRoutinePersistentInfo::ObPLOperation::SUCC;
                  LOG_INFO("succ decode dll from disk", K(ret), K(key_id_), K(merge_version));
                }
              }
            }
          }
        }
      }
    }
    if (OB_FAIL(ret) &&
        OB_ERR_UNEXPECTED != ret &&
        OB_ALLOCATE_MEMORY_FAILED != ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObRoutinePersistentInfo::insert_or_update_dll_to_disk(schema::ObSchemaGetterGuard &schema_guard,
                                                      const ObString &binary,
                                                      const ObRoutinePersistentInfo::ObPLOperation op)
{
  int ret = OB_SUCCESS;

  ObMySQLProxy *sql_proxy = nullptr;

  if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id_);
    ObDMLSqlSplicer dml;
    int64_t tenant_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, tenant_schema_version))) {
      LOG_WARN("fail to get schema version");
    } else if (OB_FAIL(gen_routine_storage_dml(exec_tenant_id, dml, tenant_schema_version, binary))) {
      LOG_WARN("gen table dml failed", K(ret));
    } else {
      ObDMLExecHelper exec(*sql_proxy, exec_tenant_id);
      int64_t affected_rows = 0;
      if (ObPLOperation::UPDATE == op) {
        if (OB_FAIL(exec.exec_update(OB_ALL_NCOMP_DLL_TNAME, dml, affected_rows))) {
          LOG_WARN("execute update failed", K(ret));
        }
      } else {
        if (OB_FAIL(exec.exec_insert(OB_ALL_NCOMP_DLL_TNAME, dml, affected_rows))) {
          LOG_WARN("execute insert failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && !is_single_row(affected_rows) && !is_zero_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", K(affected_rows), K(ret), K(op), K(key_id_));
      }
    }
    // ignore duplicate key error
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      LOG_TRACE("has a duplicate key error", K(ret), K(op), K(key_id_));
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObRoutinePersistentInfo::process_storage_dll(ObIAllocator &alloc,
                                            schema::ObSchemaGetterGuard &schema_guard,
                                            ObPLCompileUnit &unit,
                                            const ObRoutinePersistentInfo::ObPLOperation op)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (!((GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_2_0 && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0) || GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_1_0) ||
             !((data_version >= DATA_VERSION_4_2_2_0 && data_version < DATA_VERSION_4_3_0_0) || data_version >= DATA_VERSION_4_3_1_0)) {
    // do nothing
  } else if (!MTL_TENANT_ROLE_CACHE_IS_PRIMARY()) {
    // do nothing
  } else if (GCONF._enable_persistent_compiled_routine && ObRoutinePersistentInfo::ObPLOperation::NONE != op) {
    // insert/update so to inner table
    int64_t pos = 0;
    int32_t total_size = 1;
    char *buf = NULL;
    ObString dll;
    OZ (get_total_size(unit, total_size));
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(buf = (char *)alloc.alloc(total_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(total_size));
      } else {
        buf[total_size - 1] = '\0';
        dll.assign_ptr(buf, total_size);
      }
    }
    OZ (encode_dll(unit, dll, pos, 0, 0));
    OZ (insert_or_update_dll_to_disk(schema_guard, dll, op));
    if (OB_FAIL(ret) &&
        OB_ERR_UNEXPECTED != ret &&
        OB_ALLOCATE_MEMORY_FAILED != ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObRoutinePersistentInfo::delete_dll_from_disk(common::ObISQLClient &trans,
                                              uint64_t tenant_id,
                                              uint64_t key_id,
                                              uint64_t database_id)
{
  int ret = OB_SUCCESS;

  uint64_t data_version = 0;
  share::ObTenantRole tenant_role;
  ObMySQLProxy *sql_proxy = nullptr;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (!((GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_2_0 && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0) || GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_1_0) ||
             !((data_version >= DATA_VERSION_4_2_2_0 && data_version < DATA_VERSION_4_3_0_0) || data_version >= DATA_VERSION_4_3_1_0)) {
    // do nothing
  } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sql proxy", K(ret));
  } else if (OB_FAIL(ObAllTenantInfoProxy::get_tenant_role(sql_proxy, tenant_id, tenant_role))) {
    LOG_WARN("fail to get tenant role", K(ret));
  } else if (!tenant_role.is_valid()) {
    ret = OB_NEED_WAIT;
    LOG_WARN("tenant role is not ready", K(ret));
  } else if (tenant_role.is_standby()) {
    // do nothing
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObSqlString sql;
    int64_t affected_rows = 0;
    ObMySQLProxy *sql_proxy = nullptr;
    if (OB_INVALID_ID == key_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected key id.", K(ret));
    } else if (OB_FAIL(sql.assign_fmt("delete FROM %s where database_id = %ld and key_id = %ld", OB_ALL_NCOMP_DLL_TNAME, database_id, key_id))) {
      LOG_WARN("delete from __all_ncomp_dll table failed.", K(ret), K(key_id));
    } else {
      if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute query failed", K(ret), K(sql));
      } else {
        // do nothing
        LOG_INFO("succ to delete dll", K(key_id), K(tenant_id), K(affected_rows));
      }
    }
  }

  return ret;
}

}
}