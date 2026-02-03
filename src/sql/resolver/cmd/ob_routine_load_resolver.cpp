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

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/cmd/ob_routine_load_stmt.h"
#include "sql/resolver/cmd/ob_routine_load_resolver.h"
#include "lib/json/ob_json_print_utils.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "share/catalog/ob_catalog_utils.h"
#include "lib/utility/ob_fast_convert.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{


int ObCreateRoutineLoadResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObCreateRoutineLoadStmt *mystmt = NULL;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  uint64_t data_version = 0;

  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", KR(ret));
  } else if (OB_UNLIKELY(T_CREATE_ROUTINE_LOAD != node->type_)
             || OB_UNLIKELY(5 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", KR(ret), K(node->type_), K(node->num_child_));
  } else if (OB_ISNULL(session_info_)
             || OB_ISNULL(allocator_)
             || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", KR(ret), KP(session_info_), KP(allocator_), KP(schema_checker_));
  } else if (OB_ISNULL(mystmt = create_stmt<ObCreateRoutineLoadStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create select stmt", KR(ret));
  //TODO: 权限检查，判断系统租户、普通租户、login_tenant_id等。
  //      是否能在系统租户下创建？
  } else if (FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret));
  } else if (data_version < DATA_VERSION_4_5_1_0) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant data version is below 4.5.1", KR(ret), K(tenant_id));
  } else {
    stmt_ = mystmt;
    mystmt->set_tenant_id(tenant_id);
  }

  /* set exec_env */
  //TODO: exec tenant must be user tenant
  if (OB_SUCC(ret)) {
    char *job_exec_buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(job_exec_buf = static_cast<char*>(allocator_->alloc(OB_MAX_PROC_ENV_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", KR(ret));
    } else if (OB_FAIL(ObExecEnv::gen_exec_env(*session_info_, job_exec_buf, OB_MAX_PROC_ENV_LENGTH, pos))){
      LOG_WARN("generate exec env failed", K(ret), K(session_info_));
    } else {
      mystmt->set_exec_env((ObString(pos, job_exec_buf)));
    }
  }

  /* routine load job name */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[0]) {
      if (OB_UNLIKELY(T_IDENT != node->children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", KR(ret));
      } else {
        ObString job_name;
        job_name.assign_ptr((char *)(node->children_[0]->str_value_),
                              static_cast<int32_t>(node->children_[0]->str_len_));
        mystmt->set_job_name(job_name);
      }
    }
  }

  /* table name */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[1]) {
      uint64_t session_database_id = session_info_->get_database_id();
      ObString catalog_name;
      ObString database_name;
      ObString table_name;
      const ObTableSchema *tschema = nullptr;
      bool is_table_exist = false;
      if (OB_FAIL(resolve_table_relation_node(node->children_[1],
                                              table_name,
                                              database_name,
                                              catalog_name))) {
        SQL_RESV_LOG(WARN, "fail to resolve table name", KR(ret));
      } else if (OB_UNLIKELY(!ObCatalogUtils::is_internal_catalog_name(catalog_name))) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "load data into external catalog is");
      } else if (OB_FAIL(schema_checker_->check_table_exists(tenant_id,
                                                              database_name,
                                                              table_name,
                                                              false/*is_index_table*/,
                                                              false/*is_hidden*/,
                                                              is_table_exist))) {
        LOG_WARN("fail to check table or index exist", KR(ret), K(tenant_id), K(session_database_id), K(table_name));
      } else if (!is_table_exist) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", KR(ret), K(tenant_id), K(database_name), K(table_name));
      } else if (OB_FAIL(schema_checker_->get_table_schema(tenant_id,
                                                            database_name,
                                                            table_name,
                                                            false/*is_index_table*/,
                                                            tschema))) {
        LOG_WARN("get table schema failed", KR(ret), K(tenant_id), K(database_name), K(table_name));
      } else if (OB_UNLIKELY(tschema->is_view_table())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("load data to the view is not supported", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "load data to the view is");
      } else if (OB_FAIL(check_trigger_constraint(tschema))) {
        LOG_WARN("check trigger constraint failed", K(ret), KPC(tschema));
      } else {
        mystmt->set_table_id(tschema->get_table_id());
        mystmt->set_table_name(table_name);
        mystmt->set_database_id(tschema->get_database_id());
        mystmt->set_database_name(database_name);
        int32_t size = table_name.length() + database_name.length() + 6;  //  eg: `test`.`t1`
        char *buf = NULL;
        int64_t pos = 0;
        if (OB_ISNULL(buf =
            static_cast<char *>(allocator_->alloc(size * sizeof(char))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory for database and table name failed", KR(ret));
        } else if (OB_FAIL(databuff_printf(
                                buf, size, pos,
                                lib::is_oracle_mode() ? "\"%.*s\".\"%.*s\"" : "`%.*s`.`%.*s`",
                                database_name.length(), database_name.ptr(),
                                table_name.length(), table_name.ptr()))) {
          LOG_WARN("fail to print combined name", KR(ret), K(size), K(pos), K(database_name), K(table_name));
        } else {
          mystmt->set_combined_name(buf, pos);
        }
      }
    }
  }

  /* load_properties */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[2]) {
      if (OB_UNLIKELY(T_LOAD_PROPERTIES != node->children_[2]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", KR(ret));
      } else if (OB_FAIL(resolve_load_properties(*node->children_[2], *mystmt))) {
        LOG_WARN("fail to resolve load properties", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // Set default dupl_action if not set
      if (mystmt->get_dupl_action() == ObLoadDupActionType::LOAD_INVALID_MODE) {
        mystmt->set_dupl_action(ObLoadDupActionType::LOAD_STOP_ON_DUP);
      }
      // Auto fill field_or_var_list if empty
      if (mystmt->get_field_or_var_list().empty()) {
        if (OB_FAIL(resolve_empty_field_or_var_list_node(mystmt->get_table_id(),
                                mystmt->get_field_or_var_list()))) {
          LOG_WARN("resolve empty field var list failed", KR(ret), KPC(mystmt));
        }
      }
    }
  }

  /* job_properties */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[3]) {
      if (OB_UNLIKELY(T_EXTERNAL_PROPERTIES != node->children_[3]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid node", KR(ret));
      } else {
        if (OB_FAIL(resolve_job_properties(*(node->children_[3]), *mystmt))) {
          LOG_WARN("fail to resolve job properties", KR(ret));
        } else if (OB_UNLIKELY(mystmt->get_topic_name().empty())) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("topic name is empty", KR(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "without \"kafka_topic\"");
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid null node", KR(ret));
    }
  }

  return ret;
}

int ObCreateRoutineLoadResolver::resolve_load_properties(
    const ParseNode &load_prop_node,
    ObCreateRoutineLoadStmt &stmt)
{
  int ret = OB_SUCCESS;
  //Currently, we do not use direct load
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null", KR(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", KR(ret));
  } else {
    ObLoadDupActionType dupl_action = ObLoadDupActionType::LOAD_INVALID_MODE;
    for (int64_t i = 0; OB_SUCC(ret) && i < load_prop_node.num_child_; i++) {
      ParseNode *node = load_prop_node.children_[i];
      if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected node", KR(ret), K(i), K(load_prop_node.num_child_));
      } else {
        switch (node->type_) {
          case T_PARALLEL: {
            stmt.set_parallel(static_cast<int64_t>(node->children_[0]->value_));
            if (stmt.get_parallel() <= 0) {
              ret = OB_OP_NOT_ALLOW;
              LOG_WARN("parallel is too small", KR(ret), K(stmt.get_parallel()));
              LOG_USER_ERROR(OB_OP_NOT_ALLOW, "parallel equal to 0 is");
            }
            break;
          }
          case T_COLUMN_LIST: {
            if (OB_FAIL(resolve_field_or_var_list_node(*node, case_mode,
              stmt.get_database_name(),
              stmt.get_table_id(),
              stmt.get_field_or_var_list()))) {
              LOG_WARN("failed to resolve field or var list_node", KR(ret), K(case_mode), K(stmt));
            }
            break;
          }
          case T_USE_PARTITION: {
            if (OB_FAIL(resolve_partitions(*node, stmt.get_table_id(),
                                  stmt.get_part_ids(), stmt.get_part_names()))) {
              LOG_WARN("fail to resolve partition", KR(ret), K(stmt));
            }
            break;
          }
          case T_IGNORE: {
            if (ObLoadDupActionType::LOAD_REPLACE == dupl_action) {
              ret = OB_OP_NOT_ALLOW;
              LOG_WARN("replace is not allowed with ignore", KR(ret));
              LOG_USER_ERROR(OB_OP_NOT_ALLOW, "with both ignore and replace is");
            } else {
              dupl_action = ObLoadDupActionType::LOAD_IGNORE;
            }
            break;
          }
          case T_REPLACE: {
            if (ObLoadDupActionType::LOAD_IGNORE == dupl_action) {
              ret = OB_OP_NOT_ALLOW;
              LOG_WARN("ignore is not allowed with replace", KR(ret));
              LOG_USER_ERROR(OB_OP_NOT_ALLOW, "with both ignore and replace is");
            } else {
              dupl_action = ObLoadDupActionType::LOAD_REPLACE;
            }
            break;
          }
          case T_WHERE_CLAUSE: {
            stmt.set_where_clause(ObString(static_cast<int32_t>(node->str_len_), node->str_value_));
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected node type", KR(ret), K(node->type_));
            break;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (lib::is_oracle_mode()
          && (ObLoadDupActionType::LOAD_IGNORE == dupl_action
              || ObLoadDupActionType::LOAD_REPLACE == dupl_action)) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "load data with ignore or replace option in oracle mode");
      } else {
        if (ObLoadDupActionType::LOAD_INVALID_MODE == dupl_action) {
          stmt.set_dupl_action(ObLoadDupActionType::LOAD_STOP_ON_DUP);
        } else {
          stmt.set_dupl_action(dupl_action);
        }
        if (stmt.get_field_or_var_list().empty()) {
          if (OB_FAIL(resolve_empty_field_or_var_list_node(stmt.get_table_id(),
                                  stmt.get_field_or_var_list()))) {
            LOG_WARN("resolve empty field var list failed", KR(ret), K(stmt));
          }
        }
      }
    }
  }

  return ret;
}

int ObCreateRoutineLoadResolver::resolve_job_properties(
    const ParseNode &job_prop_node,
    ObCreateRoutineLoadStmt &stmt)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  int64_t buf_len = DEFAULT_BUF_LENGTH;
  char *sql_buf = NULL;
  int64_t sql_pos = 0;
  const int64_t sql_buf_len = DEFAULT_BUF_LENGTH;
  //TODO: buf length
  if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret), K(buf_len));
  } else if (OB_ISNULL(sql_buf = static_cast<char*>(allocator_->alloc(sql_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc sql_buf", KR(ret), K(sql_buf_len));
  } else if (OB_FAIL(J_OBJ_START())) {
    LOG_WARN("fail to print obj start", KR(ret));
  } else {
    bool has_first_prop_for_json = false;
    bool has_first_prop_for_sql = false;
    bool has_kafka_broker_list = false;
    const char quote_char = lib::is_oracle_mode() ? '\'' : '"';
    for (int64_t i = 0; OB_SUCC(ret) && i < job_prop_node.num_child_; i++) {
      ParseNode *node = job_prop_node.children_[i];
      if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected node", KR(ret), K(i), K(job_prop_node.num_child_));
      } else if (OB_UNLIKELY(0 >= node->num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected node num", KR(ret), K(node->num_child_));
      } else {
        switch (node->type_) {
          case T_EXTERNAL_FILE_FORMAT_TYPE: {
            ObExternalFileFormat::FormatType format_type = ObExternalFileFormat::INVALID_FORMAT;
            ObString string_v = ObString(static_cast<int32_t>(node->children_[0]->str_len_),
                                            node->children_[0]->str_value_).trim_space_only();
            for (int i = 0; i < ObExternalFileFormat::MAX_FORMAT; i++) {
              if (0 == string_v.case_compare(ObExternalFileFormat::FORMAT_TYPE_STR[i])) {
                format_type = static_cast<ObExternalFileFormat::FormatType>(i);
                break;
              }
            }
            if (ObExternalFileFormat::KAFKA_FORMAT != format_type) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not support this stream type", KR(ret), K(string_v));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "this stream type");
            }
            break;
          }
          //TODO: T_KAFKA_CUSTOM_PROPERTY更名
          case T_KAFKA_CUSTOM_PROPERTY: {
            if (OB_UNLIKELY(2 != node->num_child_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected node", KR(ret), K(node->num_child_));
            } else {
              ObString prop_name = ObString(static_cast<int32_t>(node->children_[0]->str_len_),
                                            node->children_[0]->str_value_).trim_space_only();
              ObString prop_value = ObString(static_cast<int32_t>(node->children_[1]->str_len_),
                                              node->children_[1]->str_value_).trim_space_only();
              // Validate MAX_BATCH_INTERVAL, MAX_BATCH_ROWS, MAX_BATCH_SIZE
              if (0 == prop_name.compare(ObKAFKAGeneralFormat::MAX_BATCH_INTERVAL)) {
                bool valid = false;
                int64_t integer_value = ObFastAtoi<int64_t>::atoi(prop_value.ptr(), prop_value.ptr() + prop_value.length(), valid);
                if (!valid) {
                  ret = OB_INVALID_DATA;
                  LOG_WARN("fail to convert max_batch_interval string to int", KR(ret), K(prop_value), K(integer_value));
                } else if (integer_value <= 0) {
                  ret = OB_OP_NOT_ALLOW;
                  LOG_WARN("max_batch_interval is too small", KR(ret), K(integer_value));
                  LOG_USER_ERROR(OB_OP_NOT_ALLOW, "max_batch_interval less than or equal to 0 is");
                } else {
                  stmt.set_max_batch_interval_s(integer_value);
                }
              } else if (0 == prop_name.compare(ObKAFKAGeneralFormat::MAX_BATCH_ROWS)) {
                bool valid = false;
                int64_t integer_value = ObFastAtoi<int64_t>::atoi(prop_value.ptr(), prop_value.ptr() + prop_value.length(), valid);
                if (!valid) {
                  ret = OB_INVALID_DATA;
                  LOG_WARN("fail to convert max_batch_rows string to int", KR(ret), K(prop_value), K(integer_value));
                } else if (integer_value < 200000) {
                  ret = OB_OP_NOT_ALLOW;
                  LOG_WARN("max_batch_rows is too small", KR(ret), K(integer_value));
                  LOG_USER_ERROR(OB_OP_NOT_ALLOW, "max_batch_rows less than 200000 is");
                }
              } else if (0 == prop_name.compare(ObKAFKAGeneralFormat::MAX_BATCH_SIZE)) {
                bool valid = false;
                int64_t integer_value = ObFastAtoi<int64_t>::atoi(prop_value.ptr(), prop_value.ptr() + prop_value.length(), valid);
                if (!valid) {
                  ret = OB_INVALID_DATA;
                  LOG_WARN("fail to convert max_batch_size string to int", KR(ret), K(prop_value), K(integer_value));
                } else if (integer_value < 100 * 1024 * 1024 || integer_value > 1024 * 1024 * 1024) {
                  ret = OB_OP_NOT_ALLOW;
                  LOG_WARN("max_batch_size is too small or too large", KR(ret), K(integer_value));
                  LOG_USER_ERROR(OB_OP_NOT_ALLOW, "max_batch_size less than 100MB or greater than 1GB is");
                }
              } else if (0 == prop_name.compare(ObKAFKAGeneralFormat::KAFKA_TOPIC)) {
                stmt.set_topic_name(prop_value);
              } else if (0 == prop_name.compare(ObKAFKAGeneralFormat::KAFKA_PARTITIONS)) {
                stmt.set_kafka_partitions_str(prop_value);
              } else if (0 == prop_name.compare(ObKAFKAGeneralFormat::KAFKA_OFFSETS)) {
                stmt.set_kafka_offsets_str(prop_value);
              } else {
                if (OB_FAIL(stmt.get_kafka_custom_properties().push_back(std::make_pair(prop_name, prop_value)))) {
                  LOG_WARN("fail to push back kafka custom property", KR(ret), K(prop_name), K(prop_value));
                } else if (0 == prop_name.compare(ObKAFKAGeneralFormat::KAFKA_BROKER_LIST)) {
                  has_kafka_broker_list = true;
                }
              }
              if (OB_SUCC(ret) && !ObKAFKAGeneralFormat::is_security_auth_param(prop_name)) {
                if (has_first_prop_for_json && OB_FAIL(J_COMMA())) {
                  LOG_WARN("fail to print comma", KR(ret));
                } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\"%.*s\":\"%.*s\"",
                                prop_name.length(), prop_name.ptr() /*prop_name*/,
                                prop_value.length(), prop_value.ptr() /*prop_value*/))) {
                  LOG_WARN("fail to printf", KR(ret), K(i), K(node->type_), K(buf_len), K(pos), K(prop_name), K(prop_value));
                } else {
                  has_first_prop_for_json = true;
                }
              }
              if (OB_SUCC(ret)) {
                if (0 == prop_name.compare(ObKAFKAGeneralFormat::KAFKA_PARTITIONS)
                    || 0 == prop_name.compare(ObKAFKAGeneralFormat::KAFKA_OFFSETS)) {
                } else {
                  if (has_first_prop_for_sql && OB_FAIL(databuff_printf(sql_buf, sql_buf_len, sql_pos, ", "))) {
                    LOG_WARN("fail to print comma", KR(ret), K(sql_buf_len), K(sql_pos));
                  } else if (OB_FAIL(databuff_printf(sql_buf, sql_buf_len, sql_pos, "%c%.*s%c=%c%.*s%c",
                                  quote_char, prop_name.length(), prop_name.ptr() /*prop_name*/, quote_char,
                                  quote_char, prop_value.length(), prop_value.ptr() /*prop_value*/, quote_char))) {
                    LOG_WARN("fail to printf", KR(ret), K(i), K(node->type_), K(sql_buf_len), K(sql_pos), K(prop_name), K(prop_value));
                  } else {
                    has_first_prop_for_sql = true;
                  }
                }
              }
            }
            break;
          }
          case T_EXTERNAL_FILE_FORMAT: {
            if (has_first_prop_for_json && OB_FAIL(J_COMMA())) {
              LOG_WARN("fail to print comma", KR(ret));
            } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\"FORMAT\":"))) {
              LOG_WARN("fail to printf", KR(ret), K(buf_len), K(pos));
            } else {
              has_first_prop_for_json = true;
              ObExternalFileFormat format;
              ObResolverUtils::FileFormatContext ff_ctx;
              if (OB_FAIL(format.csv_format_.init_format(ObDataInFileStruct(),
                                                        OB_MAX_COLUMN_NUMBER,
                                                        CS_TYPE_UTF8MB4_BIN))) {
                LOG_WARN("failed to init csv format", KR(ret));
              }
              for (int i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
                if (OB_ISNULL(node->children_[i])) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("fail to get unexpected NULL ptr", KR(ret), K(node->num_child_));
                } else if (OB_FAIL(ObResolverUtils::resolve_file_format(node->children_[i], format, params_, ff_ctx))) {
                  LOG_WARN("fail to resolve file format", KR(ret), K(format));
                }
              }
              if (OB_SUCC(ret) && ObExternalFileFormat::CSV_FORMAT != format.format_type_) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("not support this format type", KR(ret), K(format));
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "this format type");
              }
              if (FAILEDx(file_format_to_str(format, buf, buf_len, pos))) {
                LOG_WARN("fail to file format to str", KR(ret), K(format), KP(buf), K(buf_len), K(pos));
              } else {
                ObString file_format_str = ObString(static_cast<int32_t>(node->str_len_), node->str_value_).trim_space_only();
                if (has_first_prop_for_sql && OB_FAIL(databuff_printf(sql_buf, sql_buf_len, sql_pos, ", "))) {
                  LOG_WARN("fail to print comma", KR(ret), K(sql_buf_len), K(sql_pos));
                } else if (OB_FAIL(databuff_printf(sql_buf, sql_buf_len, sql_pos, "%.*s",
                                file_format_str.length(), file_format_str.ptr() /*file_format_str*/))) {
                  LOG_WARN("fail to printf", KR(ret), K(i), K(node->type_), K(sql_buf_len), K(sql_pos), K(file_format_str));
                } else {
                  has_first_prop_for_sql = true;
                }
              }
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected node type", KR(ret), K(node->type_));
            break;
          }
        }
      }
    }
    if (FAILEDx(J_OBJ_END())) {
      LOG_WARN("fail to print obj end", KR(ret));
    } else {
      stmt.set_job_prop_json_str(buf, pos);
      stmt.set_job_prop_sql_str(sql_buf, sql_pos);
      if (OB_UNLIKELY(!has_kafka_broker_list)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("kafka broker list is not set", KR(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "without \"bootstrap.servers\"");
      }
    }
  }
  return ret;
}

//NOTE: Currently only CSV mode parsing is provided
int ObCreateRoutineLoadResolver::file_format_to_str(
    const ObExternalFileFormat &format,
    char *buf,
    const int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(J_QUOTE())) {
    LOG_WARN("fail to print quote", KR(ret));
  } else if (OB_FAIL(J_OBJ_START())) {
    LOG_WARN("fail to print obj start", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\"TYPE\":\"CSV\", "))) {
    LOG_WARN("fail to printf kv", KR(ret));
  } else {
    int64_t origin_pos = pos;
    const ObCSVGeneralFormat &csv = format.csv_format_;
    const ObOriginFileFormat &origin_format = format.origin_file_format_str_;
    if (!origin_format.origin_line_term_str_.empty()
        && OB_FAIL(databuff_printf(buf, buf_len, pos, "\"LINE_DELIMITER\":%.*s, ", origin_format.origin_line_term_str_.length(), origin_format.origin_line_term_str_.ptr()))) {
      LOG_WARN("fail to printf", KR(ret), K(buf_len), K(pos), K(origin_format.origin_line_term_str_));
    } else if (!origin_format.origin_field_term_str_.empty()
               && OB_FAIL(databuff_printf(buf, buf_len, pos, "\"FIELD_DELIMITER\":%.*s, ", origin_format.origin_field_term_str_.length(), origin_format.origin_field_term_str_.ptr()))) {
      LOG_WARN("fail to printf", KR(ret), K(buf_len), K(pos), K(origin_format.origin_field_term_str_));
    } else if (!origin_format.origin_field_escaped_str_.empty()
              && OB_FAIL(databuff_printf(buf, buf_len, pos, "\"ESCAPE\":%.*s, ", origin_format.origin_field_escaped_str_.length(), origin_format.origin_field_escaped_str_.ptr()))) {
      LOG_WARN("fail to printf", KR(ret), K(buf_len), K(pos), K(origin_format.origin_field_escaped_str_));
    } else if (!origin_format.origin_field_enclosed_str_.empty()
               && OB_FAIL(databuff_printf(buf, buf_len, pos, "\"FIELD_OPTIONALLY_ENCLOSED_BY\":%.*s, ", origin_format.origin_field_enclosed_str_.length(), origin_format.origin_field_enclosed_str_.ptr()))) {
      LOG_WARN("fail to printf", KR(ret), K(buf_len), K(pos), K(origin_format.origin_field_enclosed_str_));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\"ENCODING\":'%s', ", ObCharset::charset_name(csv.cs_type_)))) {
      LOG_WARN("fail to printf", KR(ret), K(buf_len), K(pos), K(csv.cs_type_));
    } else if (0 != csv.null_if_.count()
               && OB_FAIL(databuff_printf(buf, buf_len, pos, "\"NULL_IF\":(%.*s), ", origin_format.origin_null_if_str_.length(), origin_format.origin_null_if_str_.ptr()))) {
      LOG_WARN("fail to printf", KR(ret), K(buf_len), K(pos), K(origin_format.origin_null_if_str_));
    } else if (csv.skip_blank_lines_
               && OB_FAIL(databuff_printf(buf, buf_len, pos, "\"SKIP_BLANK_LINES\":TRUE, "))) {
      LOG_WARN("fail to printf", KR(ret), K(buf_len), K(pos), K(csv.skip_blank_lines_));
    } else if (csv.trim_space_ &&
               OB_FAIL(databuff_printf(buf, buf_len, pos, "\"TRIM_SPACE\":TRUE, "))) {
      LOG_WARN("fail to printf", KR(ret), K(buf_len), K(pos), K(csv.trim_space_));
    } else if (csv.empty_field_as_null_ &&
               OB_FAIL(databuff_printf(buf, buf_len, pos, "\"EMPTY_FIELD_AS_NULL\":TRUE, "))) {
      LOG_WARN("fail to printf", KR(ret), K(buf_len), K(pos), K(csv.empty_field_as_null_));
    } else if (pos - origin_pos > 2 && buf[pos - 2] == ',' && buf[pos - 1] == ' ') {
      pos -= 2;
    }
    if (FAILEDx(J_OBJ_END())) {
      LOG_WARN("fail to print obj end", KR(ret));
    } else if (OB_FAIL(J_QUOTE())) {
      LOG_WARN("fail to print quote", KR(ret));
    }
  }
  return ret;
}

int ObPauseRoutineLoadResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObPauseRoutineLoadStmt *mystmt = NULL;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  uint64_t data_version = 0;

  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", KR(ret));
  } else if (OB_UNLIKELY(T_PAUSE_ROUTINE_LOAD != node->type_)
             || OB_UNLIKELY(1 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", KR(ret), K(node->type_), K(node->num_child_));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", KR(ret), KP(session_info_));
  } else if (OB_ISNULL(mystmt = create_stmt<ObPauseRoutineLoadStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create pause routine load stmt", KR(ret));
  } else if (FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret));
  } else if (data_version < DATA_VERSION_4_5_1_0) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant data version is below 4.5.1", KR(ret), K(tenant_id));
  } else {
    stmt_ = mystmt;
    mystmt->set_tenant_id(tenant_id);
  }

  /* resolve relation_name: routine_load_job_name */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[0]) {
      ParseNode *job_node = node->children_[0];
      ObString job_name;
      job_name.assign_ptr(const_cast<char*>(job_node->str_value_),
                          static_cast<int32_t>(job_node->str_len_));
      mystmt->set_job_name(job_name);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid null relation_name node", KR(ret));
    }
  }

  return ret;
}

int ObResumeRoutineLoadResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObResumeRoutineLoadStmt *mystmt = NULL;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  uint64_t data_version = 0;

  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", KR(ret));
  } else if (OB_UNLIKELY(T_RESUME_ROUTINE_LOAD != node->type_)
             || OB_UNLIKELY(1 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", KR(ret), K(node->type_), K(node->num_child_));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", KR(ret), KP(session_info_));
  } else if (OB_ISNULL(mystmt = create_stmt<ObResumeRoutineLoadStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create resume routine load stmt", KR(ret));
  } else if (FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret));
  } else if (data_version < DATA_VERSION_4_5_1_0) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant data version is below 4.5.1", KR(ret), K(tenant_id));
  } else {
    stmt_ = mystmt;
    mystmt->set_tenant_id(tenant_id);
  }

  /* resolve relation_name: routine_load_job_name */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[0]) {
      ParseNode *job_node = node->children_[0];
      ObString job_name;
      job_name.assign_ptr(const_cast<char*>(job_node->str_value_),
                          static_cast<int32_t>(job_node->str_len_));
      mystmt->set_job_name(job_name);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid null relation_name node", KR(ret));
    }
  }

  return ret;
}

int ObStopRoutineLoadResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObStopRoutineLoadStmt *mystmt = NULL;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  uint64_t data_version = 0;

  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", KR(ret));
  } else if (OB_UNLIKELY(T_STOP_ROUTINE_LOAD != node->type_)
             || OB_UNLIKELY(1 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", KR(ret), K(node->type_), K(node->num_child_));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", KR(ret), KP(session_info_));
  } else if (OB_ISNULL(mystmt = create_stmt<ObStopRoutineLoadStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create stop routine load stmt", KR(ret));
  } else if (FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret));
  } else if (data_version < DATA_VERSION_4_5_1_0) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("tenant data version is below 4.5.1", KR(ret), K(tenant_id));
  } else {
    stmt_ = mystmt;
    mystmt->set_tenant_id(tenant_id);
  }

  /* resolve relation_name: routine_load_job_name */
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[0]) {
      ParseNode *job_node = node->children_[0];
      ObString job_name;
      job_name.assign_ptr(const_cast<char*>(job_node->str_value_),
                          static_cast<int32_t>(job_node->str_len_));
      mystmt->set_job_name(job_name);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid null relation_name node", KR(ret));
    }
  }

  return ret;
}

}
}
