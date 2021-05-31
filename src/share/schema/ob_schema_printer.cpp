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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/schema/ob_schema_printer.h"

#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/allocator/page_arena.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "common/object/ob_obj_type.h"
#include "common/ob_store_format.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_unit_getter.h"
#include "share/ob_schema_status_proxy.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/config/ob_server_config.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_printer.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/parser/ob_parser.h"
#include "observer/ob_sql_client_decorator.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase {
namespace share {
namespace schema {
using namespace std;
using namespace common;
/*-----------------------------------------------------------------------------
 *  ObSchemaPrinter
 *-----------------------------------------------------------------------------*/
ObSchemaPrinter::ObSchemaPrinter(ObSchemaGetterGuard& schema_guard) : schema_guard_(schema_guard)
{}

int ObSchemaPrinter::print_table_definition(uint64_t table_id, char* buf, const int64_t& buf_len, int64_t& pos,
    const ObTimeZoneInfo* tz_info, const common::ObLengthSemantics default_length_semantics, bool agent_mode) const
{
  // TODO(: refactor this function):consider index_position in

  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  const ObDatabaseSchema* db_schema = NULL;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  const char* prefix_arr[3] = {"", " TEMPORARY", " GLOBAL TEMPORARY"};
  int prefix_idx = 0;
  if (OB_FAIL(schema_guard_.get_table_schema(table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else if (NULL == table_schema || table_schema->is_dropped_schema()) {
    ret = OB_TABLE_NOT_EXIST;
    SHARE_SCHEMA_LOG(WARN, "Unknow table", K(ret), K(table_id));
  } else if (OB_FAIL(schema_guard_.get_database_schema(table_schema->get_database_id(), db_schema))) {
    SHARE_SCHEMA_LOG(WARN, "fail to get database schema", K(ret));
  } else if (NULL == db_schema) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "database not exist", K(ret));
  } else {
    if (table_schema->is_mysql_tmp_table()) {
      prefix_idx = 1;
    } else if (table_schema->is_oracle_tmp_table()) {
      prefix_idx = 2;
    } else {
      prefix_idx = 0;
    }
    ObString new_table_name;
    ObString new_db_name;
    if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
            allocator, table_schema->get_table_name_str(), new_table_name, share::is_oracle_mode()))) {
      SHARE_SCHEMA_LOG(
          WARN, "fail to generate new name with escape character", K(ret), K(table_schema->get_table_name()));
    } else if (agent_mode &&
               OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                   allocator, db_schema->get_database_name_str(), new_db_name, share::is_oracle_mode()))) {
      SHARE_SCHEMA_LOG(
          WARN, "fail to generate new name with escape character", K(ret), K(db_schema->get_database_name_str()));
    } else if (OB_FAIL(!agent_mode
                           ? databuff_printf(buf,
                                 buf_len,
                                 pos,
                                 share::is_oracle_mode() ? "CREATE%s TABLE \"%s\" (\n" : "CREATE%s TABLE `%s` (\n",
                                 prefix_arr[prefix_idx],
                                 new_table_name.empty() ? "" : new_table_name.ptr())
                           : databuff_printf(buf,
                                 buf_len,
                                 pos,
                                 share::is_oracle_mode() ? "CREATE%s TABLE \"%s\".\"%s\" (\n"
                                                         : "CREATE%s TABLE `%s`.`%s` (\n",
                                 prefix_arr[prefix_idx],
                                 new_db_name.ptr(),
                                 new_table_name.empty() ? "" : new_table_name.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print create table prefix", K(ret), K(table_schema->get_table_name()));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing...
  } else {
    if (OB_FAIL(print_table_definition_columns(
            *table_schema, buf, buf_len, pos, tz_info, default_length_semantics, agent_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print columns", K(ret), K(*table_schema));
    } else if (OB_FAIL(print_table_definition_rowkeys(*table_schema, buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print rowkeys", K(ret), K(*table_schema));
    } else if (!agent_mode && OB_FAIL(print_table_definition_foreign_keys(*table_schema, buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print foreign keys", K(ret), K(*table_schema));
    } else if (!agent_mode && OB_FAIL(print_table_definition_indexes(*table_schema, buf, buf_len, pos, true))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print indexes", K(ret), K(*table_schema));
    } else if (!agent_mode && !share::is_oracle_mode() &&
               OB_FAIL(print_table_definition_indexes(*table_schema, buf, buf_len, pos, false))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print indexes", K(ret), K(*table_schema));
    } else if (OB_FAIL(print_table_definition_constraints(*table_schema, buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print constraints", K(ret), K(*table_schema));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n) "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print )", K(ret));
    } else if (OB_FAIL(print_table_definition_table_options(*table_schema, buf, buf_len, pos, false, agent_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print table options", K(ret), K(*table_schema));
    } else if (OB_FAIL(
                   print_table_definition_partition_options(*table_schema, buf, buf_len, pos, agent_mode, tz_info))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print partition options", K(ret), K(*table_schema));
    } else if (print_table_definition_on_commit_options(*table_schema, buf, buf_len, pos)) {
      SHARE_SCHEMA_LOG(WARN, "fail to print on commit options", K(ret), K(*table_schema));
    }
    SHARE_SCHEMA_LOG(DEBUG, "print table schema", K(ret), K(*table_schema));
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_columns(const ObTableSchema& table_schema, char* buf,
    const int64_t& buf_len, int64_t& pos, const ObTimeZoneInfo* tz_info,
    const common::ObLengthSemantics default_length_semantics, bool is_agent_mode) const
{
  int ret = OB_SUCCESS;
  bool is_first_col = true;
  ObColumnIterByPrevNextID iter(table_schema);
  const ObColumnSchemaV2* col = NULL;
  bool is_oracle_mode = share::is_oracle_mode();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  while (OB_SUCC(ret) && OB_SUCC(iter.next(col))) {
    if (OB_ISNULL(col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The column is null", K(ret));
    } else {
      SHARE_SCHEMA_LOG(DEBUG, "print_table_definition_columns", KPC(col));
      if (col->is_shadow_column()) {
        // do nothing
      } else if (col->is_hidden()) {
        // do nothing
      } else {
        if (true == is_first_col) {
          is_first_col = false;
        } else {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n"))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print enter", K(ret));
          }
        }
        ObString new_col_name;
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                       allocator, col->get_column_name_str(), new_col_name, is_oracle_mode))) {
          SHARE_SCHEMA_LOG(
              WARN, "fail to generate new name with escape character", K(ret), K(col->get_column_name_str()));
        } else if (OB_FAIL(databuff_printf(
                       buf, buf_len, pos, is_oracle_mode ? "  \"%s\" " : "  `%s` ", new_col_name.ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print column", K(ret), K(*col));
        }

        if (OB_SUCC(ret)) {
          int64_t start = pos;
          if (OB_FAIL(ob_sql_type_str(col->get_meta_type(),
                  col->get_accuracy(),
                  col->get_extended_type_info(),
                  default_length_semantics,
                  buf,
                  buf_len,
                  pos))) {
            SHARE_SCHEMA_LOG(WARN, "fail to get data type str", K(col->get_data_type()), K(*col), K(ret));
          } else if (share::is_oracle_mode()) {
            int64_t end = pos;
            ObString col_type_str(end - start, buf + start);
            ObCharset::caseup(ObCollationType::CS_TYPE_UTF8MB4_BIN, col_type_str);
          }
        }
        // zerofill, only for int, float, decimal
        if (OB_SUCCESS == ret && col->is_zero_fill()) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " zerofill"))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print zerofill", K(ret), K(*col));
          }
        }
        // string collation, e.g. CHARACTER SET latin1 COLLATE latin1_bin
        if (OB_FAIL(ret)) {
        } else if (ob_is_string_type(col->get_data_type()) || ob_is_enum_or_set_type(col->get_data_type())) {
          if (col->get_charset_type() == CHARSET_BINARY) {
            // nothing to do
          } else {
            if (col->get_charset_type() == table_schema.get_charset_type() &&
                col->get_collation_type() == table_schema.get_collation_type()) {
              // nothing to do
            } else {
              if (CHARSET_INVALID != col->get_charset_type() && CHARSET_BINARY != col->get_charset_type()) {
                if (share::is_oracle_mode()) {
                  // do not print charset type when in oracle mode
                } else if (OB_FAIL(databuff_printf(buf,
                               buf_len,
                               pos,
                               " CHARACTER SET %s",
                               ObCharset::charset_name(col->get_charset_type())))) {
                  SHARE_SCHEMA_LOG(WARN, "fail to print character set", K(ret), K(*col));
                }
              }
            }
            if (OB_SUCCESS == ret && !is_agent_mode && !is_oracle_mode &&
                !ObCharset::is_default_collation(col->get_collation_type()) &&
                CS_TYPE_INVALID != col->get_collation_type() && CS_TYPE_BINARY != col->get_collation_type()) {
              if (OB_FAIL(databuff_printf(
                      buf, buf_len, pos, " COLLATE %s", ObCharset::collation_name(col->get_collation_type())))) {
                SHARE_SCHEMA_LOG(WARN, "fail to print collate", K(ret), K(*col));
              }
            }
          }
        }
        // for visibility in oracle mode
        if (OB_SUCC(ret) && share::is_oracle_mode() && col->is_invisible_column()) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " INVISIBLE"))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print INVISIBLE", K(ret), K(*col));
          }
        }
        // mysql mode
        if (OB_SUCC(ret) && !is_oracle_mode && !col->is_generated_column()) {
          if (!col->is_nullable()) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " NOT NULL"))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print NOT NULL", K(ret));
            }
          } else if (ObTimestampType == col->get_data_type()) {
            // only timestamp need to print null;
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " NULL"))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print NULL", K(ret));
            }
          }
          // if column is not permit null and default value does not specify , don't  display DEFAULT NULL
          if (OB_SUCC(ret)) {
            if (!(ObNullType == col->get_cur_default_value().get_type() && !col->is_nullable()) &&
                !col->is_autoincrement()) {
              if (IS_DEFAULT_NOW_OBJ(col->get_cur_default_value())) {
                int16_t scale = col->get_data_scale();
                if (0 == scale) {
                  if (OB_FAIL(databuff_printf(buf, buf_len, pos, " DEFAULT %s", N_UPPERCASE_CUR_TIMESTAMP))) {
                    SHARE_SCHEMA_LOG(WARN, "fail to print DEFAULT now()", K(ret));
                  }
                } else {
                  if (OB_FAIL(
                          databuff_printf(buf, buf_len, pos, " DEFAULT %s(%d)", N_UPPERCASE_CUR_TIMESTAMP, scale))) {
                    SHARE_SCHEMA_LOG(WARN, "fail to print DEFAULT now()", K(ret));
                  }
                }
              } else {
                if (OB_FAIL(databuff_printf(buf, buf_len, pos, " DEFAULT "))) {
                  SHARE_SCHEMA_LOG(WARN, "fail to print DEFAULT", K(ret));
                } else {
                  ObObj default_value = col->get_cur_default_value();
                  default_value.set_scale(col->get_data_scale());
                  if (ob_is_enum_or_set_type(default_value.get_type())) {
                    if (OB_FAIL(
                            default_value.print_varchar_literal(col->get_extended_type_info(), buf, buf_len, pos))) {
                      SHARE_SCHEMA_LOG(WARN, "fail to print sql literal", KPC(col), K(buf), K(buf_len), K(pos), K(ret));
                    }
                  } else if (ob_is_string_tc(default_value.get_type())) {
                    if (OB_FAIL(databuff_printf(
                            buf, buf_len, pos, "'%s'", to_cstring(ObHexEscapeSqlStr(default_value.get_string()))))) {
                      SHARE_SCHEMA_LOG(WARN, "fail to print default value of string tc", K(ret));
                    }
                  } else if (OB_FAIL(default_value.print_varchar_literal(buf, buf_len, pos, tz_info))) {
                    SHARE_SCHEMA_LOG(WARN, "fail to print sql literal", K(ret));
                  }
                }
              }
            }
          }
          if (OB_SUCC(ret) && col->is_on_update_current_timestamp()) {
            int16_t scale = col->get_data_scale();
            if (0 == scale) {
              if (OB_FAIL(databuff_printf(buf, buf_len, pos, " %s", N_UPDATE_CURRENT_TIMESTAMP))) {
                SHARE_SCHEMA_LOG(WARN, "fail to print on update current_tiemstamp", K(ret));
              }
            } else {
              if (OB_FAIL(databuff_printf(buf, buf_len, pos, " %s(%d)", N_UPDATE_CURRENT_TIMESTAMP, scale))) {
                SHARE_SCHEMA_LOG(WARN, "fail to print on update current_tiemstamp", K(ret));
              }
            }
          }
          if (OB_SUCC(ret) && col->is_autoincrement()) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " AUTO_INCREMENT"))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print auto_increment", K(ret));
            }
          }
        }
        // oracle mode
        if (OB_SUCC(ret) && is_oracle_mode && !col->is_generated_column()) {
          // if column is not permit null and default value does not specify , don't  display DEFAULT NULL
          if (!(ObNullType == col->get_cur_default_value().get_type() && !col->is_nullable()) &&
              col->is_default_expr_v2_column()) {
            ObString default_value = col->get_cur_default_value().get_string();
            if (OB_FAIL(
                    databuff_printf(buf, buf_len, pos, " DEFAULT %.*s", default_value.length(), default_value.ptr()))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print sql literal", K(default_value), K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (!col->is_nullable()) {
              if (OB_FAIL(databuff_printf(buf, buf_len, pos, " NOT NULL"))) {
                SHARE_SCHEMA_LOG(WARN, "fail to print NOT NULL", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret) && col->is_generated_column()) {
          if (OB_FAIL(print_generated_column_definition(*col, buf, buf_len, table_schema, pos))) {
            LOG_WARN("print generated column definition failed", K(ret));
          }
        }
        if (OB_SUCC(ret) && !share::is_oracle_mode() && 0 < strlen(col->get_comment())) {
          if (OB_FAIL(databuff_printf(
                  buf, buf_len, pos, " COMMENT '%s'", to_cstring(ObHexEscapeSqlStr(col->get_comment_str()))))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print comment", K(ret));
          }
        }
        if (OB_SUCC(ret) && is_agent_mode) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " ID %lu", col->get_column_id()))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print column id", K(ret));
          }
        }
      }
      // TODO:add print extended_type_info
    }
  }
  if (ret != OB_ITER_END) {
    LOG_WARN("Failed to iterate all table columns. iter quit. ", K(ret));
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSchemaPrinter::print_generated_column_definition(
    const ObColumnSchemaV2& gen_col, char* buf, int64_t buf_len, const ObTableSchema& table_schema, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObString expr_str;
  ObArenaAllocator allocator("SchemaPrinter");
  sql::ObRawExprFactory expr_factory(allocator);
  sql::ObRawExpr* expr = NULL;
  ObTimeZoneInfo tz_infos;
  sql::ObRawExprPrinter raw_printer;
  raw_printer.init(buf, buf_len, &pos, &tz_infos);
  SMART_VAR(sql::ObSQLSessionInfo, session)
  {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " GENERATED ALWAYS AS ("))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print keywords", K(ret));
    } else if (OB_FAIL(gen_col.get_cur_default_value().get_string(expr_str))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print sql literal", K(ret));
    } else if (OB_FAIL(session.init(
                   0 /*default session version*/, 0 /*default session id*/, 0 /*default proxy id*/, &allocator))) {
      SHARE_SCHEMA_LOG(WARN, "fail to init session", K(ret));
    } else if (OB_FAIL(session.load_default_sys_variable(false, false))) {
      SHARE_SCHEMA_LOG(WARN, "session load default system variable failed", K(ret));
      /*
        build ObRawExpr obj,when expr_str = "CONCAT(first_name,' ',last_name)"
        avoid err print: CONCAT(first_name,\' \',last_name) */
    } else if (OB_FAIL(sql::ObRawExprUtils::build_generated_column_expr(
                   expr_str, expr_factory, session, table_schema, expr))) {
      SHARE_SCHEMA_LOG(WARN, "fail to generated column expr", K(ret));
    } else if (OB_FAIL(raw_printer.do_print(expr, sql::T_NONE_SCOPE))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print expr string", K(ret));
    } else if ((OB_FAIL(databuff_printf(buf, buf_len, pos, ")")))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print )", K(ret));
    } else if (gen_col.is_virtual_generated_column()) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " VIRTUAL"))) {
        SHARE_SCHEMA_LOG(WARN, "print virtual keyword failed", K(ret));
      }
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " STORED"))) {
      SHARE_SCHEMA_LOG(WARN, "print stored keyword failed", K(ret));
    }
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_indexes(
    const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos, bool is_unique_index) const
{
  int ret = OB_SUCCESS;
  ObStringBuf allocator;
  ObArenaAllocator arena_allocator(ObModIds::OB_SCHEMA);
  ObColumnSchemaV2 last_col;
  bool is_oralce_mode = share::is_oracle_mode();
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  if (OB_FAIL(table_schema.get_simple_index_infos_without_delay_deleted_tid(simple_index_infos))) {
    LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); i++) {
    const ObTableSchema* index_schema = NULL;
    if (OB_FAIL(schema_guard_.get_table_schema(simple_index_infos.at(i).table_id_, index_schema))) {
      LOG_WARN("fail to get table schema", K(ret));
    } else if (NULL == index_schema) {
      ret = OB_TABLE_NOT_EXIST;
      SHARE_SCHEMA_LOG(ERROR, "invalid index table id", "index_table_id", simple_index_infos.at(i).table_id_);
    } else if (index_schema->is_in_recyclebin()) {
      continue;
    } else if (OB_SUCC(ret)) {
      ObString index_name;
      ObString new_index_name;
      allocator.reuse();
      //  get the original short index name
      if (OB_FAIL(ObTableSchema::get_index_name(allocator,
              table_schema.get_table_id(),
              ObString::make_string(index_schema->get_table_name()),
              index_name))) {
        SHARE_SCHEMA_LOG(WARN, "get index table name failed");
      } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                     arena_allocator, index_name, new_index_name, share::is_oracle_mode()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(index_name));
      } else if ((is_unique_index && index_schema->is_unique_index()) ||
                 (!is_unique_index && !index_schema->is_unique_index())) {
        if (index_schema->is_unique_index()) {
          if (OB_FAIL(share::is_oracle_mode() ? databuff_printf(buf,
                                                    buf_len,
                                                    pos,
                                                    ",\n  CONSTRAINT \"%.*s\" UNIQUE ",
                                                    new_index_name.length(),
                                                    new_index_name.ptr())
                                              : databuff_printf(buf, buf_len, pos, ",\n  UNIQUE KEY "))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print UNIQUE KEY", K(ret));
          }
        } else if (index_schema->is_domain_index()) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", \n FULLTEXT KEY "))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print FULLTEXT KEY", K(ret));
          }
        } else {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n  KEY "))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print KEY", K(ret));
          }
        }

        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(share::is_oracle_mode()
                               ? databuff_printf(buf, buf_len, pos, "(")
                               : databuff_printf(
                                     buf, buf_len, pos, "`%.*s` (", new_index_name.length(), new_index_name.ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print index name", K(ret), K(index_name));
        } else {
          // index columns contain rowkeys of base table, but no need to show them.
          const int64_t index_column_num = index_schema->get_index_column_number();
          const ObRowkeyInfo& index_rowkey_info = index_schema->get_rowkey_info();
          int64_t rowkey_count = index_rowkey_info.get_size();
          last_col.reset();
          bool is_valid_col = false;
          ObArray<ObString> ctxcat_cols;
          for (int64_t k = 0; OB_SUCC(ret) && k < index_column_num; k++) {
            const ObRowkeyColumn* rowkey_column = index_rowkey_info.get_column(k);
            const ObColumnSchemaV2* col = NULL;
            if (NULL == rowkey_column) {
              ret = OB_SCHEMA_ERROR;
              SHARE_SCHEMA_LOG(WARN, "fail to get rowkey column", K(ret));
            } else if (NULL == (col = schema_guard_.get_column_schema(
                                    simple_index_infos.at(i).table_id_, rowkey_column->column_id_))) {
              ret = OB_SCHEMA_ERROR;
              SHARE_SCHEMA_LOG(WARN, "fail to get column schema", K(ret));
            } else if (!col->is_shadow_column()) {
              if (OB_SUCC(ret) && is_valid_col) {
                if (OB_FAIL(print_index_column(
                        table_schema, last_col, ctxcat_cols, false /*not last one*/, buf, buf_len, pos))) {
                  SHARE_SCHEMA_LOG(WARN, "fail to print index column", K(last_col), K(ret));
                }
              }
              // Generated column of index is convert to normal column,
              // we need to get column schema from data table here.
              if (OB_FAIL(ret)) {
              } else if (NULL == table_schema.get_column_schema(col->get_column_id())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get column schema from data table failed", K(ret));
              } else {
                last_col = *table_schema.get_column_schema(col->get_column_id());
                is_valid_col = true;
              }
            } else {
              is_valid_col = false;
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(
                    print_index_column(table_schema, last_col, ctxcat_cols, true /*last column*/, buf, buf_len, pos))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(last_col));
            } else if (OB_FAIL(print_table_definition_fulltext_indexs(ctxcat_cols, buf, buf_len, pos))) {
              LOG_WARN("print table definition fulltext indexs failed", K(ret));
            } else { /*do nothing*/
            }
          }

          // show storing columns in index
          if (OB_SUCC(ret)) {
            int64_t column_count = index_schema->get_column_count();
            if (column_count > rowkey_count) {
              bool first_storing_column = true;
              for (ObTableSchema::const_column_iterator row_col = index_schema->column_begin();
                   OB_SUCCESS == ret && NULL != row_col && row_col != index_schema->column_end();
                   row_col++) {
                int64_t k = 0;
                const ObRowkeyInfo& tab_rowkey_info = table_schema.get_rowkey_info();
                int64_t tab_rowkey_count = tab_rowkey_info.get_size();
                for (; k < tab_rowkey_count; k++) {
                  const ObRowkeyColumn* rowkey_column = tab_rowkey_info.get_column(k);
                  if (NULL != *row_col && rowkey_column->column_id_ == (*row_col)->get_column_id()) {
                    break;
                  }
                }
                if (k == tab_rowkey_count && NULL != *row_col && !(*row_col)->get_rowkey_position() &&
                    !(*row_col)->is_hidden()) {
                  if (first_storing_column) {
                    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " STORING ("))) {
                      SHARE_SCHEMA_LOG(WARN, "fail to print STORING(", K(ret));
                    }
                    first_storing_column = false;
                  }
                  if (OB_SUCC(ret)) {
                    if (OB_FAIL(databuff_printf(buf,
                            buf_len,
                            pos,
                            is_oralce_mode ? "\"%s\", " : "`%s`, ",
                            (*row_col)->get_column_name()))) {
                      SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K((*row_col)->get_column_name()));
                    }
                  }
                }
              }
              if (OB_SUCCESS == ret && !first_storing_column) {
                pos -= 2;  // fallback to the col name
                if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
                  SHARE_SCHEMA_LOG(WARN, "fail to print )", K(ret));
                }
              }
            }
          }  // end of storing columns

          // print index options
          if (OB_SUCC(ret)) {
            if (share::is_oracle_mode() && is_unique_index && index_schema->is_unique_index()) {
              // do nothing
            } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " "))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print space", K(ret));
            } else if (OB_FAIL(print_table_definition_table_options(*index_schema, buf, buf_len, pos, false))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print index options", K(ret), K(*index_schema));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObSchemaPrinter::print_table_definition_constraints(
    const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObTableSchema::const_constraint_iterator it_begin = table_schema.constraint_begin();
  ObTableSchema::const_constraint_iterator it_end = table_schema.constraint_end();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObString new_cst_name;

  for (ObTableSchema::const_constraint_iterator it = it_begin; OB_SUCC(ret) && it != it_end; it++) {
    const ObConstraint* cst = *it;
    if (NULL == cst) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(ERROR, "NULL ptr", K(cst));
    } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                   allocator, cst->get_constraint_name_str(), new_cst_name, share::is_oracle_mode()))) {
      SHARE_SCHEMA_LOG(
          WARN, "fail to generate new name with escape character", K(ret), K(cst->get_constraint_name_str()));
    } else if (share::is_mysql_mode()) {
      if (OB_FAIL(databuff_printf(buf,
              buf_len,
              pos,
              ",\n  CONSTRAINT `%.*s` CHECK (%.*s)",
              new_cst_name.length(),
              new_cst_name.ptr(),
              cst->get_check_expr_str().length(),
              cst->get_check_expr_str().ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print constraint", K(ret));
      }
    } else if (share::is_oracle_mode() && CONSTRAINT_TYPE_CHECK == cst->get_constraint_type()) {
      if (OB_FAIL(databuff_printf(buf,
              buf_len,
              pos,
              ",\n  CONSTRAINT \"%.*s\" CHECK (%.*s)",
              new_cst_name.length(),
              new_cst_name.ptr(),
              cst->get_check_expr_str().length(),
              cst->get_check_expr_str().ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print constraint", K(ret), K(*cst));
      }
      if (OB_SUCC(ret)) {
        if (true == cst->get_rely_flag()) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " RELY"))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print constraint rely", K(ret), K(*cst));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (false == cst->get_enable_flag()) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " DISABLE"))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print constraint disable", K(ret), K(*cst));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (false == cst->get_validate_flag()) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " NOVALIDATE"))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print constraint novalidate", K(ret), K(*cst));
          }
        } else if (false == cst->get_enable_flag() && true == cst->get_validate_flag()) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " VALIDATE"))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print constraint validate", K(ret), K(*cst));
          }
        }
      }
    }
  }

  return ret;
}

int ObSchemaPrinter::print_fulltext_index_column(const ObTableSchema& table_schema, const ObColumnSchemaV2& column,
    ObIArray<ObString>& ctxcat_cols, bool is_last, char* buf, int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> ctxcat_ids;
  bool is_oracle_mode = share::is_oracle_mode();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  const ObColumnSchemaV2* table_column = table_schema.get_column_schema(column.get_column_id());
  if (OB_ISNULL(table_column)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The column schema is NULL, ", K(ret));
  } else if (OB_FAIL(table_column->get_cascaded_column_ids(ctxcat_ids))) {
    STORAGE_LOG(WARN, "Failed to get cascaded column ids", K(ret));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j < ctxcat_ids.count(); ++j) {
      const ObColumnSchemaV2* ctxcat_column = NULL;
      ObString new_col_name;
      if (OB_ISNULL(ctxcat_column = table_schema.get_column_schema(ctxcat_ids.at(j)))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "The column schema is NULL, ", K(ret));
      } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                     allocator, ctxcat_column->get_column_name_str(), new_col_name, is_oracle_mode))) {
        SHARE_SCHEMA_LOG(
            WARN, "fail to generate new name with escape character", K(ret), K(ctxcat_column->get_column_name_str()));
      } else if (OB_FAIL(databuff_printf(buf,
                     buf_len,
                     pos,
                     is_last && j == ctxcat_ids.count() - 1 ? (is_oracle_mode ? "\"%s\")" : "`%s`)")
                                                            : (is_oracle_mode ? "\"%s\", " : "`%s`, "),
                     new_col_name.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(column));
      } else if (OB_FAIL(ctxcat_cols.push_back(ctxcat_column->get_column_name_str()))) {
        LOG_WARN("get fulltext index column failed", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_prefix_index_column(
    const ObColumnSchemaV2& column, bool is_last, char* buf, int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = share::is_oracle_mode();
  const ObString& expr_str = column.get_cur_default_value().is_null() ? column.get_orig_default_value().get_string()
                                                                      : column.get_cur_default_value().get_string();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  sql::ObRawExprFactory expr_factory(allocator);
  sql::ObRawExpr* expr = NULL;
  ObArray<sql::ObQualifiedName> columns;
  ObString column_name;
  sql::ObSQLSessionInfo default_session;  // session is mock,should use test_init
  int64_t const_value = 0;
  if (OB_FAIL(default_session.test_init(0, 0, 0, &allocator))) {
    LOG_WARN("init empty session failed", K(ret));
  } else if (OB_FAIL(default_session.load_default_sys_variable(false, false))) {
    LOG_WARN("session load default system variable failed", K(ret));
  } else if (OB_FAIL(sql::ObRawExprUtils::build_generated_column_expr(
                 expr_str, expr_factory, default_session, expr, columns))) {
    LOG_WARN("get generated column expr failed", K(ret));
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null");
  } else if (3 != expr->get_param_count()) {
    // Prefix index expression, there are three columns
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("It's wrong expr string", K(ret), K(expr->get_param_count()));
  } else if (1 != columns.count()) {
    // The expression column is based on a certain column
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("It's wrong expr string", K(ret), K(columns.count()));
  } else {
    column_name = columns.at(0).col_name_;
    ObString new_col_name;
    sql::ObRawExpr* t_expr0 = expr->get_param_expr(0);
    sql::ObRawExpr* t_expr1 = expr->get_param_expr(1);
    sql::ObRawExpr* t_expr2 = expr->get_param_expr(2);
    if (OB_ISNULL(t_expr0) || OB_ISNULL(t_expr1) || OB_ISNULL(t_expr2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (T_INT != t_expr2->get_expr_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr type is not int", K(ret));
    } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                   allocator, column_name, new_col_name, is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(column_name));
    } else {
      const_value = (static_cast<sql::ObConstRawExpr*>(t_expr2))->get_value().get_int();
      if (OB_FAIL(databuff_printf(buf,
              buf_len,
              pos,
              is_last ? (is_oracle_mode ? "\"%.*s\"(%ld))" : "`%.*s`(%ld))")
                      : (is_oracle_mode ? "\"%.*s\"(%ld), " : "`%.*s`(%ld), "),
              new_col_name.length(),
              new_col_name.ptr(),
              const_value))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_ordinary_index_column_expr(
    const ObColumnSchemaV2& column, bool is_last, char* buf, int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObString expr_def;
  if (OB_FAIL(column.get_cur_default_value().get_string(expr_def))) {
    LOG_WARN("get expr def from current default value failed", K(ret), K(column.get_cur_default_value()));
  } else if (OB_FAIL(
                 databuff_printf(buf, buf_len, pos, is_last ? "%.*s)" : "%.*s ", expr_def.length(), expr_def.ptr()))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print index column expr", K(ret), K(expr_def));
  }
  return ret;
}

int ObSchemaPrinter::print_index_column(const ObTableSchema& table_schema, const ObColumnSchemaV2& column,
    ObIArray<ObString>& ctxcat_cols, bool is_last, char* buf, int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = share::is_oracle_mode();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObString new_col_name;
  if (column.is_hidden() && column.is_generated_column()) {  // automatic generated column
    if (column.is_fulltext_column()) {
      if (OB_FAIL(print_fulltext_index_column(table_schema, column, ctxcat_cols, is_last, buf, buf_len, pos))) {
        LOG_WARN("print fulltext index column failed", K(ret));
      }
    } else if (column.is_prefix_column()) {
      if (OB_FAIL(print_prefix_index_column(column, is_last, buf, buf_len, pos))) {
        LOG_WARN("print prefix index column failed", K(ret));
      }
    } else if (OB_FAIL(print_ordinary_index_column_expr(column, is_last, buf, buf_len, pos))) {
      LOG_WARN("print ordinary index column expr failed", K(ret));
    }
  } else if (column.get_column_id() == OB_HIDDEN_SESSION_ID_COLUMN_ID) {
    // OB_HIDDEN_SESSION_ID_COLUMN_ID in oracle temporary table never is the last
    // column in the index, no need to do any thing
  } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                 allocator, column.get_column_name_str(), new_col_name, is_oracle_mode))) {
    SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(column.get_column_name_str()));
  } else if (OB_FAIL(databuff_printf(buf,
                 buf_len,
                 pos,
                 is_last ? (is_oracle_mode ? "\"%s\")" : "`%s`)") : (is_oracle_mode ? "\"%s\", " : "`%s`, "),
                 new_col_name.ptr()))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(column));
  } else { /*do nothing*/
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_fulltext_indexs(
    const ObIArray<ObString>& fulltext_indexs, char* buf, int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObString new_col_name;
  if (fulltext_indexs.count() > 0) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " CTXCAT("))) {
      LOG_WARN("failed to print CTXCAT keyword", K(ret));
    }
  }
  bool is_oracle_mode = share::is_oracle_mode();
  for (int64_t i = 0; OB_SUCC(ret) && i < fulltext_indexs.count() - 1; ++i) {
    const ObString& ft_name = fulltext_indexs.at(i);
    if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
            allocator, ft_name, new_col_name, is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(ft_name));
    } else if (OB_FAIL(databuff_printf(buf,
                   buf_len,
                   pos,
                   is_oracle_mode ? "\"%.*s\", " : "`%.*s`, ",
                   new_col_name.length(),
                   new_col_name.ptr()))) {
      LOG_WARN("print fulltext column name failed", K(ret), K(ft_name));
    }
  }
  if (OB_SUCC(ret) && fulltext_indexs.count() > 0) {
    const ObString& ft_name = fulltext_indexs.at(fulltext_indexs.count() - 1);
    if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
            allocator, ft_name, new_col_name, is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(ft_name));
    } else if (OB_FAIL(databuff_printf(buf,
                   buf_len,
                   pos,
                   is_oracle_mode ? "\"%.*s\")" : "`%.*s`)",
                   new_col_name.length(),
                   new_col_name.ptr()))) {
      LOG_WARN("print fulltext column name failed", K(ret), K(ft_name));
    }
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_rowkeys(
    const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo& rowkey_info = table_schema.get_rowkey_info();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  if (!table_schema.is_no_pk_table() && rowkey_info.get_size() > 0) {
    bool has_pk_constraint_name = false;
    bool is_oracle_mode = share::is_oracle_mode();
    if (is_oracle_mode) {
      ObTableSchema::const_constraint_iterator iter = table_schema.constraint_begin();
      for (; OB_SUCC(ret) && iter != table_schema.constraint_end(); ++iter) {
        ObString new_cst_name;
        if (CONSTRAINT_TYPE_PRIMARY_KEY == (*iter)->get_constraint_type()) {
          if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                  allocator, (*iter)->get_constraint_name_str(), new_cst_name, is_oracle_mode))) {
            SHARE_SCHEMA_LOG(
                WARN, "fail to generate new name with escape character", K(ret), K((*iter)->get_constraint_name_str()));
          } else if (OB_FAIL(databuff_printf(
                         buf, buf_len, pos, ",\n  CONSTRAINT \"%s\" PRIMARY KEY (", new_cst_name.ptr()))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print CONSTRAINT cons_name PRIMARY KEY(", K(ret));
          }
          has_pk_constraint_name = true;
          break;  // A table can only have one primary key constraint
        }
      }
    }
    if (OB_SUCC(ret) && !has_pk_constraint_name) {
      // When printing pk constraints in the following two cases, the pk name is not printed
      // 1. mysql mode has no primary key name
      // 2. Oracle mode 2.1.0 contains tables created by the 2.1.0 server, and does not support primary key names
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n  PRIMARY KEY ("))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print PRIMARY KEY(", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(print_rowkey_info(rowkey_info, table_schema.get_table_id(), buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print rowkey info", K(ret));
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print )", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (!share::is_oracle_mode() && table_schema.get_pk_comment_str().length() > 0) {
        if (OB_FAIL(databuff_printf(buf,
                buf_len,
                pos,
                " COMMENT '%s'",
                to_cstring(ObHexEscapeSqlStr(table_schema.get_pk_comment_str()))))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print primary key comment", K(ret), K(table_schema));
        }
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_rowkey_info(
    const ObRowkeyInfo& rowkey_info, uint64_t table_id, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("PrintRowkeyInfo");
  bool is_first_col = true;
  bool is_oracle_mode = share::is_oracle_mode();
  for (int64_t j = 0; OB_SUCC(ret) && j < rowkey_info.get_size(); j++) {
    const ObColumnSchemaV2* col = NULL;
    ObString new_col_name;
    if (NULL == (col = schema_guard_.get_column_schema(table_id, rowkey_info.get_column(j)->column_id_))) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      SHARE_SCHEMA_LOG(WARN, "fail to get column", "column_id", rowkey_info.get_column(j)->column_id_);
    } else if (col->get_column_id() == OB_HIDDEN_SESSION_ID_COLUMN_ID) {
      // do nothing
    } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                   allocator, col->get_column_name_str(), new_col_name, is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(col->get_column_name_str()));
    } else if (!col->is_shadow_column()) {
      if (true == is_first_col) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, is_oracle_mode ? "\"%s\"" : "`%s`", new_col_name.ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(col->get_column_name()));
        } else {
          is_first_col = false;
        }
      } else {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, is_oracle_mode ? ", \"%s\"" : ", `%s`", new_col_name.ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(col->get_column_name()));
        }
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_foreign_keys(
    const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  // The foreign key information should be attached to the sub-table
  if (table_schema.is_child_table()) {
    const ObIArray<ObForeignKeyInfo>& foreign_key_infos = table_schema.get_foreign_key_infos();
    bool is_oracle_mode = share::is_oracle_mode();
    FOREACH_CNT_X(foreign_key_info, foreign_key_infos, OB_SUCC(ret))
    {
      uint64_t parent_table_id = foreign_key_info->parent_table_id_;
      const ObTableSchema* parent_table_schema = NULL;
      const ObDatabaseSchema* parent_db_schema = NULL;
      const char* update_action_str = NULL;
      const char* delete_action_str = NULL;
      ObString new_fk_name;
      ObString new_parent_db_name;
      ObString new_parent_table_name;
      // Only print the foreign key information created by the child table, not the information as the parent table
      if (foreign_key_info->child_table_id_ == table_schema.get_table_id()) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n  CONSTRAINT "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print CONSTRAINT", K(ret));
        } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                       allocator, foreign_key_info->foreign_key_name_, new_fk_name, is_oracle_mode))) {
          SHARE_SCHEMA_LOG(
              WARN, "fail to generate new name with escape character", K(ret), K(foreign_key_info->foreign_key_name_));
        } else if (!foreign_key_info->foreign_key_name_.empty() && OB_FAIL(databuff_printf(buf,
                                                                       buf_len,
                                                                       pos,
                                                                       is_oracle_mode ? "\"%.*s\" " : "`%.*s` ",
                                                                       new_fk_name.length(),
                                                                       new_fk_name.ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print foreign key name", K(ret));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "FOREIGN KEY ("))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print FOREIGN KEY(", K(ret));
        } else if (OB_FAIL(print_column_list(table_schema, foreign_key_info->child_column_ids_, buf, buf_len, pos))) {
          LOG_WARN("fail to print_column_list", K(ret), K(table_schema.get_table_name_str()));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ") REFERENCES "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print ) REFERENCES ", K(ret));
        } else if (OB_FAIL(schema_guard_.get_table_schema(parent_table_id, parent_table_schema))) {
          LOG_WARN("fail to get table schema", K(ret), K(parent_table_id));
        } else if (NULL == parent_table_schema) {
          ret = OB_TABLE_NOT_EXIST;
          SHARE_SCHEMA_LOG(WARN, "unknown table", K(ret), K(parent_table_id));
        } else if (OB_FAIL(
                       schema_guard_.get_database_schema(parent_table_schema->get_database_id(), parent_db_schema))) {
          SHARE_SCHEMA_LOG(WARN, "failed to get database", K(ret), K(parent_table_schema->get_database_id()));
        } else if (NULL == parent_db_schema) {
          ret = OB_ERR_BAD_DATABASE;
          SHARE_SCHEMA_LOG(WARN, "unknown database", K(ret), K(parent_table_schema->get_database_id()));
        } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                       allocator, parent_db_schema->get_database_name_str(), new_parent_db_name, is_oracle_mode))) {
          SHARE_SCHEMA_LOG(
              WARN, "fail to generate new name with escape character", K(ret), K(foreign_key_info->foreign_key_name_));
        } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                       allocator, parent_table_schema->get_table_name_str(), new_parent_table_name, is_oracle_mode))) {
          SHARE_SCHEMA_LOG(
              WARN, "fail to generate new name with escape character", K(ret), K(foreign_key_info->foreign_key_name_));
        } else if (OB_FAIL(databuff_printf(buf,
                       buf_len,
                       pos,
                       is_oracle_mode ? "\"%s\".\"%s\"(" : "`%s`.`%s`(",
                       new_parent_db_name.ptr(),
                       new_parent_table_name.ptr()))) {
          SHARE_SCHEMA_LOG(WARN,
              "fail to print database and table name",
              K(ret),
              K(parent_db_schema->get_database_name_str()),
              K(parent_table_schema->get_table_name_str()));
        } else if (OB_FAIL(print_column_list(
                       *parent_table_schema, foreign_key_info->parent_column_ids_, buf, buf_len, pos))) {
          LOG_WARN("fail to print_column_list", K(ret), K(parent_table_schema->get_table_name_str()));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ") "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print ) ", K(ret));
        } else if (OB_ISNULL(update_action_str = foreign_key_info->get_update_action_str()) ||
                   OB_ISNULL(delete_action_str = foreign_key_info->get_delete_action_str())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("reference action is invalid",
              K(ret),
              K(foreign_key_info->update_action_),
              K(foreign_key_info->delete_action_));
        } else {
          if (is_oracle_mode) {
            if (foreign_key_info->delete_action_ == ACTION_CASCADE ||
                foreign_key_info->delete_action_ == ACTION_SET_NULL) {
              if (OB_FAIL(databuff_printf(buf, buf_len, pos, "ON DELETE %s ", delete_action_str))) {
                LOG_WARN("fail to print delete action", K(ret), K(delete_action_str));
              }
            }
          } else if (!is_oracle_mode) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, "ON UPDATE %s ", update_action_str))) {
              LOG_WARN("fail to print update action", K(ret), K(update_action_str));
            } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "ON DELETE %s ", delete_action_str))) {
              LOG_WARN("fail to print delete action", K(ret), K(delete_action_str));
            }
          }
          if (OB_SUCC(ret) && is_oracle_mode && foreign_key_info->rely_flag_) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " RELY"))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print foreign_key rely state", K(ret), K(*foreign_key_info));
            }
          }
          if (OB_SUCC(ret) && is_oracle_mode && !foreign_key_info->enable_flag_) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " DISABLE"))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print foreign_key disable state", K(ret), K(*foreign_key_info));
            }
          }
          if (OB_SUCC(ret) && is_oracle_mode && !foreign_key_info->validate_flag_) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " NOVALIDATE"))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print foreign_key novalidate state", K(ret), K(*foreign_key_info));
            }
          } else if (OB_SUCC(ret) && is_oracle_mode && !foreign_key_info->enable_flag_ &&
                     foreign_key_info->validate_flag_) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " VALIDATE"))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print foreign_key validate state", K(ret), K(*foreign_key_info));
            }
          }
        }
      }
    }
  } else {
    // table is not a child table, do nothing
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_store_format(
    const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObStoreFormatType store_format = table_schema.get_store_format();
  const char* store_format_name = ObStoreFormat::get_store_format_print_str(store_format);
  bool is_oracle_mode = share::is_oracle_mode();

  if (store_format == OB_STORE_FORMAT_INVALID) {
    // invalid store format , just skip print the store format, possible upgrade
    SQL_RESV_LOG(WARN, "Unexpected invalid table store format option", K(store_format), K(ret));
  } else if (OB_ISNULL(store_format_name)) {
    ret = OB_NOT_SUPPORTED;
    SQL_RESV_LOG(WARN, "Not supported store format in oracle mode", K(store_format), K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s ", store_format_name))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print store format", K(ret), K(table_schema), K(store_format));
  }
  if (OB_SUCC(ret) && !is_oracle_mode) {
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            "COMPRESSION = '%s' ",
            table_schema.is_compressed() ? table_schema.get_compress_func_name() : "none"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print compress method", K(ret), K(table_schema));
    }
  }

  return ret;
}

int ObSchemaPrinter::print_column_list(const ObTableSchema& table_schema, const ObIArray<uint64_t>& column_ids,
    char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  bool is_first_col = true;
  bool is_oracle_mode = share::is_oracle_mode();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
    const ObColumnSchemaV2* col = table_schema.get_column_schema(column_ids.at(i));
    ObString new_col_name;
    if (OB_ISNULL(col)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "The column schema is NULL, ", K(ret));
    } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                   allocator, col->get_column_name_str(), new_col_name, is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(col->get_column_name_str()));
    } else if (true == is_first_col) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, is_oracle_mode ? "\"%s\"" : "`%s`", new_col_name.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print child column name", K(ret), K(col->get_column_name_str()));
      } else {
        is_first_col = false;
      }
    } else {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, is_oracle_mode ? ", \"%s\"" : ", `%s`", new_col_name.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(col->get_column_name_str()));
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_comment_oracle(
    const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (!share::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
  } else if (table_schema.get_comment_str().empty()) {
    // do nothing
  } else {
    ret = databuff_printf(buf,
        buf_len,
        pos,
        ";\nCOMMENT ON TABLE \"%s\" is '%s'",
        table_schema.get_table_name(),
        to_cstring(ObHexEscapeSqlStr(table_schema.get_comment_str())));
  }

  ObColumnIterByPrevNextID iter(table_schema);
  const ObColumnSchemaV2* col = NULL;
  while (OB_SUCC(ret) && OB_SUCC(iter.next(col))) {
    if (OB_ISNULL(col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The column is null", K(ret));
    } else if (col->is_shadow_column() || col->is_hidden() || col->get_comment_str().empty()) {
      // do nothing
    } else {
      ret = databuff_printf(buf,
          buf_len,
          pos,
          ";\nCOMMENT ON COLUMN \"%s\".\"%s\" is '%s'",
          table_schema.get_table_name(),
          col->get_column_name(),
          col->get_comment());
    }
  }
  if (ret != OB_ITER_END) {
    LOG_WARN("Failed to iterate all table columns. iter quit. ", K(ret));
  } else {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObSchemaPrinter::print_table_definition_table_options(const ObTableSchema& table_schema, char* buf,
    const int64_t& buf_len, int64_t& pos, bool is_for_table_status, bool agent_mode) const
{
  const bool is_index_tbl = table_schema.is_index_table();
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret && !is_index_tbl && !is_for_table_status) {
    uint64_t auto_increment = 0;
    if (OB_FAIL(share::ObAutoincrementService::get_instance().get_sequence_value(table_schema.get_tenant_id(),
            table_schema.get_table_id(),
            table_schema.get_autoinc_column_id(),
            auto_increment))) {
      SHARE_SCHEMA_LOG(WARN, "fail to get auto_increment value", K(ret));
    } else if (auto_increment > 0) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "AUTO_INCREMENT = %lu ", auto_increment))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print auto increment", K(ret), K(auto_increment), K(table_schema));
      }
    }
  }

  if (OB_SUCCESS == ret && !is_for_table_status && !is_index_tbl &&
      CHARSET_INVALID != table_schema.get_charset_type()) {
    if (share::is_oracle_mode()) {
      // do not print charset info when in oracle mode
    } else if (OB_FAIL(databuff_printf(buf,
                   buf_len,
                   pos,
                   "DEFAULT CHARSET = %s ",
                   ObCharset::charset_name(table_schema.get_charset_type())))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print default charset", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !share::is_oracle_mode() && !agent_mode && !is_for_table_status && !is_index_tbl &&
      CS_TYPE_INVALID != table_schema.get_collation_type() &&
      !ObCharset::is_default_collation(table_schema.get_charset_type(), table_schema.get_collation_type())) {
    if (OB_FAIL(databuff_printf(
            buf, buf_len, pos, "COLLATE = %s ", ObCharset::collation_name(table_schema.get_collation_type())))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print collate", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && table_schema.is_domain_index() && !table_schema.get_parser_name_str().empty()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "WITH PARSER '%s' ", table_schema.get_parser_name()))) {
      SHARE_SCHEMA_LOG(WARN, "print parser name failed", K(ret));
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl) {
    if (OB_FAIL(print_table_definition_store_format(table_schema, buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print store format", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl && table_schema.get_expire_info().length() > 0 &&
      NULL != table_schema.get_expire_info().ptr()) {
    const ObString expire_str = table_schema.get_expire_info();
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "EXPIRE_INFO = (%.*s) ", expire_str.length(), expire_str.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print expire info", K(ret), K(expire_str));
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl) {
    int64_t paxos_replica_num = OB_INVALID_COUNT;
    if (OB_FAIL(table_schema.get_paxos_replica_num(schema_guard_, paxos_replica_num))) {
      LOG_WARN("fail to get paxos replica num", K(ret));
    } else if (OB_UNLIKELY(paxos_replica_num < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("paxos replica num error", K(ret), K(paxos_replica_num), "table_id", table_schema.get_table_id());
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "REPLICA_NUM = %ld ", paxos_replica_num))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print replica num", K(ret), K(table_schema));
    } else {
      SHARE_SCHEMA_LOG(INFO, "XXX", K(paxos_replica_num));
    }  // no more to do
  }
  if (OB_SUCCESS == ret && table_schema.get_locality_str().length() > 0) {  // locality
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            "LOCALITY = \'%.*s\' ",
            table_schema.get_locality_str().length(),
            table_schema.get_locality_str().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print locality", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && 0 < table_schema.get_primary_zone().length()) {  // primary_zone
    bool is_random = (0 == table_schema.get_primary_zone().compare(common::OB_RANDOM_PRIMARY_ZONE));
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            is_random ? "PRIMARY_ZONE = %.*s " : "PRIMARY_ZONE = \'%.*s\' ",
            table_schema.get_primary_zone().length(),
            table_schema.get_primary_zone().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print primary_zone", K(ret), K(table_schema.get_primary_zone()));
    }
  }
  if (OB_SUCCESS == ret && table_schema.get_block_size() >= 0) {
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            is_index_tbl ? "BLOCK_SIZE %ld " : "BLOCK_SIZE = %ld ",
            table_schema.get_block_size()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print block size", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && is_index_tbl && !table_schema.is_domain_index()) {
    const char* local_flag =
        table_schema.is_global_index_table() || table_schema.is_global_local_index_table() ? "GLOBAL " : "LOCAL ";
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", local_flag))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print global/local", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl) {
    if (OB_FAIL(databuff_printf(
            buf, buf_len, pos, "USE_BLOOM_FILTER = %s ", table_schema.is_use_bloomfilter() ? "TRUE" : "FALSE"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print use bloom filter", K(ret), K(table_schema));
    }
  }

  if (OB_SUCCESS == ret && !is_index_tbl && table_schema.is_enable_row_movement()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "ENABLE ROW MOVEMENT "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print row movement option", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TABLET_SIZE = %ld ", table_schema.get_tablet_size()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print tablet_size", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "PCTFREE = %ld ", table_schema.get_pctfree()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print pctfree", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl && table_schema.get_dop() > 1) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "PARALLEL %ld ", table_schema.get_dop()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print dop", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl && common::OB_INVALID_ID != table_schema.get_tablegroup_id()) {
    const ObTablegroupSchema* tablegroup_schema = schema_guard_.get_tablegroup_schema(table_schema.get_tablegroup_id());
    if (NULL != tablegroup_schema) {
      const ObString tablegroup_name = tablegroup_schema->get_tablegroup_name();
      if (tablegroup_name.length() > 0 && NULL != tablegroup_name.ptr()) {
        if (OB_FAIL(databuff_printf(
                buf, buf_len, pos, "TABLEGROUP = '%.*s' ", tablegroup_name.length(), tablegroup_name.ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print tablegroup", K(ret), K(tablegroup_name));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "tablegroup name is null");
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "tablegroup schema is null");
    }
  }

  if (OB_SUCCESS == ret && !is_index_tbl && table_schema.get_progressive_merge_num() > 0) {
    if (OB_FAIL(databuff_printf(
            buf, buf_len, pos, "PROGRESSIVE_MERGE_NUM = %ld ", table_schema.get_progressive_merge_num()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print progressive merge num", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !share::is_oracle_mode() && !is_for_table_status &&
      table_schema.get_comment_str().length() > 0) {
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            is_index_tbl ? "COMMENT '%s' " : "COMMENT = '%s' ",
            to_cstring(ObHexEscapeSqlStr(table_schema.get_comment_str()))))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print comment", K(ret), K(table_schema));
    }
  }

  if (agent_mode) {
    if (OB_SUCCESS == ret && table_schema.is_partitioned_table() && table_schema.has_partition()) {
      int64_t max_used_part_id = table_schema.get_part_option().get_max_used_part_id();
      if (-1 == max_used_part_id) {
        max_used_part_id = table_schema.get_part_option().get_part_num() - 1;
      }
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "MAX_USED_PART_ID = %ld ", max_used_part_id))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print table max_used_part_id", K(ret), K(max_used_part_id));
      }
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl && table_schema.is_read_only()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "READ ONLY "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print table read only", K(ret));
    }
  }
  ObString table_mode_str = "";
  if (OB_SUCC(ret) && !is_index_tbl) {
    if (!agent_mode) {
    } else {  // true == agent_mode
      table_mode_str = ObBackUpTableModeOp::get_table_mode_str(table_schema.get_table_mode_struct());
    }
  }
  if (OB_SUCC(ret) && table_mode_str.length() > 0) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TABLE_MODE = '%s' ", table_mode_str.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print table table_mode", K(ret));
    }
  }
  if (OB_SUCC(ret) && agent_mode) {
    if (OB_FAIL(!is_index_tbl ? databuff_printf(buf, buf_len, pos, "TABLE_ID = %lu ", table_schema.get_table_id())
                              : databuff_printf(buf,
                                    buf_len,
                                    pos,
                                    "INDEX_TABLE_ID = %lu DATA_TABLE_ID = %lu",
                                    table_schema.get_table_id(),
                                    table_schema.get_data_table_id()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print progressive merge num", K(ret), K(table_schema));
    } else if (table_schema.is_partitioned_table() && table_schema.has_partition() &&
               USER_TABLE == table_schema.get_table_type()) {
      // Currently only the user table allows partition management operations,
      // which will cause partition_id not to start from 0 and need to be backed up to max_used_part_id.
      // The following two scenarios need to wait for physical backup and restore or add syntax support
      // 1. After 2.2, the partition management operation of hash like is supported,
      // which will cause the partition_id to be discontinuous.
      // 2. Subsequent support for partition management operations of global indexes may result
      // in partition_id not starting from 0,or partition_id is not continuous.
      int64_t max_used_part_id = table_schema.get_part_option().get_max_used_part_id();
      if (-1 == max_used_part_id) {
        max_used_part_id = table_schema.get_part_option().get_part_num() - 1;
      }
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " MAX_USED_PART_ID = %ld ", max_used_part_id))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print table max_used_part_id", K(ret), K(max_used_part_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    pos -= 1;
    buf[pos] = '\0';  // remove trailer space
  }
  return ret;
}

static int print_partition_func(
    const ObTableSchema& table_schema, ObSqlString& disp_part_str, bool include_func, bool is_subpart)
{
  int ret = OB_SUCCESS;
  const ObPartitionOption& part_opt = table_schema.get_part_option();
  ObString type_str;
  ObPartitionFuncType type = part_opt.get_part_func_type();
  const ObString& func_expr = part_opt.get_part_func_expr_str();
  // replace `key_v2' with `key'
  if (PARTITION_FUNC_TYPE_KEY_V2 == type || PARTITION_FUNC_TYPE_KEY_V3 == type) {
    type = PARTITION_FUNC_TYPE_KEY;
  }
  // replace 'hash_v2' with 'hash'
  if (PARTITION_FUNC_TYPE_HASH_V2 == type) {
    type = PARTITION_FUNC_TYPE_HASH;
  }

  if (include_func) {
    if (OB_FAIL(disp_part_str.append_fmt("partition by %.*s", func_expr.length(), func_expr.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "Failed to append disp part str", K(ret));
    }
  } else if (OB_FAIL(get_part_type_str(type, type_str))) {
    SHARE_SCHEMA_LOG(WARN, "failed to get part type string", K(ret));
  } else if (OB_FAIL(disp_part_str.append_fmt(
                 "partition by %.*s(%.*s)", type_str.length(), type_str.ptr(), func_expr.length(), func_expr.ptr()))) {
    SHARE_SCHEMA_LOG(WARN, "fail to append diaplay partition expr", K(ret), K(type_str), K(func_expr));
  } else if (is_subpart) {  // sub part
    const ObPartitionOption& sub_part_opt = table_schema.get_sub_part_option();
    ObString sub_type_str;
    ObPartitionFuncType sub_type = sub_part_opt.get_part_func_type();
    // replace `key_v2',`key_v3' with `key'
    if (PARTITION_FUNC_TYPE_KEY_V2 == sub_type || PARTITION_FUNC_TYPE_KEY_V3 == sub_type) {
      sub_type = PARTITION_FUNC_TYPE_KEY;
    }
    // replace 'hash_v2' with 'hash'
    if (PARTITION_FUNC_TYPE_HASH_V2 == sub_type) {
      sub_type = PARTITION_FUNC_TYPE_HASH;
    }
    const ObString& sub_func_expr = sub_part_opt.get_part_func_expr_str();
    if (OB_FAIL(get_part_type_str(sub_type, sub_type_str))) {
      SHARE_SCHEMA_LOG(WARN, "failed to get part type string", K(ret));
    } else if (OB_FAIL(disp_part_str.append_fmt(" subpartition by %.*s(%.*s)",
                   sub_type_str.length(),
                   sub_type_str.ptr(),
                   sub_func_expr.length(),
                   sub_func_expr.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to append diaplay partition expr", K(ret), K(sub_type_str), K(sub_func_expr));
    }
  } else {
  }

  return ret;
}

static int print_tablegroup_partition_func(
    const ObTablegroupSchema& tablegroup_schema, ObSqlString& disp_part_str, bool is_subpart)
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  const ObPartitionOption& part_opt = tablegroup_schema.get_part_option();
  ObPartitionFuncType type = part_opt.get_part_func_type();
  const int64_t part_func_expr_num = tablegroup_schema.get_part_func_expr_num();
  ObString type_str;
  const bool can_change = false;
  // replace `key_v2,key_v3' with `key'
  if (PARTITION_FUNC_TYPE_KEY_V2 == type || PARTITION_FUNC_TYPE_KEY_V3 == type) {
    type = PARTITION_FUNC_TYPE_KEY;
  }
  // replace 'hash_v2' with 'hash'
  if (PARTITION_FUNC_TYPE_HASH_V2 == type) {
    type = PARTITION_FUNC_TYPE_HASH;
  }
  if (tablegroup_id <= 0 || part_func_expr_num <= 0 || type < PARTITION_FUNC_TYPE_HASH ||
      type >= PARTITION_FUNC_TYPE_MAX) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "invalid argument", K(ret), K(tablegroup_id), K(type), K(part_func_expr_num));
  } else if (is_new_tablegroup_id(tablegroup_id)) {
    if (OB_FAIL(get_part_type_str(type, type_str, can_change))) {
      SHARE_SCHEMA_LOG(WARN, "failed to get part type string", K(ret));
    } else if (OB_FAIL(disp_part_str.append_fmt(" partition by %.*s", type_str.length(), type_str.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to append disp part str", K(ret));
    } else if (is_key_part(type) || PARTITION_FUNC_TYPE_RANGE_COLUMNS == type ||
               PARTITION_FUNC_TYPE_LIST_COLUMNS == type) {
      if (OB_FAIL(disp_part_str.append_fmt(" %ld", part_func_expr_num))) {
        SHARE_SCHEMA_LOG(WARN, "fail to append disp part str", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_subpart) {
      const ObPartitionOption& sub_part_opt = tablegroup_schema.get_sub_part_option();
      ObPartitionFuncType sub_type = sub_part_opt.get_part_func_type();
      const int64_t sub_part_func_expr_num = tablegroup_schema.get_sub_part_func_expr_num();
      ObString sub_type_str;
      // replace `key_v2',`key_v3' with `key'
      if (PARTITION_FUNC_TYPE_KEY_V2 == sub_type || PARTITION_FUNC_TYPE_KEY_V3 == sub_type) {
        sub_type = PARTITION_FUNC_TYPE_KEY;
      }
      // replace `hash_v2' with `key'
      if (PARTITION_FUNC_TYPE_HASH_V2 == sub_type) {
        sub_type = PARTITION_FUNC_TYPE_HASH;
      }
      if (sub_part_func_expr_num <= 0 || sub_type < PARTITION_FUNC_TYPE_HASH || sub_type >= PARTITION_FUNC_TYPE_MAX) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_SCHEMA_LOG(WARN, "invalid argument", K(ret), K(tablegroup_id), K(sub_type), K(sub_part_func_expr_num));
      } else if (OB_FAIL(get_part_type_str(sub_type, sub_type_str, can_change))) {
        SHARE_SCHEMA_LOG(WARN, "failed to get sub_part type string", K(ret));
      } else if (OB_FAIL(
                     disp_part_str.append_fmt(" subpartition by %.*s", sub_type_str.length(), sub_type_str.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to append disp part str", K(ret));
      } else if (is_key_part(sub_type) || PARTITION_FUNC_TYPE_RANGE_COLUMNS == sub_type ||
                 PARTITION_FUNC_TYPE_LIST_COLUMNS == sub_type) {
        if (OB_FAIL(disp_part_str.append_fmt(" %ld", sub_part_func_expr_num))) {
          SHARE_SCHEMA_LOG(WARN, "fail to append disp part str", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObSchemaPrinter::print_table_definition_partition_options(const ObTableSchema& table_schema, char* buf,
    const int64_t& buf_len, int64_t& pos, bool agent_mode, const ObTimeZoneInfo* tz_info) const
{
  int ret = OB_SUCCESS;
  if (table_schema.is_partitioned_table() && !table_schema.is_oracle_tmp_table()) {
    ObString disp_part_fun_expr_str;
    ObSqlString disp_part_str;
    bool is_subpart = false;
    const ObPartitionSchema* partition_schema = &table_schema;
    if (PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
      is_subpart = true;
    }
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print enter", K(ret));
    } else if (OB_FAIL(print_partition_func(
                   table_schema, disp_part_str, is_inner_table(table_schema.get_table_id()), is_subpart))) {
      SHARE_SCHEMA_LOG(WARN, "failed to print part func", K(ret));
    } else if (FALSE_IT(disp_part_fun_expr_str = disp_part_str.string())) {
      // will not reach here
    } else if (OB_FAIL(databuff_printf(
                   buf, buf_len, pos, " %.*s", disp_part_fun_expr_str.length(), disp_part_fun_expr_str.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to printf partition expr", K(ret), K(disp_part_fun_expr_str));
    } else if (is_subpart && partition_schema->is_sub_part_template()) {
      if (OB_FAIL(print_template_sub_partition_elements(partition_schema, buf, buf_len, pos, tz_info, false))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print sub partition elements", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (table_schema.is_range_part()) {
        if (OB_FAIL(print_range_partition_elements(partition_schema, buf, buf_len, pos, agent_mode, false, tz_info))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print partition elements", K(ret));
        }
      } else if (table_schema.is_list_part()) {
        if (OB_FAIL(print_list_partition_elements(partition_schema, buf, buf_len, pos, agent_mode, false, tz_info))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print partition elements", K(ret));
        }
      } else if (is_hash_like_part(table_schema.get_part_option().get_part_func_type())) {
        if (OB_FAIL(print_hash_partition_elements(partition_schema, buf, buf_len, pos, agent_mode, tz_info))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print partition elements", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_on_commit_options(
    const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (table_schema.is_oracle_sess_tmp_table()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " ON COMMIT PRESERVE ROWS"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to printf on commit option", K(ret));
    }
  } else if (table_schema.is_oracle_trx_tmp_table()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " ON COMMIT DELETE ROWS"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to printf on commit option", K(ret));
    }
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_fulltext_indexs(const ObIArray<ObString>& fulltext_indexs,
    const uint64_t virtual_column_id, char* buf, int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(print_table_definition_fulltext_indexs(fulltext_indexs, buf, buf_len, pos))) {
    OB_LOG(WARN, "fail to print column definition", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " virtual_column_id = %lu ", virtual_column_id))) {
    OB_LOG(WARN, "fail to print virtual column id", K(ret));
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_table_options(const ObTableSchema& table_schema,
    const ObIArray<ObString>& full_text_columns, const uint64_t virtual_column_id, char* buf, const int64_t buf_len,
    int64_t& pos, bool is_for_table_status, common::ObMySQLProxy* sql_proxy, bool is_agent_mode) const
{
  const bool is_index_tbl = table_schema.is_index_table();
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret));
  }

  if (OB_SUCCESS == ret && !is_index_tbl && !is_for_table_status && is_agent_mode) {
    uint64_t auto_increment = 0;
    if (OB_ISNULL(sql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "sql_proxy is null", K(ret));
    } else if (OB_FAIL(ObSchemaPrinter::get_sequence_value(*sql_proxy,
                   table_schema.get_tenant_id(),
                   table_schema.get_table_id(),
                   table_schema.get_autoinc_column_id(),
                   auto_increment))) {
      OB_LOG(WARN, "fail to get auto_increment value", K(ret));
    } else if (auto_increment > 0) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "AUTO_INCREMENT = %lu ", auto_increment))) {
        OB_LOG(WARN, "fail to print auto increment", K(ret), K(auto_increment), K(table_schema));
      }
    }
  }

  if (OB_SUCC(ret) && !is_for_table_status && !is_index_tbl && CHARSET_INVALID != table_schema.get_charset_type()) {
    if (OB_FAIL(databuff_printf(
            buf, buf_len, pos, "DEFAULT CHARSET = %s ", ObCharset::charset_name(table_schema.get_charset_type())))) {
      OB_LOG(WARN, "fail to print default charset", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && !is_for_table_status && !is_index_tbl && CS_TYPE_INVALID != table_schema.get_collation_type() &&
      !ObCharset::is_default_collation(table_schema.get_charset_type(), table_schema.get_collation_type())) {
    if (OB_FAIL(databuff_printf(
            buf, buf_len, pos, "COLLATE = %s ", ObCharset::collation_name(table_schema.get_collation_type())))) {
      OB_LOG(WARN, "fail to print collate", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && table_schema.is_domain_index()) {
    if (full_text_columns.count() <= 0 || OB_UNLIKELY(virtual_column_id == OB_INVALID_ID)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "invalid domain index infos", K(full_text_columns), K(virtual_column_id));
    } else if (OB_FAIL(
                   print_table_definition_fulltext_indexs(full_text_columns, virtual_column_id, buf, buf_len, pos))) {
      OB_LOG(WARN, "failed to print table definition full text indexes", K(ret));
    } else if (table_schema.get_parser_name_str().empty()) {
      // do nothing
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "WITH PARSER '%s' ", table_schema.get_parser_name()))) {
      OB_LOG(WARN, "print parser name failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && !is_index_tbl) {
    if (OB_FAIL(print_table_definition_store_format(table_schema, buf, buf_len, pos))) {
      OB_LOG(WARN, "fail to print store format", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && !is_index_tbl && table_schema.get_expire_info().length() > 0 &&
      NULL != table_schema.get_expire_info().ptr()) {
    const ObString expire_str = table_schema.get_expire_info();
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "EXPIRE_INFO = (%.*s) ", expire_str.length(), expire_str.ptr()))) {
      OB_LOG(WARN, "fail to print expire info", K(ret), K(expire_str));
    }
  }
  if (OB_SUCC(ret) && !is_index_tbl) {
    int64_t paxos_replica_num = OB_INVALID_COUNT;
    if (OB_FAIL(table_schema.get_paxos_replica_num(schema_guard_, paxos_replica_num))) {
      OB_LOG(WARN, "fail to get paxos replica num", K(ret));
    } else if (OB_UNLIKELY(paxos_replica_num < 0)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "paxos_replica_num error", K(ret), K(paxos_replica_num), "table_id", table_schema.get_table_id());
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "REPLICA_NUM = %ld ", paxos_replica_num))) {
      OB_LOG(WARN, "fail to print replica num", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && table_schema.get_locality_str().length() > 0) {  // locality
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            "LOCALITY = \'%.*s\' ",
            table_schema.get_locality_str().length(),
            table_schema.get_locality_str().ptr()))) {
      OB_LOG(WARN, "fail to print replica num", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && table_schema.get_block_size() >= 0 && !is_index_tbl) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "BLOCK_SIZE = %ld ", table_schema.get_block_size()))) {
      OB_LOG(WARN, "fail to print block size", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && is_index_tbl && !table_schema.is_domain_index()) {
    const char* local_flag =
        table_schema.is_global_index_table() || table_schema.is_global_local_index_table() ? "GLOBAL " : "LOCAL ";
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", local_flag))) {
      OB_LOG(WARN, "fail to print global/local", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && !is_index_tbl) {
    if (OB_FAIL(databuff_printf(
            buf, buf_len, pos, "USE_BLOOM_FILTER = %s ", table_schema.is_use_bloomfilter() ? "TRUE" : "FALSE"))) {
      OB_LOG(WARN, "fail to print use bloom filter", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TABLET_SIZE = %ld ", table_schema.get_tablet_size()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print tablet_size", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "PCTFREE = %ld ", table_schema.get_pctfree()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print pctfree", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl && table_schema.get_dop() > 1) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "PARALLEL %ld ", table_schema.get_dop()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print dop", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && !is_index_tbl && common::OB_INVALID_ID != table_schema.get_tablegroup_id()) {
    const ObTablegroupSchema* tablegroup_schema = schema_guard_.get_tablegroup_schema(table_schema.get_tablegroup_id());
    if (NULL != tablegroup_schema) {
      const ObString tablegroup_name = tablegroup_schema->get_tablegroup_name_str();
      if (tablegroup_name.length() > 0 && NULL != tablegroup_name.ptr()) {
        if (OB_FAIL(databuff_printf(
                buf, buf_len, pos, "TABLEGROUP = '%.*s' ", tablegroup_name.length(), tablegroup_name.ptr()))) {
          OB_LOG(WARN, "fail to print tablegroup", K(ret), K(tablegroup_name));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "tablegroup name is null");
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "tablegroup schema is null");
    }
  }

  if (OB_SUCC(ret) && !is_index_tbl && table_schema.get_progressive_merge_num() > 0) {
    if (OB_FAIL(databuff_printf(
            buf, buf_len, pos, "PROGRESSIVE_MERGE_NUM = %ld ", table_schema.get_progressive_merge_num()))) {
      OB_LOG(WARN, "fail to print progressive merge num", K(ret), K(table_schema));
    }
  }

  if (OB_SUCC(ret) && is_agent_mode) {
    if (OB_FAIL(databuff_printf(
            buf, buf_len, pos, "PROGRESSIVE_MERGE_ROUND = %ld ", table_schema.get_progressive_merge_round()))) {
      OB_LOG(WARN, "fail to print progressive merge round", K(ret), K(table_schema));
    }
  }

  if (OB_SUCC(ret) && !is_for_table_status && table_schema.get_comment_str().length() > 0) {
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            is_index_tbl ? "COMMENT '%s' " : "COMMENT = '%s' ",
            to_cstring(ObHexEscapeSqlStr(table_schema.get_comment()))))) {
      OB_LOG(WARN, "fail to print comment", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && !is_index_tbl && table_schema.is_read_only()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "READ ONLY "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print table read only", K(ret));
    }
  }
  // backup table mode
  ObString table_mode_str = "";
  if (OB_SUCC(ret) && !is_index_tbl) {
    if (!is_agent_mode) {
    } else {  // true == agent_mode
      table_mode_str = ObBackUpTableModeOp::get_table_mode_str(table_schema.get_table_mode_struct());
    }
  }
  if (OB_SUCC(ret) && table_mode_str.length() > 0) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TABLE_MODE = '%s' ", table_mode_str.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print table table_mode", K(ret));
    }
  }

  // table_id for backup and restore
  if (OB_SUCC(ret) && !is_index_tbl && is_agent_mode) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " table_id = %lu ", table_schema.get_table_id()))) {
      OB_LOG(WARN, "fail to print progressive merge num", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && is_index_tbl && is_agent_mode) {
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            " index_table_id = %lu data_table_id = %lu",
            table_schema.get_table_id(),
            table_schema.get_data_table_id()))) {
      OB_LOG(WARN, "fail to print progressive merge num", K(ret), K(table_schema));
    }
  }

  if (OB_SUCC(ret) && is_agent_mode && table_schema.is_partitioned_table() && table_schema.has_partition() &&
      (USER_TABLE == table_schema.get_table_type() || USER_INDEX == table_schema.get_table_type())) {
    int64_t max_used_part_id = table_schema.get_part_option().get_max_used_part_id();
    if (-1 == max_used_part_id) {
      max_used_part_id = table_schema.get_part_option().get_part_num() - 1;
    }
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " MAX_USED_PART_ID = %ld ", max_used_part_id))) {
      OB_LOG(WARN, "fail to print table max_used_part_id", K(ret), K(max_used_part_id));
    }
  }

  return ret;
}

int ObSchemaPrinter::print_func_index_columns_definition(
    const ObString& expr_str, uint64_t column_id, char* buf, int64_t buf_len, int64_t& pos, bool is_agent_mode) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%.*s", expr_str.length(), expr_str.ptr()))) {
    OB_LOG(WARN, "failed to print column info", K(ret), K(expr_str));
  } else if (is_agent_mode && OB_FAIL(databuff_printf(buf, buf_len, pos, " id %lu", column_id))) {
    OB_LOG(WARN, "failed to print column id", K(ret), K(column_id));
  }
  return ret;
}

int ObSchemaPrinter::print_index_definition_columns(const ObTableSchema& data_schema, const ObTableSchema& index_schema,
    common::ObIArray<ObString>& full_text_columns, uint64_t& virtual_column_id, char* buf, const int64_t buf_len,
    int64_t& pos, bool is_agent_mode) const
{
  int ret = OB_SUCCESS;
  bool is_first = true;
  ObStringBuf allocator;
  ObColumnSchemaV2 last_col;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret), K(buf), K(buf_len));
  } else {
    // index columns contain rowkeys of base table, but no need to show them.
    const int64_t index_column_num = index_schema.get_index_column_number();
    const ObRowkeyInfo& index_rowkey_info = index_schema.get_rowkey_info();
    for (int64_t k = 0; OB_SUCC(ret) && k < index_column_num; k++) {
      const ObRowkeyColumn* rowkey_column = index_rowkey_info.get_column(k);
      const ObColumnSchemaV2* col = NULL;
      const ObColumnSchemaV2* data_col = NULL;
      if (NULL == rowkey_column) {
        ret = OB_SCHEMA_ERROR;
        SHARE_SCHEMA_LOG(WARN, "fail to get rowkey column", K(ret));
      } else if (NULL == (col = index_schema.get_column_schema(rowkey_column->column_id_))) {
        ret = OB_SCHEMA_ERROR;
        SHARE_SCHEMA_LOG(WARN, "fail to get column schema", K(ret));
      } else if (!col->is_shadow_column()) {
        if (!is_first) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n"))) {
            OB_LOG(WARN, "fail to print enter", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          // The column flag and default value expr of the index table are cleaned up
          // and need to be obtained from the data table schema
          if (NULL == (data_col = data_schema.get_column_schema(col->get_column_id()))) {
            ret = OB_SCHEMA_ERROR;
            SHARE_SCHEMA_LOG(WARN, "fail to get column schema", K(ret), K(*data_col));
          } else if (data_col->is_hidden() && data_col->is_generated_column()) {  // automatic generated column
            if (data_col->is_fulltext_column()) {
              // domain index
              virtual_column_id = data_col->get_column_id();
              if (OB_FAIL(print_full_text_columns_definition(data_schema,
                      data_col->get_column_name(),
                      full_text_columns,
                      buf,
                      buf_len,
                      pos,
                      is_first,
                      is_agent_mode))) {
                OB_LOG(WARN, "failed to print full text columns", K(ret));
              }
            } else if (data_col->is_func_idx_column()) {
              const ObString& expr_str = data_col->get_cur_default_value().is_null()
                                             ? data_col->get_orig_default_value().get_string()
                                             : data_col->get_cur_default_value().get_string();
              virtual_column_id = data_col->get_column_id();
              if (OB_FAIL(print_func_index_columns_definition(
                      expr_str, virtual_column_id, buf, buf_len, pos, is_agent_mode))) {
                OB_LOG(WARN, "failed to print func index columns", K(ret));
              }
            } else {
              // Prefix index
              const ObString& expr_str = data_col->get_cur_default_value().is_null()
                                             ? data_col->get_orig_default_value().get_string()
                                             : data_col->get_cur_default_value().get_string();
              ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
              sql::ObRawExprFactory expr_factory(allocator);
              sql::ObRawExpr* expr = NULL;
              ObArray<sql::ObQualifiedName> columns;
              ObString column_name;
              int64_t const_value = 0;
              SMART_VAR(sql::ObSQLSessionInfo, default_session)
              {
                if (OB_FAIL(default_session.test_init(0, 0, 0, &allocator))) {
                  OB_LOG(WARN, "init empty session failed", K(ret));
                } else if (OB_FAIL(default_session.load_default_sys_variable(false, false))) {
                  OB_LOG(WARN, "session load default system variable failed", K(ret));
                } else if (OB_FAIL(sql::ObRawExprUtils::build_generated_column_expr(
                               expr_str, expr_factory, default_session, expr, columns))) {
                  OB_LOG(WARN, "get generated column expr failed", K(ret));
                } else if (OB_ISNULL(expr)) {
                  ret = OB_ERR_UNEXPECTED;
                  OB_LOG(WARN, "expr is null");
                } else if (3 != expr->get_param_count()) {
                  ret = OB_ERR_UNEXPECTED;
                  OB_LOG(WARN, "It's wrong expr string", K(ret), K(expr->get_param_count()));
                } else if (1 != columns.count()) {
                  ret = OB_ERR_UNEXPECTED;
                  OB_LOG(WARN, "It's wrong expr string", K(ret), K(columns.count()));
                } else {
                  column_name = columns.at(0).col_name_;
                  sql::ObRawExpr* t_expr0 = expr->get_param_expr(0);
                  sql::ObRawExpr* t_expr1 = expr->get_param_expr(1);
                  sql::ObRawExpr* t_expr2 = expr->get_param_expr(2);
                  if (OB_ISNULL(t_expr0) || OB_ISNULL(t_expr1) || OB_ISNULL(t_expr2)) {
                    ret = OB_ERR_UNEXPECTED;
                    OB_LOG(WARN, "expr is null", K(ret));
                  } else if (T_INT != t_expr2->get_expr_type()) {
                    ret = OB_ERR_UNEXPECTED;
                    OB_LOG(WARN, "expr type is not int", K(ret));
                  } else {
                    const_value = (static_cast<sql::ObConstRawExpr*>(t_expr2))->get_value().get_int();
                    if (OB_FAIL(databuff_printf(buf,
                            buf_len,
                            pos,
                            !share::is_oracle_mode() ? "`%.*s`(%ld)" : "\"%.*s\"(%ld)",
                            column_name.length(),
                            column_name.ptr(),
                            const_value))) {
                      OB_LOG(WARN, "fail to print column name", K(ret), K(*col));
                    } else if (is_agent_mode &&
                               OB_FAIL(databuff_printf(buf, buf_len, pos, " id %lu", data_col->get_column_id()))) {
                      OB_LOG(WARN, "fail to print column id", K(ret), K(*col));
                    }
                  }
                }
              }
            }
          } else {
            // local index
            if (OB_FAIL(databuff_printf(
                    buf, buf_len, pos, !share::is_oracle_mode() ? " `%s`" : " \"%s\"", col->get_column_name()))) {
              OB_LOG(WARN, "fail to print column name", K(ret), K(*col));
            } else if (is_agent_mode && OB_FAIL(databuff_printf(buf, buf_len, pos, " id %lu", col->get_column_id()))) {
              OB_LOG(WARN, "fail to print column", K(ret), K(*col));
            }
          }
        }
        is_first = false;
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_full_text_columns_definition(const ObTableSchema& data_schema,
    const ObString& generated_column_name, ObIArray<ObString>& full_text_columns, char* buf, const int64_t buf_len,
    int64_t& pos, bool& is_first, bool is_agent_mode) const
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2* data_col = NULL;
  const char* prefix = "__word_segment_";
  uint64_t column_id = 0;
  if (OB_UNLIKELY(!generated_column_name.prefix_match(prefix))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "column prefix is invalid", K(ret), K(generated_column_name));
  }
  for (int64_t i = strlen(prefix); OB_SUCC(ret) && i <= generated_column_name.length(); ++i) {
    char ch = (i == generated_column_name.length() ? '_' : generated_column_name[i]);
    if (ch >= '0' && ch <= '9') {
      column_id = column_id * 10 + (ch - '0');
    } else if (ch != '_') {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "invalid character in column name", K(ret), K(ch), K(generated_column_name));
    } else if (OB_ISNULL(data_col = data_schema.get_column_schema(column_id))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "column id is invalid", K(ret), K(column_id));
    } else if (OB_FAIL(full_text_columns.push_back(data_col->get_column_name()))) {
      OB_LOG(WARN, "failed to push back column names", K(ret));
    } else if (OB_FAIL(databuff_printf(
                   buf, buf_len, pos, "%s  `%s`", is_first ? "" : ",\n", data_col->get_column_name()))) {
      OB_LOG(WARN, "fail to print column name", K(ret), K(*data_col));
    } else if (is_agent_mode && OB_FAIL(databuff_printf(buf, buf_len, pos, " id %lu", data_col->get_column_id()))) {
      OB_LOG(WARN, "fail to print column id", K(ret), K(*data_col));
    } else {
      is_first = false;
      column_id = 0;
    }
  }
  return ret;
}

int ObSchemaPrinter::print_table_index_stroing(const share::schema::ObTableSchema* index_schema,
    const share::schema::ObTableSchema* table_schema, char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_schema) || OB_ISNULL(table_schema) || OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(index_schema), K(table_schema), K(buf), K(buf_len), K(ret));
  } else {
    int64_t column_count = index_schema->get_column_count();
    const ObRowkeyInfo& index_rowkey_info = index_schema->get_rowkey_info();
    int64_t rowkey_count = index_rowkey_info.get_size();
    if (column_count > rowkey_count) {
      bool first_storing_column = true;
      for (ObTableSchema::const_column_iterator row_col = index_schema->column_begin();
           OB_SUCCESS == ret && NULL != row_col && row_col != index_schema->column_end();
           row_col++) {
        int64_t k = 0;
        const ObRowkeyInfo& tab_rowkey_info = table_schema->get_rowkey_info();
        int64_t tab_rowkey_count = tab_rowkey_info.get_size();
        for (; k < tab_rowkey_count; k++) {
          const ObRowkeyColumn* rowkey_column = tab_rowkey_info.get_column(k);
          if (NULL != *row_col && rowkey_column->column_id_ == (*row_col)->get_column_id()) {
            break;
          }
        }
        if (k == tab_rowkey_count && NULL != *row_col && !(*row_col)->get_rowkey_position() &&
            !(*row_col)->is_hidden()) {
          if (first_storing_column) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " STORING ("))) {
              OB_LOG(WARN, "fail to print STORING(", K(ret));
            }
            first_storing_column = false;
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(databuff_printf(buf,
                    buf_len,
                    pos,
                    !share::is_oracle_mode() ? "`%s`, " : "\"%s\", ",
                    (*row_col)->get_column_name()))) {
              OB_LOG(WARN, "fail to print column name", K(ret), K((*row_col)->get_column_name()));
            }
          }
        }
      }
      if (OB_SUCCESS == ret && !first_storing_column) {
        pos -= 2;  // fallback to the col name
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
          OB_LOG(WARN, "fail to print )", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_index_table_definition(uint64_t index_table_id, char* buf, const int64_t buf_len,
    int64_t& pos, const ObTimeZoneInfo* tz_info, bool is_agent_mode) const
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema* ds_schema = NULL;
  const ObTableSchema* table_schema = NULL;
  const ObTableSchema* index_table_schema = NULL;
  ObStringBuf allocator;
  ObString index_name;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret));
  } else if (OB_FAIL(schema_guard_.get_table_schema(index_table_id, index_table_schema))) {
    OB_LOG(WARN, "fail to get index table schema", K(ret));
  } else if (NULL == index_table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    OB_LOG(WARN, "Unknow table", K(ret), K(index_table_id));
  } else if (OB_FAIL(schema_guard_.get_table_schema(index_table_schema->get_data_table_id(), table_schema))) {
    OB_LOG(WARN, "fail to get data table schema", K(ret));
  } else if (NULL == table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    OB_LOG(WARN, "Unknow table", K(ret), K(index_table_schema->get_data_table_id()));
  } else if (OB_FAIL(schema_guard_.get_database_schema(table_schema->get_database_id(), ds_schema))) {
    OB_LOG(WARN, "fail to get database schema", K(ret));
  } else if (NULL == ds_schema) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "database not exist", K(ret));
  } else {
    index_name = index_table_schema->get_table_name();
    if (index_name.prefix_match(OB_MYSQL_RECYCLE_PREFIX) || index_name.prefix_match(OB_ORACLE_RECYCLE_PREFIX)) {
      // recyclebin object use original index name
    } else {
      if (OB_FAIL(ObTableSchema::get_index_name(allocator,
              table_schema->get_table_id(),
              ObString::make_string(index_table_schema->get_table_name()),
              index_name))) {
        OB_LOG(WARN, "get index table name failed");
      }
    }
    if (OB_FAIL(ret)) {
    } else if (index_table_schema->is_unique_index()) {
      if (OB_FAIL(databuff_printf(buf,
              buf_len,
              pos,
              !share::is_oracle_mode() ? "CREATE UNIQUE INDEX if not exists `%s`.`%.*s` on `%s`.`%s` (\n"
                                       : "CREATE UNIQUE INDEX \"%s\".\"%.*s\" on \"%s\".\"%s\" (\n",
              ds_schema->get_database_name(),
              index_name.length(),
              index_name.ptr(),
              ds_schema->get_database_name(),
              table_schema->get_table_name()))) {
        OB_LOG(WARN, "fail to print create table prefix", K(ret), K(table_schema->get_table_name()));
      }
    } else if (index_table_schema->is_domain_index()) {
      if (OB_FAIL(databuff_printf(buf,
              buf_len,
              pos,
              !share::is_oracle_mode() ? "CREATE FULLTEXT INDEX if not exists `%s`.`%.*s` on `%s`.`%s` (\n"
                                       : "CREATE FULLTEXT INDEX \"%s\".\"%.*s\" on \"%s\".\"%s\" (\n",
              ds_schema->get_database_name(),
              index_name.length(),
              index_name.ptr(),
              ds_schema->get_database_name(),
              table_schema->get_table_name()))) {
        OB_LOG(WARN, "fail to print create table prefix", K(ret), K(table_schema->get_table_name()));
      }
    } else {
      if (OB_FAIL(databuff_printf(buf,
              buf_len,
              pos,
              !share::is_oracle_mode() ? "CREATE INDEX if not exists `%s`.`%.*s` on `%s`.`%s` (\n"
                                       : "CREATE INDEX \"%s\".\"%.*s\" on \"%s\".\"%s\" (\n",
              ds_schema->get_database_name(),
              index_name.length(),
              index_name.ptr(),
              ds_schema->get_database_name(),
              table_schema->get_table_name()))) {
        OB_LOG(WARN, "fail to print create table prefix", K(ret), K(table_schema->get_table_name()));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObSEArray<ObString, 4> full_text_columns;
    uint64_t virtual_column_id = OB_INVALID_ID;
    if (OB_FAIL(print_index_definition_columns(*table_schema,
            *index_table_schema,
            full_text_columns,
            virtual_column_id,
            buf,
            buf_len,
            pos,
            is_agent_mode))) {
      OB_LOG(WARN, "fail to print columns", K(ret), K(*index_table_schema));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n) "))) {
      OB_LOG(WARN, "fail to print )", K(ret));
    } else if (OB_FAIL(print_table_definition_table_options(*index_table_schema,
                   full_text_columns,
                   virtual_column_id,
                   buf,
                   buf_len,
                   pos,
                   false,
                   NULL,
                   is_agent_mode))) {
      OB_LOG(WARN, "fail to print table options", K(ret), K(*index_table_schema));
    } else if (OB_FAIL(print_table_index_stroing(index_table_schema, table_schema, buf, buf_len, pos))) {
      OB_LOG(WARN, "fail to print partition table index storing", K(ret), K(*index_table_schema), K(*table_schema));
    } else if (index_table_schema->is_global_index_table() &&
               OB_FAIL(print_table_definition_partition_options(
                   *index_table_schema, buf, buf_len, pos, is_agent_mode, tz_info))) {
      OB_LOG(WARN, "fail to print partition info for index", K(ret), K(*index_table_schema));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ";\n"))) {
      OB_LOG(WARN, "fail to print end ;", K(ret));
    }
    OB_LOG(DEBUG, "print table schema", K(ret), K(*index_table_schema));
  }
  return ret;
}

int ObSchemaPrinter::print_view_definiton(uint64_t table_id, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;

  if (buf && buf_len > 0) {
    const ObTableSchema* table_schema = NULL;
    if (OB_FAIL(schema_guard_.get_table_schema(table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(table_id));
    } else if (NULL == table_schema) {
      ret = OB_TABLE_NOT_EXIST;
      SHARE_SCHEMA_LOG(WARN, "Unknow table", K(ret), K(table_id));
    } else if (OB_FAIL(databuff_printf(buf,
                   buf_len,
                   pos,
                   share::is_oracle_mode() ? "CREATE %sVIEW \"%s\" AS %.*s" : "CREATE %sVIEW `%s` AS %.*s",
                   table_schema->is_materialized_view() ? "MATERIALIZED " : "",
                   table_schema->get_table_name(),
                   table_schema->get_view_schema().get_view_definition_str().length(),
                   table_schema->get_view_schema().get_view_definition_str().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print view definition", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf is bull", K(ret));
  }
  return ret;
}

int ObSchemaPrinter::print_tablegroup_definition(uint64_t tablegroup_id, char* buf, const int64_t& buf_len,
    int64_t& pos, bool agent_mode, const ObTimeZoneInfo* tz_info)
{
  int ret = OB_SUCCESS;

  const ObTablegroupSchema* tablegroup_schema = NULL;
  if (OB_FAIL(schema_guard_.get_tablegroup_schema(tablegroup_id, tablegroup_schema))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else if (NULL == tablegroup_schema) {
    ret = OB_TABLE_NOT_EXIST;
    SHARE_SCHEMA_LOG(WARN, "Unknow table", K(ret), K(tablegroup_id));
  } else if (OB_FAIL(databuff_printf(buf,
                 buf_len,
                 pos,
                 share::is_oracle_mode() ? "CREATE TABLEGROUP \"%s\" " : "CREATE TABLEGROUP IF NOT EXISTS `%s` ",
                 tablegroup_schema->get_tablegroup_name().ptr()))) {
    SHARE_SCHEMA_LOG(
        WARN, "fail to print create tablegroup prefix", K(ret), K(tablegroup_schema->get_tablegroup_name()));
  } else if (OB_FAIL(
                 print_tablegroup_definition_tablegroup_options(*tablegroup_schema, buf, buf_len, pos, agent_mode))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print tablegroup options", K(ret), K(*tablegroup_schema));
  } else if (OB_FAIL(print_tablegroup_definition_partition_options(
                 *tablegroup_schema, buf, buf_len, pos, agent_mode, tz_info))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print partition options", K(ret), K(*tablegroup_schema));
  }
  SHARE_SCHEMA_LOG(DEBUG, "print tablegroup schema", K(ret), K(*tablegroup_schema));
  return ret;
}

int ObSchemaPrinter::print_tablegroup_definition_tablegroup_options(
    const ObTablegroupSchema& tablegroup_schema, char* buf, const int64_t& buf_len, int64_t& pos, bool agent_mode) const
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  if (tablegroup_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", K(ret), K(tablegroup_id));
  } else if (is_new_tablegroup_id(tablegroup_id)) {
    // Only tablegroups created after 2.0 have the following attributes
    if (OB_SUCCESS == ret && tablegroup_schema.get_locality_str().length() > 0) {  // locality
      if (OB_FAIL(databuff_printf(buf,
              buf_len,
              pos,
              " LOCALITY = \'%.*s\'",
              tablegroup_schema.get_locality_str().length(),
              tablegroup_schema.get_locality_str().ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print locality", K(ret), K(tablegroup_schema));
      }
    }
    if (OB_SUCCESS == ret && 0 < tablegroup_schema.get_primary_zone().length()) {  // primary_zone
      bool is_random = (0 == tablegroup_schema.get_primary_zone().compare(common::OB_RANDOM_PRIMARY_ZONE));
      if (OB_FAIL(databuff_printf(buf,
              buf_len,
              pos,
              is_random ? " PRIMARY_ZONE = %.*s" : " PRIMARY_ZONE=\'%.*s\'",
              tablegroup_schema.get_primary_zone().length(),
              tablegroup_schema.get_primary_zone().ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print primary_zone", K(ret), K(tablegroup_schema.get_primary_zone()));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(
              buf, buf_len, pos, " BINDING = %s", tablegroup_schema.get_binding() ? "TRUE" : "FALSE"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print binding", K(ret), K(buf_len), K(pos));
      }
    }
  }
  if (OB_SUCC(ret) && agent_mode) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " TABLEGROUP_ID = %ld", tablegroup_schema.get_tablegroup_id()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print tablegroup_id", K(ret), K(tablegroup_schema.get_tablegroup_id()));
    }
  }
  return ret;
}

int ObSchemaPrinter::print_tablegroup_definition_partition_options(const ObTablegroupSchema& tablegroup_schema,
    char* buf, const int64_t& buf_len, int64_t& pos, bool agent_mode, const ObTimeZoneInfo* tz_info) const
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  if (tablegroup_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", K(ret), K(tablegroup_id));
  } else if (!is_new_tablegroup_id(tablegroup_id)) {
    // skip
  } else if (PARTITION_LEVEL_ONE == tablegroup_schema.get_part_level() ||
             PARTITION_LEVEL_TWO == tablegroup_schema.get_part_level()) {
    ObString disp_part_fun_expr_str;
    ObSqlString disp_part_str;
    bool is_subpart = false;
    const ObPartitionSchema* partition_schema = &tablegroup_schema;
    if (PARTITION_LEVEL_TWO == tablegroup_schema.get_part_level()) {
      is_subpart = true;
    }
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print enter", K(ret));
    } else if (OB_FAIL(print_tablegroup_partition_func(tablegroup_schema, disp_part_str, is_subpart))) {
      SHARE_SCHEMA_LOG(WARN, "failed to print part func", K(ret));
    } else if (FALSE_IT(disp_part_fun_expr_str = disp_part_str.string())) {
      // will not reach here
    } else if (OB_FAIL(databuff_printf(
                   buf, buf_len, pos, " %.*s", disp_part_fun_expr_str.length(), disp_part_fun_expr_str.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to printf partition expr", K(ret), K(disp_part_fun_expr_str));
    } else if (is_subpart && partition_schema->is_sub_part_template()) {
      if (OB_FAIL(print_template_sub_partition_elements(partition_schema, buf, buf_len, pos, tz_info, true))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print sub partition elements", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      bool tablegroup_def = true;
      if (tablegroup_schema.is_range_part()) {
        if (OB_FAIL(print_range_partition_elements(
                partition_schema, buf, buf_len, pos, agent_mode, tablegroup_def, tz_info))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print partition elements", K(ret));
        }
      } else if (tablegroup_schema.is_list_part()) {
        if (OB_FAIL(print_list_partition_elements(
                partition_schema, buf, buf_len, pos, agent_mode, tablegroup_def, tz_info))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print partition elements", K(ret));
        }
      } else if (is_hash_like_part(tablegroup_schema.get_part_option().get_part_func_type())) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, " partitions %ld\n", tablegroup_schema.get_first_part_num()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to printf partition number", K(ret), K(tablegroup_schema.get_first_part_num()));
        }
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_hash_sub_partition_elements(
    ObSubPartition** sub_part_array, const int64_t sub_part_num, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_part_array)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "subpartition_array is NULL", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " (\n"))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print subpartition template", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_part_num; ++i) {
      const ObSubPartition* sub_partition = sub_part_array[i];
      if (OB_ISNULL(sub_partition)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "sub partition is null", K(ret), K(sub_part_num));
      } else {
        const ObString& part_name = sub_partition->get_part_name();
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "subpartition %.*s", part_name.length(), part_name.ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "print part name failed", K(ret), K(part_name));
        } else if (sub_part_num - 1 != i && OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else if (sub_part_num - 1 == i && OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else {
        }
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_hash_sub_partition_elements_for_tablegroup(
    const ObPartitionSchema*& schema, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema is null", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " subpartitions %ld", schema->get_def_sub_part_num()))) {
    SHARE_SCHEMA_LOG(WARN, "print partition number failed", K(ret));
  }
  return ret;
}

int ObSchemaPrinter::print_list_sub_partition_elements(ObSubPartition** sub_part_array, const int64_t sub_part_num,
    char* buf, const int64_t& buf_len, int64_t& pos, const common::ObTimeZoneInfo* tz_info) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_part_array)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "subpartition_array is NULL", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " (\n"))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print subpartition template", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_part_num; ++i) {
      const ObSubPartition* sub_partition = sub_part_array[i];
      if (OB_ISNULL(sub_partition)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "sub partition is null", K(ret), K(sub_part_num));
      } else {
        const ObString& part_name = sub_partition->get_part_name();
        if (OB_FAIL(databuff_printf(buf,
                buf_len,
                pos,
                "subpartition %.*s values %s (",
                part_name.length(),
                part_name.ptr(),
                share::is_oracle_mode() ? "" : "in"))) {
          SHARE_SCHEMA_LOG(WARN, "print part name failed", K(ret), K(part_name));
        } else if (OB_FAIL(ObPartitionUtils::convert_rows_to_sql_literal(
                       sub_partition->get_list_row_values(), buf, buf_len, pos, false, tz_info))) {
          SHARE_SCHEMA_LOG(WARN, "convert rows to sql literal", K(ret), K(sub_partition->get_list_row_values()));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else if (sub_part_num - 1 != i && OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else if (sub_part_num - 1 == i && OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else {
        }
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_range_sub_partition_elements(ObSubPartition** sub_part_array, const int64_t sub_part_num,
    char* buf, const int64_t& buf_len, int64_t& pos, const ObTimeZoneInfo* tz_info) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_part_array)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "subpartition_array is NULL", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " (\n"))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print subpartition template", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_part_num; ++i) {
      const ObSubPartition* sub_partition = sub_part_array[i];
      if (OB_ISNULL(sub_partition)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "sub partition is null", K(ret), K(sub_part_num));
      } else {
        const ObString& part_name = sub_partition->get_part_name();
        if (OB_FAIL(databuff_printf(
                buf, buf_len, pos, "subpartition %.*s values less than (", part_name.length(), part_name.ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "print part name failed", K(ret), K(part_name));
        } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
                       sub_partition->get_high_bound_val(), buf, buf_len, pos, false, tz_info))) {
          SHARE_SCHEMA_LOG(WARN, "convert rowkey to sql literal", K(ret), K(sub_partition->get_high_bound_val()));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else if (sub_part_num - 1 != i && OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else if (sub_part_num - 1 == i && OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else {
        }
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_template_sub_partition_elements(const ObPartitionSchema*& schema, char* buf,
    const int64_t& buf_len, int64_t& pos, const ObTimeZoneInfo* tz_info, bool is_tablegroup) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema is null", K(ret));
  } else if (!schema->is_sub_part_template()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema is not sub part template", K(ret));
  } else if (is_tablegroup && is_hash_like_part(schema->get_sub_part_option().get_part_func_type())) {
    // tablegroup not support hash subpartition template, should print subpartitions x ahead
    ret = print_hash_sub_partition_elements_for_tablegroup(schema, buf, buf_len, pos);
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " subpartition template"))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print subpartition template", K(ret));
  } else if (is_hash_like_part(schema->get_sub_part_option().get_part_func_type())) {
    ret = print_hash_sub_partition_elements(
        schema->get_def_subpart_array(), schema->get_def_sub_part_num(), buf, buf_len, pos);
  } else if (schema->is_range_subpart()) {
    ret = print_range_sub_partition_elements(
        schema->get_def_subpart_array(), schema->get_def_sub_part_num(), buf, buf_len, pos, tz_info);
  } else if (schema->is_list_subpart()) {
    ret = print_list_sub_partition_elements(
        schema->get_def_subpart_array(), schema->get_def_sub_part_num(), buf, buf_len, pos, tz_info);
  }
  return ret;
}

int ObSchemaPrinter::print_individual_sub_partition_elements(const ObPartitionSchema*& schema,
    const ObPartition* partition, char* buf, const int64_t& buf_len, int64_t& pos, const ObTimeZoneInfo* tz_info) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema) || OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "argument is null", K(ret), K(schema), K(partition));
  } else if (schema->is_sub_part_template()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema is sub part template", K(ret));
  } else if (is_hash_like_part(schema->get_sub_part_option().get_part_func_type())) {
    ret = print_hash_sub_partition_elements(
        partition->get_subpart_array(), partition->get_sub_part_num(), buf, buf_len, pos);
  } else if (schema->is_range_subpart()) {
    ret = print_range_sub_partition_elements(
        partition->get_subpart_array(), partition->get_sub_part_num(), buf, buf_len, pos, tz_info);
  } else if (schema->is_list_subpart()) {
    ret = print_list_sub_partition_elements(
        partition->get_subpart_array(), partition->get_sub_part_num(), buf, buf_len, pos, tz_info);
  }
  return ret;
}

int ObSchemaPrinter::print_list_partition_elements(const ObPartitionSchema*& schema, char* buf, const int64_t& buf_len,
    int64_t& pos, bool agent_mode /*false*/, bool tablegroup_def /*false*/,
    const common::ObTimeZoneInfo* tz_info /*NULL*/) const
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema is null", K(ret));
  } else {
    ObPartition** part_array = schema->get_part_array();
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "partition_array is NULL", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n("))) {
      SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
    } else {
      int64_t part_num = schema->get_first_part_num();
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
        const ObPartition* partition = part_array[i];
        if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "partition is NULL", K(ret), K(part_num));
        } else {
          const ObString& part_name = partition->get_part_name();
          bool print_collation = agent_mode && tablegroup_def;
          if (OB_FAIL(databuff_printf(buf,
                  buf_len,
                  pos,
                  "partition %.*s values %s (",
                  part_name.length(),
                  part_name.ptr(),
                  share::is_oracle_mode() ? "" : "in"))) {
            SHARE_SCHEMA_LOG(WARN, "print partition name failed", K(ret), K(part_name));
          } else if (OB_FAIL(ObPartitionUtils::convert_rows_to_sql_literal(
                         partition->get_list_row_values(), buf, buf_len, pos, print_collation, tz_info))) {
            SHARE_SCHEMA_LOG(WARN, "convert rows to sql literal failed", K(ret), K(partition->get_list_row_values()));
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
            SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
          } else if (!schema->is_sub_part_template() &&
                     OB_FAIL(print_individual_sub_partition_elements(schema, partition, buf, buf_len, pos, tz_info))) {
            SHARE_SCHEMA_LOG(WARN, "failed to print individual sub partition elements", K(ret));
          } else if (part_num - 1 != i && OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n"))) {
            SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
          } else if (part_num - 1 == i && OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
            SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
          } else {
          }
        }
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_range_partition_elements(const ObPartitionSchema*& schema, char* buf, const int64_t& buf_len,
    int64_t& pos, bool agent_mode, bool tablegroup_def, const common::ObTimeZoneInfo* tz_info) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema is null", K(ret));
  } else {
    ObPartition** part_array = schema->get_part_array();
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "partition_array is NULL", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n("))) {
      SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
    } else {
      int64_t part_num = schema->get_first_part_num();
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
        const ObPartition* partition = part_array[i];
        if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "partition is NULL", K(ret), K(part_num));
        } else {
          const ObString& part_name = partition->get_part_name();
          bool print_collation = agent_mode && tablegroup_def;
          if (OB_FAIL(databuff_printf(
                  buf, buf_len, pos, "partition %.*s values less than (", part_name.length(), part_name.ptr()))) {
            SHARE_SCHEMA_LOG(WARN, "print partition name failed", K(ret), K(part_name));
          } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
                         partition->get_high_bound_val(), buf, buf_len, pos, print_collation, tz_info))) {
            SHARE_SCHEMA_LOG(WARN, "convert rowkey to sql literal failed", K(ret), K(partition->get_high_bound_val()));
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
            SHARE_SCHEMA_LOG(WARN, "print failed", K(ret));
          } else if (agent_mode && !tablegroup_def) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " id %ld", partition->get_part_id()))) {  // print id
              SHARE_SCHEMA_LOG(WARN, "print part_id failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (!schema->is_sub_part_template() &&
                OB_FAIL(print_individual_sub_partition_elements(schema, partition, buf, buf_len, pos, tz_info))) {
              SHARE_SCHEMA_LOG(WARN, "failed to print individual sub partition elements", K(ret));
            } else if (part_num - 1 != i && OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n"))) {
              SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
            } else if (part_num - 1 == i && OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
              SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_database_definiton(
    uint64_t database_id, bool if_not_exists, char* buf, const int64_t& buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  int64_t mark_pos = 0;
  const ObDatabaseSchema* database_schema = NULL;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(buf), K(buf_len));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_guard_.get_database_schema(database_id, database_schema))) {
      LOG_WARN("get database schema failed ", K(ret));
    } else if (NULL == database_schema) {
      ret = OB_ERR_BAD_DATABASE;
      SHARE_SCHEMA_LOG(WARN, "Unknow database", K(ret), K(database_id));
    } else if (OB_FAIL(databuff_printf(buf,
                   buf_len,
                   pos,
                   share::is_oracle_mode() ? "CREATE DATABASE %s\"%.*s\"" : "CREATE DATABASE %s`%.*s`",
                   true == if_not_exists ? "IF NOT EXISTS " : "",
                   database_schema->get_database_name_str().length(),
                   database_schema->get_database_name_str().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print database definition", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    mark_pos = pos;
  }
  if (OB_SUCCESS == ret && CHARSET_INVALID != database_schema->get_charset_type()) {
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            " DEFAULT CHARACTER SET = %s",
            ObCharset::charset_name(database_schema->get_charset_type())))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print default charset", K(ret), K(*database_schema));
    }
  }
  if (OB_SUCCESS == ret && !share::is_oracle_mode() && CS_TYPE_INVALID != database_schema->get_collation_type() &&
      !ObCharset::is_default_collation(database_schema->get_collation_type())) {
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            " DEFAULT COLLATE = %s",
            ObCharset::collation_name(database_schema->get_collation_type())))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print default collate", K(ret), K(*database_schema));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t paxos_replica_num = OB_INVALID_COUNT;
    if (OB_FAIL(database_schema->get_paxos_replica_num(schema_guard_, paxos_replica_num))) {
      LOG_WARN("fail to get paxos replica num", K(ret));
    } else if (OB_UNLIKELY(paxos_replica_num < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN(
          "paxos replica num error", K(ret), K(paxos_replica_num), "database_id", database_schema->get_database_id());
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " REPLICA_NUM = %ld", paxos_replica_num))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print replica num", K(ret), K(*database_schema));
    } else {
    }  // no more to do
  }

  if (OB_SUCCESS == ret && 0 < database_schema->get_primary_zone().length()) {  // primary_zone
    bool is_random = (0 == database_schema->get_primary_zone().compare(common::OB_RANDOM_PRIMARY_ZONE));
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            is_random ? " PRIMARY_ZONE = %.*s" : " PRIMARY_ZONE = \'%.*s\'",
            database_schema->get_primary_zone().length(),
            database_schema->get_primary_zone().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print primary_zone", K(ret), K(database_schema->get_primary_zone()));
    }
  }
  if (OB_SUCC(ret) && database_schema->is_read_only()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " READ ONLY"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print database read only", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t tablegroup_id = database_schema->get_default_tablegroup_id();
    if (common::OB_INVALID_ID != tablegroup_id) {
      const ObTablegroupSchema* tablegroup_schema = schema_guard_.get_tablegroup_schema(tablegroup_id);
      if (NULL != tablegroup_schema) {
        const ObString tablegroup_name = tablegroup_schema->get_tablegroup_name();
        if (!tablegroup_name.empty()) {
          if (OB_FAIL(databuff_printf(
                  buf, buf_len, pos, " DEFAULT TABLEGROUP = %.*s", tablegroup_name.length(), tablegroup_name.ptr()))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print tablegroup", K(ret), K(tablegroup_name));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "tablegroup name is empty");
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "tablegroup schema is null");
      }
    }
  }
  if (OB_SUCCESS == ret && pos > mark_pos) {
    buf[pos] = '\0';  // remove trailer dot and space
  }
  return ret;
}

int ObSchemaPrinter::print_tenant_definition(uint64_t tenant_id, common::ObMySQLProxy* sql_proxy, char* buf,
    const int64_t& buf_len, int64_t& pos, bool is_agent_mode) const
{
  int ret = OB_SUCCESS;
  const ObTenantSchema* tenant_schema = NULL;
  const ObSysVariableSchema* sys_variable_schema = NULL;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(buf), K(buf_len));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_guard_.get_tenant_info(tenant_id, tenant_schema))) {
      SHARE_SCHEMA_LOG(WARN, "failed to get tenant info", K(tenant_id), K(ret));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      SHARE_SCHEMA_LOG(WARN, "tenant not exist", K(tenant_id), K(ret));
    } else if (OB_FAIL(schema_guard_.get_sys_variable_schema(tenant_id, sys_variable_schema))) {
      SHARE_SCHEMA_LOG(WARN, "get sys variable schema failed", K(ret));
    } else if (OB_ISNULL(sys_variable_schema)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "sys variable schema is null", K(ret));
    } else if (OB_FAIL(databuff_printf(buf,
                   buf_len,
                   pos,
                   "CREATE TENANT %.*s ",
                   tenant_schema->get_tenant_name_str().length(),
                   tenant_schema->get_tenant_name_str().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print tenant name", K(ret), K(tenant_schema->get_tenant_name_str()));
    }
  }

  if (OB_SUCC(ret)) {  // charset
    const ObSysVarSchema* server_collation = NULL;
    ObObj server_collation_obj;
    ObCollationType server_collation_type = CS_TYPE_INVALID;
    ObArenaAllocator allocator;
    if (OB_FAIL(schema_guard_.get_tenant_system_variable(tenant_id, SYS_VAR_COLLATION_SERVER, server_collation))) {
      LOG_WARN("fail to get tenant system variable", K(ret));
    } else if (OB_FAIL(server_collation->get_value(&allocator, NULL, server_collation_obj))) {
      LOG_WARN("fail to get value", K(ret));
    } else {
      server_collation_type = static_cast<ObCollationType>(server_collation_obj.get_int());
      if (ObCharset::is_valid_collation(server_collation_type) &&
          OB_FAIL(databuff_printf(buf, buf_len, pos, "charset=%s, ", ObCharset::charset_name(server_collation_type)))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print default charset", K(ret), K(server_collation_type));
      }
    }
  }

  if (OB_SUCC(ret)) {  // replica_num
    int64_t paxos_replica_num = OB_INVALID_COUNT;
    if (OB_FAIL(tenant_schema->get_paxos_replica_num(schema_guard_, paxos_replica_num))) {
      LOG_WARN("fail to get paxos replica num", K(ret));
    } else if (OB_UNLIKELY(paxos_replica_num < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("paxos replica num error", K(ret), K(paxos_replica_num), "tenant_id", tenant_schema->get_tenant_id());
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "replica_num=%ld, ", paxos_replica_num))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print default charset", K(ret));
    } else {
    }  // no more to do
  }
  if (OB_SUCC(ret)) {  // zone_list
    common::ObArray<common::ObZone> zone_list;
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "zone_list("))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print zone_list", K(ret));
    } else if (OB_FAIL(tenant_schema->get_zone_list(zone_list))) {
      SHARE_SCHEMA_LOG(WARN, "fail to get zone list", K(ret));
    } else {
      int64_t i = 0;
      for (i = 0; OB_SUCC(ret) && i < zone_list.count() - 1; i++) {
        if (OB_FAIL(databuff_printf(buf,
                buf_len,
                pos,
                "\'%.*s\', ",
                static_cast<int32_t>(zone_list.at(i).size()),
                zone_list.at(i).ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print zon_list", K(ret), K(zone_list.at(i)));
        }
      }
      if (OB_SUCCESS == ret && zone_list.count() > 0) {
        if (OB_FAIL(databuff_printf(buf,
                buf_len,
                pos,
                "\'%.*s\'), ",
                static_cast<int32_t>(zone_list.at(i).size()),
                zone_list.at(i).ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print zon_list", K(ret), K(zone_list.at(i)));
        }
      }
    }
  }
  if (OB_SUCCESS == ret && 0 < tenant_schema->get_primary_zone().length()) {  // primary_zone
    bool is_random = (0 == tenant_schema->get_primary_zone().compare(common::OB_RANDOM_PRIMARY_ZONE));
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            is_random ? "primary_zone=%.*s, " : "primary_zone=\'%.*s\', ",
            tenant_schema->get_primary_zone().length(),
            tenant_schema->get_primary_zone().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print primary_zone", K(ret), K(tenant_schema->get_primary_zone()));
    }
  }
  if (OB_SUCCESS == ret && tenant_schema->get_locality_str().length() > 0) {  // locality
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            "locality=\'%.*s\', ",
            tenant_schema->get_locality_str().length(),
            tenant_schema->get_locality_str().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print locality", K(ret), K(tenant_schema->get_locality_str()));
    }
  }
  if (OB_SUCC(ret)) {  // resource_pool_list
    ObUnitInfoGetter ui_getter;
    typedef ObSEArray<ObResourcePool, 10> PoolList;
    PoolList pool_list;
    if (OB_ISNULL(sql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "sql proxy is null", K(ret));
    } else if (OB_FAIL(ui_getter.init(*sql_proxy, &GCONF))) {
      SHARE_SCHEMA_LOG(WARN, "init unit getter fail", K(ret));
    } else if (OB_FAIL(ui_getter.get_pools_of_tenant(tenant_id, pool_list))) {
      SHARE_SCHEMA_LOG(WARN, "faile to get pool list", K(tenant_id));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "resource_pool_list("))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print resource_pool_list", K(ret));
    } else {
      int64_t i = 0;
      for (i = 0; OB_SUCC(ret) && i < pool_list.count() - 1; i++) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\'%s\', ", pool_list.at(i).name_.ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print resource_pool_list", K(ret), K(pool_list.at(i)));
        }
      }
      if (OB_SUCCESS == ret && pool_list.count() > 0) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\'%s\')", pool_list.at(i).name_.ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print resource_pool_list", K(ret), K(pool_list.at(i)));
        }
      }
    }
  }
  if (OB_SUCC(ret) && sys_variable_schema->is_read_only()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", read only"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print tenant read only", K(ret));
    }
  }

  if (OB_SUCC(ret) && share::is_oracle_mode() && is_agent_mode) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " set ob_tcp_invited_nodes='%%', ob_compatibility_mode='oracle'"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print tenant ob_compatibility_mode", K(ret));
    } else if (OB_FAIL(add_create_tenant_variables(tenant_id, sql_proxy, buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "failed to add create tenant variables", K(ret));
    }
  }
  return ret;
}

int ObSchemaPrinter::add_create_tenant_variables(const uint64_t tenant_id, common::ObMySQLProxy* const sql_proxy,
    char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  HEAP_VAR(ObMySQLProxy::ReadResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    int64_t flags = ObSysVarFlag::NONE;
    int64_t data_type = ObNullType;
    ObString name;
    ObString string_value;
    int64_t int_value = INT64_MAX;
    uint64_t uint_value = UINT64_MAX;

    if (OB_ISNULL(buf) || OB_ISNULL(sql_proxy)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "argument is invalid", K(ret));
    } else if (OB_FAIL(sql.assign_fmt("select flags, data_type, name, value from __all_virtual_sys_variable"
                                      " where tenant_id= %lu;",
                   tenant_id))) {
      OB_LOG(WARN, "fail to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy->read(res, sql.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", K(ret));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret));
    }

    while (OB_SUCC(ret)) {
      flags = ObSysVarFlag::NONE;
      data_type = ObNullType;
      int_value = INT64_MAX;
      uint_value = UINT64_MAX;
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          OB_LOG(WARN, "fail to get next result", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "result is NULL", K(ret));
      } else if (OB_FAIL(result->get_int("flags", flags))) {
        OB_LOG(WARN, "fail to get value", K(ret));
      } else {
        if (((flags & ObSysVarFlag::GLOBAL_SCOPE) != 0) && ((flags & ObSysVarFlag::WITH_CREATE) != 0)) {
          // only backup global and not read only variable
          if (OB_FAIL(result->get_varchar("name", name))) {
            OB_LOG(WARN, "fail to get name", K(ret));
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", %.*s ", name.length(), name.ptr()))) {
            OB_LOG(WARN, "fail to print name", K(name), K(ret));
          } else if (OB_FAIL(result->get_int("data_type", data_type))) {
            OB_LOG(WARN, "fail to get data type", K(ret));
          } else {
            if (ObIntType == data_type) {
              if (OB_FAIL(result->get_int("value", int_value))) {
                OB_LOG(WARN, "fail to int value", K(ret));
              } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " = %ld", int_value))) {
                OB_LOG(WARN, "fail to int value", K(int_value), K(ret));
              }
            } else if (ObUInt64Type == data_type) {
              if (OB_FAIL(result->get_uint("value", uint_value))) {
                OB_LOG(WARN, "fail to uint value", K(ret));
              } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " = %lu", uint_value))) {
                OB_LOG(WARN, "fail to uint value", K(uint_value), K(ret));
              }
            } else if (ObVarcharType == data_type) {
              if (OB_FAIL(result->get_varchar("value", string_value))) {
                OB_LOG(WARN, "fail to string value", K(ret));
              } else if (OB_FAIL(databuff_printf(
                             buf, buf_len, pos, " = '%.*s'", string_value.length(), string_value.ptr()))) {
                OB_LOG(WARN, "fail to string value", K(string_value), K(ret));
              }
            } else if (ObNumberType == data_type) {
              if (OB_FAIL(result->get_varchar("value", string_value))) {
                OB_LOG(WARN, "fail to string value", K(ret));
              } else if (OB_FAIL(databuff_printf(
                             buf, buf_len, pos, " = %.*s", string_value.length(), string_value.ptr()))) {
                OB_LOG(WARN, "fail to string value", K(string_value), K(ret));
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              OB_LOG(WARN, "this variables type is not support", K(ret), K(data_type));
            }
          }  // else
        }
      }
    }  // while
  }
  return ret;
}

int ObSchemaPrinter::print_foreign_key_definition(
    const ObForeignKeyInfo& foreign_key_info, char* buf, int64_t buf_len, int64_t& pos) const
{
  /*
    mysql
    ALTER TABLE tbl_name ADD [CONSTRAINT [symbol]] FOREIGN KEY
    [index_name] (col_name,...) reference_definition

    oracle
    ALTER TABLE tbl_name ADD [CONSTRAINT symbol]  FOREIGN KEY
    (col_name,...) references_clause

    =>

    ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s...) REFERENCES %s(%s...) %s...
   */
  int ret = OB_SUCCESS;
  bool is_oracle_mode = share::is_oracle_mode();
  const ObDatabaseSchema* child_database_schema = NULL;
  const ObDatabaseSchema* parent_database_schema = NULL;
  const ObTableSchema* child_table_schema = NULL;
  const ObTableSchema* parent_table_schema = NULL;
  ObString foreign_key_name = foreign_key_info.foreign_key_name_;
  ObString child_database_name;
  ObString parent_database_name;
  ObString child_table_name;
  ObString parent_table_name;
  ObString child_column_name;
  ObString parent_column_name;
  const char* update_action_str = NULL;
  const char* delete_action_str = NULL;
  OZ(schema_guard_.get_table_schema(foreign_key_info.child_table_id_, child_table_schema));
  OZ(schema_guard_.get_table_schema(foreign_key_info.parent_table_id_, parent_table_schema));
  OV(OB_NOT_NULL(child_table_schema), OB_TABLE_NOT_EXIST, foreign_key_info.child_table_id_);
  OV(OB_NOT_NULL(parent_table_schema), OB_TABLE_NOT_EXIST, foreign_key_info.parent_table_id_);
  OZ(schema_guard_.get_database_schema(child_table_schema->get_database_id(), child_database_schema));
  OZ(schema_guard_.get_database_schema(parent_table_schema->get_database_id(), parent_database_schema));
  OV(OB_NOT_NULL(child_database_schema), OB_ERR_BAD_DATABASE, child_table_schema->get_database_id());
  OV(OB_NOT_NULL(parent_database_schema), OB_ERR_BAD_DATABASE, parent_table_schema->get_database_id());

  OX(child_database_name = child_database_schema->get_database_name_str());
  OX(parent_database_name = parent_database_schema->get_database_name_str());
  OX(child_table_name = child_table_schema->get_table_name_str());
  OX(parent_table_name = parent_table_schema->get_table_name_str());
  OV(!child_database_name.empty());
  OV(!parent_database_name.empty());
  OV(!child_table_name.empty());
  OV(!parent_table_name.empty());

  OX(BUF_PRINTF(
      is_oracle_mode ? "ALTER TABLE \"%.*s\".\"%.*s\" ADD CONSTRAINT " : "ALTER TABLE `%.*s`.`%.*s` ADD CONSTRAINT ",
      child_database_name.length(),
      child_database_name.ptr(),
      child_table_name.length(),
      child_table_name.ptr()));
  if (!foreign_key_name.empty()) {
    OX(BUF_PRINTF(is_oracle_mode ? "\"%.*s\" " : "`%.*s` ", foreign_key_name.length(), foreign_key_name.ptr()));
  }
  OX(BUF_PRINTF("FOREIGN KEY ("));
  OX(print_column_list(*child_table_schema, foreign_key_info.child_column_ids_, buf, buf_len, pos));
  OX(BUF_PRINTF(is_oracle_mode ? ") REFERENCES \"%.*s\".\"%.*s\" (" : ") REFERENCES `%.*s`.`%.*s` (",
      parent_database_name.length(),
      parent_database_name.ptr(),
      parent_table_name.length(),
      parent_table_name.ptr()));
  OX(print_column_list(*parent_table_schema, foreign_key_info.parent_column_ids_, buf, buf_len, pos));
  OX(BUF_PRINTF(") "));
  if (!is_oracle_mode && foreign_key_info.update_action_ != ACTION_RESTRICT &&
      foreign_key_info.update_action_ != ACTION_NO_ACTION) {
    OX(update_action_str = foreign_key_info.get_update_action_str());
    OV(OB_NOT_NULL(update_action_str), OB_ERR_UNEXPECTED, foreign_key_info.update_action_);
    OX(BUF_PRINTF("ON UPDATE %s ", update_action_str));
  }
  if (foreign_key_info.delete_action_ != ACTION_RESTRICT && foreign_key_info.delete_action_ != ACTION_NO_ACTION) {
    OX(delete_action_str = foreign_key_info.get_delete_action_str());
    OV(OB_NOT_NULL(delete_action_str), OB_ERR_UNEXPECTED, foreign_key_info.delete_action_);
    OX(BUF_PRINTF("ON DELETE %s", delete_action_str));
  }
  return ret;
}

int ObSchemaPrinter::deep_copy_obj(ObIAllocator& allocator, const ObObj& src, ObObj& dest) const
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t pos = 0;
  int64_t size = src.get_deep_copy_size();

  if (size > 0) {
    if (NULL == (buf = static_cast<char*>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Fail to allocate memory, ", K(size), K(ret));
    } else if (OB_FAIL(dest.deep_copy(src, buf, size, pos))) {
      LOG_WARN("Fail to deep copy obj, ", K(ret));
    }
  } else {
    dest = src;
  }

  return ret;
}

int ObSchemaPrinter::get_sequence_value(common::ObMySQLProxy& sql_proxy, const uint64_t tenant_id,
    const uint64_t table_id, const uint64_t column_id, uint64_t& seq_value)
{
  int ret = OB_SUCCESS;
  int sql_len = 0;
  HEAP_VAR(char[OB_MAX_SQL_LENGTH], sql)
  {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult* result = NULL;
      ObISQLClient* sql_client = &sql_proxy;
      ObSQLClientRetryWeak sql_client_retry_weak(sql_client, tenant_id, OB_ALL_SEQUENCE_V2_TID);
      sql_len = snprintf(sql,
          OB_MAX_SQL_LENGTH,
          " SELECT sequence_value, sync_value FROM %s"
          " WHERE tenant_id = %lu AND sequence_key = %lu AND column_id = %lu",
          OB_ALL_SEQUENCE_V2_TNAME,
          ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
          ObSchemaUtils::get_extract_schema_id(tenant_id, table_id),
          column_id);

      if (sql_len >= OB_MAX_SQL_LENGTH || sql_len <= 0) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("failed to format sql. size not enough");
      } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql))) {
        LOG_WARN(" failed to read data", K(ret));
      } else if (NULL == (result = res.get_result())) {
        LOG_WARN("failed to get result", K(ret));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
        LOG_INFO("there is no sequence record", K(ret));
        seq_value = 0;
        ret = OB_SUCCESS;
      } else if (OB_FAIL(result->get_uint(0l, seq_value))) {
        LOG_WARN("fail to get int_value.", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_sequence_definition(
    const ObSequenceSchema& sequence_schema, char* buf, const int64_t& buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  const ObString& seq_name = sequence_schema.get_sequence_name();
  const number::ObNumber& min_value = sequence_schema.get_min_value();
  const number::ObNumber& max_value = sequence_schema.get_max_value();
  const number::ObNumber& start_with = sequence_schema.get_start_with();
  const number::ObNumber& inc = sequence_schema.get_increment_by();
  const number::ObNumber& cache = sequence_schema.get_cache_size();
  bool cycle = sequence_schema.get_cycle_flag();
  bool order = sequence_schema.get_order_flag();

  OX(BUF_PRINTF("CREATE SEQUENCE \"%.*s\" minvalue ", seq_name.length(), seq_name.ptr()));
  OX(min_value.format(buf, buf_len, pos, 0));
  OX(BUF_PRINTF(" maxvalue "));
  OX(max_value.format(buf, buf_len, pos, 0));
  OX(BUF_PRINTF(" increment by "));
  OX(inc.format(buf, buf_len, pos, 0));
  OX(BUF_PRINTF(" start with "));
  OX(start_with.format(buf, buf_len, pos, 0));

  const int64_t cache_size_of_nocache = 1;
  if (cache.is_equal(cache_size_of_nocache)) {
    OX(BUF_PRINTF(" nocache"));
  } else {
    OX(BUF_PRINTF(" cache "));
    OX(cache.format(buf, buf_len, pos, 0));
  }
  OX(BUF_PRINTF(order ? " order " : " noorder "));
  OX(BUF_PRINTF(cycle ? "cycle" : "nocycle"));

  return ret;
}

// print pk/check constraint definition
int ObSchemaPrinter::print_constraint_definition(const ObDatabaseSchema& db_schema, const ObTableSchema& table_schema,
    uint64_t constraint_id, char* buf, const int64_t& buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = lib::is_oracle_mode();
  const ObConstraint* cst = NULL;
  ObArenaAllocator allocator("PrintConsDef");
  ObTableSchema::const_constraint_iterator it_begin = table_schema.constraint_begin();
  ObTableSchema::const_constraint_iterator it_end = table_schema.constraint_end();
  const ObString& db_name = db_schema.get_database_name_str();
  const ObString& tb_name = table_schema.get_table_name_str();
  for (ObTableSchema::const_constraint_iterator it = it_begin; OB_SUCC(ret) && it != it_end; it++) {
    const ObConstraint* tmp_cst = *it;
    if (constraint_id == tmp_cst->get_constraint_id()) {
      cst = tmp_cst;
      break;
    }
  }
  ObString cst_name;
  if (OB_ISNULL(cst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("constraint not found in table schema", K(ret), K(constraint_id));
  } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                 allocator, cst->get_constraint_name_str(), cst_name, is_oracle_mode))) {
    LOG_WARN("generate new name with escape name str failed", K(ret));
  } else {
    const ObString& cst_expr = cst->get_check_expr_str();
    OX(BUF_PRINTF(is_oracle_mode ? "ALTER TABLE \"%.*s\".\"%.*s\" ADD CONSTRAINT \"%.*s\" "
                                 : "ALTER TABLE `%.*s`.`%.*s` ADD CONSTRAINT `%.*s` ",
        db_name.length(),
        db_name.ptr(),
        tb_name.length(),
        tb_name.ptr(),
        cst_name.length(),
        cst_name.ptr()));
    switch (cst->get_constraint_type()) {
      case CONSTRAINT_TYPE_PRIMARY_KEY:
        OX(BUF_PRINTF("PRIMARY KEY ("));
        OZ(print_rowkey_info(table_schema.get_rowkey_info(), table_schema.get_table_id(), buf, buf_len, pos));
        OX(BUF_PRINTF(")"));
        break;
      case CONSTRAINT_TYPE_CHECK:
        OX(BUF_PRINTF(is_oracle_mode ? "CHECK %.*s " : "CHECK %.*s ", cst_expr.length(), cst_expr.ptr()));
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "Not supported print constraint type", K(ret), K(cst->get_constraint_type()));
    }
  }
  return ret;
}

int ObSchemaPrinter::print_user_definition(
    uint64_t tenant_id, const ObUserInfo& user_info, char* buf, const int64_t& buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  common::ObArray<const ObUserInfo*> user_schemas;

  const ObString& user_name = user_info.get_user_name_str();
  const ObString& host_name = user_info.get_host_name_str();
  const ObString& user_passwd = user_info.get_passwd_str();
  bool is_oracle_mode = share::is_oracle_mode();

  if (OB_FAIL(databuff_printf(buf,
          buf_len,
          pos,
          is_oracle_mode ? "create user \"%.*s\" " : "create user if not exists `%.*s` ",
          user_name.length(),
          user_name.ptr()))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print user name", K(user_name), K(ret));
  } else if (host_name.compare(OB_DEFAULT_HOST_NAME) != 0) {
    if (OB_FAIL(databuff_printf(
            buf, buf_len, pos, is_oracle_mode ? "@\"%.*s\" " : "@`%.*s` ", host_name.length(), host_name.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print host name", K(host_name), K(ret));
    }
  }

  if (OB_SUCC(ret) && user_passwd.length() > 0) {
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            is_oracle_mode ? "IDENTIFIED BY VALUES \"%.*s\" " : "IDENTIFIED BY PASSWORD '%.*s' ",
            user_passwd.length(),
            user_passwd.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print user passwd", K(user_name), K(user_passwd), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const ObProfileSchema* profile_schema = NULL;
    if (OB_INVALID_ID == user_info.get_profile_id()) {
    } else if (OB_FAIL(schema_guard_.get_profile_schema_by_id(tenant_id, user_info.get_profile_id(), profile_schema))) {
      if (OB_OBJECT_NAME_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        SHARE_SCHEMA_LOG(WARN, "get profile schena failed", K(ret));
      }
    } else {
      if (OB_FAIL(databuff_printf(buf,
              buf_len,
              pos,
              is_oracle_mode ? "PROFILE \"%.*s\" " : "PROFILE `%.*s` ",
              profile_schema->get_profile_name_str().length(),
              profile_schema->get_profile_name_str().ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print profile", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    switch (user_info.get_ssl_type()) {
      case ObSSLType::SSL_TYPE_NOT_SPECIFIED: {
        break;
      }
      case ObSSLType::SSL_TYPE_NONE: {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, " REQUIRE NONE "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print ssl info", K(user_name), K(ret));
        }
        break;
      }
      case ObSSLType::SSL_TYPE_ANY: {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, " REQUIRE SSL "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print ssl info", K(user_name), K(ret));
        }
        break;
      }
      case ObSSLType::SSL_TYPE_X509: {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, " REQUIRE X509 "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print ssl info", K(user_name), K(ret));
        }
        break;
      }
      case ObSSLType::SSL_TYPE_SPECIFIED: {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, " REQUIRE "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print ssl info", K(user_name), K(ret));
        } else if (!user_info.get_ssl_cipher_str().empty() &&
                   OB_FAIL(databuff_printf(buf, buf_len, pos, "CIPHER '%s' ", user_info.get_ssl_cipher()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print ssl info", K(user_name), K(ret));
        } else if (!user_info.get_x509_issuer_str().empty() &&
                   OB_FAIL(databuff_printf(buf, buf_len, pos, "ISSUER '%s' ", user_info.get_x509_issuer()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print ssl info", K(user_name), K(ret));
        } else if (!user_info.get_x509_subject_str().empty() &&
                   OB_FAIL(databuff_printf(buf, buf_len, pos, "SUBJECT '%s' ", user_info.get_x509_subject()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print ssl info", K(user_name), K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "unknown ssl type", K(user_info.get_ssl_type()), K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_hash_partition_elements(const ObPartitionSchema*& schema, char* buf, const int64_t& buf_len,
    int64_t& pos, bool agent_mode, const common::ObTimeZoneInfo* tz_info) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema is null", K(ret));
  } else {
    ObPartition** part_array = schema->get_part_array();
    if (OB_ISNULL(part_array)) {
      if (is_virtual_table(schema->get_table_id())) {
        // virtual table
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, " partitions %ld\n", schema->get_first_part_num()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to printf partition number", K(ret), K(schema->get_first_part_num()));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "partition_array is NULL", K(ret));
      }
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n("))) {
      SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
    } else {
      int64_t part_num = schema->get_first_part_num();
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
        const ObPartition* partition = part_array[i];
        if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "partition is NULL", K(ret), K(part_num));
        } else {
          const ObString& part_name = partition->get_part_name();
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, "partition %.*s", part_name.length(), part_name.ptr()))) {
            SHARE_SCHEMA_LOG(WARN, "print partition name failed", K(ret), K(part_name));
          } else if (agent_mode &&
                     OB_FAIL(databuff_printf(buf, buf_len, pos, " id %ld", partition->get_part_id()))) {  // print id
            SHARE_SCHEMA_LOG(WARN, "print part_id failed", K(ret));
          } else if (!schema->is_sub_part_template() &&
                     OB_FAIL(print_individual_sub_partition_elements(schema, partition, buf, buf_len, pos, tz_info))) {
            SHARE_SCHEMA_LOG(WARN, "failed to print individual sub partition elements", K(ret));
          } else if (part_num - 1 != i && OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n"))) {
            SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
          } else if (part_num - 1 == i && OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
            SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

}  // end namespace schema
}  // end of namespace share
}  // end namespace oceanbase
