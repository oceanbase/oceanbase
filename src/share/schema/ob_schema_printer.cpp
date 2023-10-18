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
#include "sql/resolver/cmd/ob_load_data_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_printer.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/parser/ob_parser.h"
#include "observer/ob_sql_client_decorator.h"
#include "pl/parser/ob_pl_parser.h"
#include "pl/ob_pl_resolver.h"
#include "sql/ob_sql_utils.h"
#include "share/ob_get_compat_mode.h"



namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace std;
using namespace common;

/*-----------------------------------------------------------------------------
 *  ObSchemaPrinter
 *-----------------------------------------------------------------------------*/
ObSchemaPrinter::ObSchemaPrinter(ObSchemaGetterGuard &schema_guard,
                                 bool strict_compat,
                                 bool sql_quote_show_create,
                                 bool ansi_quotes)
    : schema_guard_(schema_guard),
      strict_compat_(strict_compat),
      sql_quote_show_create_(sql_quote_show_create),
      ansi_quotes_(ansi_quotes)
{
}

int ObSchemaPrinter::print_table_definition(const uint64_t tenant_id,
                                          const uint64_t table_id,
                                          char* buf,
                                          const int64_t& buf_len,
                                          int64_t& pos,
                                          const ObTimeZoneInfo *tz_info,
                                          const common::ObLengthSemantics default_length_semantics,
                                          bool agent_mode,
                                          ObSQLMode sql_mode,
                                          ObCharsetType charset_type) const
{
  //TODO(yaoying.yyy: refactor this function):consider index_position in

  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  const ObDatabaseSchema *db_schema = NULL;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  const char* prefix_arr[4] = {"", " TEMPORARY", " GLOBAL TEMPORARY", " EXTERNAL"};
  int prefix_idx = 0;
  bool is_oracle_mode = false;
  if (OB_FAIL(schema_guard_.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id));
  } else if (NULL == table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    SHARE_SCHEMA_LOG(WARN, "Unknow table", K(ret), K(table_id));
  } else if (OB_FAIL(table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle compat mode", KR(ret), K(table_id));
  } else if (OB_FAIL(schema_guard_.get_database_schema(tenant_id, table_schema->get_database_id(), db_schema))) {
    SHARE_SCHEMA_LOG(WARN, "fail to get database schema", K(ret), K(tenant_id));
  } else if (NULL == db_schema) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "database not exist", K(ret));
  } else {
    if (table_schema->is_mysql_tmp_table()) {
      prefix_idx = 1;
    } else if (table_schema->is_oracle_tmp_table()) {
      prefix_idx = 2;
    } else if (table_schema->is_external_table()) {
      prefix_idx = 3;
    } else {
      prefix_idx = 0;
    }
    ObString new_table_name;
    ObString new_db_name;
    if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(allocator,
                                                                table_schema->get_table_name_str(),
                                                                new_table_name,
                                                                is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(table_schema->get_table_name()));
    } else if (agent_mode && OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                                     allocator, db_schema->get_database_name_str(),
                                     new_db_name, is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(db_schema->get_database_name_str()));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "CREATE%s TABLE ", prefix_arr[prefix_idx]))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print create table prefix", K(ret));
    } else if (agent_mode &&
               OB_FAIL(print_identifier(buf, buf_len, pos, new_db_name, is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print create table prefix identifier", K(ret));
    } else if (agent_mode &&
               OB_FAIL(databuff_printf(buf, buf_len, pos, "."))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print create table prefix", K(ret));
    } else if (OB_FAIL(print_identifier(buf, buf_len, pos, new_table_name, is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print create table prefix identifier", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " (\n"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print create table prefix", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(print_table_definition_columns(*table_schema, buf, buf_len, pos, tz_info, default_length_semantics, agent_mode, sql_mode, charset_type))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print columns", K(ret), K(*table_schema));
    } else if (OB_FAIL(print_table_definition_rowkeys(*table_schema, buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print rowkeys", K(ret), K(*table_schema));
    } else if (!agent_mode && OB_FAIL(print_table_definition_foreign_keys(*table_schema, buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print foreign keys", K(ret), K(*table_schema));
    } else if (!agent_mode && OB_FAIL(print_table_definition_indexes(*table_schema, buf, buf_len, pos, true, sql_mode, tz_info))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print indexes", K(ret), K(*table_schema));
    } else if (!agent_mode && lib::is_mysql_mode()
               && OB_FAIL(print_table_definition_indexes(*table_schema, buf, buf_len, pos, false, sql_mode, tz_info))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print indexes", K(ret), K(*table_schema));
    } else if (OB_FAIL(print_table_definition_constraints(*table_schema, buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print constraints", K(ret), K(*table_schema));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n) "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print )", K(ret));
    } else if (table_schema->is_external_table() && (OB_FAIL(print_external_table_file_info(*table_schema, allocator, buf, buf_len, pos)))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print external table file format", K(ret));
    } else if (OB_FAIL(print_table_definition_table_options(*table_schema, buf, buf_len, pos, false, agent_mode, sql_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print table options", K(ret), K(*table_schema));
    } else if (OB_FAIL(print_table_definition_partition_options(*table_schema, buf, buf_len, pos, agent_mode, tz_info))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print partition options", K(ret), K(*table_schema));
    } else if (OB_FAIL(print_table_definition_on_commit_options(*table_schema, buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print on commit options", K(ret), K(*table_schema));
    }
    SHARE_SCHEMA_LOG(DEBUG, "print table schema", K(ret), K(*table_schema));
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_columns(const ObTableSchema &table_schema,
                                                    char* buf,
                                                    const int64_t& buf_len,
                                                    int64_t& pos,
                                                    const ObTimeZoneInfo *tz_info,
                                                    const common::ObLengthSemantics default_length_semantics,
                                                    bool is_agent_mode,
                                                    ObSQLMode sql_mode,
                                                    ObCharsetType charset_type) const
{
  int ret = OB_SUCCESS;
  bool is_first_col = true;
  ObColumnIterByPrevNextID iter(table_schema);
  const ObColumnSchemaV2 *col = NULL;
  bool is_oracle_mode = false;
  if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_schema));
  }
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  hash::ObHashMap<uint64_t, uint64_t> not_null_cst_map;
  if (is_oracle_mode) {
    if (OB_FAIL(not_null_cst_map.create(table_schema.get_column_count(), ObModIds::OB_SCHEMA))) {
      LOG_WARN("create hash map failed", K(ret));
    } else if (OB_FAIL(table_schema.get_not_null_constraint_map(not_null_cst_map))) {
      LOG_WARN("get not null constraint map failed", K(ret));
    }
  }
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
                   allocator,
                   col->get_column_name_str(),
                   new_col_name,
                   is_oracle_mode))) {
          SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(col->get_column_name_str()));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "  "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print column", K(ret), K(*col));
        } else if (OB_FAIL(print_identifier(buf, buf_len, pos, new_col_name, is_oracle_mode))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print column", K(ret), K(*col));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print column", K(ret), K(*col));
        }

        if (OB_SUCC(ret)) {
          int64_t start = pos;
          const uint64_t sub_type = col->is_xmltype() ?
                                    col->get_sub_data_type() : static_cast<uint64_t>(col->get_geo_type());
          if (OB_FAIL(ob_sql_type_str(col->get_meta_type(),
                                      col->get_accuracy(),
                                      col->get_extended_type_info(),
                                      default_length_semantics,
                                      buf, buf_len, pos, sub_type))) {
            SHARE_SCHEMA_LOG(WARN, "fail to get data type str", K(col->get_data_type()), K(*col), K(ret));
          } else if (is_oracle_mode) {
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
        } else if (ob_is_string_type(col->get_data_type())
                   || ob_is_enum_or_set_type(col->get_data_type())) {
          if (col->get_charset_type() == CHARSET_BINARY) {
            // nothing to do
          } else {
            if (col->get_charset_type() == table_schema.get_charset_type()
                && col->get_collation_type() == table_schema.get_collation_type()) {
              //nothing to do
            } else {
              if (CHARSET_INVALID != col->get_charset_type()
                  && CHARSET_BINARY != col->get_charset_type()) {
                if (is_oracle_mode) {
                  //do not print charset type when in oracle mode
                } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " CHARACTER SET %s",
                      ObCharset::charset_name(col->get_charset_type())))) {
                  SHARE_SCHEMA_LOG(WARN, "fail to print character set", K(ret), K(*col));
                }
              }
            }
            if (OB_SUCCESS == ret
                && !is_agent_mode
                && !is_oracle_mode
                && !ObCharset::is_default_collation(col->get_collation_type())
                && CS_TYPE_INVALID != col->get_collation_type()
                && CS_TYPE_BINARY != col->get_collation_type()) {
              if (OB_FAIL(databuff_printf(buf, buf_len, pos, " COLLATE %s",
                      ObCharset::collation_name(col->get_collation_type())))) {
                SHARE_SCHEMA_LOG(WARN, "fail to print collate", K(ret), K(*col));
              }
            }
          }
        }
        // for visibility in oracle mode
        if (OB_SUCC(ret) && is_oracle_mode && col->is_invisible_column()) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " INVISIBLE"))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print INVISIBLE", K(ret), K(*col));
          }
        }
        if (OB_SUCC(ret) && col->is_generated_column()) {
          lib::Worker::CompatMode compat_mode = (is_oracle_mode ?
            lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL);
          lib::CompatModeGuard tmpCompatModeGuard(compat_mode);
          if (OB_FAIL(print_generated_column_definition(*col, buf, buf_len, table_schema, pos))) {
            SHARE_SCHEMA_LOG(WARN, "print generated column definition fail", K(ret));
          }
        }
        if (OB_SUCC(ret) && col->is_identity_column()) {
          if (OB_FAIL(print_identity_column_definition(*col, buf, buf_len, table_schema, pos))) {
            SHARE_SCHEMA_LOG(WARN, "print identity column definition fail", K(ret));
          }
        }
        // mysql mode
        if (OB_SUCC(ret) && !is_oracle_mode) {
          if (!col->is_nullable()) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " NOT NULL"))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print NOT NULL", K(ret));
            }
          } else if (ObTimestampType == col->get_data_type()) {
            //only timestamp need to print null;
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " NULL"))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print NULL", K(ret));
            }
          }
        }
        // adapt mysql geometry column format:`g` linestring NOT NULL /*!80003 SRID 4326 */
        if (OB_SUCC(ret) && !is_oracle_mode && ob_is_geometry(col->get_data_type())) {
          uint32_t srid = col->get_srid();
          if (!col->is_default_srid()
              && OB_FAIL(databuff_printf(buf, buf_len, pos, " /*!80003 SRID %u */", srid))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print geometry srid", K(ret), K(srid));
          }
        }
        if (OB_SUCC(ret) && !is_oracle_mode && !col->is_generated_column()) {
          //if column is not permit null and default value does not specify , don't  display DEFAULT NULL
          if (OB_SUCC(ret)) {
            if (!(ObNullType == col->get_cur_default_value().get_type() && !col->is_nullable())
                && !col->is_autoincrement()) {
              if (IS_DEFAULT_NOW_OBJ(col->get_cur_default_value())) {
                int16_t scale = col->get_data_scale();
                if (0 == scale) {
                  if (OB_FAIL(databuff_printf(buf, buf_len, pos, " DEFAULT %s", N_UPPERCASE_CUR_TIMESTAMP))) {
                    SHARE_SCHEMA_LOG(WARN, "fail to print DEFAULT now()", K(ret));
                  }
                } else {
                  if (OB_FAIL(databuff_printf(buf, buf_len, pos, " DEFAULT %s(%d)", N_UPPERCASE_CUR_TIMESTAMP, scale))) {
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
                    if (OB_FAIL(default_value.print_varchar_literal(col->get_extended_type_info(), buf, buf_len, pos))) {
                      SHARE_SCHEMA_LOG(WARN, "fail to print sql literal", KPC(col), K(buf), K(buf_len), K(pos), K(ret));
                    }
                  } else if (ob_is_string_tc(default_value.get_type())) {
                    ObCollationType collation_type = ObCharset::get_default_collation(charset_type);
                    ObString out_str = default_value.get_string();
                    if (OB_FAIL(ObCharset::charset_convert(allocator, default_value.get_string(), default_value.get_collation_type(), collation_type, out_str))) {
                      SHARE_SCHEMA_LOG(WARN, "fail to convert charset", K(ret));
                    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "'%s'", to_cstring(ObHexEscapeSqlStr(out_str))))) {
                      SHARE_SCHEMA_LOG(WARN, "fail to print default value of string tc", K(ret));
                    }
                  } else if (OB_FAIL(default_value.print_varchar_literal(buf, buf_len, pos, tz_info))) {
                    SHARE_SCHEMA_LOG(WARN, "fail to print sql literal", K(ret));
                  }
                }
              }
            }
          }
          if (OB_SUCC(ret) && col->is_on_update_current_timestamp() && !is_no_field_options(sql_mode)) {
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
          if (OB_SUCC(ret) && col->is_autoincrement() && !is_no_field_options(sql_mode)) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " AUTO_INCREMENT"))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print auto_increment", K(ret));
            }
          }
        }
        // oracle mode
        if (OB_SUCC(ret) && is_oracle_mode) {
          //if column is not permit null and default value does not specify , don't  display DEFAULT NULL
          if (!col->is_generated_column()
              && !(ObNullType == col->get_cur_default_value().get_type() && col->has_not_null_constraint())
              && col->is_default_expr_v2_column()) {
            ObString default_value = col->get_cur_default_value().get_string();
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " DEFAULT %.*s", default_value.length(), default_value.ptr()))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print sql literal", K(default_value), K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            const ObConstraint *cst = NULL;
            if (col->has_not_null_constraint()) {
              uint64_t cst_id = OB_INVALID_ID;
              if (OB_FAIL(not_null_cst_map.get_refactored(col->get_column_id(), cst_id))) {
                if (OB_HASH_NOT_EXIST == ret) {
                  if (OB_FAIL(databuff_printf(buf, buf_len, pos, " NOT NULL"))) {
                    SHARE_SCHEMA_LOG(WARN, "fail to print NOT NULL", K(ret));
                  } else if (print_constraint_stat(col->is_not_null_rely_column(),
                            col->is_not_null_enable_column(), col->is_not_null_validate_column(),
                            buf, buf_len, pos)) {
                    LOG_WARN("print constraint state failed", K(ret));
                  }
                } else {
                  LOG_WARN("get refactored failed", K(ret));
                }
              } else if (OB_ISNULL(cst = table_schema.get_constraint(cst_id))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get constraint failed", K(ret), K(cst_id));
              } else if (is_oracle_mode && cst->is_sys_generated_name(false/*check_unknown*/)) {
                if (OB_FAIL(databuff_printf(buf, buf_len, pos, " NOT NULL"))) {
                  SHARE_SCHEMA_LOG(WARN, "fail to print NOT NULL", K(ret));
                }
              } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " CONSTRAINT \"%.*s\" NOT NULL",
                                                cst->get_constraint_name_str().length(),
                                                cst->get_constraint_name_str().ptr()))) {
                SHARE_SCHEMA_LOG(WARN, "fail to print NOT NULL", K(ret));
              }
              if (OB_SUCC(ret) &&
                  OB_FAIL(print_constraint_stat(col->is_not_null_rely_column(),
                        col->is_not_null_enable_column(), col->is_not_null_validate_column(),
                        buf, buf_len, pos))) {
                LOG_WARN("print constraint state failed", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret) && !is_oracle_mode && 0 < strlen(col->get_comment())) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " COMMENT '%s'", to_cstring(ObHexEscapeSqlStr(col->get_comment_str()))))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print comment", K(ret));
          }
        }
        if (OB_SUCC(ret) && is_agent_mode) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " ID %lu", col->get_column_id()))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print column id", K(ret));
          }
        }
      }
      //TODO:add print extended_type_info
    }
  }
  if (ret != OB_ITER_END) {
    LOG_WARN("Failed to iterate all table columns. iter quit. ", K(ret));
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSchemaPrinter::print_generated_column_definition(const ObColumnSchemaV2 &gen_col,
                                                       char *buf,
                                                       int64_t buf_len,
                                                       const ObTableSchema &table_schema,
                                                       int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObString expr_str;
  ObArenaAllocator allocator("SchemaPrinter");
  sql::ObRawExprFactory expr_factory(allocator);
  sql::ObRawExpr *expr = NULL;
  ObTimeZoneInfo tz_infos;
  sql::ObRawExprPrinter raw_printer(buf, buf_len, &pos, &schema_guard_, &tz_infos);
  SMART_VAR(sql::ObSQLSessionInfo, session) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " GENERATED ALWAYS AS ("))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print keywords", K(ret));
    } else if (OB_FAIL(gen_col.get_cur_default_value().get_string(expr_str))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print sql literal", K(ret));
    } else if (OB_FAIL(session.init(0 /*default session id*/,
                                    0 /*default proxy id*/,
                                    &allocator))) {
      SHARE_SCHEMA_LOG(WARN, "fail to init session", K(ret));
    } else if (OB_FAIL(session.load_default_sys_variable(false, false))) {
      SHARE_SCHEMA_LOG(WARN, "session load default system variable failed", K(ret));
      /* bug:
        构建ObRawExpr对象,当 expr_str = "CONCAT(first_name,' ',last_name)"
        避免错误的打印成： CONCAT(first_name,\' \',last_name) */
    } else if(OB_FAIL(sql::ObRawExprUtils::build_generated_column_expr(NULL,
                                                                       expr_str,
                                                                       expr_factory,
                                                                       session,
                                                                       table_schema,
                                                                       expr))) {
      SHARE_SCHEMA_LOG(WARN, "fail to generated column expr", K(ret));
    } else if (OB_FAIL(raw_printer.do_print(expr, sql::T_NONE_SCOPE))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print expr string", K(ret));
    } else if ((OB_FAIL(databuff_printf(buf, buf_len, pos, ")")))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print )", K(ret));
    } else if (!table_schema.is_external_table()
               && OB_FAIL(databuff_printf(buf, buf_len, pos, gen_col.is_virtual_generated_column() ?
                                          " VIRTUAL" : " STORED"))) {
      SHARE_SCHEMA_LOG(WARN, "print virtual keyword failed", K(ret));
    }
  }
  return ret;
}

int ObSchemaPrinter::print_identity_column_definition(const ObColumnSchemaV2 &iden_col,
                                                      char *buf,
                                                      int64_t buf_len,
                                                      const ObTableSchema &table_schema,
                                                      int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("SchemaPrinter");
  const ObSequenceSchema *sequence_schema = NULL;

  SMART_VAR(sql::ObSQLSessionInfo, session) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " GENERATED "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print keywords", K(ret));
    } else if (iden_col.is_always_identity_column()
               && OB_FAIL(databuff_printf(buf, buf_len, pos, "ALWAYS"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print keywords", K(ret));
    } else if (iden_col.is_default_identity_column()
               && OB_FAIL(databuff_printf(buf, buf_len, pos, "BY DEFAULT"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print keywords", K(ret));
    } else if (iden_col.is_default_on_null_identity_column()
               && OB_FAIL(databuff_printf(buf, buf_len, pos, "BY DEFAULT ON NULL"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print keywords", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " AS IDENTITY"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print keywords", K(ret));
    } else {
      const ObSequenceSchema *sequence_schema = NULL;
      const ObDatabaseSchema *database_schema = NULL;
      if (OB_FAIL(schema_guard_.get_sequence_schema(table_schema.get_tenant_id(),
                                                    iden_col.get_sequence_id(),
                                                    sequence_schema))) {
        SHARE_SCHEMA_LOG(WARN, "get sequence schema failed", K(table_schema.get_tenant_id()),
                                                             K(iden_col.get_sequence_id()),
                                                             K(ret));
      } else if (OB_ISNULL(sequence_schema)) {
        ret = OB_NOT_INIT;
        SHARE_SCHEMA_LOG(WARN, "sequence not found", K(ret));
      } else if (OB_FAIL(print_sequence_definition(*sequence_schema, buf, buf_len, pos, false))) {
        SHARE_SCHEMA_LOG(WARN, "Generate sequence definition fail", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_single_index_definition(const ObTableSchema *index_schema,
    const ObTableSchema &table_schema, ObIAllocator &allocator, char* buf, const int64_t& buf_len,
    int64_t& pos, const bool is_unique_index, const bool is_oracle_mode,
    const bool is_alter_table_add, ObSQLMode sql_mode, const ObTimeZoneInfo *tz_info) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "index is not exist", K(ret));
  } else {
    ObString index_name;
    ObString new_index_name;
    //  get the original short index name
    if (OB_FAIL(ObTableSchema::get_index_name(allocator, table_schema.get_table_id(),
                ObString::make_string(index_schema->get_table_name()), index_name))) {
      SHARE_SCHEMA_LOG(WARN, "get index table name failed");
    } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(allocator,
               index_name, new_index_name, is_oracle_mode))) {
     SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(index_name));
    } else if ((is_unique_index && index_schema->is_unique_index()) ||
        (!is_unique_index && !index_schema->is_unique_index())) {
      if (!is_alter_table_add && OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n "))) {
        // !is_alter_table_add for show create table
        // is_alter_table_add for dbms_metadata.get_ddl getting uk cst info
        SHARE_SCHEMA_LOG(WARN, "fail to print comma", K(ret));
      } else if (index_schema->is_unique_index()) {
        if (is_oracle_mode) {
          if (index_schema->is_sys_generated_name(false/*check_unknown*/)) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " UNIQUE "))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print UNIQUE KEY", K(ret));
            }
          } else {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " CONSTRAINT \"%.*s\" UNIQUE ",
                                        new_index_name.length(), new_index_name.ptr()))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print UNIQUE KEY", K(ret));
            }
          }
        } else {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " UNIQUE KEY "))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print UNIQUE KEY", K(ret));
          }
        }
      } else if (index_schema->is_domain_index()) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, " FULLTEXT KEY "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print FULLTEXT KEY", K(ret));
        }
      } else if (index_schema->is_spatial_index()) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " SPATIAL KEY "))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print SPATIAL KEY", K(ret));
          }
      } else {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, " KEY "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print KEY", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        //do nothing
      } else if (!is_oracle_mode &&
                 OB_FAIL(print_identifier(buf, buf_len, pos, new_index_name, is_oracle_mode))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print index name", K(ret), K(index_name));
      } else if (!is_oracle_mode &&
                 OB_FAIL(databuff_printf(buf, buf_len, pos, " "))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print index name", K(ret), K(index_name));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "("))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print index name", K(ret), K(index_name));
      } else {
        // index columns contain rowkeys of base table, but no need to show them.
        const int64_t index_column_num = index_schema->get_index_column_number();
        const ObRowkeyInfo &index_rowkey_info = index_schema->get_rowkey_info();
        int64_t rowkey_count = index_rowkey_info.get_size();
        ObColumnSchemaV2 last_col;
        bool is_valid_col = false;
        ObArray<ObString> ctxcat_cols;
        for (int64_t k = 0; OB_SUCC(ret) && k < index_column_num; k++) {
          const ObRowkeyColumn *rowkey_column = index_rowkey_info.get_column(k);
          const ObColumnSchemaV2 *col = NULL;
          if (NULL == rowkey_column) {
            ret = OB_SCHEMA_ERROR;
            SHARE_SCHEMA_LOG(WARN, "fail to get rowkey column", K(ret));
          } else if (NULL == (col = schema_guard_.get_column_schema(index_schema->get_tenant_id(),
                                                                    index_schema->get_table_id(),
                                                                    rowkey_column->column_id_))) {
            ret = OB_SCHEMA_ERROR;
            SHARE_SCHEMA_LOG(WARN, "fail to get column schema", K(ret), KPC(index_schema));
          } else if (!col->is_shadow_column()) {
            if (OB_SUCC(ret) && is_valid_col) {
              if (OB_FAIL(print_index_column(table_schema, last_col, ctxcat_cols, false /* not last one */, buf, buf_len, pos))) {
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
          if (OB_FAIL(print_index_column(table_schema, last_col, ctxcat_cols, true /* last column */, buf, buf_len, pos))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(last_col));
          } else if (!strict_compat_ && OB_FAIL(print_table_definition_fulltext_indexs(is_oracle_mode, ctxcat_cols, buf, buf_len, pos))) {
            LOG_WARN("print table definition fulltext indexs failed", K(ret));
          } else { /*do nothing*/ }
        }
        // show storing columns in index
        if (OB_SUCC(ret) && !strict_compat_ && !is_no_key_options(sql_mode)) {
          int64_t column_count = index_schema->get_column_count();
          if (column_count >= rowkey_count) {
            bool first_storing_column = true;
            for (ObTableSchema::const_column_iterator row_col = index_schema->column_begin();
                OB_SUCCESS == ret && NULL != row_col && row_col != index_schema->column_end();
                row_col++) {
              if ((*row_col)->is_user_specified_storing_column()) {
                if (first_storing_column) {
                  if (OB_FAIL(databuff_printf(buf, buf_len, pos, " STORING ("))) {
                    SHARE_SCHEMA_LOG(WARN, "fail to print STORING(", K(ret));
                  }
                  first_storing_column = false;
                }
                if (OB_SUCC(ret)) {
                  if (OB_FAIL(print_identifier(buf, buf_len, pos,
                                               (*row_col)->get_column_name(), is_oracle_mode))) {
                    SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K((*row_col)->get_column_name()));
                  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
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
        } // end of storing columns
        // print index options
        if (OB_SUCC(ret)) {
          if (is_oracle_mode && is_unique_index && index_schema->is_unique_index()) {
            if (INDEX_TYPE_UNIQUE_LOCAL == index_schema->get_index_type()) {
              if (OB_FAIL(databuff_printf(buf, buf_len, pos, " USING INDEX LOCAL"))) {
                SHARE_SCHEMA_LOG(WARN, "fail to print USING INDEX LOCAL", K(ret));
              }
            } else {
              // do nothing
            }
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " "))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print space", K(ret));
          } else if (OB_FAIL(print_table_definition_table_options(*index_schema, buf,
                buf_len, pos, false, false, sql_mode))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print index options", K(ret), K(*index_schema));
          }
        }
        // print index partition info
        if (OB_SUCC(ret) && lib::is_mysql_mode()) {
          if (index_schema->is_global_index_table()
              && OB_FAIL(print_table_definition_partition_options(*index_schema, buf, buf_len, pos, false, tz_info))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print partition info for index", K(ret), KPC(index_schema));
          }
        }
      }
    }
  }

  return ret;
}

int ObSchemaPrinter::print_table_definition_indexes(const ObTableSchema &table_schema,
                                                    char* buf,
                                                    const int64_t& buf_len,
                                                    int64_t& pos,
                                                    bool is_unique_index,
                                                    ObSQLMode sql_mode,
                                                    const ObTimeZoneInfo *tz_info) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_allocator(ObModIds::OB_SCHEMA);
  bool is_oracle_mode = false;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle compat mode", KR(ret), K(table_schema));
  } else if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); i++) {
    const ObTableSchema *index_schema = NULL;
    if (OB_FAIL(schema_guard_.get_table_schema(table_schema.get_tenant_id(),
                simple_index_infos.at(i).table_id_, index_schema))) {
      LOG_WARN("fail to get table schema", K(ret));
    } else if (NULL == index_schema) {
      ret = OB_TABLE_NOT_EXIST;
      SHARE_SCHEMA_LOG(ERROR, "invalid index table id", "index_table_id", simple_index_infos.at(i).table_id_);
    } else if (index_schema->is_in_recyclebin()) {
      continue;
    } else if (is_oracle_mode && is_unique_index && index_schema->is_partitioned_table()
               && INDEX_TYPE_UNIQUE_LOCAL != index_schema->get_index_type()) {
      // In oracle mode, only the non-partitioned unique index can be printed as an unique constraint, other unique indexes will not be printed.
      continue;
    } else if (strict_compat_ && (index_schema->is_global_index_table() ||
                                  index_schema->is_global_local_index_table())) {
      // For strictly compatible with MySQL,
      // Do not print global index.
    } else if (OB_FAIL(print_single_index_definition(index_schema, table_schema, arena_allocator,
                       buf, buf_len, pos, is_unique_index, is_oracle_mode, false, sql_mode, tz_info))) {
      LOG_WARN("print single index definition failed", K(ret));
    }
  }

  return ret;
}

int ObSchemaPrinter::print_constraint_stat(const bool rely_flag,
                                          const bool enable_flag,
                                          const bool validate_flag,
                                          char* buf,
                                          const int64_t buf_len,
                                          int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    // default value is NORELY, so only need to print when value is RELY.
    if (true == rely_flag) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " RELY"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print constraint rely", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    // always print ENBLAE/DISABLE
    if (false == enable_flag) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " DISABLE"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print constraint disable", K(ret));
      }
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " ENABLE"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print constraint enable", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // default validate if enable, default novalidate if disable;
    // only print VALIDATE/NOVALIDATE if not default value
    if (true == enable_flag && false == validate_flag) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " NOVALIDATE"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print constraint novalidate", K(ret));
      }
    } else if (false == enable_flag && true == validate_flag) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " VALIDATE"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print constraint validate", K(ret));
      }
    }
  }
  return ret;
}

// print out-of-line constraint
int ObSchemaPrinter::print_table_definition_constraints(const ObTableSchema &table_schema,
                                                        char* buf,
                                                        const int64_t& buf_len,
                                                        int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObTableSchema::const_constraint_iterator it_begin = table_schema.constraint_begin();
  ObTableSchema::const_constraint_iterator it_end = table_schema.constraint_end();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObString new_cst_name;
  bool is_oracle_mode = false;
  if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_schema));
  }
  for (ObTableSchema::const_constraint_iterator it = it_begin;
       OB_SUCC(ret) && it != it_end; it++) {
    const ObConstraint *cst = *it;
    if (NULL == cst) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(ERROR, "NULL ptr", K(cst));
    } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
               allocator,
               cst->get_constraint_name_str(),
               new_cst_name,
               is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(cst->get_constraint_name_str()));
    } else if (lib::is_mysql_mode() && CONSTRAINT_TYPE_NOT_NULL != cst->get_constraint_type()) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n  CONSTRAINT "))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print constraint", K(ret));
      } else if (OB_FAIL(print_identifier(buf, buf_len, pos, new_cst_name, false))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print constraint", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " CHECK (%.*s)",
                                         cst->get_check_expr_str().length(),
                                         cst->get_check_expr_str().ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print constraint", K(ret));
      }
      if (OB_SUCC(ret) && !cst->get_enable_flag()) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "  NOT ENFORCED"))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print constraint not enforced");
        }
      }
    } else if (is_oracle_mode && CONSTRAINT_TYPE_CHECK == cst->get_constraint_type()) {
      if (cst->is_sys_generated_name(false/*check_unknown*/)) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print", K(ret), K(*cst));
        }
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n  CONSTRAINT "))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print constraint", K(ret), K(*cst));
      } else if (OB_FAIL(print_identifier(buf, buf_len, pos, new_cst_name, true))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print constraint", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " CHECK (%.*s)",
                                         cst->get_check_expr_str().length(),
                                         cst->get_check_expr_str().ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print constraint", K(ret));
      } else if (OB_FAIL(print_constraint_stat(cst->get_rely_flag(), cst->get_enable_flag(),
                                               cst->is_validated(), buf, buf_len, pos))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print constraint stat", K(ret), K(*cst));
      }
    }
  }

  return ret;
}

int ObSchemaPrinter::print_fulltext_index_column(const ObTableSchema &table_schema,
                                                 const ObColumnSchemaV2 &column,
                                                 ObIArray<ObString> &ctxcat_cols,
                                                 bool is_last,
                                                 char *buf,
                                                 int64_t buf_len,
                                                 int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> ctxcat_ids;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  const ObColumnSchemaV2 *table_column = table_schema.get_column_schema(column.get_column_id());
  bool is_oracle_mode = false;
  if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_schema));
  } else if (OB_ISNULL(table_column)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The column schema is NULL, ", K(ret));
  } else if (OB_FAIL(table_column->get_cascaded_column_ids(ctxcat_ids))) {
    STORAGE_LOG(WARN, "Failed to get cascaded column ids", K(ret));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j < ctxcat_ids.count(); ++j) {
      const ObColumnSchemaV2 *ctxcat_column = NULL;
      ObString new_col_name;
      if (OB_ISNULL(ctxcat_column = table_schema.get_column_schema(ctxcat_ids.at(j)))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "The column schema is NULL, ", K(ret));
      } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                 allocator,
                 ctxcat_column->get_column_name_str(),
                 new_col_name,
                 is_oracle_mode))) {
        SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(ctxcat_column->get_column_name_str()));
      } else if (OB_FAIL(print_identifier(buf, buf_len, pos, new_col_name, is_oracle_mode))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(column));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                         is_last && j == ctxcat_ids.count() - 1 ? ")" : ", "))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(column));
      } else if (OB_FAIL(ctxcat_cols.push_back(ctxcat_column->get_column_name_str()))) {
        LOG_WARN("get fulltext index column failed", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_spatial_index_column(const ObTableSchema &table_schema,
                                                const ObColumnSchemaV2 &column,
                                                char *buf,
                                                int64_t buf_len,
                                                int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObObjType type = column.get_meta_type().get_type();

  if (ObVarcharType != type && ObUInt64Type != type) {
    STORAGE_LOG(WARN, "Invalid type for spatial index column", K(ret), K(type));
  } else if (ObVarcharType == column.get_meta_type().get_type()) {
    // do nothing
  } else { // ObUInt64Type, cellid column
    bool is_oracle_mode = false;
    ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
    uint64_t geo_col_id = column.get_geo_col_id();
    const ObColumnSchemaV2 *geo_col = table_schema.get_column_schema(geo_col_id);
    ObString new_col_name;
    if (OB_ISNULL(geo_col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The geo column schema is null, ", K(ret), K(geo_col_id));
    } else if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to get compat mode", KR(ret), K(table_schema));
    } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
        allocator, geo_col->get_column_name_str(), new_col_name, is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character",
        K(ret), K(column.get_column_name_str()));
    } else if (OB_FAIL(print_identifier(buf, buf_len, pos, new_col_name, is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(new_col_name));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(new_col_name));
    }
  }

  return ret;
}

int ObSchemaPrinter::print_prefix_index_column(const ObColumnSchemaV2 &column,
                                               bool is_last,
                                               char *buf,
                                               int64_t buf_len,
                                               int64_t &pos) const
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  const ObString &expr_str = column.get_cur_default_value().is_null() ?
      column.get_orig_default_value().get_string() :
      column.get_cur_default_value().get_string();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  sql::ObRawExprFactory expr_factory(allocator);
  sql::ObRawExpr *expr = NULL;
  ObArray<sql::ObQualifiedName> columns;
  ObString column_name;
  int64_t const_value = 0;
  SMART_VAR(sql::ObSQLSessionInfo, default_session) {
    if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
                column.get_tenant_id(), column.get_table_id(), is_oracle_mode))) {
      LOG_WARN("fail to get compat mode", KR(ret), K(column));
    } else if (OB_FAIL(default_session.test_init(0, 0, 0, &allocator))) {
      LOG_WARN("init empty session failed", K(ret));
    } else if (OB_FAIL(default_session.load_default_sys_variable(false, false))) {
      LOG_WARN("session load default system variable failed", K(ret));
    } else if (OB_FAIL(sql::ObRawExprUtils::build_generated_column_expr(expr_str, expr_factory,
                                                              default_session, expr, columns, NULL,
                                                              false /* allow_sequence */, NULL))) {
      LOG_WARN("get generated column expr failed", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null");
    } else if (3 != expr->get_param_count()) {
      // 前缀索引表达式，有三列
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("It's wrong expr string", K(ret), K(expr->get_param_count()));
    } else if (1 != columns.count()) {
      // 表达式列基于某一列
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("It's wrong expr string", K(ret), K(columns.count()));
    } else {
      column_name = columns.at(0).col_name_;
      ObString new_col_name;
      sql::ObRawExpr *t_expr0= expr->get_param_expr(0);
      sql::ObRawExpr *t_expr1 = expr->get_param_expr(1);
      sql::ObRawExpr *t_expr2 = expr->get_param_expr(2);
      if (OB_ISNULL(t_expr0) || OB_ISNULL(t_expr1) || OB_ISNULL(t_expr2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (T_INT != t_expr2->get_expr_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr type is not int", K(ret));
      } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                  allocator,
                  column_name,
                  new_col_name,
                  is_oracle_mode))) {
        SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(column_name));
      }else {
        const_value = (static_cast<sql::ObConstRawExpr*>(t_expr2))->get_value().get_int();
        if (OB_FAIL(print_identifier(buf, buf_len, pos, new_col_name, is_oracle_mode))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                           is_last ? "(%ld))" : "(%ld), ", const_value))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_ordinary_index_column_expr(const ObColumnSchemaV2 &column,
                                                      bool is_last,
                                                      char *buf,
                                                      int64_t buf_len,
                                                      int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObString expr_def;
  if (OB_FAIL(column.get_cur_default_value().get_string(expr_def))) {
    LOG_WARN("get expr def from current default value failed", K(ret), K(column.get_cur_default_value()));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                     is_last ? "%.*s)" : "%.*s, ",
                                     expr_def.length(),
                                     expr_def.ptr()))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print index column expr", K(ret), K(expr_def));
  }
  return ret;
}

int ObSchemaPrinter::print_index_column(const ObTableSchema &table_schema,
                                        const ObColumnSchemaV2 &column,
                                        ObIArray<ObString> &ctxcat_cols,
                                        bool is_last,
                                        char *buf,
                                        int64_t buf_len,
                                        int64_t &pos) const
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObString new_col_name;
  if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_schema));
  } else if (column.is_hidden() && column.is_generated_column()) { //automatic generated column
    if (column.is_fulltext_column()) {
      if (OB_FAIL(print_fulltext_index_column(table_schema,
                                              column,
                                              ctxcat_cols,
                                              is_last,
                                              buf,
                                              buf_len,
                                              pos))) {
        LOG_WARN("print fulltext index column failed", K(ret));
      }
    } else if (column.is_spatial_generated_column()) {
      if (OB_FAIL(print_spatial_index_column(table_schema,
                                             column,
                                             buf,
                                             buf_len,
                                             pos))) {
        LOG_WARN("print spatial index column failed", K(ret));
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
             allocator,
             column.get_column_name_str(),
             new_col_name,
             is_oracle_mode))) {
    SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(column.get_column_name_str()));
  } else if (OB_FAIL(print_identifier(buf, buf_len, pos, new_col_name, is_oracle_mode))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(column));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, is_last ? ")" : ", "))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(column));
  } else { /*do nothing*/ }
  return ret;
}

int ObSchemaPrinter::print_table_definition_fulltext_indexs(
    const bool is_oracle_mode,
    const ObIArray<ObString> &fulltext_indexs,
    char *buf,
    int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObString new_col_name;
  if (fulltext_indexs.count() > 0) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " CTXCAT("))) {
      LOG_WARN("failed to print CTXCAT keyword", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < fulltext_indexs.count() - 1; ++i) {
    const ObString &ft_name = fulltext_indexs.at(i);
    if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                allocator,
                ft_name,
                new_col_name,
                is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(ft_name));
    } else if (OB_FAIL(print_identifier(buf, buf_len, pos, new_col_name, is_oracle_mode))) {
      LOG_WARN("print fulltext column name failed", K(ret), K(ft_name));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", " ))) {
      LOG_WARN("print const text failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && fulltext_indexs.count() > 0) {
    const ObString &ft_name = fulltext_indexs.at(fulltext_indexs.count() - 1);
    if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                allocator,
                ft_name,
                new_col_name,
                is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(ft_name));
    } else if (OB_FAIL(print_identifier(buf, buf_len, pos, new_col_name, is_oracle_mode))) {
      LOG_WARN("print fulltext column name failed", K(ret), K(ft_name));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")" ))) {
      LOG_WARN("print const text failed", K(ret));
    }
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_rowkeys(const ObTableSchema &table_schema,
                                                    char* buf,
                                                    const int64_t& buf_len,
                                                    int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo& rowkey_info = table_schema.get_rowkey_info();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  bool is_oracle_mode = false;
  if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_schema));
  } else if (!table_schema.is_heap_table() && rowkey_info.get_size() > 0) {
    bool has_pk_constraint_name = false;
    if (is_oracle_mode) {
      ObTableSchema::const_constraint_iterator iter = table_schema.constraint_begin();
      for (; OB_SUCC(ret) && iter != table_schema.constraint_end(); ++iter) {
        ObString new_cst_name;
        if (CONSTRAINT_TYPE_PRIMARY_KEY == (*iter)->get_constraint_type()){
          if (is_oracle_mode && (*iter)->is_sys_generated_name(false/*check_unknown*/)) {
            // do nothing
          } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                      allocator,
                      (*iter)->get_constraint_name_str(),
                      new_cst_name,
                      is_oracle_mode))) {
            SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K((*iter)->get_constraint_name_str()));
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                     ",\n  CONSTRAINT \"%s\" PRIMARY KEY (", new_cst_name.ptr()))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print CONSTRAINT cons_name PRIMARY KEY(", K(ret));
          } else {
            has_pk_constraint_name = true;
          }
          break; // 一张表只可能有一个主键约束
        }
      }
    }
    if (OB_SUCC(ret) && !has_pk_constraint_name) {
      //以下三种情况打印pk约束时，不打印pk名称
      //1. mysql mode 没有主键名称
      //2. oracle mode 2.1.0 之前包含2.1.0 server创建的表，不支持主键名
      //3. oracle mode 主键名称由系统生成
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n  PRIMARY KEY ("))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print PRIMARY KEY(", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(print_rowkey_info(rowkey_info,
                                                  table_schema.get_tenant_id(),
                                                  table_schema.get_table_id(),
                                                  buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print rowkey info", K(ret));
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print )", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (!is_oracle_mode && table_schema.get_pk_comment_str().length() > 0) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, " COMMENT '%s'" ,
            to_cstring(ObHexEscapeSqlStr(table_schema.get_pk_comment_str()))))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print primary key comment", K(ret), K(table_schema));
        }
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_rowkey_info(
    const ObRowkeyInfo& rowkey_info,
    const uint64_t tenant_id,
    const uint64_t table_id,
    char* buf,
    const int64_t& buf_len,
    int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("PrintRowkeyInfo");
  bool is_first_col = true;
  bool is_oracle_mode = false;
  if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
      tenant_id, table_id, is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_id));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < rowkey_info.get_size(); j++) {
    const ObColumnSchemaV2* col = NULL;
    ObString new_col_name;
    if (NULL == (col = schema_guard_.get_column_schema(
                tenant_id,
                table_id,
                rowkey_info.get_column(j)->column_id_))) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      SHARE_SCHEMA_LOG(WARN, "fail to get column", KR(ret), K(tenant_id),
                       "column_id", rowkey_info.get_column(j)->column_id_);
    } else if (col->get_column_id() == OB_HIDDEN_SESSION_ID_COLUMN_ID) {
      // do nothing
    } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                allocator,
                col->get_column_name_str(),
                new_col_name,
                is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret),
                        K(col->get_column_name_str()));
    } else if (!col->is_shadow_column()) {
      if (true == is_first_col) {
        if (OB_FAIL(print_identifier(buf, buf_len, pos, new_col_name, is_oracle_mode))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(col->get_column_name()));
        } else {
          is_first_col = false;
        }
      } else {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print const ptr", K(ret));
        } else if (OB_FAIL(print_identifier(buf, buf_len, pos, new_col_name, is_oracle_mode))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(col->get_column_name()));
        }
      }
    }
  }
  return ret;
}

template<typename T>
int ObSchemaPrinter::print_referenced_table_info(
    char* buf, const int64_t &buf_len, int64_t &pos, ObArenaAllocator &allocator,
    const bool is_oracle_mode, const ObForeignKeyInfo *foreign_key_info,
    const T *&parent_table_schema) const
{
  int ret = OB_SUCCESS;
  ObString new_parent_db_name;
  ObString new_parent_table_name;
  const ObDatabaseSchema *parent_db_schema = NULL;
  if (NULL == parent_table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    SHARE_SCHEMA_LOG(WARN, "unknown table", K(ret));
  } else if (OB_FAIL(schema_guard_.get_database_schema(parent_table_schema->get_tenant_id(), parent_table_schema->get_database_id(), parent_db_schema))) {
    SHARE_SCHEMA_LOG(WARN, "failed to get database", K(ret), K(parent_table_schema->get_database_id()));
  } else if (NULL == parent_db_schema) {
    ret = OB_ERR_BAD_DATABASE;
    SHARE_SCHEMA_LOG(WARN, "unknown database", K(ret), K(parent_table_schema->get_database_id()));
  } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
             allocator,
             parent_db_schema->get_database_name_str(),
             new_parent_db_name,
             is_oracle_mode))) {
    SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(foreign_key_info->foreign_key_name_));
  } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
             allocator,
             parent_table_schema->get_table_name_str(),
             new_parent_table_name,
             is_oracle_mode))) {
    SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(foreign_key_info->foreign_key_name_));
  } else if (OB_FAIL(print_identifier(buf, buf_len, pos, new_parent_db_name, is_oracle_mode))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print database name", K(ret),
                     K(parent_db_schema->get_database_name_str()));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "."))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print const", K(ret));
  } else if (OB_FAIL(print_identifier(buf, buf_len, pos, new_parent_table_name, is_oracle_mode))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print table name", K(ret),
                     K(parent_table_schema->get_table_name_str()));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "("))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print const", K(ret));
  } else if (OB_FAIL(print_column_list(*parent_table_schema, foreign_key_info->parent_column_ids_, buf, buf_len, pos))) {
    LOG_WARN("fail to print_column_list", K(ret), K(parent_table_schema->get_table_name_str()));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ") "))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print ) ", K(ret));
  }

  return ret;
}

// description: 在 show create table 的时候格式化打印外键信息到标准输出
//
// @param [in] table_schema
// @param [in] buf            记录打印内容的缓冲区
// @param [in] buf_len        OB_MAX_VARCHAR_LENGTH
// @param [in] pos            记录缓冲区中最后一个字符的下标
//
// @return oceanbase error code defined in lib/ob_errno.def
int ObSchemaPrinter::print_table_definition_foreign_keys(const ObTableSchema &table_schema,
    char* buf,
    const int64_t &buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  // foreign key 信息应该依附与子表
  bool is_oracle_mode = false;
  if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_schema));
  } else if (table_schema.is_child_table()) {
    const ObIArray<ObForeignKeyInfo> &foreign_key_infos = table_schema.get_foreign_key_infos();
    FOREACH_CNT_X(foreign_key_info, foreign_key_infos, OB_SUCC(ret)) {
      uint64_t parent_table_id = foreign_key_info->parent_table_id_;
      const char *update_action_str = NULL;
      const char *delete_action_str = NULL;
      ObString new_fk_name;
      // 只打印子表创建的外键信息，不打印作为父表的信息
      if (foreign_key_info->child_table_id_ == table_schema.get_table_id()) {
        if (is_oracle_mode && foreign_key_info->is_sys_generated_name(false/*check_unknown*/)) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n  "))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print CONSTRAINT", K(ret));
          }
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n  CONSTRAINT "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print CONSTRAINT", K(ret));
        } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                   allocator,
                   foreign_key_info->foreign_key_name_,
                   new_fk_name,
                   is_oracle_mode))) {
         SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(foreign_key_info->foreign_key_name_));
        } else if (!foreign_key_info->foreign_key_name_.empty() &&
                   OB_FAIL(print_identifier(buf, buf_len, pos, new_fk_name, is_oracle_mode))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print foreign key name", K(ret));
        } else if (!foreign_key_info->foreign_key_name_.empty() &&
                   OB_FAIL(databuff_printf(buf, buf_len, pos, " "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print foreign key name", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "FOREIGN KEY ("))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print FOREIGN KEY(", K(ret));
        } else if (OB_FAIL(print_column_list(table_schema, foreign_key_info->child_column_ids_, buf, buf_len, pos))) {
          LOG_WARN("fail to print_column_list", K(ret), K(table_schema.get_table_name_str()));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ") REFERENCES "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print ) REFERENCES ", K(ret));
        }
        if (OB_SUCC(ret)) {
          if (foreign_key_info->is_parent_table_mock_) {
            const ObMockFKParentTableSchema *mock_fk_parent_table_schema = NULL;
            if (OB_FAIL(schema_guard_.get_mock_fk_parent_table_schema_with_id(table_schema.get_tenant_id(), parent_table_id, mock_fk_parent_table_schema))) {
              LOG_WARN("fail to get table schema", K(ret), K(parent_table_id));
            } else if (OB_ISNULL(mock_fk_parent_table_schema)) {
              ret = OB_TABLE_NOT_EXIST;
              SHARE_SCHEMA_LOG(WARN, "unknown table", K(ret), K(parent_table_id));
            } else if (OB_FAIL(print_referenced_table_info(buf, buf_len, pos, allocator, is_oracle_mode, foreign_key_info, mock_fk_parent_table_schema))) {
              SHARE_SCHEMA_LOG(WARN, "print_referenced_table_info failed", K(ret), KPC(mock_fk_parent_table_schema));
            }
          } else {
            const ObTableSchema *parent_table_schema = NULL;
            if (OB_FAIL(schema_guard_.get_table_schema(table_schema.get_tenant_id(), parent_table_id, parent_table_schema))) {
              LOG_WARN("fail to get table schema", K(ret), K(parent_table_id));
            } else if (OB_ISNULL(parent_table_schema)) {
              ret = OB_TABLE_NOT_EXIST;
              SHARE_SCHEMA_LOG(WARN, "unknown table", K(ret), K(parent_table_id));
            } else if (OB_FAIL(print_referenced_table_info(buf, buf_len, pos, allocator, is_oracle_mode, foreign_key_info, parent_table_schema))) {
              SHARE_SCHEMA_LOG(WARN, "print_referenced_table_info failed", K(ret), KPC(parent_table_schema));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(update_action_str = foreign_key_info->get_update_action_str()) ||
                     OB_ISNULL(delete_action_str = foreign_key_info->get_delete_action_str())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("reference action is invalid", K(ret), K(foreign_key_info->update_action_), K(foreign_key_info->delete_action_));
          } else {
            if (is_oracle_mode) {
              if (foreign_key_info->delete_action_ == ACTION_CASCADE || foreign_key_info->delete_action_ == ACTION_SET_NULL) {
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
            } else if (OB_SUCC(ret) && is_oracle_mode && !foreign_key_info->enable_flag_ && foreign_key_info->validate_flag_) {
              if (OB_FAIL(databuff_printf(buf, buf_len, pos, " VALIDATE"))) {
                SHARE_SCHEMA_LOG(WARN, "fail to print foreign_key validate state", K(ret), K(*foreign_key_info));
              }
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

int ObSchemaPrinter::print_table_definition_store_format(const ObTableSchema &table_schema,
                                                         char* buf,
                                                         const int64_t& buf_len,
                                                         int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObStoreFormatType store_format = table_schema.get_store_format();
  const char* store_format_name = ObStoreFormat::get_store_format_print_str(store_format);
  bool is_oracle_mode = false;
  if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_schema));
  } else if (store_format == OB_STORE_FORMAT_INVALID) {
    // invalid store format , just skip print the store format, possible upgrade
    SQL_RESV_LOG(WARN, "Unexpected invalid table store format option", K(store_format), K(ret));
  } else if (OB_ISNULL(store_format_name)) {
    ret = OB_NOT_SUPPORTED;
    SQL_RESV_LOG(WARN, "Not supported store format in oracle mode", K(store_format), K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s ", store_format_name))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print store format", K(ret), K(table_schema), K(store_format));
  }
  if (OB_SUCC(ret) && !strict_compat_ && !is_oracle_mode) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "COMPRESSION = '%s' ",
            table_schema.is_compressed() ? table_schema.get_compress_func_name() : "none"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print compress method", K(ret), K(table_schema));
    }
  }

  return ret;
}

// description: 在 show create table 的时候根据输入的 column_ids 格式化打印对应的 column_name 到标准输出
//
// @param [in] table_schema
// @param [in] column_ids
// @param [in] buf            记录打印内容的缓冲区
// @param [in] buf_len        OB_MAX_VARCHAR_LENGTH
// @param [in] pos            记录缓冲区中最后一个字符的下标
//
// @return oceanbase error code defined in lib/ob_errno.def
template<typename T>
int ObSchemaPrinter::print_column_list(const T &table_schema,
                      const common::ObIArray<uint64_t> &column_ids,
                      char* buf,
                      const int64_t& buf_len,
                      int64_t& pos) const
{
  int ret = OB_SUCCESS;
  bool is_first_col = true;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  bool is_oracle_mode = false;
  if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_schema));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    bool is_column_exist = false;
    ObString ori_col_name;
    ObString new_col_name;
    table_schema.get_column_name_by_column_id(column_ids.at(i), ori_col_name, is_column_exist);
    if (!is_column_exist) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "The column schema is NULL, ", K(ret), K(table_schema), K(column_ids.at(i)));
    } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
               allocator,
               ori_col_name,
               new_col_name,
               is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to generate new name with escape character", K(ret), K(ori_col_name));
    } else if (true == is_first_col) {
      if (OB_FAIL(print_identifier(buf, buf_len, pos, new_col_name, is_oracle_mode))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print child column name", K(ret), K(ori_col_name));
      } else {
        is_first_col = false;
      }
    } else {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(ori_col_name));
      } else if (OB_FAIL(print_identifier(buf, buf_len, pos, new_col_name, is_oracle_mode))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print column name", K(ret), K(ori_col_name));
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_comment_oracle(const ObTableSchema &table_schema,
                                                           char *buf,
                                                           const int64_t &buf_len,
                                                           int64_t &pos) const
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_schema));
  } else if (!is_oracle_mode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table should be oracle mode", KR(ret), K(table_schema));
  } else if (table_schema.get_comment_str().empty()) {
    //do nothing
  } else {
    ret = databuff_printf(buf, buf_len, pos,
                          ";\nCOMMENT ON TABLE \"%s\" is '%s'",
                          table_schema.get_table_name(),
                          to_cstring(ObHexEscapeSqlStr(table_schema.get_comment_str())));
  }

  ObColumnIterByPrevNextID iter(table_schema);
  const ObColumnSchemaV2 *col = NULL;
  while (OB_SUCC(ret) && OB_SUCC(iter.next(col))) {
    if (OB_ISNULL(col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The column is null", K(ret));
    } else if (col->is_shadow_column()
               || col->is_hidden()
               || col->get_comment_str().empty()) {
      // do nothing
    } else {
      ret = databuff_printf(buf, buf_len, pos,
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

int ObSchemaPrinter::print_table_definition_table_options(const ObTableSchema &table_schema,
                                                          char* buf,
                                                          const int64_t& buf_len,
                                                          int64_t& pos,
                                                          bool is_for_table_status,
                                                          bool agent_mode,
                                                          ObSQLMode sql_mode) const
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  const bool is_index_tbl = table_schema.is_index_table();
  const uint64_t tenant_id = table_schema.get_tenant_id();
  if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_schema));
  }
  if (OB_SUCCESS == ret && !is_index_tbl && !is_for_table_status
      && !is_no_field_options(sql_mode) && !is_no_table_options(sql_mode)) {
    uint64_t auto_increment = 0;
    if (OB_FAIL(share::ObAutoincrementService::get_instance().get_sequence_value(
          table_schema.get_tenant_id(), table_schema.get_table_id(),
          table_schema.get_autoinc_column_id(), table_schema.is_order_auto_increment_mode(),
          table_schema.get_truncate_version(), auto_increment))) {
      SHARE_SCHEMA_LOG(WARN, "fail to get auto_increment value", K(ret));
    } else if (auto_increment > 0) {
      if (table_schema.get_auto_increment() > auto_increment) {
        auto_increment = table_schema.get_auto_increment();
      }
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "AUTO_INCREMENT = %lu ", auto_increment))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print auto increment", K(ret), K(auto_increment), K(table_schema));
      } else if (!strict_compat_ && OB_FAIL(databuff_printf(buf, buf_len, pos, "AUTO_INCREMENT_MODE = '%s' ",
                         table_schema.is_order_auto_increment_mode() ? "ORDER" : "NOORDER"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print auto increment mode", K(ret), K(table_schema));
      }
    }
  }

  if (OB_SUCCESS == ret && !is_for_table_status && !is_index_tbl
      && !is_no_table_options(sql_mode) && CHARSET_INVALID != table_schema.get_charset_type()) {
    if (is_oracle_mode) {
      //do not print charset info when in oracle mode
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "DEFAULT CHARSET = %s ",
                                             ObCharset::charset_name(table_schema.get_charset_type())))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print default charset", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_oracle_mode && !agent_mode && !is_for_table_status
      && !is_index_tbl && !is_no_table_options(sql_mode) && CS_TYPE_INVALID != table_schema.get_collation_type()
      && !ObCharset::is_default_collation(table_schema.get_charset_type(), table_schema.get_collation_type())) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "COLLATE = %s ",
                                             ObCharset::collation_name(table_schema.get_collation_type())))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print collate", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && table_schema.is_domain_index()
      && !is_no_key_options(sql_mode) && !table_schema.get_parser_name_str().empty()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "WITH PARSER '%s' ", table_schema.get_parser_name()))) {
      SHARE_SCHEMA_LOG(WARN, "print parser name failed", K(ret));
    }
  }
  if (OB_SUCCESS == ret && !is_index_tbl && !is_no_table_options(sql_mode) && !table_schema.is_external_table()) {
    if (OB_FAIL(print_table_definition_store_format(table_schema, buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print store format", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !strict_compat_ && !is_index_tbl && !is_no_table_options(sql_mode)
      && table_schema.get_expire_info().length() > 0
      && NULL != table_schema.get_expire_info().ptr()) {
    const ObString expire_str = table_schema.get_expire_info();
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "EXPIRE_INFO = (%.*s) ",
                                expire_str.length(), expire_str.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print expire info", K(ret), K(expire_str));
    }
  }
  if (OB_SUCCESS == ret && !strict_compat_ && !is_index_tbl && !is_no_table_options(sql_mode)
      && !table_schema.is_external_table()) {
    int64_t paxos_replica_num = OB_INVALID_COUNT;
    if (OB_FAIL(table_schema.get_paxos_replica_num(schema_guard_, paxos_replica_num))) {
      LOG_WARN("fail to get paxos replica num", K(ret));
    } else if (OB_UNLIKELY(paxos_replica_num < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("paxos replica num error", K(ret), K(paxos_replica_num),
               "table_id", table_schema.get_table_id());
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "REPLICA_NUM = %ld ", paxos_replica_num))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print replica num", K(ret), K(table_schema));
    } else {
      SHARE_SCHEMA_LOG(INFO, "XXX", K(paxos_replica_num));
    } // no more to do
  }
  if (OB_SUCCESS == ret && !strict_compat_ && table_schema.get_block_size() >= 0
      && !is_no_key_options(sql_mode) && !is_no_table_options(sql_mode)
      && !table_schema.is_external_table()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                is_index_tbl ? "BLOCK_SIZE %ld " : "BLOCK_SIZE = %ld ",
                                table_schema.get_block_size()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print block size", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !strict_compat_ && is_index_tbl && !table_schema.is_domain_index()
      && !is_no_key_options(sql_mode)) {
    const char* local_flag = table_schema.is_global_index_table()
                             || table_schema.is_global_local_index_table()
                             ? "GLOBAL " : "LOCAL ";
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", local_flag))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print global/local", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !strict_compat_ && !is_index_tbl && !is_no_table_options(sql_mode)
      && !table_schema.is_external_table()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "USE_BLOOM_FILTER = %s ",
                                table_schema.is_use_bloomfilter() ? "TRUE" : "FALSE"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print use bloom filter", K(ret), K(table_schema));
    }
  }

  if (OB_SUCCESS == ret && !is_index_tbl && table_schema.is_enable_row_movement()
      && !is_no_table_options(sql_mode) && !table_schema.is_external_table()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "ENABLE ROW MOVEMENT "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print row movement option", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !strict_compat_ && !is_index_tbl && !is_no_table_options(sql_mode)
      && !table_schema.is_external_table()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TABLET_SIZE = %ld ",
                                table_schema.get_tablet_size()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print tablet_size", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !strict_compat_ && !is_index_tbl && !is_no_table_options(sql_mode)
      && !table_schema.is_external_table()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "PCTFREE = %ld ",
                                table_schema.get_pctfree()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print pctfree", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !strict_compat_ && !is_index_tbl && table_schema.get_dop() > 1
      && !is_no_table_options(sql_mode)) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "PARALLEL %ld ",
                                table_schema.get_dop()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print dop", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !strict_compat_ && !is_index_tbl
      && common::OB_INVALID_ID != table_schema.get_tablegroup_id()
      && !is_no_table_options(sql_mode)) {
    const ObTablegroupSchema *tablegroup_schema = schema_guard_.get_tablegroup_schema(
          tenant_id, table_schema.get_tablegroup_id());
    if (NULL != tablegroup_schema) {
      const ObString tablegroup_name = tablegroup_schema->get_tablegroup_name();
      if (tablegroup_name.length() > 0 && NULL != tablegroup_name.ptr()) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TABLEGROUP = '%.*s' ",
                                    tablegroup_name.length(), tablegroup_name.ptr()))) {
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

  if (OB_SUCCESS == ret && !strict_compat_
      && !is_index_tbl && table_schema.get_progressive_merge_num() > 0
      && !is_no_table_options(sql_mode)) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "PROGRESSIVE_MERGE_NUM = %ld ",
            table_schema.get_progressive_merge_num()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print progressive merge num", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !is_oracle_mode && !is_for_table_status && table_schema.get_comment_str().length() > 0) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                is_index_tbl ? "COMMENT '%s' " : "COMMENT = '%s' ",
                                to_cstring(ObHexEscapeSqlStr(table_schema.get_comment_str()))))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print comment", K(ret), K(table_schema));
    }
  }

  if (OB_SUCCESS == ret && OB_INVALID_ID != table_schema.get_tablespace_id()
      && !is_no_table_options(sql_mode)) {
    if (!is_oracle_mode && table_schema.is_index_table()) {
      // do nothing
    } else if (OB_FAIL(print_tablespace_definition_for_table(table_schema.get_tenant_id(),
                table_schema.get_tablespace_id(), buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print tablespace option", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !strict_compat_ && !is_index_tbl && table_schema.is_read_only()
      && !is_no_table_options(sql_mode)) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "READ ONLY "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print table read only", K(ret));
    }
  }
  ObString table_mode_str = "";
  if (OB_SUCC(ret) && !strict_compat_ && !is_index_tbl && !is_no_table_options(sql_mode)) {
    if (!agent_mode) {
      if (table_schema.is_queuing_table()) {
        table_mode_str = "QUEUING";
      }
    } else { // true == agent_mode
      table_mode_str = ObBackUpTableModeOp::get_table_mode_str(table_schema.get_table_mode_struct());
    }
  }
  if (OB_SUCC(ret) && !strict_compat_ && table_mode_str.length() > 0 && !is_no_table_options(sql_mode)) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TABLE_MODE = '%s' ", table_mode_str.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print table table_mode", K(ret));
    }
  }
  if (OB_SUCC(ret) && !strict_compat_ && agent_mode) {
    if (OB_FAIL(!is_index_tbl ?
                databuff_printf(buf, buf_len, pos, "TABLE_ID = %lu ", table_schema.get_table_id()) :
                databuff_printf(buf, buf_len, pos, "INDEX_TABLE_ID = %lu DATA_TABLE_ID = %lu",
                                table_schema.get_table_id(), table_schema.get_data_table_id()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print progressive merge num", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret)
      && !strict_compat_
      && ObDuplicateScopeChecker::is_valid_replicate_scope(table_schema.get_duplicate_scope())
      && !is_no_table_options(sql_mode)) {
    // 目前只支持cluster
    if (table_schema.get_duplicate_scope() == ObDuplicateScope::DUPLICATE_SCOPE_CLUSTER) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "DUPLICATE_SCOPE = 'CLUSTER' "))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print table duplicate scope", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !strict_compat_ && !is_index_tbl
      && table_schema.get_ttl_definition().length() > 0
      && NULL != table_schema.get_ttl_definition().ptr()) {
    const ObString ttl_definition = table_schema.get_ttl_definition();
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TTL = (%.*s) ",
                                ttl_definition.length(), ttl_definition.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print ttl definition", K(ret), K(ttl_definition));
    }
  }
  if (OB_SUCC(ret) && !strict_compat_ && !is_index_tbl
      && table_schema.get_kv_attributes().length() > 0
      && NULL != table_schema.get_kv_attributes().ptr()) {
    const ObString kv_attributes = table_schema.get_kv_attributes();
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "KV_ATTRIBUTES = (%.*s) ",
                            kv_attributes.length(), kv_attributes.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print kv attributes", K(ret), K(kv_attributes));
    }
  }
  if (OB_SUCC(ret) && pos > 0) {
    pos -= 1;
    buf[pos] = '\0';      // remove trailer space
  }
  return ret;
}

static int print_partition_func(const ObTableSchema &table_schema,
                                ObSqlString &disp_part_str,
                                bool is_subpart)
{
  int ret = OB_SUCCESS;
  const ObPartitionOption &part_opt = table_schema.get_part_option();
  ObString type_str;
  ObPartitionFuncType type = part_opt.get_part_func_type();
  const ObString &func_expr = part_opt.get_part_func_expr_str();

  bool is_oracle_mode = false;
  if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_schema));
  } else if (OB_FAIL(get_part_type_str(is_oracle_mode, type, type_str))) {
    SHARE_SCHEMA_LOG(WARN, "failed to get part type string", K(ret));
  } else if (OB_FAIL(disp_part_str.append_fmt("partition by %.*s(%.*s)",
                                              type_str.length(),
                                              type_str.ptr(),
                                              func_expr.length(),
                                              func_expr.ptr()))) {
    SHARE_SCHEMA_LOG(WARN, "fail to append diaplay partition expr", K(ret), K(type_str), K(func_expr));
  } else if (is_subpart) { // sub part
    const ObPartitionOption &sub_part_opt = table_schema.get_sub_part_option();
    ObString sub_type_str;
    ObPartitionFuncType sub_type = sub_part_opt.get_part_func_type();

    const ObString &sub_func_expr = sub_part_opt.get_part_func_expr_str();
    if (OB_FAIL(get_part_type_str(is_oracle_mode, sub_type, sub_type_str))) {
      SHARE_SCHEMA_LOG(WARN, "failed to get part type string", K(ret));
    } else if (OB_FAIL(disp_part_str.append_fmt(" subpartition by %.*s(%.*s)",
                                                sub_type_str.length(),
                                                sub_type_str.ptr(),
                                                sub_func_expr.length(),
                                                sub_func_expr.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to append diaplay partition expr",
          K(ret), K(sub_type_str), K(sub_func_expr));
    }
  } else {}

  return ret;
}

static int print_tablegroup_partition_func(
    const ObTablegroupSchema &tablegroup_schema,
    ObSqlString &disp_part_str,
    bool is_subpart)
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  const ObPartitionOption &part_opt = tablegroup_schema.get_part_option();
  ObPartitionFuncType type = part_opt.get_part_func_type();
  const int64_t part_func_expr_num = tablegroup_schema.get_part_func_expr_num();
  ObString type_str;
  bool is_oracle_mode = false; // can't change part type
  if (tablegroup_id <= 0
      || part_func_expr_num <= 0
      || type < PARTITION_FUNC_TYPE_HASH
      || type >= PARTITION_FUNC_TYPE_MAX) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "invalid argument", K(ret), K(tablegroup_id), K(type), K(part_func_expr_num));
  } else if (!is_sys_tablegroup_id(tablegroup_id)) {
    if (OB_FAIL(get_part_type_str(is_oracle_mode, type, type_str))) {
      SHARE_SCHEMA_LOG(WARN, "failed to get part type string", K(ret));
    } else if (OB_FAIL(disp_part_str.append_fmt(" partition by %.*s",
                                                type_str.length(),
                                                type_str.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to append disp part str", K(ret));
    } else if (is_key_part(type)
               || PARTITION_FUNC_TYPE_RANGE_COLUMNS == type
               || PARTITION_FUNC_TYPE_LIST_COLUMNS == type) {
      if (OB_FAIL(disp_part_str.append_fmt(" %ld", part_func_expr_num))) {
        SHARE_SCHEMA_LOG(WARN, "fail to append disp part str", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_subpart) {
      const ObPartitionOption &sub_part_opt = tablegroup_schema.get_sub_part_option();
      ObPartitionFuncType sub_type = sub_part_opt.get_part_func_type();
      const int64_t sub_part_func_expr_num = tablegroup_schema.get_sub_part_func_expr_num();
      ObString sub_type_str;

      if (sub_part_func_expr_num <= 0
          || sub_type < PARTITION_FUNC_TYPE_HASH
          || sub_type >= PARTITION_FUNC_TYPE_MAX) {
        ret = OB_INVALID_ARGUMENT;
        SHARE_SCHEMA_LOG(WARN, "invalid argument", K(ret), K(tablegroup_id), K(sub_type), K(sub_part_func_expr_num));
      } else if (OB_FAIL(get_part_type_str(is_oracle_mode, sub_type, sub_type_str))) {
        SHARE_SCHEMA_LOG(WARN, "failed to get sub_part type string", K(ret));
      } else if (OB_FAIL(disp_part_str.append_fmt(" subpartition by %.*s",
                                                  sub_type_str.length(),
                                                  sub_type_str.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to append disp part str", K(ret));
      } else if (is_key_part(sub_type)
                 || PARTITION_FUNC_TYPE_RANGE_COLUMNS == sub_type
                 || PARTITION_FUNC_TYPE_LIST_COLUMNS == sub_type) {
        if (OB_FAIL(disp_part_str.append_fmt(" %ld", sub_part_func_expr_num))) {
          SHARE_SCHEMA_LOG(WARN, "fail to append disp part str", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObSchemaPrinter::print_interval_if_ness(const ObTableSchema &table_schema,
                                            char* buf,
                                            const int64_t& buf_len,
                                            int64_t& pos,
                                            const ObTimeZoneInfo *tz_info) const
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_schema));
  } else if (table_schema.is_interval_part()) {
    const ObRowkey &interval_range = table_schema.get_interval_range();
    OZ (databuff_printf(buf, buf_len, pos, " INTERVAL ("));

    OZ (ObPartitionUtils::convert_rowkey_to_sql_literal(is_oracle_mode,
                                                        interval_range,
                                                        buf,
                                                        buf_len,
                                                        pos,
                                                        false,
                                                        tz_info));
    OZ (databuff_printf(buf, buf_len, pos, ") "));
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_partition_options(const ObTableSchema &table_schema,
                                                              char* buf,
                                                              const int64_t& buf_len,
                                                              int64_t& pos,
                                                              bool agent_mode,
                                                              const ObTimeZoneInfo *tz_info) const
{
  int ret = OB_SUCCESS;
  if (table_schema.is_partitioned_table()
      && !table_schema.is_index_local_storage()
      && !table_schema.is_oracle_tmp_table()) {
    ObString disp_part_fun_expr_str;
    ObSqlString disp_part_str;
    bool is_subpart = false;
    const ObPartitionSchema *partition_schema = &table_schema;
    if (PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
      is_subpart = true;
      if (strict_compat_) {
        is_subpart &= is_subpartition_valid_in_mysql(table_schema);
      }
    }
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print enter", K(ret));
    } else if (OB_FAIL(print_partition_func(table_schema, disp_part_str, is_subpart))) {
      SHARE_SCHEMA_LOG(WARN, "failed to print part func", K(ret));
    } else if (FALSE_IT(disp_part_fun_expr_str = disp_part_str.string())) {
      // will not reach here
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " %.*s",
                                       disp_part_fun_expr_str.length(),
                                       disp_part_fun_expr_str.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to printf partition expr", K(ret), K(disp_part_fun_expr_str));
    } else if (OB_FAIL(print_interval_if_ness(table_schema, buf, buf_len, pos, tz_info))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print interval", K(ret));
     } else if (!strict_compat_ && is_subpart && partition_schema->sub_part_template_def_valid()) {
      if (OB_FAIL(print_template_sub_partition_elements(partition_schema, buf, buf_len, pos, tz_info, false))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print sub partition elements", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      bool print_sub_part_element = is_subpart &&
                                    (strict_compat_ || !partition_schema->sub_part_template_def_valid());
      if (table_schema.is_range_part()) {
        if (OB_FAIL(print_range_partition_elements(partition_schema, buf, buf_len, pos,
                                                   print_sub_part_element, agent_mode, false, tz_info))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print partition elements", K(ret));
        }
      } else if (table_schema.is_list_part()) {
        if (OB_FAIL(print_list_partition_elements(partition_schema, buf, buf_len, pos,
                                                  print_sub_part_element, agent_mode, false, tz_info))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print partition elements", K(ret));
        }
      } else if (is_hash_like_part(table_schema.get_part_option().get_part_func_type())) {
        if (OB_FAIL(print_hash_partition_elements(partition_schema, buf, buf_len, pos,
                                                  print_sub_part_element, agent_mode, tz_info))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print partition elements", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_on_commit_options(const ObTableSchema &table_schema,
                                                              char* buf,
                                                              const int64_t& buf_len,
                                                              int64_t& pos) const
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

int ObSchemaPrinter::print_table_definition_fulltext_indexs(
    const bool is_oracle_mode,
    const ObIArray<ObString> &fulltext_indexs,
    const uint64_t virtual_column_id,
    char *buf,
    int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(print_table_definition_fulltext_indexs(
                     is_oracle_mode, fulltext_indexs, buf, buf_len, pos))) {
    OB_LOG(WARN, "fail to print column definition", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " virtual_column_id = %lu ", virtual_column_id))) {
    OB_LOG(WARN, "fail to print virtual column id", K(ret));
  }
  return ret;
}

int ObSchemaPrinter::print_table_definition_table_options(
    const ObTableSchema &table_schema,
    const ObIArray<ObString> &full_text_columns,
    const uint64_t virtual_column_id,
    char* buf,
    const int64_t buf_len,
    int64_t& pos,
    bool is_for_table_status,
    common::ObMySQLProxy *sql_proxy,
    bool is_agent_mode) const
{
  int ret = OB_SUCCESS;
  const bool is_index_tbl = table_schema.is_index_table();
  const uint64_t tenant_id = table_schema.get_tenant_id();
  bool is_oracle_mode = false;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret));
  } else if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_schema));
  }

  if (OB_SUCCESS == ret && !strict_compat_ && !is_index_tbl && !is_for_table_status && is_agent_mode) {
    uint64_t auto_increment = 0;
    if (OB_ISNULL(sql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "sql_proxy is null", K(ret));
    } else if (OB_FAIL(share::ObAutoincrementService::get_instance().get_sequence_value(
                         table_schema.get_tenant_id(),
                         table_schema.get_table_id(),
                         table_schema.get_autoinc_column_id(),
                         table_schema.is_order_auto_increment_mode(),
                         table_schema.get_truncate_version(),
                         auto_increment))) {
      OB_LOG(WARN, "fail to get auto_increment value", K(ret));
    } else if (auto_increment > 0) {
      if (table_schema.get_auto_increment() > auto_increment) {
        auto_increment = table_schema.get_auto_increment();
      }
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "AUTO_INCREMENT = %lu ", auto_increment))) {
        OB_LOG(WARN, "fail to print auto increment", K(ret), K(auto_increment), K(table_schema));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "AUTO_INCREMENT_MODE = '%s' ",
                         table_schema.is_order_auto_increment_mode() ? "ORDER" : "NOORDER"))) {
        OB_LOG(WARN, "fail to print auto increment mode", K(ret), K(table_schema));
      }
    }
  }

  if (OB_SUCC(ret)  && !is_for_table_status && !is_index_tbl && CHARSET_INVALID != table_schema.get_charset_type()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "DEFAULT CHARSET = %s ",
                                             ObCharset::charset_name(table_schema.get_charset_type())))) {
      OB_LOG(WARN, "fail to print default charset", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret)  && !is_for_table_status && !is_index_tbl && CS_TYPE_INVALID != table_schema.get_collation_type()
      && !ObCharset::is_default_collation(table_schema.get_charset_type(), table_schema.get_collation_type())) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "COLLATE = %s ",
                                             ObCharset::collation_name(table_schema.get_collation_type())))) {
      OB_LOG(WARN, "fail to print collate", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && table_schema.is_domain_index()) {
    if (full_text_columns.count() <= 0 || OB_UNLIKELY(virtual_column_id == OB_INVALID_ID)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "invalid domain index infos", K(full_text_columns), K(virtual_column_id));
    } else if (!strict_compat_ && OB_FAIL(print_table_definition_fulltext_indexs(
               is_oracle_mode, full_text_columns, virtual_column_id, buf, buf_len, pos))) {
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
  if (OB_SUCC(ret) && !is_index_tbl && table_schema.get_expire_info().length() > 0
      && NULL != table_schema.get_expire_info().ptr()) {
    const ObString expire_str = table_schema.get_expire_info();
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "EXPIRE_INFO = (%.*s) ",
                                expire_str.length(), expire_str.ptr()))) {
      OB_LOG(WARN, "fail to print expire info", K(ret), K(expire_str));
    }
  }
  if (OB_SUCC(ret) && !strict_compat_ && !is_index_tbl) {
    int64_t paxos_replica_num = OB_INVALID_COUNT;
    if (OB_FAIL(table_schema.get_paxos_replica_num(schema_guard_, paxos_replica_num))) {
      OB_LOG(WARN, "fail to get paxos replica num", K(ret));
    } else if (OB_UNLIKELY(paxos_replica_num < 0)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "paxos_replica_num error", K(ret), K(paxos_replica_num),
          "table_id", table_schema.get_table_id());
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "REPLICA_NUM = %ld ",
        paxos_replica_num))) {
      OB_LOG(WARN, "fail to print replica num", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && !strict_compat_
      && table_schema.get_block_size() >= 0 && !is_index_tbl) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                "BLOCK_SIZE = %ld ",
                                table_schema.get_block_size()))) {
      OB_LOG(WARN, "fail to print block size", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && !strict_compat_ && is_index_tbl && !table_schema.is_domain_index()) {
    const char* local_flag = table_schema.is_global_index_table()
                             || table_schema.is_global_local_index_table()
                             ? "GLOBAL " : "LOCAL ";
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", local_flag))) {
      OB_LOG(WARN, "fail to print global/local", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && !strict_compat_
      && is_index_tbl && is_oracle_mode && !table_schema.is_index_visible()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "INVISIBLE"))) {
      OB_LOG(WARN, "fail to print invisible option", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && !strict_compat_ && !is_index_tbl) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "USE_BLOOM_FILTER = %s ",
                                table_schema.is_use_bloomfilter() ? "TRUE" : "FALSE"))) {
      OB_LOG(WARN, "fail to print use bloom filter", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !strict_compat_ && !is_index_tbl) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TABLET_SIZE = %ld ",
                                table_schema.get_tablet_size()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print tablet_size", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !strict_compat_ && !is_index_tbl) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "PCTFREE = %ld ",
                                table_schema.get_pctfree()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print pctfree", K(ret), K(table_schema));
    }
  }
  if (OB_SUCCESS == ret && !strict_compat_ && !is_index_tbl && table_schema.get_dop() > 1) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "PARALLEL %ld ",
                                table_schema.get_dop()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print dop", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && !strict_compat_
      && !is_index_tbl && common::OB_INVALID_ID != table_schema.get_tablegroup_id()) {
    const ObTablegroupSchema *tablegroup_schema = schema_guard_.get_tablegroup_schema(
          tenant_id, table_schema.get_tablegroup_id());
    if (NULL != tablegroup_schema) {
      const ObString tablegroup_name = tablegroup_schema->get_tablegroup_name_str();
      if (tablegroup_name.length() > 0 && NULL != tablegroup_name.ptr()) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TABLEGROUP = '%.*s' ",
                                    tablegroup_name.length(), tablegroup_name.ptr()))) {
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

  if (OB_SUCC(ret) && !strict_compat_
      && !is_index_tbl && table_schema.get_progressive_merge_num() > 0) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "PROGRESSIVE_MERGE_NUM = %ld ",
            table_schema.get_progressive_merge_num()))) {
      OB_LOG(WARN, "fail to print progressive merge num", K(ret), K(table_schema));
    }
  }

  if (OB_SUCC(ret) && !strict_compat_ && is_agent_mode) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "PROGRESSIVE_MERGE_ROUND = %ld ",
            table_schema.get_progressive_merge_round()))) {
      OB_LOG(WARN, "fail to print progressive merge round", K(ret), K(table_schema));
    }
  }

  if (OB_SUCC(ret) && !is_for_table_status && table_schema.get_comment_str().length() > 0) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                is_index_tbl ? "COMMENT '%s' " : "COMMENT = '%s' ",
                                to_cstring(ObHexEscapeSqlStr(table_schema.get_comment()))))) {
      OB_LOG(WARN, "fail to print comment", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && !strict_compat_ && !is_index_tbl && table_schema.is_read_only()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "READ ONLY "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print table read only", K(ret));
    }
  }
  // backup table mode
  ObString table_mode_str = "";
  if (OB_SUCC(ret) && !strict_compat_ && !is_index_tbl) {
    if (!is_agent_mode) {
      if (table_schema.is_queuing_table()) {
        table_mode_str = "QUEUING";
      }
    } else { // true == agent_mode
      table_mode_str = ObBackUpTableModeOp::get_table_mode_str(table_schema.get_table_mode_struct());
    }
  }
  if (OB_SUCC(ret) && !strict_compat_ && table_mode_str.length() > 0) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TABLE_MODE = '%s' ", table_mode_str.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print table table_mode", K(ret));
    }
  }

  //table_id for backup and restore
  if (OB_SUCC(ret) && !is_index_tbl && is_agent_mode) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " table_id = %lu ",
            table_schema.get_table_id()))) {
      OB_LOG(WARN, "fail to print progressive merge num", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && is_index_tbl && is_agent_mode) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " index_table_id = %lu data_table_id = %lu",
        table_schema.get_table_id(), table_schema.get_data_table_id()))) {
      OB_LOG(WARN, "fail to print progressive merge num", K(ret), K(table_schema));
    }
  }
  if (OB_SUCC(ret) && !is_index_tbl && !strict_compat_) {
    const ObString ttl_definition = table_schema.get_ttl_definition();
    if (ttl_definition.empty()) {
      // do nothing
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "TTL = (%.*s) ",
         ttl_definition.length(), ttl_definition.ptr()))) {
      OB_LOG(WARN, "fail to print ttl definition", K(ret), K(ttl_definition));
    }
  }

  if (OB_SUCC(ret) && !is_index_tbl && !strict_compat_) {
    const ObString kv_attributes = table_schema.get_kv_attributes();
    if (kv_attributes.empty()) {
      // do nothing
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "KV_ATTRIBUTES = (%.*s) ",
         kv_attributes.length(), kv_attributes.ptr()))) {
      OB_LOG(WARN, "fail to print kv attributes", K(ret), K(kv_attributes));
    }
  }

  return ret;
}

int ObSchemaPrinter::print_func_index_columns_definition(
    const ObString &expr_str,
    uint64_t column_id,
    char *buf,
    int64_t buf_len,
    int64_t &pos,
    bool is_agent_mode) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%.*s",
              expr_str.length(), expr_str.ptr()))) {
    OB_LOG(WARN, "failed to print column info", K(ret), K(expr_str));
  } else if (is_agent_mode
             && OB_FAIL(databuff_printf(buf, buf_len, pos, " id %lu", column_id))) {
    OB_LOG(WARN, "failed to print column id", K(ret), K(column_id));
  }
  return ret;
}

int ObSchemaPrinter::print_index_definition_columns(
    const ObTableSchema &data_schema,
    const ObTableSchema &index_schema,
    common::ObIArray<ObString> &full_text_columns,
    uint64_t &virtual_column_id,
    char* buf,
    const int64_t buf_len,
    int64_t& pos,
    bool is_agent_mode) const
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
    const ObRowkeyInfo &index_rowkey_info = index_schema.get_rowkey_info();
    for (int64_t k = 0; OB_SUCC(ret) && k < index_column_num; k++) {
      const ObRowkeyColumn *rowkey_column = index_rowkey_info.get_column(k);
      const ObColumnSchemaV2 *col = NULL;
      const ObColumnSchemaV2 *data_col = NULL;
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
          // 索引表的column flag以及default value expr被清理掉，需要从data table schema获取
          bool is_oracle_mode = false;
          if (OB_FAIL(data_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
            LOG_WARN("fail to check oracle mode", KR(ret), K(data_schema));
          } else if (NULL == (data_col = data_schema.get_column_schema(col->get_column_id()))) {
            ret = OB_SCHEMA_ERROR;
            SHARE_SCHEMA_LOG(WARN, "fail to get column schema", K(ret), K(*data_col));
          } else if (data_col->is_hidden() && data_col->is_generated_column()) { //automatic generated column
            if (data_col->is_fulltext_column()) {
              // domain index
              virtual_column_id = data_col->get_column_id();
              if (OB_FAIL(print_full_text_columns_definition(
                            data_schema, data_col->get_column_name(), full_text_columns,
                            buf, buf_len, pos, is_first, is_agent_mode))) {
                OB_LOG(WARN, "failed to print full text columns", K(ret));
              }
            } else if (data_col->is_func_idx_column()) {
              const ObString &expr_str = data_col->get_cur_default_value().is_null() ?
                data_col->get_orig_default_value().get_string() :
                data_col->get_cur_default_value().get_string();
              virtual_column_id = data_col->get_column_id();
              if(OB_FAIL(print_func_index_columns_definition(
                      expr_str, virtual_column_id,
                      buf, buf_len, pos, is_agent_mode))) {
                OB_LOG(WARN, "failed to print func index columns", K(ret));
              }
            } else {
              // 前缀索引
              const ObString &expr_str = data_col->get_cur_default_value().is_null() ?
                data_col->get_orig_default_value().get_string() :
                data_col->get_cur_default_value().get_string();
              ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
              sql::ObRawExprFactory expr_factory(allocator);
              sql::ObRawExpr *expr = NULL;
              ObArray<sql::ObQualifiedName> columns;
              ObString column_name;
              int64_t const_value = 0;
              SMART_VAR(sql::ObSQLSessionInfo, default_session) {//此处是mock的session，因此应该使用test_init,否则无法为tz_mgr初始化
                if (OB_FAIL(default_session.test_init(0, 0, 0, &allocator))) {
                  OB_LOG(WARN, "init empty session failed", K(ret));
                } else if (OB_FAIL(default_session.load_default_sys_variable(false, false))) {
                  OB_LOG(WARN, "session load default system variable failed", K(ret));
                } else if (OB_FAIL(sql::ObRawExprUtils::build_generated_column_expr(expr_str, expr_factory,
                        default_session, expr, columns, &data_schema, false /* sequence_allowed */, NULL))) {
                  OB_LOG(WARN, "get generated column expr failed", K(ret));
                } else if (OB_ISNULL(expr)) {
                  ret = OB_ERR_UNEXPECTED;
                  OB_LOG(WARN, "expr is null");
                } else if (3 != expr->get_param_count()) {
                  // 前缀索引表达式，有三列
                  ret = OB_ERR_UNEXPECTED;
                  OB_LOG(WARN, "It's wrong expr string", K(ret), K(expr->get_param_count()));
                } else if (1 != columns.count()) {
                  // 表达式列基于某一列
                  ret = OB_ERR_UNEXPECTED;
                  OB_LOG(WARN, "It's wrong expr string", K(ret), K(columns.count()));
                } else {
                  column_name = columns.at(0).col_name_;
                  sql::ObRawExpr *t_expr0= expr->get_param_expr(0);
                  sql::ObRawExpr *t_expr1 = expr->get_param_expr(1);
                  sql::ObRawExpr *t_expr2 = expr->get_param_expr(2);
                  if (OB_ISNULL(t_expr0) || OB_ISNULL(t_expr1) || OB_ISNULL(t_expr2)) {
                    ret = OB_ERR_UNEXPECTED;
                    OB_LOG(WARN, "expr is null", K(ret));
                  } else if (T_INT != t_expr2->get_expr_type()) {
                    ret = OB_ERR_UNEXPECTED;
                    OB_LOG(WARN, "expr type is not int", K(ret));
                  } else {
                    const_value = (static_cast<sql::ObConstRawExpr*>(t_expr2))->get_value().get_int();
                    if (OB_FAIL(print_identifier(buf, buf_len, pos, column_name, is_oracle_mode))) {
                      OB_LOG(WARN, "fail to print column name", K(ret), K(*col));
                    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "(%ld)", const_value))) {
                      OB_LOG(WARN, "fail to print column name", K(ret), K(*col));
                    } else if (is_agent_mode
                              && OB_FAIL(databuff_printf(buf, buf_len, pos,
                                          " id %lu", data_col->get_column_id()))) {
                      OB_LOG(WARN, "fail to print column id", K(ret), K(*col));
                    }
                  }
                }
              }
            }
          } else {
            // 普通索引
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " "))) {
              OB_LOG(WARN, "fail to print column name", K(ret), K(*col));
            } else if (OB_FAIL(print_identifier(buf, buf_len, pos, col->get_column_name(), is_oracle_mode))) {
              OB_LOG(WARN, "fail to print column name", K(ret), K(*col));
            } else if (is_agent_mode
                       && OB_FAIL(databuff_printf(buf, buf_len, pos, " id %lu",
                                  col->get_column_id()))) {
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

int ObSchemaPrinter::print_full_text_columns_definition(const ObTableSchema &data_schema,
                                                             const ObString &generated_column_name,
                                                             ObIArray<ObString> &full_text_columns,
                                                             char *buf, const int64_t buf_len, int64_t &pos,
                                                             bool &is_first,
                                                             bool is_agent_mode) const
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *data_col = NULL;
  const char *prefix = "__word_segment_";
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
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s  ", is_first ? "" : ",\n"))) {
      OB_LOG(WARN, "fail to print column name", K(ret), K(*data_col));
    } else if (OB_FAIL(print_identifier(buf, buf_len, pos, data_col->get_column_name(), false))) {
      OB_LOG(WARN, "fail to print column name", K(ret), K(*data_col));
    } else if (is_agent_mode
               && OB_FAIL(databuff_printf(buf, buf_len, pos, " id %lu",
                                          data_col->get_column_id()))) {
      OB_LOG(WARN, "fail to print column id", K(ret), K(*data_col));
    } else {
      is_first = false;
      column_id = 0;
    }
  }
  return ret;
}

int ObSchemaPrinter::print_table_index_stroing(
    const share::schema::ObTableSchema *index_schema,
    const share::schema::ObTableSchema *table_schema,
    char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_ISNULL(index_schema) || OB_ISNULL(table_schema) || OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(index_schema), K(table_schema), K(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(index_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), KPC(index_schema));
  } else {
    int64_t column_count = index_schema->get_column_count();
    const ObRowkeyInfo &index_rowkey_info = index_schema->get_rowkey_info();
    int64_t rowkey_count = index_rowkey_info.get_size();
    if (column_count > rowkey_count) {
      bool first_storing_column = true;
      for (ObTableSchema::const_column_iterator row_col = index_schema->column_begin();
          OB_SUCCESS == ret && NULL != row_col && row_col != index_schema->column_end();
          row_col++) {
        int64_t k = 0;
        const ObRowkeyInfo &tab_rowkey_info = table_schema->get_rowkey_info();
        int64_t tab_rowkey_count = tab_rowkey_info.get_size();
        for (; k < tab_rowkey_count; k++) {
          const ObRowkeyColumn *rowkey_column = tab_rowkey_info.get_column(k);
          if (NULL != *row_col &&
              rowkey_column->column_id_ == (*row_col)->get_column_id()) {
            break;
          }
        }
        if (k == tab_rowkey_count && NULL != *row_col
            && !(*row_col)->get_rowkey_position() && !(*row_col)->is_hidden()) {
          if (first_storing_column) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " STORING ("))) {
              OB_LOG(WARN, "fail to print STORING(", K(ret));
            }
            first_storing_column = false;
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(print_identifier(buf, buf_len, pos, (*row_col)->get_column_name(), is_oracle_mode))) {
              OB_LOG(WARN, "fail to print column name", K(ret), K((*row_col)->get_column_name()));
            } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
              OB_LOG(WARN, "fail to print const", K(ret));
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

int ObSchemaPrinter::print_index_table_definition(
    const uint64_t tenant_id,
    const uint64_t index_table_id,
    char* buf, const int64_t buf_len, int64_t& pos,
    const ObTimeZoneInfo *tz_info,
    bool is_agent_mode) const
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema *ds_schema = NULL;
  const ObTableSchema *table_schema = NULL;
  const ObTableSchema *index_table_schema = NULL;
  ObStringBuf allocator;
  ObString index_name;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "argument is invalid", K(ret));
  } else if (OB_FAIL(schema_guard_.get_table_schema(tenant_id,
             index_table_id, index_table_schema))) {
    OB_LOG(WARN, "fail to get index table schema", K(ret), K(tenant_id));
  } else if (NULL == index_table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    OB_LOG(WARN, "Unknow table", K(ret), K(index_table_id));
  } else if (OB_FAIL(schema_guard_.get_table_schema(tenant_id,
             index_table_schema->get_data_table_id(), table_schema))) {
    OB_LOG(WARN, "fail to get data table schema", K(ret), K(tenant_id));
  } else if (NULL == table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    OB_LOG(WARN, "Unknow table", K(ret), K(index_table_schema->get_data_table_id()));
  } else if (OB_FAIL(schema_guard_.get_database_schema(tenant_id,
             table_schema->get_database_id(), ds_schema))) {
    OB_LOG(WARN, "fail to get database schema", K(ret), K(tenant_id));
  } else if (NULL == ds_schema) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "database not exist", K(ret));
  } else {
    index_name = index_table_schema->get_table_name();
    if (index_name.prefix_match(OB_MYSQL_RECYCLE_PREFIX)
        || index_name.prefix_match(OB_ORACLE_RECYCLE_PREFIX)){
      // recyclebin object use original index name
    } else {
      if (OB_FAIL(ObTableSchema::get_index_name(allocator, table_schema->get_table_id(),
          ObString::make_string(index_table_schema->get_table_name()), index_name))) {
        OB_LOG(WARN, "get index table name failed");
      }
    }
    bool is_oracle_mode = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(index_table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check oracle mode", KR(ret), KPC(index_table_schema));
    } else if (index_table_schema->is_unique_index()) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                  !is_oracle_mode ? "CREATE UNIQUE INDEX if not exists "
                                                    : "CREATE UNIQUE INDEX "))) {
        OB_LOG(WARN, "fail to print create table prefix", K(ret), K(table_schema->get_table_name()));
      }
    } else if (index_table_schema->is_domain_index()) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                  !is_oracle_mode ? "CREATE FULLTEXT INDEX if not exists "
                                                    : "CREATE FULLTEXT INDEX "))) {
        OB_LOG(WARN, "fail to print create table prefix", K(ret), K(table_schema->get_table_name()));
      }
    } else {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                  !is_oracle_mode ? "CREATE INDEX if not exists "
                                                    : "CREATE INDEX "))) {
        OB_LOG(WARN, "fail to print create table prefix", K(ret), K(table_schema->get_table_name()));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(print_identifier(buf, buf_len, pos, ds_schema->get_database_name(), is_oracle_mode))) {
      OB_LOG(WARN, "fail to print create table prefix", K(ret), K(table_schema->get_table_name()));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "."))) {
      OB_LOG(WARN, "fail to print const str", K(ret));
    } else if (OB_FAIL(print_identifier(buf, buf_len, pos, index_name, is_oracle_mode))) {
      OB_LOG(WARN, "fail to print create table prefix", K(ret), K(table_schema->get_table_name()));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " on "))) {
      OB_LOG(WARN, "fail to print const str", K(ret));
    } else if (OB_FAIL(print_identifier(buf, buf_len, pos, ds_schema->get_database_name(), is_oracle_mode))) {
      OB_LOG(WARN, "fail to print create table prefix", K(ret), K(table_schema->get_table_name()));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "."))) {
      OB_LOG(WARN, "fail to print const str", K(ret));
    } else if (OB_FAIL(print_identifier(buf, buf_len, pos, table_schema->get_table_name(), is_oracle_mode))) {
      OB_LOG(WARN, "fail to print create table prefix", K(ret), K(table_schema->get_table_name()));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " (\n"))) {
      OB_LOG(WARN, "fail to print const str", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObSEArray<ObString, 4> full_text_columns;
    uint64_t virtual_column_id = OB_INVALID_ID;
    if (OB_FAIL(print_index_definition_columns(
                  *table_schema, *index_table_schema, full_text_columns, virtual_column_id, buf, buf_len, pos, is_agent_mode))) {
      OB_LOG(WARN, "fail to print columns", K(ret), K(*index_table_schema));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n) "))) {
      OB_LOG(WARN, "fail to print )", K(ret));
    } else if (OB_FAIL(print_table_definition_table_options(
                       *index_table_schema, full_text_columns, virtual_column_id, buf, buf_len, pos, false, NULL, is_agent_mode))) {
      OB_LOG(WARN, "fail to print table options", K(ret), K(*index_table_schema));
    } else if (!strict_compat_ && OB_FAIL(print_table_index_stroing(index_table_schema, table_schema, buf, buf_len, pos))) {
      OB_LOG(WARN, "fail to print partition table index storing", K(ret), K(*index_table_schema), K(*table_schema));
    } else if (index_table_schema->is_global_index_table()
               && OB_FAIL(print_table_definition_partition_options(*index_table_schema, buf, buf_len, pos, is_agent_mode, tz_info))) {
      OB_LOG(WARN, "fail to print partition info for index", K(ret), K(*index_table_schema));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ";\n"))) {
      OB_LOG(WARN, "fail to print end ;", K(ret));
    }
    OB_LOG(DEBUG, "print table schema", K(ret), K(*index_table_schema));
  }
  return ret;
}

int ObSchemaPrinter::print_view_definiton(
    const uint64_t tenant_id,
    const uint64_t table_id,
    char *buf,
    const int64_t &buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (buf && buf_len > 0) {
    const ObTableSchema *table_schema = NULL;
    bool is_oracle_mode = false;
    if (OB_FAIL(schema_guard_.get_table_schema(tenant_id, table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(table_id));
    } else if (NULL == table_schema) {
      ret = OB_TABLE_NOT_EXIST;
      SHARE_SCHEMA_LOG(WARN, "Unknow table", K(ret), K(table_id));
    } else if (OB_FAIL(table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check oracle mode", KR(ret), KPC(table_schema));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "CREATE %s%sVIEW ",
            is_oracle_mode && table_schema->is_view_created_by_or_replace_force() ? "OR REPLACE FORCE " : "",
            table_schema->is_materialized_view() ? "MATERIALIZED " : ""))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print view definition", K(ret));
    } else if (OB_FAIL(print_identifier(buf, buf_len, pos, table_schema->get_table_name(), is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print view definition", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " AS "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print view definition", K(ret));
    } else if (OB_FAIL(print_view_define_str(buf, buf_len, pos, is_oracle_mode,
                                             table_schema->get_view_schema().get_view_definition_str()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print view definition", K(ret));
    } else if (!table_schema->get_view_schema().get_view_is_updatable() && is_oracle_mode) {
      // with read only is only supported in syntax in oracle mode, but some inner system view is
      // nonupdatable, so we have to check compatible mode here.
      // view in oracle mode can't be both with read only and with check option.
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " WITH READ ONLY"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print view definition with read only", K(ret));
      }
    } else if (VIEW_CHECK_OPTION_CASCADED == table_schema->get_view_schema().get_view_check_option()) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " WITH CHECK OPTION"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print view definition with check option", K(ret));
      }
    } else if (VIEW_CHECK_OPTION_LOCAL == table_schema->get_view_schema().get_view_check_option()) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " WITH LOCAL CHECK OPTION"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print view definition with local check option", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf is bull", K(ret));
  }
  return ret;
}

int ObSchemaPrinter::print_tablegroup_definition(
    const uint64_t tenant_id,
    const uint64_t tablegroup_id,
    char* buf,
    const int64_t& buf_len,
    int64_t& pos,
    bool agent_mode,
    const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;

  const ObTablegroupSchema *tablegroup_schema = NULL;
  bool is_oracle_mode = false;
  if (OB_FAIL(schema_guard_.get_tablegroup_schema(tenant_id, tablegroup_id, tablegroup_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(tablegroup_id));
  } else if (NULL == tablegroup_schema) {
    ret = OB_TABLE_NOT_EXIST;
    SHARE_SCHEMA_LOG(WARN, "Unknow table", K(ret), K(tablegroup_id));
  } else if (OB_FAIL(tablegroup_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
    SHARE_SCHEMA_LOG(WARN, "fail to check oracle mode", KR(ret), K(tablegroup_id));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                     is_oracle_mode ?  "CREATE TABLEGROUP "
                                                     : "CREATE TABLEGROUP IF NOT EXISTS "))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print create tablegroup prefix", K(ret), K(tablegroup_schema->get_tablegroup_name()));
  } else if (OB_FAIL(print_identifier(buf, buf_len, pos, tablegroup_schema->get_tablegroup_name(), is_oracle_mode))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print create tablegroup prefix", K(ret), K(tablegroup_schema->get_tablegroup_name()));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " "))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print create tablegroup prefix", K(ret), K(tablegroup_schema->get_tablegroup_name()));
  } else if (OB_FAIL(print_tablegroup_definition_tablegroup_options(*tablegroup_schema, buf, buf_len, pos, agent_mode))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print tablegroup options", K(ret), K(*tablegroup_schema));
  }
  SHARE_SCHEMA_LOG(DEBUG, "print tablegroup schema", K(ret), K(*tablegroup_schema));
  return ret;
}

int ObSchemaPrinter::print_tablespace_definition(
    const uint64_t tenant_id,
    const uint64_t tablespace_id,
    char* buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const ObTablespaceSchema *tablespace_schema = NULL;
  if (OB_FAIL(schema_guard_.get_tablespace_schema(tenant_id, tablespace_id, tablespace_schema))) {
    SHARE_SCHEMA_LOG(WARN, "fail to get tablespace schema", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "CREATE TABLESPACE "))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print create tablespace prefix", K(ret), K(tablespace_schema->get_tablespace_name()));
  } else if (OB_FAIL(print_identifier(buf, buf_len, pos, tablespace_schema->get_tablespace_name(), lib::is_oracle_mode()))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print create tablespace prefix", K(ret), K(tablespace_schema->get_tablespace_name()));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " "))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print create tablespace prefix", K(ret), K(tablespace_schema->get_tablespace_name()));
  }
  return ret;
}

int ObSchemaPrinter::print_tablespace_definition_for_table(
    const uint64_t tenant_id,
    const uint64_t tablespace_id,
    char* buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const ObTablespaceSchema *tablespace_schema = NULL;
  if (OB_INVALID_ID == tablespace_id) {
    /*do nothing*/
  } else if (OB_FAIL(schema_guard_.get_tablespace_schema(tenant_id, tablespace_id, tablespace_schema))) {
    SHARE_SCHEMA_LOG(WARN, "fail to get tablespace schema", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " TABLESPACE "))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print create tablespace prefix", K(ret), K(tablespace_schema->get_tablespace_name()));
  } else if (OB_FAIL(print_identifier(buf, buf_len, pos, tablespace_schema->get_tablespace_name(), lib::is_oracle_mode()))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print create tablespace prefix", K(ret), K(tablespace_schema->get_tablespace_name()));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " "))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print create tablespace prefix", K(ret), K(tablespace_schema->get_tablespace_name()));
  }
  return ret;
}

int ObSchemaPrinter::print_tablegroup_definition_tablegroup_options(
    const ObTablegroupSchema &tablegroup_schema,
    char* buf,
    const int64_t& buf_len,
    int64_t& pos,
    bool agent_mode) const
{
  int ret = OB_SUCCESS;
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  if (tablegroup_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablegroup_id is invalid", K(ret), K(tablegroup_id));
  } else if (agent_mode) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " TABLEGROUP_ID = %ld", tablegroup_schema.get_tablegroup_id()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print tablegroup_id", K(ret), K(tablegroup_schema.get_tablegroup_id()));
    }
  }
  if (OB_SUCC(ret)) {
    bool is_oracle_mode = false;
    uint64_t compat_version = OB_INVALID_VERSION;
    uint64_t tenant_id = tablegroup_schema.get_tenant_id();
    if (OB_FAIL(tablegroup_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    SHARE_SCHEMA_LOG(WARN, "fail to check oracle mode", KR(ret), K(tablegroup_id));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
        LOG_WARN("get min data_version failed", K(ret), K(tenant_id));
    } else if (compat_version >= DATA_VERSION_4_2_0_0) {
      const ObString sharding = tablegroup_schema.get_sharding();
      if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                  is_oracle_mode
                                  ? " SHARDING = \"%.*s\""
                                  : " SHARDING = %.*s",
                                  sharding.length(), sharding.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print tablegroup sharding", K(ret), K(tablegroup_schema.get_sharding()));
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_hash_sub_partition_elements(ObSubPartition **sub_part_array,
                                                       const int64_t sub_part_num,
                                                       char* buf,
                                                       const int64_t& buf_len,
                                                       int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_part_array)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "subpartition_array is NULL", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " (\n"))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print subpartition template", K(ret));
  } else {
    for (int64_t i = 0 ; OB_SUCC(ret) && i < sub_part_num; ++i) {
      const ObSubPartition *sub_partition = sub_part_array[i];
      if (OB_ISNULL(sub_partition)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "sub partition is null", K(ret), K(sub_part_num));
      } else {
        const ObString &part_name = sub_partition->get_part_name();
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "subpartition "))) {
          SHARE_SCHEMA_LOG(WARN, "print subpartition failed", K(ret));
        } else if (OB_FAIL(print_identifier(buf, buf_len, pos, part_name, lib::is_oracle_mode()))) {
          SHARE_SCHEMA_LOG(WARN, "print part name failed", K(ret), K(part_name));
        } else if (OB_FAIL(print_tablespace_definition_for_table(
                   sub_partition->get_tenant_id(), sub_partition->get_tablespace_id(), buf, buf_len, pos))) {
          SHARE_SCHEMA_LOG(WARN, "print tablespace definition failed", K(ret));
        } else if (sub_part_num - 1 != i && OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else if (sub_part_num - 1 == i && OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else {}
      }
    }
  }
  return ret;
}

// TODO: yibo tablegroup还不支持hash分区自定义分区名，暂时保留原始的打印方式
int ObSchemaPrinter::print_hash_sub_partition_elements_for_tablegroup(const ObPartitionSchema *&schema,
                                                                      char* buf,
                                                                      const int64_t& buf_len,
                                                                      int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema is null", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " subpartitions %ld", schema->get_def_sub_part_num()))) {
    SHARE_SCHEMA_LOG(WARN, "print partition number failed", K(ret));
  } else if (OB_NOT_NULL(schema->get_def_subpart_array())) {
    const ObSubPartition *sub_partition =  schema->get_def_subpart_array()[0];
    if (OB_FAIL(print_tablespace_definition_for_table(
                sub_partition->get_tenant_id(), sub_partition->get_tablespace_id(), buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to printf tablespace definition", K(ret));
    }
  }
  return ret;
}

int ObSchemaPrinter::print_list_sub_partition_elements(
    const bool is_oracle_mode,
    ObSubPartition **sub_part_array,
    const int64_t sub_part_num,
    char *buf,
    const int64_t &buf_len,
    int64_t &pos,
    const common::ObTimeZoneInfo *tz_info) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_part_array)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "subpartition_array is NULL", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " (\n"))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print subpartition template", K(ret));
  } else {
    for (int64_t i = 0 ; OB_SUCC(ret) && i < sub_part_num; ++i) {
      const ObSubPartition *sub_partition = sub_part_array[i];
      if (OB_ISNULL(sub_partition)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "sub partition is null", K(ret), K(sub_part_num));
      } else {
        const ObString &part_name = sub_partition->get_part_name();
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "subpartition "))) {
          SHARE_SCHEMA_LOG(WARN, "print subpartition failed", K(ret));
        } else if (OB_FAIL(print_identifier(buf, buf_len, pos, part_name, is_oracle_mode))) {
          SHARE_SCHEMA_LOG(WARN, "print part name failed", K(ret), K(part_name));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " values %s (",
                                    is_oracle_mode ? "" : "in"))) {
          SHARE_SCHEMA_LOG(WARN, "print values failed", K(ret));
        } else if (OB_FAIL(ObPartitionUtils::convert_rows_to_sql_literal(
                   is_oracle_mode, sub_partition->get_list_row_values(), buf, buf_len, pos, false, tz_info))) {
          SHARE_SCHEMA_LOG(WARN, "convert rows to sql literal",
              K(ret), K(sub_partition->get_list_row_values()));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else if (OB_FAIL(print_tablespace_definition_for_table(
                   sub_partition->get_tenant_id(), sub_partition->get_tablespace_id(), buf, buf_len, pos))) {
          SHARE_SCHEMA_LOG(WARN, "print tablespace definition failed", K(ret));
        } else if (sub_part_num - 1 != i && OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else if (sub_part_num - 1 == i && OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else {}
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_range_sub_partition_elements(
    const bool is_oracle_mode,
    ObSubPartition **sub_part_array,
    const int64_t sub_part_num,
    char * buf,
    const int64_t & buf_len,
    int64_t &pos,
    const ObTimeZoneInfo *tz_info) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_part_array)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "subpartition_array is NULL", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " (\n"))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print subpartition template", K(ret));
  } else {
    for (int64_t i = 0 ; OB_SUCC(ret) && i < sub_part_num; ++i) {
      const ObSubPartition *sub_partition = sub_part_array[i];
      if (OB_ISNULL(sub_partition)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "sub partition is null", K(ret), K(sub_part_num));
      } else {
        const ObString &part_name = sub_partition->get_part_name();
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "subpartition "))) {
          SHARE_SCHEMA_LOG(WARN, "print subpartition failed", K(ret));
        } else if (OB_FAIL(print_identifier(buf, buf_len, pos, part_name, is_oracle_mode))) {
          SHARE_SCHEMA_LOG(WARN, "print part name failed", K(ret), K(part_name));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " values less than ("))) {
          SHARE_SCHEMA_LOG(WARN, "print values less than failed", K(ret));
        } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
                   is_oracle_mode, sub_partition->get_high_bound_val(),
                   buf, buf_len, pos, false, tz_info))) {
          SHARE_SCHEMA_LOG(WARN, "convert rowkey to sql literal",
              K(ret), K(sub_partition->get_high_bound_val()));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else if (OB_FAIL(print_tablespace_definition_for_table(
                   sub_partition->get_tenant_id(), sub_partition->get_tablespace_id(), buf, buf_len, pos))) {
          SHARE_SCHEMA_LOG(WARN, "print tablespace definition failed", K(ret));
        } else if (sub_part_num - 1 != i && OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else if (sub_part_num - 1 == i && OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
          SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
        } else {}
      }
    }
  }
  return ret;
}


int ObSchemaPrinter::print_template_sub_partition_elements(const ObPartitionSchema *&schema,
                                                           char* buf,
                                                           const int64_t& buf_len,
                                                           int64_t &pos,
                                                           const ObTimeZoneInfo *tz_info,
                                                           bool is_tablegroup) const
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema is null", K(ret));
  } else if (!schema->sub_part_template_def_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema is not sub part template", K(ret));
  } else if (OB_FAIL(schema->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), KPC(schema));
  } else if (is_tablegroup &&
             is_hash_like_part(schema->get_sub_part_option().get_part_func_type())) {
    // tablegroup还不支持hash subpartition template 语法, 先print subpartitions x
    ret = print_hash_sub_partition_elements_for_tablegroup(schema, buf, buf_len, pos);
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " subpartition template"))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print subpartition template", K(ret));
  } else if (is_hash_like_part(schema->get_sub_part_option().get_part_func_type())) {
    ret = print_hash_sub_partition_elements(schema->get_def_subpart_array(),
                                            schema->get_def_sub_part_num(),
                                            buf, buf_len, pos);
  } else if (schema->is_range_subpart()) {
    ret = print_range_sub_partition_elements(is_oracle_mode,
                                             schema->get_def_subpart_array(),
                                             schema->get_def_sub_part_num(),
                                             buf, buf_len, pos, tz_info);
  } else if (schema->is_list_subpart()) {
    ret = print_list_sub_partition_elements(is_oracle_mode,
                                            schema->get_def_subpart_array(),
                                            schema->get_def_sub_part_num(),
                                            buf, buf_len, pos, tz_info);
  }
  return ret;
}

int ObSchemaPrinter::print_individual_sub_partition_elements(const ObPartitionSchema *&schema,
                                                             const ObPartition *partition,
                                                             char* buf,
                                                             const int64_t& buf_len,
                                                             int64_t &pos,
                                                             const ObTimeZoneInfo *tz_info) const
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_ISNULL(schema) || OB_ISNULL(partition)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "argument is null", K(ret), K(schema), K(partition));
  } else if (!strict_compat_ && schema->sub_part_template_def_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema is sub part template", K(ret));
  } else if (OB_FAIL(schema->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), KPC(schema));
  } else if (is_hash_like_part(schema->get_sub_part_option().get_part_func_type())) {
    ret = print_hash_sub_partition_elements(partition->get_subpart_array(),
                                            partition->get_sub_part_num(),
                                            buf, buf_len, pos);
  } else if (schema->is_range_subpart()) {
    ret = print_range_sub_partition_elements(is_oracle_mode,
                                             partition->get_subpart_array(),
                                             partition->get_sub_part_num(),
                                             buf, buf_len, pos, tz_info);
  } else if (schema->is_list_subpart()) {
    ret = print_list_sub_partition_elements(is_oracle_mode,
                                            partition->get_subpart_array(),
                                            partition->get_sub_part_num(),
                                            buf, buf_len, pos, tz_info);
  }
  return ret;
}

int ObSchemaPrinter::print_list_partition_elements(const ObPartitionSchema *&schema,
                                                   char* buf,
                                                   const int64_t& buf_len,
                                                   int64_t& pos,
                                                   bool print_sub_part_element,
                                                   bool agent_mode/*false*/,
                                                   bool tablegroup_def/*false*/,
                                                   const common::ObTimeZoneInfo *tz_info/*NULL*/) const
{
  int ret = common::OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema is null", K(ret));
  } else if (OB_FAIL(schema->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), KPC(schema));
  } else {
    ObPartition **part_array = schema->get_part_array();
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "partition_array is NULL", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n("))) {
      SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
    } else {
      int64_t part_num = schema->get_first_part_num();
      bool is_first = true;
      for (int64_t i = 0 ; OB_SUCC(ret) && i < part_num; ++i) {
        const ObPartition *partition = part_array[i];
        if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "partition is NULL", K(ret), K(part_num));
        } else {
          const ObString &part_name = partition->get_part_name();
          bool print_collation = agent_mode && tablegroup_def;
          if (strict_compat_&&
              partition->get_list_row_values().count() == 1 &&
              partition->get_list_row_values().at(0).get_count() == 1 &&
              partition->get_list_row_values().at(0).get_cell(0).is_max_value()) {
            // default partition
            // do nothing
          } else if (!is_first && OB_FAIL(databuff_printf(buf, buf_len, pos, ",\n"))) {
            SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "partition "))) {
            SHARE_SCHEMA_LOG(WARN, "print partition failed", K(ret));
          } else if (OB_FAIL(print_identifier(buf, buf_len, pos, part_name, is_oracle_mode))) {
            SHARE_SCHEMA_LOG(WARN, "print partition name failed", K(ret), K(part_name));
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " values %s (",
                                      is_oracle_mode ? "" : "in"))) {
            SHARE_SCHEMA_LOG(WARN, "print values failed", K(ret));
          } else if (OB_FAIL(ObPartitionUtils::convert_rows_to_sql_literal(
                     is_oracle_mode, partition->get_list_row_values(), buf, buf_len, pos, print_collation, tz_info))) {
            SHARE_SCHEMA_LOG(WARN, "convert rows to sql literal failed",
                K(ret), K(partition->get_list_row_values()));
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
            SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
          } else if (OB_FAIL(print_tablespace_definition_for_table(
                     partition->get_tenant_id(), partition->get_tablespace_id(), buf, buf_len, pos))) {
            SHARE_SCHEMA_LOG(WARN, "print tablespace definition failed", K(ret));
          } else if (print_sub_part_element
                     && OB_NOT_NULL(partition->get_subpart_array())
                     && OB_FAIL(print_individual_sub_partition_elements(schema, partition, buf,
                                                                     buf_len, pos, tz_info))) {
              SHARE_SCHEMA_LOG(WARN, "failed to print individual sub partition elements", K(ret));
          } else {
            is_first = false;
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
        SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_range_partition_elements(const ObPartitionSchema *&schema,
                                                    char* buf,
                                                    const int64_t& buf_len,
                                                    int64_t& pos,
                                                    bool print_sub_part_element,
                                                    bool agent_mode,
                                                    bool tablegroup_def,
                                                    const common::ObTimeZoneInfo *tz_info) const
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema is null", K(ret));
  } else if (OB_FAIL(schema->check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), KPC(schema));
  } else {
    ObPartition **part_array = schema->get_part_array();
    if (OB_ISNULL(part_array)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "partition_array is NULL", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n("))) {
      SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
    } else {
      int64_t part_num = schema->get_first_part_num();
      if (schema->is_interval_part()) {
        for (int64_t i = 0 ; OB_SUCC(ret) && i < part_num; ++i) {
          const ObPartition *partition = part_array[i];
          if (OB_ISNULL(partition)) {
            ret = OB_ERR_UNEXPECTED;
            SHARE_SCHEMA_LOG(WARN, "partition is NULL", K(ret), K(part_num));
          } else if (0 < partition->get_low_bound_val().get_obj_cnt()) {
            part_num = i;
            break;
          }
        }
      }

      for (int64_t i = 0 ; OB_SUCC(ret) && i < part_num; ++i) {
        const ObPartition *partition = part_array[i];
        if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "partition is NULL", K(ret), K(part_num));
        } else {
          const ObString &part_name = partition->get_part_name();
          bool print_collation = agent_mode && tablegroup_def;
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, "partition "))) {
            SHARE_SCHEMA_LOG(WARN, "print partition failed", K(ret));
          } else if (OB_FAIL(print_identifier(buf, buf_len, pos, part_name, is_oracle_mode))) {
            SHARE_SCHEMA_LOG(WARN, "print partition name failed", K(ret), K(part_name));
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " values less than ("))) {
            SHARE_SCHEMA_LOG(WARN, "print values less than failed", K(ret));
          } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(
                     is_oracle_mode, partition->get_high_bound_val(),
                     buf, buf_len, pos, print_collation, tz_info))) {
            SHARE_SCHEMA_LOG(WARN, "convert rowkey to sql literal failed",
                K(ret), K(partition->get_high_bound_val()));
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
            SHARE_SCHEMA_LOG(WARN, "print failed", K(ret));
          } else if (agent_mode && !tablegroup_def) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, " id %ld", partition->get_part_id()))) { // print id
              SHARE_SCHEMA_LOG(WARN, "print part_id failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(print_tablespace_definition_for_table(
                partition->get_tenant_id(), partition->get_tablespace_id(), buf, buf_len, pos))) {
              SHARE_SCHEMA_LOG(WARN, "print tablespace id failed", K(ret), K(part_name));
            } else if (print_sub_part_element
                       && OB_NOT_NULL(partition->get_subpart_array())
                       && OB_FAIL(print_individual_sub_partition_elements(schema, partition, buf,
                                                                          buf_len, pos, tz_info))) {
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
    const uint64_t tenant_id,
    const uint64_t database_id,
    bool if_not_exists,
    char *buf,
    const int64_t &buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t mark_pos = 0;
  const ObDatabaseSchema *database_schema = NULL;

  if (OB_ISNULL(buf) ||  buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(buf), K(buf_len));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_guard_.get_database_schema(tenant_id, database_id, database_schema))) {
      LOG_WARN("get database schema failed ", K(ret), K(tenant_id));
    } else if (NULL == database_schema) {
      ret = OB_ERR_BAD_DATABASE;
      SHARE_SCHEMA_LOG(WARN, "Unknow database", K(ret), K(tenant_id), K(database_id));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                       "CREATE DATABASE %s",
                                       true == if_not_exists? "IF NOT EXISTS " : ""))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print database definition", K(ret));
    } else if (OB_FAIL(print_identifier(buf, buf_len, pos,
                                        database_schema->get_database_name_str(),
                                        lib::is_oracle_mode()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print database definition", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    mark_pos = pos;
  }
  if (OB_SUCCESS == ret && CHARSET_INVALID != database_schema->get_charset_type()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " DEFAULT CHARACTER SET = %s",
                                             ObCharset::charset_name(database_schema->get_charset_type())))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print default charset", K(ret), K(*database_schema));
    }
  }
  if (OB_SUCCESS == ret && !lib::is_oracle_mode() && CS_TYPE_INVALID != database_schema->get_collation_type()
      && !ObCharset::is_default_collation(database_schema->get_collation_type())) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " DEFAULT COLLATE = %s",
                                             ObCharset::collation_name(database_schema->get_collation_type())))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print default collate", K(ret), K(*database_schema));
    }
  }
  if (OB_SUCC(ret) && !strict_compat_) {
    int64_t paxos_replica_num = OB_INVALID_COUNT;
    if (OB_FAIL(database_schema->get_paxos_replica_num(schema_guard_, paxos_replica_num))) {
      LOG_WARN("fail to get paxos replica num", K(ret));
    } else if (OB_UNLIKELY(paxos_replica_num < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("paxos replica num error", K(ret), K(paxos_replica_num),
               "database_id", database_schema->get_database_id());
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " REPLICA_NUM = %ld", paxos_replica_num))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print replica num", K(ret), K(*database_schema));
    } else {} // no more to do
  }

  if (OB_SUCC(ret) && !strict_compat_ && database_schema->is_read_only()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " READ ONLY"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print database read only", K(ret));
    }
  }
  if (OB_SUCC(ret) && !strict_compat_) {
    uint64_t tablegroup_id = database_schema->get_default_tablegroup_id();
    if (common::OB_INVALID_ID != tablegroup_id) {
      const ObTablegroupSchema *tablegroup_schema = schema_guard_.get_tablegroup_schema(
                                                    tenant_id, tablegroup_id);
      if (NULL != tablegroup_schema) {
        const ObString tablegroup_name = tablegroup_schema->get_tablegroup_name();
        if (!tablegroup_name.empty()) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " DEFAULT TABLEGROUP = %.*s",
                                                   tablegroup_name.length(),
                                                   tablegroup_name.ptr()))) {
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
    buf[pos] = '\0';      // remove trailer dot and space
  }
  return ret;
}

int ObSchemaPrinter::print_tenant_definition(uint64_t tenant_id,
                            common::ObMySQLProxy *sql_proxy,
                            char* buf,
                            const int64_t& buf_len,
                            int64_t& pos,
                            bool is_agent_mode) const
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = NULL;
  const ObSysVariableSchema *sys_variable_schema = NULL;

  if (OB_ISNULL(buf) ||  buf_len <= 0) {
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
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
            "CREATE TENANT %.*s ",
            tenant_schema->get_tenant_name_str().length(),
            tenant_schema->get_tenant_name_str().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print tenant name", K(ret), K(tenant_schema->get_tenant_name_str()));
    }
  }

  if (OB_SUCC(ret)) {// charset
    const ObSysVarSchema *server_collation = NULL;
    ObObj server_collation_obj;
    ObCollationType server_collation_type = CS_TYPE_INVALID;
    ObArenaAllocator allocator;
    if (OB_FAIL(schema_guard_.get_tenant_system_variable(tenant_id,
                                                         SYS_VAR_COLLATION_SERVER,
                                                         server_collation))) {
      LOG_WARN("fail to get tenant system variable", K(ret));
    } else if (OB_FAIL(server_collation->get_value(&allocator, NULL, server_collation_obj))) {
      LOG_WARN("fail to get value", K(ret));
    } else {
      server_collation_type = static_cast<ObCollationType>(server_collation_obj.get_int());
      if (ObCharset::is_valid_collation(server_collation_type)
          && OB_FAIL(databuff_printf(buf, buf_len, pos, "charset=%s, ",
                                     ObCharset::charset_name(server_collation_type)))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print default charset", K(ret), K(server_collation_type));
      }
    }
  }

  if (OB_SUCC(ret)) {// replica_num
    int64_t paxos_replica_num = OB_INVALID_COUNT;
    if (OB_FAIL(tenant_schema->get_paxos_replica_num(schema_guard_, paxos_replica_num))) {
      LOG_WARN("fail to get paxos replica num", K(ret));
    } else if (OB_UNLIKELY(paxos_replica_num < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("paxos replica num error", K(ret), K(paxos_replica_num),
               "tenant_id", tenant_schema->get_tenant_id());
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "replica_num=%ld, ", paxos_replica_num))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print default charset", K(ret));
    } else {} // no more to do
  }
  if (OB_SUCC(ret)) {// zone_list
    common::ObArray<common::ObZone> zone_list;
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "zone_list("))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print zone_list", K(ret));
    } else if (OB_FAIL(tenant_schema->get_zone_list(zone_list))) {
      SHARE_SCHEMA_LOG(WARN, "fail to get zone list", K(ret));
    } else {
      int64_t i = 0;
      for (i = 0 ; OB_SUCC(ret) && i < zone_list.count() - 1; i++) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\'%.*s\', ",
                                                 static_cast<int32_t>(zone_list.at(i).size()),
                                                 zone_list.at(i).ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print zon_list", K(ret), K(zone_list.at(i)));
        }
      }
      if (OB_SUCCESS == ret && zone_list.count() > 0) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\'%.*s\'), ",
                                                 static_cast<int32_t>(zone_list.at(i).size()),
                                                 zone_list.at(i).ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print zon_list", K(ret), K(zone_list.at(i)));
        }
      }
    }
  }
  if (OB_SUCCESS == ret && 0 < tenant_schema->get_primary_zone().length()) { //primary_zone
    bool is_random = (0 == tenant_schema->get_primary_zone().compare(common::OB_RANDOM_PRIMARY_ZONE));
    if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                is_random ? "primary_zone=%.*s, " : "primary_zone=\'%.*s\', ",
                                tenant_schema->get_primary_zone().length(),
                                tenant_schema->get_primary_zone().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print primary_zone", K(ret), K(tenant_schema->get_primary_zone()));
    }
  }
  if (OB_SUCCESS == ret && tenant_schema->get_locality_str().length() > 0) { // locality
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "locality=\'%.*s\', ",
                                             tenant_schema->get_locality_str().length(),
                                             tenant_schema->get_locality_str().ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print locality", K(ret), K(tenant_schema->get_locality_str()));
    }
  }
  if (OB_SUCC(ret)) { //resource_pool_list
    ObUnitInfoGetter ui_getter;
    typedef ObArray<ObResourcePool> PoolList;
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
      for ( i = 0 ; OB_SUCC(ret) && i < pool_list.count() - 1; i++) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\'%s\', ",
                                                 pool_list.at(i).name_.ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print resource_pool_list", K(ret), K(pool_list.at(i)));
        }
      }
      if (OB_SUCCESS == ret && pool_list.count() > 0) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\'%s\')",
                                                 pool_list.at(i).name_.ptr()))) {
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

  if (OB_SUCC(ret) && lib::is_oracle_mode() && is_agent_mode) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " set ob_tcp_invited_nodes='%%', ob_compatibility_mode='oracle'"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print tenant ob_compatibility_mode", K(ret));
    } else if (OB_FAIL(add_create_tenant_variables(tenant_id, sql_proxy, buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "failed to add create tenant variables", K(ret));
    }
  }
  return ret;
}

int ObSchemaPrinter::add_create_tenant_variables(
    const uint64_t tenant_id, common::ObMySQLProxy *const sql_proxy,
    char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  HEAP_VAR(ObMySQLProxy::ReadResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
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
        " where tenant_id= %lu;", tenant_id))) {
      OB_LOG(WARN, "fail to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy->read(res, sql.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", K(ret));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", K(ret));
    }

    while(OB_SUCC(ret)) {
      flags = ObSysVarFlag::NONE;
      data_type = ObNullType;
      int_value = INT64_MAX;
      uint_value = UINT64_MAX;
      if (OB_FAIL(result->next())) {
        if(OB_ITER_END != ret) {
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
        if(((flags & ObSysVarFlag::GLOBAL_SCOPE) != 0) && ((flags & ObSysVarFlag::WITH_CREATE) != 0)) {
          //only backup global and not read only variable
          if(OB_FAIL(result->get_varchar("name", name))) {
            OB_LOG(WARN, "fail to get name", K(ret));
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", %.*s ",
              name.length(), name.ptr()))) {
            OB_LOG(WARN, "fail to print name", K(name), K(ret));
          } else if(OB_FAIL(result->get_int("data_type", data_type))) {
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
              } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " = '%.*s'", string_value.length(),
                  string_value.ptr()))) {
                OB_LOG(WARN, "fail to string value", K(string_value), K(ret));
              }
            } else if (ObNumberType == data_type) {
              if (OB_FAIL(result->get_varchar("value", string_value))) {
                OB_LOG(WARN, "fail to string value", K(ret));
              } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " = %.*s", string_value.length(),
                  string_value.ptr()))) {
                OB_LOG(WARN, "fail to string value", K(string_value), K(ret));
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              OB_LOG(WARN, "this variables type is not support", K(ret), K(data_type));
            }
          }//else
        }
      }
    }//while
  }
  return ret;
}

int ObSchemaPrinter::print_element_type(const uint64_t tenant_id,
                                        const uint64_t element_type_id,
                                        const ObUDTBase *elem_type_info,
                                        char* buf,
                                        const int64_t& buf_len,
                                        int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (elem_type_info->is_base_type()) {
    int64_t type_pos = 0;
    char type_str[OB_MAX_SYS_PARAM_NAME_LENGTH];
    bzero(type_str, OB_MAX_SYS_PARAM_NAME_LENGTH);
    OZ (ob_sql_type_str(type_str,
                        OB_MAX_SYS_PARAM_NAME_LENGTH,
                        type_pos,
                        static_cast<ObObjType>(element_type_id),
                        elem_type_info->get_length(),
                        elem_type_info->get_precision(),
                        elem_type_info->get_scale(),
                        static_cast<ObCollationType>(elem_type_info->get_coll_type())),
                        elem_type_info, element_type_id);
    OZ (databuff_printf(buf, buf_len, pos, "%s", type_str));
  } else {
    const ObUDTTypeInfo *udt_info = NULL;
    const ObDatabaseSchema *db_schema = NULL;
    OZ (schema_guard_.get_udt_info(tenant_id, element_type_id, udt_info));
    if (OB_SUCC(ret) && OB_ISNULL(udt_info)) {
      ret = OB_ERR_SP_UNDECLARED_TYPE;
      SHARE_SCHEMA_LOG(WARN, "Unknow type", K(ret), K(element_type_id));
    }
    OZ (schema_guard_.get_database_schema(tenant_id, udt_info->get_database_id(), db_schema));
    if (OB_SUCC(ret) && OB_ISNULL(db_schema)) {
      ret = OB_ERR_BAD_DATABASE;
      SHARE_SCHEMA_LOG(WARN, "Unknow database", K(ret), K(udt_info->get_database_id()));
    }
    OZ (databuff_printf(buf, buf_len, pos,
                        "\"%.*s\".\"%.*s\"",
                        db_schema->get_database_name_str().length(),
                        db_schema->get_database_name_str().ptr(),
                        udt_info->get_type_name().length(),
                        udt_info->get_type_name().ptr()));
  }
  return ret;
}

int ObSchemaPrinter::print_udt_definition(const uint64_t tenant_id,
                                          const uint64_t udt_id,
                                          char* buf,
                                          const int64_t& buf_len,
                                          int64_t& pos) const
{
  int ret = OB_SUCCESS;
  const ObUDTTypeInfo *udt_info = NULL;
  const ObDatabaseSchema *db_schema = NULL;
  CK (lib::is_oracle_mode());
  OZ (schema_guard_.get_udt_info(tenant_id, udt_id, udt_info));
  if (OB_SUCC(ret) && OB_ISNULL(udt_info)) {
    ret = OB_ERR_SP_UNDECLARED_TYPE;
    SHARE_SCHEMA_LOG(WARN, "Unknow type", K(ret), K(udt_id));
  }
  OZ (schema_guard_.get_database_schema(tenant_id, udt_info->get_database_id(), db_schema));
  if (OB_SUCC(ret) && OB_ISNULL(db_schema)) {
    ret = OB_ERR_BAD_DATABASE;
    SHARE_SCHEMA_LOG(WARN, "Unknow database", K(ret), K(udt_info->get_database_id()));
  }
  OZ (databuff_printf(buf, buf_len, pos,
                      "CREATE OR REPLACE%s TYPE \"%.*s\".\"%.*s\" ",
                      udt_info->is_noneditionable() ? " NONEDITIONABLE" : "",
                      db_schema->get_database_name_str().length(),
                      db_schema->get_database_name_str().ptr(),
                      udt_info->get_type_name().length(),
                      udt_info->get_type_name().ptr()));
  if (OB_FAIL(ret)) {
  } else if (udt_info->is_collection()) {
    if (udt_info->is_varray()) {
      OZ (databuff_printf(buf, buf_len, pos, " IS VARRAY(%ld) OF ",
                          udt_info->get_coll_info()->get_upper_bound()));
    } else {
      OZ (databuff_printf(buf, buf_len, pos, " IS TABLE OF "));
    }
    OZ (print_element_type(tenant_id,
                           udt_info->get_coll_info()->get_elem_type_id(),
                           udt_info->get_coll_info(),
                           buf, buf_len, pos));
  } else {
    const common::ObIArray<ObUDTObjectType*> &udt_src = udt_info->get_object_type_infos();
    if (0 == udt_src.count()) {
      OZ (databuff_printf(buf, buf_len, pos, " IS OBJECT ( \n"));
      for (int64_t i = 0; OB_SUCC(ret) && i < udt_info->get_attributes(); ++i) {
        ObUDTTypeAttr* attr = udt_info->get_attrs().at(i);
        CK (OB_NOT_NULL(attr));
        OZ (databuff_printf(buf, buf_len, pos, "  \"%.*s\" ",
                            attr->get_name().length(), attr->get_name().ptr()));
        OZ (print_element_type(tenant_id,
                               attr->get_type_attr_id(), attr,
                               buf, buf_len, pos));
        if (OB_SUCC(ret)) {
          if (i != udt_info->get_attributes() - 1) {
            OZ (databuff_printf(buf, buf_len, pos, ",\n"));
          } else {
            OZ (databuff_printf(buf, buf_len, pos, ")\n"));
          }
        }
      }
    } else {
      const ObUDTObjectType *udt_spec = NULL;
      udt_spec = udt_src.at(0);
      CK (0 < udt_src.count() && OB_NOT_NULL(udt_spec));
      OZ (print_object_definition(udt_spec, buf, buf_len, pos));
      OZ (databuff_printf(buf, buf_len, pos, "\n"));
    }
  }
  SHARE_SCHEMA_LOG(DEBUG, "print user define type schema", K(ret), K(*udt_info));
  return ret;
}

int ObSchemaPrinter::print_udt_body_definition(const uint64_t tenant_id,
                                               const uint64_t udt_id,
                                               char* buf,
                                               const int64_t& buf_len,
                                               int64_t& pos,
                                               bool &body_exist) const
{
  int ret = OB_SUCCESS;
  const ObUDTTypeInfo *udt_info = NULL;
  const ObUDTObjectType *udt_body_info = NULL;
  const ObDatabaseSchema *db_schema = NULL;
  body_exist = false;
  CK (lib::is_oracle_mode());
  OZ (schema_guard_.get_udt_info(tenant_id, udt_id, udt_info));
  if (OB_SUCC(ret) && OB_ISNULL(udt_info)) {
    ret = OB_ERR_SP_UNDECLARED_TYPE;
    SHARE_SCHEMA_LOG(WARN, "Unknow type", K(ret), K(udt_id));
  }
  OZ (schema_guard_.get_database_schema(tenant_id, udt_info->get_database_id(), db_schema));
  if (OB_SUCC(ret) && OB_ISNULL(db_schema)) {
    ret = OB_ERR_BAD_DATABASE;
    SHARE_SCHEMA_LOG(WARN, "Unknow database", K(ret), K(udt_info->get_database_id()));
  }
  if (OB_FAIL(ret)) {
  } else if (udt_info->get_object_type_infos().count() != 2) {
  } else if (OB_ISNULL(udt_body_info = udt_info->get_object_type_infos().at(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udt body info is null", K(ret));
  } else {
    // OZ (databuff_printf(buf, buf_len, pos, "CREATE "));
    // OZ (databuff_printf(buf, buf_len, pos, "%.*s",
    //                     udt_body_info->get_source().length(), udt_body_info->get_source().ptr()));
    OZ (databuff_printf(buf, buf_len, pos,
                      "CREATE OR REPLACE%s TYPE BODY \"%.*s\".\"%.*s\" IS \n",
                      udt_info->is_noneditionable() ? " NONEDITIONABLE" : "",
                      db_schema->get_database_name_str().length(),
                      db_schema->get_database_name_str().ptr(),
                      udt_info->get_type_name().length(),
                      udt_info->get_type_name().ptr()));
    OZ (print_object_definition(udt_body_info, buf, buf_len, pos));
    OZ (databuff_printf(buf, buf_len, pos, "\nEND;\n"));
    body_exist = true;
  }
  SHARE_SCHEMA_LOG(DEBUG, "print user define type schema", K(ret), K(*udt_info));
  return ret;
}

int ObSchemaPrinter::print_object_definition(const ObUDTObjectType *object,
                                             char *buf,
                                             const int64_t &buf_len,
                                             int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  pl::ObPLParser parser(allocator, ObCharsets4Parser());
  ObStmtNodeTree *object_stmt = NULL;
  const ParseNode *src_node = NULL;
  ObString object_src;
  CK (!object->get_source().empty());
  OZ (parser.parse_package(object->get_source(), object_stmt, ObDataTypeCastParams(), NULL, false));
  CK (OB_NOT_NULL(object_stmt));
  CK (T_STMT_LIST == object_stmt->type_);
  OX (src_node = object_stmt->children_[0]);
  if (OB_SUCC(ret) && T_SP_PRE_STMTS == src_node->type_) {
    OZ (pl::ObPLResolver::resolve_condition_compile(
     allocator,
     NULL,
     &schema_guard_,
     NULL,
     NULL,
     &(object->get_exec_env()),
     src_node,
     src_node,
     true /*inner_parse*/));
  }
  CK (OB_NOT_NULL(src_node));
  OX (object_src = ObString(src_node->str_len_, src_node->str_value_));
  CK (!object_src.empty());
  OZ (databuff_printf(buf, buf_len, pos, "%.*s",
                      object_src.length(),
                      object_src.ptr()));
  SHARE_SCHEMA_LOG(DEBUG, "print object schema", K(ret), K(*object));
  return ret;
}

int ObSchemaPrinter::print_routine_param_type(const ObRoutineParam *param,
                                              const ObStmtNodeTree *param_type,
                                              char *buf,
                                              const int64_t &buf_len,
                                              int64_t &pos,
                                              const ObTimeZoneInfo *tz_info) const
{
  int ret = OB_SUCCESS;
  UNUSED(tz_info);
  CK (OB_NOT_NULL(param));
  if (OB_FAIL(ret)) {
  } else if (param->is_extern_type()) {
    ObParamExternType type = param->get_extern_type_flag();
    const ObDatabaseSchema *database_schema = NULL;
    CK (lib::is_oracle_mode());
    CK (!param->get_type_name().empty());
    if (OB_FAIL(ret)) {
    } else if (param->is_sys_refcursor_type()) {
      OZ (databuff_printf(buf, buf_len, pos, " %.*s", param->get_type_name().length(), param->get_type_name().ptr()));
    } else {
      CK (param->get_type_owner() != OB_INVALID_ID);
      OZ (schema_guard_.get_database_schema(param->get_tenant_id(),
                                          param->get_type_owner(),
                                          database_schema));
      if (OB_SUCC(ret))  {
        if (param->get_type_subname().empty()) {
          OZ (databuff_printf(buf, buf_len, pos, " \"%.*s\".\"%.*s\"",
                              database_schema->get_database_name_str().length(),
                              database_schema->get_database_name_str().ptr(),
                              param->get_type_name().length(),
                              param->get_type_name().ptr()));
        } else if (is_oceanbase_sys_database_id(param->get_type_owner())
                  && 0 == param->get_type_subname().case_compare("STANDARD")) {
          OZ (databuff_printf(buf, buf_len, pos, " %.*s",
                              param->get_type_name().length(),
                              param->get_type_name().ptr()));
        } else {
          OZ (databuff_printf(buf, buf_len, pos, " \"%.*s\".\"%.*s\".\"%.*s\"",
                              database_schema->get_database_name_str().length(),
                              database_schema->get_database_name_str().ptr(),
                              param->get_type_subname().length(),
                              param->get_type_subname().ptr(),
                              param->get_type_name().length(),
                              param->get_type_name().ptr()));
        }
      }
      if (OB_SUCC(ret)) {
        if (SP_EXTERN_PKG_VAR == type
            || SP_EXTERN_TAB_COL == type
            || SP_EXTERN_PKGVAR_OR_TABCOL == type) {
          OZ (databuff_printf(buf, buf_len, pos, "%%TYPE"));
        } else if (SP_EXTERN_TAB == type) {
          OZ (databuff_printf(buf, buf_len, pos, "%%ROWTYPE"));
        }
      }
    }
  } else if (param->is_pl_integer_type()) {
    pl::ObPLIntegerType type = param->get_pl_integer_type();
    switch (type) {
      case pl::ObPLIntegerType::PL_PLS_INTEGER:
        OZ (databuff_printf(buf, buf_len, pos, " PLS_INTEGER"));
        break;
      case pl::ObPLIntegerType::PL_BINARY_INTEGER:
        OZ (databuff_printf(buf, buf_len, pos, " BINARY_INTEGER"));
        break;
      case pl::ObPLIntegerType::PL_NATURAL:
        OZ (databuff_printf(buf, buf_len, pos, " NATURAL"));
        break;
      case pl::ObPLIntegerType::PL_NATURALN:
        OZ (databuff_printf(buf, buf_len, pos, " NATURALN"));
        break;
      case pl::ObPLIntegerType::PL_POSITIVE:
        OZ (databuff_printf(buf, buf_len, pos, " POSITIVE"));
        break;
      case pl::ObPLIntegerType::PL_POSITIVEN:
        OZ (databuff_printf(buf, buf_len, pos, " POSITIVEN"));
        break;
      case pl::ObPLIntegerType::PL_SIGNTYPE:
        OZ (databuff_printf(buf, buf_len, pos, " SIGNTYPE"));
        break;
      case pl::ObPLIntegerType::PL_SIMPLE_INTEGER:
        OZ (databuff_printf(buf, buf_len, pos, " SIMPLE_INTEGER"));
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected pls integer type", K(type));
        break;
    }
    CK (lib::is_oracle_mode());
  } else {
    if (lib::is_mysql_mode()) {
      int64_t type_pos = 0;
      char type_str[OB_MAX_SYS_PARAM_NAME_LENGTH];
      bzero(type_str, OB_MAX_SYS_PARAM_NAME_LENGTH);
      OZ (ob_sql_type_str(type_str,
                          OB_MAX_SYS_PARAM_NAME_LENGTH,
                          type_pos,
                          param->get_param_type().get_obj_type(),
                          param->get_param_type().get_length(),
                          param->get_param_type().get_precision(),
                          param->get_param_type().get_scale(),
                          param->get_param_type().get_collation_type()));
      OZ (databuff_printf(buf, buf_len, pos, " %s", type_str));
    } else {
      ObString type_str;
      CK (OB_NOT_NULL(param_type));
      OX (type_str = ObString(param_type->str_len_, param_type->str_value_));
      CK (!type_str.empty());
      OZ (databuff_printf(buf, buf_len, pos, " %.*s", type_str.length(), type_str.ptr()));
    }
  }
  return ret;
}

int ObSchemaPrinter::print_routine_definition_param_v1(const ObRoutineInfo &routine_info,
                                                    const ObStmtNodeTree *param_list,
                                                    char* buf,
                                                    const int64_t& buf_len,
                                                    int64_t& pos,
                                                    const ObTimeZoneInfo *tz_info) const
{
  int ret = OB_SUCCESS;
  bool is_first_param = true;
  bool is_oracle_mode = lib::is_oracle_mode();

  if (is_oracle_mode) {
    CK (OB_NOT_NULL(param_list));
    CK (T_SP_PARAM_LIST == param_list->type_);
    CK (param_list->num_child_ == routine_info.get_param_count());
  }

  for(int64_t i = 0; OB_SUCC(ret) && i < routine_info.get_param_count(); ++i) {
    ObRoutineParam *param = NULL;
    OZ (routine_info.get_routine_param(i, param));
    CK (OB_NOT_NULL(param));
    CK (!param->is_ret_param());

    if (OB_SUCC(ret)) {
      if (is_first_param) {
        is_first_param = false;
      } else {
        OZ (databuff_printf(buf, buf_len, pos, ","));
      }
    }

    if (OB_SUCC(ret) && !is_oracle_mode && !routine_info.is_function()) {
      if (param->is_out_sp_param() || param->is_inout_sp_param()) {
        OZ (databuff_printf(buf, buf_len, pos, param->is_out_sp_param() ? " OUT" : " INOUT"));
      } else {
        OZ (databuff_printf(buf, buf_len, pos, " IN"));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " "))) {
        LOG_WARN("failed to print const str", K(ret));
      } else if (OB_FAIL(print_identifier(buf, buf_len, pos, param->get_param_name(), is_oracle_mode))) {
        LOG_WARN("failed to print param", K(ret));
      }
    }

    if (OB_SUCC(ret) && is_oracle_mode) {
      if (param->is_out_sp_param() || param->is_inout_sp_param()) {
        OZ (databuff_printf(buf, buf_len, pos, param->is_out_sp_param() ? " OUT" : " IN OUT"));
      } else {
        OZ (databuff_printf(buf, buf_len, pos, " IN"));
      }
    }

    if (is_oracle_mode) {
      ObStmtNodeTree *param_node = NULL;
      ObStmtNodeTree *type_node = NULL;
      CK (OB_NOT_NULL(param_node = param_list->children_[i]));
      CK (T_SP_PARAM == param_node->type_);
      CK (OB_NOT_NULL(type_node = param_node->children_[1]));
      OZ (print_routine_param_type(param, type_node, buf, buf_len, pos, tz_info));
    } else {
      OZ (print_routine_param_type(param, NULL, buf, buf_len, pos, tz_info));
    }

    if (OB_SUCC(ret) && !param->get_default_value().empty()) {
      OZ (databuff_printf(buf,
                          buf_len,
                          pos,
                          " DEFAULT %.*s",
                          param->get_default_value().length(),
                          param->get_default_value().ptr()));
    }
  }
  return ret;
}

int ObSchemaPrinter::print_routine_definition_v1(const ObRoutineInfo *routine_info,
                                              const ObStmtNodeTree *param_list,
                                              const ObStmtNodeTree *return_type,
                                              const ObString &body,
                                              const ObString &clause,
                                              char* buf,
                                              const int64_t& buf_len,
                                              int64_t &pos,
                                              const ObTimeZoneInfo *tz_info) const
{
  int ret = OB_SUCCESS;
  const char *routine_type = NULL;
  const ObString db_name;
  const ObDatabaseSchema *db_schema = NULL;
  CK (OB_NOT_NULL(routine_info));
  CK (!body.empty());
  OZ (schema_guard_.get_database_schema(routine_info->get_tenant_id(),
      routine_info->get_database_id(), db_schema), routine_info);
  if (OB_SUCC(ret) && OB_ISNULL(db_schema)) {
    ret = OB_ERR_BAD_DATABASE;
    SHARE_SCHEMA_LOG(WARN, "Unknow database", K(ret), K(routine_info->get_database_id()));
  }
  OX (routine_type = routine_info->is_procedure() ? "PROCEDURE" : "FUNCTION");
  OZ (databuff_printf(buf, buf_len, pos,
                      lib::is_oracle_mode() ?
                      "CREATE OR REPLACE%s%.*s %s " :
                      "CREATE DEFINER =%s %.*s %s ",
                      routine_info->is_noneditionable() ? " NONEDITIONABLE" : "",
                      routine_info->get_priv_user().length(),
                      routine_info->get_priv_user().ptr(),
                      routine_type));
  OZ (print_identifier(buf, buf_len, pos, db_schema->get_database_name_str(), lib::is_oracle_mode()),
                       K(routine_type), K(db_name), K(routine_info));
  OZ (databuff_printf(buf, buf_len, pos, "."));
  OZ (print_identifier(buf, buf_len, pos, routine_info->get_routine_name(), lib::is_oracle_mode()),
                       K(routine_type), K(db_name), K(routine_info));
  OZ (databuff_printf(buf, buf_len, pos, "\n"));
  if (OB_SUCC(ret) && routine_info->get_param_count() > 0) {
    OZ (databuff_printf(buf, buf_len, pos, "(\n"));
    OZ (print_routine_definition_param_v1(*routine_info, param_list, buf, buf_len, pos, tz_info));
    OZ (databuff_printf(buf, buf_len, pos, "\n)"));
  } else {
    if (lib::is_mysql_mode()) {
      OZ (databuff_printf(buf, buf_len, pos, "()\n"));
    }
  }
  if (OB_SUCC(ret) && routine_info->is_function()) {
    const ObRoutineParam *routine_param = NULL;
    OZ (databuff_printf(buf, buf_len, pos, lib::is_oracle_mode() ? " RETURN" : " RETURNS"));
    OX (routine_param = static_cast<const ObRoutineParam*>(routine_info->get_ret_info()));
    OZ (print_routine_param_type(routine_param, return_type, buf, buf_len, pos, tz_info));
  }
  if (OB_SUCC(ret) && lib::is_mysql_mode()) {
    if (routine_info->is_no_sql()) {
      OZ (databuff_printf(buf, buf_len, pos, "\nNO SQL"));
    } else if (routine_info->is_reads_sql_data()) {
      OZ (databuff_printf(buf, buf_len, pos, "\nREADS SQL DATA"));
    } else if (routine_info->is_modifies_sql_data()) {
      OZ (databuff_printf(buf, buf_len, pos, "\nMODIFIES SQL DATA"));
    }
    OZ (databuff_printf(buf, buf_len, pos, "%s",
        routine_info->is_deterministic() ? "\nDETERMINISTIC" : ""));
    OZ (databuff_printf(buf, buf_len, pos, "%s",
        routine_info->is_invoker_right() ? "\nINVOKER" : ""));
    if (OB_SUCC(ret) && OB_NOT_NULL(routine_info->get_comment())) {
      OZ (databuff_printf(buf, buf_len, pos, "\nCOMMENT "));
      OZ (print_identifier(buf, buf_len, pos, routine_info->get_comment(), false));
      OZ (databuff_printf(buf, buf_len, pos, "\n"));
    }
  }
  if (OB_SUCC(ret) && lib::is_oracle_mode() && !clause.empty()) {
    OZ (databuff_printf(buf, buf_len, pos, " %.*s\n", clause.length(), clause.ptr()));
  }
  OZ (databuff_printf(buf, buf_len, pos,
      lib::is_oracle_mode() ? (routine_info->is_aggregate() ? "\nAGGREGATE USING %.*s" : " IS\n%.*s")
                              : " %.*s", body.length(), body.ptr()));
  return ret;
}

int ObSchemaPrinter::print_routine_definition(
    const uint64_t tenant_id,
    const uint64_t routine_id,
    const sql::ObExecEnv &exec_env,
    char* buf,
    const int64_t& buf_len,
    int64_t& pos,
    const ObTimeZoneInfo *tz_info) const
{
  int ret = OB_SUCCESS;
  const ObRoutineInfo *routine_info = NULL;
  bool use_v1 = true;
  OZ (schema_guard_.get_routine_info(tenant_id, routine_id, routine_info), routine_id);
  if (OB_SUCC(ret) && OB_ISNULL(routine_info)) {
    ret = OB_ERR_SP_DOES_NOT_EXIST;
    SHARE_SCHEMA_LOG(WARN, "Unknow routine", K(ret), K(routine_id));
  }

  if (OB_SUCC(ret)) {
    ObArenaAllocator allocator;
    ObString routine_stmt;
    ParseResult parse_result;
    const ObString &routine_body = routine_info->get_routine_body();
    CK(!routine_body.empty());

    // TODO: disable Oracle mode for OB-JDBC compatibility, will enable it soon
    if (OB_FAIL(ret) || lib::is_oracle_mode()) {
      // do nothing
    } else if (routine_info->get_routine_body().prefix_match_ci("procedure")
               || routine_info->get_routine_body().prefix_match_ci("function")) {
      use_v1 = false;

      pl::ObPLParser parser(allocator, ObCharsets4Parser(), exec_env.get_sql_mode());

      if (lib::is_mysql_mode()) {
        const char prefix[] = "CREATE\n";
        int64_t prefix_len = STRLEN(prefix);
        int64_t buf_sz = prefix_len + routine_body.length();
        char *stmt_buf = static_cast<char *>(allocator.alloc(buf_sz));
        if (OB_ISNULL(stmt_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for routine body buffer", K(buf_sz));
        } else {
          MEMCPY(stmt_buf, prefix, prefix_len);
          MEMCPY(stmt_buf + prefix_len, routine_body.ptr(), routine_body.length());

          routine_stmt.assign_ptr(stmt_buf, buf_sz);
        }
      } else { // oracle mode
        routine_stmt = routine_body;
      }
      CK(!routine_stmt.empty());

      OZ (parser.parse(routine_stmt, routine_stmt, parse_result, true));
    }

    if (OB_SUCC(ret) && !use_v1) {
      if (lib::is_mysql_mode()) { // mysql mode
        if (OB_FAIL(print_routine_definition_v2_mysql(
                      *routine_info,
                      parse_result.result_tree_,
                      exec_env,
                      buf,
                      buf_len,
                      pos,
                      tz_info))) {
          LOG_WARN("failed to print definition for mysql routine", K(*routine_info));
        }
      } else { // TODO: oracle mode, never use this branch for now
        if (OB_FAIL(print_routine_definition_v2_oracle(
                      *routine_info,
                      parse_result.result_tree_,
                      buf,
                      buf_len,
                      pos,
                      tz_info))) {
          LOG_WARN("failed to print definition for oracle routine", K(*routine_info));
        }
      }
    }
  }

  if (OB_FAIL(ret) || !use_v1) {
    //do nothing
  } else if (lib::is_mysql_mode()) {
    ObString clause;
    OZ (print_routine_definition_v1(routine_info, NULL, NULL, routine_info->get_routine_body(), clause, buf, buf_len, pos, tz_info));
  } else { // oracle mode
    ObString routine_body = routine_info->get_routine_body();
    ObString actully_body;
    ObString routine_clause;
    ObStmtNodeTree *parse_tree = NULL;
    const ObStmtNodeTree *routine_tree = NULL;
    ObArenaAllocator allocator;
    pl::ObPLParser parser(allocator, ObCharsets4Parser());
    ObStmtNodeTree *param_list = NULL;
    ObStmtNodeTree *return_type = NULL;
    ObStmtNodeTree *clause_list = NULL;
    CK (!routine_body.empty());
    OZ (parser.parse_routine_body(routine_body, parse_tree, false));
    CK (OB_NOT_NULL(parse_tree));
    CK (T_STMT_LIST == parse_tree->type_);
    CK (1 == parse_tree->num_child_);
    CK (OB_NOT_NULL(parse_tree->children_[0]));
    OX (routine_tree = parse_tree->children_[0]);
    if (OB_SUCC(ret) && T_SP_PRE_STMTS == routine_tree->type_) {
      OZ (pl::ObPLResolver::resolve_condition_compile(
        allocator,
        NULL,
        &schema_guard_,
        NULL,
        NULL,
        &(routine_info->get_exec_env()),
        routine_tree,
        routine_tree,
        true /*inner_parse*/));
    }
    CK (OB_NOT_NULL(routine_tree));
    LOG_INFO("print routine define", K(routine_tree->type_), K(routine_info->is_function()), K(routine_body));
    CK (routine_info->is_function() ? T_SF_SOURCE == routine_tree->type_
                                      || T_SF_AGGREGATE_SOURCE == routine_tree->type_
                                    : T_SP_SOURCE == routine_tree->type_);
    CK (routine_info->is_function() ? 6 == routine_tree->num_child_
                                    : 4 == routine_tree->num_child_);
    CK (routine_info->is_function() ? OB_NOT_NULL(routine_tree->children_[5])
                                    : OB_NOT_NULL(routine_tree->children_[3]));
    OX (actully_body = routine_info->is_function() ?
          ObString(routine_tree->children_[5]->str_len_, routine_tree->children_[5]->str_value_)
        : ObString(routine_tree->children_[3]->str_len_, routine_tree->children_[3]->str_value_));
    OX (clause_list = routine_info->is_function() ? routine_tree->children_[3] : routine_tree->children_[2]);
    if (OB_SUCC(ret) && OB_NOT_NULL(clause_list)) {
      OX (routine_clause = ObString(clause_list->str_len_, clause_list->str_value_));
    }
    OX (param_list = routine_tree->children_[1]);
    OX (return_type = (routine_info->is_function() ? routine_tree->children_[2] : NULL));
    CK (!actully_body.empty());
    OZ (print_routine_definition_v1(routine_info, param_list, return_type, actully_body, routine_clause, buf, buf_len, pos, tz_info));
  }
  return ret;
}

int ObSchemaPrinter::print_routine_definition_v2_mysql(
    const ObRoutineInfo &routine_info,
    const ObStmtNodeTree *parse_tree,
    const ObExecEnv &exec_env,
    char* buf,
    const int64_t& buf_len,
    int64_t& pos,
    const common::ObTimeZoneInfo *tz_info) const
{
  UNUSED(tz_info);
  int ret = OB_SUCCESS;

  ObString priv_user;
  ObString user_name;
  ObString host_name;

  ParseNode *create_node = nullptr;
  ParseNode *body_node = nullptr;

  bool is_ansi_quote = false;
  IS_ANSI_QUOTES(exec_env.get_sql_mode(), is_ansi_quote);

  // the quote character is determined by SQL_MODE ANSI_QUOTES of routine creation
  const char *quote_char = is_ansi_quote ? "\"" : "`";

  // use quote or not is determined by SQL_QUOTE_SHOW_CREATE variable of the SHOW CREATE statement execution
  const char *quote = sql_quote_show_create_ ? quote_char : "";

  CK (OB_NOT_NULL(parse_tree) && T_STMT_LIST == parse_tree->type_ && 1 == parse_tree->num_child_);
  OX (create_node = parse_tree->children_[0]);
  CK (OB_NOT_NULL(create_node));
  CK (T_SP_CREATE == create_node->type_ || T_SF_CREATE == create_node->type_);
  OX (body_node = routine_info.is_procedure() ? create_node->children_[4]
                                              : create_node->children_[5]);
  CK (OB_NOT_NULL(body_node));
  CK (OB_NOT_NULL(body_node->raw_text_));

  OX (priv_user = routine_info.get_priv_user());
  OX (user_name = priv_user.split_on('@'));
  OX (host_name = priv_user);

  // quote around username is controlled by SQL_QUOTE_SHOW_CREATE variable,
  // but quote around hostname is always shown, no matter what SQL_QUOTE_SHOW_CREATE is.
  OZ (databuff_printf(buf, buf_len, pos, "CREATE DEFINER = %s%.*s%s@%s%.*s%s %s ",
                      quote,
                      user_name.length(),
                      user_name.ptr(),
                      quote,
                      quote_char,
                      host_name.length(),
                      host_name.ptr(),
                      quote_char,
                      routine_info.is_procedure() ? "PROCEDURE" : "FUNCTION"));

  OZ (databuff_printf(buf, buf_len, pos, "%s%.*s%s",
                      quote,
                      routine_info.get_routine_name().length(),
                      routine_info.get_routine_name().ptr(),
                      quote),
      K(buf), K(buf_len), K(routine_info));

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (create_node->children_[2] != nullptr) {
    const ParseNode &param_node = *create_node->children_[2];
    OZ (databuff_printf(buf, buf_len, pos, "(%.*s)",
                        static_cast<int32_t>(param_node.str_len_),
                        param_node.str_value_),
        K(buf), K(buf_len), K(routine_info), K(param_node.str_value_), K(param_node.str_len_));
  } else {
    OZ (databuff_printf(buf, buf_len, pos, "()"),
        K(buf), K(buf_len), K(routine_info));
  }

  if (OB_SUCC(ret) && routine_info.is_function()) {
    const ObRoutineParam *return_type = nullptr;
    OZ (databuff_printf(buf, buf_len, pos, " RETURNS"));
    OX (return_type =
           static_cast<const ObRoutineParam *>(routine_info.get_ret_info()));
    OZ (print_routine_param_type(return_type, nullptr, buf, buf_len, pos, tz_info),
        K(buf), K(buf_len), K(routine_info));
  }

  if (OB_SUCC(ret)) {
    if (routine_info.is_no_sql()) {
      OZ (databuff_printf(buf, buf_len, pos, "\n    NO SQL"));
    } else if (routine_info.is_reads_sql_data()) {
      OZ (databuff_printf(buf, buf_len, pos, "\n    READS SQL DATA"));
    } else if (routine_info.is_modifies_sql_data()) {
      OZ (databuff_printf(buf, buf_len, pos, "\n    MODIFIES SQL DATA"));
    }
    OZ (databuff_printf(buf, buf_len, pos, "%s",
        routine_info.is_deterministic() ? "\n    DETERMINISTIC" : ""));
    OZ (databuff_printf(buf, buf_len, pos, "%s",
        routine_info.is_invoker_right() ? "\n    SQL SECURITY INVOKER" : ""));
    if (OB_SUCC(ret) && OB_NOT_NULL(routine_info.get_comment())) {
      OZ (databuff_printf(buf, buf_len, pos, "\n    COMMENT '%.*s'",
          routine_info.get_comment().length(),
          routine_info.get_comment().ptr()));
    }
  }

  OZ (databuff_printf(buf, buf_len, pos, "\n%.*s",
                      static_cast<int32_t>(body_node->text_len_),
                      body_node->raw_text_),
      K(buf), K(buf_len), K(routine_info), K(body_node->raw_text_), K(body_node->text_len_));

  return ret;
}

int ObSchemaPrinter::print_routine_definition_v2_oracle(
    const ObRoutineInfo &routine_info,
    const ObStmtNodeTree *parse_tree,
    char* buf,
    const int64_t& buf_len,
    int64_t& pos,
    const common::ObTimeZoneInfo *tz_info) const
{
  UNUSED(tz_info);
  int ret = OB_SUCCESS;
  const ParseNode *routine_tree = nullptr;
  ObArenaAllocator allocator;
  CK (OB_NOT_NULL(parse_tree) && T_STMT_LIST == parse_tree->type_ && 1 == parse_tree->num_child_);
  CK (OB_NOT_NULL(parse_tree->children_[0]));
  OX (routine_tree = parse_tree->children_[0]);
  if (OB_SUCC(ret) && T_SP_PRE_STMTS == routine_tree->type_) {
    OZ (pl::ObPLResolver::resolve_condition_compile(
      allocator,
      NULL,
      &schema_guard_,
      NULL,
      NULL,
      &(routine_info.get_exec_env()),
      routine_tree,
      routine_tree,
      true /*inner_parse*/));
  }
  CK (OB_NOT_NULL(routine_tree));
  CK (T_SP_SOURCE == routine_tree->type_ || T_SF_SOURCE == routine_tree->type_ || T_SF_AGGREGATE_SOURCE == routine_tree->type_);

  const ObString db_name;
  const ObDatabaseSchema *db_schema = nullptr;
  OZ (schema_guard_.get_database_schema(routine_info.get_tenant_id(),
      routine_info.get_database_id(), db_schema), routine_info);
  if (OB_SUCC(ret) && OB_ISNULL(db_schema)) {
    ret = OB_ERR_BAD_DATABASE;
    SHARE_SCHEMA_LOG(WARN, "Unknow database", K(ret), K(routine_info.get_database_id()));
  }
  OZ (databuff_printf(
       buf, buf_len, pos, "  CREATE OR REPLACE %s %s \"%.*s\".\"%.*s\" %.*s",
       routine_info.is_noneditionable() ? "NONEDITIONABLE" : "EDITIONABLE",
       routine_info.is_procedure() ? "PROCEDURE" : "FUNCTION",
       db_schema->get_database_name_str().length(),
       db_schema->get_database_name_str().ptr(),
       routine_info.get_routine_name().length(),
       routine_info.get_routine_name().ptr(),
       static_cast<int32_t>(routine_tree->str_len_),
       routine_tree->str_value_));

  return ret;
}

int ObSchemaPrinter::print_foreign_key_definition(
    const uint64_t tenant_id,
    const ObForeignKeyInfo &foreign_key_info,
    char *buf, int64_t buf_len, int64_t &pos) const
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
  const ObDatabaseSchema *child_database_schema = NULL;
  const ObDatabaseSchema *parent_database_schema = NULL;
  const ObTableSchema *child_table_schema = NULL;
  const ObTableSchema *parent_table_schema = NULL;
  const ObMockFKParentTableSchema *mock_fk_parent_table_schema = NULL;
  ObString foreign_key_name = foreign_key_info.foreign_key_name_;
  ObString child_database_name;
  ObString parent_database_name;
  ObString child_table_name;
  ObString parent_table_name;
  ObString child_column_name;
  ObString parent_column_name;
  const char *update_action_str = NULL;
  const char *delete_action_str = NULL;
  bool child_table_is_oracle_mode = false;
  bool parent_table_is_oracle_mode = false;
  bool is_oracle_mode = false;
  uint64_t parent_database_id = OB_INVALID_ID;
  OZ (schema_guard_.get_table_schema(
      tenant_id, foreign_key_info.child_table_id_, child_table_schema));
  OV (OB_NOT_NULL(child_table_schema), OB_TABLE_NOT_EXIST, foreign_key_info.child_table_id_);
  OZ (child_table_schema->check_if_oracle_compat_mode(child_table_is_oracle_mode));

  if (OB_SUCC(ret)) {
    if (foreign_key_info.is_parent_table_mock_) {
      if (OB_FAIL(schema_guard_.get_mock_fk_parent_table_schema_with_id(child_table_schema->get_tenant_id(), foreign_key_info.parent_table_id_, mock_fk_parent_table_schema))) {
        LOG_WARN("fail to check_mock_fk_parent_table_exist_by_id", K(ret));
      } else if (OB_ISNULL(mock_fk_parent_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("mock_fk_parent_table_schema is not exist or is null", K(ret), K(mock_fk_parent_table_schema), K(child_table_schema->get_tenant_id()), K(foreign_key_info.parent_table_id_));
      }
    } else {
      OZ (schema_guard_.get_table_schema(tenant_id, foreign_key_info.parent_table_id_, parent_table_schema));
      OV (OB_NOT_NULL(parent_table_schema), OB_TABLE_NOT_EXIST, foreign_key_info.parent_table_id_);
      OZ (parent_table_schema->check_if_oracle_compat_mode(parent_table_is_oracle_mode));
      OV (child_table_is_oracle_mode == parent_table_is_oracle_mode)
      OX (is_oracle_mode = child_table_is_oracle_mode)
    }
  }
  OZ (schema_guard_.get_database_schema(tenant_id, child_table_schema->get_database_id(), child_database_schema));
  OX (parent_database_id = foreign_key_info.is_parent_table_mock_ ? mock_fk_parent_table_schema->get_database_id() : parent_table_schema->get_database_id())
  OZ (schema_guard_.get_database_schema(tenant_id, parent_database_id, parent_database_schema));
  OV (OB_NOT_NULL(child_database_schema), OB_ERR_BAD_DATABASE, child_table_schema->get_database_id());
  OV (OB_NOT_NULL(parent_database_schema), OB_ERR_BAD_DATABASE, parent_database_id);

  OX (child_database_name = child_database_schema->get_database_name_str());
  OX (parent_database_name = parent_database_schema->get_database_name_str());
  OX (child_table_name = child_table_schema->get_table_name_str());
  OX (parent_table_name = foreign_key_info.is_parent_table_mock_ ? mock_fk_parent_table_schema->get_mock_fk_parent_table_name() : parent_table_schema->get_table_name_str());
  OV (!child_database_name.empty());
  OV (!parent_database_name.empty());
  OV (!child_table_name.empty());
  OV (!parent_table_name.empty());

  OX (BUF_PRINTF("ALTER TABLE "));
  OZ (print_identifier(buf, buf_len, pos, child_database_name, is_oracle_mode));
  OX (BUF_PRINTF("."));
  OZ (print_identifier(buf, buf_len, pos, child_table_name, is_oracle_mode));
  OX (BUF_PRINTF(" ADD CONSTRAINT "));

  if (!foreign_key_name.empty()) {
    OZ (print_identifier(buf, buf_len, pos, foreign_key_name, is_oracle_mode));
    OX (BUF_PRINTF(" "));
  }
  OX (BUF_PRINTF("FOREIGN KEY ("));
  OX (print_column_list(*child_table_schema, foreign_key_info.child_column_ids_, buf, buf_len, pos));

  OX (BUF_PRINTF(") REFERENCES "));
  OZ (print_identifier(buf, buf_len, pos, parent_database_name, is_oracle_mode));
  OX (BUF_PRINTF("."));
  OZ (print_identifier(buf, buf_len, pos, parent_table_name, is_oracle_mode));
  OX (BUF_PRINTF(" ("));

  if (OB_SUCC(ret)) {
    if (foreign_key_info.is_parent_table_mock_) {
      OX (print_column_list(*mock_fk_parent_table_schema, foreign_key_info.parent_column_ids_, buf, buf_len, pos));
    } else {
      OX (print_column_list(*parent_table_schema, foreign_key_info.parent_column_ids_, buf, buf_len, pos));
    }
  }
  OX (BUF_PRINTF(") "));
  if (!is_oracle_mode &&
      foreign_key_info.update_action_ != ACTION_RESTRICT &&
      foreign_key_info.update_action_ != ACTION_NO_ACTION) {
    OX (update_action_str = foreign_key_info.get_update_action_str());
    OV (OB_NOT_NULL(update_action_str), OB_ERR_UNEXPECTED, foreign_key_info.update_action_);
    OX (BUF_PRINTF("ON UPDATE %s ", update_action_str));
  }
  if (foreign_key_info.delete_action_ != ACTION_RESTRICT &&
      foreign_key_info.delete_action_ != ACTION_NO_ACTION) {
    OX (delete_action_str = foreign_key_info.get_delete_action_str());
    OV (OB_NOT_NULL(delete_action_str), OB_ERR_UNEXPECTED, foreign_key_info.delete_action_);
    OX (BUF_PRINTF("ON DELETE %s", delete_action_str));
  }
  if (is_oracle_mode) {
    OX (print_constraint_stat(foreign_key_info.rely_flag_, foreign_key_info.enable_flag_,
                              foreign_key_info.is_validated(), buf, buf_len, pos));
  }
  return ret;
}

int ObSchemaPrinter::print_trigger_definition(const ObTriggerInfo &trigger_info,
                                              char* buf, int64_t buf_len, int64_t &pos,
                                              bool get_ddl) const
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema *database_schema = NULL;
  if (lib::is_oracle_mode()) {
    if (!trigger_info.is_system_type()) {
      OZ (schema_guard_.get_database_schema(trigger_info.get_tenant_id(),
          trigger_info.get_database_id(), database_schema));
      CK (OB_NOT_NULL(database_schema));
      OZ (BUF_PRINTF("CREATE OR REPLACE TRIGGER \"%.*s\".\"%.*s\"",
                    database_schema->get_database_name_str().length(),
                    database_schema->get_database_name_str().ptr(),
                    trigger_info.get_trigger_name().length(),
                    trigger_info.get_trigger_name().ptr()),
          trigger_info.get_trigger_name());
      if (trigger_info.is_simple_dml_type()) {
        OZ (print_simple_trigger_definition(trigger_info, buf, buf_len, pos, get_ddl),
            trigger_info.get_trigger_name());
      } else {
        OZ (print_compound_instead_trigger_definition(trigger_info, buf, buf_len, pos, get_ddl),
            trigger_info.get_trigger_name());
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support print trigger type", K(trigger_info.get_trigger_type()), K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "not support print this trigger type");
    }
  } else {
    OZ (BUF_PRINTF("CREATE DEFINER = %.*s %.*s",
                   trigger_info.get_trigger_priv_user().length(),
                   trigger_info.get_trigger_priv_user().ptr(),
                   trigger_info.get_trigger_body().length(),
                   trigger_info.get_trigger_body().ptr()),
        trigger_info.get_trigger_name());
    OX (LOG_DEBUG("mysql trigger define", K(*buf)));
  }
  return ret;
}

int ObSchemaPrinter::print_simple_trigger_definition(const ObTriggerInfo &trigger_info,
                                                     char *buf, int64_t buf_len, int64_t &pos,
                                                     bool get_ddl) const
{
  int ret = OB_SUCCESS;
  bool has_print_event = false;
  OV (ObTriggerInfo::TriggerType::TT_SIMPLE_DML == trigger_info.get_trigger_type(),
      OB_ERR_UNEXPECTED, trigger_info.get_trigger_type());
  // ^ is xor: one and only one condition should be true.
  OV (trigger_info.has_before_point() ^ trigger_info.has_after_point(),
      OB_INVALID_ARGUMENT, trigger_info.has_before_point(), trigger_info.has_after_point());
  OV (trigger_info.has_stmt_point() ^ trigger_info.has_row_point(),
      OB_INVALID_ARGUMENT, trigger_info.has_stmt_point(), trigger_info.has_row_point());

  ObArenaAllocator alloc;
  sql::ObParser parser(alloc, trigger_info.get_sql_mode());
  ParseResult parse_result;
  ParseNode *stmt_list_node = NULL;
  const ParseNode *trigger_source_node = NULL;
  const ParseNode *trigger_define_node = NULL;
  const ParseNode *trigger_body_node = NULL;
  OZ (parser.parse(trigger_info.get_trigger_body(), parse_result, TRIGGER_MODE,
                  false, false, true),
      trigger_info.get_trigger_body());
  // stmt list node
  OV (OB_NOT_NULL(stmt_list_node = parse_result.result_tree_));
  OV (stmt_list_node->type_ == T_STMT_LIST, OB_ERR_UNEXPECTED, stmt_list_node->type_);
  OV (stmt_list_node->num_child_ == 1, OB_ERR_UNEXPECTED, stmt_list_node->num_child_);
  OV (OB_NOT_NULL(stmt_list_node->children_));
  // trigger source node.
  OV (OB_NOT_NULL(trigger_source_node = stmt_list_node->children_[0]));
  if (OB_SUCC(ret) && T_SP_PRE_STMTS == trigger_source_node->type_) {
    OZ (pl::ObPLResolver::resolve_condition_compile(
        alloc,
        NULL,
        &schema_guard_,
        NULL,
        NULL,
        &(trigger_info.get_package_exec_env()),
        trigger_source_node,
        trigger_source_node,
        true /*inner_parse*/,
        true /*is_trigger*/));
  }
  if (!get_ddl) {
    OZ (BUF_PRINTF(trigger_info.has_before_point() ? "\nBEFORE" : "\nAFTER"));
    if (trigger_info.has_insert_event()) {
      OZ (BUF_PRINTF(" INSERT"));
      has_print_event = true;
    }
    if (trigger_info.has_delete_event()) {
      OZ (BUF_PRINTF(has_print_event ? " OR DELETE" : " DELETE"));
      has_print_event = true;
    }
    if (trigger_info.has_update_event()) {
      if (!trigger_info.get_update_columns().empty()) {

        OZ (BUF_PRINTF(has_print_event ? " OR %.*s" : " %.*s",
                      trigger_info.get_update_columns().length(),
                      trigger_info.get_update_columns().ptr()));
      } else {
        OZ (BUF_PRINTF(has_print_event ? " OR UPDATE": " UPDATE"));
      }
    }
    OZ (print_trigger_base_object(trigger_info, buf, buf_len, pos),
        trigger_info.get_trigger_name());
    if (ObTriggerInfo::TriggerType::TT_SIMPLE_DML == trigger_info.get_trigger_type()
        && trigger_info.has_row_point()) {
      OZ (print_trigger_referencing(trigger_info, buf, buf_len, pos),
          trigger_info.get_trigger_name());
    }
    if (trigger_info.has_row_point()) {
      OZ (BUF_PRINTF("\nFOR EACH ROW"));
    }
    if (!trigger_info.get_when_condition().empty()) {
      OZ (BUF_PRINTF("\nWHEN(%.*s)",
                    trigger_info.get_when_condition().length(),
                    trigger_info.get_when_condition().ptr()));
    }
    if (OB_FAIL(ret)) {
    } else if (trigger_source_node->type_ == T_TG_SOURCE) {
      // trigger define node.
      OV (OB_NOT_NULL(trigger_define_node = trigger_source_node->children_[1]));
      OV (T_TG_SIMPLE_DML == trigger_define_node->type_,
            OB_ERR_UNEXPECTED, trigger_define_node->type_);
      OV (OB_NOT_NULL(trigger_body_node = trigger_define_node->children_[4]));

      OZ (BUF_PRINTF("\n%.*s",
                    (int)(trigger_body_node->str_len_),
                    trigger_body_node->str_value_));
    }
  } else {
    CK (trigger_source_node->type_ == T_TG_SOURCE);
    OZ (BUF_PRINTF(" %.*s",
                   (int)(trigger_source_node->str_len_),
                   trigger_source_node->str_value_));
  }
  if (OB_SUCC(ret) && get_ddl) {
    OZ (print_trigger_status(trigger_info, buf, buf_len, pos));
  }
  return ret;
}

int ObSchemaPrinter::print_compound_instead_trigger_definition(const ObTriggerInfo &trigger_info,
                                                               char *buf, int64_t buf_len, int64_t &pos,
                                                               bool get_ddl) const
{
  int ret = OB_SUCCESS;
  OV (trigger_info.is_compound_dml_type() || trigger_info.is_instead_dml_type(),
      OB_ERR_UNEXPECTED, trigger_info.get_trigger_type());
  ObArenaAllocator alloc;
  sql::ObParser parser(alloc, trigger_info.get_sql_mode());
  ParseResult parse_result;
  OZ (parser.parse(trigger_info.get_trigger_body(), parse_result, TRIGGER_MODE,
                  false, false, true),
      trigger_info.get_trigger_body());
  CK (OB_NOT_NULL(parse_result.result_tree_) && OB_NOT_NULL(parse_result.result_tree_->children_[0]));
  OZ (BUF_PRINTF(" %.*s", (int)(parse_result.result_tree_->children_[0]->str_len_),
                 parse_result.result_tree_->children_[0]->str_value_));
  if (OB_SUCC(ret) && get_ddl) {
    OZ (print_trigger_status(trigger_info, buf, buf_len, pos));
  }
  return ret;
}

int ObSchemaPrinter::print_trigger_status(const ObTriggerInfo &trigger_info,
                                          char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema *database_schema = NULL;
  OZ (schema_guard_.get_database_schema(trigger_info.get_tenant_id(),
                                        trigger_info.get_database_id(),
                                        database_schema));
  CK (OB_NOT_NULL(database_schema));
  OZ (BUF_PRINTF("; \n\nALTER TRIGGER \"%.*s\".\"%.*s\" %s",
                 database_schema->get_database_name_str().length(),
                 database_schema->get_database_name_str().ptr(),
                 trigger_info.get_trigger_name().length(),
                 trigger_info.get_trigger_name().ptr(),
                 trigger_info.is_enable() ? "ENABLE" : "DISABLE"));
  return ret;
}

int ObSchemaPrinter::print_trigger_base_object(const ObTriggerInfo &trigger_info,
                                               char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  const ObDatabaseSchema *database_schema = NULL;
  const uint64_t tenant_id = trigger_info.get_tenant_id();
  OV (!trigger_info.is_system_type(), OB_NOT_SUPPORTED, trigger_info.get_trigger_type());
  OZ (schema_guard_.get_table_schema(tenant_id, trigger_info.get_base_object_id(), table_schema),
      trigger_info.get_base_object_id());
  OV (OB_NOT_NULL(table_schema), OB_ERR_UNEXPECTED, trigger_info.get_base_object_id());
  OZ (schema_guard_.get_database_schema(tenant_id,
      table_schema->get_database_id(), database_schema),
      trigger_info.get_base_object_id());
  OV (OB_NOT_NULL(database_schema), OB_ERR_UNEXPECTED, table_schema->get_database_id());
  OV (!database_schema->get_database_name_str().empty() && !table_schema->get_table_name_str().empty(),
      OB_ERR_UNEXPECTED, trigger_info.get_base_object_id());
  OZ (BUF_PRINTF("\nON %.*s.%.*s",
                 database_schema->get_database_name_str().length(),
                 database_schema->get_database_name_str().ptr(),
                 table_schema->get_table_name_str().length(),
                 table_schema->get_table_name_str().ptr()));
  return ret;
}

int ObSchemaPrinter::print_trigger_referencing(const ObTriggerInfo &trigger_info,
                                               char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OV (!trigger_info.get_ref_old_name().empty() && !trigger_info.get_ref_new_name().empty(),
      OB_ERR_UNEXPECTED, trigger_info.get_ref_old_name(), trigger_info.get_ref_new_name());
  OZ (BUF_PRINTF("\nREFERENCING OLD AS %.*s NEW AS %.*s",
                 trigger_info.get_ref_old_name().length(),
                 trigger_info.get_ref_old_name().ptr(),
                 trigger_info.get_ref_new_name().length(),
                 trigger_info.get_ref_new_name().ptr()));
  return ret;
}

int ObSchemaPrinter::deep_copy_obj(ObIAllocator &allocator, const ObObj &src, ObObj &dest) const
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  int64_t size = src.get_deep_copy_size();

   if (size > 0) {
     if (NULL == (buf = static_cast<char*>(allocator.alloc(size)))) {
       ret = OB_ALLOCATE_MEMORY_FAILED;
       LOG_ERROR("Fail to allocate memory, ", K(size), K(ret));
     } else if (OB_FAIL(dest.deep_copy(src, buf, size, pos))){
       LOG_WARN("Fail to deep copy obj, ", K(ret));
     }
   } else {
     dest = src;
   }

  return ret;
}

int ObSchemaPrinter::print_sequence_definition(const ObSequenceSchema &sequence_schema,
                                               char *buf,
                                               const int64_t &buf_len,
                                               int64_t &pos,
                                               bool is_from_create_sequence) const
{
  int ret = OB_SUCCESS;
  const ObString &seq_name = sequence_schema.get_sequence_name();
  const number::ObNumber &min_value = sequence_schema.get_min_value();
  const number::ObNumber &max_value = sequence_schema.get_max_value();
  const number::ObNumber &start_with = sequence_schema.get_start_with();
  const number::ObNumber &inc = sequence_schema.get_increment_by();
  const number::ObNumber &cache = sequence_schema.get_cache_size();
  bool cycle = sequence_schema.get_cycle_flag();
  bool order = sequence_schema.get_order_flag();

  if (is_from_create_sequence) {
    OX(BUF_PRINTF("CREATE SEQUENCE \"%.*s\"", seq_name.length(), seq_name.ptr()));
  }
  OX(BUF_PRINTF(" minvalue "));
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

// print unique constraint definition for dbms_metadata.get_ddl in oracle mode
int ObSchemaPrinter::print_unique_cst_definition(
    const ObDatabaseSchema &db_schema,
    const ObTableSchema &table_schema, // data_table
    const ObTableSchema &unique_index_schema,
    char *buf, const int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("PrintUkCstDef");
  const ObString &db_name = db_schema.get_database_name_str();
  const ObString &tb_name = table_schema.get_table_name_str();
  ObString new_db_name;
  ObString new_data_table_name;
  ObSQLMode sql_mode = SMO_ORACLE;
  const bool is_oracle_mode = true;

  if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(allocator,
              db_schema.get_database_name_str(), new_db_name, is_oracle_mode))) {
    SHARE_SCHEMA_LOG(WARN, "fail to generate new db_name with escape character",
                     K(ret), K(db_schema.get_database_name_str()));
  } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(allocator,
             table_schema.get_table_name_str(), new_data_table_name, is_oracle_mode))) {
    SHARE_SCHEMA_LOG(WARN, "fail to generate new data_table_name with escape character",
                     K(ret), K(table_schema.get_table_name_str()));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
             "ALTER TABLE \"%.*s\".\"%.*s\" ADD",
             new_db_name.length(), new_db_name.ptr(),
             new_data_table_name.length(), new_data_table_name.ptr()))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print alter table add constraint prefix", K(ret), K(new_db_name), K(new_data_table_name));
  } else if (OB_FAIL(print_single_index_definition(&unique_index_schema, table_schema, allocator,
                     buf, buf_len, pos, true, lib::is_oracle_mode(), true, sql_mode, NULL))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print unique constraint columns", K(ret));
  }

  return ret;

}

// print pk/check/not null constraint definition
int ObSchemaPrinter::print_constraint_definition(const ObDatabaseSchema &db_schema,
                                                const ObTableSchema &table_schema,
                                                uint64_t constraint_id,
                                                char *buf,
                                                const int64_t &buf_len,
                                                int64_t &pos)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  const ObConstraint *cst = NULL;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  ObArenaAllocator allocator("PrintConsDef");
  ObTableSchema::const_constraint_iterator it_begin = table_schema.constraint_begin();
  ObTableSchema::const_constraint_iterator it_end = table_schema.constraint_end();
  const ObString &db_name = db_schema.get_database_name_str();
  const ObString &tb_name = table_schema.get_table_name_str();
  for (ObTableSchema::const_constraint_iterator it = it_begin;
       OB_SUCC(ret) && it != it_end; it++) {
    const ObConstraint *tmp_cst = *it;
    if (constraint_id == tmp_cst->get_constraint_id()) {
      cst = tmp_cst;
      break;
    }
  }
  ObString cst_name;
  if (OB_ISNULL(cst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("constraint not found in table schema", K(ret), K(constraint_id));
  } else if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(table_schema));
  } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
                     allocator,
                     cst->get_constraint_name_str(),
                     cst_name,
                     is_oracle_mode))) {
    LOG_WARN("generate new name with escape name str failed", K(ret));
  } else if (CONSTRAINT_TYPE_NOT_NULL == cst->get_constraint_type()) {
    // can't add not null constraint with "alter table add constraint"
    // add not null constraint with "alter table modify c1", it's compatible with Oracle.
    if (! is_oracle_mode) {
      // definition of constraint if only printed in dbms_metadata.get_ddl, so not suppoted in mysql mode.
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("print not null constraint in mysql mode not supported", K(ret));
    } else {
      int64_t column_id = OB_INVALID_ID;
      ObString column_name;
      const ObColumnSchemaV2 *column_schema = NULL;
      ObConstraint::const_cst_col_iterator column_begin = cst->cst_col_begin();
      if (1 != cst->get_column_cnt() || OB_ISNULL(column_begin)
          || OB_UNLIKELY(column_begin + 1 != cst->cst_col_end())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("count of columns in not null constraint should be one.", K(ret), K(*cst));
      } else if (OB_FAIL(schema_guard_.get_column_schema(tenant_id,
                                                         cst->get_table_id(),
                                                         *column_begin, column_schema))) {
        LOG_WARN("get column schema failed", K(ret), K(tenant_id), K(*cst));
      } else if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", K(ret));
      } else if (FALSE_IT(column_name = column_schema->get_column_name_str())) {
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                      "ALTER TABLE \"%.*s\".\"%.*s\" MODIFY (\"%.*s\" CONSTRAINT \"%.*s\" NOT NULL",
                      db_name.length(), db_name.ptr(),
                      tb_name.length(), tb_name.ptr(),
                      column_name.length(), column_name.ptr(),
                      cst_name.length(), cst_name.ptr()))) {
        LOG_WARN("failed to print not null constraint", K(ret));
      } else if (OB_FAIL(print_constraint_stat(cst->get_rely_flag(), cst->get_enable_flag(),
                                               cst->is_validated(), buf, buf_len, pos))) {
        LOG_WARN("failed to print constraint stat", K(ret), K(*cst));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
        LOG_WARN("failed to print", K(ret));
      }
    }
  } else {
    const ObString &cst_expr = cst->get_check_expr_str();
    OX (BUF_PRINTF("ALTER TABLE "));
    OZ (print_identifier(buf, buf_len, pos, db_name, is_oracle_mode));
    OX (BUF_PRINTF("."));
    OZ (print_identifier(buf, buf_len, pos, tb_name, is_oracle_mode));
    OX (BUF_PRINTF(" ADD CONSTRAINT "));
    OZ (print_identifier(buf, buf_len, pos, cst_name, is_oracle_mode));
    OX (BUF_PRINTF(" "));
    switch (cst->get_constraint_type()) {
      case CONSTRAINT_TYPE_PRIMARY_KEY:
        OX (BUF_PRINTF("PRIMARY KEY ("));
        OZ (print_rowkey_info(table_schema.get_rowkey_info(),
                              table_schema.get_tenant_id(),
                              table_schema.get_table_id(),
                              buf, buf_len, pos));
        OX (BUF_PRINTF(")"));
        break;
      case CONSTRAINT_TYPE_CHECK:
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "CHECK %.*s", cst_expr.length(), cst_expr.ptr()))) {
          LOG_WARN("print check constraint failed", K(ret));
        } else if (OB_FAIL(print_constraint_stat(cst->get_rely_flag(), cst->get_enable_flag(),
                                                 cst->is_validated(), buf, buf_len, pos))) {
          LOG_WARN("failed to print constraint stat", K(ret), K(*cst));
        }
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "Not supported print constraint type", K(ret),
                      K(cst->get_constraint_type()));
    }
  }
  return ret;
}

int ObSchemaPrinter::print_user_definition(uint64_t tenant_id,
                                          const ObUserInfo &user_info,
                                          char *buf,
                                          const int64_t &buf_len,
                                          int64_t &pos,
                                          bool is_role)
{
  int ret = OB_SUCCESS;
  common::ObArray<const ObUserInfo *> user_schemas;

  const ObString &user_name = user_info.get_user_name_str();
  const ObString &host_name = user_info.get_host_name_str();
  const ObString &user_passwd = user_info.get_passwd_str();
  bool is_oracle_mode = lib::is_oracle_mode();
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                              is_oracle_mode ? "create %s "
                                             : "create %s if not exists ",
                              is_role ? "role" : "user"))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print user name", K(user_name), K(ret));
  } else if (OB_FAIL(print_identifier(buf, buf_len, pos, user_name, is_oracle_mode))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print user name", K(user_name), K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " "))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print user name", K(user_name), K(ret));
  } else if (host_name.compare(OB_DEFAULT_HOST_NAME) != 0) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "@"))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print host name", K(host_name), K(ret));
    } else if (OB_FAIL(print_identifier(buf, buf_len, pos, host_name, is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print host name", K(host_name), K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print host name", K(host_name), K(ret));
    }
  }

  if (OB_SUCC(ret) && user_passwd.length() > 0) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos,
        is_oracle_mode ? "IDENTIFIED BY VALUES \"%.*s\" " : "IDENTIFIED BY PASSWORD '%.*s' ",
        user_passwd.length(), user_passwd.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print user passwd", K(user_name), K(user_passwd), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const ObProfileSchema *profile_schema = NULL;
    if (OB_INVALID_ID == user_info.get_profile_id()) {
    } else if (OB_FAIL(schema_guard_.get_profile_schema_by_id(tenant_id, user_info.get_profile_id(),
                                                      profile_schema))) {
      if (OB_OBJECT_NAME_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        SHARE_SCHEMA_LOG(WARN, "get profile schena failed", K(ret));
      }
    } else {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "PROFILE "))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print profile", K(ret));
      } else if (OB_FAIL(print_identifier(buf, buf_len, pos,
                                          profile_schema->get_profile_name_str(),
                                          is_oracle_mode))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print profile", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " "))) {
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
        } else if (!user_info.get_ssl_cipher_str().empty()
                  && OB_FAIL(databuff_printf(buf, buf_len, pos, "CIPHER '%s' ",
                                            user_info.get_ssl_cipher()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print ssl info", K(user_name), K(ret));
        } else if (!user_info.get_x509_issuer_str().empty()
                  && OB_FAIL(databuff_printf(buf, buf_len, pos, "ISSUER '%s' ",
                                            user_info.get_x509_issuer()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print ssl info", K(user_name), K(ret));
        } else if (!user_info.get_x509_subject_str().empty()
                  && OB_FAIL(databuff_printf(buf, buf_len, pos, "SUBJECT '%s' ",
                                            user_info.get_x509_subject()))) {
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

int ObSchemaPrinter::print_hash_partition_elements(const ObPartitionSchema *&schema,
                                                   char* buf,
                                                   const int64_t& buf_len,
                                                   int64_t& pos,
                                                   bool print_sub_part_element,
                                                   bool agent_mode,
                                                   const common::ObTimeZoneInfo *tz_info) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "schema is null", K(ret));
  } else {
    ObPartition **part_array = schema->get_part_array();
    if (OB_ISNULL(part_array)) {
      if (is_virtual_table(schema->get_table_id())) {
        // 虚拟表
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, " partitions %ld\n",
                    schema->get_first_part_num()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to printf partition number",
                      K(ret), K(schema->get_first_part_num()));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "partition_array is NULL", K(ret));
      }
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n("))) {
      SHARE_SCHEMA_LOG(WARN, "print enter failed", K(ret));
    } else {
      int64_t part_num = schema->get_first_part_num();
      for (int64_t i = 0 ; OB_SUCC(ret) && i < part_num; ++i) {
        const ObPartition *partition = part_array[i];
        if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "partition is NULL", K(ret), K(part_num));
        } else {
          const ObString &part_name = partition->get_part_name();
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, "partition "))) {
            SHARE_SCHEMA_LOG(WARN, "print partition failed", K(ret));
          } else if (OB_FAIL(print_identifier(buf, buf_len, pos, part_name, lib::is_oracle_mode()))) {
            SHARE_SCHEMA_LOG(WARN, "print partition name failed", K(ret), K(part_name));
          } else if (agent_mode &&
                     OB_FAIL(databuff_printf(buf, buf_len, pos, " id %ld", partition->get_part_id()))) { // print id
            SHARE_SCHEMA_LOG(WARN, "print part_id failed", K(ret));
          } else if (OB_FAIL(print_tablespace_definition_for_table(
                     partition->get_tenant_id(), partition->get_tablespace_id(), buf, buf_len, pos))) {
            SHARE_SCHEMA_LOG(WARN, "print tablespace id failed", K(ret), K(part_name));
          } else if (print_sub_part_element
                     && OB_NOT_NULL(partition->get_subpart_array())
                     && OB_FAIL(print_individual_sub_partition_elements(schema, partition, buf,
                                                                        buf_len, pos, tz_info))) {
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

int ObSchemaPrinter::print_synonym_definition(const ObSynonymInfo &synonym_info,
                                              char *buf,
                                              const int64_t &buf_len,
                                              int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || 0 >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "arguemnt is invalid", K(ret), K(buf_len), KP(buf));
  } else {
    const ObDatabaseSchema * synonym_ds_schema = NULL;
    uint64_t synonym_db_id = synonym_info.get_database_id();
    ObString synonym_db_name;
    const ObString &synonym_name = synonym_info.get_synonym_name_str();
    const ObDatabaseSchema * object_ds_schema = NULL;
    const uint64_t tenant_id = synonym_info.get_tenant_id();
    uint64_t object_db_id = synonym_info.get_object_database_id();
    ObString object_db_name;
    const ObString &object_name = synonym_info.get_object_name_str();
    if (OB_FAIL(schema_guard_.get_database_schema(tenant_id, synonym_db_id, synonym_ds_schema))) {
      OB_LOG(WARN, "fail to get database schema", K(ret), K(tenant_id), K(synonym_db_id));
    } else if (OB_ISNULL(synonym_ds_schema)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "synonym database schema is NULL", K(ret));
    } else if (OB_FAIL(schema_guard_.get_database_schema(tenant_id, object_db_id, object_ds_schema))) {
      OB_LOG(WARN, "fail to get database schema", K(ret), K(tenant_id), K(object_db_id));
    } else if (OB_ISNULL(object_ds_schema)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "object database schema is NULL", K(ret));
    } else {
      synonym_db_name = synonym_ds_schema->get_database_name();
      object_db_name = object_ds_schema->get_database_name();
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(synonym_db_name.ptr()) || OB_ISNULL(synonym_name.ptr()) ||
          OB_ISNULL(object_db_name.ptr()) || OB_ISNULL(object_name.ptr())) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "pointer is invalid", KP(synonym_db_name.ptr()), KP(synonym_name.ptr()),
          KP(object_db_name.ptr()), KP(object_name.ptr()));
      } else {
        uint64_t tenant_id = synonym_ds_schema->get_tenant_id();
        bool is_public = is_public_database_id(synonym_db_id);
        const char *public_flag = is_public ? " PUBLIC" : "";
        const char *synonym_db_name_flag = is_public ? "" : synonym_db_name.ptr();
        const char *synonym_db_name_fill = is_public ? "" : "\".\"";
        if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                "CREATE OR REPLACE%s SYNONYM \"%s%s%s\" FOR \"%s\".\"%s\";",
                public_flag,
                synonym_db_name_flag,
                synonym_db_name_fill,
                synonym_name.ptr(),
                object_db_name.ptr(),
                object_name.ptr()))) {
          OB_LOG(WARN, "fail to print create synonym", K(ret), K(synonym_name));
        }
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_external_table_file_info(const ObTableSchema &table_schema,
                                                    ObIAllocator& allocator,
                                                    char* buf,
                                                    const int64_t& buf_len,
                                                    int64_t& pos) const
{
  int ret = OB_SUCCESS;
  // 1. print file location, pattern
  const ObString &location = table_schema.get_external_file_location();
  const ObString &pattern = table_schema.get_external_file_pattern();
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\nLOCATION='%.*s'", location.length(), location.ptr()))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print LOCATION", K(ret));
  } else if (!pattern.empty() && OB_FAIL(databuff_printf(buf, buf_len, pos, "\nPATTERN='%.*s'", pattern.length(), pattern.ptr()))) {
    SHARE_SCHEMA_LOG(WARN, "fail to print PATTERN", K(ret));
  }

  // 2. print file format
  if (OB_SUCC(ret)) {
    ObExternalFileFormat format;
    if (OB_FAIL(format.load_from_string(table_schema.get_external_file_format(), allocator))) {
      SHARE_SCHEMA_LOG(WARN, "fail to load from json string", K(ret));
    } else if (format.format_type_ != ObExternalFileFormat::CSV_FORMAT) {
      SHARE_SCHEMA_LOG(WARN, "unsupported to print file format", K(ret), K(format.format_type_));
    } else {
      const ObCSVGeneralFormat &csv = format.csv_format_;
      const ObOriginFileFormat &origin_format = format.origin_file_format_str_;
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\nFORMAT (\n"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print FORMAT (", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "  TYPE = 'CSV',"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print TYPE", K(ret));
      } else if (OB_FAIL(0 != csv.line_term_str_.case_compare(ObDataInFileStruct::DEFAULT_LINE_TERM_STR) &&
                        databuff_printf(buf, buf_len, pos, "\n  LINE_DELIMITER = %.*s,", origin_format.origin_line_term_str_.length(), origin_format.origin_line_term_str_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print LINE_DELIMITER", K(ret));
      } else if (OB_FAIL(0 != csv.field_term_str_.case_compare(ObDataInFileStruct::DEFAULT_FIELD_TERM_STR) &&
                        databuff_printf(buf, buf_len, pos, "\n  FIELD_DELIMITER = %.*s,", origin_format.origin_field_term_str_.length(), origin_format.origin_field_term_str_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print FIELD_DELIMITER", K(ret));
      } else if (OB_FAIL(ObDataInFileStruct::DEFAULT_FIELD_ESCAPED_CHAR != csv.field_escaped_char_ &&
                        databuff_printf(buf, buf_len, pos, "\n  ESCAPE = %.*s,", origin_format.origin_field_escaped_str_.length(), origin_format.origin_field_escaped_str_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ESCAPE", K(ret));
      } else if (OB_FAIL(ObDataInFileStruct::DEFAULT_FIELD_ENCLOSED_CHAR != csv.field_enclosed_char_ &&
                        databuff_printf(buf, buf_len, pos, "\n  FIELD_OPTIONALLY_ENCLOSED_BY = %.*s,", origin_format.origin_field_enclosed_str_.length(), origin_format.origin_field_enclosed_str_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print FIELD_OPTIONALLY_ENCLOSED_BY", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  ENCODING = '%s',", ObCharset::charset_name(csv.cs_type_)))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print ENCODING", K(ret));
      } else if (OB_FAIL(0 != csv.skip_header_lines_ &&
                        databuff_printf(buf, buf_len, pos, "\n  SKIP_HEADER = %ld,", csv.skip_header_lines_))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print SKIP_HEADER", K(ret));
      } else if (OB_FAIL(csv.skip_blank_lines_ &&
                        databuff_printf(buf, buf_len, pos, "\n  SKIP_BLANK_LINES = TRUE,"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print SKIP_BLANK_LINES", K(ret));
      } else if (OB_FAIL(csv.trim_space_ &&
                        databuff_printf(buf, buf_len, pos, "\n  TRIM_SPACE = TRUE,"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print TRIM_SPACE", K(ret));
      } else if (OB_FAIL(csv.empty_field_as_null_ &&
                        databuff_printf(buf, buf_len, pos, "\n  EMPTY_FIELD_AS_NULL = TRUE,"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print EMPTY_FIELD_AS_NULL", K(ret));
      } else if (OB_FAIL(0 != csv.null_if_.count() &&
                        databuff_printf(buf, buf_len, pos, "\n  NULL_IF = (%.*s),", origin_format.origin_null_if_str_.length(), origin_format.origin_null_if_str_.ptr()))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print NULL_IF", K(ret));
      } else {
        --pos;
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n) "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print )", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaPrinter::print_identifier(char* buf,
                                      const int64_t& buf_len,
                                      int64_t& pos,
                                      const ObString &ident,
                                      bool is_oracle_mode) const
{
  int ret = OB_SUCCESS;
  bool require_quotes = false;
  const char* format_str = "%.*s";
  if (is_oracle_mode) {
    format_str = "\"%.*s\"";
    require_quotes = true;
  } else if (sql_quote_show_create_) {
    format_str = "`%.*s`";
    require_quotes = true;
  } else if (OB_FAIL(ObSQLUtils::print_identifier_require_quotes(CS_TYPE_UTF8MB4_GENERAL_CI,
                                                                 ident,
                                                                 require_quotes))) {
    LOG_WARN("failed to check identifier require quotes", K(ret));
  } else if (!require_quotes) {
    format_str = "%.*s";
  } else {
    format_str = "`%.*s`";
  }

  if (OB_FAIL(ret)) {
  } else if (!require_quotes || (require_quotes && !ansi_quotes_)) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, format_str, ident.length(),
                                ident.empty() ? "" : ident.ptr()))){
      SHARE_SCHEMA_LOG(WARN, "fail to print indentifer", K(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\""))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print indentifer", K(ret));
    }
    for (int64_t index = 0; OB_SUCC(ret) && !ident.empty() && index < ident.length(); ++index) {
      if (ident[index] == '"') {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\"\""))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print indentifer", K(ret));
        }
      } else if (ident[index] == '`' &&
                 index + 1 < ident.length() &&
                 ident[index + 1] == '`') {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "`"))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print indentifer", K(ret));
        } else {
          ++index;
        }
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%c", ident[index]))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print indentifer", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(databuff_printf(buf, buf_len, pos, "\""))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print indentifer", K(ret));
    }
  }
  return ret;
}

int ObSchemaPrinter::print_view_define_str(char* buf,
                                           const int64_t &buf_len,
                                           int64_t& pos,
                                           bool is_oracle_mode,
                                           const ObString &sql) const
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode ||
      (sql_quote_show_create_ && !ansi_quotes_)) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%.*s", sql.length(), sql.ptr()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print view define str", K(ret));
    }
  } else {
    const char *begin = sql.ptr();
    const char *cursor = begin;
    const char *end = begin + sql.length();
    int state = 0;
    while (OB_SUCC(ret) && cursor < end) {
      if (0 == state) {
        // init state, find `, ', " ,/* ,--
        while (cursor < end) {
          if (*cursor == '`' || *cursor == '\'' || *cursor == '"') {
            break;
          } else if (*cursor == '/' && cursor + 1 < end && *(cursor + 1) == '*') {
            break;
          } else if (*cursor == '-' && cursor + 1 < end && *(cursor + 1) == '-') {
            break;
          } else {
            ++cursor;
          }
        }
        if (cursor - begin > 0 &&
            OB_FAIL(databuff_printf(buf, buf_len, pos,
                                    "%.*s",
                                    static_cast<ObString::obstr_size_t>(cursor - begin),
                                    begin))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print view define str", K(ret));
        } else if (OB_FALSE_IT(begin = cursor)) {
        } else if (cursor >= end) {
          // do nothing
        } else if (*cursor == '`') {
          state = 1;
        } else if (*cursor == '\'') {
          state = 2;
        } else if (*cursor == '"') {
          state = 3;
        } else if (*cursor == '/')  {
          state = 4;
        } else if (*cursor == '-') {
          state = 5;
        } else {
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "fail to print view define str, get unexpected state", K(ret), K(cursor));
        }
      } else if (1 == state) {
        // process `
        ++cursor;
        while (cursor < end) {
          if (*cursor == '`' && cursor + 1 < end && *(cursor + 1) == '`') {
            cursor += 2;
          } else if (*cursor == '`') {
            break;
          } else {
            ++cursor;
          }
        }
        if (OB_UNLIKELY(cursor >= end) ||
            OB_UNLIKELY(*cursor != '`') ||
            OB_UNLIKELY(cursor - begin < 1)) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "fail to print view define str", K(ret), K(sql), K(begin), K(cursor));
        } else if (OB_FAIL(print_identifier(buf, buf_len, pos,
                                            ObString(cursor - begin - 1, begin + 1),
                                            is_oracle_mode))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print view define str", K(ret), K(sql), K(begin));
        } else {
          ++cursor;
          begin = cursor;
          state = 0;
        }
      } else if (2 == state || 3 == state) {
        // process ' "
        ++cursor;
        const char c = (2 == state) ? '\'' : '"';
        while (cursor < end) {
          if (*cursor == '\\' && cursor + 1 < end && *(cursor + 1) == c) {
            cursor += 2;
          } else if (*cursor == '\\' && cursor + 1 < end &&
                     *(cursor + 1) == '\\') {
            cursor += 2;
          } else if (*cursor == c) {
            break;
          } else {
            ++cursor;
          }
        }
        if (OB_UNLIKELY(cursor >= end) ||
            OB_UNLIKELY(*cursor != c) ||
            OB_UNLIKELY(cursor - begin < 1)) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "fail to print view define str", K(ret), K(sql), K(begin), K(cursor));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                           "%.*s", static_cast<ObString::obstr_size_t>(cursor - begin + 1),
                                           begin))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print view define str", K(ret), K(sql), K(begin), K(cursor));
        } else {
          ++cursor;
          begin = cursor;
          state = 0;
        }
      } else if (4 == state) {
        // process /*
        cursor += 2;
        while (cursor < end) {
          if (*cursor == '*' && cursor + 1 < end && *(cursor + 1) == '/') {
            break;
          } else {
            ++cursor;
          }
        }
        if (OB_UNLIKELY(cursor + 1 >= end) ||
            OB_UNLIKELY(*cursor != '*' || *(cursor + 1) != '/')) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "fail to print view define str", K(ret), K(sql), K(begin), K(cursor));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                           "%.*s", static_cast<ObString::obstr_size_t>(cursor - begin + 2),
                                           begin))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print view define str", K(ret), K(sql), K(begin), K(cursor));
        } else {
          cursor += 2;
          begin = cursor;
          state = 0;
        }
      } else if (5 == state) {
        // process --
        cursor += 2;
        while (cursor < end) {
          if (*cursor == '\n') {
            ++cursor;
            break;
          } else {
            ++cursor;
          }
        }
        if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                    "%.*s", static_cast<ObString::obstr_size_t>(cursor - begin),
                                    begin))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print view define str", K(ret), K(sql), K(begin), K(cursor));
        } else {
          begin = cursor;
          state = 0;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "fail to print view define str, get unexpected state", K(ret), K(cursor), K(state));
      }
    }
  }
  return ret;
}

} // end namespace schema
} //end of namespace share
}   // end namespace oceanbase
