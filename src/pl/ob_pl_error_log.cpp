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

#define USING_LOG_PREFIX PL

#include "lib/string/ob_string.h"
#include "pl/ob_pl_error_log.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "lib/mysqlclient/ob_isql_connection.h"

namespace oceanbase
{
namespace pl
{
  int PlPackageErrorLog::create_error_log(sql::ObExecContext &ctx,
            sql::ParamStore &params,
            common::ObObj &result)
  {
    UNUSED(result);
    int ret = OB_SUCCESS;
    bool is_skip_column = false;
    uint64_t database_id = OB_INVALID_ID;
    ObSchemaGetterGuard schema_guard;
    ObString dml_table_name;
    ObString column_type_define; // 列定义的type
    char *buf = NULL;
    ObString create_table_sql;
    const ObTableSchema *table_schema = NULL;
    common::ObMySQLProxy *sql_proxy = NULL;
    observer::ObInnerSQLConnectionPool *pool = NULL;
    sqlclient::ObISQLConnection *conn = NULL;
    ObSQLSessionInfo *session;
    uint64_t tenant_id = OB_INVALID_ID;
    ObArenaAllocator allocator(ObModIds::OB_SQL_STRING);
    const char *ora_err_number = "ORA_ERR_NUMBER$ NUMBER";
    const char *ora_err_mesg = "ORA_ERR_MESG$ VARCHAR2(2000)";
    const char *ora_err_row_id = "ORA_ERR_ROWID$ UROWID(4000)";
    const char *ora_err_optyp = "ORA_ERR_OPTYP$ VARCHAR2(2)";
    const char *ora_err_tag = "ORA_ERR_TAG$ VARCHAR2(2000)";
    const char *prefix_err_table = "ERR$_";
    const ObString create_sql_prefix = "create table";
    const ObString table_space_str = "tablespace";

    if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(ctx.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.schema_service_ is NULL or sessionInfo is null");
    } else {
      session = ctx.get_my_session();
    }

    // param count == 5, params.at(0) is not null and params.at(4) is boolean type,
    CK (5 == params.count());
    CK (!params.at(0).is_null())
    CK (params.at(4).is_tinyint());
    for (int i = 0; i < params.count() - 1; i++) {
      if (!params.at(i).is_null()) {
        CK (params.at(i).is_string_type());
      }
    }

    // params(0) is base dml table name, last one must be bool type
    dml_table_name = params.at(0).get_string();
    params.at(4).get_bool(is_skip_column);
    ObString database_name = session->get_database_name();
    tenant_id = session->get_effective_tenant_id();

    // alloc buff for ObString
    if (OB_FAIL(ret)) {

    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(OB_MAX_VARCHAR_LENGTH)))) {
      // alloc buff, which size = 1024 * 1024
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(OB_MAX_VARCHAR_LENGTH), K(ret));
    }
    create_table_sql.assign_buffer(buf, static_cast<ObString::obstr_size_t>(OB_MAX_VARCHAR_LENGTH));

    // create_table_sql = "create table "
    create_table_sql.write(create_sql_prefix.ptr(), create_sql_prefix.length());
    create_table_sql.write(" ", 1);
    // table_name 转换规则： aaa -> AAA, "aaa" -> aaa
    // create_table_sql = "create table xxxx("
    bool small_letter = true;
    resolve_table_name(dml_table_name, allocator, small_letter);
    if (!params.at(1).is_null()) {
      // 用户定义了err_log_table_name
      if (!params.at(2).is_null()) {
        // 用户指定了 database(USER)
        ObString table_ownner = params.at(2).get_string();
        create_table_sql.write(table_ownner.ptr(), table_ownner.length());
        create_table_sql.write(".", 1);
      }
      // 用户定义了 ERR_LOG_TABLE_NAME
      ObString err_log_table_name = params.at(1).get_string();
      create_table_sql.write(err_log_table_name.ptr(), err_log_table_name.length());
    } else {
      // 用户没定义 ERR_LOG_TABLE_NAME
      if (small_letter) {
        create_table_sql.write("\"", 1);
      }
      create_table_sql.write(prefix_err_table, static_cast<ObString::obstr_size_t>(strlen(prefix_err_table)));
      create_table_sql.write(dml_table_name.ptr(), dml_table_name.length());
      if (small_letter) {
        create_table_sql.write("\"", 1);
      }
    }
    create_table_sql.write("(", 1);

    // create_table_sql = "create table xxxx(ORA_ERR_NUMBER$ NUMBER......"
    create_table_sql.write(ora_err_number, static_cast<ObString::obstr_size_t>(strlen(ora_err_number)));
    create_table_sql.write(",", 1);
    create_table_sql.write(ora_err_mesg, static_cast<ObString::obstr_size_t>(strlen(ora_err_mesg)));
    create_table_sql.write(",", 1);
    create_table_sql.write(ora_err_row_id, static_cast<ObString::obstr_size_t>(strlen(ora_err_row_id)));
    create_table_sql.write(",", 1);
    create_table_sql.write(ora_err_optyp, static_cast<ObString::obstr_size_t>(strlen(ora_err_optyp)));
    create_table_sql.write(",", 1);
    create_table_sql.write(ora_err_tag, static_cast<ObString::obstr_size_t>(strlen(ora_err_tag)));

    // get table_schema by table_name
    if (OB_FAIL(ret)) {
      LOG_WARN("ret is not ob_success", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get tenant schema guard fail", K(ret));
    } else if (OB_FAIL(schema_guard.get_database_id(tenant_id, database_name, database_id))) {
      LOG_WARN("get database is fail", K(ret), K(database_name));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                     database_id,
                                                     dml_table_name,
                                                     false,
                                                     table_schema))) {
      LOG_WARN("get tenant table_schema fail", K(ret), K(dml_table_name));
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("ret is fail", K(ret), K(table_schema));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is null", K(table_schema));
    } else {
      ObColumnIterByPrevNextID iter(*table_schema);
      const ObColumnSchemaV2 *column_schema = NULL;
      ObString new_col_name;
      while (OB_SUCC(ret) && OB_SUCC(iter.next(column_schema))) {
        column_type_define.reset();
        new_col_name.reset();
        if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The column schema is null", K(ret));
        } else if (column_schema->is_hidden()) {
          // jump hidden column, but if it is sync ddl user,  not jump __pk_increment
          continue;
        } else if (OB_FAIL(resolve_column_info(column_type_define, column_schema))) {
          if (ret == OB_NOT_SUPPORTED && is_skip_column) {
            ret = OB_SUCCESS;
            continue;
          }
        } else if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
            allocator,
            column_schema->get_column_name_str(),
            new_col_name,
            true))) {
          LOG_WARN("fail to generate escape character", K(ret));
        } else {
          create_table_sql.write(",\"", 2);
          create_table_sql.write(new_col_name.ptr(), new_col_name.length());
          create_table_sql.write("\" ", 2);
          create_table_sql.write(column_type_define.ptr(), column_type_define.length());
        }
      }
      create_table_sql.write(")", 1);
      if (!params.at(3).is_null()) {
        ObString table_space = params.at(3).get_string();
        create_table_sql.write(" ", 1);
        create_table_sql.write(table_space_str.ptr(), table_space_str.length());
        create_table_sql.write(" ", 1);
        create_table_sql.write(table_space.ptr(), table_space.length());
      }

      if (ret != OB_ITER_END) {
        LOG_WARN("Failed to iterate all table columns. iter quit. ", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
    int64_t affected_rows = 0;
    if (OB_FAIL(ret)) {
      LOG_WARN("ret is not OB_SUCCESS", K(ret));
    } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_) || OB_ISNULL(sql_proxy->get_pool())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy must not null", K(ret), KP(GCTX.sql_proxy_));
    } else if (OB_ISNULL(pool = static_cast<observer::ObInnerSQLConnectionPool*>(sql_proxy->get_pool()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool must not null", K(ret));
    } else if (OB_FAIL(pool->acquire(session, conn))) {
      LOG_WARN("failed to get conn", K(ret));
    } else if (OB_FAIL(conn->execute_write(tenant_id, to_cstring(create_table_sql),
        affected_rows))) {
      LOG_WARN("failed to excute create table sql", K(ret), K(create_table_sql));
    }
    return ret;
  }

  int PlPackageErrorLog::resolve_table_name(ObString &table_name, ObIAllocator &allocator, bool &small_letter) {
    int ret = OB_SUCCESS;
    int64_t str_len = table_name.length() - 2;
    if (str_len <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("base_table_name is null");
    }
    if ('\"' == table_name[0] && '\"' == table_name[table_name.length() - 1]) {
      LOG_WARN("before table_name is", K(table_name), K(table_name.length()));
      small_letter = true;
      char *buff = static_cast<char *>(allocator.alloc(str_len));
      if (OB_ISNULL(buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory failed.", K(buff), K(ret));
      } else {
        int32_t len_str = 0;
        for (int32_t i = 1; len_str < str_len && i < table_name.length() - 1; ++i) {
          buff[len_str++] = table_name[i];
        }
        table_name.assign_ptr(buff, static_cast<int64_t>(len_str));
      }
    } else {
      small_letter = false;
      // 转大写
      ObCharset::caseup(CS_TYPE_UTF8MB4_BIN, table_name);
    }
    return ret;
  }

  int PlPackageErrorLog::resolve_column_info(ObString &column_define, const ObColumnSchemaV2 *column_schema) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_schema is null");
    }
    ObObjOType column_o_type = ob_obj_type_to_oracle_type(column_schema->get_data_type());
    switch (column_o_type) {
      case ObONotSupport:
      case ObONullType:
        // 不支持
        ret = OB_NOT_SUPPORTED;
        break;
      case ObOSmallIntType:
      case ObOIntType:
      case ObONumberFloatType:
      case ObOBinFloatType:
      case ObOBinDoubleType:
      case ObONumberType:
        // varchar2(4000)
        column_define = "VARCHAR2(4000)";
        break;
      case ObOCharType:
      case ObOVarcharType:
      case ObONVarchar2Type:
      case ObONCharType:
        // varchar2(32767)
        column_define = "VARCHAR2(4000)";
        break;
      case ObODateType:
      case ObOTimestampTZType:
      case ObOTimestampLTZType:
      case ObOTimestampType:
      case ObOIntervalYMType:
      case ObOIntervalDSType:
        // varchar2(4000)
        column_define = "VARCHAR2(4000)";
        break;
      case ObOLobType:
      case ObOExtendType:
      case ObOUnknownType:
        // 不支持
        ret = OB_NOT_SUPPORTED;
        break;
      case ObORawType:
        // Raw(2000)
        column_define = "RAW(2000)";
        break;
      case ObOURowIDType:
      case ObOLobLocatorType:
      case ObOMaxType:
        // 不支持
        ret = OB_NOT_SUPPORTED;
        break;
    }
    return ret;
  }

} // namespace pl
} // oceanbase
