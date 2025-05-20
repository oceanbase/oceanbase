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

#define USING_LOG_PREFIX RS


#include "ob_ddl_service.h"

namespace oceanbase
{
namespace rootserver
{

// Notice: this function is only used for dropping lob aux table that's main table has been dropped casued by some bugs.
int ObDDLService::force_drop_lonely_lob_aux_table(const ObForceDropLonelyLobAuxTableArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid drop lob arg", KR(ret), K(arg));
  } else if (OB_ISNULL(schema_service_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service_ or sql_proxy_ is null", KR(ret), KP(schema_service_), KP(sql_proxy_));
  } else {
    ObSchemaGetterGuard schema_guard;
    ObDDLSQLTransaction trans(schema_service_);
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    int64_t refreshed_schema_version = 0;
    const ObTableSchema *lob_meta_table_schema_ptr = nullptr;
    const ObTableSchema *lob_piece_table_schema_ptr = nullptr;
    const uint64_t tenant_id = arg.get_tenant_id();
    uint64_t data_table_id = arg.get_data_table_id();
    bool exist = false;

    HEAP_VAR(ObTableSchema, tmp_lob_table_schema) {
      if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard with version in inner table", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
        LOG_WARN("fail to get tenant schema version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id, refreshed_schema_version))) {
        LOG_WARN("fail to start trans", KR(ret), K(tenant_id), K(refreshed_schema_version));

      // 1. check data table exist. if exist, it's not allowed to drop
      } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id, data_table_id, exist))) {
        LOG_WARN("fail to check table exist", KR(ret), K(tenant_id), K(arg));
      } else if (exist) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("data table exist, so cannot drop lob table", KR(ret), K(arg));

      // 2. get and check lob meta table
      } else if (OB_FAIL(check_and_get_aux_table_schema(schema_guard, tenant_id, arg.get_aux_lob_meta_table_id(),
          data_table_id, ObTableType::AUX_LOB_META, lob_meta_table_schema_ptr))) {
        LOG_WARN("fail to get lob meta table schema", KR(ret), K(tenant_id), K(arg));
      // 3. get and check lob piece table
      } else if (OB_FAIL(check_and_get_aux_table_schema(schema_guard, tenant_id, arg.get_aux_lob_piece_table_id(),
          data_table_id, ObTableType::AUX_LOB_PIECE, lob_piece_table_schema_ptr))) {
        LOG_WARN("fail to get lob piece table schema", KR(ret), K(tenant_id), K(arg));

      // 4. drop lob meta table
      } else if (OB_FAIL(tmp_lob_table_schema.assign(*lob_meta_table_schema_ptr))) {
        LOG_WARN("fail to assign lob meta table schema", KR(ret));
      } else if (OB_FAIL(ddl_operator.drop_table(tmp_lob_table_schema, trans, nullptr/*ddl_stmt_str*/, false/*is_truncate_table*/,
          nullptr/*drop_table_set*/, false/*is_drop_db*/, true/*delete_priv*/, true/*is_force_drop_lonely_lob_aux_table*/))) {
        LOG_WARN("fail to drop lob meta table", KR(ret), K(tmp_lob_table_schema));
      } else if (FALSE_IT(tmp_lob_table_schema.reset())) {
      // 5. drop lob piece table
      } else if (OB_FAIL(tmp_lob_table_schema.assign(*lob_piece_table_schema_ptr))) {
        LOG_WARN("fail to assign lob piece table schema", KR(ret));
      } else if (OB_FAIL(ddl_operator.drop_table(tmp_lob_table_schema, trans, nullptr/*ddl_stmt_str*/, false/*is_truncate_table*/,
          nullptr/*drop_table_set*/, false/*is_drop_db*/, true/*delete_priv*/, true/*is_force_drop_lonely_lob_aux_table*/))) {
        LOG_WARN("fail to drop lob piece table", KR(ret), K(tmp_lob_table_schema));
      }
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN_RET(temp_ret, "trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(publish_schema(tenant_id))) {
      LOG_WARN("publish_schema failed", KR(ret));
    }
  }
  LOG_ERROR("NOTICE: there are force_drop_lonely_lob_aux_table", KR(ret), K(arg));
  return ret;
}

int ObDDLService::check_and_get_aux_table_schema(ObSchemaGetterGuard &schema_guard, const uint64_t tenant_id, const uint64_t aux_table_id,
                                                 const uint64_t data_table_id, const ObTableType table_type, const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, aux_table_id, table_schema))) {
    LOG_WARN("failed get_table_schema", KR(ret), K(tenant_id), K(aux_table_id), K(data_table_id), K(table_type));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("aux table schema is null", KR(ret), K(tenant_id), K(aux_table_id), K(data_table_id), K(table_type));
  } else if (table_schema->get_data_table_id() != data_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data table id is not match", KR(ret), K(tenant_id), K(data_table_id), K(table_type),
        K(table_schema->get_data_table_id()), KPC(table_schema));
  } else if (table_schema->get_table_type() != table_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is not expected", KR(ret), K(tenant_id), K(aux_table_id), K(data_table_id), K(table_type),
        K(table_schema->get_table_type()), KPC(table_schema));
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase