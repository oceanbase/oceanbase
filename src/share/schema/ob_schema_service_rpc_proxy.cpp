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

#include "share/schema/ob_schema_service_rpc_proxy.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase {
namespace obrpc {
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

/*
int ObGetLatestSchemaVersionP::process()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema_service_)) {
    ret = OB_INNER_STAT_ERROR;
    SHARE_SCHEMA_LOG(WARN, "inner stat error", K(ret), K(schema_service_));
  } else {
    result_ = schema_service_->get_refreshed_schema_version();
  }

  return ret;
}
*/

OB_SERIALIZE_MEMBER(
    ObAllSchema, tenant_, users_, databases_, tablegroups_, tables_, outlines_, db_privs_, table_privs_);

ObGetAllSchemaP::ObGetAllSchemaP(ObMultiVersionSchemaService* schema_service)
    : schema_service_(schema_service), buf_(NULL), buf_len_(0)
{}

ObGetAllSchemaP::~ObGetAllSchemaP()
{}

int ObGetAllSchemaP::before_process()
{
  /*
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema_service_)) {
    ret = OB_INNER_STAT_ERROR;
    SHARE_SCHEMA_LOG(WARN, "inner stat error", K(ret), K(schema_service_));
  } else {
    SHARE_SCHEMA_LOG(INFO, "get all schema", K(arg_));

    const int64_t schema_version = arg_.schema_version_;
    const ObString &tenant_name = arg_.tenant_name_;
    uint64_t tenant_id = OB_INVALID_ID;
    ObAllSchema all_schema;
    ObSchemaGetterGuard guard;
    const ObTenantSchema *tenant = NULL;
    ObArray<const ObUserInfo *> users;
    ObArray<const ObDatabaseSchema *> databases;
    ObArray<const ObTablegroupSchema *> tablegroups;
    ObArray<const ObTableSchema *> tables;
    ObArray<const ObOutlineInfo *> outlines;
    ObArray<const ObDBPriv *> db_privs;
    ObArray<const ObTablePriv *> table_privs;
    if (OB_FAIL(schema_service_->get_schema_guard(guard, schema_version))) {
      SHARE_SCHEMA_LOG(WARN, "get schema guard failed", K(ret));
    } else if (OB_FAIL(guard.get_tenant_info(tenant_name, tenant))) {
      SHARE_SCHEMA_LOG(WARN, "get tenant schema failed", K(ret));
    } else if (NULL == tenant) {
      ret = OB_TENANT_NOT_EXIST;
      SHARE_SCHEMA_LOG(WARN, "tenant not exist", K(ret));
    } else if (FALSE_IT(tenant_id = tenant->get_tenant_id())) {
    } else if (OB_FAIL(guard.get_user_schemas_in_tenant(tenant_id, users))) {
      SHARE_SCHEMA_LOG(WARN, "get user schemas failed", K(ret));
    } else if (OB_FAIL(guard.get_database_schemas_in_tenant(tenant_id, databases))) {
      SHARE_SCHEMA_LOG(WARN, "get database schemas failed", K(ret));
    // TODO: no current interface so far
    //} else if (OB_FAIL(guard.get_tablegroup_schemas_in_tenant(tenant_id, tablegroups))) {
    //  SHARE_SCHEMA_LOG(WARN, "get tablegroup schemas failed", K(ret));
    } else if (OB_FAIL(guard.get_table_schemas_in_tenant(tenant_id, tables))) {
      SHARE_SCHEMA_LOG(WARN, "get table schemas failed", K(ret));
    } else if (OB_FAIL(guard.get_outline_infos_in_tenant(tenant_id, outlines))) {
      SHARE_SCHEMA_LOG(WARN, "get outline infos failed", K(ret));
    } else if (OB_FAIL(guard.get_db_priv_with_tenant_id(tenant_id, db_privs))) {
      SHARE_SCHEMA_LOG(WARN, "get db privs failed", K(ret));
    } else if (OB_FAIL(guard.get_table_priv_with_tenant_id(tenant_id, table_privs))) {
      SHARE_SCHEMA_LOG(WARN, "get table privs failed", K(ret));
    } else {
      all_schema.tenant_ = *tenant;
      #define ADD_SCHEMA(SCHEMA)                         \
        FOREACH_CNT_X(SCHEMA, SCHEMA##s, OB_SUCC(ret)) { \
          if (OB_ISNULL(SCHEMA)) {                       \
            ret = OB_ERR_UNEXPECTED;                                               \
            SHARE_SCHEMA_LOG(WARN, "NULL ptr", #SCHEMA, SCHEMA, K(ret));           \
          } else if (OB_ISNULL(*SCHEMA)) {                                         \
            ret = OB_ERR_UNEXPECTED;                                               \
            SHARE_SCHEMA_LOG(WARN, "NULL ptr", #SCHEMA"_schema", *SCHEMA, K(ret)); \
          } else if (OB_FAIL(all_schema.SCHEMA##s_.push_back(**SCHEMA))) {         \
            SHARE_SCHEMA_LOG(WARN, "add "#SCHEMA" failed", K(ret));  \
          }                                                          \
        }
      ADD_SCHEMA(user);
      ADD_SCHEMA(database);
      ADD_SCHEMA(tablegroup);
      ADD_SCHEMA(table);
      ADD_SCHEMA(outline);
      ADD_SCHEMA(db_priv);
      ADD_SCHEMA(table_priv);
      #undef ADD_SCHEMA
    }
    if (OB_SUCC(ret)) {
      char *buf = NULL;
      int64_t seri_size = all_schema.get_serialize_size();
      int64_t seri_pos = 0;
      if (NULL == (buf = static_cast<char*>(ob_malloc(seri_size, ObModIds::OB_SCHEMA_RPC_BUF)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_SCHEMA_LOG(ERROR, "fail to alloc buffer", K(seri_size));
      } else if (OB_FAIL(all_schema.serialize(buf, seri_size, seri_pos))) {
        SHARE_SCHEMA_LOG(WARN, "serialize failed", K(ret));
      } else {
        buf_ = buf;
        buf_len_ = seri_size;
      }
    }
  }

  return ret;
  */
  return OB_NOT_SUPPORTED;
}

int ObGetAllSchemaP::process()
{
  int ret = OB_SUCCESS;

  static const int64_t BUF_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
  int64_t pos = 0;
  while (OB_SUCC(ret)) {
    if (pos >= buf_len_) {
      break;
    }
    int64_t buf_len = pos + BUF_SIZE <= buf_len_ ? BUF_SIZE : buf_len_ - pos;
    result_.reset();
    if (!result_.set_data(buf_ + pos, buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "set data failed", K(ret));
    } else if (FALSE_IT(result_.get_position() = buf_len)) {
      // not reach here
    } else if (OB_FAIL(flush())) {
      SHARE_SCHEMA_LOG(WARN, "flush failed");
    } else {
      pos += buf_len;
    }
  }

  return ret;
}

int ObGetAllSchemaP::after_process()
{
  int ret = OB_SUCCESS;
  if (NULL != buf_) {
    ob_free(buf_);
  }
  return ret;
}

}  // namespace obrpc

}  // namespace oceanbase
