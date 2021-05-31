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

#define USING_LOG_PREFIX SHARE
#include "ob_sequence_ddl_proxy.h"
#include "lib/string/ob_string.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/sequence/ob_sequence_option_builder.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "rootserver/ob_ddl_operator.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObSequenceDDLProxy::ObSequenceDDLProxy(ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy& sql_proxy)
    : schema_service_(schema_service), sql_proxy_(sql_proxy)
{}

ObSequenceDDLProxy::~ObSequenceDDLProxy()
{}

int ObSequenceDDLProxy::create_sequence(ObSequenceSchema& seq_schema, const common::ObBitSet<>& opt_bitset,
    common::ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& schema_guard, const ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSequenceOptionBuilder opt_builder;
  uint64_t sequence_id = OB_INVALID_ID;
  ObArray<ObSchemaType> conflict_schema_types;
  if (OB_FAIL(schema_guard.check_oracle_object_exist(seq_schema.get_tenant_id(),
          seq_schema.get_database_id(),
          seq_schema.get_sequence_name(),
          SEQUENCE_SCHEMA,
          false,
          conflict_schema_types))) {
    LOG_WARN("fail to check oracle_object exist", K(ret), K(seq_schema));
  } else if (conflict_schema_types.count() > 0) {
    ret = OB_ERR_EXIST_OBJECT;
    LOG_WARN("Name is already used by an existing object", K(ret), K(seq_schema));
  } else if (OB_FAIL(opt_builder.build_create_sequence_option(opt_bitset, seq_schema.get_sequence_option()))) {
    LOG_WARN("fail build create sequence option", K(seq_schema), K(ret));
  } else {
    uint64_t new_sequence_id = OB_INVALID_ID;
    uint64_t tenant_id = seq_schema.get_tenant_id();
    int64_t new_schema_version = OB_INVALID_VERSION;
    ObSchemaService* schema_service = schema_service_.get_schema_service();
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("schema_service must not null", K(ret));
    } else if (OB_FAIL(schema_service->fetch_new_sequence_id(tenant_id, new_sequence_id))) {
      LOG_WARN("failed to fetch new_sequence_id", K(tenant_id), K(ret));
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else {
      seq_schema.set_sequence_id(new_sequence_id);
      seq_schema.set_schema_version(new_schema_version);
      if (OB_FAIL(schema_service->get_sequence_sql_service().insert_sequence(seq_schema, &trans, ddl_stmt_str))) {
        LOG_WARN("insert sequence info failed", K(seq_schema.get_sequence_name()), K(ret));
      } else {
        LOG_INFO("create sequence", K(lbt()), K(seq_schema));
      }
    }
  }
  return ret;
}

int ObSequenceDDLProxy::alter_sequence(share::schema::ObSequenceSchema& seq_schema,
    const common::ObBitSet<>& opt_bitset, common::ObMySQLTransaction& trans,
    share::schema::ObSchemaGetterGuard& schema_guard, const common::ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  uint64_t sequence_id = OB_INVALID_ID;
  bool exists = false;
  const share::schema::ObSequenceSchema* cur_sequence_schema = nullptr;
  const uint64_t tenant_id = seq_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSequenceOption& opt_new = seq_schema.get_sequence_option();
  ObSequenceOptionBuilder opt_builder;
  if (OB_FAIL(schema_guard.check_sequence_exist_with_name(seq_schema.get_tenant_id(),
          seq_schema.get_database_id(),
          seq_schema.get_sequence_name(),
          exists,
          sequence_id))) {
    LOG_WARN("fail get sequence", K(seq_schema), K(ret));
  } else if (!exists) {
    ret = OB_OBJECT_NAME_NOT_EXIST;
    LOG_WARN("sequence not exists", K(sequence_id), K(ret));
    LOG_USER_ERROR(OB_OBJECT_NAME_NOT_EXIST, "sequence");
  } else if (OB_FAIL(schema_guard.get_sequence_schema(seq_schema.get_tenant_id(), sequence_id, cur_sequence_schema))) {
    LOG_WARN("fail get sequence schema", K(ret));
  } else if (OB_ISNULL(cur_sequence_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL unexpected", K(ret));
  } else if (OB_FAIL(opt_builder.build_alter_sequence_option(
                 opt_bitset, cur_sequence_schema->get_sequence_option(), opt_new))) {
    LOG_WARN("fail build alter sequence option", K(seq_schema), K(*cur_sequence_schema), K(ret));
  } else {
    ObSchemaService* schema_service = schema_service_.get_schema_service();
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("schema_service must not null", K(ret));
    } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
      LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
    } else {
      seq_schema.set_sequence_id(sequence_id);
      seq_schema.set_schema_version(new_schema_version);
      if (OB_FAIL(
              schema_service->get_sequence_sql_service().replace_sequence(seq_schema, false, &trans, ddl_stmt_str))) {
        LOG_WARN("alter sequence info failed", K(seq_schema.get_sequence_name()), K(ret));
      } else {
        LOG_INFO("alter sequence", K(lbt()), K(seq_schema));
      }
    }
  }
  return ret;
}

int ObSequenceDDLProxy::drop_sequence(share::schema::ObSequenceSchema& seq_schema, common::ObMySQLTransaction& trans,
    share::schema::ObSchemaGetterGuard& schema_guard, const common::ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;

  uint64_t sequence_id = OB_INVALID_ID;
  const uint64_t tenant_id = seq_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  bool exists = false;
  ObSchemaService* schema_service = schema_service_.get_schema_service();

  OZ(rootserver::ObDDLOperator::drop_obj_privs(tenant_id,
      seq_schema.get_sequence_id(),
      static_cast<uint64_t>(ObObjectType::SEQUENCE),
      trans,
      schema_service_,
      schema_guard));

  if (OB_FAIL(schema_guard.check_sequence_exist_with_name(seq_schema.get_tenant_id(),
          seq_schema.get_database_id(),
          seq_schema.get_sequence_name(),
          exists,
          sequence_id))) {
    LOG_WARN("fail get sequence", K(seq_schema), K(ret));
  } else if (!exists) {
    ret = OB_OBJECT_NAME_NOT_EXIST;
    LOG_WARN("sequence does not exist", K(seq_schema), K(ret));
    LOG_USER_ERROR(OB_OBJECT_NAME_NOT_EXIST, "sequence");
  } else {
    seq_schema.set_sequence_id(sequence_id);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_sequence_sql_service().drop_sequence(
                 seq_schema, new_schema_version, &trans, ddl_stmt_str))) {
    LOG_WARN("drop sequence info failed", K(seq_schema.get_sequence_name()), K(ret));
  } else {
    LOG_INFO("drop sequence", K(lbt()), K(seq_schema));
  }

  return ret;
}

int ObSequenceDDLProxy::rename_sequence(share::schema::ObSequenceSchema& seq_schema, common::ObMySQLTransaction& trans,
    const common::ObString* ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  ObSchemaService* schema_service = schema_service_.get_schema_service();
  const uint64_t tenant_id = seq_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;

  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    seq_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_sequence_sql_service().replace_sequence(seq_schema, true, &trans, ddl_stmt_str))) {
      LOG_WARN("rename sequence info failed", K(ret), K(seq_schema.get_sequence_name()));
    } else {
      LOG_INFO("rename sequence", K(lbt()), K(seq_schema));
    }
  }

  return ret;
}
