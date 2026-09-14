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

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_sql_udt_dict_schema_ctx.h"
#include "ob_log_meta_data_struct.h"
#include "ob_log_schema_getter.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace libobcdc
{

ObCDCSqlUdtDictSchemaCtx::ObCDCSqlUdtDictSchemaCtx()
  : is_inited_(false),
    tenant_info_(NULL),
    arena_allocator_(ObModIds::OB_LOG),
    db_schemas_()
{
}

ObCDCSqlUdtDictSchemaCtx::~ObCDCSqlUdtDictSchemaCtx()
{
  reset();
}

int ObCDCSqlUdtDictSchemaCtx::init(ObDictTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObCDCSqlUdtDictSchemaCtx init twice", KR(ret));
  } else {
    tenant_info_ = &tenant_info;
    is_inited_ = true;
  }

  return ret;
}

void ObCDCSqlUdtDictSchemaCtx::reset()
{
  db_schemas_.reset();
  arena_allocator_.reset();
  tenant_info_ = NULL;
  is_inited_ = false;
}

int ObCDCSqlUdtDictSchemaCtx::get_udt_info(const uint64_t tenant_id,
    const uint64_t udt_id,
    const share::schema::ObUDTTypeInfo *&udt_schema)
{
  int ret = OB_SUCCESS;
  udt_schema = NULL;

  if (OB_ISNULL(tenant_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid sql udt dict schema ctx", KR(ret));
  } else if (OB_FAIL(tenant_info_->get_udt_schema(tenant_id, udt_id, udt_schema, 0))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get_udt_schema from data_dict failed", KR(ret), K(tenant_id), K(udt_id));
    }
  }

  return ret;
}

int ObCDCSqlUdtDictSchemaCtx::get_or_create_database_schema_(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const share::schema::ObDatabaseSchema *&db_schema)
{
  int ret = OB_SUCCESS;
  db_schema = NULL;
  share::schema::ObDatabaseSchema *cached_schema = NULL;

  if (OB_ISNULL(tenant_info_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_info_ is null", KR(ret));
  } else {
    for (int64_t idx = 0; OB_ISNULL(cached_schema) && idx < db_schemas_.count(); ++idx) {
      share::schema::ObDatabaseSchema *schema = db_schemas_.at(idx);
      if (OB_NOT_NULL(schema) && database_id == schema->get_database_id()) {
        cached_schema = schema;
      }
    }

    if (OB_NOT_NULL(cached_schema)) {
      db_schema = cached_schema;
    } else {
      DBSchemaInfo db_schema_info;
      void *buf = NULL;
      if (OB_FAIL(tenant_info_->get_database_schema_info(database_id, db_schema_info))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_ERROR("get_database_schema_info from data_dict failed", KR(ret),
                    K(tenant_id), K(database_id));
        }
      } else if (OB_ISNULL(buf = arena_allocator_.alloc(sizeof(share::schema::ObDatabaseSchema)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc database schema failed", KR(ret), K(database_id));
      } else {
        cached_schema = new (buf) share::schema::ObDatabaseSchema(&arena_allocator_);
        cached_schema->set_tenant_id(tenant_id);
        cached_schema->set_database_id(database_id);
        cached_schema->set_schema_version(db_schema_info.version_);
        if (OB_FAIL(cached_schema->set_database_name(db_schema_info.name_))) {
          LOG_ERROR("set database name failed", KR(ret), K(database_id), K(db_schema_info));
        } else if (OB_FAIL(db_schemas_.push_back(cached_schema))) {
          LOG_ERROR("push database schema into cache failed", KR(ret), K(database_id));
        } else {
          db_schema = cached_schema;
        }
      }
    }
  }

  return ret;
}

int ObCDCSqlUdtDictSchemaCtx::get_database_schema(const uint64_t tenant_id,
    const uint64_t database_id,
    const share::schema::ObDatabaseSchema *&db_schema)
{
  int ret = OB_SUCCESS;
  db_schema = NULL;

  if (OB_FAIL(get_or_create_database_schema_(tenant_id, database_id, db_schema))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get_or_create_database_schema_ failed", KR(ret), K(tenant_id), K(database_id));
    }
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
