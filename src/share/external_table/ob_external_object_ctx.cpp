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

#define USING_LOG_PREFIX SQL
#include "share/external_table/ob_external_object_ctx.h"

#include "share/ob_define.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/ob_sql_context.h"

namespace oceanbase
{
namespace share
{

OB_DEF_SERIALIZE(ObExternalObject)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, type, tenant_id, catalog_id, database_id, database_name, table_id, table_name);
  return ret;
}

OB_DEF_DESERIALIZE(ObExternalObject)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, type, tenant_id, catalog_id, database_id, database_name, table_id, table_name);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExternalObject)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, type, tenant_id, catalog_id, database_id, database_name, table_id, table_name);
  return len;
}

int ObExternalObjectCtx::init(int64_t capacity)
{
  int ret = OB_SUCCESS;
  OZ(external_objects_.init(capacity));
  return ret;
}

int ObExternalObjectCtx::add_table_schema(const uint64_t tenant_id,
                                          const uint64_t catalog_id,
                                          const uint64_t database_id,
                                          const uint64_t table_id,
                                          const common::ObString &table_name)
{
  int ret = OB_SUCCESS;
  ObExternalObject object;
  object.type = ObExternalObjectType::TABLE_SCHEMA;
  object.tenant_id = tenant_id;
  object.catalog_id = catalog_id;
  object.database_id = database_id;
  object.table_id = table_id;
  OZ(ob_write_string(allocator_, table_name, object.table_name));
  OZ(external_objects_.push_back(object));
  return ret;
}

int ObExternalObjectCtx::add_database_schema(const uint64_t tenant_id,
                                             const uint64_t catalog_id,
                                             const uint64_t database_id,
                                             const common::ObString &database_name)
{
  int ret = OB_SUCCESS;
  ObExternalObject object;
  object.type = ObExternalObjectType::DATABASE_SCHEMA;
  object.tenant_id = tenant_id;
  object.catalog_id = catalog_id;
  object.database_id = database_id;
  OZ(ob_write_string(allocator_, database_name, object.database_name));
  OZ(external_objects_.push_back(object));
  return ret;
}

int ObExternalObjectCtx::get_table_schema(const uint64_t table_id, const ObExternalObject *&obj) const
{
  int ret = OB_SUCCESS;
  obj = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < external_objects_.count(); i++) {
    const ObExternalObject &cur_obj = external_objects_.at(i);
    if (cur_obj.type == ObExternalObjectType::TABLE_SCHEMA && cur_obj.table_id == table_id) {
      obj = &cur_obj;
      break;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(obj)) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObExternalObjectCtx::get_database_schema(const uint64_t database_id, const ObExternalObject *&obj) const
{
  int ret = OB_SUCCESS;
  obj = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < external_objects_.count(); i++) {
    const ObExternalObject &cur_obj = external_objects_.at(i);
    if (cur_obj.type == ObExternalObjectType::DATABASE_SCHEMA && cur_obj.database_id == database_id) {
      obj = &cur_obj;
      break;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(obj)) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

const common::ObIArray<ObExternalObject> &ObExternalObjectCtx::get_external_objects() const { return external_objects_; }

OB_DEF_SERIALIZE(ObExternalObjectCtx)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, external_objects_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExternalObjectCtx)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, external_objects_);

  // hold ObString's memory
  for (int64_t i = 0; OB_SUCC(ret) && i < external_objects_.count(); i++) {
    ObExternalObject &object = external_objects_.at(i);
    OZ(ob_write_string(allocator_, object.database_name, object.database_name));
    OZ(ob_write_string(allocator_, object.table_name, object.table_name));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExternalObjectCtx)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, external_objects_);
  return len;
}

} // namespace share
} // namespace oceanbase