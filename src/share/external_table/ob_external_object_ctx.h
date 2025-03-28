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

#ifndef OB_EXTERNAL_OBJECT_CTX_H
#define OB_EXTERNAL_OBJECT_CTX_H

#include "sql/engine/cmd/ob_load_data_parser.h"

namespace oceanbase
{
namespace share
{

namespace schema
{
class ObSchemaGetterGuard;
class ObTableSchema;
class ObSimpleDatabaseSchema;
class ObDatabaseSchema;
} // namespace schema

enum class ObExternalObjectType
{
  INVALID = 0,
  TABLE_SCHEMA,
  DATABASE_SCHEMA
};

struct ObExternalObject
{
  OB_UNIS_VERSION(1);

public:
  // TABLE_SCHEMA will store catalog_id/database_id/table_id/table_name
  // DATABASE_SCHEMA will store catalog_id/database_id/database_name
  ObExternalObjectType type = ObExternalObjectType::INVALID;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  uint64_t catalog_id = OB_INVALID_ID;
  uint64_t database_id = OB_INVALID_ID;
  common::ObString database_name;
  uint64_t table_id = OB_INVALID_ID;
  common::ObString table_name;
  TO_STRING_KV(K(type), K(tenant_id), K(catalog_id), K(database_id), K(database_name), K(table_id), K(table_name));
};

class ObExternalObjectCtx
{
  OB_UNIS_VERSION(1);

public:
  explicit ObExternalObjectCtx(common::ObIAllocator &allocator) : allocator_(allocator), external_objects_(allocator) {}
  ~ObExternalObjectCtx() = default;
  int init(int64_t capacity);
  int add_table_schema(const uint64_t tenant_id,
                       const uint64_t catalog_id,
                       const uint64_t database_id,
                       const uint64_t table_id,
                       const common::ObString &table_name);
  int add_database_schema(const uint64_t tenant_id,
                          const uint64_t catalog_id,
                          const uint64_t database_id,
                          const common::ObString &database_name);
  int get_table_schema(const uint64_t table_id, const ObExternalObject *&obj) const;
  int get_database_schema(const uint64_t database_id, const ObExternalObject *&obj) const;
  const common::ObIArray<ObExternalObject> &get_external_objects() const;
  TO_STRING_KV(K_(external_objects));
private:
  common::ObIAllocator &allocator_;
  common::ObFixedArray<ObExternalObject, common::ObIAllocator> external_objects_;
};

} // namespace share
} // namespace oceanbase

#endif // OB_EXTERNAL_OBJECT_ID_NAME_MAPPING_H
