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

#ifndef OCEANBASE_LIBOBCDC_OB_CDC_SQL_UDT_DICT_SCHEMA_CTX_H_
#define OCEANBASE_LIBOBCDC_OB_CDC_SQL_UDT_DICT_SCHEMA_CTX_H_

#include "lib/container/ob_se_array.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObUDTTypeInfo;
class ObDatabaseSchema;
} // namespace schema
} // namespace share

namespace libobcdc
{

class ObDictTenantInfo;


class ObCDCSqlUdtDictSchemaCtx
{
public:
  ObCDCSqlUdtDictSchemaCtx();
  ~ObCDCSqlUdtDictSchemaCtx();

  int init(ObDictTenantInfo &tenant_info);
  void reset();

  int get_udt_info(const uint64_t tenant_id,
                   const uint64_t udt_id,
                   const share::schema::ObUDTTypeInfo *&udt_schema);
  int get_database_schema(const uint64_t tenant_id,
                          const uint64_t database_id,
                          const share::schema::ObDatabaseSchema *&db_schema);

private:
  int get_or_create_database_schema_(const uint64_t tenant_id,
                                     const uint64_t database_id,
                                     const share::schema::ObDatabaseSchema *&db_schema);

private:
  bool is_inited_;
  ObDictTenantInfo *tenant_info_;
  common::ObArenaAllocator arena_allocator_;
  common::ObSEArray<share::schema::ObDatabaseSchema *, 4> db_schemas_;
};

} // namespace libobcdc
} // namespace oceanbase

#endif
