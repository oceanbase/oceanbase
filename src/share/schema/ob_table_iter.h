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

#ifndef OCEANBASE_TABLE_SCHEMA_ITERATOR_H_
#define OCEANBASE_TABLE_SCHEMA_ITERATOR_H_

#include "lib/container/ob_array.h"
#include "share/ob_define.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;

class ObTenantIterator
{
public:
  ObTenantIterator();
  virtual ~ObTenantIterator() {}

  int init(ObMultiVersionSchemaService &schema_service);
  int init(ObSchemaGetterGuard &schema_guard);
  virtual int next(uint64_t &tenant_id);

  TO_STRING_KV(K_(is_inited), K_(cur_tenant_idx), K_(tenant_ids))
private:
  int get_tenant_ids(ObMultiVersionSchemaService &schema_service);
  bool is_inited_;
  int64_t cur_tenant_idx_;
  common::ObArray<uint64_t> tenant_ids_;
};

class ObITableIterator
{
public:
  ObITableIterator() {}
  virtual ~ObITableIterator() {}

  virtual int next(uint64_t &table_id) = 0;
};

class ObTenantTableIterator : public ObITableIterator
{
public:
  ObTenantTableIterator();
  virtual ~ObTenantTableIterator() {}

  int init(ObMultiVersionSchemaService *schema_service, const uint64_t tenant_id);
  virtual int next(uint64_t &table_id);

  TO_STRING_KV(K_(is_inited), K_(cur_table_idx), K_(table_ids));
private:
  int get_table_ids(ObMultiVersionSchemaService *schema_service, const uint64_t tenant_id);
  bool is_inited_;
  int64_t cur_table_idx_;
  common::ObArray<uint64_t> table_ids_;
};

/*
 * The meaning of PartitionEntity here refers to the physical unit that contains the entity partition
 */
class ObTenantPartitionEntityIterator : public ObITableIterator
{
public:
  ObTenantPartitionEntityIterator()
    : is_inited_(false),
      cur_idx_(0),
      entity_id_array_() {}
  virtual ~ObTenantPartitionEntityIterator() {}

  int init(ObSchemaGetterGuard &schema_guard, const uint64_t tenant_id);
  virtual int next(uint64_t &partition_entity_id) override;

  TO_STRING_KV(K_(is_inited), K_(cur_idx), K_(entity_id_array));
private:
  int get_partition_entity_id_array(
      ObSchemaGetterGuard &schema_guard,
      const uint64_t tenant_id);
private:
  bool is_inited_;
  int64_t cur_idx_;
  common::ObArray<uint64_t> entity_id_array_;
};

}//end of schema
}//end of share
}//end of oceanbase
#endif // OCEANBASE_TABLE_SCHEMA_ITERATOR_H_
