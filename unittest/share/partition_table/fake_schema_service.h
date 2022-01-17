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

#ifndef OCEANBASE_ROOTSERVER_TEST_FAKE_SERVER_SERVICE_H
#define OCEANBASE_ROOTSERVER_TEST_FAKE_SERVER_SERVICE_H
#define private public
#include "../schema/mock_schema_service.h"
namespace oceanbase {
using namespace common;
namespace share {
namespace schema {
class FakeSchemaService : public MockSchemaService {
public:
  const static uint64_t TENANT_ID = 1;
  const static int64_t TABLE_COUNT = 256;
  FakeSchemaService() : null_table_id_(0), table_cnt_(0), version_(0)
  {}
  int init()
  {
    return MockSchemaService::init();
  }
  virtual int gen_tenant_schema(const uint64_t tenant_id, ObTenantSchema& tenant_schema);
  virtual int gen_table_schema(const uint64_t tenant_id, const uint64_t pure_id, const uint64_t pure_db_id,
      const uint64_t pure_tg_id, ObTableSchema& table_schema);
  virtual int get_table_schema(const uint64_t table_id, const ObTableSchema*& table_schema);
  virtual int batch_get_next_table(
      const ObTenantTableId tenant_table_id, const int64_t get_size, common::ObIArray<ObTenantTableId>& table_array);
  virtual int64_t get_schema_version() const
  {
    return version_;
  }
  virtual void set_schema_version(const uint64_t version)
  {
    version_ = static_cast<int64_t>(version);
  }

  int add_tenant(const ObTenantSchema& tenant_schema)
  {
    return MockSchemaService::add_tenant_schema(tenant_schema, version_++);
  }
  int get_schema_guard(ObSchemaGetterGuard& guard)
  {
    return MockSchemaService::get_schema_guard(guard, version_);
  }
  int add_table(ObTableSchema& table_schema, const bool exist = true)
  {
    int ret = OB_SUCCESS;
    const ObTenantSchema* tenant_schema = NULL;
    if (OB_SUCC(ret)) {
      ObSchemaGetterGuard schema_guard;
      if (OB_FAIL(get_schema_guard(schema_guard))) {
        SHARE_SCHEMA_LOG(WARN, "get schema guard fail", K(ret));
      } else if (OB_FAIL(schema_guard.get_tenant_info(table_schema.get_tenant_id(), tenant_schema))) {
        SHARE_SCHEMA_LOG(WARN, "get_tenant_info failed");
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_TENANT_NOT_EXIST;
        SHARE_SCHEMA_LOG(WARN, "tenant schema is null", K(ret));
      } else if (OB_FAIL(add_table_schema(table_schema, version_++))) {
        SHARE_SCHEMA_LOG(WARN, "add table schema failed", K(ret));
      }
    }
    if (table_cnt_ == TABLE_COUNT) {
      ret = OB_ERROR;
    } else if (OB_FAIL(table_ids_.push_back(table_schema.get_table_id()))) {
      SHARE_SCHEMA_LOG(WARN, "push_back failed", K(ret));
      //} else if (OB_FAIL(schema_mgr_.add_new_table_schema(table_schema))) {
      //  SHARE_SCHEMA_LOG(WARN, "add_new_table_schema failed", K(ret));
    } else if (exist) {
      if (OB_FAIL(tables_[table_cnt_].assign(table_schema))) {
        SHARE_SCHEMA_LOG(WARN, "fail to assign schema", K(ret));
      } else {
        ++table_cnt_;
      }
    }
    // if (combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_CORE_META_TABLE_TID) == table_schema.get_table_id()) {
    //  if (NULL == schema_mgr_.get_table_schema(table_schema.get_table_id())) {
    //    OB_ASSERT(false);
    //  }
    //}
    return ret;
  }
  int del_table(const uint64_t table_id)
  {
    int ret = OB_SUCCESS;
    int64_t index = -1;
    for (int64_t i = 0; i < table_ids_.count(); ++i) {
      if (table_ids_[i] == table_id) {
        index = i;
        break;
      }
    }
    if (-1 != index) {
      if (OB_FAIL(table_ids_.remove(index))) {
        SHARE_SCHEMA_LOG(WARN, "remove failed", K(index), K(ret));
      } else {
        common::hash::ObHashSet<uint64_t> del_tables;
        del_tables.create(1024);
        del_tables.set_refactored(table_id);
        for (int64_t i = index; OB_SUCC(ret) && i < table_cnt_ - 1; ++i) {
          if (OB_FAIL(tables_[i].assign(tables_[i + 1]))) {
            LOG_WARN("fail to assign schema", K(ret));
          }
        }
        --table_cnt_;
      }
    }
    return ret;
  }
  // virtual const ObSchemaManager *get_schema_manager_by_version(const int64_t version = 0,
  //                                                             const bool for_merge = false)
  //{
  //  UNUSED(version);
  //  UNUSED(for_merge);
  //  return &schema_mgr_;
  //}
  // virtual int release_schema(const ObSchemaManager *schema)
  //{
  //  UNUSED(schema);
  //  return OB_SUCCESS;
  //}
  // virtual int get_schema_manager(ObSchemaManagerGuard &guard)
  //{
  //  int ret = OB_SUCCESS;
  //  if (OB_FAIL(guard.init(*this, &schema_mgr_))) {
  //    SHARE_SCHEMA_LOG(WARN, "guard init failed", K(ret));
  //  }
  //  return ret;
  //}
  virtual int check_table_exist(const uint64_t table_id, bool& exist);

public:
  uint64_t null_table_id_;
  ObArray<uint64_t> table_ids_;
  ObTableSchema tables_[TABLE_COUNT];
  int64_t table_cnt_;
  int64_t version_;
  // ObSchemaManager schema_mgr_;
};

int FakeSchemaService::batch_get_next_table(
    const ObTenantTableId tenant_table_id, const int64_t get_size, common::ObIArray<ObTenantTableId>& table_array)
{
  int ret = OB_SUCCESS;
  if (tenant_table_id.table_id_ > 1 || table_cnt_ <= 0) {
    return OB_ITER_END;
  }
  ObTenantTableId ttid;
  ttid.tenant_id_ = TENANT_ID;
  table_array.reuse();
  for (int64_t i = 0; i < table_ids_.count() && i < get_size; ++i) {
    ttid.table_id_ = table_ids_.at(i);
    table_array.push_back(ttid);
  }
  return ret;
}

int FakeSchemaService::get_table_schema(const uint64_t table_id, const ObTableSchema*& table_schema)
{
  int ret = OB_TABLE_NOT_EXIST;
  table_schema = NULL;
  if (table_id == null_table_id_) {
    ret = OB_SUCCESS;
  } else {
    for (int64_t i = 0; i < table_cnt_; ++i) {
      if (table_id == tables_[i].get_table_id()) {
        ret = OB_SUCCESS;
        table_schema = &tables_[i];
        break;
      }
    }
  }
  return ret;
}

int FakeSchemaService::check_table_exist(const uint64_t table_id, bool& exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  for (int64_t i = 0; i < table_cnt_; ++i) {
    if (table_id == tables_[i].get_table_id()) {
      exist = true;
      break;
    }
  }
  return ret;
}

int FakeSchemaService::gen_tenant_schema(const uint64_t tenant_id, ObTenantSchema& tenant_schema)
{
  int ret = OB_SUCCESS;
  char tenant_name[64];
  if (snprintf(tenant_name, 64, "tenant_%lu", tenant_id) >= 64) {
    ret = OB_BUF_NOT_ENOUGH;
    SHARE_SCHEMA_LOG(WARN, "buf not enough", K(ret));
  } else {
    tenant_schema.set_tenant_id(tenant_id);
    tenant_schema.set_tenant_name(tenant_name);
    tenant_schema.set_comment("this is a test tenant");
    tenant_schema.add_zone("zone");
    tenant_schema.set_primary_zone("zone");
    tenant_schema.set_locality("");
  }
  return ret;
}

int FakeSchemaService::gen_table_schema(const uint64_t tenant_id, const uint64_t pure_id, const uint64_t pure_db_id,
    const uint64_t pure_tg_id, ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  table_schema.reset();
  ObInnerTableSchema::all_core_table_schema(table_schema);
  char table_name[64];
  if (snprintf(table_name, 64, "table_%lu", combine_id(tenant_id, pure_id)) >= 64) {
    ret = OB_BUF_NOT_ENOUGH;
    SHARE_SCHEMA_LOG(WARN, "buf not enough", K(ret));
  } else {
    table_schema.set_table_name(table_name);
    table_schema.set_tenant_id(tenant_id);
    table_schema.set_table_id(combine_id(tenant_id, pure_id));
    table_schema.set_database_id(combine_id(tenant_id, pure_db_id));
    table_schema.set_tablegroup_id(combine_id(tenant_id, pure_tg_id));
  }
  return ret;
}
}  // end namespace schema
}  // end namespace share
}  // end namespace oceanbase
#endif
