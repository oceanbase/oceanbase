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

#ifndef MOCK_MULTI_VERSION_SCHEMA_SERVICE_FOR_FALLBACK_H_
#define MOCK_MULTI_VERSION_SCHEMA_SERVICE_FOR_FALLBACK_H_

#include <gtest/gtest.h>
#define private public
#include "share/schema/ob_multi_version_schema_service.h"
#define private public
#include "share/schema/ob_server_schema_service.h"


namespace oceanbase
{
namespace share
{
namespace schema
{

class MockMultiVersionSchemaServiceForFallback : public ObMultiVersionSchemaService
{
public:
  MockMultiVersionSchemaServiceForFallback()
  {
    baseline_schema_version_ = 0;
  }
  virtual ~MockMultiVersionSchemaServiceForFallback() {}

  int init(const int64_t slot_for_cache, const int64_t slot_for_liboblog);
  int destory();
  int prepare(const int64_t max_version);
  void dump();
  void dump_schema_mgr();
  void dump_mem_mgr() {
    mem_mgr_.dump();
  }
  void dump_mem_mgr_for_liboblog() {
    mem_mgr_for_liboblog_.dump();
  }
  int add_tenant(const ObSimpleTenantSchema &tenant);
  int add_sys_variable(const ObSimpleSysVariableSchema &sys_variable);
  int add_table(const ObSimpleTableSchemaV2 &table);
  int add_database(const ObSimpleDatabaseSchema &databse);
  void get_tenant_schema(const int64_t tenant_id,
                         ObSimpleTenantSchema &tenant);

  virtual int fallback_schema_mgr(ObSchemaMgr &schema_mgr,
                                  const int64_t schema_version);

};

int MockMultiVersionSchemaServiceForFallback::init(const int64_t slot_for_cache,
                                                   const int64_t slot_for_liboblog)
{
  int ret = common::OB_SUCCESS;
  ret = mem_mgr_.init(common::ObModIds::OB_SCHEMA_MGR);
  if (OB_FAIL(ret)) return ret;

  ret = mem_mgr_for_liboblog_.init(common::ObModIds::OB_SCHEMA_MGR_FOR_ROLLBACK);
  if (OB_FAIL(ret)) return ret;

  void *tmp_ptr = NULL;
  common::ObIAllocator *allocator = NULL;
  ret = mem_mgr_.alloc(sizeof(ObSchemaMgr), tmp_ptr, &allocator);
  if (OB_FAIL(ret)) return ret;

  schema_mgr_for_cache_ = new (tmp_ptr) ObSchemaMgr(*allocator);
  ret = schema_mgr_for_cache_->init();
  if (OB_FAIL(ret)) return ret;

//  refreshed_schema_version_ = 1;

  //init slot
  ret = schema_mgr_cache_.init(slot_for_cache);
  if (OB_FAIL(ret)) return ret;
  ret = schema_mgr_cache_for_liboblog_.init(slot_for_liboblog);
  if (OB_FAIL(ret)) return ret;

  //make it cross check_inner_stat
  schema_service_ = (ObSchemaService *)(0x1);
  proxy_ = (common::ObMySQLProxy *)(0x1);
  config_ = (common::ObCommonConfig *)(0x1);
  init_ = true;

//  ret = add_schema(true);
//  if (OB_FAIL(ret)) return ret;

  return common::OB_SUCCESS;
}

int MockMultiVersionSchemaServiceForFallback::destory()
{
  schema_service_ = NULL;
  proxy_ = NULL;
  config_ = NULL;
  return 0;
}

int MockMultiVersionSchemaServiceForFallback::add_table(const ObSimpleTableSchemaV2 &table)
{
  int ret =  schema_mgr_for_cache_->add_table(table);
  if (OB_FAIL(ret)) return ret;
  schema_mgr_for_cache_->set_schema_version(table.get_schema_version());
  return ret;
}

int MockMultiVersionSchemaServiceForFallback::add_database(const ObSimpleDatabaseSchema &database)
{
  int ret = schema_mgr_for_cache_->add_database(database);
  if (OB_FAIL(ret)) return ret;
  schema_mgr_for_cache_->set_schema_version(database.get_schema_version());
  return ret;
}

int MockMultiVersionSchemaServiceForFallback::add_tenant(const ObSimpleTenantSchema &tenant)
{
  int ret = schema_mgr_for_cache_->add_tenant(tenant);
  if (OB_FAIL(ret)) return ret;
  schema_mgr_for_cache_->set_schema_version(tenant.get_schema_version());
  return ret;
}

int MockMultiVersionSchemaServiceForFallback::add_sys_variable(const ObSimpleSysVariableSchema &sys_variable)
{
  int ret = schema_mgr_for_cache_->sys_variable_mgr_.add_sys_variable(sys_variable);
  if (OB_FAIL(ret)) return ret;
  schema_mgr_for_cache_->set_schema_version(sys_variable.get_schema_version());
  return ret;
}

void MockMultiVersionSchemaServiceForFallback::dump()
{
  schema_mgr_for_cache_->dump();
}

void MockMultiVersionSchemaServiceForFallback::dump_schema_mgr()
{
  schema_mgr_cache_for_liboblog_.dump();
}

int MockMultiVersionSchemaServiceForFallback::fallback_schema_mgr(ObSchemaMgr &schema_mgr,
                                                                  const int64_t schema_version)
{
  int ret = common::OB_SUCCESS;
  int64_t cur_version = schema_mgr.get_schema_version();
  SHARE_SCHEMA_LOG(INFO, "fallback schema", K(cur_version), K(schema_version));
  if (cur_version > schema_version) {//do fallback
    for (int64_t i = cur_version; i > schema_version; --i) {
      ret = schema_mgr.del_tenant(i); //schema_version is equal to tenant_id
      if (OB_FAIL(ret)) {
        schema_mgr.dump();
        return ret;
      }
    }
    schema_mgr.set_schema_version(schema_version);
  } else {
    for (int64_t i = cur_version + 1; i <= schema_version; ++i) {//do forward
      ObSimpleTenantSchema tenant;
      tenant.set_schema_version(i);
      get_tenant_schema(i, tenant);
      ret = schema_mgr.add_tenant(tenant);
      if (OB_FAIL(ret)) return ret;
      ObSimpleSysVariableSchema sys_variable;
      sys_variable.set_tenant_id(tenant.get_tenant_id());
      sys_variable.set_schema_version(tenant.get_schema_version());
      sys_variable.set_name_case_mode(common::OB_LOWERCASE_AND_INSENSITIVE);
      ret = schema_mgr.sys_variable_mgr_.add_sys_variable(sys_variable);
      if (OB_FAIL(ret)) return ret;
    }
    schema_mgr.set_schema_version(schema_version);
  }
  return ret;
}

int MockMultiVersionSchemaServiceForFallback::prepare(const int64_t max_version)
{
//  ObSimpleTenantSchema tenant;
//  get_tenant_schema(OB_SYS_TENANT_ID, tenant);
//  ret = schema_service.add_tenant(tenant);
//  ASSERT_EQ(OB_SUCCESS, ret);
//  ret = schema_service.add_schema(); //add to schema_mgr cache
//  ASSERT_EQ(OB_SUCCESS, ret);

  int ret = common::OB_SUCCESS;
  int64_t user_tenant_id = 1;
  int64_t schema_version = 1;
  for (; schema_version <= max_version;) {
    ObSimpleTenantSchema user_tenant;
    user_tenant.set_schema_version(schema_version++);
    get_tenant_schema(user_tenant_id++, user_tenant);
    ret = add_tenant(user_tenant);
    if (OB_FAIL(ret)) return ret;
    ret = add_schema(); //add to schema_mgr cache
    if (OB_FAIL(ret)) return ret;
    ObSimpleSysVariableSchema sys_variable;
    sys_variable.set_tenant_id(user_tenant.get_tenant_id());
    sys_variable.set_schema_version(user_tenant.get_schema_version());
    sys_variable.set_name_case_mode(common::OB_LOWERCASE_AND_INSENSITIVE);
    ret = add_sys_variable(sys_variable);
    if (OB_FAIL(ret)) return ret;
    ret = add_schema(); //add to schema_mgr cache
    if (OB_FAIL(ret)) return ret;
  }
  return ret;
}

void MockMultiVersionSchemaServiceForFallback::get_tenant_schema(const int64_t tenant_id,
                                                                 ObSimpleTenantSchema &tenant)
{
  char name[20];
  sprintf(name, "%s_%ld", "tenant", tenant_id);
  tenant.set_tenant_id(tenant_id);
  tenant.set_tenant_name(common::ObString::make_string(name));
}


} //schema
} //share
} //oceanbase


#endif //MOCK_MULTI_VERSION_SCHEMA_SERVICE
