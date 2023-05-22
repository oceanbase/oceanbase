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

#ifndef MOCK_SCHEMA_SERVICE_H_
#define MOCK_SCHEMA_SERVICE_H_

#define private public
#define protected public

#include "share/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_cache.h"
#include "share/schema/ob_schema_mgr.h"


namespace oceanbase
{
using namespace common;
namespace common
{
class ObKVCacheHandle;
}
namespace share
{
namespace schema
{

class MockObTableSchema : public ObTableSchema
{
public:
  MockObTableSchema(common::ObIAllocator *allocator) : ObTableSchema(allocator) {}
  MockObTableSchema() {}
  virtual int get_paxos_replica_num(ObSchemaGetterGuard &schema_guard, int64_t &num) const
  {
    UNUSED(schema_guard);
    num = 3;
    SHARE_SCHEMA_LOG(INFO, "MockTableSchema", K(num), "table_id", get_table_id());
    return OB_SUCCESS;
  }
};

class MockSchemaService : public ObMultiVersionSchemaService
{
static const int64_t SCHEMA_CACHE_BUCKET_NUM = 512;
static const int64_t DEFAULT_TENANT_SET_SIZE = 64;
typedef common::hash::ObHashMap<ObSchemaCacheKey,
                                const ObSchemaCacheValue*,
                                common::hash::NoPthreadDefendMode> NoSwapCache;
public:
  MockSchemaService() {}
  virtual ~MockSchemaService() {}
  int init()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObSysTableChecker::instance().init())) {
      SHARE_SCHEMA_LOG(WARN, "fail to init tenant space table checker", K(ret));
    } else if (OB_FAIL(cache_.create(SCHEMA_CACHE_BUCKET_NUM,
        ObModIds::OB_SCHEMA_CACHE_SYS_CACHE_MAP))) {
      SHARE_SCHEMA_LOG(WARN, "init cache failed", K(ret));
    } else if (mgr_.init()) {
      SHARE_SCHEMA_LOG(WARN, "init mgr failed", K(ret));
    } else {
      ObSimpleTenantSchema simple_schema;
      ObTenantSchema sys_tenant;
      sys_tenant.set_tenant_id(OB_SYS_TENANT_ID);
      sys_tenant.set_schema_version(OB_CORE_SCHEMA_VERSION);
      sys_tenant.set_locality("auto_locality_strategy");
      sys_tenant.add_zone("zone");
      ObSysVariableSchema sys_variable;
      sys_variable.set_tenant_id(OB_SYS_TENANT_ID);
      sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
      sys_variable.set_schema_version(OB_CORE_SCHEMA_VERSION);
      if (OB_FAIL(sys_tenant.set_tenant_name(OB_SYS_TENANT_NAME))) {
        SHARE_SCHEMA_LOG(WARN, "Set tenant name error", K(ret));
      } else if (OB_FAIL(convert_to_simple_schema(sys_tenant, simple_schema))) {
        SHARE_SCHEMA_LOG(WARN, "convert_to_simple_schema failed", K(ret));
      } else if (OB_FAIL(mgr_.add_tenant(simple_schema))) {
        SHARE_SCHEMA_LOG(WARN, "add tenant failed", K(ret));
      } else if (OB_FAIL(add_schema_to_cache(TENANT_SCHEMA, sys_tenant.get_tenant_id(),
                                             sys_tenant.get_schema_version(), sys_tenant))) {
        SHARE_SCHEMA_LOG(WARN, "add schema to cache failed", K(ret));
      } else if (OB_FAIL(add_sys_variable_schema(sys_variable, sys_variable.get_schema_version()))) {
        SHARE_SCHEMA_LOG(WARN, "add schema to cache failed", K(ret));
      }
    }
    return ret;
  }


//  MOCK_METHOD2(get_all_schema,
//      int(ObSchemaManager &out_schema, const int64_t frozen_version));
//  MOCK_METHOD2(check_table_exist,
//      int(const uint64_t table_id, bool &exist));

  virtual int get_schema_guard(ObSchemaGetterGuard &guard,
                       int64_t schema_version = common::OB_INVALID_VERSION,
                        const bool force_fallback = false)
  {
    int ret = OB_SUCCESS;

    UNUSED(force_fallback);
    bool is_standby_cluster = false;
    if (OB_FAIL(guard.reset())) {
      SHARE_SCHEMA_LOG(WARN, "fail to reset guard", K(ret));
    } else if (OB_FAIL(guard.init(is_standby_cluster))) {
      SHARE_SCHEMA_LOG(WARN, "fail to init guard", K(ret));
    } else {
      ObArray<const ObSimpleTenantSchema *> tenant_schemas;
      ObSchemaMgrHandle handle(guard.mod_);
      const int64_t snapshot_version = common::OB_INVALID_VERSION == schema_version ? INT64_MAX : schema_version;
      const ObSchemaMgr *mgr = &mgr_;
      if (OB_FAIL(mgr_.get_tenant_schemas(tenant_schemas))) {
        SHARE_SCHEMA_LOG(WARN, "fail to get tenant schemas", KR(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_schemas.count(); i++) {
        const ObSimpleTenantSchema *&tenant = tenant_schemas.at(i);
        if (OB_NOT_NULL(tenant)) {
          ObRefreshSchemaStatus schema_status;
          const uint64_t tenant_id = tenant->get_tenant_id();
          schema_status.tenant_id_ = tenant_id;
          ObSchemaMgrInfo schema_mgr_info(tenant_id,
                                          snapshot_version,
                                          mgr,
                                          handle,
                                          schema_status);
          if (OB_FAIL(guard.schema_mgr_infos_.push_back(schema_mgr_info))) {
            SHARE_SCHEMA_LOG(WARN, "fail to push back schema mgr info", KR(ret), K(schema_mgr_info));
          }
        }
      }
      guard.schema_service_ = this;
      guard.schema_guard_type_ = ObSchemaGetterGuard::SCHEMA_GUARD;
    }
    return ret;
  }

  int add_tenant_schema(const ObTenantSchema &tenant_schema, int64_t schema_version)
  {
    int ret = OB_SUCCESS;
    ObSimpleTenantSchema simple_schema;
    if (OB_FAIL(convert_to_simple_schema(tenant_schema, simple_schema))) {
      SHARE_SCHEMA_LOG(WARN, "convert_to_simple_schema failed", K(ret));
    } else if (OB_FAIL(mgr_.add_tenant(simple_schema))) {
      SHARE_SCHEMA_LOG(WARN, "add user failed", K(ret));
    } else if (OB_FAIL(add_schema_to_cache(TENANT_SCHEMA, tenant_schema.get_tenant_id(),
                                           schema_version, tenant_schema))) {
      SHARE_SCHEMA_LOG(WARN, "add schema to cache failed, ret", K(ret));
    }
    return ret;
  }

  int add_sys_variable_schema(const ObSysVariableSchema &sys_variable, int64_t schema_version)
  {
    int ret = OB_SUCCESS;
    ObSimpleSysVariableSchema simple_schema;
    if (OB_FAIL(convert_to_simple_schema(sys_variable, simple_schema))) {
      SHARE_SCHEMA_LOG(WARN, "convert_to_simple_schema failed", K(ret));
    } else if (OB_FAIL(mgr_.sys_variable_mgr_.add_sys_variable(simple_schema))) {
      SHARE_SCHEMA_LOG(WARN, "add user failed", K(ret));
    } else if (OB_FAIL(add_schema_to_cache(SYS_VARIABLE_SCHEMA, simple_schema.get_tenant_id(),
                                           schema_version, sys_variable))) {
      SHARE_SCHEMA_LOG(WARN, "add schema to cache failed, ret", K(ret));
    }
    return ret;
  }

  int add_tablegroup_schema(const ObTablegroupSchema &tablegroup_schema, int64_t schema_version)
  {
    int ret = OB_SUCCESS;
    ObSimpleTablegroupSchema simple_schema;
    if (OB_FAIL(convert_to_simple_schema(tablegroup_schema, simple_schema))) {
      SHARE_SCHEMA_LOG(WARN, "convert_to_simple_schema failed", K(ret));
    } else if (OB_FAIL(mgr_.add_tablegroup(simple_schema))) {
      SHARE_SCHEMA_LOG(WARN, "add user failed", K(ret));
    } else if (OB_FAIL(add_schema_to_cache(TABLEGROUP_SCHEMA, tablegroup_schema.get_tablegroup_id(),
                                           schema_version, tablegroup_schema))) {
      SHARE_SCHEMA_LOG(WARN, "add schema to cache failed, ret", K(ret));
    }
    return ret;
  }

  int add_user_schema(const ObUserInfo &user_schema, int64_t schema_version)
  {
    int ret = OB_SUCCESS;

    ObSimpleUserSchema simple_schema;
    if (OB_FAIL(convert_to_simple_schema(user_schema, simple_schema))) {
      SHARE_SCHEMA_LOG(WARN, "convert_to_simple_schema failed", K(ret));
    } else if (OB_FAIL(mgr_.add_user(simple_schema))) {
      SHARE_SCHEMA_LOG(WARN, "add user failed", K(ret));
    } else if (OB_FAIL(add_schema_to_cache(USER_SCHEMA, user_schema.get_user_id(),
                                           schema_version, user_schema))) {
      SHARE_SCHEMA_LOG(WARN, "add schema to cache failed, ret", K(ret));
    }
    return ret;
  }

  int add_table_schema(const ObTableSchema &table_schema, int64_t schema_version)
  {
    int ret = OB_SUCCESS;

    ObSimpleTableSchemaV2 simple_schema;
    if (OB_FAIL(convert_to_simple_schema(table_schema, simple_schema))) {
      SHARE_SCHEMA_LOG(WARN, "convert_to_simple_schema failed", K(ret));
    } else if (OB_FAIL(mgr_.add_table(simple_schema))) {
      SHARE_SCHEMA_LOG(WARN, "add table failed", K(ret));
    } else if (OB_FAIL(add_schema_to_cache(TABLE_SCHEMA, table_schema.get_table_id(),
                                           schema_version, table_schema))) {
      SHARE_SCHEMA_LOG(WARN, "add schema to cache failed, ret", K(ret));
    }
    return ret;
  }

  int drop_table_schema(const uint64_t tenant_id, const uint64_t table_id)
  {
    int ret = OB_SUCCESS;

    if (OB_FAIL(mgr_.del_table(ObTenantTableId(tenant_id, table_id)))) {
      SHARE_SCHEMA_LOG(WARN, "delete table failed", K(ret));
    }
    return ret;
  }

  int add_database_schema(const ObDatabaseSchema &database_schema, int64_t schema_version)
  {
    int ret = OB_SUCCESS;

    ObSimpleDatabaseSchema simple_schema;
    if (OB_FAIL(convert_to_simple_schema(database_schema, simple_schema))) {
      SHARE_SCHEMA_LOG(WARN, "convert_to_simple_schema failed", K(ret));
    } else if (OB_FAIL(mgr_.add_database(simple_schema))) {
      SHARE_SCHEMA_LOG(WARN, "add database failed", K(ret));
    } else if (OB_FAIL(add_schema_to_cache(DATABASE_SCHEMA, database_schema.get_database_id(),
                                           schema_version, database_schema))) {
      SHARE_SCHEMA_LOG(WARN, "add schema to cache failed", K(ret));
    }
    return ret;
  }

  int add_outline_schema(const ObOutlineInfo &outline_schema, int64_t schema_version)
  {
    int ret = OB_SUCCESS;

    ObSimpleOutlineSchema simple_schema;
    if (OB_FAIL(convert_to_simple_schema(outline_schema, simple_schema))) {
      SHARE_SCHEMA_LOG(WARN, "convert_to_simple_schema failed", K(ret));
    } else if (OB_FAIL(mgr_.outline_mgr_.add_outline(simple_schema))) {
      SHARE_SCHEMA_LOG(WARN, "add outline failed", K(ret));
    } else if (OB_FAIL(add_schema_to_cache(OUTLINE_SCHEMA, outline_schema.get_outline_id(),
                                           schema_version, outline_schema))) {
      SHARE_SCHEMA_LOG(WARN, "add schema to cache failed", K(ret));
    }
    return ret;
  }

  int add_db_priv(const ObDBPriv &db_priv, int64_t schema_version)
  {
    UNUSED(schema_version);
    return mgr_.priv_mgr_.add_db_priv(db_priv);
  }

  int add_table_priv(const ObTablePriv &table_priv, int64_t schema_version)
  {
    UNUSED(schema_version);
    return mgr_.priv_mgr_.add_table_priv(table_priv);
  }

  int check_table_exist(const uint64_t table_id, bool &exist)
  {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    exist = false;
//    ObSchemaGetterGuard schema_guard;
    common::ObKVCacheHandle handle;
    const ObSchema *schema = NULL;
    const ObRefreshSchemaStatus schema_status;
    if (OB_SUCCESS
        != (tmp_ret = get_schema(&mgr_, schema_status, ObSchemaType::TABLE_SCHEMA, table_id, 1, handle, schema))) {
      if (OB_ENTRY_NOT_EXIST != tmp_ret) {
        ret = tmp_ret;
        STORAGE_LOG(WARN, "failed to get schema", K(ret), K(table_id));
      }
    } else {
      exist = true;
    }
    return ret;
  }

  int get_schema(const ObSchemaMgr *mgr,
                 const ObRefreshSchemaStatus &schema_status,
                 const ObSchemaType schema_type,
                 const uint64_t schema_id,
                 const int64_t schema_version,
                 ObKVCacheHandle &handle,
                 const ObSchema *&schema) override
  {
    int ret = OB_SUCCESS;
    UNUSED(handle);
    UNUSED(mgr);
    UNUSED(schema_status);
    schema = NULL;

    ObSchemaCacheKey cache_key(schema_type, OB_SYS_TENANT_ID, schema_id, schema_version);
    const ObSchemaCacheValue *cache_value = NULL;
    int hash_ret = cache_.get_refactored(cache_key, cache_value);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      ret = OB_ENTRY_NOT_EXIST;
      SHARE_SCHEMA_LOG(WARN, "schema item not exist",
                       "cache_size", cache_.size(),
                       K(schema_type),
                       K(schema_id),
                       K(schema_version),
                       K(ret));
    } else if (OB_SUCCESS == hash_ret) {
      // do-nothing
    } else {
      ret = hash_ret;
      SHARE_SCHEMA_LOG(WARN, "get value from sys cache failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(cache_value)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "NULL ptr", K(ret));
      } else {
        schema = cache_value->schema_;
      }
    }

    return ret;
  }
private:
  int convert_to_simple_schema(
      const ObTenantSchema &schema,
      ObSimpleTenantSchema &simple_schema)
  {
    int ret= OB_SUCCESS;

    simple_schema.set_tenant_id(schema.get_tenant_id());
    simple_schema.set_tenant_name(schema.get_tenant_name_str());
    simple_schema.set_schema_version(schema.get_schema_version());
    simple_schema.set_locality(schema.get_locality_str());

    return ret;
  }

  int convert_to_simple_schema(
      const ObSysVariableSchema &schema,
      ObSimpleSysVariableSchema &simple_schema)
  {
    int ret= OB_SUCCESS;

    simple_schema.set_tenant_id(schema.get_tenant_id());
    simple_schema.set_name_case_mode(schema.get_name_case_mode());
    simple_schema.set_schema_version(schema.get_schema_version());

    return ret;
  }

  int convert_to_simple_schema(
      const ObTablegroupSchema &schema,
      ObSimpleTablegroupSchema &simple_schema)
  {
    int ret= OB_SUCCESS;

    simple_schema.set_tenant_id(schema.get_tenant_id());
    simple_schema.set_tablegroup_id(schema.get_tablegroup_id());
    simple_schema.set_tablegroup_name(schema.get_tablegroup_name_str());
    simple_schema.set_schema_version(schema.get_schema_version());

    return ret;
  }

  int convert_to_simple_schema(
      const ObUserInfo &schema,
      ObSimpleUserSchema &simple_schema)
  {
    int ret= OB_SUCCESS;

    simple_schema.set_tenant_id(schema.get_tenant_id());
    simple_schema.set_user_id(schema.get_user_id());
    simple_schema.set_user_name(schema.get_user_name_str());
    simple_schema.set_host(schema.get_host_name_str());
    simple_schema.set_schema_version(schema.get_schema_version());

    return ret;
  }

  int convert_to_simple_schema(const ObTableSchema &schema,
                               ObSimpleTableSchemaV2 &simple_schema)
  {
    return ObServerSchemaService::convert_to_simple_schema(schema, simple_schema);
  }

  int convert_to_simple_schema(const ObDatabaseSchema &schema,
                               ObSimpleDatabaseSchema &simple_schema)
  {
    int ret= OB_SUCCESS;

    simple_schema.set_tenant_id(schema.get_tenant_id());
    simple_schema.set_database_id(schema.get_database_id());
    simple_schema.set_database_name(schema.get_database_name_str());
    // TODO: should fetch from tenant schema
    simple_schema.set_name_case_mode(OB_SYS_TENANT_ID == schema.get_tenant_id() ?
       OB_ORIGIN_AND_INSENSITIVE : OB_LOWERCASE_AND_INSENSITIVE);
    simple_schema.set_schema_version(schema.get_schema_version());

    return ret;
  }

  int convert_to_simple_schema(const ObOutlineInfo &schema,
                               ObSimpleOutlineSchema &simple_schema)
  {
    int ret= OB_SUCCESS;

    simple_schema.set_tenant_id(schema.get_tenant_id());
    simple_schema.set_outline_id(schema.get_outline_id());
    simple_schema.set_database_id(schema.get_database_id());
    simple_schema.set_name(schema.get_name_str());
    simple_schema.set_signature(schema.get_signature_str());
    simple_schema.set_schema_version(schema.get_schema_version());

    return ret;
  }

  int add_schema_to_cache(ObSchemaType schema_type,
                          uint64_t schema_id,
                          int64_t schema_version,
                          const ObSchema &schema)
  {
    int ret = OB_SUCCESS;

    ObSchemaCacheKey cache_key(schema_type, OB_SYS_TENANT_ID, schema_id, schema_version);
    ObSchema &tmp_schema = const_cast<ObSchema &>(schema);
    ObSchemaCacheValue tmp_cache_value(schema_type, &tmp_schema);
    int64_t deep_copy_size = tmp_cache_value.size();
    char *tmp_ptr = (char *)ob_malloc(deep_copy_size, "ScheCacSysCacVa");
    ObIKVCacheValue *kv_cache_value = NULL;
    if (NULL == tmp_ptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_SCHEMA_LOG(WARN, "alloc failed", K(ret));
    } else if (OB_FAIL(tmp_cache_value.deep_copy((tmp_ptr),
                                                 deep_copy_size,
                                                 kv_cache_value))) {
      SHARE_SCHEMA_LOG(WARN, "deep copy cache value failed", K(ret), K(tmp_ptr),
               K(deep_copy_size));
    } else if (OB_ISNULL(kv_cache_value)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "cache value is NULL", K(ret), K(kv_cache_value));
    } else {
      ObSchemaCacheValue *cache_value = static_cast<ObSchemaCacheValue *>(kv_cache_value);
      if (TABLE_SCHEMA == schema_type) {
        ObTableSchema &table_schema = dynamic_cast<ObTableSchema&>(const_cast<ObSchema&>(schema));
        int64_t size = table_schema.get_convert_size() + sizeof(common::ObDataBuffer);
        common::ObDataBuffer *databuf = new (tmp_ptr + sizeof(ObSchemaCacheValue) + sizeof(table_schema))
          common::ObDataBuffer(tmp_ptr + sizeof(ObSchemaCacheValue) + sizeof(table_schema) + sizeof(common::ObDataBuffer),
                               size - sizeof(table_schema) - sizeof(common::ObDataBuffer));
        new (const_cast<ObSchema *>(cache_value->schema_)) MockObTableSchema(databuf);
        if (OB_FAIL((*((ObTableSchema *)(cache_value->schema_))).assign(table_schema))) {
          SHARE_SCHEMA_LOG(WARN, "fail to assign schema", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        int overwrite_flag = 1;
        int hash_ret = cache_.set_refactored(cache_key, cache_value, overwrite_flag);
        if (OB_SUCCESS == hash_ret) {
          // do-nothing
        } else {
          ret = hash_ret;
          SHARE_SCHEMA_LOG(WARN, "put value to cache failed", K(ret), K(hash_ret),
              K(cache_key), K(*cache_value));
        }
      }
    }

    return ret;
  }
private:
  ObSchemaMgr mgr_;
  NoSwapCache cache_;
};

} // schema
} // share
} // oceanbase

#endif /* MOCK_SCHEMA_SERVICE_H_ */
