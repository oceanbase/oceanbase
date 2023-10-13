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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "rootserver/parallel_ddl/ob_index_name_checker.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_multi_version_schema_service.h"
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

ObIndexNameCache::ObIndexNameCache(
  const uint64_t tenant_id,
  common::ObMySQLProxy &sql_proxy)
  : mutex_(common::ObLatchIds::IND_NAME_CACHE_LOCK),
    tenant_id_(tenant_id),
    sql_proxy_(sql_proxy),
    allocator_(ObMemAttr(OB_SYS_TENANT_ID, "IndNameInfo", ObCtxIds::SCHEMA_SERVICE)),
    cache_(ModulePageAllocator(allocator_)),
    loaded_(false)
{
}

void ObIndexNameCache::reset_cache()
{
  lib::ObMutexGuard guard(mutex_);
  (void) inner_reset_cache_();
}

void ObIndexNameCache::inner_reset_cache_()
{
  cache_.destroy();
  allocator_.reset();
  loaded_ = false;
  FLOG_INFO("[INDEX NAME CACHE] reset index name map", K_(tenant_id));
}

int ObIndexNameCache::check_index_name_exist(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &index_name,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  bool is_oracle_mode = false;
  if (OB_UNLIKELY(
      OB_INVALID_TENANT_ID == tenant_id
      || tenant_id_ != tenant_id
      || OB_INVALID_ID == database_id
      || index_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id_), K(tenant_id), K(database_id), K(index_name));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
             tenant_id, is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode", KR(ret), K(tenant_id));
  } else {
    lib::ObMutexGuard guard(mutex_);
    ObString idx_name;
    uint64_t data_table_id = OB_INVALID_ID;
    if (OB_FAIL(try_load_cache_())) {
      LOG_WARN("fail to load index name cache", KR(ret), K(tenant_id));
    } else if (is_recyclebin_database_id(database_id)) {
      idx_name = index_name;
      data_table_id = OB_INVALID_ID;
    } else {
      uint64_t data_table_id = ObSimpleTableSchemaV2::extract_data_table_id_from_index_name(index_name);
      if (OB_INVALID_ID == database_id) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid index name", KR(ret), K(index_name));
      } else if (OB_FAIL(ObSimpleTableSchemaV2::get_index_name(index_name, idx_name))) {
        LOG_WARN("fail to get original index name", KR(ret), K(index_name));
      } else {
        data_table_id = (is_oracle_mode && !is_mysql_sys_database_id(database_id)) ?
                        OB_INVALID_ID : data_table_id;
      }
    }
    if (OB_SUCC(ret)) {
      ObIndexSchemaHashWrapper index_name_wrapper(
                               tenant_id,
                               database_id,
                               data_table_id,
                               idx_name);
      ObIndexNameInfo *index_name_info = NULL;
      if (OB_FAIL(cache_.get_refactored(index_name_wrapper, index_name_info))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get index name info", KR(ret), K(index_name_wrapper));
        }
      } else if (OB_ISNULL(index_name_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index name info is null", KR(ret), K(index_name_wrapper));
      } else {
        is_exist = true;
        LOG_INFO("index name exist", KR(ret), KPC(index_name_info),
                 K(database_id), K(index_name), K(data_table_id), K(idx_name));
        // Before call check_index_name_exist(), index_name will be locked by trans first.
        // And add_index_name() will be called before trans commit.
        //
        // It may has garbage when trans commit failed after add_index_name() is called.
        // So, we need to double check if index name actually exists in inner table when confict occurs.
        ObSchemaService *schema_service_impl = NULL;
        uint64_t index_id = OB_INVALID_ID;
        if (OB_ISNULL(schema_service_impl = GSCHEMASERVICE.get_schema_service())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema service impl is null", KR(ret));
        } else if (OB_FAIL(schema_service_impl->get_index_id(
                   sql_proxy_, tenant_id, database_id,
                   index_name_info->get_index_name(), index_id))) {
          LOG_WARN("fail to get index id", KR(ret), KPC(index_name_info));
        } else if (OB_INVALID_ID != index_id) {
          is_exist = true;
        } else {
          is_exist = false;
          FLOG_INFO("garbage index name exist, should be erased", KPC(index_name_info),
                    K(database_id), K(index_name), K(data_table_id), K(idx_name));
          if (OB_FAIL(cache_.erase_refactored(index_name_wrapper))) {
            LOG_WARN("fail to erase key", KR(ret), K(index_name_wrapper));
            if (OB_HASH_NOT_EXIST != ret) {
              (void) inner_reset_cache_();
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObIndexNameCache::add_index_name(
    const share::schema::ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = index_schema.get_tenant_id();
  const uint64_t database_id = index_schema.get_database_id();
  const ObString &index_name = index_schema.get_table_name_str();
  const ObTableType table_type = index_schema.get_table_type();
  uint64_t data_table_id = index_schema.get_data_table_id();
  bool is_oracle_mode = false;
  if (OB_UNLIKELY(
      OB_INVALID_TENANT_ID == tenant_id
      || tenant_id_ != tenant_id
      || OB_INVALID_ID == database_id
      || index_name.empty()
      || !is_index_table(table_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id_),
             K(tenant_id), K(database_id), K(index_name), K(table_type));
  } else if (OB_UNLIKELY(!is_recyclebin_database_id(database_id)
             && index_schema.get_origin_index_name_str().empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index schema", KR(ret), K(index_schema));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
             tenant_id, is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode", KR(ret), K(tenant_id));
  } else {
    lib::ObMutexGuard guard(mutex_);
    if (OB_FAIL(try_load_cache_())) {
      LOG_WARN("fail to load index name cache", KR(ret), K(tenant_id));
    } else {
      void *buf = NULL;
      ObIndexNameInfo *index_name_info = NULL;
      ObString idx_name;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObIndexNameInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc index name info", KR(ret));
      } else if (FALSE_IT(index_name_info = new (buf) ObIndexNameInfo())) {
      } else if (OB_FAIL(index_name_info->init(allocator_, index_schema))) {
        LOG_WARN("fail to init index name info", KR(ret), K(index_schema));
      } else if (is_recyclebin_database_id(database_id)) {
        data_table_id = OB_INVALID_ID;
        idx_name = index_name_info->get_index_name();
      } else {
        data_table_id = (is_oracle_mode && !is_mysql_sys_database_id(database_id)) ?
                        OB_INVALID_ID : index_name_info->get_data_table_id();
        idx_name = index_name_info->get_original_index_name();
      }
      if (OB_SUCC(ret)) {
        int overwrite = 0;
        ObIndexSchemaHashWrapper index_name_wrapper(index_name_info->get_tenant_id(),
                                                    database_id,
                                                    data_table_id,
                                                    idx_name);
        if (OB_FAIL(cache_.set_refactored(index_name_wrapper, index_name_info, overwrite))) {
          LOG_WARN("fail to set refactored", KR(ret), KPC(index_name_info));
          if (OB_HASH_EXIST == ret) {
            ObIndexNameInfo **exist_index_info = cache_.get(index_name_wrapper);
            if (OB_NOT_NULL(exist_index_info) && OB_NOT_NULL(*exist_index_info)) {
              FLOG_ERROR("[INDEX NAME CACHE] duplicated index info exist",
                         KR(ret), KPC(index_name_info), KPC(*exist_index_info));
            }
          } else {
            (void) inner_reset_cache_();
          }
        } else {
          FLOG_INFO("[INDEX NAME CACHE] add index name to cache", KR(ret), KPC(index_name_info));
        }
      }
    }
  }
  return ret;
}

// need protect by mutex_
int ObIndexNameCache::try_load_cache_()
{
  int ret = OB_SUCCESS;
  if (loaded_) {
    // do nothing
  } else {
    (void) inner_reset_cache_();

    ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = tenant_id_;
    int64_t schema_version = OB_INVALID_VERSION;
    int64_t timeout_ts = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(GSCHEMASERVICE.get_schema_version_in_inner_table(
        sql_proxy_, schema_status, schema_version))) {
      LOG_WARN("fail to get schema version", KR(ret), K(schema_status));
    } else if (!ObSchemaService::is_formal_version(schema_version)) {
      ret = OB_EAGAIN;
      LOG_WARN("schema version is informal, need retry", KR(ret), K(schema_status), K(schema_version));
    } else if (OB_FAIL(ObShareUtil::get_ctx_timeout(GCONF.internal_sql_execute_timeout, timeout_ts))) {
      LOG_WARN("fail to get timeout", KR(ret));
    } else {
      int64_t original_timeout_ts = THIS_WORKER.get_timeout_ts();
      int64_t schema_version = OB_INVALID_VERSION;
      THIS_WORKER.set_timeout_ts(timeout_ts);

      ObSchemaGetterGuard guard;
      int64_t start_time = ObTimeUtility::current_time();
      if (OB_FAIL(GSCHEMASERVICE.async_refresh_schema(tenant_id_, schema_version))) {
        LOG_WARN("fail to refresh schema", KR(ret), K_(tenant_id), K(schema_version));
      } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(tenant_id_, guard))) {
        LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(guard.get_schema_version(tenant_id_, schema_version))) {
        LOG_WARN("fail to get schema version", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(guard.deep_copy_index_name_map(allocator_, cache_))) {
        LOG_WARN("fail to deep copy index name map", KR(ret), K_(tenant_id));
      } else {
        loaded_ = true;
        FLOG_INFO("[INDEX NAME CACHE] load index name map", KR(ret), K_(tenant_id),
                  K(schema_version), "cost", ObTimeUtility::current_time() - start_time);
      }

      if (OB_FAIL(ret)) {
        (void) inner_reset_cache_();
        LOG_WARN("load index name map failed", KR(ret), K_(tenant_id),
                 K(schema_version), "cost", ObTimeUtility::current_time() - start_time);
      }

      THIS_WORKER.set_timeout_ts(original_timeout_ts);
    }
  }
  return ret;
}

ObIndexNameChecker::ObIndexNameChecker()
  : rwlock_(),
    allocator_(ObMemAttr(OB_SYS_TENANT_ID, "IndNameCache", ObCtxIds::SCHEMA_SERVICE)),
    index_name_cache_map_(),
    sql_proxy_(NULL),
    inited_(false)
{
}

ObIndexNameChecker::~ObIndexNameChecker()
{
  destroy();
}

int ObIndexNameChecker::init(common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    const int64_t BUCKET_NUM = 1024;
    if (OB_FAIL(index_name_cache_map_.create(BUCKET_NUM, "IndNameMap", "IndNameMap"))) {
      LOG_WARN("fail to create hash map", KR(ret));
    } else {
      sql_proxy_ = &sql_proxy;
      inited_ = true;
    }
  }
  return ret;
}

void ObIndexNameChecker::destroy()
{
  SpinWLockGuard guard(rwlock_);
  if (inited_) {
    FOREACH(it, index_name_cache_map_) {
      if (OB_NOT_NULL(it->second)) {
        (it->second)->~ObIndexNameCache();
        it->second = NULL;
      }
    }
    index_name_cache_map_.destroy();
    allocator_.reset();
    sql_proxy_ = NULL;
    inited_ = false;
  }
}

void ObIndexNameChecker::reset_all_cache()
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (inited_) {
    FOREACH(it, index_name_cache_map_) {
      if (OB_NOT_NULL(it->second)) {
        (void) (it->second)->reset_cache();
      }
    }
  }
}

int ObIndexNameChecker::reset_cache(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ObIndexNameCache *cache = NULL;
    if (OB_FAIL(index_name_cache_map_.get_refactored(tenant_id, cache))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get refactored", KR(ret), K(tenant_id));
      } else {
        // tenant not in cache, just skip
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cache is null", KR(ret), K(tenant_id));
    } else {
      (void) cache->reset_cache();
    }
  }
  return ret;
}

int ObIndexNameChecker::check_index_name_exist(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const ObString &index_name,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  bool can_skip = false;
  is_exist = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_tenant_can_be_skipped_(tenant_id, can_skip))) {
    LOG_WARN("fail to check tenant", KR(ret), K(tenant_id));
  } else if (can_skip) {
    // do nothing
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
             || OB_INVALID_ID == database_id
             || index_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(database_id), K(index_name));
  } else if (OB_FAIL(try_init_index_name_cache_map_(tenant_id))) {
    LOG_WARN("fail to init index name cache", KR(ret), K(tenant_id));
  } else {
    SpinRLockGuard guard(rwlock_);
    ObIndexNameCache *cache = NULL;
    if (OB_FAIL(index_name_cache_map_.get_refactored(tenant_id, cache))) {
      LOG_WARN("fail to get refactored", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cache is null", KR(ret));
    } else if (OB_FAIL(cache->check_index_name_exist(
      tenant_id, database_id, index_name, is_exist))) {
      LOG_WARN("fail to check index name exist",
               KR(ret), K(tenant_id), K(database_id), K(index_name));
    }
  }
  return ret;
}

int ObIndexNameChecker::add_index_name(
    const share::schema::ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = index_schema.get_tenant_id();
  bool can_skip = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_tenant_can_be_skipped_(tenant_id, can_skip))) {
    LOG_WARN("fail to check tenant", KR(ret), K(tenant_id));
  } else if (can_skip) {
    // do nothing
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id));
  } else if (OB_FAIL(try_init_index_name_cache_map_(tenant_id))) {
    LOG_WARN("fail to init index name cache", KR(ret), K(tenant_id));
  } else {
    SpinRLockGuard guard(rwlock_);
    ObIndexNameCache *cache = NULL;
    if (OB_FAIL(index_name_cache_map_.get_refactored(tenant_id, cache))) {
      LOG_WARN("fail to get refactored", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cache is null", KR(ret));
    } else if (OB_FAIL(cache->add_index_name(index_schema))) {
      LOG_WARN("fail to add index name", KR(ret), K(index_schema));
    }
  }
  return ret;
}

// only cache oracle tenant's index name map
int ObIndexNameChecker::check_tenant_can_be_skipped_(
    const uint64_t tenant_id,
    bool &can_skip)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  can_skip = false;
  if (is_sys_tenant(tenant_id)
      || is_meta_tenant(tenant_id)) {
    can_skip = true;
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
             tenant_id, is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode", KR(ret), K(tenant_id));
  } else {
    can_skip = !is_oracle_mode;
  }
  return ret;
}

int ObIndexNameChecker::try_init_index_name_cache_map_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else {
    ObIndexNameCache *cache = NULL;
    if (OB_FAIL(index_name_cache_map_.get_refactored(tenant_id, cache))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get cache", KR(ret), K(tenant_id));
      } else {
        ret = OB_SUCCESS;
        cache = NULL;
        void *buf = NULL;
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObIndexNameCache)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret));
        } else if (FALSE_IT(cache = new (buf) ObIndexNameCache(tenant_id, *sql_proxy_))) {
        } else if (OB_FAIL(index_name_cache_map_.set_refactored(tenant_id, cache))) {
          LOG_WARN("fail to set cache", KR(ret), K(tenant_id));
        }
      }
    } else {
      // cache exist, just skip
    }
  }
  return ret;
}
