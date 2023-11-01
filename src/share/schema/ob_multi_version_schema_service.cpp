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
#include "share/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/time/ob_time_utility.h"
#include "lib/container/ob_vector.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/utility/ob_print_utils.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/rc/ob_context.h"
#include "share/ob_schema_status_proxy.h"
#include "share/ob_global_stat_proxy.h"
// for materialized view
#include "sql/parser/ob_parser.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/resolver/dml/ob_view_table_resolver.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server.h"
#include "share/schema/ob_outline_mgr.h"
#include "share/schema/ob_udt_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace common::hash;
using namespace oceanbase::sql;
using namespace oceanbase::observer;

namespace share
{
namespace schema
{

bool ObMultiVersionSchemaService::g_skip_resolve_materialized_view_definition_ = false;

const char *ObMultiVersionSchemaService::print_refresh_schema_mode(const RefreshSchemaMode mode)
{
  const char *mode_str= "UNKNOWN";

  switch (mode) {
    case NORMAL: {
      mode_str = "normal";
      break;
    }
    case FORCE_FALLBACK: {
      mode_str = "force_fallback";
      break;
    }
    case FORCE_LAZY: {
      mode_str = "force_lazy";
      break;
    }
    default: {
      mode_str = "UNKNOWN";
      break;
    }
  }

  return mode_str;
}


///////////////////////////////////////////////////////

#define dbg_construct_task 0

ObSchemaConstructTask::ObSchemaConstructTask()
{
  schema_tasks_.set_attr(SET_USE_500(ObMemAttr(OB_SERVER_TENANT_ID, "SchemaTasks", ObCtxIds::SCHEMA_SERVICE)));
  (void)pthread_mutex_init(&schema_mutex_, NULL);
  (void)pthread_cond_init(&schema_cond_, NULL);
}

ObSchemaConstructTask::~ObSchemaConstructTask()
{
  (void)pthread_mutex_destroy(&schema_mutex_);
  (void)pthread_cond_destroy(&schema_cond_);
}

ObSchemaConstructTask& ObSchemaConstructTask::get_instance()
{
  static ObSchemaConstructTask task;
  return task;
}

// FIXME: After the schema is split, the schema_version between tenants may be repeated,
// but the probability is relatively small, and it can be changed to restrict concurrency by (tenant_id, version) later
// blocked if existing same version, or over max parallel size
void ObSchemaConstructTask::cc_before(const int64_t version)
{
  lock();
  if (count() == 0) {
    // leader
  } else {
    do {
      if (exist(version)) {
        wait(version);
      } else {
        break;
      }
    } while (true);
  }

  do {
    if (count() > MAX_PARALLEL_TASK) {
      wait(version);
    } else {
      add(version);
      unlock();
      break;
    }
  } while (true);
}

// must called after cc_before
void ObSchemaConstructTask::cc_after(const int64_t version)
{
  lock();
  remove(version);
  wakeup(version);
  unlock();
}

void ObSchemaConstructTask::lock()
{
  (void)pthread_mutex_lock(&schema_mutex_);
}

void ObSchemaConstructTask::unlock()
{
  (void)pthread_mutex_unlock(&schema_mutex_);
}

void ObSchemaConstructTask::wait(const int64_t version)
{
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_sec += 1;
  if (dbg_construct_task) {
    LOG_WARN_RET(OB_SUCCESS, "task: waiting", K(version), K(count()));
  }
  int rc = 0;
  do {
    rc = ob_pthread_cond_timedwait(&schema_cond_, &schema_mutex_, &ts);
  } while (0);
  (void) rc; // make compiler happy
}

void ObSchemaConstructTask::wakeup(const int64_t version)
{
  if (dbg_construct_task) {
    LOG_WARN_RET(OB_SUCCESS, "task: wakingup", K(version), K(count()));
  }
  (void)pthread_cond_broadcast(&schema_cond_);
}

int ObSchemaConstructTask::get_idx(int64_t id)
{
  int hit_idx = -1;
  for (int i = 0; i < schema_tasks_.count(); i++) {
    if (id == schema_tasks_.at(i)) {
      hit_idx = i;
      break;
    }
  }
  return hit_idx;
}

// must protected by mutex
void ObSchemaConstructTask::add(int64_t id)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = schema_tasks_.push_back(id))) {
    LOG_WARN("failed to push back task", K(id), K(ret));
  }

  if (dbg_construct_task) {
    LOG_WARN("task: add ", K(id), K(count()));
  }
}

// must protected by mutex
void ObSchemaConstructTask::remove(int64_t id)
{
  int ret = OB_SUCCESS;
  int idx = get_idx(id);
  if (idx != -1) {
    if (OB_SUCCESS != (ret = schema_tasks_.remove(idx))) {
      LOG_WARN("failed to remove task", K(id), K(ret));
    }
  } else {
    LOG_WARN("failed to get task idx", K(id));
  }

  if (dbg_construct_task) {
    LOG_WARN("task: remove", K(id), K(count()));
  }
}

int ObMultiVersionSchemaService::init_multi_version_schema_struct(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const ObSchemaStore *schema_store = NULL;
  if (OB_ISNULL(schema_store = schema_store_map_.get(tenant_id))) {
    if (OB_FAIL(schema_store_map_.create(tenant_id, init_version_cnt_, init_version_cnt_for_liboblog_))) {
      LOG_WARN("fail to create schema store", K(ret), K(tenant_id));
    }
  } else {
    LOG_INFO("schema store already exist", K(ret), K(tenant_id));
  }
  return ret;
}

int ObMultiVersionSchemaService::update_schema_cache(
    common::ObIArray<ObTableSchema*> &schema_array)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("update schema cache", K(lbt()));
  for (int64_t i = 0; OB_SUCC(ret) && i < schema_array.count(); ++i) {
    ObTableSchema *table = schema_array.at(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema is null", KR(ret));
    } else if (OB_FAIL(ObSysTableChecker::fill_sys_index_infos(*table))) {
      LOG_WARN("fail to fill sys indexes", KR(ret), "table_id", table->get_table_id());
    } else if (OB_FAIL(schema_cache_.put_schema(TABLE_SCHEMA,
                                                table->get_tenant_id(),
                                                table->get_table_id(),
                                                table->get_schema_version(),
                                                *table))) {
      LOG_WARN("put schema failed", KR(ret), "table_id", table->get_table_id());
    } else {
      LOG_INFO("put schema succeed", K(*table));
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::update_schema_cache(
    common::ObIArray<ObTableSchema> &schema_array)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("update schema cache", K(lbt()));
  for (int64_t i = 0; OB_SUCC(ret) && i < schema_array.count(); ++i) {
    ObTableSchema &table = schema_array.at(i);
    if (OB_FAIL(ObSysTableChecker::fill_sys_index_infos(table))) {
      LOG_WARN("fail to fill sys indexes", KR(ret), "table_id", table.get_table_id());
    } else if (OB_FAIL(schema_cache_.put_schema(TABLE_SCHEMA,
                                                table.get_tenant_id(),
                                                table.get_table_id(),
                                                table.get_schema_version(),
                                                table))) {
      LOG_WARN("put schema failed", KR(ret), "table_id", table.get_table_id());
    } else {
      LOG_INFO("put schema succeed", K(table));
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::update_schema_cache(
    const common::ObIArray<ObTenantSchema> &schema_array)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("update schema cache", K(lbt()));
  for (int64_t i = 0; OB_SUCC(ret) && i < schema_array.count(); ++i) {
    const ObTenantSchema &tenant = schema_array.at(i);
    if (OB_FAIL(schema_cache_.put_schema(TENANT_SCHEMA,
                                         OB_SYS_TENANT_ID,
                                         tenant.get_tenant_id(),
                                         tenant.get_schema_version(),
                                         tenant))) {
      LOG_WARN("put schema failed", K(ret));
    } else {
      LOG_INFO("put schema succeed", K(tenant));
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::update_schema_cache(
    const share::schema::ObSysVariableSchema &schema)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("update schema cache", K(lbt()));
  if (OB_FAIL(schema_cache_.put_schema(SYS_VARIABLE_SCHEMA,
                                       schema.get_tenant_id(),
                                       schema.get_tenant_id(),
                                       schema.get_schema_version(),
                                       schema))) {
    LOG_WARN("put schema failed", K(ret));
  } else {
    LOG_INFO("put schema succeed", K(schema));
  }
  return ret;
}

// for materialized view, construct the 'full schema' with 'column generated rules'
int ObMultiVersionSchemaService::build_full_materalized_view_schema(
    ObSchemaGetterGuard &schema_guard,
    ObIAllocator &allocator,
    ObTableSchema *&view_schema)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = view_schema->get_tenant_id();
  const ObTenantSchema *tenant_schema = NULL;
  if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(tenant_id), K(ret));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(tenant_schema), K(ret));
  } else {
    ObParser parser(allocator, DEFAULT_MYSQL_MODE);
    ParseResult view_result;
    // parse view define
    const ObString &view_def = view_schema->get_view_schema().get_view_definition();
    if (OB_FAIL(parser.parse(view_def, view_result))) {
      LOG_WARN("parse view defination failed", K(view_def), K(ret));
    } else {
      // resolve params
      ObResolverParams resolver_ctx;
      ObSchemaChecker schema_checker;
      ObStmtFactory stmt_factory(allocator);
      ObRawExprFactory expr_factory(allocator);
      SMART_VAR(ObSQLSessionInfo, default_session) {
        if (OB_FAIL(schema_checker.init(schema_guard))) {
          LOG_WARN("fail to init schema_checker", K(ret));
        } else if (OB_FAIL(default_session.init(0, 0, &allocator))) {
          LOG_WARN("init empty session failed", K(ret));
        } else if (OB_FAIL(default_session.load_default_sys_variable(false, false))) {
          LOG_WARN("session load default system variable failed", K(ret));
        } else if (OB_FAIL(default_session.init_tenant(tenant_schema->get_tenant_name(), tenant_id))) {
          LOG_WARN("fail to set tenant", "tenant", tenant_schema->get_tenant_name(),
                   "id", tenant_id, K(ret));
        } else {
          resolver_ctx.allocator_ = &allocator;
          resolver_ctx.schema_checker_ = &schema_checker;
          resolver_ctx.session_info_ = &default_session;
          resolver_ctx.stmt_factory_ = &stmt_factory;
          resolver_ctx.expr_factory_ = &expr_factory;
          resolver_ctx.query_ctx_ = stmt_factory.get_query_ctx();
          ObSelectResolver view_resolver(resolver_ctx);

          ParseNode *view_stmt_node = view_result.result_tree_->children_[0];
          if (!view_stmt_node) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected children for view result after parse", K(ret));
          } else if (OB_FAIL(view_resolver.resolve(*view_stmt_node))) {
            LOG_WARN("resolve view definition failed", K(ret));
          } else { /*do nothing*/ }
        }
      }
    }
  }

  return ret;
}

// for ObLatestSchemaGuard
int ObMultiVersionSchemaService::get_latest_schema(
    common::ObIAllocator &allocator,
    const ObSchemaType schema_type,
    const uint64_t tenant_id,
    const uint64_t schema_id,
    const ObSchema *&schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(TENANT_SCHEMA == schema_type && !is_sys_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id for TENANT_SCHEMA", KR(ret), K(tenant_id), K(schema_id));
  } else if (OB_UNLIKELY(SYS_VARIABLE_SCHEMA == schema_type && tenant_id != schema_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and schema_id not match for TENANT_SCHEMA",
             KR(ret), K(tenant_id), K(schema_id));
  } else if (OB_UNLIKELY(!is_normal_schema(schema_type)
             || OB_INVALID_ID == schema_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(schema_type), K(tenant_id), K(schema_id));
  } else if ((TABLE_SCHEMA == schema_type
              || TABLE_SIMPLE_SCHEMA == schema_type)
             && OB_ALL_CORE_TABLE_TID == schema_id) {
    const ObTableSchema *hard_code_schema = schema_cache_.get_all_core_table();
    if (OB_ISNULL(hard_code_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("all core table schema is null", KR(ret));
    } else if (is_sys_tenant(tenant_id)) {
      schema = hard_code_schema;
    } else {
      ObTableSchema* new_table = NULL;
      if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator, new_table))) {
        LOG_WARN("fail to alloc table schema", KR(ret));
      } else if (OB_FAIL(new_table->assign(*hard_code_schema))) {
        LOG_WARN("fail to assign all core schema", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(
                tenant_id, *new_table))) {
        LOG_WARN("fail to construct tenant's __all_core_table schema", KR(ret), K(tenant_id));
      } else {
        schema = static_cast<const ObSchema*>(new_table);
      }
    }
  } else {
    ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = tenant_id;
    const int64_t schema_version = INT64_MAX;
    ObSchema *new_schema = NULL;
    if (OB_FAIL(schema_fetcher_.fetch_schema(schema_type,
                                             schema_status,
                                             schema_id,
                                             schema_version,
                                             allocator,
                                             new_schema))) {
      LOG_WARN("fail to fetch schema", KR(ret), K(schema_type),
               K(tenant_id), K(schema_id), K(schema_version));
    } else if (OB_ISNULL(new_schema)) {
      // schema not exist or schema history is recycled.
    } else if (TABLE_SCHEMA != schema_type) {
      schema = new_schema;
    } else {
      ObTableSchema *new_table = static_cast<ObTableSchema *>(new_schema);
      if (MATERIALIZED_VIEW == new_table->get_table_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support to fetch latest mv", KR(ret), "table_id", schema_id);
      } else if (OB_ALL_CORE_TABLE_TID == schema_id) {
        // do-nothing
      } else if (!need_construct_aux_infos_(*new_table)) {
        // do-nothing
      } else if (ObSysTableChecker::is_sys_table_has_index(schema_id)) {
        if (OB_FAIL(ObSysTableChecker::fill_sys_index_infos(*new_table))) {
          LOG_WARN("fail to fill sys indexes", KR(ret), K(tenant_id), "table_id", schema_id);
        }
      } else if (OB_FAIL(construct_aux_infos_(*sql_proxy_,
                 schema_status, tenant_id, *new_table))) {
        LOG_WARN("fail to construct aux infos", KR(ret), K(tenant_id), "table_id", schema_id);
      }
      if (OB_SUCC(ret)) {
        schema = static_cast<const ObSchema*>(new_table);
      }
    }
  }
  return ret;
}

// In order to facilitate the processing of the tenant_space table and index, the logic of updating the schema_cache
// is placed in the upper layer, and at the same time, the schema_guard is passed in to obtain the tenant schema
// Whether it is lazy mode is distinguished by whether mgr is NULL
int ObMultiVersionSchemaService::get_schema(const ObSchemaMgr *mgr,
                                            const ObRefreshSchemaStatus &schema_status,
                                            const ObSchemaType schema_type,
                                            const uint64_t schema_id,
                                            const int64_t schema_version,
                                            ObKVCacheHandle &handle,
                                            const ObSchema *&schema)
{
  int ret = OB_SUCCESS;
  const bool is_lazy = (NULL == mgr);
  uint64_t tenant_id = schema_status.tenant_id_;
  bool update_history_cache = false;
  schema = NULL;
  if (TENANT_SCHEMA == schema_type && !is_sys_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id for TENANT_SCHEMA", KR(ret), K(tenant_id), K(schema_id));
  } else if (SYS_VARIABLE_SCHEMA == schema_type && tenant_id != schema_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id and schema_id not match for TENANT_SCHEMA",
             KR(ret), K(tenant_id), K(schema_id));
  } else if (TABLE_SIMPLE_SCHEMA == schema_type && !ObSchemaService::g_liboblog_mode_) {
    // The simple table schema is currently only available for liboblog
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("fail to get simple table", K(ret),
             KP(mgr), K(tenant_id), K(schema_id), K(schema_version));
  } else if ((TABLE_SCHEMA == schema_type || TABLE_SIMPLE_SCHEMA == schema_type)
             && OB_ALL_CORE_TABLE_TID == schema_id) {
    const ObTableSchema *hard_code_schema = schema_cache_.get_all_core_table();
    if (OB_ISNULL(hard_code_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("all core table schema is null", KR(ret));
    } else if (is_sys_tenant(tenant_id)) {
      schema = hard_code_schema;
    } else {
      ObTableSchema new_schema;
      if (OB_FAIL(schema_cache_.get_schema(TABLE_SCHEMA,
                                           tenant_id,
                                           schema_id,
                                           OB_CORE_SCHEMA_VERSION ,
                                           handle,
                                           schema))) {
        if (ret != OB_ENTRY_NOT_EXIST) {
          LOG_WARN("get schema from cache failed", KR(ret), K(tenant_id),
                   K(schema_type), K(schema_id), K(schema_version));
        } else if (OB_FAIL(new_schema.assign(*hard_code_schema))) { // overwrite ret
          LOG_WARN("fail to assign all core schema", KR(ret), K(tenant_id));
        } else if (OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(
                  tenant_id, new_schema))) {
          LOG_WARN("fail to construct tenant's __all_core_table schema", KR(ret), K(tenant_id));
        } else if (OB_FAIL(schema_cache_.put_and_fetch_schema(schema_type,
                                                              tenant_id,
                                                              schema_id,
                                                              OB_CORE_SCHEMA_VERSION,
                                                              new_schema,
                                                              handle,
                                                              schema))) {
          LOG_WARN("fail to push back tenant's __all_core_table schema", KR(ret), K(tenant_id));
        }
      }
    }
  } else if (TENANT_SCHEMA == schema_type && OB_GTS_TENANT_ID == schema_id) {
    schema = schema_cache_.get_full_gts_tenant();
  } else if (OB_FAIL(schema_cache_.get_schema(schema_type,
                                              tenant_id,
                                              schema_id,
                                              schema_version,
                                              handle,
                                              schema))) {
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("get schema from cache failed", K(tenant_id), K(schema_type), K(schema_id),
               K(schema_version), K(ret));
    } else {
      // fetch schema and renew cache
      ret = OB_SUCCESS;
      LOG_TRACE("schema cache miss", K(tenant_id), K(schema_type), K(schema_id), K(schema_version));

      ObSchema *tmp_schema = NULL;
      ObArenaAllocator allocator(ObModIds::OB_TEMP_VARIABLES);
      bool has_hit = false;
      // Use this to mark whether the schema exists in the specified version
      bool not_exist = false;

      // 1. Query the version history dictionary table
      if (OB_FAIL(ret)) {
      } else if (is_lazy
                 && (TENANT_SCHEMA == schema_type
                     || TABLE_SCHEMA == schema_type
                     || TABLE_SIMPLE_SCHEMA == schema_type
                     || TABLEGROUP_SCHEMA == schema_type
                     || DATABASE_SCHEMA == schema_type)) {
        ObSchemaType fetch_schema_type = TABLE_SIMPLE_SCHEMA == schema_type ? TABLE_SCHEMA : schema_type;
        VersionHisKey key(fetch_schema_type, tenant_id, schema_id);
        VersionHisVal val;
        if (OB_FAIL(get_schema_version_history(schema_status, tenant_id, schema_version,
                                               key, val, not_exist))) {
          LOG_WARN("fail to get schema version history", K(ret), K(schema_type),
                   K(tenant_id),  K(schema_id), K(key), K(schema_version));
        }
        if (OB_SUCC(ret) && !not_exist) {
          int i = 0;
          int64_t precise_version = OB_INVALID_VERSION;
          for (; i < val.valid_cnt_; ++i) {
            if (val.versions_[i] <= schema_version) {
              break;
            }
          }
          if (i < val.valid_cnt_) {
            if (0 == i && val.is_deleted_) {
              not_exist = true;
              LOG_INFO("schema has been deleted under specified version", KR(ret),
                       K(key), K(val), K(schema_version), K(precise_version));
            } else {
              // Access cache with accurate version
              precise_version = val.versions_[i];
            }
          } else if (schema_version < val.min_version_) {
            not_exist = true;
            LOG_INFO("schema has not been created under specified version",
                     KR(ret), K(key), K(val), K(schema_version));
          } else if (schema_version == val.min_version_) {
            precise_version = val.min_version_;
            LOG_INFO("use min schema version as precise schema version",
                     KR(ret), K(key), K(val), K(schema_version));
          } else {
            // i >= cnt && schema_version > val.min_version_
            // try use discrete schema version relationship
            if (OB_FAIL(schema_cache_.get_schema_history_cache(
                schema_type, tenant_id, schema_id, schema_version, precise_version))) {
              if (OB_ENTRY_NOT_EXIST != ret) {
                LOG_WARN("get schema history cache failed",
                         KR(ret), K(schema_type), K(tenant_id), K(schema_id), K(schema_version));
              } else {
                ret = OB_SUCCESS;
                update_history_cache = true;
                LOG_INFO("precise version not founded since schema version is too old, " \
                         "will retrieve it from inner table", KR(ret), K(key), K(val),
                         K(schema_version), "schema_type", schema_type_str(schema_type));
              }
            }
          }

          // try use precise_version
          if (OB_SUCC(ret) && precise_version > 0) {
            if (OB_FAIL(schema_cache_.get_schema(schema_type,
                                                 tenant_id,
                                                 schema_id,
                                                 precise_version,
                                                 handle,
                                                 schema))) {
              if (ret != OB_ENTRY_NOT_EXIST) {
                LOG_WARN("get schema from cache failed", KR(ret), K(key),
                         K(schema_version), K(precise_version));
              } else {
                ret = OB_SUCCESS;
              }
            } else {
              LOG_TRACE("precise version hit", K(key), K(schema_version), K(precise_version),
                       K(tenant_id), K(schema_id), "schema_type", schema_type_str(schema_type));
              has_hit = true;
            }
          }
        }
      }

      // 2. Query inner table
      if (OB_SUCC(ret) && !not_exist && !has_hit) {
        if (OB_FAIL(schema_fetcher_.fetch_schema(schema_type,
                                                 schema_status,
                                                 schema_id,
                                                 schema_version,
                                                 allocator,
                                                 tmp_schema))) {
          LOG_WARN("fetch schema failed", K(tenant_id), K(schema_type),
                   K(schema_id), K(schema_version), K(ret));
        } else if (OB_ISNULL(tmp_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(tenant_id), K(schema_type), K(schema_id),
                   K(schema_version), KP(tmp_schema), K(ret));
        } else if (TABLE_SCHEMA == schema_type) {
          ObTableSchema *table_schema = static_cast<ObTableSchema *>(tmp_schema);
          // process index
          if (OB_ALL_CORE_TABLE_TID == schema_id) {
            // do-nothing
          } else if (!need_construct_aux_infos_(*table_schema)) {
            // do-nothing
          } else if (ObSysTableChecker::is_sys_table_has_index(schema_id)) {
            if (OB_FAIL(ObSysTableChecker::fill_sys_index_infos(*table_schema))) {
              LOG_WARN("fail to fill sys indexes", KR(ret), K(tenant_id), "table_id", schema_id);
            }
          } else if (is_lazy) {
            if (OB_FAIL(construct_aux_infos_(
                *sql_proxy_, schema_status, tenant_id, *table_schema))) {
              LOG_WARN("fail to construct aux infos", KR(ret),
                       K(schema_status), K(tenant_id), KPC(table_schema));
            }
          } else {
            if (OB_FAIL(add_aux_schema_from_mgr(*mgr, *table_schema, USER_INDEX))) {
              LOG_WARN("get index schemas failed", K(ret), KPC(table_schema));
            } else if (OB_FAIL(add_aux_schema_from_mgr(*mgr, *table_schema, AUX_VERTIAL_PARTITION_TABLE))) {
              LOG_WARN("get aux vp table schemas failed", K(ret), KPC(table_schema));
            } else if (OB_FAIL(add_aux_schema_from_mgr(*mgr, *table_schema, AUX_LOB_META))) {
              LOG_WARN("get aux lob meta table schemas failed", K(ret), KPC(table_schema));
            } else if (OB_FAIL(add_aux_schema_from_mgr(*mgr, *table_schema, AUX_LOB_PIECE))) {
              LOG_WARN("get aux lob data table schemas failed", K(ret), KPC(table_schema));
            }
          }
          // process mv
          if (OB_FAIL(ret)) {
          } else if (MATERIALIZED_VIEW == table_schema->get_table_type()
                     && !g_skip_resolve_materialized_view_definition_) {
            // Ideally, the current function should no longer rely on schema_guard,
            // but in order to deal with compatibility, it has to be used here
            ObSchemaGetterGuard schema_guard;
            const ObSimpleTableSchemaV2 *mv_schema = NULL;
            if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
              LOG_WARN("get schema guard failed", K(ret), K(tenant_id));
            } else if (OB_FAIL(schema_guard.get_simple_table_schema(
                       tenant_id, schema_id, mv_schema))) {
              LOG_WARN("get table schema failed", K(tenant_id), K(schema_id), K(ret));
            } else if (mv_schema != NULL && mv_schema->get_schema_version() == schema_version) {
              // do-nothing
            } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard, schema_version, true))) {
              LOG_WARN("get schema guard failed", K(tenant_id), K(schema_id), K(schema_version), K(ret));
            }
            if (FAILEDx(build_full_materalized_view_schema(schema_guard, allocator, table_schema))) {
              LOG_WARN("fail to make columns for materialized table schema",
                       K(ret), K(tenant_id), K(schema_id));
            }
          }
        }

        // 3. convert schema_version to raise cache hit ratio
        int64_t precise_version = schema_version;
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(tmp_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(tenant_id), K(schema_id), KP(tmp_schema), K(ret));
        } else if (TABLE_SCHEMA == schema_type) {
          ObTableSchema *table_schema = static_cast<ObTableSchema *>(tmp_schema);
          precise_version = table_schema->get_schema_version();
          // add debug info
          if (ObSysTableChecker::is_sys_table_has_index(table_schema->get_table_id())) {
            ObTaskController::get().allow_next_syslog();
            LOG_INFO("fetch sys table schema with index", KR(ret),
                     K(schema_status), K(schema_id),
                     K(schema_version), K(precise_version),
                     "schema_mgr_version", OB_ISNULL(mgr) ? 0 : mgr->get_schema_version(),
                     "table_name", table_schema->get_table_name(),
                     "index_cnt", table_schema->get_index_tid_count());
          }
          if (is_system_table(table_schema->get_table_id())) {
            LOG_TRACE("fetch sys table schema with lob", KR(ret),
                      K(schema_status), K(schema_id),
                      K(schema_version), K(precise_version),
                      "schema_mgr_version", OB_ISNULL(mgr) ? 0 : mgr->get_schema_version(),
                      "table_name", table_schema->get_table_name(),
                      "lob_meta_table_id", table_schema->get_aux_lob_meta_tid(),
                      "lob_piece_table_id", table_schema->get_aux_lob_piece_tid());
          }
        } else if (TABLE_SIMPLE_SCHEMA == schema_type) {
          ObSimpleTableSchemaV2 *table_schema = static_cast<ObSimpleTableSchemaV2 *>(tmp_schema);
          precise_version = table_schema->get_schema_version();
        } else if (TENANT_SCHEMA == schema_type) {
          ObTenantSchema *tenant_schema = static_cast<ObTenantSchema *>(tmp_schema);
          precise_version = tenant_schema->get_schema_version();
        } else if (TABLEGROUP_SCHEMA == schema_type) {
          ObTablegroupSchema *tablegroup_schema = static_cast<ObTablegroupSchema *>(tmp_schema);
          precise_version = tablegroup_schema->get_schema_version();
        } else if (DATABASE_SCHEMA == schema_type) {
          ObDatabaseSchema *database_schema = static_cast<ObDatabaseSchema *>(tmp_schema);
          precise_version = database_schema->get_schema_version();
        }

        // 4. renew cache
        if (FAILEDx(schema_cache_.put_and_fetch_schema(
                    schema_type,
                    tenant_id,
                    schema_id,
                    precise_version,
                    *tmp_schema,
                    handle,
                    schema))) {
          LOG_WARN("put and fetch schema failed", K(tenant_id), K(schema_type),
                   K(schema_id), K(precise_version), K(schema_version), KR(ret));
        } else if (update_history_cache
                   && OB_FAIL(schema_cache_.put_schema_history_cache(
                      schema_type, tenant_id, schema_id, schema_version, precise_version))) {
          LOG_WARN("fail to put schema history cache", KR(ret), K(schema_type),
                   K(tenant_id), K(schema_id), K(schema_version), K(precise_version));
        }

#ifndef NDEBUG
        if (OB_SUCC(ret) && is_lazy) {
          // The expectation of lazy mode is to use the specific schema's schema_version to take guard.
          // add a check to see if there is any usage that does not match the expected behavior.
          if (TABLE_SCHEMA == schema_type
              || TABLE_SIMPLE_SCHEMA == schema_type
              || TENANT_SCHEMA == schema_type
              || TABLEGROUP_SCHEMA == schema_type
              || DATABASE_SCHEMA == schema_type) {
            if (precise_version != schema_version) {
              LOG_INFO("schema_version not match in lazy mode", K(ret),
                       K(precise_version), K(schema_version), K(schema_id), K(schema_type));
            }
          } else {
            LOG_INFO("schema_type not match in lazy mode", K(ret),
                     K(schema_version), K(schema_id), K(schema_type));
          }
        }
#endif
      }
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::add_aux_schema_from_mgr(
    const ObSchemaMgr &mgr,
    ObTableSchema &table_schema,
    const ObTableType table_type)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObSimpleTableSchemaV2 *, 8> simple_aux_tables;
  if (OB_FAIL(mgr.get_aux_schemas(table_schema.get_tenant_id(),
              table_schema.get_table_id(), simple_aux_tables, table_type))) {
    LOG_WARN("get aux table schemas failed", K(ret), K(table_schema.get_table_id()));
  } else {
    FOREACH_CNT_X(tmp_simple_aux_table, simple_aux_tables, OB_SUCC(ret)) {
      const ObSimpleTableSchemaV2 *simple_aux_table = *tmp_simple_aux_table;
      if (OB_ISNULL(simple_aux_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret));
      } else {
        if (simple_aux_table->is_index_table() || simple_aux_table->is_materialized_view()) {
          if (OB_FAIL(table_schema.add_simple_index_info(ObAuxTableMetaInfo(
                     simple_aux_table->get_table_id(),
                     simple_aux_table->get_table_type(),
                     simple_aux_table->get_index_type())))) {
            LOG_WARN("fail to add simple_index_info", K(ret), K(*simple_aux_table));
          }
        } else if (simple_aux_table->is_aux_vp_table()) {
          if (OB_FAIL(table_schema.add_aux_vp_tid(simple_aux_table->get_table_id()))) {
            LOG_WARN("add aux_vp table id failed", K(ret), K(simple_aux_table->get_table_id()));
          }
        } else if (simple_aux_table->is_aux_lob_meta_table()) {
          table_schema.set_aux_lob_meta_tid(simple_aux_table->get_table_id());
        } else if (simple_aux_table->is_aux_lob_piece_table()) {
          table_schema.set_aux_lob_piece_tid(simple_aux_table->get_table_id());
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObMultiVersionSchemaService::put_fallback_liboblog_schema_to_slot(
    ObSchemaMgrCache &schema_mgr_cache_for_liboblog,
    ObSchemaMemMgr &mem_mgr_for_liboblog,
    ObSchemaMgr *&target_mgr,
    ObSchemaMgrHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(put_fallback_schema_to_slot(target_mgr,
                                          schema_mgr_cache_for_liboblog,
                                          mem_mgr_for_liboblog,
                                          handle))) {
    LOG_WARN("put fallback schema to slot failed", K(ret));
  } else {
    //protect by the lock mem_mgr_for_liboblog_mutex_, so no concurrent here
    static int64_t last_dump_time = 0;
    int64_t current_time = ObTimeUtility::current_time();
    if (current_time - last_dump_time > 60000000) {// print every 60 seconds
      schema_mgr_cache_for_liboblog_.dump();
      last_dump_time = current_time;
    }
  }
  return ret;
}

/**
 * put fallback schema to slot for liboblog and check need to switch allocator
 */
int ObMultiVersionSchemaService::put_fallback_schema_to_slot(ObSchemaMgr *&new_mgr,
                                                             ObSchemaMgrCache &schema_mgr_cache,
                                                             ObSchemaMemMgr &schema_mem_mgr,
                                                             ObSchemaMgrHandle &handle)
{
  int ret = OB_SUCCESS;
  ObSchemaMgr *eli_schema_mgr = NULL;
  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_FAIL(schema_mgr_cache.put(new_mgr, eli_schema_mgr, &handle))) {
    LOG_WARN("put schema mgr failed", K(ret));
  } else {
    int64_t cost = ObTimeUtility::current_time() - start_time;
    LOG_INFO("put schema mgr succeed", K(cost),
             "schema_version", new_mgr->get_schema_version(),
             "eliminated_schema_version", NULL != eli_schema_mgr ?
                 eli_schema_mgr->get_schema_version() : OB_INVALID_VERSION);
    if (OB_FAIL(schema_mem_mgr.free_schema_mgr(eli_schema_mgr))) {
      LOG_WARN("free eli schema mgr falied", KR(ret));
    } else {
      // The allocator of the schema_mgr generated by the schema fallback is independent
      // and does not need to be released through switch_allocator.
      // For the current use scenarios of OB, liboblog, and agentserver in 2.x, fallback will not consume
      // non-latest schema mgr for a long time, and the memory release of schema_mgr generated by fallback
      // can be accelerated through background thread inspection.
    }
  }
  return ret;
}

/**
 * fallback to target_version
 *
 */
int ObMultiVersionSchemaService::fallback_schema_mgr_for_liboblog(
    const ObRefreshSchemaStatus &schema_status,
    const int64_t target_version,
    const int64_t latest_local_version,
    const ObSchemaMgr *&schema_mgr,
    ObSchemaMgrHandle &handle)
{
  int ret = OB_SUCCESS;
  ObSchemaMgrCache *schema_mgr_cache_for_liboblog = NULL;
  ObSchemaMemMgr *mem_mgr_for_liboblog = NULL;
  ObSchemaMgrCache *schema_mgr_cache = NULL;
//   add concurrency control for fallback
  ObSchemaConstructTask &task = ObSchemaConstructTask::get_instance();
  uint64_t tenant_id = schema_status.tenant_id_;
  task.cc_before(target_version);
  // Determine whether the schema has been split according to whether the tenant_id is legal
  ObSchemaStore* schema_store = NULL;
  if (OB_ISNULL(schema_store = schema_store_map_.get(tenant_id))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to get schema_store", K(ret));
  } else if (OB_FAIL(mem_mgr_for_liboblog_map_.get_refactored(tenant_id, mem_mgr_for_liboblog))) {
    LOG_WARN("fail to get mem mgr", K(ret), K(tenant_id));
  } else if (OB_ISNULL(mem_mgr_for_liboblog)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_mgr is null", K(ret), K(tenant_id));
  } else {
    schema_mgr_cache = &schema_store->schema_mgr_cache_;
    schema_mgr_cache_for_liboblog = &schema_store->schema_mgr_cache_for_liboblog_;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_mgr_cache_for_liboblog->get(target_version,
                                                        schema_mgr,
                                                        handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get schema mgr failed", K(ret), K(target_version));
      schema_mgr_cache_for_liboblog->dump();
    } else if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("full schema is not ready, cann't get fallback schema guard",
                K(ret), K(schema_status), K(target_version));
    } else {
      FLOG_INFO("[FALLBACK_SCHEMA] schema mgr cache for liboblog miss",
                K(schema_status), K(target_version));
      const int64_t start = ObTimeUtility::current_time();
      const ObSchemaMgr *target_mgr = NULL;
      ObSchemaMgrHandle target_mgr_handle;
      // true if 1) miss in fallback OR 2) hit but with larger abs
      bool need_extra_get = false;

      // for faster fallback, find the schema manager with nearest version:
      ret = schema_mgr_cache_for_liboblog->get_nearest(target_version,
                                                       target_mgr,
                                                       target_mgr_handle);
      if (OB_FAIL(ret)) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_ERROR("get_nearest schema_mgr failed",
                    K(ret), K(schema_status), K(target_version));
        } else {
          need_extra_get = true;
          LOG_INFO("get from liboblog slot failed, need extra get",
                    K(ret), K(schema_status), K(target_version));
        }
      } else {
        if (NULL == target_mgr) {
          ret = OB_SCHEMA_ERROR;
          LOG_ERROR("get target_mgr is NULL", K(ret), K(schema_status), K(target_version));
        } else if (
            llabs(target_mgr->get_schema_version() - target_version) >
            llabs(latest_local_version - target_version)) {
          need_extra_get = true;
          LOG_INFO("get from liboblog slot failed, need extra get",
                    K(ret), K(schema_status), K(target_version), K(latest_local_version));
        }
      }

      if (need_extra_get) {
        // reset to success due to might OB_ENTRY_NOT_EXIST
        if (OB_FAIL(schema_mgr_cache->get(latest_local_version, target_mgr,
                target_mgr_handle))) {
          LOG_WARN("get schema mgr failed",
                   K(ret), K(schema_status), K(target_version), K(latest_local_version));
          schema_mgr_cache_.dump();
        } else if (NULL == target_mgr) {
          ret = OB_SCHEMA_ERROR;
          LOG_ERROR("get target_mgr is NULL",
                    K(ret), K(schema_status), K(target_version));
        } else {}
      }

      int64_t from_version = OB_INVALID_VERSION;
      if (OB_SUCC(ret)) {
        HEAP_VAR(ObSchemaMgr, tmp_mgr) {
          lib::ObMutexGuard mutex_guard(mem_mgr_for_liboblog_mutex_);
          from_version = target_mgr->get_schema_version();
          if (OB_FAIL(tmp_mgr.init())) {
            LOG_WARN("init tmp_mgr failed",
                     K(ret), K(schema_status), K(from_version), K(target_version));
          } else {
            const int64_t tmp_start = ObTimeUtility::current_time();
            if (OB_FAIL(tmp_mgr.deep_copy(*target_mgr))) {
              LOG_WARN("deep copy schema_mgr failed",
                       K(ret), K(schema_status), K(from_version), K(target_version));
            }
            LOG_INFO("deep copy schema_mgr cost", KR(ret),
                     "cost", ObTimeUtility::current_time() - tmp_start,
                     K(schema_status), K(from_version), K(target_version));
          }
          if (OB_SUCC(ret)) {
            bool alloc_for_liboblog = true;
            ObSchemaMgr *new_mgr = NULL;
            target_mgr_handle.reset();
            if (OB_FAIL(fallback_schema_mgr(schema_status, tmp_mgr, target_version))) {
              LOG_WARN("fallback schema mgr falied",
                       K(ret), K(schema_status), K(from_version), K(target_version));
            } else if (OB_FAIL(mem_mgr_for_liboblog->alloc_schema_mgr(new_mgr, alloc_for_liboblog))) {
              LOG_WARN("alloc schema mgr for liboblog failed",
                       K(ret), K(schema_status), K(from_version), K(target_version));
            } else if (OB_ISNULL(new_mgr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("new_mgr is null",
                       K(ret), K(schema_status), K(from_version), K(target_version));
            } else if (OB_FAIL(new_mgr->init())) {
              LOG_WARN("init new_mgr failed",
                       K(ret), K(schema_status), K(from_version), K(target_version));
            } else {
              const int64_t tmp_start = ObTimeUtility::current_time();
              if (OB_FAIL(new_mgr->deep_copy(tmp_mgr))) {
                LOG_WARN("fail to copy mgr", K(ret));
              }
              LOG_INFO("deep copy schema_mgr cost", KR(ret),
                       "cost", ObTimeUtility::current_time() - tmp_start);

              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(put_fallback_liboblog_schema_to_slot(
                         *schema_mgr_cache_for_liboblog, *mem_mgr_for_liboblog, new_mgr, handle))) {
                LOG_WARN("put fallback schema to slot failed", K(ret));
              } else {
                schema_mgr = new_mgr;
              }
            }
            if (OB_FAIL(ret)) {
              int tmp_ret = OB_SUCCESS;
              schema_mgr = NULL;
              if (OB_TMP_FAIL(mem_mgr_for_liboblog->free_schema_mgr(new_mgr))) {
                LOG_ERROR("fail to free schema mgr", K(ret), K(tmp_ret), K(from_version));
              }
            }
          }
        }
      }

      FLOG_INFO("[FALLBACK_SCHEMA] fallback schema mgr for liboblog finish",
                KR(ret), K(schema_status), K(from_version), K(target_version),
                "cost", ObTimeUtility::current_time() - start);
    }
  } else {
    LOG_TRACE("[FALLBACK_SCHEMA] get schema_mgr from cache", K(schema_status), K(target_version));
  }
  task.cc_after(target_version);
  return ret;
}

int ObMultiVersionSchemaService::get_cluster_schema_guard(
    ObSchemaGetterGuard &guard,
    const RefreshSchemaMode refresh_schema_mode /* = RefreshSchemaMode::NORMAL */)
{
  int ret = OB_SUCCESS;
  // new schema refresh
  if (OB_FAIL(guard.fast_reset())) {
    LOG_WARN("fail to reset guard", K(ret));
  } else if (OB_FAIL(guard.init(GCTX.is_standby_cluster()))) {
    LOG_WARN("fail to init guard", K(ret));
  } else {
    ObSEArray<uint64_t, 1> tenant_ids;
    if (!ObSchemaService::g_liboblog_mode_// Avoid using schema_status_proxy for agentserver and liboblog
        && OB_FAIL(check_restore_tenant_exist(tenant_ids, guard.restore_tenant_exist_))) {
      LOG_WARN("fail to check restore tenant exist", K(ret));
    }
  }
  // get schema status
  ObArray<ObRefreshSchemaStatus> schema_status_array;
  if (OB_SUCC(ret) && guard.use_schema_status()) {
    ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
    if (OB_ISNULL(schema_status_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_status_proxy is null", K(ret));
    } else if (OB_FAIL(schema_status_proxy->get_refresh_schema_status(schema_status_array))) {
      LOG_WARN("fail to get refresh schema status", K(ret));
    } else {}
  }
  if (OB_SUCC(ret)) {
    const ObSchemaMgrInfo *sys_schema_mgr_info = NULL;
    ObRefreshSchemaStatus schema_status;
    const RefreshSchemaMode need_fallback = RefreshSchemaMode::NORMAL;
    int64_t sys_snapshot_version = OB_INVALID_VERSION;
    ObSchemaStore* sys_schema_store = NULL;
    if (OB_FAIL(get_schema_status(schema_status_array, OB_SYS_TENANT_ID, schema_status))) {
      LOG_WARN("fail to get schema status", K(ret));
    } else if (NULL == (sys_schema_store = schema_store_map_.get(OB_SYS_TENANT_ID))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sys schema store");
    } else if (OB_FAIL(get_tenant_refreshed_schema_version(
               OB_SYS_TENANT_ID, sys_snapshot_version))) {
      LOG_WARN("fail to get sys refreshed schema version", K(ret));
    } else if (OB_FAIL(add_schema_mgr_info(guard, sys_schema_store, schema_status,
               OB_SYS_TENANT_ID, sys_snapshot_version, sys_snapshot_version, need_fallback))) {
      LOG_WARN("fail to add schema mgr info", K(ret), K(sys_snapshot_version),
          "refresh_schema_mode", print_refresh_schema_mode(need_fallback));
    } else if (OB_FAIL(guard.get_schema_mgr_info(OB_SYS_TENANT_ID, sys_schema_mgr_info))) {
      LOG_WARN("fail to get sys schema mgr info", K(ret));
    } else if (OB_ISNULL(sys_schema_mgr_info->get_schema_mgr())) {
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("get simple schema in lazy mode not supported", K(ret));
    } else {
      ObArray<const ObSimpleTenantSchema *> tenant_schemas;
      const ObSchemaMgr *sys_schema_mgr = sys_schema_mgr_info->get_schema_mgr();
      if (OB_FAIL(sys_schema_mgr->get_tenant_schemas(tenant_schemas))) {
        LOG_WARN("fail to get tenant_schemas", K(ret));
      } else {
        FOREACH_X(tenant, tenant_schemas, OB_SUCC(ret)) {
          int64_t tenant_snapshot_version = OB_INVALID_VERSION;
          ObSchemaStore* schema_store = NULL;
          schema_status.reset();
          uint64_t tenant_id = OB_INVALID_TENANT_ID;
          if (OB_ISNULL(tenant) || OB_ISNULL(*tenant)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tenant schema is null", K(ret));
          } else if (FALSE_IT(tenant_id = (*tenant)->get_tenant_id())) {
          } else if (OB_SYS_TENANT_ID == tenant_id) {
            //skip
          } else if (NULL == (schema_store = schema_store_map_.get(tenant_id))) {
            ret = OB_SCHEMA_EAGAIN;
            LOG_WARN("get tenant schema store fail, maybe local schema is old",
                     K(ret), KPC(*tenant));
          } else if (FALSE_IT(tenant_snapshot_version = schema_store->get_refreshed_version())) {
          } else {
            // switchover/failover not clear schema_status, Cannot trust schema_status content unconditionally
            // bugfix:
            if (guard.is_standby_cluster() || (*tenant)->is_restore()) {
              if (OB_FAIL(get_schema_status(schema_status_array, tenant_id, schema_status))) {
                LOG_WARN("fail to get schema status", K(ret), KPC(*tenant));
              }
            } else {
              schema_status.tenant_id_ = tenant_id;
            }
            if (FAILEDx(add_schema_mgr_info(guard, schema_store, schema_status, tenant_id,
                tenant_snapshot_version, tenant_snapshot_version, refresh_schema_mode))) {
              LOG_WARN("fail to add schema mgr info", K(ret), KPC(*tenant), K(tenant_snapshot_version),
                  "refresh_schema_mode", print_refresh_schema_mode(refresh_schema_mode));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    guard.schema_service_ = this;
    guard.schema_guard_type_ = ObSchemaGetterGuard::SCHEMA_GUARD;
  }
  return ret;
}

/*
 *  Specify the version to take schema_guard.
 *  If sys_schema_version is not specified, it means that the system tenant
 *  takes the latest schema version refreshed to the local machine
 *   - tenant_schema_version: Indicates the version of the schema of the user tenant except the system tables.
 *   - sys_schema_version: Indicates the version of the schema of the system tenant and the sys tables in user tenant.
 */
int ObMultiVersionSchemaService::get_tenant_schema_guard(
    const uint64_t tenant_id,
    ObSchemaGetterGuard &guard,
    int64_t tenant_schema_version/* = common::OB_INVALID_VERSION*/,
    int64_t sys_schema_version/* = common::OB_INVALID_VERSION*/,
    const RefreshSchemaMode refresh_schema_mode /* = RefreshSchemaMode::NORMAL */)
{
  int ret = OB_SUCCESS;
  // For system tenants, tenant_schema_version and sys_schema_version are consistent
  sys_schema_version = OB_SYS_TENANT_ID == tenant_id ? tenant_schema_version : sys_schema_version;

  // new schema refresh process
  int64_t sys_latest_local_version = OB_INVALID_VERSION;
  int64_t tenant_latest_local_version = OB_INVALID_VERSION;
  int64_t sys_snapshot_version = OB_INVALID_VERSION;
  int64_t tenant_snapshot_version = OB_INVALID_VERSION;
  int64_t baseline_schema_version = OB_INVALID_VERSION;
  ObRefreshSchemaStatus sys_schema_status;
  ObRefreshSchemaStatus tenant_schema_status;
  ObSchemaStore* sys_schema_store = NULL;
  ObSchemaStore* tenant_schema_store = NULL;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(guard.fast_reset())) {
    LOG_WARN("fail to reset schema guard", K(ret));
  } else if (OB_FAIL(guard.init(GCTX.is_standby_cluster()))) {
    LOG_WARN("fail to init guard", K(ret));
  }
  sys_schema_status.tenant_id_ = OB_SYS_TENANT_ID;

  // get system tenant schema_mgr_info
  if (OB_FAIL(ret)) {
  } else if (NULL == (sys_schema_store = schema_store_map_.get(OB_SYS_TENANT_ID))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get sys schema store fail", K(ret));
  } else if (OB_INVALID_VERSION == (sys_latest_local_version = sys_schema_store->get_refreshed_version())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys refreshed schema version is invalid", K(ret), K(sys_latest_local_version), K(sys_schema_version));
  } else if (sys_schema_version > sys_latest_local_version) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("specified schema version larger than latest schema version, need retry",
             KR(ret), K(sys_schema_version), K(sys_latest_local_version));
  } else {
    sys_snapshot_version = OB_INVALID_VERSION == sys_schema_version ?
                           sys_latest_local_version : sys_schema_version;
    if (!ObSchemaService::g_liboblog_mode_
        && OB_INVALID_VERSION != sys_schema_version) {
      // for max avaliablity, ignore tmp_ret
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = get_baseline_schema_version(
          OB_SYS_TENANT_ID, false/*auto_update*/, baseline_schema_version))) {
        LOG_WARN("get sys tenant baseline schema version failed", KR(tmp_ret));
      }
    }
    if (OB_INVALID_VERSION != baseline_schema_version
        && OB_INVALID_VERSION != sys_schema_version
        && sys_schema_version < baseline_schema_version) {
      LOG_INFO("change sys schema version to baseline",
               "sys schema version", sys_schema_version,
               "baseline version", baseline_schema_version);
      sys_snapshot_version = baseline_schema_version;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_schema_mgr_info(guard, sys_schema_store, sys_schema_status,
               OB_SYS_TENANT_ID, sys_snapshot_version, sys_latest_local_version,
               refresh_schema_mode))) {
      LOG_WARN("fail to add schema mgr info", K(ret), K(sys_snapshot_version),
               "refresh_schema_mode", print_refresh_schema_mode(refresh_schema_mode));
    } else {
      // The guard will be used in subsequent check_tenant_is_restore, and some members will be set up here
      // to avoid reporting errors during subsequent consumption of guard
      guard.tenant_id_ = OB_SYS_TENANT_ID;
      guard.schema_service_ = this;
      guard.schema_guard_type_ = ObSchemaGetterGuard::TENANT_SCHEMA_GUARD;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    // Avoid circular dependencies
  } else if (ObSchemaService::g_liboblog_mode_) {
    tenant_schema_status.tenant_id_ = tenant_id;
  } else if (!guard.is_standby_cluster()
             && OB_FAIL(check_tenant_is_restore(&guard, tenant_id, guard.restore_tenant_exist_))) {
    LOG_WARN("fail to check restore tenant exist", K(ret), K(tenant_id));
  } else if (guard.use_schema_status()) {
    ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
    if (OB_ISNULL(schema_status_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_status_proxy is null", K(ret));
    } else if (OB_FAIL(schema_status_proxy->get_refresh_schema_status(tenant_id, tenant_schema_status))) {
      LOG_WARN("fail to get refresh schema status", K(ret));
    }
  } else {
    tenant_schema_status.tenant_id_ = tenant_id;
  }

  // get user tenant schema_mgr_info
  if (OB_FAIL(ret)) {
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    // The system tenant has already taken it, you can skip here
  } else if (OB_CORE_SCHEMA_VERSION == tenant_schema_version) {
    // the scenario where specifying the version takes the guard. At this time, the tenant has not been created yet,
    // and a special schema_version will be passed in. At this time, no error will be reported to avoid
    // error of the tenant building in transaction two.
    LOG_DEBUG("tenant maybe not create yet, just skip", K(ret), K(tenant_id));
  } else if (NULL == (tenant_schema_store = schema_store_map_.get(tenant_id))) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("get tenant schema store fail, maybe local schema is old", K(ret), K(tenant_id));
  } else if (FALSE_IT(tenant_latest_local_version = tenant_schema_store->get_refreshed_version())) {
  } else if (tenant_schema_version > tenant_latest_local_version) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("specified schema version larger than latest schema version, need retry",
             KR(ret), K(tenant_schema_version), K(tenant_latest_local_version),
             K(sys_schema_version), K(sys_latest_local_version));
  } else {
    tenant_snapshot_version = OB_INVALID_VERSION == tenant_schema_version ?
                              tenant_latest_local_version : tenant_schema_version;
    if (!ObSchemaService::g_liboblog_mode_
        && OB_INVALID_VERSION != tenant_snapshot_version) {
      // for max avaliablity, ignore tmp_ret
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = get_baseline_schema_version(
          tenant_id, false/*auto_update*/, baseline_schema_version))) {
        LOG_WARN("get baseline schema version failed", KR(tmp_ret), K(tenant_id));
      }
    }
    if (OB_INVALID_VERSION != baseline_schema_version
        && OB_INVALID_VERSION != tenant_schema_version
        && tenant_schema_version < baseline_schema_version) {
      LOG_INFO("change tenant schema version to baseline",
                K(tenant_id), K(tenant_schema_version), K(baseline_schema_version));
      tenant_snapshot_version = baseline_schema_version;
    }
    // check tenant exist
    const ObSchemaMgrInfo *sys_schema_mgr_info = NULL;
    if (OB_FAIL(ret)) {
    } else if (sys_snapshot_version == tenant_schema_store->get_checked_sys_version()) {
      // sys schema version match, no need to check tenant existence.
    } else if (OB_FAIL(guard.get_schema_mgr_info(OB_SYS_TENANT_ID, sys_schema_mgr_info))) {
      LOG_WARN("fail to get sys schema mgr info", K(ret));
    } else if (OB_ISNULL(sys_schema_mgr_info->get_schema_mgr())) {
      // lazy mode
      const ObSchema *base_schema = NULL;
      ObKVCacheHandle handle;
      if (OB_FAIL(get_schema(NULL, sys_schema_status, TENANT_SCHEMA, tenant_id,
                             tenant_snapshot_version, handle, base_schema))) {
        LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id), K(tenant_snapshot_version));
      } else if (OB_ISNULL(base_schema)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("tenant not exist", K(ret), K(tenant_id), "schema_version", tenant_snapshot_version);
      }
    } else {
      const ObSimpleTenantSchema *tenant_schema = NULL;
      const ObSchemaMgr *sys_schema_mgr = sys_schema_mgr_info->get_schema_mgr();
      if (OB_FAIL(sys_schema_mgr->get_tenant_schema(tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("tenant not exist", KR(ret), K(tenant_id),
                 "schema_version", sys_schema_mgr->get_schema_version());
      } else {
        tenant_schema_store->update_checked_sys_version(sys_snapshot_version);
      }
    }
    if (FAILEDx(add_schema_mgr_info(guard, tenant_schema_store, tenant_schema_status,
                tenant_id, tenant_snapshot_version, tenant_latest_local_version,
                refresh_schema_mode))) {
      LOG_WARN("fail to add schema mgr info", K(ret), K(tenant_id), K(tenant_snapshot_version),
               "refresh_schema_mode", print_refresh_schema_mode(refresh_schema_mode));
    }
  }

  if (OB_SUCC(ret)) {
    guard.tenant_id_ = tenant_id;
    LOG_DEBUG("get tenant schema guard", K(tenant_id), K(tenant_schema_version), K(sys_schema_version));
  }

  if (OB_FAIL(ret)
      && OB_TENANT_HAS_BEEN_DROPPED != ret
      && ObSchemaService::g_liboblog_mode_) {
    TenantStatus tenant_status = TENANT_STATUS_INVALID;
    int temp_ret = query_tenant_status(tenant_id, tenant_status);
    if (OB_SUCCESS != temp_ret){
      LOG_WARN("query tenant status failed", K(ret), K(temp_ret), K(tenant_id));
    } else if (TENANT_DELETED == tenant_status) {
      LOG_INFO("tenant has been dropped, no need retry", K(ret), K(tenant_id));
      ret = OB_TENANT_HAS_BEEN_DROPPED; //overwrite ret
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::get_tenant_ids(ObIArray<uint64_t> &tenant_ids) {
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("get all tenants failed", K(ret));
  }
  return ret;
}

int ObMultiVersionSchemaService::get_tenant_full_schema_guard(
    const uint64_t tenant_id,
    ObSchemaGetterGuard &guard,
    bool check_formal /*= true*/)
{
  int ret = OB_SUCCESS;

  if (!is_tenant_full_schema(tenant_id)) {
    ret = OB_TENANT_SCHEMA_NOT_FULL;
    if (EXECUTE_COUNT_PER_SEC(1)) {
      LOG_WARN("tenant schema not full", K(ret), K(tenant_id));
    }
  } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id));
  } else if (check_formal && OB_FAIL(guard.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret), K(tenant_id));
  }
  return ret;
}

int ObMultiVersionSchemaService::add_schema_mgr_info(
    ObSchemaGetterGuard &schema_guard,
    ObSchemaStore* schema_store,
    const ObRefreshSchemaStatus &schema_status,
    const uint64_t tenant_id,
    const int64_t snapshot_version,
    const int64_t latest_local_version,
    const RefreshSchemaMode refresh_schema_mode /* = RefreshSchemaMode::NORMAL */)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *schema_mgr = NULL;
  ObSchemaMgrInfo* new_schema_mgr_info = NULL;
  if (OB_INVALID_TENANT_ID == tenant_id
      || snapshot_version <= 0
      || latest_local_version <= 0
      || NULL == schema_store) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or snapshot_version", K(ret), K(tenant_id), K(snapshot_version), KP(schema_store));
  } else if (schema_status.tenant_id_ != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema_status or tenant_id", K(ret), K(tenant_id), K(snapshot_version), K(schema_status));
  } else {
    ObSchemaMgrHandle handle(schema_guard.mod_);
    ObSchemaMgrInfo schema_mgr_info(tenant_id,
                                    snapshot_version,
                                    schema_mgr,
                                    handle,
                                    schema_status);
    int64_t count = schema_guard.schema_mgr_infos_.count();
    // Guaranteed to be monotonically increasing when inserted
    if (count > 0 && schema_guard.schema_mgr_infos_[count - 1].get_tenant_id() >= tenant_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant_id should greator than the last one", K(ret),
               "pre_tenant_id", schema_guard.schema_mgr_infos_[count - 1].get_tenant_id(), K(tenant_id));
    } else if (OB_FAIL(schema_guard.schema_mgr_infos_.push_back(schema_mgr_info))) {
      LOG_WARN("fail to push back schema_mgr_info", K(ret), K(tenant_id));
    } else {
      new_schema_mgr_info = &schema_guard.schema_mgr_infos_.at(count);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(new_schema_mgr_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr is null", KR(ret), K(tenant_id));
  } else {
    ObSchemaMgrHandle& handle = new_schema_mgr_info->get_schema_mgr_handle();
    bool need_fallback = false;
    if (OB_FAIL(ret)) {
    } else if (RefreshSchemaMode::FORCE_FALLBACK == refresh_schema_mode) {
      // Avoid force_fallback mode occupying the schema slot to cause OOM, schema_mgr deep_copy to schema_mgr_cache_for_liboblog
      need_fallback = true;
    } else if (OB_FAIL(schema_store->schema_mgr_cache_.get(snapshot_version, schema_mgr, handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get schema mgr failed", K(ret), K(snapshot_version));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret) && need_fallback) {
      // fallback
      if (OB_FAIL(schema_store->schema_mgr_cache_for_liboblog_.get(snapshot_version, schema_mgr, handle))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("[FALLBACK_SCHEMA] get schema mgr failed",
                    K(ret), K(schema_status), K(snapshot_version));
        } else if (OB_FAIL(fallback_schema_mgr_for_liboblog(schema_status, snapshot_version,
            latest_local_version, schema_mgr, handle))) {
          LOG_WARN("[FALLBACK_SCHEMA] fallback schema mgr for liboblog failed", K(ret),
                   K(schema_status), K(snapshot_version), K(latest_local_version));
        } else {
          LOG_TRACE("[FALLBACK_SCHEMA] get schema_mgr by fallback schema_mgr",
                    K(schema_status), K(snapshot_version), K(latest_local_version));
        }
      } else {
        LOG_TRACE("[FALLBACK_SCHEMA] get schema_mgr by fallback schema_mgr",
                  K(schema_status), K(snapshot_version), K(latest_local_version));
      }
    }
    if (OB_SUCC(ret)) {
      new_schema_mgr_info->set_schema_mgr(schema_mgr);
      if (snapshot_version == latest_local_version
          /*&& RefreshSchemaMode::FORCE_LAZY != refresh_schema_mode*/
          && OB_ISNULL(schema_mgr)) {
        LOG_INFO("should not be lazy mode", K(ret), KPC(new_schema_mgr_info), K(latest_local_version));
      }
    }
  }

  return ret;
}

/*
 * Background description:
 * There are two sources of this schema_version
 * 1. schema_version of SSTABLE:
 *    This is the initial transmission of RS. The schema_version cannot guarantee that the schema of the database table
 *    and the index table when the table is created can be seen at the same time.
 *    it may only see one of them, which has historical problems.
 *
 * 2. schema_version of minor or major compaction:
 *    1) 1.4.x
 *       - minor freeze uses the schema_verison on the memstore (no push when no data is written,
 *        so the schema_verison must be able to see the corresponding table schema);
 *       - Merge the DDL thread, and ensure that there is no concurrent table creation when taking the schema_version,
 *        so the schema_version must be able to see the Partition created before the merge.
 *    2) 2.0, 2.1.x:
 *       - minor freeze uses the schema_version corresponding to gc_snapshot_version, and minor freeze and major freeze
 *        confirm the schema_version does not go through the DDL thread, so the concurrently created Partition
 *        may not be seen in the corresponding schema_version.
 *    3) 2.2.x:
 *       - The snapshot point & schema_version of minor freeze and major freeze is split by tenant,
 *        and the DDL thread is used.
 *
 * In summary: 1.4.x & 2.2.x use SSTABLE & minor freeze and major freeze schema_version
 *       must get the schema of the table to be merged;
 *       2.0 & 2.1.x When table_schema cannot be obtained from a given schema_version,
 *       orig_schema_version must be used to obtain table_schema.
 *
 * save_schema_version is schema_version or the corresponding orig_schema_version
 */
int ObMultiVersionSchemaService::retry_get_schema_guard(
    const uint64_t tenant_id,
    const int64_t schema_version,
    const uint64_t table_id,
    ObSchemaGetterGuard &schema_guard,
    int64_t &save_schema_version)
{
  int ret = OB_SUCCESS;

  int32_t retry_time = 0;
  const ObTableSchema *table_schema = NULL;
  save_schema_version = schema_version;

  if (!ObSchemaService::is_formal_version(schema_version)
      || 0 == schema_version) {
    // There are several special versions of schema_version, here only warning is printed, and the version is not verified
    // 1. schema_version = 0 : Before 223, the partition did not record the schema_version,
    //  and the minor free will use this value to get the schema;
    // 2. schema_version = 1 : 225 The demand for schema_history recovery point calculation.
    //  Merger will increase the partition's schema_version, and the system tenant 1 version schema_version is 1.
    // 3. schema_version = 2 : The first schema_version version of bootstrap (only memory is used temporarily);
    // 4. informal version : System tenant has system table/core table changes
    // For version 0~2, system tenant will be automatically converted to baseline_schema_version,
    // and user tenants will report an error; the informative version has little effect on the full schema
    // Here the defense is removed first, only print warn
    LOG_WARN("get schema guard with informal version", K(table_id), K(schema_version));
  }
  if (OB_SUCC(ret)) {
    while (retry_time < MAX_RETRY_TIMES) {
      bool is_dropped = false;
      if (OB_FAIL(check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
        if (OB_NOT_INIT != ret) {
          LOG_WARN("check if tenant has been dropped failed", K(ret), K(table_id));
        }
      } else if (is_dropped) {
        ret = OB_TENANT_HAS_BEEN_DROPPED;
        LOG_WARN("tenant has been deleted, just skip", K(ret), K(tenant_id));
      } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard, schema_version))) {
        if (OB_SCHEMA_EAGAIN != ret) {
          LOG_WARN("fail to get tenant schema guard", K(ret), K(table_id), K(schema_version));
        }
      }
      if (OB_SCHEMA_EAGAIN != ret && OB_NOT_INIT != ret) {
        break;
      } else {
        ob_usleep(RETRY_INTERVAL_US);
        ++retry_time;
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get tenant schema guard", K(ret), K(table_id), K(schema_version));
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to get schema guard", K(ret), K(schema_version));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(table_id));
  } else if (OB_NOT_NULL(table_schema)) {
    // success
  } else {
    // table not exist , return guard which can get original table schema
    ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = tenant_id;

    bool is_restore = false;
    if (is_sys_tenant(tenant_id)
        || is_meta_tenant(tenant_id)
        || ObSchemaService::g_liboblog_mode_) {
      // skip
    } else if (!schema_guard.is_standby_cluster()
               && OB_FAIL(check_tenant_is_restore(&schema_guard, tenant_id, is_restore))) {
      LOG_WARN("fail to check restore tenant exist", K(ret), K(tenant_id));
    } else if (schema_guard.is_standby_cluster() || is_restore) {
      ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
      if (OB_ISNULL(schema_status_proxy)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_status_proxy is null", K(ret));
      } else if (OB_FAIL(schema_status_proxy->get_refresh_schema_status(
                         tenant_id, schema_status))) {
        LOG_WARN("fail to get refresh schema status", K(ret));
      } else {}
    }
    if (OB_FAIL(ret)) {
    } else if (is_inner_table(table_id)) {
      int64_t baseline_schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(get_baseline_schema_version(tenant_id, false/*auto_update*/, baseline_schema_version))) {
        LOG_WARN("fail to get baseline schema version", K(ret), K(tenant_id));
      } else if (baseline_schema_version <= 0) {
        ret = OB_SCHEMA_EAGAIN;
        LOG_WARN("baseline schema version is invalid, try later",
                 K(ret), K(tenant_id), K(table_id), K(schema_version));
      } else {
        // try use version_his_map
        VersionHisKey key(TABLE_SCHEMA, tenant_id, table_id);
        VersionHisVal val;
        int ret = version_his_map_.get_refactored(key, val);
        if (OB_SUCCESS != ret && OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to get table version history", K(ret), K(key), K(schema_version));
        } else if (OB_HASH_NOT_EXIST == ret) { // overwrite ret
          int64_t local_version = OB_INVALID_VERSION;
          if (OB_FAIL(get_tenant_refreshed_schema_version(tenant_id, local_version))) {
            LOG_WARN("fail to get tenant refreshed schema_version",
                     K(ret), K(tenant_id), K(key), K(schema_version));
          } else if (local_version <= OB_CORE_SCHEMA_VERSION) {
            ret = OB_SCHEMA_EAGAIN;
            LOG_WARN("local schema is old, try later",
                     K(ret), K(tenant_id), K(key), K(schema_version), K(local_version));
          } else if (OB_FAIL(construct_schema_version_history(
                             schema_status, local_version, key, val))) {
            LOG_WARN("fail to load table schema version history",
                     K(ret), K(schema_status), K(key), K(schema_version), K(local_version));
          } else if (0 >= val.min_version_ || 0 == val.valid_cnt_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sys table not exist", K(ret), K(tenant_id),
                     K(key), K(val), K(schema_version));
          } else {
            save_schema_version = max(val.min_version_, baseline_schema_version);
          }
        } else {
          save_schema_version = max(val.min_version_, baseline_schema_version);
        }
      }
    } else {
      // try use orig_schema_version
      if (OB_FAIL(ret)) {
      } else if (NULL == schema_service_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_service_ is null", K(ret));
      } else if (OB_FAIL(schema_service_->get_ori_schema_version(
                         schema_status, tenant_id, table_id, save_schema_version))) {
        if (OB_ITER_END == ret) {
          // There are several situations where orig_schema_version cannot be obtained:
          // 1. The table built in 1.4.x does not have orig_schema_version,
          //  and the following situations occur when entering this branch:
          //    - The schema_version is derived from major or minor freeze and is a relatively large value.
          //      At this time, the table has been dropped, and errors will continue to be reported
          //      until the partition is recycled by the GC logic;
          //    - BUG, should not enter this branch.
          // 2. 2.0 and 2.1.x concurrently build tables, the transaction is not committed and needs to be tried again.
          ret = OB_SCHEMA_EAGAIN;
          LOG_WARN("orig_schema_version not exist, try again",
                   K(ret), K(table_id), K(schema_version));
        } else {
          LOG_WARN("failed to get_ori_schema_version", K(ret), K(save_schema_version), K(table_id));
        }
      } else if (schema_version > save_schema_version) {
        // If the specified version is greater than orig_schema_version, the table can be determined to be deleted
        ret = OB_TABLE_IS_DELETED;
        ObTaskController::get().allow_next_syslog();
        LOG_INFO("table is deleted",K(ret), K(table_id), K(schema_version), K(save_schema_version));
      } else {}
    }
    if (OB_SUCC(ret)) {
      while (retry_time < MAX_RETRY_TIMES) {
        bool is_dropped = false;
        if (OB_FAIL(check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
          if (OB_NOT_INIT != ret) {
            LOG_WARN("check if tenant has been dropped failed", K(ret), K(table_id));
          }
        } else if (is_dropped) {
          ret = OB_TENANT_HAS_BEEN_DROPPED;
          LOG_WARN("tenant has been deleted, just skip", K(ret), K(tenant_id));
        } else if (OB_FAIL(get_tenant_schema_guard(
                           tenant_id, schema_guard, save_schema_version))) {
          if (OB_SCHEMA_EAGAIN != ret) {
            LOG_WARN("fail to get tenant schema guard",
                     K(ret), K(table_id), K(schema_version), K(save_schema_version));
          }
        }
        if (OB_SCHEMA_EAGAIN != ret && OB_NOT_INIT != ret) {
          break;
        } else {
          ob_usleep(RETRY_INTERVAL_US);
          ++retry_time;
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to get tenant schema guard",
                 K(ret), K(table_id), K(schema_version), K(save_schema_version));
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to get tenant schema guard",
                 K(ret), K(table_id), K(schema_version), K(save_schema_version));
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
        LOG_WARN("failed to get table schema",
                 K(ret), K(tenant_id), K(table_id), K(schema_version), K(save_schema_version));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("table should exist",
                 K(ret), K(table_id), K(schema_version), K(save_schema_version));
      }
    }
  }
  if (OB_SUCC(ret)) {
    schema_guard.schema_guard_type_ = ObSchemaGetterGuard::TABLE_SCHEMA_GUARD;
  }
  return ret;
}

ObMultiVersionSchemaService::ObMultiVersionSchemaService() :
    init_(false),
    schema_refresh_mutex_(common::ObLatchIds::REFRESH_SCHEMA_LOCK),
    schema_cache_(),
    schema_mgr_cache_(),
    schema_mgr_cache_for_liboblog_(),
    schema_fetcher_(),
    schema_info_rwlock_(common::ObLatchIds::REFRESHED_SCHEMA_CACHE_LOCK),
    last_refreshed_schema_info_(),
    init_version_cnt_(OB_INVALID_COUNT),
    init_version_cnt_for_liboblog_(OB_INVALID_COUNT),
    schema_store_map_()
{
}

ObMultiVersionSchemaService::~ObMultiVersionSchemaService()
{
  destroy();
}

void ObMultiVersionSchemaService::stop()
{
  ddl_trans_controller_.stop();
}

void ObMultiVersionSchemaService::wait()
{
  ddl_trans_controller_.wait();
}

//FIXME: Will there be a memory leak
int ObMultiVersionSchemaService::destroy()
{
  int ret = OB_SUCCESS;
  schema_store_map_.destroy();
  ddl_trans_controller_.destroy();
  schema_cache_.destroy();
  return ret;
}

ObMultiVersionSchemaService &ObMultiVersionSchemaService::get_instance()
{
  static ObMultiVersionSchemaService THE_ONE;
  return THE_ONE;
}

// init in main thread
int ObMultiVersionSchemaService::init(
    ObMySQLProxy *sql_proxy,
    ObDbLinkProxy *dblink_proxy,
    const ObCommonConfig *config,
    const int64_t init_version_count,
    const int64_t init_version_count_for_liboblog)
{
  int ret = OB_SUCCESS;

  if (true == init_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init schema manager twice, ", K(ret));
  } else if (OB_FAIL(ObServerSchemaService::init(sql_proxy, dblink_proxy, config))) {
    LOG_WARN("failed to init base class ObServerSchemaService,", K(ret));
  } else if (OB_FAIL(schema_fetcher_.init(schema_service_, sql_proxy))) {
    LOG_WARN("fail to init schema cache", K(ret));
  } else if (OB_FAIL(schema_cache_.init())) {
    LOG_WARN("fail to init schema cache", K(ret));
  } else if (OB_FAIL(schema_mgr_cache_.init(init_version_count, ObSchemaMgrCache::REFRESH))) {
    LOG_WARN("fail to init schema mgr cache", K(ret));
  } else if (OB_FAIL(schema_mgr_cache_for_liboblog_.init(init_version_count_for_liboblog,
                                                         ObSchemaMgrCache::FALLBACK))) {
    LOG_WARN("fail to init schema mgr cache", K(ret));
  } else if (OB_FAIL(ddl_trans_controller_.init(this))) {
    LOG_WARN("fail to init ddl trans controller", KR(ret));
  } else if (OB_FAIL(ddl_epoch_mgr_.init(sql_proxy, this))) {
    LOG_WARN("fail to init ddl epoch mgr", KR(ret));
  } else {
    // init sys schema struct
    init_version_cnt_ = init_version_count;
    init_version_cnt_for_liboblog_ = init_version_count_for_liboblog;
    if (OB_FAIL(schema_store_map_.init(TENANT_MAP_BUCKET_NUM))) {
      LOG_WARN("fail to init schema store map", K(ret));
    } else if (OB_FAIL(init_multi_version_schema_struct(OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to init multi version schema struct", K(ret));
    } else if (OB_FAIL(init_sys_tenant_user_schema())) {
      LOG_WARN("fail to init schema cache", K(ret));
    } else if (OB_FAIL(init_original_schema())) {
      LOG_WARN("failed to init original schema", K(ret));
    }
  }

  return ret;
}

bool ObMultiVersionSchemaService::check_inner_stat() const
{
  bool ret = true;
  if (!ObServerSchemaService::check_inner_stat() || !init_) {
    ret = false;
    LOG_WARN("inner stat error", K(init_));
  }
  return ret;
}

// add sys_tenant/user schema to cache, just for bootstrap
// 1. before bootstrap : 'alter system bootstrap' (obmp_connect will fetch user_info)
// 2. during bootstrap : 'create_partition' will fetch tenant_schema
int ObMultiVersionSchemaService::init_sys_tenant_user_schema()
{
  int ret = OB_SUCCESS;

  ObTenantSchema sys_tenant;
  ObSysVariableSchema sys_variable;
  ObUserInfo sys_user;

  sys_tenant.set_tenant_id(OB_SYS_TENANT_ID);
  sys_tenant.set_schema_version(OB_CORE_SCHEMA_VERSION);

  sys_user.set_tenant_id(OB_SYS_TENANT_ID);
  sys_user.set_user_id(OB_SYS_USER_ID);
  sys_user.set_priv_set(OB_PRIV_ALL | OB_PRIV_GRANT | OB_PRIV_BOOTSTRAP);
  sys_user.set_schema_version(OB_CORE_SCHEMA_VERSION);

  sys_variable.set_tenant_id(OB_SYS_TENANT_ID);
  sys_variable.set_schema_version(OB_CORE_SCHEMA_VERSION);
  sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);

  if (OB_FAIL(sys_variable.load_default_system_variable(true))) {
    LOG_WARN("load sys tenant default system variable failed", K(ret));
  } else if (OB_FAIL(sys_tenant.set_tenant_name(OB_SYS_TENANT_NAME))) {
    LOG_WARN("Set sys tenant name error", K(ret));
  } else if (OB_FAIL(sys_user.set_user_name(OB_SYS_USER_NAME))){
    LOG_WARN("Set user name error", K(ret));
  } else if (OB_FAIL(sys_user.set_host(OB_SYS_HOST_NAME))){
    LOG_WARN("Set host name error", K(ret));
  } else if (OB_FAIL(schema_cache_.put_schema(TENANT_SCHEMA,
                                              OB_SYS_TENANT_ID,
                                              sys_tenant.get_tenant_id(),
                                              sys_tenant.get_schema_version(),
                                              sys_tenant))) {
    LOG_WARN("put schema failed", K(ret));
  } else if (OB_FAIL(schema_cache_.put_schema(USER_SCHEMA,
                                              OB_SYS_TENANT_ID,
                                              sys_user.get_user_id(),
                                              sys_user.get_schema_version(),
                                              sys_user))) {
    LOG_WARN("put schema failed", K(ret));
  } else if (OB_FAIL(schema_cache_.put_schema(SYS_VARIABLE_SCHEMA,
                                              OB_SYS_TENANT_ID,
                                              sys_variable.get_tenant_id(),
                                              sys_variable.get_schema_version(),
                                              sys_variable))) {
    LOG_WARN("put schema failed", K(ret));
  } else {}

  return ret;
}

int ObMultiVersionSchemaService::broadcast_tenant_schema(
    const uint64_t tenant_id,
    const common::ObIArray<share::schema::ObTableSchema> &table_schemas)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(schema_refresh_mutex_);
  FOREACH_CNT_X(table_schema, table_schemas, OB_SUCC(ret)) {
    if (OB_FAIL(schema_cache_.put_schema(
                TABLE_SCHEMA,
                tenant_id,
                table_schema->get_table_id(),
                table_schema->get_schema_version(),
                *table_schema))) {
      LOG_WARN("put sys schema to schema cache failed",
               KR(ret), K(tenant_id), KPC(table_schema));
    } else {
      LOG_INFO("add sys table schema", KR(ret), K(tenant_id), KPC(table_schema));
    }
  }
  auto attr = SET_USE_500("BroFullSchema", ObCtxIds::SCHEMA_SERVICE);
  ObArenaAllocator allocator(attr);
  ObArray<ObSimpleTableSchemaV2*> simple_table_schemas(
                  common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                  common::ModulePageAllocator(allocator));
  ObSchemaMgr *schema_mgr_for_cache = NULL;
  const bool refresh_full_schema = true;
  if (FAILEDx(convert_to_simple_schema(allocator, table_schemas, simple_table_schemas))) {
    LOG_WARN("failed to convert", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(
             tenant_id, schema_mgr_for_cache))) {
    LOG_WARN("fail to get schema mgr for cache", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_mgr_for_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_mgr_for_cache->add_tables(simple_table_schemas, refresh_full_schema))) {
    LOG_WARN("failed to add tables", KR(ret), K(tenant_id));
  } else if (FALSE_IT(schema_mgr_for_cache->set_schema_version(
             OB_CORE_SCHEMA_VERSION + 1))) {
  } else if (OB_FAIL(add_schema(tenant_id, false))) {
    LOG_WARN("fail to publish initial schema", KR(ret), K(tenant_id));
  } else {
    LOG_INFO("broadcast tenant schema", KR(ret), K(tenant_id));
  }
  return ret;
}

// check table exist
// table_schema_version: Indicates the schema_version corresponding to table_schema
// 1) OB_INVALID_VERSION, Indicates to take the latest version of the local guard
//  (not the latest version of the internal table)
// 2) table_schema_version > local refreshed schema_version, Indicates that the local schema is behind,
//  and a special error code is returned
// 3) table_schema_version <= local refreshed schema_version: Indicates that the local schema is new enough,
//  take the latest version of the local guard for judgment
int ObMultiVersionSchemaService::check_table_exist(
  const uint64_t tenant_id,
  const uint64_t database_id,
  const ObString &table_name,
  const bool is_index,
  const int64_t table_schema_version,
  bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    int64_t local_version = OB_INVALID_VERSION;
    if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited, ", K(ret), K(database_id), K(table_name));
    } else if (table_schema_version >= 0 && OB_FAIL(get_tenant_refreshed_schema_version(tenant_id, local_version))) {
      LOG_WARN("fail to get local schema version", K(ret), K(tenant_id), K(table_name), K(table_schema_version));
    } else if (table_schema_version > local_version) {
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("local schema is old, try again", K(ret), K(tenant_id), K(table_name), K(table_schema_version));
    } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret), K(database_id), K(table_name));
    } else if (OB_SUCCESS
        != (ret = schema_guard.check_table_exist(tenant_id,
            database_id,
            table_name,
            is_index,
            ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
            exist))) {
      LOG_WARN("failed to check table exist, ", K(ret));
    }
  }
  return ret;
}

// table_schema_version: schema_version of table_schema
// 1) OB_INVALID_VERSION, Indicates to take the latest version of the local guard
//  (not the latest version of the internal table)
// 2) table_schema_version > local refreshed schema_version, Indicates that the local schema is behind,
//  and a special error code is returned
// 3) table_schema_version <= local refreshed schema_version: Indicates that the local schema is new enough,
//  take the latest version of the local guard for judgment
int ObMultiVersionSchemaService::check_table_exist(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t table_schema_version,
    bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    int64_t local_version = OB_INVALID_VERSION;
    if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited,", K(ret), K(table_id));
    } else if (table_schema_version >= 0 && OB_FAIL(get_tenant_refreshed_schema_version(tenant_id, local_version))) {
      LOG_WARN("fail to get local schema version", K(ret), K(tenant_id), K(table_id), K(table_schema_version));
    } else if (table_schema_version > local_version) {
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("local schema is old, try again", K(ret), K(tenant_id), K(table_id), K(table_schema_version));
    } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret), K(table_id));
    } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id, table_id, exist))) {
      LOG_WARN("failed to check table exist,", K(tenant_id), K(table_id), K(ret));
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::check_database_exist(
  const uint64_t tenant_id,
  const ObString &database_name,
  uint64_t &database_id,
  bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited, ", K(ret));
    } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret));
    } else if (OB_FAIL(schema_guard.check_database_exist(tenant_id,
                                                         database_name,
                                                         exist,
                                                         &database_id))) {
      LOG_WARN(
          "failed to check database exist, ",
          K(tenant_id),
          "tablegroup_name",
          to_cstring(database_name),
          K(ret));
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::check_tablegroup_exist(
  const uint64_t tenant_id,
  const ObString &tablegroup_name,
  uint64_t &tablegroup_id,
  bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited,", K(ret));
    } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret));
    } else if (OB_FAIL(schema_guard.check_tablegroup_exist(tenant_id,
                                                           tablegroup_name,
                                                           exist,
                                                           &tablegroup_id))) {
      LOG_WARN(
          "failed to check tablegroup exist, ",
          K(tenant_id),
          "tablegroup_name",
          to_cstring(tablegroup_name),
          K(ret));
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::check_if_tenant_has_been_dropped(
    const uint64_t tenant_id,
    bool &is_dropped)
{
  int ret = OB_SUCCESS;
  is_dropped = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    if (!is_tenant_full_schema(OB_SYS_TENANT_ID)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited,", K(ret), K(tenant_id));
    } else if (OB_FAIL(get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
      LOG_WARN("failed to check if tenant has been dropped", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::check_if_tenant_schema_has_been_refreshed(
    const uint64_t tenant_id,
    bool &is_refreshed)
{
  int ret = OB_SUCCESS;
  is_refreshed = false;
  bool sys_schema_not_full = false;
  bool tenant_schema_not_full = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(refresh_full_schema_map_.get_refactored(OB_SYS_TENANT_ID, sys_schema_not_full))) {
  } else if (sys_schema_not_full) {
    // observer may be not start service
    is_refreshed = false;
  } else if (OB_FAIL(refresh_full_schema_map_.get_refactored(tenant_id, tenant_schema_not_full))) {
  } else if (tenant_schema_not_full) {
    is_refreshed = false;
  } else {
    // tenant's schema is full
    is_refreshed = true;
  }

  LOG_TRACE("check if tenant schema has been refreshed", KR(ret), K(sys_schema_not_full), K(tenant_schema_not_full), K(is_refreshed), K(tenant_id));
  return ret;
}

int ObMultiVersionSchemaService::check_is_creating_standby_tenant(
    const uint64_t tenant_id,
    bool &is_creating_standby)
{
  int ret = OB_SUCCESS;
  is_creating_standby = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    if (!is_tenant_full_schema(OB_SYS_TENANT_ID)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited,", KR(ret), K(tenant_id));
    } else if (OB_FAIL(get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("get schema guard failed ", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.check_is_creating_standby_tenant(tenant_id, is_creating_standby))) {
      LOG_WARN("failed to check if it is a creating standby tenant", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::init_original_schema()
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(schema_manager_rwlock_);
  const bool force_add = true;
  if (OB_FAIL(add_schema(OB_SYS_TENANT_ID, force_add))) {
    LOG_WARN("fail to add schema_manager_for_cache into snapshot", K(ret));
  } else {
    init_ = true;
  }
  return ret;
}

// schema version must incremental
int ObMultiVersionSchemaService::add_schema(
    const uint64_t tenant_id,
    const bool force_add)
{
  int ret = OB_SUCCESS;

  ObSchemaMgr *schema_mgr_for_cache = NULL;
  ObSchemaMemMgr *mem_mgr = NULL;
  ObSchemaMgrCache *schema_mgr_cache = NULL;
  int64_t new_schema_version = OB_INVALID_VERSION;
  int64_t refreshed_schema_version = OB_INVALID_VERSION;
  int64_t received_broadcast_version = OB_INVALID_VERSION;
  ObSchemaStore* schema_store = NULL;
  const int64_t start_time = ObTimeUtility::current_time();
  if (!force_add && !check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_store = schema_store_map_.get(tenant_id))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to get schema_store", K(ret));
  } else if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
    LOG_WARN("fail to get schema mgr for cache", K(ret), K(ret));
  } else if (OB_ISNULL(schema_mgr_for_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr is null", K(ret), K(tenant_id));
  } else if (OB_FAIL(mem_mgr_map_.get_refactored(tenant_id, mem_mgr))) {
    LOG_WARN("fail to get mem mgr", K(ret), K(tenant_id));
  } else if (OB_ISNULL(mem_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_mgr is null", K(ret), K(tenant_id));
  } else {
    schema_mgr_cache = &schema_store->schema_mgr_cache_;
    new_schema_version = schema_mgr_for_cache->get_schema_version();
    refreshed_schema_version = schema_store->get_refreshed_version();
    if (OB_ISNULL(schema_mgr_cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_mgr_cache is null", K(ret), K(tenant_id));
    } else if (refreshed_schema_version > new_schema_version) {
      LOG_WARN("add schema is old",
               K(refreshed_schema_version),
               K(new_schema_version),
               K(received_broadcast_version));
    }
    FLOG_INFO("add schema", K(tenant_id), K(refreshed_schema_version), K(new_schema_version));

    bool is_exist = false;
    if (FAILEDx(schema_mgr_cache->check_schema_mgr_exist(new_schema_version, is_exist))) {
      LOG_WARN("fail to check schema_mgr exist", K(ret), K(tenant_id), K(new_schema_version));
    } else if (is_exist) {
      LOG_INFO("schema mgr already exist, just skip", K(ret), K(tenant_id), K(new_schema_version));
    } else if (OB_FAIL(alloc_and_put_schema_mgr_(*mem_mgr, *schema_mgr_for_cache, *schema_mgr_cache))) {
      LOG_WARN("fail to alloc and put schema mgr", KR(ret));
    }
    // try switch allocator
    if (OB_SUCC(ret)) {
      bool can_switch = false;
      int64_t max_schema_slot_num = OB_MAX_VERSION_COUNT;
      omt::ObTenantConfigGuard tenant_config(OTC_MGR.get_tenant_config_with_lock(tenant_id));
      if (tenant_config.is_valid()) {
        max_schema_slot_num = tenant_config->_max_schema_slot_num;
      }
      const int64_t switch_cnt = ObSchemaService::g_liboblog_mode_ ? init_version_cnt_ : max_schema_slot_num;
      if (OB_FAIL(mem_mgr->check_can_switch_allocator(switch_cnt, can_switch))) {
        LOG_WARN("fail to check can switch allocator", KR(ret));
      } else if (can_switch) {
        // Switch allocator && rewrite schema_mgr_for_cache_
        if (OB_FAIL(switch_allocator_(*mem_mgr, schema_mgr_for_cache))) {
          LOG_WARN("fail to alloc schema mgr after switch allocator", KR(ret), K(tenant_id), K(new_schema_version));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // Because RS only notifies other observers through RPC, the received_broadcast_version of the local observer
      // is not updated
      // This variable will be copied in obmp_query to ob_latest_schema_version in the session variable
      // The proxy will use the variable ob_latest_schema_version to ensure that
      // multiple observers are connected to the same schema version.
      schema_store->update_refreshed_version(new_schema_version);
      FLOG_INFO("[REFRESH_SCHEMA] change refreshed_schema_version with new mode", K(tenant_id), K(new_schema_version));
      // To reduce allocator's memory more frequently
      if (OB_FAIL(try_gc_allocator_when_add_schema_(tenant_id, mem_mgr, schema_mgr_cache))) {
        LOG_WARN("fail to gc current allocator when add schema", KR(ret), K(tenant_id), K(new_schema_version));
      }
    }
    int64_t end_time = ObTimeUtility::current_time();
    LOG_INFO("finish add schema", KR(ret), K(tenant_id), K(new_schema_version), "cost_ts", start_time - end_time);
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_PUT_SCHEMA);
ERRSIM_POINT_DEF(ERRSIM_ASSIGN_NEW_MGR);
int ObMultiVersionSchemaService::alloc_and_put_schema_mgr_(
    ObSchemaMemMgr &mem_mgr,
    ObSchemaMgr &latest_schema_mgr,
    ObSchemaMgrCache &schema_mgr_cache)
{
  int ret = OB_SUCCESS;
  ObSchemaMgr *new_mgr = NULL;
  bool alloc_for_liboblog = false;
  ObSchemaMgr *eli_schema_mgr = NULL;
  const uint64_t tenant_id = latest_schema_mgr.get_tenant_id();
  const int64_t schema_version = latest_schema_mgr.get_schema_version();
  if (OB_FAIL(mem_mgr.alloc_schema_mgr(new_mgr, alloc_for_liboblog))) {
    LOG_WARN("fail to alloc mem", KR(ret));
  } else {
    if (OB_ISNULL(new_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new_mgr is NULL", KR(ret), K(tenant_id), K(schema_version));
    } else if (OB_FAIL(new_mgr->init())) {
      LOG_WARN("init schema mgr falied", KR(ret));
    } else if (OB_UNLIKELY(ERRSIM_ASSIGN_NEW_MGR)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("turn on error injection ERRSIM_ASSIGN_NEW_MGR", KR(ret));
    } else if (OB_FAIL(new_mgr->assign(latest_schema_mgr))) {
      LOG_WARN("assign schema mgr falied", KR(ret));
    } else if (OB_UNLIKELY(ERRSIM_PUT_SCHEMA)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("turn on error injection ERRSIM_PUT_SCHEMA", KR(ret));
    } else if (OB_FAIL(schema_mgr_cache.put(new_mgr, eli_schema_mgr))) {
      LOG_WARN("put schema mgr failed", KR(ret));
    } else {
      LOG_INFO("put schema mgr succeed",
                "schema_version", new_mgr->get_schema_version(),
                "eliminated_schema_version", NULL != eli_schema_mgr ?
                  eli_schema_mgr->get_schema_version() : OB_INVALID_VERSION, K(tenant_id));
    }
    int tmp_ret = OB_SUCCESS;
    // whatever put success or put failed, we should try to free eli_schema_mgr
    if (OB_TMP_FAIL(mem_mgr.free_schema_mgr(eli_schema_mgr))) {
      LOG_ERROR("fail to free eli_schema_mgr", KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    // whatever assign/put/free schema mgr failed, new schema mgr will be useless, so free it
    if (OB_FAIL(ret)) {
      LOG_WARN("handle new schema mgr failed", KR(ret), K(schema_version), K(tenant_id));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(mem_mgr.free_schema_mgr(new_mgr))) {
        LOG_ERROR("fail to free new_mgr", KR(tmp_ret));
      }
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_SET_REFACTOR);
ERRSIM_POINT_DEF(ERRSIM_AFTER_SET_REFACTOR);
int ObMultiVersionSchemaService::switch_allocator_(
    ObSchemaMemMgr &mem_mgr,
    ObSchemaMgr *&latest_schema_mgr)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();

  if (OB_ISNULL(latest_schema_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("latest schema mgr is NULL", KR(ret));
  } else if (OB_FAIL(mem_mgr.switch_allocator())) {
    LOG_WARN("switch allocator falied", KR(ret));
  } else {
    bool overwrite = true;
    bool need_switch_back = true;
    bool alloc_for_liboblog = false;
    ObSchemaMgr *new_mgr = NULL;
    ObSchemaMgr *old_mgr = latest_schema_mgr;
    const uint64_t tenant_id = latest_schema_mgr->get_tenant_id();
    const int64_t schema_version = latest_schema_mgr->get_schema_version();
    LOG_INFO("try to switch allocator", KR(ret), K(tenant_id), K(schema_version));

    if (OB_FAIL(mem_mgr.alloc_schema_mgr(new_mgr, alloc_for_liboblog))) {
      LOG_WARN("fail to alloc mem", KR(ret));
    } else {
      if (OB_ISNULL(new_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new mgr is NULL", KR(ret), K(tenant_id), K(schema_version));
      } else if (OB_FAIL(new_mgr->init())) {
        LOG_WARN("init new schema mgr falied", K(ret));
      } else if (OB_FAIL(new_mgr->deep_copy(*old_mgr))) {
        LOG_WARN("deep copy old schema mgr falied", K(ret));
      } else if (OB_UNLIKELY(ERRSIM_SET_REFACTOR)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("turn on error injection ERRSIM_SET_REFACTOR", KR(ret));
      } else if (OB_FAIL(schema_mgr_for_cache_map_.set_refactored(tenant_id, new_mgr, overwrite))) {
        LOG_WARN("fail to set schema mgr for cache", K(ret));
      } else {
        // handle new schema mgr success, no need to switch back allocator
        need_switch_back = false;
        latest_schema_mgr = new_mgr;
        if (OB_UNLIKELY(ERRSIM_AFTER_SET_REFACTOR)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("turn on error injection ERRSIM_AFTER_SET_REFACTOR", KR(ret));
        } else if (OB_FAIL(mem_mgr.free_schema_mgr(old_mgr))) {
          // old mgr will be release in try_gc_another_allocator
          LOG_ERROR("fail to free old schema mgr", KR(ret));
        }
      }
    }
    // switch back allocator when cur allocator can not use
    // 1.alloc new schema mgr failed
    // 2.handle new schema failed, like deep copy
    if (need_switch_back) {
      LOG_WARN("after switch allocator, handle schema mgr encounters something wrong", KR(ret), K(tenant_id), K(schema_version));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(mem_mgr.switch_back_allocator())) {
        LOG_ERROR("switch back allocator falied", KR(tmp_ret));
      } else if (OB_TMP_FAIL(mem_mgr.free_schema_mgr(new_mgr))) {
        LOG_ERROR("fail to free new_mgr", KR(tmp_ret), K(schema_version));
      }
    }
    int64_t end_time = ObTimeUtility::current_time();
    LOG_INFO("finish switch allocator", KR(ret), K(tenant_id), K(schema_version), "cost_ts", end_time - start_time);
  }
  return ret;
}

int ObMultiVersionSchemaService::async_refresh_schema(
    const uint64_t tenant_id,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  int64_t local_schema_version = OB_INVALID_VERSION;
  bool check_formal = ObSchemaService::is_formal_version(schema_version);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(schema_version));
  } else if (OB_FAIL(get_tenant_refreshed_schema_version(
                     tenant_id, local_schema_version))) {
    LOG_WARN("fail to get tenant refreshed schema version",
             KR(ret), K(tenant_id), K(schema_version));
  } else if (local_schema_version >= schema_version
             && (!check_formal || ObSchemaService::is_formal_version(local_schema_version))) {
    // do nothing
  } else {
    int64_t retry_cnt = 0;
    const __useconds_t RETRY_IDLE_TIME = 10 * 1000L; // 10ms
    const int64_t MAX_RETRY_CNT = 100 * 1000 * 1000L / RETRY_IDLE_TIME; // 100s at most
    const int64_t SUBMIT_TASK_FREQUENCE = 2 * 1000 * 1000L / RETRY_IDLE_TIME; // each 2s
    while (OB_SUCC(ret)) {
      if (THIS_WORKER.is_timeout()
          || (INT64_MAX == THIS_WORKER.get_timeout_ts() && retry_cnt >= MAX_RETRY_CNT)) {
        ret = OB_TIMEOUT;
        LOG_WARN("already timeout", KR(ret), K(tenant_id), K(schema_version));
      } else if (OB_FAIL(get_tenant_refreshed_schema_version(
                         tenant_id, local_schema_version))) {
        LOG_WARN("fail to get tenant refreshed schema version",
                 KR(ret), K(tenant_id), K(schema_version));
      } else if (local_schema_version >= schema_version
                 && (!check_formal || ObSchemaService::is_formal_version(local_schema_version))) {
        // success
        break;
      } else {
        if (0 == retry_cnt % SUBMIT_TASK_FREQUENCE) {
          {
            bool is_dropped = false;
            ObSchemaGetterGuard guard;
            if (OB_FAIL(get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
              LOG_WARN("fail to get schema guard", KR(ret));
            } else if (OB_FAIL(guard.check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
              LOG_WARN("fail to check if tenant has been dropped", KR(ret), K(tenant_id));
            } else if (is_dropped) {
              ret = OB_TENANT_HAS_BEEN_DROPPED;
              LOG_WARN("tenant has been dropped", KR(ret), K(tenant_id));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(GCTX.ob_service_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("observice is null", K(ret));
          } else if (OB_FAIL(GCTX.ob_service_->submit_async_refresh_schema_task(
                             tenant_id, schema_version))) {
            if (OB_EAGAIN == ret || OB_SIZE_OVERFLOW == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to submit async refresh schema task",
                       KR(ret), K(tenant_id), K(schema_version));
            }
          }
        }
        if (OB_SUCC(ret)) {
          retry_cnt++;
          ob_usleep(RETRY_IDLE_TIME);
        }
      }
    }
  }
  return ret;
}


/*
 * 1. If tenant_id is OB_INVALID_TENANT_ID, it means refresh the schema of all tenants,
 *  otherwise only the schema of the corresponding tenant will be refreshed.
 * 2. The new schema_version broadcast mechanism does not support the read-only zone scheme.
 * 3. When refreshing the schema, you must first obtain the schema_version version
 *  that each tenant wants to refresh from the internal table:
 *  1) For system tenants of the primary cluster and standalone cluster, the schema refresh is strengthened
 *    consistent read, and the max_schema_version is obtained directly from the corresponding __all_ddl_operation.
 *  2) For user tenants of the standalone cluster, a consistent read is required to obtain
 *    the max_schema_version of the corresponding tenant from __all_core_table.
 * 4. If a tenant fails to update to the specified version, an error will be reported and the external logic will try again
 */
int ObMultiVersionSchemaService::refresh_and_add_schema(const ObIArray<uint64_t> &tenant_ids,
                                                        bool check_bootstrap/* = false*/)
{
  FLOG_INFO("[REFRESH_SCHEMA] start to refresh and add schema", K(tenant_ids));
  const int64_t start = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  bool is_standby_cluster = GCTX.is_standby_cluster();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    lib::ObMutexGuard guard(schema_refresh_mutex_);
    auto func = [&]() {
      ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
      // This is just to reduce SQL calls, the error code can be ignored
      int tmp_ret = OB_SUCCESS;
      bool restore_tenant_exist = false;
      if (ObSchemaService::g_liboblog_mode_) {
        // Avoid using schema_status_proxy for agentserver and liboblog
        restore_tenant_exist = false;
      } else if (OB_SUCCESS != (tmp_ret = check_restore_tenant_exist(tenant_ids, restore_tenant_exist))) {
        LOG_WARN("fail to check restore tenant exist", K(ret), K(tmp_ret), K(tenant_ids));
        restore_tenant_exist = true;
      }
      if (is_standby_cluster || restore_tenant_exist) {
        if (OB_ISNULL(schema_status_proxy)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema_status_proxy is null", K(ret));
        } else if (OB_FAIL(schema_status_proxy->load_refresh_schema_status())) {
          LOG_WARN("fail to load refresh schema status", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (check_bootstrap) {
        // The schema refresh triggered by the heartbeat is forbidden in the bootstrap phase,
        // and it needs to be judged in the schema_refresh_mutex_lock
        //
        int64_t baseline_schema_version = OB_INVALID_VERSION;
        if (OB_FAIL(get_baseline_schema_version(OB_SYS_TENANT_ID, true/*auto_update*/, baseline_schema_version))) {
          LOG_WARN("fail to get baseline_schema_version", K(ret));
        } else if (baseline_schema_version < 0) {
          // still in bootstrap phase, refresh schema is not allowed
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("refresh schema in bootstrap phase is not allowed", K(ret));
        }
      }

      // Ensure that the memory on the stack requested during the refresh schema process also uses the default 500 tenant
      ObArenaAllocator allocator(ObModIds::OB_MODULE_PAGE_ALLOCATOR, OB_MALLOC_BIG_BLOCK_SIZE, OB_SERVER_TENANT_ID);
      ObSchemaStackAllocatorGuard guard(&allocator);

      ObArray<uint64_t> all_tenant_ids;
      if (OB_FAIL(ret)) {
      } else if (0 == tenant_ids.count()) {
        // refresh all tenant schema
        ObSchemaMgr *schema_mgr = NULL;
        if (OB_FAIL(refresh_tenant_schema(OB_SYS_TENANT_ID))) {
          LOG_WARN("fail to refresh sys schema", K(ret), K(all_tenant_ids));
        } else if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(OB_SYS_TENANT_ID, schema_mgr))) {
          LOG_WARN("fail to get sys schema mgr for cache", K(ret));
        } else if (OB_ISNULL(schema_mgr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema_mgr is null", K(ret));
        } else if (OB_FAIL(schema_mgr->get_tenant_ids(all_tenant_ids))) {
          LOG_WARN("fail to get all tenant_ids", K(ret));
        } else {
          // Ignore that some tenants fail to refresh the schema,
          // and need to report an error to the upper layer to avoid pushing up last_refresh_schema_info
          int tmp_ret = OB_SUCCESS;
          for (int64_t i = 0; i < all_tenant_ids.count(); i++) {
            const uint64_t tenant_id = all_tenant_ids.at(i);
            if (OB_SYS_TENANT_ID == tenant_id) {
              // skip
            } else if (OB_SUCCESS != (tmp_ret = refresh_tenant_schema(tenant_id))) {
              LOG_WARN("fail to refresh tenant schema", K(tmp_ret), K(tenant_id));
            }
            if (OB_SUCCESS != tmp_ret && OB_SUCCESS == ret) {
              ret = tmp_ret;
            }
          }
        }
      } else {
        // Ignore that some tenants fail to refresh the schema,
        // and need to report an error to the upper layer to avoid pushing up last_refresh_schema_info
        int tmp_ret = OB_SUCCESS;
        for (int64_t i = 0; i < tenant_ids.count(); i++) {
          const uint64_t tenant_id = tenant_ids.at(i);
          if (OB_INVALID_TENANT_ID == tenant_id) {
            tmp_ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid tenant_id", K(tmp_ret), K(tenant_id));
          } else if (OB_SUCCESS != (tmp_ret = refresh_tenant_schema(tenant_id))) {
            LOG_WARN("fail to refresh tenant schema", K(tmp_ret), K(tenant_id));
          }
          if (OB_SUCCESS != tmp_ret && OB_SUCCESS == ret) {
            ret = tmp_ret;
          }
        }
      }
    };
    CREATE_WITH_TEMP_ENTITY_P(!ObSchemaService::g_liboblog_mode_, RESOURCE_OWNER, common::OB_SERVER_TENANT_ID)
    {
      func();
    } else {
      // Two aspects are considered, one is that there is no omt module in one side,
      // and the other is that the tenant has not been loaded during the omt startup phase.
      func();
    }
  }
  FLOG_INFO("[REFRESH_SCHEMA] end refresh and add schema", KR(ret), K(tenant_ids),
            "cost", ObTimeUtility::current_time() - start);
  return ret;
}

// It is used to determine the initial Partition set when liboblog starts, and obtain the maximum schema_version
// that meets the requirements of <=timestamp. The schema_version should be as large as possible;
// it is also used for schema history recycle
// 1. Bootstrap non-split mode or tenant has no new ddl after upgrade split: return max (schema_version),
//  compatible with non-split mode
// 2. Bootstrap split mode or tenant has new ddl after upgrade split:
//  Return the schema_version corresponding to the largest transaction boundary
int ObMultiVersionSchemaService::get_schema_version_by_timestamp(
    const ObRefreshSchemaStatus &schema_status,
    const uint64_t tenant_id,
    int64_t timestamp,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id || timestamp <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id), K(timestamp));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy or schema_service is null", K(ret), KP(sql_proxy_), KP(schema_service_));
  } else if (OB_FAIL(schema_service_->get_schema_version_by_timestamp(
                     *sql_proxy_, schema_status, tenant_id, timestamp, schema_version))) {
    LOG_WARN("fail to get schema_version by timestamp", K(ret), K(tenant_id), K(timestamp));
  }
  LOG_INFO("[REFRESH_SCHEMA] get_schema_version_by_timestamp", K(ret), K(tenant_id), K(timestamp), K(schema_version));
  return ret;
}

// Used to determine the tenant's starting schema_version during the operation of liboblog.
// Unlike the startup time, liboblog assumes that the Partition set of the tenant's initial user table is empty.
// In order to ensure that the ddl is not output and the Partition is not obtained,
// the schema_version needs to be as small as possible.
//
// Discuss several situations:
// 1. OB_DDL_ADD_TENANT: Indicates a tenant created in a non-split mode,
//  and the schema_version can be the schema_version corresponding to OB_DDL_ADD_TENANT;
// 2. OB_DDL_ADD_TENANT_START: liboblog does not think that the tenant is created successfully,
//  only outputs DDL without actual operation;
// 3. OB_DDL_ADD_TENANT_END: liboblog determines its working mode based on whether ob is in split mode at the start time
//  and whether OB_DDL_FINISH_SCHEMA_SPLIT has been processed(Can be inconsistent with the working mode of the schema module)
//   3.1) 2.2 Tenants created in non-split mode: take the transaction boundary of transaction two,
//    but at this time __all_ddl_operation is not split, and it is difficult to determine the transaction boundary.
//   3.2) 2.2 Tenants created by split mode: take the transaction boundary of transaction two,
//    which is equivalent to the first transaction boundary of the tenant __all_ddl_operation.
// In summary, this interface is only used in the 3.2) scenario, and the rest of the scenarios are handled by liboblog.
int ObMultiVersionSchemaService::get_first_trans_end_schema_version(
    const uint64_t tenant_id,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;
  if (!ObSchemaService::g_liboblog_mode_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("only work for liboblog", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy or schema_service is null", K(ret), KP(sql_proxy_), KP(schema_service_));
  } else if (OB_FAIL(schema_service_->get_first_trans_end_schema_version(
                     *sql_proxy_, tenant_id, schema_version))) {
    LOG_WARN("fail to get first trans end schema_version", K(ret), K(tenant_id));
  }
  LOG_INFO("[REFRESH_SCHEMA] get_first_trans_end_schema_version", K(ret), K(tenant_id), K(schema_version));
  return ret;
}

// Only for liboblog, it will automatically switch the working mode of the schema module (old mode -> new mode)
// tenant_id is OB_INVALID_TENANT_ID, which means refreshing the schema of all tenants
int ObMultiVersionSchemaService::auto_switch_mode_and_refresh_schema(
    const uint64_t tenant_id,
    const int64_t expected_schema_version /* = OB_INVALID_SCHEMA_VERSION */ )
{
  LOG_INFO("[REFRESH_SCHEMA] start to refresh and add schema", K(tenant_id));
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (!ObSchemaService::g_liboblog_mode_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("only work for liboblog", K(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy or schema_service_impl is null", K(ret), KP(sql_proxy_), KP(schema_service_));
  } else {
    lib::ObMutexGuard guard(schema_refresh_mutex_);
    auto func = [&]() {
      // Ensure that the memory on the stack requested during the refresh schema process also uses the default 500 tenant
      ObArenaAllocator allocator(ObModIds::OB_MODULE_PAGE_ALLOCATOR, OB_MALLOC_BIG_BLOCK_SIZE, OB_SERVER_TENANT_ID);
      ObSchemaStackAllocatorGuard guard(&allocator);

      bool need_refresh_schema = true; // The schema needs to be refreshed by default
      // If the user configures expected_schema_version, first obtain the latest schema version.
      // If the schema version is greater than or equal to expected_schema_version, there is no need to refresh the schema
      if (OB_INVALID_TENANT_ID != tenant_id && OB_INVALID_SCHEMA_VERSION != expected_schema_version) {
        // For a single tenant, its schema_version is always comparable whether it is the old version of
        // the cluster-level schema_version or the new version of the group-level chema_version
        int64_t refreshed_version = OB_INVALID_SCHEMA_VERSION;
        if (OB_FAIL(get_tenant_refreshed_schema_version(tenant_id, refreshed_version))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            // The tenant does not exist. Normally, the schema needs to be refreshed.
            need_refresh_schema = true;
            ret = OB_SUCCESS;
          } else {
            LOG_ERROR("get_tenant_refreshed_schema_version fail", KR(ret), K(tenant_id));
          }
        } else {
          need_refresh_schema = (expected_schema_version > refreshed_version);
          if (!need_refresh_schema) {
            LOG_INFO("tenant schema has been refreshed to expected schema version, need not refresh",
                K(tenant_id), K(expected_schema_version), K(refreshed_version));
          }
        }
      }

      ObArray<uint64_t> tenant_ids;
      // use new mode & refresh specified tenant's schema
      if (OB_INVALID_TENANT_ID == tenant_id) {
        // refresh all tenant schema
      } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
        LOG_WARN("fail to push_back tenant_id", K(ret), K(tenant_id));
      }

      if (OB_FAIL(ret)) {
      } else if (0 == tenant_ids.count()) {
        // refresh all tenant schema
        ObSchemaMgr *schema_mgr = NULL;
        ObArray<uint64_t> all_tenant_ids;
        if (OB_FAIL(refresh_tenant_schema(OB_SYS_TENANT_ID))) {
          LOG_WARN("fail to refresh sys schema", K(ret), K(all_tenant_ids));
        } else if (OB_FAIL(schema_mgr_for_cache_map_.get_refactored(OB_SYS_TENANT_ID, schema_mgr))) {
          LOG_WARN("fail to get sys schema mgr for cache", K(ret));
        } else if (OB_ISNULL(schema_mgr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema_mgr is null", K(ret));
        } else if (OB_FAIL(schema_mgr->get_tenant_ids(all_tenant_ids))) {
          LOG_WARN("fail to get all tenant_ids", K(ret));
        } else {
          // Ignore that some tenants fail to refresh the schema, but need to report an error to the upper level
          int tmp_ret = OB_SUCCESS;
          for (int64_t i = 0; i < all_tenant_ids.count(); i++) {
            const uint64_t tenant_id = all_tenant_ids.at(i);
            if (OB_SYS_TENANT_ID == tenant_id) {
              // skip
            } else if (OB_SUCCESS != (tmp_ret = refresh_tenant_schema(tenant_id))) {
              LOG_WARN("fail to refresh tenant schema", K(tmp_ret), K(tenant_id));
              TenantStatus tenant_status = TENANT_STATUS_INVALID;
              int temp_ret = query_tenant_status(tenant_id, tenant_status);
              if (OB_SUCCESS != temp_ret){
                LOG_WARN("fail to query tenant status", K(tmp_ret), K(temp_ret), K(tenant_id));
              } else if (TENANT_DELETED == tenant_status) {
                LOG_INFO("tenant has been dropped, just ignore", K(tmp_ret), K(tenant_id));
                tmp_ret = OB_SUCCESS;
              }
            }
            if (OB_SUCCESS != tmp_ret && OB_SUCCESS == ret) {
              ret = tmp_ret;
            }
          }
        }
      } else {
        // single tenant schema refresh
        int64_t local_version = OB_INVALID_VERSION;
        if (OB_INVALID_TENANT_ID == tenant_id) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
        } else if (!need_refresh_schema) {
          // schema is new enough, just skip
        } else if (OB_FAIL(get_tenant_refreshed_schema_version(tenant_id, local_version))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            // The tenant related data structure does not exist, trigger the system tenant schema refresh,
            // and initialize the related data structure
            if (OB_FAIL(refresh_tenant_schema(OB_SYS_TENANT_ID))) {
              LOG_WARN("refresh sys schema failed", K(ret));
            }
          } else {
            LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(tenant_id));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(refresh_tenant_schema(tenant_id))) {
          LOG_WARN("fail to refresh tenant schema", K(ret), K(tenant_id));
          TenantStatus tenant_status = TENANT_STATUS_INVALID;
          int temp_ret = query_tenant_status(tenant_id, tenant_status);
          if (OB_SUCCESS != temp_ret){
            LOG_WARN("query tenant status failed", K(ret), K(temp_ret), K(tenant_id));
          } else if (TENANT_DELETED == tenant_status) {
            LOG_INFO("tenant has been dropped, no need retry", K(ret), K(tenant_id));
            ret = OB_TENANT_HAS_BEEN_DROPPED; //overwrite ret
          }
        }
      }
    };

    if (OB_SUCCESS == ret) {
      CREATE_WITH_TEMP_ENTITY_P(!ObSchemaService::g_liboblog_mode_, RESOURCE_OWNER, common::OB_SERVER_TENANT_ID)
      {
        func();
      } else {
        // Two aspects are considered, one is that there is no omt module in one side,
        // and the other is that the tenant has not been loaded during the omt startup phase.
        func();
      }
    }
  }
  LOG_INFO("[REFRESH_SCHEMA] end refresh and add schema", K(ret), K(tenant_id));
  return ret;
}

// refresh schema by tenant
// 1. System tenants strengthen the consistency of reading and brushing schema
// 2. user tenants of the primary cluster strengthened to read and refresh schema consistently
// 3. user tenants of the standalone cluster are weakly consistent in reading and refresh schema,
//  and they need to obtain the weakly consistent read version number and the visible schema version
int ObMultiVersionSchemaService::refresh_tenant_schema(
    const uint64_t tenant_id)
{
  FLOG_INFO("[REFRESH_SCHEMA] start to refresh and add schema by tenant", K(tenant_id));
  const int64_t start = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  bool refresh_full_schema = false;
  bool is_standby_cluster = GCTX.is_standby_cluster();
  bool is_restore = false;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret));
  } else if (!ObSchemaService::g_liboblog_mode_
             && OB_FAIL(check_tenant_is_restore(NULL, tenant_id, is_restore))) {
    LOG_WARN("fail to check restore tenant exist", KR(ret), K(tenant_id));
  } else {
    int64_t new_received_schema_version = OB_INVALID_VERSION;
    ObRefreshSchemaStatus refresh_schema_status;
    ObISQLClient &sql_client = *sql_proxy_;

    // read refresh_schema_status from inner table
    if ((!is_standby_cluster && !is_restore)
         || is_sys_tenant(tenant_id)
         || is_meta_tenant(tenant_id)) {
      // 1. System tenants strengthen the consistency of reading and refresh schema
      // 2. user tenants of the primary cluster strengthened to read and refresh schema consistently
      refresh_schema_status.reset();
      refresh_schema_status.tenant_id_ = tenant_id;
      refresh_schema_status.snapshot_timestamp_ = OB_INVALID_TIMESTAMP;
      refresh_schema_status.readable_schema_version_ = OB_INVALID_VERSION;
    } else {
      // 3. user tenants of the standalone cluster weaken the consistent read and refresh schema
      // 4. primary cluster restore tenants are weak, consistent read and refresh schema
      ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
      if (OB_ISNULL(schema_status_proxy)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_status_proxy is null", KR(ret));
      } else if (OB_FAIL(schema_status_proxy->get_refresh_schema_status(tenant_id, refresh_schema_status))) {
        LOG_WARN("fail to get refresh schema status", KR(ret), K(tenant_id));
      } else if (refresh_schema_status.snapshot_timestamp_ == 0) {
        // The standalone cluster RS has not yet pushed the tenant's schema version (the standalone cluster RS
        // needs to change the internal table first, and then change the memory value),
        // and skip the tenant schema refresh at this time
        ret = OB_SCHEMA_EAGAIN;
        LOG_INFO("[REFRESH_SCHEMA] tenant schema is not ready, just skip", KR(ret), K(tenant_id), K(refresh_schema_status));
      }
    }

    if (OB_SUCC(ret)) {
      bool need_refresh = true;
      int64_t baseline_schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(get_baseline_schema_version(tenant_id, true/*auto_update*/, baseline_schema_version))) {
        LOG_WARN("fail to get baseline schema version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(refresh_full_schema_map_.get_refactored(tenant_id, refresh_full_schema))) {
        LOG_WARN("refresh full schema", KR(ret), K(tenant_id));
      } else if (!refresh_full_schema) {
        if (OB_FAIL(get_schema_version_in_inner_table(
            sql_client, refresh_schema_status, new_received_schema_version))) {
          LOG_WARN("fail to get tenant schema version", KR(ret), K(refresh_schema_status));
        } else {
          ObSchemaStore* schema_store = schema_store_map_.get(tenant_id);
          if (NULL == schema_store) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get schema store", KR(ret));
          } else {
            // schema_store->update_received_version(new_received_schema_version);
            // if (schema_store->get_refreshed_version() >= schema_store->get_received_version()) {
            if (schema_store->get_refreshed_version() >= new_received_schema_version) {
              need_refresh = false;
              LOG_TRACE("[REFRESH_SCHEMA] local refreshed schema version is greater than received schema version, just skip",
                        KR(ret), K(tenant_id), K(schema_store->received_version_), K(schema_store->refreshed_version_));
            }
          }
        }
      }

      if (OB_SUCC(ret) && need_refresh) {
        if (OB_FAIL(refresh_schema(refresh_schema_status))) {
          LOG_WARN("fail to refresh schema by tenant", KR(ret), K(refresh_schema_status));
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_INVALID_SCHEMA_VERSION != new_received_schema_version) {
        if (OB_SUCCESS != (tmp_ret = set_tenant_received_broadcast_version(tenant_id, new_received_schema_version))) {
          LOG_WARN("fail to set tenant received schema version", KR(tmp_ret), K(tenant_id), K(new_received_schema_version));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
    }
  }
  FLOG_INFO("[REFRESH_SCHEMA] end refresh and add schema by tenant", KR(ret), K(tenant_id),
            "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObMultiVersionSchemaService::publish_schema(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const bool force_add = false;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(add_schema(tenant_id, force_add))) {
    LOG_WARN("fail to add schema", K(ret), K(tenant_id));
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

int ObMultiVersionSchemaService::check_outline_exist_with_name(const uint64_t tenant_id,
                                                               const uint64_t database_id,
                                                               const common::ObString &outline_name,
                                                               uint64_t &outline_id,
                                                               bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id
                           || outline_name.empty())) {
      LOG_WARN("invalid arguments", K(tenant_id), K(database_id), K(outline_name), K(ret));
    } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret));
    } else if (OB_FAIL(schema_guard.check_outline_exist_with_name(
                tenant_id,
                database_id,
                outline_name,
                outline_id,
                exist))) {
      LOG_WARN("failed to check outline name exist", K(tenant_id), K(database_id), K(outline_name),
               K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObMultiVersionSchemaService::check_outline_exist_with_sql(const uint64_t tenant_id,
                                                              const uint64_t database_id,
                                                              const common::ObString &paramlized_sql,
                                                              bool &exist)

{
  int ret = OB_SUCCESS;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited", K(ret));
    } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret));
    } else if (OB_FAIL(schema_guard.check_outline_exist_with_sql(
                tenant_id,
                database_id,
                paramlized_sql,
                exist))) {
      LOG_WARN("failed to check outline sql exist", K(tenant_id), K(database_id),
               K(paramlized_sql), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObMultiVersionSchemaService::check_synonym_exist(const uint64_t tenant_id,
    const uint64_t database_id,
    const common::ObString &synonym_name,
    bool &exist,
    uint64_t &synonym_id)

{
  int ret = OB_SUCCESS;
  exist = false;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited", K(ret));
    } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret));
    } else if (OB_FAIL(schema_guard.check_synonym_exist_with_name(
                tenant_id,
                database_id,
                synonym_name,
                exist,
                synonym_id))) {
      LOG_WARN("failed to check synonym sql exist",
          K(tenant_id), K(database_id),
               K(synonym_name), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObMultiVersionSchemaService::check_udf_exist(const uint64_t tenant_id,
                                                 const common::ObString &name,
                                                 bool &exist,
                                                 uint64_t &udf_id)
{
  int ret = OB_SUCCESS;
  exist = false;
  udf_id = OB_INVALID_ID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited", K(ret));
    } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret));
    } else if (OB_FAIL(schema_guard.check_udf_exist_with_name(
                tenant_id,
                name,
                exist,
                udf_id))) {
      LOG_WARN("failed to check udf sql exist",
          K(tenant_id), K(name), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObMultiVersionSchemaService::check_sequence_exist(const uint64_t tenant_id,
                                                      const uint64_t database_id,
                                                      const common::ObString &name,
                                                      bool &exist)
{
  int ret = OB_SUCCESS;
  uint64_t sequence_id = OB_INVALID_ID;
  bool is_system_generated = false;
  exist = false;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited", K(ret));
    } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.check_sequence_exist_with_name(
                tenant_id,
                database_id,
                name,
                exist,
                sequence_id,
                is_system_generated))) {
      LOG_WARN("failed to check sequence exist",
          K(tenant_id), K(name), K(ret));
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::check_label_se_policy_column_name_exist(const uint64_t tenant_id,
                                                                         const ObString &column_name,
                                                                         bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    const ObLabelSePolicySchema *schema = NULL;
    if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited", K(ret));
    } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret));
    } else if (OB_FAIL(schema_guard.get_label_se_policy_schema_by_column_name(
                         tenant_id,
                         column_name,
                         schema))) {
      LOG_WARN("failed to check schema exist", K(tenant_id), K(ret));
    } else {
      is_exist = (schema != NULL);
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::check_label_se_component_short_name_exist(const uint64_t tenant_id,
                                                                           const uint64_t policy_id,
                                                                           const int64_t comp_type,
                                                                           const ObString &short_name,
                                                                           bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    const ObLabelSeComponentSchema *schema = NULL;
    if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited", K(ret));
    } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret));
    } else if (OB_FAIL(schema_guard.get_label_se_component_schema_by_short_name(
                         tenant_id,
                         policy_id,
                         comp_type,
                         short_name,
                         schema))) {
      LOG_WARN("failed to check schema exist", K(short_name),
          K(tenant_id), K(policy_id), K(comp_type), K(ret));
    } else {
      is_exist = (schema != NULL);
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::check_label_se_component_long_name_exist(const uint64_t tenant_id,
                                                                          const uint64_t policy_id,
                                                                          const int64_t comp_type,
                                                                          const ObString &long_name,
                                                                          bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    const ObLabelSeComponentSchema *schema = NULL;
    if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited", K(ret));
    } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret));
    } else if (OB_FAIL(schema_guard.get_label_se_component_schema_by_long_name(
                         tenant_id,
                         policy_id,
                         comp_type,
                         long_name,
                         schema))) {
      LOG_WARN("failed to check schema exist", K(long_name),
          K(tenant_id), K(policy_id), K(comp_type), K(ret));
    } else {
      is_exist = (schema != NULL);
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::check_outline_exist_with_sql_id(const uint64_t tenant_id,
                                                              const uint64_t database_id,
                                                              const common::ObString &sql_id,
                                                              bool &exist)

{
  int ret = OB_SUCCESS;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited", K(ret));
    } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret));
    } else if (OB_FAIL(schema_guard.check_outline_exist_with_sql_id(
                tenant_id,
                database_id,
                sql_id,
                exist))) {
      LOG_WARN("failed to check outline sql exist", K(tenant_id), K(database_id),
               K(sql_id), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}


//-----------For managing privileges-----------

int ObMultiVersionSchemaService::check_user_exist(
    const uint64_t tenant_id,
    const common::ObString &user_name,
    const common::ObString &host_name,
    bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    uint64_t user_id = OB_INVALID_ID;
    ret = check_user_exist(tenant_id, user_name, host_name, user_id, exist);
  }
  return ret;
}

int ObMultiVersionSchemaService::check_user_exist(
    const uint64_t tenant_id,
    const common::ObString &user_name,
    const common::ObString &host_name,
    uint64_t &user_id,
    bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited", K(ret));
    } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret));
    } else if (OB_FAIL(schema_guard.check_user_exist(tenant_id,
                                                     user_name,
                                                     host_name,
                                                     exist,
                                                     &user_id))) {
      LOG_WARN("failed to check user exist", K(ret));
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::check_user_exist(
    const uint64_t tenant_id,
    const uint64_t user_id,
    bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    if (!is_tenant_full_schema(tenant_id)) {
      ret = OB_NOT_INIT;
      LOG_WARN("local schema not inited", K(ret));
    } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed ", K(ret));
    } else if (OB_FAIL(schema_guard.check_user_exist(tenant_id, user_id, exist))) {
      LOG_WARN("failed to check table exist", K(ret));
    }
  }
  return ret;
}

void ObMultiVersionSchemaService::dump_schema_statistics()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(schema_refresh_mutex_);
  FLOG_INFO("[SCHEMA_STATISTICS] dump schema statistics info start");
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else {
    FOREACH(it, mem_mgr_map_) {
      uint64_t tenant_id = (*it).first;
      ObSchemaMemMgr *mem_mgr = (*it).second;
      FLOG_INFO("[SCHEMA_STATISTICS] dump schema for refresh start", K(ret), K(tenant_id));
      if (OB_ISNULL(mem_mgr)) {
        LOG_INFO("mem_mgr is null", K(ret), K(tenant_id));
      } else {
        mem_mgr->dump();

        ObSchemaMgr *schema_mgr_for_cache = NULL;
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = schema_mgr_for_cache_map_.get_refactored(tenant_id, schema_mgr_for_cache))) {
          if (OB_HASH_NOT_EXIST != tmp_ret) {
            LOG_WARN("fail to get schema_mgr", K(tmp_ret), K(tenant_id));
          }
        } else {
          schema_mgr_for_cache->dump();
        }

        ObSchemaStore *schema_store = NULL;
        ObSchemaMgrCache *schema_mgr_cache = NULL;
        if (OB_ISNULL(schema_store = schema_store_map_.get(tenant_id))) {
          LOG_INFO("schema_store is null", K(ret), K(tenant_id));
        } else if (OB_ISNULL(schema_mgr_cache = &schema_store->schema_mgr_cache_)) {
          LOG_INFO("schema_mgr_cache is null", K(ret), K(tenant_id));
        } else {
          schema_mgr_cache->dump();
        }
      }
      FLOG_INFO("[SCHEMA_STATISTICS] dump schema for refresh end", K(ret), K(tenant_id));
    }

    FOREACH(it, mem_mgr_for_liboblog_map_) {
      uint64_t tenant_id = (*it).first;
      ObSchemaMemMgr *mem_mgr = (*it).second;
      FLOG_INFO("[SCHEMA_STATISTICS] dump schema for fallback start", K(ret), K(tenant_id));
      if (OB_ISNULL(mem_mgr)) {
        LOG_INFO("mem_mgr is null", K(ret), K(tenant_id));
      } else {
        mem_mgr->dump();

        ObSchemaStore *schema_store = NULL;
        ObSchemaMgrCache *schema_mgr_cache = NULL;
        if (OB_ISNULL(schema_store = schema_store_map_.get(tenant_id))) {
          LOG_INFO("schema_store is null", K(ret), K(tenant_id));
        } else if (OB_ISNULL(schema_mgr_cache = &schema_store->schema_mgr_cache_for_liboblog_)) {
          LOG_INFO("schema_mgr_cache is null", K(ret), K(tenant_id));
        } else {
          schema_mgr_cache->dump();
        }
      }
      FLOG_INFO("[SCHEMA_STATISTICS] dump schema for fallback end", K(ret), K(tenant_id));
    }
  }
}

int ObMultiVersionSchemaService::try_eliminate_schema_mgr()
{
  int ret = OB_SUCCESS;
  ObHashSet<uint64_t> candidates;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  }
  // 1. try gc dropped tenant's schema mgr
  if (FAILEDx(try_gc_tenant_schema_mgr())) {
    LOG_WARN("fail to gc tenant schema mgr", K(ret));
  }
  // 2. try gc exist tenant's schema mgr
  // - another allocator (only for refresh)
  // - schema_mgr for fallback
  if (FAILEDx(try_gc_existed_tenant_schema_mgr())) {
    LOG_WARN("fail to gc existed tenant schema mgr", K(ret));
  }
  return ret;
}

// try gc dropped tenant's schema mgr
int ObMultiVersionSchemaService::try_gc_tenant_schema_mgr()
{
  int ret = OB_SUCCESS;
  ObHashSet<uint64_t> candidates;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_FAIL(candidates.create(DEFAULT_TENANT_SET_SIZE))) {
    LOG_WARN("fail to create hash set", K(ret));
  } else if (OB_FAIL(get_gc_candidates(candidates))) {
    LOG_WARN("fail to get gc candidates", K(ret));
  } else {
    FOREACH_X(it, candidates, OB_SUCC(ret)) {
      if (OB_FAIL(try_gc_tenant_schema_mgr((*it).first))) {
        LOG_WARN("fail to eliminate schema mgr", K(ret), "tenant_id", (*it).first);
      } else {
        LOG_INFO("try eliminate schema mgr", K(ret), "tenant_id", (*it).first);
      }
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::get_gc_candidates(ObHashSet<uint64_t> &candidates)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  ObHashSet<uint64_t> tenant_id_set;
  // The purpose of locking here is to ensure that there are no new tenants in the GC phase,
  // and to prevent the new tenant schema from being considered as dropped tenants and being GC dropped
  // during the schema refresh process.
  lib::ObMutexGuard guard(schema_refresh_mutex_);
  if (!is_sys_full_schema()) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("full schema is not ready, cann't get fallback schema guard", K(ret));
  } else if (OB_FAIL(get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard failed ", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", K(ret));
  } else if (OB_FAIL(tenant_id_set.create(DEFAULT_TENANT_SET_SIZE))) {
    LOG_WARN("fail to create hash set", K(ret));
  } else {

    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
      if (OB_FAIL(tenant_id_set.set_refactored(tenant_ids.at(i)))) {
        LOG_WARN("fail to push back tenant id", K(ret), K(tenant_ids.at(i)));
      }
    }

# define GET_GC_TENANT_CANDIDATES(MEM_MGR_MAP) \
      FOREACH_X(it, MEM_MGR_MAP, OB_SUCC(ret)) { \
        bool can_release = false; \
        uint64_t tenant_id = (*it).first; \
        ObSchemaMemMgr *mem_mgr = (*it).second; \
        ret = tenant_id_set.exist_refactored(tenant_id); \
        if (OB_HASH_EXIST == ret) { \
          ret = OB_SUCCESS; \
        } else if (OB_HASH_NOT_EXIST != ret) { \
          LOG_WARN("fail to check tenant exist", K(ret), K(tenant_id)); \
        } else if (OB_ISNULL(mem_mgr)) { \
          ret = OB_ERR_UNEXPECTED; \
          LOG_WARN("mem_mgr is null", K(ret), K(tenant_id)); \
        } else if (OB_FAIL(mem_mgr->check_can_release(can_release))) { \
          LOG_WARN("fail to check if tenant can release", K(ret), K(tenant_id)); \
        } else if (!can_release) { \
          continue; \
        } else if (OB_FAIL(candidates.set_refactored(tenant_id))) { \
          LOG_WARN("fail to set candidate", K(ret), K(tenant_id)); \
        } \
      }

    GET_GC_TENANT_CANDIDATES(mem_mgr_map_);

    if (OB_SUCC(ret)) {
      // In order to prevent the tenant schema from being released by mistake in the fallback process,
      // lock protection is required here
      lib::ObMutexGuard guard(mem_mgr_for_liboblog_mutex_);
      GET_GC_TENANT_CANDIDATES(mem_mgr_for_liboblog_map_);
    }
# undef GET_GC_TENANT_CANDIDATES

    if (OB_SUCC(ret)) {
      ret = candidates.exist_refactored(OB_SYS_TENANT_ID);
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("sys tenant should not exist in candidates", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

/*
 * Reclaim the memory occupied by schema_mgr
 * 1. system tenant:
 *   - not supported
 * 2. user tenant
 *   - recycle schema_mgr_for_cache object
 *   - Try to reclaim the memory occupied by multiple versions of ObSchemaMgr occupied by schema_mem_mgr
 *   - Try to reclaim the memory occupied by multiple versions of ObSchemaMgr occupied by schema_mem_mgr_for_liboblog
 *   - reset refreshed_schema_version, received_broadcast_version and refresh_full_schema
 *
 */
int ObMultiVersionSchemaService::try_gc_tenant_schema_mgr(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(try_gc_tenant_schema_mgr_for_refresh(tenant_id))) {
    LOG_WARN("fail to gc tenant schema mgr for refresh", KR(ret), K(tenant_id));
  } else if (OB_FAIL(try_gc_tenant_schema_mgr_for_fallback(tenant_id))) {
    LOG_WARN("fail to gc tenant schema mgr for fallback", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObMultiVersionSchemaService::try_gc_tenant_schema_mgr_for_refresh(
    uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    lib::ObMutexGuard guard(schema_refresh_mutex_);
    ObSchemaMemMgr *mem_mgr = NULL;
    ObSchemaMgrCache *schema_mgr_cache = NULL;
    ObSchemaStore* schema_store = NULL;

    if (NULL == (schema_store = schema_store_map_.get(tenant_id))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to get schema_store", K(ret));
    } else if (OB_FAIL(mem_mgr_map_.get_refactored(tenant_id, mem_mgr))) {
      LOG_WARN("fail to get mem mgr", K(ret), K(tenant_id));
    } else {
      schema_store->reset_version();
      schema_mgr_cache = &schema_store->schema_mgr_cache_;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_gc_tenant_schema_mgr(mem_mgr, schema_mgr_cache))) {
      LOG_WARN("fail to eliminate schema mgr", K(ret), K(tenant_id));
    } else if (OB_FAIL(destroy_schema_struct(tenant_id))) {
      LOG_WARN("fail to destroy schema struct", K(ret), K(tenant_id));
    }
  }
  LOG_INFO("try gc schema mgr for refresh", K(ret), K(tenant_id));
  return ret;
}

int ObMultiVersionSchemaService::try_gc_tenant_schema_mgr_for_fallback(
    uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    lib::ObMutexGuard guard(mem_mgr_for_liboblog_mutex_);
    ObSchemaMemMgr *mem_mgr = NULL;
    ObSchemaMgrCache *schema_mgr_cache = NULL;
    ObSchemaStore* schema_store = NULL;

    // reset schema mgr for fallback in new mode
    if (NULL == (schema_store = schema_store_map_.get(tenant_id))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to get schema_store", K(ret));
    } else if (OB_FAIL(mem_mgr_for_liboblog_map_.get_refactored(tenant_id, mem_mgr))) {
      LOG_WARN("fail to get mem mgr", K(ret), K(tenant_id));
    } else {
      schema_mgr_cache = &schema_store->schema_mgr_cache_for_liboblog_;
      if (OB_FAIL(try_gc_tenant_schema_mgr(mem_mgr, schema_mgr_cache))) {
        LOG_WARN("fail to eliminate schema mgr", K(ret), K(tenant_id));
      }
    }
  }
  LOG_INFO("try gc schema mgr for fallback", K(ret), K(tenant_id));
  return ret;
}

// need protect by lock
int ObMultiVersionSchemaService::try_gc_tenant_schema_mgr(ObSchemaMemMgr *&mem_mgr, ObSchemaMgrCache *&schema_mgr_cache)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_mgr is null", K(ret));
  } else if (OB_ISNULL(schema_mgr_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_mgr_cache is null", K(ret));
  } else {
    ObSchemaMgr *eli_schema_mgr = NULL;
    uint64_t tenant_id = mem_mgr->get_tenant_id();
    bool need_gc_schema_mgr = false;
    do {
      if (OB_FAIL(schema_mgr_cache->try_gc_tenant_schema_mgr(eli_schema_mgr))) {
        LOG_WARN("fail to eliminate schema mgr", K(ret), K(tenant_id));
      } else if (FALSE_IT(need_gc_schema_mgr = OB_NOT_NULL(eli_schema_mgr))) {
      } else if (OB_FAIL(mem_mgr->free_schema_mgr(eli_schema_mgr))) {
        LOG_ERROR("free eli schema mgr falied", KR(ret), K(tenant_id));
      }
    } while (OB_SUCC(ret) && need_gc_schema_mgr);

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(mem_mgr->try_reset_allocator())) {
      LOG_WARN("fail to reset allocator", K(ret), K(tenant_id));
    }
  }
  return ret;
}

// try release exist tenant's another allocator
int ObMultiVersionSchemaService::try_gc_existed_tenant_schema_mgr()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_sys_full_schema()) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("full schema is not ready, cann't get fallback schema guard", KR(ret));
  } else if (OB_FAIL(get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard failed ", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", KR(ret));
  } else {
    ObSchemaMemMgr *mem_mgr = NULL;
    ObSchemaMgrCache *schema_mgr_cache = NULL;
    for (int64_t i = 0; i < tenant_ids.count(); i++) { // ignore ret
      ObSchemaStore* schema_store = NULL;
      const uint64_t tenant_id = tenant_ids.at(i);
      if (NULL == (schema_store = schema_store_map_.get(tenant_id))) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("fail to get schema_store", KR(ret));
      } else if (FALSE_IT(schema_mgr_cache = &schema_store->schema_mgr_cache_)) {
      } else if (OB_FAIL(mem_mgr_map_.get_refactored(tenant_id, mem_mgr))) {
        LOG_WARN("fail to get mem mgr", KR(ret), K(tenant_id));
      } else {
        // ignore failure in eache scene
        // 1. another allocator for schema refresh
        if (OB_FAIL(try_gc_another_allocator(tenant_id, mem_mgr, schema_mgr_cache))) {
          LOG_WARN("fail to gc another allocator", KR(ret), K(tenant_id));
        }
        // 2. schema fallback
        if (ObSchemaService::g_liboblog_mode_) {
          // liboblog/agentserver do nothing
        } else if (OB_FAIL(try_gc_tenant_schema_mgr_for_fallback(tenant_id))) {
          LOG_WARN("fail to gc tenant schema mgr for fallback", KR(ret), K(tenant_id));
        }
        // 3.let schema mgr to free slot's memory
        if (OB_FAIL(try_gc_current_allocator(tenant_id, mem_mgr, schema_mgr_cache))) {
          LOG_WARN("fail to gc current slot", KR(ret), K(tenant_id));
        }
      }
    }
  }
  return ret;
}

// need protected by schema_refresh_mutex_
int ObMultiVersionSchemaService::try_gc_another_allocator(
    const uint64_t tenant_id,
    ObSchemaMemMgr *&mem_mgr,
    ObSchemaMgrCache *&schema_mgr_cache)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", K(ret));
  } else if (OB_ISNULL(mem_mgr) || OB_ISNULL(schema_mgr_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mem_mgr or schema_mgr_cahe is null",
             K(ret), K(tenant_id), KP(mem_mgr), KP(schema_mgr_cache));
  } else if (tenant_id != mem_mgr->get_tenant_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not match", K(ret), K(tenant_id),
             "mem_tenant_id", mem_mgr->get_tenant_id());
  } else {
    lib::ObMutexGuard guard(schema_refresh_mutex_);
    ObArray<void *> another_ptrs;
    int64_t local_version = OB_INVALID_VERSION;
    if (OB_FAIL(mem_mgr->get_another_ptrs(another_ptrs))) {
      LOG_WARN("fail to get another ptrs", K(ret), K(tenant_id));
    } else if (OB_FAIL(get_tenant_refreshed_schema_version(tenant_id, local_version))) {
      LOG_WARN("fail to get local refreshed schema version", K(ret), K(tenant_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < another_ptrs.count(); i++) {
        ObSchemaMgr *tmp_mgr = NULL;
        if (OB_ISNULL(another_ptrs.at(i))) {
          LOG_WARN("ptrs is null", K(ret), K(tenant_id), K(i));
        } else if (FALSE_IT(tmp_mgr = static_cast<ObSchemaMgr *>(another_ptrs.at(i)))) {
        } else if (tmp_mgr->get_schema_version() >= local_version) {
          ret = OB_SCHEMA_EAGAIN;
          LOG_INFO("schema mgr is in used, try reset another allocator next round",
                   K(ret), K(tenant_id), "version", tmp_mgr->get_schema_version(),
                   K(local_version));
        }
      }
      ObSchemaMgr *eli_schema_mgr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < another_ptrs.count(); i++) {
        if (OB_ISNULL(another_ptrs.at(i))) {
          LOG_WARN("ptrs is null", K(ret), K(tenant_id), K(i));
        } else if (FALSE_IT(eli_schema_mgr = static_cast<ObSchemaMgr *>(another_ptrs.at(i)))) {
        } else if (OB_FAIL(schema_mgr_cache->try_eliminate_schema_mgr(eli_schema_mgr))) {
          LOG_WARN("fail to eliminate schema_mgr", K(ret), K(tenant_id), K(eli_schema_mgr));
        } else if (OB_FAIL(mem_mgr->free_schema_mgr(eli_schema_mgr))) {
          LOG_ERROR("free eli schema mgr falied", KR(ret), K(tenant_id));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(mem_mgr->try_reset_another_allocator())) {
        LOG_WARN("fail to reset another allocator", K(ret), K(tenant_id));
      }
    }
  }
  LOG_INFO("try gc another allocator", K(ret), K(tenant_id));
  return ret;
}

// try to gc current allocator's schema mgr, it can reduce the number of schema mgr in the background
int ObMultiVersionSchemaService::try_gc_current_allocator(
    const uint64_t tenant_id,
    ObSchemaMemMgr *&mem_mgr,
    ObSchemaMgrCache *&schema_mgr_cache)
{
  int ret = OB_SUCCESS;
  int64_t recycle_interval = GCONF._schema_memory_recycle_interval;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_ISNULL(mem_mgr) || OB_ISNULL(schema_mgr_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mem_mgr or schema_mgr_cahe is null",
             KR(ret), K(tenant_id), KP(mem_mgr), KP(schema_mgr_cache));
  } else if (tenant_id != mem_mgr->get_tenant_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id not match", KR(ret), K(tenant_id), K(mem_mgr->get_tenant_id()));
  } else if (0 == recycle_interval) {
    // 0 means turn off gc current allocator
    LOG_TRACE("_schema_memory_recycle_interval is zero, turn off gc current allocator", K(tenant_id));
  } else {
    int64_t start_time = ObTimeUtility::current_time();
    ObArray<void *> current_ptrs;
    int64_t local_version = OB_INVALID_VERSION;
    lib::ObMutexGuard guard(schema_refresh_mutex_);

    if (OB_FAIL(mem_mgr->get_current_ptrs(current_ptrs))) {
      LOG_WARN("fail to get another ptrs", KR(ret), K(tenant_id));
    } else if (OB_FAIL(get_tenant_refreshed_schema_version(tenant_id, local_version))) {
      LOG_WARN("fail to get local refreshed schema version", KR(ret), K(tenant_id));
    } else if (!ObSchemaService::is_formal_version(local_version)) {
      LOG_TRACE("ignore to free current allocator when refreshed version is not formal", K(tenant_id), K(local_version));
    } else {
      int64_t eli_timestamp = 0;
      ObSchemaMgr *eli_schema_mgr = NULL;
      int64_t eli_schema_version = OB_INVALID_VERSION;
      for (int64_t i = 0; OB_SUCC(ret) && i < current_ptrs.count(); i++) {
        if (OB_ISNULL(current_ptrs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ptrs is null", KR(ret), K(tenant_id), K(i));
        } else {
          eli_schema_mgr = static_cast<ObSchemaMgr *>(current_ptrs.at(i));
          eli_timestamp = eli_schema_mgr->get_timestamp_in_slot();
          eli_schema_version = eli_schema_mgr->get_schema_version();
          if (eli_schema_version >= local_version
              || (recycle_interval > ObClockGenerator::getClock() - eli_timestamp)) {
            LOG_TRACE("no need to gc current allocator's schema mgr", K(tenant_id), K(eli_timestamp),
                      K(eli_schema_version), K(local_version), K(recycle_interval));
          } else {
            //gc only those that have been put in the slot for more than recycle_interval
            LOG_INFO("try to gc current allocator's schema mgr which is in slot",
                     K(tenant_id), K(eli_schema_version), K(local_version),
                     K(eli_timestamp), K(recycle_interval));
            if (OB_FAIL(schema_mgr_cache->try_eliminate_schema_mgr(eli_schema_mgr))) {
              if (OB_EAGAIN == ret) {
                // schema mgr in use, just ignore
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to eliminate schema_mgr", KR(ret), K(tenant_id),
                         K(eli_schema_version), K(eli_timestamp));
              }
            } else if (OB_FAIL(mem_mgr->free_schema_mgr(eli_schema_mgr))) {
              LOG_ERROR("free eli schema mgr falied", KR(ret), K(tenant_id));
            }
          }
        }
      }
    }
    int64_t end_time = ObTimeUtility::current_time();
    LOG_INFO("finish gc current allocator's schema mgr which is in slot", KR(ret), K(tenant_id),
             "cost_ts", end_time - start_time);
  }
  return ret;
}

bool ObMultiVersionSchemaService::compare_schema_mgr_info_(
     const ObSchemaMgr *lhs,
     const ObSchemaMgr *rhs)
{
  return lhs->get_schema_version() < rhs->get_schema_version();
}

// need protected by schema_refresh_mutex_
// try to gc current and another allocators' schema mgr, it can reduce the number of schema mgr in the foreground
// 1.reserve_mgr_count can let us reserve the number of total schema mgr
// 2.we can turn this off by set _schema_memory_recycle_interval to zero
int ObMultiVersionSchemaService::try_gc_allocator_when_add_schema_(
    const uint64_t tenant_id,
    ObSchemaMemMgr *&mem_mgr,
    ObSchemaMgrCache *&schema_mgr_cache)
{
  int ret = OB_SUCCESS;
  ObArray<void *> all_ptrs;
  int64_t local_version = OB_INVALID_VERSION;
  int64_t reserve_version = OB_INVALID_VERSION;
  int64_t start_time = ObTimeUtility::current_time();
  const int64_t reserve_mgr_count = RESERVE_SCHEMA_MGR_CNT;
  if (OB_ISNULL(mem_mgr) || OB_ISNULL(schema_mgr_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mem_mgr or schema_mgr_cahe is null",
             KR(ret), K(tenant_id), KP(mem_mgr), KP(schema_mgr_cache));
  } else if (0 > reserve_mgr_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("reserve_mgr_count is less than zero", KR(ret));
  } else if (0 == GCONF._schema_memory_recycle_interval) {
    // ignore
  } else if (OB_FAIL(mem_mgr->get_all_ptrs(all_ptrs))) {
    LOG_WARN("fail to get another ptrs", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_refreshed_schema_version(tenant_id, local_version))) {
    LOG_WARN("fail to get local refreshed schema version", KR(ret), K(tenant_id));
  } else if (!ObSchemaService::is_formal_version(local_version)) {
    LOG_TRACE("ignore to free current allocator when refreshed version is not formal", K(tenant_id), K(local_version));
  } else {
    SchemaMgrIterator iter;
    SchemaMgrInfos schema_mgr_infos;
    ObSchemaMgr *eli_schema_mgr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_ptrs.count(); i++) {
      iter = schema_mgr_infos.end();
      eli_schema_mgr = static_cast<ObSchemaMgr *>(all_ptrs.at(i));
      if (OB_ISNULL(eli_schema_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("eli_schema_mgr is null", KR(ret), K(tenant_id), K(i));
      } else if (OB_FAIL(schema_mgr_infos.insert(eli_schema_mgr, iter, compare_schema_mgr_info_))) {
        LOG_WARN("fail to insert schema mgr info", KR(ret), K(tenant_id));
      }
    }
    if (OB_FAIL(ret)) {
      // ignore
    } else if (all_ptrs.count() != schema_mgr_infos.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("all_ptrs and schema_mgr_infos count not equal", KR(ret), K(tenant_id),
                K(all_ptrs.count()), K(schema_mgr_infos.count()));
    } else {
      int64_t schema_mgr_cnt = schema_mgr_infos.count();
      int64_t reserve_index = schema_mgr_cnt > reserve_mgr_count ?
                              schema_mgr_cnt - reserve_mgr_count - 1 : OB_INVALID_INDEX;
      // we should skip free schema mgr when schema_mgr_cnt less than reserve_mgr_count
      if (reserve_index >= 0 && 0 != schema_mgr_cnt) {
        reserve_version = schema_mgr_infos.at(reserve_index)->get_schema_version();
      }
    }
    int64_t eli_schema_version = 0;
    int64_t total_schema_ptr_cnt = all_ptrs.count();
    int64_t remain_schema_ptr_cnt = all_ptrs.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < total_schema_ptr_cnt; i++) {
      eli_schema_mgr = static_cast<ObSchemaMgr *>(all_ptrs.at(i));
      if (OB_ISNULL(eli_schema_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("eli_schema_mgr is null", KR(ret), K(tenant_id), K(i));
      } else {
        eli_schema_version = eli_schema_mgr->get_schema_version();
        if (eli_schema_version >= local_version
            || eli_schema_version >= reserve_version) {
          LOG_TRACE("no need to gc allocator's schema mgr", K(tenant_id),
                    K(eli_schema_version), K(local_version), K(reserve_version));
        } else {
          LOG_INFO("try to gc allocator's schema mgr which schema version is less than reserve_version",
                    K(tenant_id), K(eli_schema_version), K(local_version), K(reserve_version));
          if (OB_FAIL(schema_mgr_cache->try_eliminate_schema_mgr(eli_schema_mgr))) {
            if (OB_EAGAIN == ret) {
              // schema mgr in use, just ignore
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to eliminate schema_mgr", KR(ret), K(tenant_id), K(eli_schema_version));
            }
          } else if (OB_FAIL(mem_mgr->free_schema_mgr(eli_schema_mgr))) {
            LOG_ERROR("free eli schema mgr falied", KR(ret), K(tenant_id));
          } else {
            remain_schema_ptr_cnt--;
          }
        }
      }
    } // for
    int64_t end_time = ObTimeUtility::current_time();
    LOG_INFO("finish gc allocator's schema mgr when add schema", KR(ret), K(tenant_id),
              K(total_schema_ptr_cnt), K(remain_schema_ptr_cnt), K(reserve_version),
              "cost_ts", end_time - start_time);
  }
  return ret;
}

bool ObMultiVersionSchemaService::is_sys_full_schema() const
{
  return is_tenant_full_schema(OB_SYS_TENANT_ID);
}

bool ObMultiVersionSchemaService::is_tenant_full_schema(const uint64_t tenant_id) const
{
  bool bret = false;
  int64_t schema_version = OB_INVALID_VERSION;
  int ret = get_tenant_refreshed_schema_version(tenant_id, schema_version);
  bret = OB_SUCC(ret) && schema_version > OB_CORE_SCHEMA_VERSION;
  return bret;
}

// factor of election priority
bool ObMultiVersionSchemaService::is_tenant_not_refreshed(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool schema_not_refreshed = false;
  if (OB_FAIL(refresh_full_schema_map_.get_refactored(tenant_id, schema_not_refreshed))) {
    // 1. tenant not exist
    // 2. tenant schema not refreshed yet after create tenant or restart observer.
    schema_not_refreshed = true;
    LOG_TRACE("fail to get refresh full schema flag from map", KR(ret), K(tenant_id));
  } else {
    // 1. when schema_not_refreshed = false, it means tenant refreshed full schema once.
    // 2. when schema_not_refreshed = true, it means tenant schema should be refreshed or tenant has been dropped.
    if (schema_not_refreshed) {
      ObSchemaGetterGuard guard;
      ObSimpleTenantSchema *tenant_schema = NULL;
      if (OB_FAIL(get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
        schema_not_refreshed = false;
        LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        schema_not_refreshed = true;
        LOG_TRACE("tenant should be refreshed or has been dropped", KR(ret), K(tenant_id));
      } else if (tenant_schema->is_normal()) {
        schema_not_refreshed = true;
      } else {
        // To make ls leader stable when tenant is in abnormal status.
        schema_not_refreshed = false;
      }
    }
  }
  return schema_not_refreshed;
}

// for obmp_connect
bool ObMultiVersionSchemaService::is_tenant_refreshed(const uint64_t tenant_id) const
{
  bool bret = false;
  bool schema_not_refreshed = false;
  int ret = refresh_full_schema_map_.get_refactored(tenant_id, schema_not_refreshed);
  if (OB_SUCC(ret)) {
    bret = !schema_not_refreshed;
  }
  return bret;
}

// sql should retry when tenant is normal but never refresh schema successfully.
bool ObMultiVersionSchemaService::is_schema_error_need_retry(
     ObSchemaGetterGuard *guard,
     const uint64_t tenant_id)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  bool schema_not_full = false;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(refresh_full_schema_map_.get_refactored(OB_SYS_TENANT_ID, schema_not_full))) {
    // skip
  } else if (schema_not_full) {
    // observer may be not start service, do not retry
  } else if (OB_FAIL(refresh_full_schema_map_.get_refactored(tenant_id, schema_not_full))) {
    // skip
  } else if (!schema_not_full) {
    // tenant's schema is full, do not retry
  } else {
    ObSchemaGetterGuard tmp_guard;
    const ObSimpleTenantSchema *tenant_schema = NULL;
    if (OB_ISNULL(guard)) {
      if (OB_FAIL(get_tenant_schema_guard(OB_SYS_TENANT_ID, tmp_guard))) {
        LOG_WARN("fail to get sys tenant's guard", KR(ret));
      } else if (OB_FAIL(tmp_guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
      }
    } else if (OB_FAIL(guard->get_tenant_info(tenant_id, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(tenant_schema)) {
      // 1. create tenant, schema not refreshed
      // 2. tenant has been dropped
    } else if (!tenant_schema->is_normal()) {
      // 1. creating
      // 2. restoring
      // 3. dropping
    } else {
      // observer restarts
      bret = true;
    }
  }
  return bret;
}

int ObMultiVersionSchemaService::get_tenant_refreshed_schema_version(
    const uint64_t tenant_id,
    int64_t &schema_version,
    const bool core_version) const
{
  int ret = OB_SUCCESS;
  int64_t refreshed_schema_version = OB_INVALID_VERSION;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id), K(core_version));
  } else {
    // new schema refresh
    const ObSchemaStore* schema_store = NULL;
    if (NULL == (schema_store = schema_store_map_.get(tenant_id))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_TRACE("fail to get schema_store", K(ret));
    } else {
      refreshed_schema_version = schema_store->get_refreshed_version();
    }
  }
  if (OB_SUCC(ret)) {
    schema_version = (!core_version || refreshed_schema_version > 0) ? refreshed_schema_version : OB_CORE_SCHEMA_VERSION;
  }
  return ret;
}

int ObMultiVersionSchemaService::get_tenant_received_broadcast_version(
    const uint64_t tenant_id,
    int64_t &schema_version,
    const bool core_schema_version) const
{
  int ret = OB_SUCCESS;
  int64_t received_broadcast_version = OB_INVALID_VERSION;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id), K(core_schema_version));
  } else {
    const ObSchemaStore* schema_store = NULL;
    if (NULL == (schema_store = schema_store_map_.get(tenant_id))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to get schema_store", K(ret));
    } else {
      received_broadcast_version = schema_store->get_received_version();
    }
  }
  if (OB_SUCC(ret)) {
    schema_version = (!core_schema_version || received_broadcast_version > 0) ? received_broadcast_version : OB_CORE_SCHEMA_VERSION;
  }
  return ret;
}

int ObMultiVersionSchemaService::get_tenant_broadcast_consensus_version(
    const uint64_t tenant_id,
    int64_t &consensus_version)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    const ObSchemaStore* schema_store = NULL;
    if (NULL == (schema_store = schema_store_map_.get(tenant_id))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to get schema_store", KR(ret));
    } else {
      consensus_version = schema_store->get_consensus_version();
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::set_tenant_broadcast_consensus_version(
    const uint64_t tenant_id,
    const int64_t consensus_version)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id), K(consensus_version));
  } else {
    ObSchemaStore* schema_store = NULL;
    if (NULL == (schema_store = schema_store_map_.get(tenant_id))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to get schema_store", KR(ret));
    } else {
      schema_store->update_consensus_version(consensus_version);
      LOG_INFO("try to set tenant broadcast consensus version", KR(ret), K(tenant_id), K(consensus_version));
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::set_tenant_received_broadcast_version(
    const uint64_t tenant_id,
    const int64_t version)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id), K(version));
  } else if (version != OB_CORE_SCHEMA_VERSION) {
    ObSchemaStore* schema_store = NULL;
    if (NULL == (schema_store = schema_store_map_.get(tenant_id))) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to get schema_store", K(ret));
    } else {
      schema_store->update_received_version(version);
      LOG_INFO("try to set tenant received_broadcast_version", K(ret), K(tenant_id), K(version));
    }
  } else {
    ret = OB_OLD_SCHEMA_VERSION;
  }
  return ret;
}

int ObMultiVersionSchemaService::get_last_refreshed_schema_info(ObRefreshSchemaInfo &schema_info)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(schema_info_rwlock_);
  if (OB_FAIL(schema_info.assign(last_refreshed_schema_info_))) {
    LOG_WARN("fail to assign schema info", K(ret), K(schema_info), K_(last_refreshed_schema_info));
  }
  return ret;
}

int ObMultiVersionSchemaService::set_last_refreshed_schema_info(const ObRefreshSchemaInfo &schema_info)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(schema_info_rwlock_);
  const uint64_t last_sequence_id = last_refreshed_schema_info_.get_sequence_id();
  const uint64_t new_sequence_id = schema_info.get_sequence_id();
  if (OB_INVALID_ID == new_sequence_id
      || (OB_INVALID_ID != last_sequence_id && last_sequence_id >= new_sequence_id)) {
    LOG_INFO("no need to set last refreshed schema info", K(ret), K(last_refreshed_schema_info_), K(schema_info));
  } else if (OB_FAIL(last_refreshed_schema_info_.assign(schema_info))) {
    LOG_WARN("fail to assign last refreshed schema info", K(ret), K(schema_info), K_(last_refreshed_schema_info));
  }
  return ret;
}

int ObMultiVersionSchemaService::gen_new_schema_version(
    uint64_t tenant_id,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  int64_t refreshed_schema_version = OB_INVALID_VERSION;
  schema_version = OB_INVALID_VERSION;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_refreshed_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("fail to get refreshed schema version", K(ret), K(tenant_id), K(refreshed_schema_version));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id, refreshed_schema_version, schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id), K(refreshed_schema_version));
  }
  return ret;
}

int ObMultiVersionSchemaService::gen_batch_new_schema_versions(
    const uint64_t tenant_id,
    const int64_t version_cnt,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  int64_t refreshed_schema_version = OB_INVALID_VERSION;
  schema_version = OB_INVALID_VERSION;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                 || version_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_refreshed_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("fail to get refreshed schema version", KR(ret), K(tenant_id), K(refreshed_schema_version));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_->gen_batch_new_schema_versions(
             tenant_id, refreshed_schema_version, version_cnt, schema_version))) {
    LOG_WARN("fail to gen new schema_version", KR(ret), K(tenant_id), K(version_cnt), K(refreshed_schema_version));
  }
  return ret;
}

int ObMultiVersionSchemaService::get_new_schema_version(uint64_t tenant_id, int64_t &schema_version) {
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret), K(tenant_id));
  } else {
    ret = schema_service_->get_new_schema_version(tenant_id, schema_version);
  }
  return ret;
}

int ObMultiVersionSchemaService::get_tenant_mem_info(
    const uint64_t &tenant_id,
    common::ObIArray<ObSchemaMemory> &tenant_mem_infos)
{
  int ret = OB_SUCCESS;
  ObSchemaMemMgr *mem_mgr = NULL;

  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mem_mgr_map_.get_refactored(tenant_id, mem_mgr))) {
    LOG_WARN("fail to get tenant mem_mgr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(mem_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_mgr is NULL", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mem_mgr->get_all_alloc_info(tenant_mem_infos))) {
    LOG_WARN("fail to get mem_mgr alloc info", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObMultiVersionSchemaService::get_tenant_slot_info(
    common::ObIAllocator &allocator,
    const uint64_t &tenant_id,
    common::ObIArray<ObSchemaSlot> &tenant_slot_infos)
{
  int ret = OB_SUCCESS;
  ObSchemaStore * schema_store = NULL;

  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_store = schema_store_map_.get(tenant_id))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to get schema_store", KR(ret), K(tenant_id));
  } else if (OB_FAIL((schema_store->schema_mgr_cache_).get_slot_info(allocator, tenant_slot_infos))) {
    LOG_WARN("fail tp get slot info from schema_mgr_cache", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObMultiVersionSchemaService::get_schema_store_tenants(common::ObIArray<uint64_t> &tenant_ids) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(schema_store_map_.get_all_tenant(tenant_ids))) {
    LOG_WARN("fail to get all_tenant from schema_store_map_", KR(ret));
  }
  return ret;
}

bool ObMultiVersionSchemaService::check_schema_store_tenant_exist(const uint64_t &tenant_id) {
  bool exist = true;
  ObSchemaStore* schema_store = NULL;
  if (OB_ISNULL(schema_store = schema_store_map_.get(tenant_id))) {
    exist = false;
  }
  return exist;
}

// If schema_status_array is empty, it means strong consistent reading
int ObMultiVersionSchemaService::get_schema_status(
    const ObArray<ObRefreshSchemaStatus> &schema_status_array,
    const uint64_t tenant_id,
    ObRefreshSchemaStatus &schema_status)
{
  int ret = OB_SUCCESS;
  bool finded = false;
  schema_status.reset();
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (schema_status_array.empty()) {
    schema_status.tenant_id_ = tenant_id;
  } else {
    FOREACH_X(schema_status_ptr, schema_status_array, !finded && OB_SUCC(ret)) {
      if (OB_ISNULL(schema_status_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_status is null", K(ret));
      } else if (schema_status_ptr->tenant_id_ == tenant_id) {
        finded = true;
        schema_status = *schema_status_ptr;
      }
    }
    if (OB_SUCC(ret) && !finded) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("tenant schema status not finded", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::get_schema_version_history(
    const ObRefreshSchemaStatus &fetch_schema_status,
    const uint64_t fetch_tenant_id,
    const int64_t schema_version,
    const VersionHisKey &key,
    VersionHisVal &val,
    bool &not_exist)
{
  int ret = OB_SUCCESS;
  not_exist = false;
  val.reset();
  if (!key.is_valid()
      || schema_version < 0
      || OB_INVALID_TENANT_ID == fetch_tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(key), K(fetch_tenant_id), K(schema_version));
  } else {
    int hash_ret = version_his_map_.get_refactored(key, val);
    if (hash_ret == OB_HASH_NOT_EXIST || schema_version > val.snapshot_version_) {
      int64_t snapshot_version = OB_INVALID_VERSION;
      if (OB_FAIL(get_tenant_refreshed_schema_version(fetch_tenant_id, snapshot_version))) {
        LOG_WARN("fail to get tenant refreshed schema_version", K(ret), K(fetch_tenant_id));
      } else if (OB_FAIL(construct_schema_version_history(fetch_schema_status, snapshot_version, key, val))) {
        LOG_WARN("construct failed", K(ret), K(snapshot_version), K(key));
      } else if (0 == val.valid_cnt_) {
        //FIXME: When the specified schema is too small, there is no corresponding record in the history,
        //  and a null pointer is returned.
        //       1) The history is complete, indicating that the schema for a given version does not exist;
        //       2) If history is reclaimed, it may also enter the branch, but considering the following points,
        //        it will not be processed for the time being:
        //       - 2.x not reclaim history
        //       - 1.4.x Even if history is reclaimed, it is also reclaiming long-awaited schema multi-version information,
        //        and the corresponding multi-version information is likely not to be relied on;
        //       - The reservoir fetches table_schema through retry_get_schema_guard, even if the incoming schema_version
        //        is too small, it will return the schema version that the table_schema exists for the first time
        not_exist = true;
        LOG_INFO("specific schema_version is small, schema not exist",
                 K(ret), K(fetch_schema_status), K(schema_version), K(snapshot_version));
      } else {
        if (OB_FAIL(version_his_map_.set_refactored(key, val, 1 /*overwrite val*/))) {
          LOG_WARN("set map failed", K(ret));
        } else {
          LOG_INFO("construct_schema_version_history succeed", K(key), K(val));
        }
      }
    }
  }
  return ret;
}

// Since the scenario where the standalone cluster is switched to the primary cluster cannot reset schema_status atomically,
// the schema_status of the primary cluster cannot be trusted.
// Taking into account the need to control schema playback after physical recovery,
// tenant_schema needs to be combined to determine whether schema_status needs to be used
int ObMultiVersionSchemaService::check_tenant_is_restore(
    ObSchemaGetterGuard *schema_guard,
    const uint64_t tenant_id,
    bool &is_restore)
{
  int ret = OB_SUCCESS;
  is_restore = false;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (is_sys_tenant(tenant_id)
             || is_meta_tenant(tenant_id)) {
    is_restore = false;
  } else if (OB_ISNULL(schema_guard)) {
    ObSchemaGetterGuard tmp_guard;
    if (OB_FAIL(get_tenant_schema_guard(OB_SYS_TENANT_ID, tmp_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(tmp_guard.check_tenant_is_restore(tenant_id, is_restore))) {
      LOG_WARN("fail to check tenant is restore", K(ret), K(tenant_id));
    }
  } else if (OB_FAIL(schema_guard->check_tenant_is_restore(tenant_id, is_restore))) {
    LOG_WARN("fail to check tenant is restore", K(ret), K(tenant_id));
  }
  return ret;
}

int ObMultiVersionSchemaService::check_restore_tenant_exist(
    const ObIArray<uint64_t> &tenant_ids,
    bool &exist)
{
  int ret = OB_SUCCESS;
  exist  = false;
  ObSchemaGetterGuard schema_guard;
  if (1 == tenant_ids.count() && OB_SYS_TENANT_ID == tenant_ids.at(0)) {
    exist = false;
  } else if (OB_FAIL(get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get sys schema_guard", K(ret));
  } else {
    ObArray<uint64_t> tmp_tenant_ids;
    if (tenant_ids.count() == 0 && OB_FAIL(schema_guard.get_tenant_ids(tmp_tenant_ids))) {
      LOG_WARN("fail to get tenant ids", K(ret));
    } else {
      const ObIArray<uint64_t> &tenants = 0 == tenant_ids.count() ? tmp_tenant_ids : tenant_ids;
      const ObSimpleTenantSchema *tenant = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && !exist && i < tenants.count(); i++) {
        const uint64_t tenant_id = tenants.at(i);
        if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant))) {
          LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
        } else if (OB_ISNULL(tenant)) {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("tenant is null", K(ret));
        } else if (tenant->is_restore()) {
          exist = true;
        }
      }
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::get_tenant_name_case_mode(
    const uint64_t tenant_id,
    ObNameCaseMode &name_case_mode)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObSimpleSysVariableSchema *sys_variable = NULL;
  bool is_oracle_mode = false;
  name_case_mode = OB_NAME_CASE_INVALID;
  if (is_sys_tenant(tenant_id)
      || is_meta_tenant(tenant_id)) {
    name_case_mode = OB_ORIGIN_AND_INSENSITIVE;
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K(tenant_id));
  } else if (is_oracle_mode) {
    // name_case_mode is incorrent and meaningless for oracle tenant.
    name_case_mode = OB_ORIGIN_AND_SENSITIVE;
  } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.get_sys_variable_schema(tenant_id, sys_variable))) {
    LOG_WARN("fail to get sys variable schema", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sys_variable)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("sys variable not exist", KR(ret), K(tenant_id));
  } else {
    name_case_mode = sys_variable->get_name_case_mode();
  }
  return ret;
}

// Obtain the least referenced schema version of the observer for schema recycling
int ObMultiVersionSchemaService::get_recycle_schema_version(
    const uint64_t tenant_id,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;
  const ObSchemaStore *schema_store = NULL;
  if (OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_store = schema_store_map_.get(tenant_id))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("schema store not exist", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_store->schema_mgr_cache_.get_recycle_schema_version(schema_version))) {
    LOG_WARN("fail to get recycle schema version", K(ret), K(schema_version));
  }
  if (OB_FAIL(ret)) {
    schema_version = OB_INVALID_VERSION;
  }
  return ret;
}

int ObMultiVersionSchemaService::fetch_link_table_schema(const ObDbLinkSchema *dblink_schema,
                                                         const ObString &database_name,
                                                         const ObString &table_name,
                                                         ObIAllocator &allocator,
                                                         ObTableSchema *&table_schema,
                                                         sql::ObSQLSessionInfo *session_info,
                                                         const ObString &dblink_name,
                                                         bool is_reverse_link,
                                                         uint64_t *current_scn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is NULL", K(ret));
  } else if (OB_FAIL(schema_service_->get_link_table_schema(dblink_schema,
                                                            database_name, table_name,
                                                            allocator, table_schema,
                                                            session_info,
                                                            dblink_name,
                                                            is_reverse_link,
                                                            current_scn))) {
    LOG_WARN("get link table schema failed", K(ret), K(is_reverse_link));
  }
  return ret;
}

int ObMultiVersionSchemaService::update_baseline_schema_version(
    const uint64_t tenant_id,
    const int64_t baseline_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t bl_schema_version = OB_INVALID_VERSION;
  ObSchemaStore* schema_store = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret), K(tenant_id));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_store = schema_store_map_.get(tenant_id))) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("fail to get schema_store", KR(ret), K(tenant_id));
  } else {
    bl_schema_version = schema_store->get_baseline_schema_version();
    if (baseline_schema_version < bl_schema_version) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(tenant_id),
               K(baseline_schema_version), K(bl_schema_version));
    } else {
      schema_store->update_baseline_schema_version(baseline_schema_version);
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::get_baseline_schema_version(
    const uint64_t tenant_id,
    bool auto_update,
    int64_t &baseline_schema_version)
{
  int ret = OB_SUCCESS;
  ObSchemaStore* schema_store = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret), K(tenant_id));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(schema_store = schema_store_map_.get(tenant_id))) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("fail to get schema_store", KR(ret), K(tenant_id));
  } else {
    baseline_schema_version = schema_store->get_baseline_schema_version();
    if (OB_INVALID_VERSION == baseline_schema_version && auto_update) {
      ObISQLClient &sql_client = *sql_proxy_;
      ObRefreshSchemaStatus schema_status;
      if (ObSchemaService::g_liboblog_mode_) {
        //FIXME:(yanmu.ztl) For now, liboblog won't fetch schema from standby cluster or restore tenant.
        schema_status.tenant_id_ = tenant_id;
        schema_status.snapshot_timestamp_ = OB_INVALID_VERSION;
        schema_status.readable_schema_version_ = OB_INVALID_VERSION;
      } else {
        ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
        if (OB_ISNULL(schema_status_proxy)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema_status_proxy is null", K(ret));
        } else if (OB_FAIL(schema_status_proxy->get_refresh_schema_status(tenant_id, schema_status))) {
          LOG_WARN("fail to get refresh schema status", KR(ret), K(schema_status));
        }
      }
      if (FAILEDx(schema_service_->get_baseline_schema_version(
                  sql_client, schema_status, baseline_schema_version))) {
        LOG_WARN("get baseline schema version failed", KR(ret), K(schema_status));
      } else if (baseline_schema_version < OB_INVALID_VERSION) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected baseline schema version",
                  KR(ret), K(schema_status), K(baseline_schema_version));
      } else {
        schema_store->update_baseline_schema_version(baseline_schema_version);
        LOG_INFO("fetch baseline schema version finish",
                 KR(ret), K(schema_status), K(baseline_schema_version));
      }
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::get_tablet_to_table_history(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t schema_version,
    ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  table_ids.reset();
  int64_t tablet_ids_cnt = tablet_ids.count();
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
             || tablet_ids_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(tablet_ids_cnt));
  } else if (OB_FAIL(table_ids.reserve(tablet_ids_cnt))) {
    LOG_WARN("fail to reserve array", KR(ret), K(tablet_ids_cnt));
  } else {
    // record idx of tablet_ids which can't get tablet-table from cache
    ObArray<int64_t> fetch_idxs;
    ObTabletCacheKey key;
    uint64_t table_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      const ObTabletID &tablet_id = tablet_ids.at(i);
      if (!tablet_id.is_valid_with_tenant(tenant_id)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tablet_id or tenant_id", KR(ret), K(tenant_id), K(tablet_id));
      } else if (tablet_id.is_inner_tablet()) {
        // case 1: inner tablet_id is equal to its table_id
        table_id = tablet_id.id();
      } else if (OB_FAIL(key.init(tenant_id, tablet_id, schema_version))) {
        LOG_WARN("fail to init key", KR(ret), K(tenant_id), K(tablet_id), K(schema_version));
      } else if (OB_FAIL(schema_cache_.get_tablet_cache(key, table_id))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get from cache", KR(ret), K(key));
        } else if (OB_FAIL(fetch_idxs.push_back(i))) { // overwrite ret
          LOG_WARN("fail to push back fetch tablet_ids", KR(ret), K(tenant_id), K(tablet_id), K(i));
        } else {
          // case 2: cache miss, fetch later
          table_id = OB_INVALID_ID; // occupancy
        }
      } else {
        // case 3: cache hit
        LOG_TRACE("fetch tablet-table from cache",
                  K(tenant_id), K(schema_version), K(tablet_id), K(table_id));
      }
      if (FAILEDx(table_ids.push_back(table_id))) {
        LOG_WARN("fail to push back table_id", KR(ret), K(tenant_id), K(tablet_id), K(table_id));
      }
    } // end for

    if (OB_SUCC(ret) && tablet_ids_cnt != table_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("array cnt not match", KR(ret), K(tenant_id),
               K(tablet_ids_cnt), "table_ids_cnt", table_ids.count());
    }

    if (OB_SUCC(ret) && fetch_idxs.count() > 0) {
      // init map
      const int64_t BUCKET_NUM = 10000;
      common::hash::ObHashMap<ObTabletID, uint64_t> tablet_map; // (tablet_id, table_id)
      if (OB_UNLIKELY(schema_version <= 0
          || !ObSchemaService::is_formal_version(schema_version))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(tablet_ids_cnt), K(schema_version));
      } else if (OB_FAIL(tablet_map.create(BUCKET_NUM, "TbtTbPair", "TbtTbPair"))) {
        LOG_WARN("fail to create hashmap", KR(ret), K(tenant_id));
      }

      // fetch result
      const int64_t EACH_BATCH_CNT = 1000;
      int64_t start_idx = 0;
      int64_t end_idx = min(fetch_idxs.count(), start_idx + EACH_BATCH_CNT);
      while (OB_SUCC(ret)
             && end_idx <= fetch_idxs.count()
             && end_idx - start_idx > 0) {
        if (OB_FAIL(batch_fetch_tablet_to_table_history_(
            tenant_id, tablet_ids, schema_version,
            fetch_idxs, start_idx, end_idx, tablet_map))) {
          LOG_WARN("fail to fetch tablet-table history", KR(ret), K(tenant_id), K(schema_version));
        } else {
          start_idx = end_idx;
          end_idx = min(fetch_idxs.count(), start_idx + EACH_BATCH_CNT);
        }
      } // end while

      // construct result
      for (int64_t i = 0; OB_SUCC(ret) && i < fetch_idxs.count(); i++) {
        int64_t idx = fetch_idxs.at(i);
        if (idx < 0 || idx >= tablet_ids_cnt) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("idx is invalid", KR(ret), K(tenant_id), K(idx), K(tablet_ids_cnt));
        } else {
          const ObTabletID &tablet_id = tablet_ids.at(idx);
          if (OB_FAIL(tablet_map.get_refactored(tablet_id, table_id))) {
            if (OB_HASH_NOT_EXIST != ret) {
              LOG_WARN("fail to get from map", KR(ret), K(tenant_id), K(tablet_id));
            } else {
              // Can't fetch from inner table. tablet-table history may be recycled or never exists.
              table_ids.at(idx) = OB_INVALID_ID;
              ret = OB_SUCCESS;
            }
          } else {
            table_ids.at(idx) = table_id;
            LOG_TRACE("fetch from tablet-table inner table",
                      K(tenant_id), K(schema_version), K(tablet_id), K(table_id));
          }
        }
      } // end for
    }
  }
  return ret;
}

// cal purge recyclebin need timeout
int ObMultiVersionSchemaService::cal_purge_need_timeout(
    const obrpc::ObPurgeRecycleBinArg &purge_recyclebin_arg,
    int64_t &cal_timeout)
{
  int ret = OB_SUCCESS;
  int64_t tmp_timeout = 0;
  int64_t total_purge_count = 0;
  ObArray<ObRecycleObject> recycle_objs;
  int64_t purge_num = purge_recyclebin_arg.purge_num_;
  const uint64_t tenant_id = purge_recyclebin_arg.tenant_id_;
  const int64_t expire_time = purge_recyclebin_arg.expire_time_;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is NULL", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is NULL", KR(ret));
  } else {
    rootserver::ObDDLOperator ddl_operator(*this, *sql_proxy_);
    if (OB_FAIL(ddl_operator.fetch_expire_recycle_objects(tenant_id, expire_time, recycle_objs))) {
      LOG_WARN("fail to get fetch expire recycle objects", KR(ret), K(purge_recyclebin_arg));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < recycle_objs.count() && total_purge_count < purge_num; i++) {
      const ObRecycleObject &recycle_obj = recycle_objs.at(i);
      switch(recycle_obj.get_type()) {
          case ObRecycleObject::VIEW:
          case ObRecycleObject::TABLE: {
            int64_t cal_table_timeout = 0;
            const uint64_t table_id = recycle_obj.get_table_id();
            if (OB_FAIL(cal_purge_table_timeout_(tenant_id, table_id, cal_table_timeout, total_purge_count))) {
              LOG_WARN("fail to cal purge table timeout", KR(ret), K(tenant_id), K(table_id));
            } else {
              tmp_timeout += cal_table_timeout;
            }
            break;
          }
          case ObRecycleObject::DATABASE: {
            int64_t cal_database_timeout = 0;
            const int64_t database_id = recycle_obj.get_database_id();
            if (OB_FAIL(cal_purge_database_timeout_(tenant_id, database_id, cal_database_timeout, total_purge_count))) {
              LOG_WARN("fail to cal purge database timeout", KR(ret));
            } else {
              tmp_timeout += cal_database_timeout;
            }
            break;
          }
          case ObRecycleObject::TENANT: {
            tmp_timeout += GCONF.rpc_timeout;
            total_purge_count++;
            break;
          }
          case ObRecycleObject::TRIGGER:
          case ObRecycleObject::INDEX:
          case ObRecycleObject::AUX_LOB_META:
          case ObRecycleObject::AUX_LOB_PIECE:
          case ObRecycleObject::AUX_VP: {
            continue;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unknown recycle object type", K(recycle_obj));
          }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t high_bound_timeout = 0;
    int64_t low_bound_timeout = 10 * GCONF.rpc_timeout;
    if (0 == total_purge_count) {
      cal_timeout = 0;
    // if this worker or ctxs' timeout not be set, use ddl timeout as high bound value
    } else if (OB_FAIL(ObShareUtil::get_ctx_timeout(GCONF._ob_ddl_timeout, high_bound_timeout))) {
      LOG_WARN("fail to set timeout", KR(ret));
    } else {
      // to prevent tmp_timeout is too small, use low_bound_timeout to compare
      tmp_timeout = std::max(low_bound_timeout, tmp_timeout);
      cal_timeout = std::min(high_bound_timeout, tmp_timeout);
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::cal_purge_table_timeout_(
    const uint64_t &tenant_id,
    const uint64_t &table_id,
    int64_t &cal_table_timeout,
    int64_t &total_purge_count)
{
  int ret = OB_SUCCESS;
  int64_t part_num = 0;
  cal_table_timeout = 0;
  ObArray<uint64_t> table_ids;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *orig_table_schema = NULL;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;

  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
            || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is not invalid", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema_gaurd", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, orig_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(orig_table_schema)) {
    // ignore
  } else if (OB_FAIL(orig_table_schema->get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos", KR(ret), K(*orig_table_schema));
  } else {
    total_purge_count++;
    part_num = orig_table_schema->get_all_part_num();
    ObIndexType index_type = INDEX_TYPE_IS_NOT;
    ObTableType table_type = MAX_TABLE_TYPE;
    // get all index table id
    int64_t index_count = simple_index_infos.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < index_count; ++i) {
      index_type = simple_index_infos.at(i).index_type_;
      table_type = simple_index_infos.at(i).table_type_;
      if (index_has_tablet(index_type)) {
        if (OB_FAIL(table_ids.push_back(simple_index_infos.at(i).table_id_))) {
          LOG_WARN("failed to push index id to index_ids",
                  KR(ret), K(i), K(simple_index_infos.at(i).table_id_));
        }
      }
    }
    // get lob table id
    if (OB_SUCC(ret) && orig_table_schema->has_lob_aux_table()) {
      uint64_t mtid = orig_table_schema->get_aux_lob_meta_tid();
      uint64_t ptid = orig_table_schema->get_aux_lob_piece_tid();
      if (OB_INVALID_ID == mtid || OB_INVALID_ID == ptid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Expect meta tid and piece tid valid",
                KR(ret), K(mtid), K(ptid));
      } else if (OB_FAIL(table_ids.push_back(mtid))) {
        LOG_WARN("fail to push back lob meta tid", KR(ret), K(mtid));
      } else if (OB_FAIL(table_ids.push_back(ptid))) {
        LOG_WARN("fail to push back lob piece tid", KR(ret), K(ptid));
      }
    }
    // get vp table
    if (OB_SUCC(ret)) {
      ObSEArray<uint64_t, 16> aux_tid_array; // for aux_vp or aux_lob
      if (OB_FAIL(orig_table_schema->get_aux_vp_tid_array(aux_tid_array))) {
        LOG_WARN("get_aux_vp_tid_array failed", K(ret), KPC(orig_table_schema));
      } else {
        int64_t array_count = aux_tid_array.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < array_count; i++) {
          uint64_t table_id = aux_tid_array.at(i);
          if (OB_FAIL(table_ids.push_back(table_id))) {
            LOG_WARN("fail to push back vp", KR(ret), K(table_id));
          }
        }
      }
    }
    // cal tablet cost
    if (OB_SUCC(ret) && 0 != table_ids.count()) {
      const ObSimpleTableSchemaV2 *tmp_table_schema = NULL;
      const int64_t table_count = table_ids.count();

      for (int64_t i = 0; OB_SUCC(ret) && i < table_count; ++i) {
        int64_t table_id = table_ids.at(i);
        if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, table_id, tmp_table_schema))) {
          LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
        } else if (OB_ISNULL(tmp_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema is NULL", KR(ret), K(tenant_id), K(table_id));
        } else {
          part_num += tmp_table_schema->get_all_part_num();
        }
      }
    }
    // has autoinc
    if (OB_SUCC(ret) && 0 != orig_table_schema->get_autoinc_column_id()) {
      cal_table_timeout += GCONF.rpc_timeout;
    }
    // has sequence
    if (OB_SUCC(ret) && (orig_table_schema->is_user_table() || orig_table_schema->is_oracle_tmp_table())) {
      for (ObTableSchema::const_column_iterator iter = orig_table_schema->column_begin();
          OB_SUCC(ret) && iter != orig_table_schema->column_end(); ++iter) {
        ObColumnSchemaV2 *column_schema = *iter;
        if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column schema is NULL", KR(ret), K(tenant_id), K(table_id));
        } else if (column_schema->is_identity_column()) {
          cal_table_timeout += GCONF.rpc_timeout;
        }
      }
    }
    // has rls
    if (OB_SUCC(ret)) {
      cal_table_timeout += orig_table_schema->get_rls_policy_ids().count() * GCONF.rpc_timeout;
      cal_table_timeout += orig_table_schema->get_rls_group_ids().count() * GCONF.rpc_timeout;
      cal_table_timeout += orig_table_schema->get_rls_context_ids().count() * GCONF.rpc_timeout;
    }
    // has audit
    if (OB_SUCC(ret) && (orig_table_schema->is_user_table() || orig_table_schema->is_external_table())) {
      ObArray<const ObSAuditSchema *> audits;
      uint64_t table_id = orig_table_schema->get_table_id();
      if (OB_FAIL(schema_guard.get_audit_schema_in_owner(tenant_id, AUDIT_TABLE, table_id, audits))) {
        LOG_WARN("fail to get audit schema in owner", KR(ret), K(tenant_id), K(table_id));
      } else {
        cal_table_timeout += audits.count() * GCONF.rpc_timeout;
      }
    }
    // has trigger
    if (OB_SUCC(ret)) {
      const ObIArray<uint64_t> &trigger_id_list = orig_table_schema->get_trigger_list();
      cal_table_timeout += trigger_id_list.count() * GCONF.rpc_timeout;
    }
    if (OB_SUCC(ret)) {
      //100 tablet 2s,default 2s
      cal_table_timeout += (part_num / 100 + (part_num % 100 == 0 ? 0 : 1)) * GCONF.rpc_timeout;
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::cal_purge_database_timeout_(
    const uint64_t &tenant_id,
    const uint64_t &database_id,
    int64_t &cal_database_timeout,
    int64_t &total_purge_count)
{
  int ret = OB_SUCCESS;
  int64_t part_num = 0;
  cal_database_timeout = 0;
  ObSchemaGetterGuard schema_guard;
  ObArray<ObRecycleObject> recycle_objs;
  bool need_cal_timeout = true;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
            || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is not valid", KR(ret), K(tenant_id), K(database_id));
  } else if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id), K(database_id));
  } else {
    const ObSimpleDatabaseSchema *database_schema = NULL;
    if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_id, database_schema))) {
      LOG_WARN("fail to get database schema", KR(ret), K(tenant_id), K(database_id));
    } else if (OB_ISNULL(database_schema)) {
      need_cal_timeout = false;
    }
  }
  if (OB_SUCC(ret) && need_cal_timeout) {
    total_purge_count++;
    schema_guard.reset();
    // database itself
    cal_database_timeout += GCONF.rpc_timeout;
    // cal table which is already in recyclebin
    if (OB_FAIL(schema_service_->fetch_recycle_objects_of_db(tenant_id,
                                                            database_id,
                                                            *sql_proxy_,
                                                            recycle_objs))) {
      LOG_WARN("fetch recycle objects of db failed", KR(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < recycle_objs.count(); ++i) {
        int64_t tmp_count = 0;
        int64_t tmp_table_timeout = 0;
        const ObRecycleObject &recycle_obj = recycle_objs.at(i);
        const uint64_t table_id = recycle_obj.get_table_id();
        if (OB_FAIL(cal_purge_table_timeout_(tenant_id, table_id, tmp_table_timeout, tmp_count))) {
          LOG_WARN("fail to cal purge table timeout", KR(ret), K(tenant_id), K(table_id));
        } else {
          cal_database_timeout += tmp_table_timeout;
        }
      }
    }
    // to prevent schema memory hang, we should use get_tenant_schema_guard to reuse memory
    // cal delete tables in database
    if (OB_SUCC(ret)) {
      ObArray<uint64_t> table_ids;
      if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_table_ids_in_database(tenant_id, database_id, table_ids))) {
        LOG_WARN("get tables in database failed", KR(ret), K(tenant_id), K(database_id));
      } else {
        schema_guard.reset();
        for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
          int64_t tmp_count = 0;
          int64_t tmp_table_timeout = 0;
          uint64_t table_id = table_ids.at(i);
          if (OB_FAIL(cal_purge_table_timeout_(tenant_id, table_id, tmp_table_timeout, tmp_count))) {
            LOG_WARN("fail to get purge table timeout", KR(ret), K(tenant_id), K(table_id));
          } else {
            cal_database_timeout += tmp_table_timeout;
          }
        }
      }
    }
    // cal outline
    if (OB_SUCC(ret)) {
      ObArray<const ObSimpleOutlineSchema *> outlines;
      if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_simple_outline_schemas_in_database(tenant_id, database_id, outlines))) {
        LOG_WARN("fail to get outlines in database failed", KR(ret), K(tenant_id), K(database_id));
      } else {
        cal_database_timeout += outlines.count() * GCONF.rpc_timeout;
      }
    }
    // cal synonyms
    if (OB_SUCC(ret)) {
      ObArray<const ObSimpleSynonymSchema *> synonyms;
      if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_simple_synonym_schemas_in_database(tenant_id, database_id, synonyms))) {
        LOG_WARN("fail to get synonym in database failed", KR(ret), K(tenant_id), K(database_id));
      } else {
        cal_database_timeout += synonyms.count() * GCONF.rpc_timeout;
      }
    }
    // cal packags
    if (OB_SUCC(ret)) {
      ObArray<const ObSimplePackageSchema *> packages;
      if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_simple_package_schemas_in_database(tenant_id, database_id, packages))) {
        LOG_WARN("fail to get packages in database failed", KR(ret), K(tenant_id), K(database_id));
      } else {
        cal_database_timeout += packages.count() * GCONF.rpc_timeout;
      }
    }
    // cal routines
    if (OB_SUCC(ret)) {
      ObArray<const ObSimpleRoutineSchema *> routines;
      if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_simple_routine_schemas_in_database(tenant_id, database_id, routines))) {
        LOG_WARN("fail to get routines in database failed", KR(ret), K(tenant_id), K(database_id));
      } else {
        cal_database_timeout += routines.count() * GCONF.rpc_timeout;
      }
    }
    // cal udts
    if (OB_SUCC(ret)) {
      ObArray<const ObSimpleUDTSchema *> udts;
      if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_simple_udt_schemas_in_database(tenant_id, database_id, udts))) {
        LOG_WARN("fail to get udts in database failed", KR(ret), K(tenant_id), K(database_id));
      } else {
        cal_database_timeout += udts.count() * GCONF.rpc_timeout;
      }
    }
    // cal sequences
    if (OB_SUCC(ret)) {
      ObArray<const ObSequenceSchema *> sequences;
      if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_sequence_infos_in_database(tenant_id, database_id, sequences))) {
        LOG_WARN("fail to get sequences in database failed", KR(ret), K(tenant_id), K(database_id));
      } else {
        cal_database_timeout += sequences.count() * GCONF.rpc_timeout;
      }
    }
    // cal mock_fk
    if (OB_SUCC(ret)) {
      ObArray<const ObSimpleMockFKParentTableSchema *> mock_fk_parent_table_schemas;
      if (OB_FAIL(get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_simple_mock_fk_parent_table_schemas_in_database(tenant_id, database_id, mock_fk_parent_table_schemas))) {
        LOG_WARN("fail to get mock_fk_parent_table_schemas in database failed", KR(ret), K(tenant_id), K(database_id));
      } else {
        cal_database_timeout += mock_fk_parent_table_schemas.count() * GCONF.rpc_timeout;
      }
    }
  }
  return ret;
}

int ObMultiVersionSchemaService::batch_fetch_tablet_to_table_history_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const int64_t schema_version,
    const ObIArray<int64_t> &tablet_idxs,
    const int64_t start_idx,
    const int64_t end_idx,
    ObHashMap<ObTabletID, uint64_t> &tablet_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
             || start_idx < 0
             || end_idx - start_idx <= 0
             || end_idx > tablet_idxs.count()
             || tablet_ids.count() <= 0
             || tablet_idxs.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id), K(start_idx), K(end_idx),
             "tablet_ids_cnt", tablet_ids.count(), "tablet_idxs_cnt", tablet_idxs.count());
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;
      ObSqlString tablet_ids_sql;
      for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; i++) {
        int64_t idx = tablet_idxs.at(i);
        if (idx < 0 || idx >= tablet_ids.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("idx is invalid", KR(ret), K(tenant_id), K(idx),
                   "tablet_ids_cnt", tablet_ids.count());
        } else if (OB_FAIL(tablet_ids_sql.append_fmt("%s%lu",
                   i == start_idx ? "" : ", ", tablet_ids.at(idx).id()))) {
          LOG_WARN("fail to append sql", KR(ret), K(tenant_id), K(idx));
        }
      } // end for

      if (FAILEDx(sql.assign_fmt(
          "SELECT * FROM (SELECT *, row_number() "
          "OVER (PARTITION BY tenant_id, tablet_id ORDER BY schema_version DESC) AS row_num "
          "FROM %s WHERE tenant_id = %lu AND tablet_id in (%.*s) AND schema_version <= %ld) "
          "WHERE row_num = 1",
          OB_ALL_TABLET_TO_TABLE_HISTORY_TNAME,
          ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
          tablet_ids_sql.string().length(),
          tablet_ids_sql.string().ptr(),
          schema_version))) {
        LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(schema_version));
      } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), K(tenant_id), K(sql));
      } else {
        ObTabletID tablet_id;
        uint64_t id = OB_INVALID_ID;
        uint64_t table_id = OB_INVALID_ID;
        bool is_deleted = false;
        ObTabletCacheKey key;
        ObTabletCacheValue value;
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          EXTRACT_INT_FIELD_MYSQL(*result, "is_deleted", is_deleted, bool);
          EXTRACT_INT_FIELD_MYSQL(*result, "tablet_id", id, uint64_t);
          EXTRACT_INT_FIELD_MYSQL_WITH_TENANT_ID(*result, "table_id", table_id, tenant_id);
          if (OB_SUCC(ret) && is_deleted) { // tablet has been dropped
            table_id = OB_INVALID_ID;
          }
          tablet_id = id;
          if (FAILEDx(key.init(tenant_id, tablet_id, schema_version))) {
            LOG_WARN("fail to init key", KR(ret), K(tenant_id), K(tablet_id), K(schema_version));
          } else if (OB_FAIL(tablet_map.set_refactored(tablet_id, table_id))) { // won't overwrite
            LOG_WARN("fail to set tablet-table pair", KR(ret), K(tenant_id), K(tablet_id), K(table_id));
          } else if (OB_FAIL(schema_cache_.put_tablet_cache(key, table_id))) {
            LOG_WARN("fail to put tablet cache", KR(ret), K(key), K(table_id));
          }
        } // end while

        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
          LOG_WARN("fail to get result", KR(ret), K(tenant_id), K(sql));
        }
      }
    } // end SMART_VAR
  }
  return ret;
}

int ObMultiVersionSchemaService::get_dropped_tenant_ids(
    common::ObIArray<uint64_t> &dropped_tenant_ids)
{
  int ret = OB_SUCCESS;
  dropped_tenant_ids.reset();
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else {
    SpinRLockGuard guard(schema_manager_rwlock_);
    ObSchemaGetterGuard schema_guard;
    if (OB_UNLIKELY(!is_tenant_full_schema(OB_SYS_TENANT_ID))) {
      ret = OB_NOT_INIT;
      LOG_WARN("sys schema is not full", KR(ret));
    } else if (OB_FAIL(get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("get sys tenant schema guard failed", KR(ret));
    } else if (OB_FAIL(schema_guard.get_dropped_tenant_ids(dropped_tenant_ids))) {
      LOG_WARN("get dropped tenant ids failed", KR(ret));
    }
  }
  return ret;
}

}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase
