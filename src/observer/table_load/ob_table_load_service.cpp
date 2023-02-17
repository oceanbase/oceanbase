// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <suzhi.yt@oceanbase.com>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace lib;
using namespace share::schema;
using namespace table;

/**
 * ObGCTask
 */

int ObTableLoadService::ObGCTask::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadService::ObGCTask init twice", KR(ret), KP(this));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObTableLoadService::ObGCTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadService::ObGCTask not init", KR(ret), KP(this));
  } else {
    LOG_DEBUG("table load start gc", K(tenant_id_));
    ObTableLoadManager &manager = service_.get_manager();
    ObArray<ObTableLoadTableCtx *> inactive_table_ctx_array;
    if (OB_FAIL(manager.get_inactive_table_ctx_list(inactive_table_ctx_array))) {
      LOG_WARN("fail to get inactive table ctx list", KR(ret), K(tenant_id_));
    }
    for (int64_t i = 0; i < inactive_table_ctx_array.count(); ++i) {
      ObTableLoadTableCtx *table_ctx = inactive_table_ctx_array.at(i);
      const uint64_t table_id = table_ctx->param_.table_id_;
      const uint64_t hidden_table_id = table_ctx->ddl_param_.dest_table_id_;
      // check if table ctx is removed
      if (table_ctx->is_dirty()) {
        LOG_DEBUG("table load ctx is dirty", K(tenant_id_), "table_id", table_ctx->param_.table_id_,
                  "ref_count", table_ctx->get_ref_count());
      }
      // check if table ctx is activated
      else if (table_ctx->get_ref_count() > 1) {
        LOG_DEBUG("table load ctx is active", K(tenant_id_), "table_id",
                  table_ctx->param_.table_id_, "ref_count", table_ctx->get_ref_count());
      }
      // check if table ctx can be recycled
      else {
        ObSchemaGetterGuard schema_guard;
        const ObTableSchema *table_schema = nullptr;
        if (hidden_table_id == OB_INVALID_ID) {
          LOG_INFO("hidden table has not been created, gc table load ctx", K(tenant_id_),
                   K(table_id), K(hidden_table_id));
          ObTableLoadService::remove_ctx(table_ctx);
        } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(tenant_id_, hidden_table_id,
                                                               schema_guard, table_schema))) {
          if (OB_UNLIKELY(OB_TABLE_NOT_EXIST != ret)) {
            LOG_WARN("fail to get table schema", KR(ret), K(tenant_id_), K(hidden_table_id));
          } else {
            LOG_INFO("hidden table not exist, gc table load ctx", K(tenant_id_), K(table_id),
                     K(hidden_table_id));
            ObTableLoadService::remove_ctx(table_ctx);
          }
        } else if (table_schema->is_in_recyclebin()) {
          LOG_INFO("hidden table is in recyclebin, gc table load ctx", K(tenant_id_), K(table_id),
                   K(hidden_table_id));
          ObTableLoadService::remove_ctx(table_ctx);
        } else {
          LOG_DEBUG("table load ctx is running", K(tenant_id_), K(table_id), K(hidden_table_id));
        }
      }
      manager.put_table_ctx(table_ctx);
    }
  }
}

/**
 * ObReleaseTask
 */

int ObTableLoadService::ObReleaseTask::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadService::ObReleaseTask init twice", KR(ret), KP(this));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObTableLoadService::ObReleaseTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadService::ObReleaseTask not init", KR(ret), KP(this));
  } else {
    LOG_DEBUG("table load start release", K(tenant_id_));
    ObArray<ObTableLoadTableCtx *> releasable_table_ctx_array;
    if (OB_FAIL(service_.manager_.get_releasable_table_ctx_list(releasable_table_ctx_array))) {
      LOG_WARN("fail to get releasable table ctx list", KR(ret), K(tenant_id_));
    }
    for (int64_t i = 0; i < releasable_table_ctx_array.count(); ++i) {
      ObTableLoadTableCtx *table_ctx = releasable_table_ctx_array.at(i);
      const uint64_t table_id = table_ctx->param_.table_id_;
      const uint64_t hidden_table_id = table_ctx->ddl_param_.dest_table_id_;
      LOG_INFO("free table ctx", K(tenant_id_), K(table_id), K(hidden_table_id), KP(table_ctx));
      ObTableLoadService::free_ctx(table_ctx);
    }
  }
}

/**
 * ObTableLoadService
 */

int ObTableLoadService::mtl_init(ObTableLoadService *&service)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_ISNULL(service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(service));
  } else if (OB_FAIL(service->init(tenant_id))) {
    LOG_WARN("fail to init table load service", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTableLoadService::check_support_direct_load(uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_id));
  } else {
    const uint64_t tenant_id = MTL_ID();
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(
          ObTableLoadSchema::get_table_schema(tenant_id, table_id, schema_guard, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
    } else if (OB_FAIL(ObTableLoadSchema::check_constraints(schema_guard, table_schema))) {
      LOG_WARN("fail to check schema constraints", KR(ret), K(tenant_id), K(table_id));
    }
  }
  return ret;
}

ObTableLoadTableCtx *ObTableLoadService::alloc_ctx()
{
  return OB_NEW(ObTableLoadTableCtx, ObMemAttr(MTL_ID(), "TLD_TableCtxVal"));
}

void ObTableLoadService::free_ctx(ObTableLoadTableCtx *table_ctx)
{
  if (OB_NOT_NULL(table_ctx)) {
    OB_DELETE(ObTableLoadTableCtx, "TLD_TableCtxVal", table_ctx);
    table_ctx = nullptr;
  }
}

int ObTableLoadService::add_ctx(ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ObTableLoadUniqueKey key(table_ctx->param_.table_id_, table_ctx->ddl_param_.task_id_);
    ret = service->get_manager().add_table_ctx(key, table_ctx);
  }
  return ret;
}

int ObTableLoadService::remove_ctx(ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ObTableLoadUniqueKey key(table_ctx->param_.table_id_, table_ctx->ddl_param_.task_id_);
    ret = service->get_manager().remove_table_ctx(key);
  }
  return ret;
}

int ObTableLoadService::get_ctx(const ObTableLoadUniqueKey &key, ObTableLoadTableCtx *&table_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ret = service->get_manager().get_table_ctx(key, table_ctx);
  }
  return ret;
}

int ObTableLoadService::get_ctx(const ObTableLoadKey &key, ObTableLoadTableCtx *&table_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ret = service->get_manager().get_table_ctx_by_table_id(key.table_id_, table_ctx);
  }
  return ret;
}

void ObTableLoadService::put_ctx(ObTableLoadTableCtx *table_ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    service->get_manager().put_table_ctx(table_ctx);
  }
}

ObTableLoadService::ObTableLoadService()
  : gc_task_(*this), release_task_(*this), is_inited_(false)
{
}

int ObTableLoadService::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadService init twice", KR(ret), KP(this));
  } else if (OB_FAIL(manager_.init())) {
    LOG_WARN("fail to init table ctx manager", KR(ret));
  } else if (OB_FAIL(gc_task_.init(tenant_id))) {
    LOG_WARN("fail to init gc task", KR(ret));
  } else if (OB_FAIL(release_task_.init(tenant_id))) {
    LOG_WARN("fail to init release task", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadService not init", KR(ret), KP(this));
  } else {
    gc_timer_.set_run_wrapper(MTL_CTX());
    if (OB_FAIL(gc_timer_.init("TLD_GC"))) {
      LOG_WARN("fail to init gc timer", KR(ret));
    } else if (OB_FAIL(gc_timer_.schedule(gc_task_, GC_INTERVAL, true))) {
      LOG_WARN("fail to schedule gc task", KR(ret));
    } else if (OB_FAIL(gc_timer_.schedule(release_task_, RELEASE_INTERVAL, true))) {
      LOG_WARN("fail to schedule release task", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadService::stop()
{
  int ret = OB_SUCCESS;
  gc_timer_.stop();
  return ret;
}

void ObTableLoadService::wait()
{
  gc_timer_.wait();
}

void ObTableLoadService::destroy()
{
  is_inited_ = false;
  gc_timer_.destroy();
}

} // namespace observer
} // namespace oceanbase
