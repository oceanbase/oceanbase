// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <suzhi.yt@oceanbase.com>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_schema.h"
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
    auto fn = [this](uint64_t table_id, ObTableLoadTableCtx *) -> bool {
      int ret = OB_SUCCESS;
      ObTableLoadTableCtx *table_ctx = nullptr;
      if (OB_FAIL(service_.get_table_ctx(table_id, table_ctx))) {
      } else if (table_ctx->is_dirty()) {
        LOG_DEBUG("table load ctx is dirty", K(tenant_id_), K(table_id), "ref_count", table_ctx->get_ref_count());
      } else if (table_ctx->get_ref_count() > 1) {
        // wait all task exit
      } else {
        ObSchemaGetterGuard schema_guard;
        const ObTableSchema *table_schema = nullptr;
        uint64_t target_table_id = table_ctx->param_.target_table_id_;
        if (target_table_id == OB_INVALID_ID) {
          // do nothing because hidden table has not been created
        } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(tenant_id_,
                                                               target_table_id,
                                                               schema_guard,
                                                               table_schema))) {
          if (OB_UNLIKELY(OB_TABLE_NOT_EXIST != ret)) {
            LOG_WARN("fail to get table schema", KR(ret), K(tenant_id_), K(target_table_id));
          } else {
            // table not exist, gc table load ctx
            LOG_DEBUG("table not exist gc table load ctx", K(tenant_id_), K(target_table_id));
            service_.remove_table_ctx(table_ctx);
          }
        } else if (table_schema->is_in_recyclebin()) {
          // table in recyclebin, gc table load ctx
          LOG_DEBUG("table is in recyclebin gc table load ctx", K(tenant_id_), K(target_table_id));
          service_.remove_table_ctx(table_ctx);
        } else {
          LOG_DEBUG("table load ctx is running", K(target_table_id));
        }
      }
      if (OB_NOT_NULL(table_ctx)) {
        service_.put_table_ctx(table_ctx);
        table_ctx = nullptr;
      }
      return true;
    };
    service_.table_ctx_manager_.for_each(fn);
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
    ObArray<ObTableLoadTableCtx *> releasable_ctx_array;
    {
      ObMutexGuard guard(service_.mutex_);
      ObTableLoadTableCtx *table_ctx = nullptr;
      DLIST_FOREACH_REMOVESAFE(table_ctx, service_.dirty_list_)
      {
        if (table_ctx->get_ref_count() > 0) {
          // wait all task exit
        } else if (OB_FAIL(releasable_ctx_array.push_back(table_ctx))) {
          LOG_WARN("fail to push back", KR(ret));
        } else {
          abort_unless(OB_NOT_NULL(service_.dirty_list_.remove(table_ctx)));
        }
      }
    }
    for (int64_t i = 0; i < releasable_ctx_array.count(); ++i) {
      ObTableLoadTableCtx *table_ctx = releasable_ctx_array.at(i);
      LOG_INFO("free table ctx", KP(table_ctx));
      service_.table_ctx_manager_.free_value(table_ctx);
    }
  }
}

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

int ObTableLoadService::create_ctx(const ObTableLoadParam &param, ObTableLoadTableCtx *&ctx,
                                   bool &is_new)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ret = service->create_table_ctx(param, ctx, is_new);
  }
  return ret;
}

int ObTableLoadService::get_ctx(const ObTableLoadKey &key, ObTableLoadTableCtx *&ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ret = service->get_table_ctx(key.table_id_, ctx);
  }
  return ret;
}

void ObTableLoadService::put_ctx(ObTableLoadTableCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    service->put_table_ctx(ctx);
  }
}

int ObTableLoadService::remove_ctx(ObTableLoadTableCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ret = service->remove_table_ctx(ctx);
  }
  return ret;
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
  } else if (OB_FAIL(table_ctx_manager_.init())) {
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
    if (OB_FAIL(gc_timer_.init("TableLoadGc"))) {
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

int ObTableLoadService::create_table_ctx(const ObTableLoadParam &param, ObTableLoadTableCtx *&ctx,
                                         bool &is_new)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param));
  } else {
    if (OB_FAIL(table_ctx_manager_.get_or_new(param.table_id_, ctx, is_new, param))) {
      LOG_WARN("fail to new and insert table ctx", KR(ret));
    } else if (OB_ISNULL(ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null table ctx", KR(ret), K(param));
    } else if (OB_UNLIKELY(ctx->is_dirty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dirty table ctx", KR(ret));
    } else {
      ctx->inc_ref_count();
    }
  }
  return ret;
}

int ObTableLoadService::get_table_ctx(uint64_t table_id, ObTableLoadTableCtx *&ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_id));
  } else if (OB_FAIL(table_ctx_manager_.get(table_id, ctx))) {
    LOG_WARN("fail to get table ctx", KR(ret), K(table_id));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null table ctx", KR(ret), K(table_id));
  } else if (OB_UNLIKELY(ctx->is_dirty())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("table ctx is dirty", KR(ret));
  } else {
    ctx->inc_ref_count();
  }
  return ret;
}

void ObTableLoadService::put_table_ctx(ObTableLoadTableCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(ctx));
  } else {
    abort_unless(ctx->dec_ref_count() >= 0);
  }
}

int ObTableLoadService::remove_table_ctx(ObTableLoadTableCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(ctx));
  } else if (OB_UNLIKELY(ctx->is_dirty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dirty table ctx", KR(ret), KP(ctx));
  } else {
    ctx->set_dirty();
    if (OB_FAIL(table_ctx_manager_.remove(ctx->param_.table_id_, ctx))) {
      LOG_WARN("fail to remove table ctx", KR(ret));
    } else {
      ObMutexGuard guard(mutex_);
      abort_unless(dirty_list_.add_last(ctx));
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
