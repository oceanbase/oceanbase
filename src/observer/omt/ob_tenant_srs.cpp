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

#define USING_LOG_PREFIX SERVER_OMT
#include "ob_tenant_srs.h"
#include "ob_tenant_srs.h"
#include "lib/string/ob_sql_string.h"
#include "common/ob_smart_var.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/ob_thread_mgr.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_struct.h"
#include "lib/geo/ob_geo_utils.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;

namespace oceanbase
{
namespace omt
{

int ObTenantSrs::mtl_init(ObTenantSrs* &tenant_srs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tenant_srs->init())) {
    LOG_WARN("fail to init tenant srs", K(ret));
  }
  return ret;
}

int ObTenantSrs::start()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant srs isn't inited", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(), srs_update_periodic_task_, 0, false))) {
    LOG_WARN("failed to schedule tenant srs update task", K(ret));
  }
  return ret;
}

void ObTenantSrs::stop()
{
  if (OB_LIKELY(inited_)) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), srs_update_periodic_task_);
  }
}

void ObTenantSrs::wait()
{
  if (OB_LIKELY(inited_)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), srs_update_periodic_task_);
  }
}

void ObTenantSrs::destroy()
{
  if (OB_LIKELY(inited_)) {
    cancle_update_task();
    recycle_old_snapshots();
    recycle_last_snapshots();
    allocator_.~ObFIFOAllocator();
  }
}

int ObTenantSrs::init()
{
  int ret = OB_SUCCESS;
  sql_proxy_ = GCTX.sql_proxy_;
  lib::ObMemAttr mem_attr(MTL_ID(), "TenantSrs");
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantSrs init twice.", K(ret));
  } else if (FALSE_IT(alloc_.set_tenant_id(MTL_ID()))) {
  } else if (OB_FAIL(allocator_.init(&alloc_, OB_MALLOC_MIDDLE_BLOCK_SIZE, mem_attr))) {
    LOG_WARN("ObTenantSrs allocator init failed.", K(ret));
  } else {
    page_allocator_.set_allocator(&allocator_);
    page_allocator_.set_attr(mem_attr);
    mode_arena_.init(DEFAULT_PAGE_SIZE, page_allocator_);
    if (OB_FAIL(srs_update_periodic_task_.init(this))) {
      LOG_WARN("failed to init srs update task", K(ret));
    } else {
      inited_ = true;
      infinite_plane_.minX_ = INT32_MIN;
      infinite_plane_.minY_ = INT32_MIN;
      infinite_plane_.maxX_ = INT32_MAX;
      infinite_plane_.maxY_ = INT32_MAX;
    }
  }
  return ret;
}

ObSrsCacheGuard::~ObSrsCacheGuard()
{
  if (OB_NOT_NULL(srs_cache_)) {
    srs_cache_->dec_ref_count();
  }
}

int ObSrsCacheGuard::get_srs_item(uint64_t srs_id, const ObSrsItem *&srs_item)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *tmp_srs_item = NULL;
  if (OB_ISNULL(srs_cache_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("srs_cache is null", K(ret));
  } else if (srs_id > UINT_MAX32) {
    ret = OB_ERR_WARN_DATA_OUT_OF_RANGE;
    LOG_WARN("srs id out of range", K(ret), K(srs_id));
  } else if (OB_SUCC(srs_cache_->get_srs_item(srs_id, tmp_srs_item))) {
    srs_item = tmp_srs_item;
  } else {
    LOG_WARN("failed to find srs item", K(ret), K(srs_id));
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_ERR_SRS_NOT_FOUND;
      LOG_USER_ERROR(OB_ERR_SRS_NOT_FOUND, static_cast<uint32_t>(srs_id));
    }
  }
  return ret;
}

int ObTenantSrs::get_tenant_srs_guard(ObSrsCacheGuard &srs_guard)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_get_last_snapshot(srs_guard))) {
    if (ret == OB_ERR_EMPTY_QUERY) {
      ret = OB_ERR_SRS_EMPTY;
      LOG_WARN("srs table might be empty", K(ret), K(MTL_ID()));
      LOG_USER_ERROR(OB_ERR_SRS_EMPTY);
    } else {
      LOG_WARN("failed to get tenant srs", K(ret), K(MTL_ID()));
    }
  }
  return ret;
}

int ObTenantSrs::get_srs_bounds(uint64_t srid, const ObSrsItem *srs_item, const ObSrsBoundsItem *&bounds_item)
{
  int ret = OB_SUCCESS;
  if (srid == 0) {
    bounds_item = &infinite_plane_;
  } else if (OB_ISNULL(srs_item)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_ERROR("srs item is null", K(ret));
  } else {
    const ObSrsBoundsItem *tmp_bounds = srs_item->get_bounds();
    if (isnan(tmp_bounds->minX_) || isnan(tmp_bounds->minY_)
        || isnan(tmp_bounds->maxX_) || isnan(tmp_bounds->maxY_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid bounds info", K(ret), K(srid), K(srs_item->get_srid()), K(*tmp_bounds));
    } else {
      bounds_item = tmp_bounds;
    }
  }
  return ret;
}

int ObTenantSrs::get_last_sys_snapshot(ObSrsCacheSnapShot *&sys_cache)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(last_sys_snapshot_)) {
    ret = OB_ERR_EMPTY_QUERY;
    LOG_WARN("last sys snapshot is null", K(ret), K(local_sys_srs_version_), K(remote_sys_srs_version_));
  } else {
    sys_cache = last_sys_snapshot_;
    last_sys_snapshot_->inc_ref_count();
  }
  return ret;
}

int ObTenantSrs::get_last_user_snapshot(ObSrsCacheSnapShot *&user_cache)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(last_user_snapshot_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("last sys snapshot is null", K(ret), K(local_user_srs_version_), K(remote_user_srs_version_));
  } else {
    user_cache = last_user_snapshot_;
    last_user_snapshot_->inc_ref_count();
  }
  return ret;
}

int ObTenantSrs::try_get_last_snapshot(ObSrsCacheGuard &srs_guard)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_last_snapshot(srs_guard))) {
    if (ret == OB_ERR_EMPTY_QUERY) {
      int8_t retry_cnt = RETRY_TIMES;
      int64_t start_time = ::oceanbase::common::ObTimeUtility::current_time();
      while (true) {
        usleep(RETRY_INTERVAL_US);
        if (OB_SUCC(get_last_snapshot(srs_guard))) {
          break;
        } else if (ret != OB_ERR_EMPTY_QUERY || (--retry_cnt) <= 0) {
          LOG_WARN("failed to get srs snapshot", K(ret), K(retry_cnt));
          break;
        } else {
          // do nothing
        }
      }
      LOG_WARN("wait loop time cost : ", K(::oceanbase::common::ObTimeUtility::current_time() - start_time));
    } else {
      LOG_WARN("failed to get last srs snapshot", K(ret));
    }
  }
  return ret;
}

int ObTenantSrs::get_last_snapshot(ObSrsCacheGuard &srs_guard)
{
  int ret = OB_SUCCESS;
  ObSrsCacheSnapShot *sys_cache = NULL;
  TCRLockGuard guard(lock_);
  if (OB_FAIL(get_last_sys_snapshot(sys_cache))) {
    LOG_WARN("failed to get last sys snapshot", K(ret));
  } else {
    srs_guard.set_srs_snapshot(sys_cache);
  }
  return ret;
}

int ObTenantSrs::refresh_srs(bool is_sys)
{
  int ret = OB_SUCCESS;
  ObSrsCacheSnapShot *srs = NULL;
  uint64_t tenant_data_version = 0;
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_1_0_0) {
    ret = OB_ERR_EMPTY_QUERY;
    LOG_INFO("srs exist min version 4.1", K(tenant_data_version));
  } else if (OB_FAIL(fetch_all_srs(srs, is_sys))) {
    if (ret == OB_ERR_EMPTY_QUERY ) {
      LOG_DEBUG("srs table is empty", K(is_sys));
    } else {
      LOG_WARN("failed to fetch ObSrsCacheSnapShot", K(ret), K(is_sys));
    }
  } else {
    TCWLockGuard guard(lock_);
    ObSrsCacheSnapShot *&last_snapshot = is_sys ? last_sys_snapshot_ : last_user_snapshot_;
    uint64_t &local_version = is_sys ?  local_sys_srs_version_ : local_user_srs_version_;
    uint64_t &remote_version = is_sys ?  remote_sys_srs_version_ : remote_user_srs_version_;
    if (last_snapshot != NULL) {
      if (last_snapshot->get_ref_count() > 0
          && OB_FAIL(srs_old_snapshots_.push_back(last_snapshot))) {
        LOG_WARN("failed to push last_snapshot to recycle queue", K(ret), K(tenant_id), K(is_sys));
      } else {
        allocator_.free(last_snapshot);
      }
    }
    last_snapshot = srs;
    local_version = srs->get_srs_version();
    LOG_INFO("fetch srs cache snapshot success", K(local_version), K(remote_version),
             K(srs->get_srs_count()), K(srs_old_snapshots_.size()), K(tenant_id), K(is_sys));
  }
  return ret;
}

int ObTenantSrs::refresh_sys_srs()
{
  return refresh_srs(true);
}

int ObTenantSrs::refresh_usr_srs()
{
  return refresh_srs(false);
}

int ObTenantSrs::TenantSrsUpdatePeriodicTask::init(ObTenantSrs *srs)
{
  tenant_srs_ = srs;
  return OB_SUCCESS;
}

int ObTenantSrs::TenantSrsUpdateTask::init(ObTenantSrs *srs)
{
  tenant_srs_ = srs;
  return OB_SUCCESS;
}

void ObTenantSrs::recycle_last_snapshots()
{
  TCWLockGuard guard(lock_);
  if (OB_NOT_NULL(last_sys_snapshot_) &&
      last_sys_snapshot_->get_ref_count() <= 0) {
    last_sys_snapshot_->~ObSrsCacheSnapShot();
    allocator_.free(last_sys_snapshot_);
    last_sys_snapshot_ = NULL;
  }
  if (OB_NOT_NULL(last_user_snapshot_) &&
      last_user_snapshot_->get_ref_count() <= 0) {
    last_user_snapshot_->~ObSrsCacheSnapShot();
    allocator_.free(last_user_snapshot_);
    last_user_snapshot_ = NULL;
  }
}

uint32_t ObTenantSrs::get_snapshots_size()
{
  TCRLockGuard guard(lock_);
  uint32_t count = srs_old_snapshots_.size();
  if (OB_NOT_NULL(last_sys_snapshot_)) {
    count++;
  }
  if (OB_NOT_NULL(last_user_snapshot_)) {
    count++;
  }
  return count;
}

void ObTenantSrs::recycle_old_snapshots()
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(lock_);
  uint32_t i = 0;
  for (; i < srs_old_snapshots_.size(); i++) {
    ObSrsCacheSnapShot *snap = srs_old_snapshots_[i];
    if (snap->get_ref_count() <= 0) {
      if (OB_FAIL(srs_old_snapshots_.remove(i)))  {
        LOG_WARN("failed to remove old snapshot", K(ret));
      } else {
        snap->~ObSrsCacheSnapShot();
        allocator_.free(snap);
      }
    }
  }
}

void ObTenantSrs::TenantSrsUpdateTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_srs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to do srs update task. tenant_srs is null", K(ret));
  } else if (OB_FAIL(tenant_srs_->refresh_sys_srs())) {
    if (ret != OB_ERR_EMPTY_QUERY) {
      LOG_WARN("failed to refresh sys srs", K(ret), K(tenant_srs_->remote_sys_srs_version_),
              K(tenant_srs_->local_sys_srs_version_));
    }
  }
}

void ObTenantSrs::TenantSrsUpdatePeriodicTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService *schema_service = nullptr;
  const uint64_t tenant_id = MTL_ID();
  bool is_sys_overdue = false;
  bool is_user_overdue = false;
  uint32_t delay = SLEEP_USECONDS;


  if ((tenant_id == OB_SYS_TENANT_ID && !GSCHEMASERVICE.is_sys_full_schema()) ||
      (tenant_id != OB_SYS_TENANT_ID && !GSCHEMASERVICE.is_tenant_full_schema(tenant_id))) {
    delay = BOOTSTRAP_PERIOD;
  } else {
    uint32_t old_snapshot_size = 0;
    {
      TCRLockGuard guard(tenant_srs_->lock_);
      is_sys_overdue = tenant_srs_->local_sys_srs_version_ < tenant_srs_->remote_sys_srs_version_;
      is_user_overdue = tenant_srs_->local_user_srs_version_ < tenant_srs_->remote_user_srs_version_;
      old_snapshot_size = tenant_srs_->srs_old_snapshots_.size();
    }

    if ((is_sys_overdue || tenant_srs_->last_sys_snapshot_ == NULL)
        && OB_FAIL(tenant_srs_->refresh_sys_srs())) {
      if (ret != OB_ERR_EMPTY_QUERY) {
        LOG_WARN("failed to refresh sys srs", K(ret), K(tenant_srs_->remote_sys_srs_version_),
                 K(tenant_srs_->local_sys_srs_version_));
      } else {
        delay = BOOTSTRAP_PERIOD;
      }
    }
    if (is_user_overdue) {
      // to do:user srs refresh
    }
    if (old_snapshot_size > 0) {
      tenant_srs_->recycle_old_snapshots();
    }
  }
  // timer task, ignore error code
  if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(), *this, delay, false))) {
    LOG_WARN("schedule srs update task failed", K(ret));
  }
}

int ObTenantSrs::cancle_update_task()
{
  int ret = OB_SUCCESS;
  bool is_exist = true;
  if (OB_FAIL(TG_TASK_EXIST(MTL(omt::ObSharedTimer*)->get_tg_id(), srs_update_periodic_task_, is_exist))) {
    LOG_WARN("failed to check tenant srs update task", K(ret));
  } else if (is_exist) {
    if (OB_FAIL(TG_CANCEL_R(MTL(omt::ObSharedTimer*)->get_tg_id(), srs_update_periodic_task_))) {
      LOG_WARN("failed to cancel tenant srs update task", K(ret));
    }
  }
  return ret;
}

int ObSrsCacheSnapShot::get_srs_item(uint64_t srid, const ObSrsItem *&srs_item)
{
  int ret = OB_SUCCESS;
  const ObSrsItem *tmp_srs_item = NULL;
  if (OB_SUCC(srs_item_map_.get_refactored(srid, tmp_srs_item))) {
    srs_item = tmp_srs_item;
  }
  return ret;
}


int ObTenantSrs::fetch_all_srs(ObSrsCacheSnapShot *&srs_snapshot, bool is_sys_srs)
{
  int ret = OB_SUCCESS;
  uint64_t srs_version = 0;
  ObSrsCacheType snapshot_type;
  ObSrsCacheSnapShot *snapshot = NULL;
  uint32_t res_count = 0;
  const uint64_t tenant_id = MTL_ID();

  ObSqlString sql;
  ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_, tenant_id, OB_ALL_SPATIAL_REFERENCE_SYSTEMS_TID);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    if (is_sys_srs) {
      ret = sql.append_fmt("SELECT * FROM %s WHERE (SRS_ID < %d AND SRS_ID != 0) OR SRS_ID > %d",
        OB_ALL_SPATIAL_REFERENCE_SYSTEMS_TNAME, USER_SRID_MIN, USER_SRID_MAX);
      snapshot_type = ObSrsCacheType::SYSTEM_RESERVED;
    } else {
      ret = sql.append_fmt("SELECT * FROM %s WHERE SRS_ID >= %d AND SRS_ID <= %d",
        OB_ALL_SPATIAL_REFERENCE_SYSTEMS_TNAME, USER_SRID_MIN, USER_SRID_MAX);
      snapshot_type = ObSrsCacheType::USER_DEFINED;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("append sql failed", K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(sql), K(ret), K(tenant_id));
    } else if (OB_UNLIKELY(NULL == (result = res.get_result()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get result. ", K(ret));
    } else {
      while (OB_SUCC(ret) && OB_SUCCESS == (ret = result->next())) {
        const ObSrsItem *srs_item = NULL;
        const ObSrsItem *tmp = NULL;
        res_count++;
        if (OB_ISNULL(snapshot)) {
          snapshot = OB_NEWx(ObSrsCacheSnapShot, &allocator_, &allocator_, snapshot_type);
          if (OB_ISNULL(snapshot)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to create ObSrsCacheSnapShot", K(ret));
          } else if (OB_FAIL(snapshot->init())) {
            LOG_WARN("failed to init ObSrsCacheSnapShot", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(snapshot->parse_srs_item(result, srs_item, srs_version))) {
          LOG_WARN("failed to parse srs item from sys_table", K(ret));
          result->print_info();
        } else if (OB_FAIL(snapshot->get_srs_item(srs_item->get_srid(), tmp))) {
          if (ret == OB_HASH_NOT_EXIST) {
            if (OB_FAIL(snapshot->add_srs_item(srs_item->get_srid(), srs_item))) {
              LOG_WARN("failed to add srs item to snapshot", K(ret), K(srs_item->get_srid()));
            }
          } else {
            LOG_WARN("failed to get srs item from snapshot", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("duplicated srid in snapshot", K(ret));
          result->print_info();
        }
      }

      if (ret == OB_ITER_END) { // ob_success
        if (res_count == 0) { // empty result
          ret = OB_ERR_EMPTY_QUERY;
        } else {
          if (OB_FAIL(generate_pg_reserved_srs(snapshot))) {
            LOG_WARN("failed to geneate pg reserved srs", K(ret));
            allocator_.free(snapshot);
          } else {
            snapshot->set_srs_version(srs_version);
            srs_snapshot = snapshot;
          }
        }
      } else if (snapshot != NULL) {
        allocator_.free(snapshot);
        LOG_WARN("failed to get all srs item, iter quit", K(ret));
      }
    }
  }
  return ret;
}

int ObSrsCacheSnapShot::extract_bounds_numberic(ObMySQLResult *result, const char *field_name, double &value)
{
  int ret = OB_SUCCESS;
  number::ObNumber nmb;
  if (OB_SUCC(result->get_number(field_name, nmb))) {
    const char *nmb_buf = nmb.format();
    if (OB_ISNULL(nmb_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nmb_buf is NULL", K(ret));
    } else {
      double val = 0.0;
      char *endptr = NULL;
      int err = 0;
      ObString num_str(strlen(nmb_buf), nmb_buf);
      val = ObCharset::strntodv2(num_str.ptr(), num_str.length(), &endptr, &err);
      if (EOVERFLOW == err && (-DBL_MAX == value || DBL_MAX == value)) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("invalid numberic value", K(ret), K(err), K(num_str));
      } else {
        value = val;
      }
    }
  } else if (OB_ERR_NULL_VALUE) {
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("failed to get number", K(ret), KP(field_name));
  }
  return ret;
}

int ObSrsCacheSnapShot::parse_srs_item(ObMySQLResult *result, const ObSrsItem *&srs_item, uint64_t &srs_version)
{
  int ret = OB_SUCCESS;
  ObString srs_name, organization, definition, description, proj4text;
  uint64_t organization_coordsys_id = 0;
  uint64_t srs_id = 0;
  double min_x = NAN;
  double min_y = NAN;
  double max_x = NAN;
  double max_y = NAN;
  ObSpatialReferenceSystemBase *srs_info = NULL;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(common::OB_SERVER_TENANT_ID, "SRSWKTParser"));

  EXTRACT_UINT_FIELD_MYSQL(*result, "srs_id", srs_id, uint64_t);
  EXTRACT_UINT_FIELD_MYSQL(*result, "srs_version", srs_version, uint64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(*result, "srs_name", srs_name);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "organization", organization);
  EXTRACT_UINT_FIELD_MYSQL(*result, "organization_coordsys_id", organization_coordsys_id, uint64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(*result, "definition", definition);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "description", description);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "proj4text", proj4text);

  if (OB_FAIL(extract_bounds_numberic(result, "minX", min_x))) {
    LOG_WARN("failed to extract minx value", K(ret));
  } else if (OB_FAIL(extract_bounds_numberic(result, "minY", min_y))) {
    LOG_WARN("failed to extract miny value", K(ret));
  } else if (OB_FAIL(extract_bounds_numberic(result, "maxX", max_x))) {
    LOG_WARN("failed to extract maxx value", K(ret));
  } else if (OB_FAIL(extract_bounds_numberic(result, "maxY", max_y))) {
    LOG_WARN("failed to extract maxy value", K(ret));
  } else if (OB_FAIL(ObSrsWktParser::parse_srs_wkt(*allocator_, srs_id, definition, srs_info))) {
    LOG_WARN("failed to parse srs wkt from definition", K(ret), K(definition));
  } else {
    ObSrsItem *new_srs_item = OB_NEWx(ObSrsItem, allocator_, srs_info);
    if (OB_ISNULL(new_srs_item)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for srs item", K(ret));
    } else if (!proj4text.empty()) {
      srs_info->set_bounds(min_x, min_y, max_x, max_y);
      if (OB_FAIL(srs_info->set_proj4text(*allocator_, proj4text))) {
        LOG_WARN("fail to set proj4text for srs item", K(ret), K(srs_id));
      }
    }
    if (OB_SUCC(ret)) {
      srs_item = new_srs_item;
    }
  }
  return ret;
}

int ObSrsCacheSnapShot::add_pg_reserved_srs_item(const ObString &pg_wkt, const uint32_t srs_id)
{
  int ret = OB_SUCCESS;
  ObString proj4text;
  ObSpatialReferenceSystemBase *srs_info = NULL;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(common::OB_SERVER_TENANT_ID, "SRSWKTParser"));

  if (OB_FAIL(ObSrsWktParser::parse_srs_wkt(*allocator_, srs_id, pg_wkt, srs_info))) {
    LOG_WARN("failed to parse pg reserved srs wkt", K(ret), K(srs_id), K(pg_wkt));
  } else {
    ObSrsItem *new_srs_item = OB_NEWx(ObSrsItem, allocator_, srs_info);
    if (OB_ISNULL(new_srs_item)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for srs item", K(ret));
    } else if (OB_FAIL(ObGeoTypeUtil::get_pg_reserved_prj4text(allocator_, srs_id, proj4text))) {
      LOG_WARN("fail to generate proj4text for pg srs item", K(ret));
    } else if (OB_FAIL(add_srs_item(new_srs_item->get_srid(), new_srs_item))) {
      LOG_WARN("failed to add pg srs item to snapshot", K(ret), K(new_srs_item->get_srid()));
    } else {
      srs_info->set_proj4text(proj4text);
    }
  }
  return ret;
}

int ObTenantSrs::generate_pg_reserved_srs(ObSrsCacheSnapShot *&srs_snapshot)
{
  int ret = OB_SUCCESS;
  char wkt_buf[MAX_WKT_LEN] = {0};
  if (OB_FAIL(srs_snapshot->add_pg_reserved_srs_item(NORTH_STEREO_WKT, SRID_NORTH_STEREO_PG))) {
    LOG_WARN("failed to parse pg reserved srs item", K(ret), K(SRID_NORTH_STEREO_PG));
  } else if (OB_FAIL(srs_snapshot->add_pg_reserved_srs_item(WORLD_MERCATOR_WKT, SRID_WORLD_MERCATOR_PG))) {
    LOG_WARN("failed to parse pg reserved srs item", K(ret), K(SRID_WORLD_MERCATOR_PG));
  } else if (OB_FAIL(srs_snapshot->add_pg_reserved_srs_item(SOUTH_LAMBERT_WKT, SRID_SOUTH_LAMBERT_PG))) {
    LOG_WARN("failed to parse pg reserved srs item", K(ret), K(SRID_SOUTH_LAMBERT_PG));
  } else if (OB_FAIL(srs_snapshot->add_pg_reserved_srs_item(NORTH_LAMBERT_WKT, SRID_NORTH_LAMBERT_PG))) {
    LOG_WARN("failed to parse pg reserved srs item", K(ret), K(SRID_NORTH_LAMBERT_PG));
  }

  // "+proj=utm +zone=%d +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
  for (int id = SRID_SOUTH_UTM_START_PG; id <= SRID_SOUTH_UTM_END_PG && OB_SUCC(ret); id++) {
    memset(wkt_buf, 0, MAX_WKT_LEN);
    int longitude = -177 + ((id - SRID_SOUTH_UTM_START_PG) * 6);
    snprintf(wkt_buf, MAX_WKT_LEN, SOUTH_UTM_WKT, longitude);
    ObString SOUTH_UTM = ObString::make_string(wkt_buf);
    if (OB_FAIL(srs_snapshot->add_pg_reserved_srs_item(SOUTH_UTM, id))) {
      LOG_WARN("failed to parse pg reserved srs item", K(ret), K(id));
    }
  }
  // +proj=utm +zone=%d +ellps=WGS84 +datum=WGS84 +units=m +no_defs
  for (int id = SRID_NORTH_UTM_START_PG; id <= SRID_NORTH_UTM_END_PG && OB_SUCC(ret); id++) {
    memset(wkt_buf, 0, MAX_WKT_LEN);
    int longitude = -177 + ((id - SRID_NORTH_UTM_START_PG) * 6);
    snprintf(wkt_buf, MAX_WKT_LEN, NORTH_UTM_WKT, longitude);
    ObString NORTH_UTM = ObString::make_string(wkt_buf);
    if (OB_FAIL(srs_snapshot->add_pg_reserved_srs_item(NORTH_UTM, id))) {
      LOG_WARN("failed to parse pg reserved srs item", K(ret), K(id));
    }
  }

  // +proj=laea +ellps=WGS84 +datum=WGS84 +lat_0=%g +lon_0=%g +units=m +no_defs
  for (int id = SRID_LAEA_START_PG; id < SRID_LAEA_END_PG && OB_SUCC(ret); id++) {
    int zone = id - SRID_LAEA_START_PG;
    int xzone = zone % 20;
    int yzone = zone / 20;
    double lat_0 = 30.0 * (yzone - 3) + 15.0;
    double lon_0 = 0.0;
    if  ( yzone == 2 || yzone == 3 ) {
      lon_0 = 30.0 * (xzone - 6) + 15.0;
    } else if ( yzone == 1 || yzone == 4 ) {
      lon_0 = 45.0 * (xzone - 4) + 22.5;
    } else if ( yzone == 0 || yzone == 5 ) {
      lon_0 = 90.0 * (xzone - 2) + 45.0;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid pg srid", K(ret), K(id), K(xzone), K(yzone));
    }

    if (OB_SUCC(ret)) {
      while (lon_0 > 180) {
        lon_0 -= 360;
      }
      while (lon_0 < -180) {
        lon_0 += 360;
      }

      memset(wkt_buf, 0, MAX_WKT_LEN);
      snprintf(wkt_buf, MAX_WKT_LEN, LAEA_WKT, lat_0, lon_0);
      ObString LAEA = ObString::make_string(wkt_buf);
      if (OB_FAIL(srs_snapshot->add_pg_reserved_srs_item(LAEA, id))) {
        LOG_WARN("failed to parse pg reserved srs item", K(ret), K(id));
      }
    }
  }
  return ret;
}

}  // omt
}  // oceanbase
