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

#define USING_LOG_PREFIX  OBLOG_FETCHER

#include "share/rc/ob_tenant_base.h"  // MTL_ID
#include "ob_log_fetch_stream_container_mgr.h"

#ifdef ERRSIM
ERRSIM_POINT_DEF(LOGFETCHER_ALLOC_FSC_FAILED);
#endif

namespace oceanbase
{
namespace logfetcher
{
ObFsContainerMgr::ObFsContainerMgr() :
    is_inited_(false),
    self_tenant_id_(OB_INVALID_TENANT_ID),
    rpc_(nullptr),
    stream_worker_(nullptr),
    progress_controller_(nullptr),
    log_handler_(nullptr),
    fsc_map_(),
    fsc_pool_(),
    fs_pool_(),
    rpc_result_pool_()
{

}

ObFsContainerMgr::~ObFsContainerMgr()
{
  destroy();
}

int ObFsContainerMgr::init(
    const uint64_t source_tenant_id,
    const uint64_t self_tenant_id,
    const int64_t svr_stream_cached_count,
    const int64_t fetch_stream_cached_count,
    const int64_t rpc_result_cached_count,
    IObLogRpc &rpc,
    IObLSWorker &stream_worker,
    PartProgressController &progress_controller,
    ILogFetcherHandler &log_handler)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObFsContainerMgr inited twice", KR(ret));
  } else if (OB_FAIL(fsc_map_.init(ObModIds::OB_LOG_SVR_STREAM_MAP, self_tenant_id))) {
    LOG_ERROR("fsc_map_ init fail", KR(ret));
  } else if (OB_FAIL(fsc_pool_.init(svr_stream_cached_count,
          ObModIds::OB_LOG_SVR_STREAM_POOL,
          self_tenant_id,
          SVR_STREAM_POOL_BLOCK_SIZE))) {
    LOG_ERROR("init FetchStreamContainer pool fail", KR(ret));
  } else if (OB_FAIL(fs_pool_.init(fetch_stream_cached_count))) {
    LOG_ERROR("init fetch stream pool fail", KR(ret), K(fetch_stream_cached_count));
  } else if (OB_FAIL(rpc_result_pool_.init(self_tenant_id, rpc_result_cached_count))) {
    LOG_ERROR("init rpc result pool fail", KR(ret), K(source_tenant_id), K(rpc_result_cached_count));
  } else {
    rpc_ = &rpc;
    self_tenant_id_ = self_tenant_id;
    stream_worker_ = &stream_worker;
    progress_controller_ = &progress_controller;
    log_handler_ = &log_handler;
    is_inited_ = true;
  }

  return ret;
}

void ObFsContainerMgr::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    self_tenant_id_ = OB_INVALID_TENANT_ID;
    rpc_ = nullptr;
    stream_worker_ = nullptr;
    progress_controller_ = nullptr;
    log_handler_ = nullptr;

    (void)fsc_map_.destroy();
    fsc_pool_.destroy();
    fs_pool_.destroy();
    rpc_result_pool_.destroy();
  }
}

int ObFsContainerMgr::add_fsc(const FetchStreamType stype,
    const logservice::TenantLSID &tls_id)
{
  int ret = OB_SUCCESS;
  FetchStreamContainer *fsc = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObFsContainerMgr has not be inited");
  } else if (OB_UNLIKELY(! is_fetch_stream_type_valid(stype))
      || OB_ISNULL(rpc_)
      || OB_ISNULL(stream_worker_)
      || OB_ISNULL(progress_controller_)) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("invalid argument", KR(ret), K(stype), K(rpc_), K(stream_worker_),
        K(progress_controller_));
#ifdef ERRSIM
  } else if (OB_FAIL(LOGFETCHER_ALLOC_FSC_FAILED)) {
    LOG_ERROR("ERRSIM: failed to alloc fsc");
#endif
  } else if (OB_FAIL(fsc_pool_.alloc(fsc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate fsc from pool failed", KR(ret), K(tls_id), K(fsc));
  } else if (OB_ISNULL(fsc)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate fsc from pool failed", KR(ret), K(tls_id), K(fsc));
  } else {
    fsc->reset(stype,
        self_tenant_id_,
        *rpc_,
        fs_pool_,
        *stream_worker_,
        rpc_result_pool_,
        *progress_controller_,
        *log_handler_);

    if (OB_FAIL(fsc_map_.insert(tls_id, fsc))) {
      LOG_ERROR("insert into fsc_map_ fail", KR(ret), K(tls_id), K(fsc));
    } else {
      LOG_INFO("[STAT] [FSC_MGR] [ALLOC]", K(tls_id), K(fsc), KPC(fsc));
    }
  }

  return ret;
}

int ObFsContainerMgr::remove_fsc(const logservice::TenantLSID &tls_id)
{
  int ret = OB_SUCCESS;
  FetchStreamContainer *fsc = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObFsContainerMgr has not be inited");
  } else if (OB_FAIL(get_fsc(tls_id, fsc))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("ObFsContainerMgr get_fsc failed", KR(ret));
    } else {
      LOG_WARN("tls_id not in fsc", K(tls_id));
    }
  // explicitly call FetchStreamContainer::reset because ObSmallObjPool may not invoke the destructor of the object,
  // which cause incorrect destruct order of objects.
  } else if (FALSE_IT(fsc->reset())) {
  } else if (OB_FAIL(fsc_pool_.free(fsc))) {
    LOG_ERROR("fsc_pool_ free failed", KR(ret), K(tls_id), KPC(fsc));
  } else if (OB_FAIL(fsc_map_.erase(tls_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("fsc_map_ erase failed", KR(ret), K(tls_id));
    } else {
      LOG_WARN("tls_id not in fsc_map", K(tls_id));
    }
  } else {}

  return ret;
}

int ObFsContainerMgr::get_fsc(const logservice::TenantLSID &tls_id,
    FetchStreamContainer *&fsc)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObFsContainerMgr has not be inited");
  } else if (OB_FAIL(fsc_map_.get(tls_id, fsc))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("FetchStreamContainer Map get failed", KR(ret), K(tls_id), K(fsc));
    }
  } else if (OB_ISNULL(fsc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fsc is nullptr", KR(ret), K(tls_id), K(fsc));
  } else {}

  return ret;
}

void ObFsContainerMgr::print_stat()
{
  int ret = OB_SUCCESS;

  int64_t alloc_count = fsc_pool_.get_alloc_count();
  int64_t free_count = fsc_pool_.get_free_count();
  int64_t fixed_count = fsc_pool_.get_fixed_count();
  int64_t used_count = alloc_count - free_count;
  int64_t dynamic_count = (alloc_count > fixed_count) ? alloc_count - fixed_count : 0;

  _LOG_INFO("[STAT] [FS_CONTAINER_POOL] USED=%ld FREE=%ld FIXED=%ld DYNAMIC=%ld",
      used_count, free_count, fixed_count, dynamic_count);

  fs_pool_.print_stat();
  rpc_result_pool_.print_stat();

  TenantStreamStatFunc tenant_stream_stat_func;
  if (OB_FAIL(fsc_map_.for_each(tenant_stream_stat_func))) {
    LOG_ERROR("for each FetchStreamContainer map fail", KR(ret));
  } else {
    _LOG_INFO("[STAT] [FETCH_STREAM] TENANT=%lu/sec, TRAFFIC=%s/sec", self_tenant_id_, SIZE_TO_STR(tenant_stream_stat_func.total_traffic_));
  }
}

bool ObFsContainerMgr::TenantStreamStatFunc::operator() (const logservice::TenantLSID &key, FetchStreamContainer *value)
{
  int64_t ls_traffic = 0;
  (void) value->do_stat(ls_traffic);
  total_traffic_ += ls_traffic;
  return true;
}

} // namespace logfetcher
} // namespace oceanbase
