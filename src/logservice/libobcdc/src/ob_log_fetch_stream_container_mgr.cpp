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

#include "ob_log_fetch_stream_container_mgr.h"

namespace oceanbase
{
namespace libobcdc
{
ObFsContainerMgr::ObFsContainerMgr() :
    is_inited_(false),
    rpc_(nullptr),
    stream_worker_(nullptr),
    progress_controller_(nullptr),
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

int ObFsContainerMgr::init(const int64_t svr_stream_cached_count,
    const int64_t fetch_stream_cached_count,
    const int64_t rpc_result_cached_count,
    IObLogRpc &rpc,
    IObLSWorker &stream_worker,
    PartProgressController &progress_controller)
{
  int ret = OB_SUCCESS;

  // TODO mod ID
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObFsContainerMgr inited twice", KR(ret));
  } else if (OB_FAIL(fsc_map_.init(ObModIds::OB_LOG_SVR_STREAM_MAP))) {
    LOG_ERROR("fsc_map_ init fail", KR(ret));
  } else if (OB_FAIL(fsc_pool_.init(svr_stream_cached_count,
          ObModIds::OB_LOG_SVR_STREAM_POOL,
          OB_SERVER_TENANT_ID,
          SVR_STREAM_POOL_BLOCK_SIZE))) {
    LOG_ERROR("init FetchStreamContainer pool fail", KR(ret));
  } else if (OB_FAIL(fs_pool_.init(fetch_stream_cached_count))) {
    LOG_ERROR("init fetch stream pool fail", KR(ret), K(fetch_stream_cached_count));
  } else if (OB_FAIL(rpc_result_pool_.init(rpc_result_cached_count))) {
    LOG_ERROR("init rpc result pool fail", KR(ret), K(rpc_result_cached_count));
  } else {
    rpc_ = &rpc;
    stream_worker_ = &stream_worker;
    progress_controller_ = &progress_controller;
    is_inited_ = true;
  }

  return ret;
}

void ObFsContainerMgr::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    rpc_ = nullptr;
    stream_worker_ = nullptr;
    progress_controller_ = nullptr;

    (void)fsc_map_.destroy();
    fsc_pool_.destroy();
    fs_pool_.destroy();
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
  } else if (OB_FAIL(fsc_pool_.alloc(fsc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate fsc from pool failed", KR(ret), K(tls_id), K(fsc));
  } else if (OB_ISNULL(fsc)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate fsc from pool failed", KR(ret), K(tls_id), K(fsc));
  } else {
    fsc->reset(stype,
        *rpc_,
        fs_pool_,
        *stream_worker_,
        rpc_result_pool_,
        *progress_controller_);

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
    LOG_ERROR("ObFsContainerMgr get_fsc failed", KR(ret));
  } else if (OB_FAIL(fsc_pool_.free(fsc))) {
    LOG_ERROR("fsc_pool_ free failed", KR(ret), K(tls_id), KPC(fsc));
  } else if (OB_FAIL(fsc_map_.erase(tls_id))) {
    LOG_ERROR("fsc_map_ erase failed", KR(ret), K(tls_id));
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
  SvrStreamStatFunc svr_stream_stat_func;

  int64_t alloc_count = fsc_pool_.get_alloc_count();
  int64_t free_count = fsc_pool_.get_free_count();
  int64_t fixed_count = fsc_pool_.get_fixed_count();
  int64_t used_count = alloc_count - free_count;
  int64_t dynamic_count = (alloc_count > fixed_count) ? alloc_count - fixed_count : 0;

  _LOG_INFO("[STAT] [FS_CONTAINER_POOL] USED=%ld FREE=%ld FIXED=%ld DYNAMIC=%ld",
      used_count, free_count, fixed_count, dynamic_count);

  fs_pool_.print_stat();
  rpc_result_pool_.print_stat();

  // Statistics every FetchStreamContainer
  if (OB_FAIL(fsc_map_.for_each(svr_stream_stat_func))) {
    LOG_ERROR("for each FetchStreamContainer map fail", KR(ret));
  }
}

} // namespace libobcdc
} // namespace oceanbase
