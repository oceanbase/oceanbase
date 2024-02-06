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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_ls_fetch_ctx.h"

#include "lib/hash_func/murmur_hash.h"        // murmurhash

#include "ob_log_utils.h"                     // get_timestamp
#include "ob_log_config.h"                    // ObLogFetcherConfig
#include "ob_log_ls_fetch_mgr.h"              // IObLogLSFetchMgr
#include "ob_log_trace_id.h"                  // ObLogTraceIdGuard
#include "ob_log_fetcher_err_handler.h"       // TCTX
#include "ob_log_fetcher.h"                   // IObLogFetcher
#include "logservice/common_util/ob_log_time_utils.h"
#include "logservice/restoreservice/ob_remote_log_source_allocator.h"

#define STAT(level, fmt, args...) OBLOG_FETCHER_LOG(level, "[STAT] [FETCH_CTX] " fmt, ##args)
#define _STAT(level, fmt, args...) _OBLOG_FETCHER_LOG(level, "[STAT] [FETCH_CTX] " fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)
#define DSTAT(fmt, args...) STAT(DEBUG, fmt, ##args)

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::palf;

namespace oceanbase
{
namespace logfetcher
{
/////////////////////////////// Functors //////////////////////////////////
int LSFetchCtxGetSourceFunctor::operator()(const ObLSID &id, logservice::ObRemoteSourceGuard &guard)
{
  int ret = OB_SUCCESS;
  logservice::ObRemoteLogParent *source = NULL;
  logservice::ObRemoteLogParent *ctx_source = ls_fetch_ctx_.get_archive_source();
  if (OB_ISNULL(ctx_source)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("source in LSFetchCtx is null", KR(ret), K(ls_fetch_ctx_));
  } else if (OB_ISNULL(source = logservice::ObResSrcAlloctor::alloc(ctx_source->get_source_type(), id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate remote log parent failed", KR(ret), KPC(ctx_source), K(id));
  } else if (OB_FAIL(ctx_source->deep_copy_to(*source))) {
    LOG_ERROR("deep copy source failed", KR(ret), KPC(ctx_source));
  } else if (OB_FAIL(guard.set_source(source))) {
    LOG_ERROR("source guard set source failed", KR(ret), KPC(source));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(source)) {
    logservice::ObResSrcAlloctor::free(source);
    source = nullptr;
  }
  return ret;
}

int LSFetchCtxUpdateSourceFunctor::operator()(const ObLSID &id, logservice::ObRemoteLogParent *source)
{
  int ret = OB_SUCCESS;
  logservice::ObRemoteLogParent *ctx_source = ls_fetch_ctx_.get_archive_source();
  if (OB_ISNULL(ctx_source) || OB_ISNULL(source)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("source_ in LSFetchCtx or the argument source is null", KR(ret), K(ls_fetch_ctx_), K(ctx_source), K(source));
  } else if (ctx_source->get_source_type() != source->get_source_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source type not same when updating source", KR(ret), K(ls_fetch_ctx_), KPC(ctx_source), KPC(source));
  } else if (OB_FAIL(ctx_source->update_locate_info(*source))) {
    LOG_ERROR("update locate info failed", KR(ret), KPC(ctx_source), KPC(source));
  } else { }
  return ret;
}

/////////////////////////////// LSFetchCtx /////////////////////////////////
LSFetchCtx::LSFetchCtx() :
    source_(NULL),
    remote_iter_(LSFetchCtxGetSourceFunctor(*this), LSFetchCtxUpdateSourceFunctor(*this))
{
  reset();
}

LSFetchCtx::~LSFetchCtx()
{
  reset();
}

void LSFetchCtx::reset()
{
  // Note: The default stream type for setting ls is hot stream
  stype_ = FETCH_STREAM_TYPE_HOT;
  state_ = STATE_NORMAL;
  fetching_mode_ = ClientFetchingMode::FETCHING_MODE_UNKNOWN;
  discarded_ = false;
  is_loading_data_dict_baseline_data_ = false;
  if (NULL != source_) {
    logservice::ObResSrcAlloctor::free(source_);
    source_ = NULL;
  }
  remote_iter_.reset();
  tls_id_.reset();
  serve_info_.reset();
  progress_id_ = -1;
  ls_fetch_mgr_ = NULL;
  fetch_stream_host_ = NULL;
  ls_ctx_add_info_ = NULL;
  last_sync_progress_ = OB_INVALID_TIMESTAMP;
  progress_.reset();
  start_parameters_.reset();
  fetch_info_.reset();
  svr_list_need_update_ = false;
  start_lsn_locate_req_.reset();
  end_lsn_locate_req_.reset();
  FetchTaskListNode::reset();
  mem_storage_.destroy();
  group_iterator_.destroy();
  data_dict_iterator_.reset();
  fetched_log_size_ = 0;
  ctx_desc_.reset();
  err_handler_ = NULL;
}

int LSFetchCtx::init(
    const logservice::TenantLSID &tls_id,
    const ObLogFetcherStartParameters &start_parameters,
    const bool is_loading_data_dict_baseline_data,
    const int64_t progress_id,
    const ClientFetchingMode fetching_mode,
    const ObBackupPathString &archive_dest_str,
    ObILogFetcherLSCtxAddInfo &ls_ctx_add_info,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;
  const int64_t start_tstamp_ns = start_parameters.get_start_tstamp_ns();
  const palf::LSN &start_lsn = start_parameters.get_start_lsn();
  // If the start lsn is 0, the service is started from creation
  const bool start_serve_from_create = (palf::PALF_INITIAL_LSN_VAL == start_lsn.val_);
  ObBackupDest archive_dest;

  reset();

  // Default is SYS LS type if it is a sys LS, otherwise it is a hot stream
  stype_ = (tls_id.is_sys_log_stream()) ? FETCH_STREAM_TYPE_SYS_LS : FETCH_STREAM_TYPE_HOT;
  fetching_mode_ = fetching_mode;
  is_loading_data_dict_baseline_data_ = is_loading_data_dict_baseline_data;
  tls_id_ = tls_id;
  serve_info_.reset(start_serve_from_create, start_tstamp_ns);
  progress_id_ = progress_id;
  ls_ctx_add_info_ = &ls_ctx_add_info;
  progress_.reset(start_lsn, start_tstamp_ns);
  start_parameters_ = start_parameters;
  fetched_log_size_ = 0;
  err_handler_ = &err_handler;

  if (start_lsn.is_valid()) {
    // LSN is valid, init mem_storage; otherwise after need locate start_lsn success, we can init mem_storage
    if (OB_FAIL(init_group_iterator_(start_lsn))) {
      LOG_ERROR("init_group_iterator_ failed", KR(ret), K_(tls_id), K(start_lsn));
    }
  }

  if (OB_SUCC(ret) && is_direct_fetching_mode(fetching_mode)) {
    if (OB_FAIL(init_archive_dest_(archive_dest_str, archive_dest))) {
      LOG_ERROR("init archive dest failed", KR(ret), K(archive_dest_str));
    } else if (OB_FAIL(init_archive_source_(archive_dest))) {
      LOG_ERROR("init_archive_source failed", KR(ret), K(archive_dest_str), K_(tls_id));
    }
  }

  LOG_INFO("LSFetchCtx init", KR(ret), K(tls_id), K(start_tstamp_ns), K(start_lsn));

  return ret;
}

int LSFetchCtx::init_group_iterator_(const palf::LSN &start_lsn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! start_lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("start_lsn is not valid", KR(ret), K(tls_id_), K(start_lsn));
  } else {
    palf::GetFileEndLSN group_iter_end_func = [&](){ return palf::LSN(palf::LOG_MAX_LSN_VAL); };

    if (OB_FAIL(mem_storage_.init(start_lsn))) {
      LOG_ERROR("init mem_storage_ failed", KR(ret), K_(tls_id), K(start_lsn));
    }
    // init palf iterator
    else if (OB_UNLIKELY(group_iterator_.is_inited())) {
      ret = OB_INIT_TWICE;
      LOG_ERROR("mem_storage_ or group_iterator_ already inited", KR(ret), K_(mem_storage), K_(group_iterator));
    } else if (OB_FAIL(group_iterator_.init(start_lsn, group_iter_end_func, &mem_storage_))) {
      LOG_ERROR("init group_iterator_ failed" ,KR(ret), K_(tls_id), K(start_lsn));
    }
  }

  return ret;
}

int LSFetchCtx::init_archive_dest_(const ObBackupPathString &archive_dest_str,
    ObBackupDest &archive_dest)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(archive_dest.set(archive_dest_str))) {
    LOG_ERROR("archive dest set archive_dest_str failed", KR(ret), K(archive_dest_str));
  }
  return ret;
}

int LSFetchCtx::init_archive_source_(const ObBackupDest &archive_dest)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tls_id_.get_ls_id();
  if (OB_NOT_NULL(source_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("source is not null when init archive source", KR(ret), K(source_));
  } else if (OB_ISNULL(source_ = logservice::ObResSrcAlloctor::alloc(
    share::ObLogRestoreSourceType::LOCATION, ls_id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed when alloc archive source", KR(ret), K(ls_id));
  } else {
    logservice::ObRemoteLocationParent *location_source = static_cast<logservice::ObRemoteLocationParent*>(source_);
    if (OB_FAIL(! archive_dest.is_valid())) {
      ret = OB_INVALID_BACKUP_DEST;
      LOG_ERROR("archive_dest_ is not valid", KR(ret), K(archive_dest));
    } else if (OB_ISNULL(location_source)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("location source is null", KR(ret));
    } else if (OB_FAIL(location_source->set(archive_dest, SCN::max_scn()))) {
      LOG_ERROR("location source set archive dest failed", KR(ret), K(archive_dest));
    } else {}

    if (OB_FAIL(ret) && OB_NOT_NULL(location_source)) {
      logservice::ObResSrcAlloctor::free(location_source);
      location_source = nullptr;
    }
  }
  return ret;
}

int LSFetchCtx::init_remote_iter()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tls_id_.get_tenant_id();
  const ObLSID &ls_id = tls_id_.get_ls_id();
  const LSN &start_lsn = progress_.get_next_lsn();
  const int64_t cur_log_progress = progress_.get_progress();
  archive::LargeBufferPool *large_buffer_pool = NULL;
  logservice::ObLogExternalStorageHandler *log_ext_handler = NULL;
  SCN start_scn;

  if (remote_iter_.is_init()) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("remote iter is inited", KR(ret), K_(remote_iter));
  } else if (OB_FAIL(start_scn.convert_from_ts(cur_log_progress/1000))) {
    LOG_ERROR("convert log progress to start scn failed", KR(ret), K(cur_log_progress));
  } else if (OB_FAIL(get_large_buffer_pool(large_buffer_pool))) {
    LOG_ERROR("get large buffer pool failed", KR(ret));
  } else if (OB_FAIL(get_log_ext_handler(log_ext_handler))) {
    LOG_ERROR("get log external handler failed", KR(ret));
  } else if (OB_FAIL(remote_iter_.init(tenant_id, ls_id, start_scn, start_lsn,
      LSN(LOG_MAX_LSN_VAL), large_buffer_pool, log_ext_handler))) {
    LOG_ERROR("remote iter init failed", KR(ret), K(tenant_id), K(ls_id), K(start_scn), K(start_lsn));
  }
  return ret;
}

int LSFetchCtx::append_log(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (! mem_storage_.is_inited()) {
    const LSN &start_lsn = progress_.get_next_lsn();

    if (OB_FAIL(mem_storage_.init(start_lsn))) {
      LOG_ERROR("init mem_storage_ failed", KR(ret), K_(tls_id), K(start_lsn));
    } else if (OB_FAIL(group_iterator_.reuse(start_lsn))) {
      LOG_ERROR("MemPalfBufferIterator resuse failed", KR(ret), K_(tls_id), K(start_lsn));
    } else {
      LOG_TRACE("mem_storage_ init and MemPalfBufferIterator resuse succ", K_(tls_id), K(start_lsn));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(mem_storage_.append(buf, buf_len))) {
      LOG_ERROR("append log into mem_storage_ failed", KR(ret), K_(tls_id), KP(buf), K(buf_len),
          K_(mem_storage), K_(group_iterator));
    } else {
      ATOMIC_AAF(&fetched_log_size_, buf_len);
      LOG_TRACE("append_log succ", K(buf_len), KPC(this));
    }
  }

  return ret;
}

void LSFetchCtx::reset_memory_storage()
{
  mem_storage_.destroy();
}

int LSFetchCtx::get_next_group_entry(
    palf::LogGroupEntry &group_entry,
    palf::LSN &lsn,
    const char *&buf)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!group_iterator_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_ERROR("group_iterator_ not init!");
  } else if (OB_FAIL(group_iterator_.next())) {
    if (OB_ITER_END != ret) {
      LOG_ERROR("iterate group_entry failed", KR(ret), K_(group_iterator));
    }
  } else if (OB_FAIL(group_iterator_.get_entry(buf, group_entry, lsn))) {
    LOG_ERROR("get_next_group_entry failed", KR(ret), K_(group_iterator), K(group_entry), K(lsn));
  } else { /* success */ }

  return ret;
}

int LSFetchCtx::get_next_remote_group_entry(
    palf::LogGroupEntry &group_entry,
    palf::LSN &lsn,
    const char *&buf,
    int64_t &buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! remote_iter_.is_init())) {
    ret = OB_NOT_INIT;
    LOG_ERROR("remote iter not inited", KR(ret));
  } else if (OB_FAIL(remote_iter_.next(group_entry, lsn, buf, buf_size))) {
    if (OB_INVALID_DATA == ret) {
      LOG_WARN("remote log group entry contains invalid data, maybe write-read conflict", KR(ret), K_(remote_iter));
    } else if (OB_ITER_END != ret && OB_NEED_RETRY != ret) {
      LOG_ERROR("remote iterator failed to iterate", KR(ret), K_(remote_iter));
    }
  }
  return ret;
}

int LSFetchCtx::get_log_entry_iterator(
    const palf::LogGroupEntry &group_entry,
    const palf::LSN &start_lsn,
    palf::MemPalfBufferIterator &entry_iter)
{
  int ret = OB_SUCCESS;
  palf::GetFileEndLSN entry_iter_end_func = [&](){ return start_lsn + group_entry.get_group_entry_size(); };

  if (OB_FAIL(entry_iter.init(start_lsn, entry_iter_end_func, &mem_storage_))) {
    LOG_ERROR("entry_iter init failed", KR(ret), K_(mem_storage), K_(group_iterator), K(group_entry), K(start_lsn));
  }

  return ret;
}

int LSFetchCtx::sync(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  return ret;
}

int LSFetchCtx::update_progress(
    const palf::LogGroupEntry &group_entry,
    const palf::LSN &group_entry_lsn)
{
  int ret = OB_SUCCESS;
  const int64_t submit_ts = group_entry.get_scn().get_val_for_logservice();
  const int64_t group_entry_serialize_size = group_entry.get_serialize_size();

  // Verifying log continuity
  if (OB_UNLIKELY(progress_.get_next_lsn() != group_entry_lsn)) {
    ret = OB_LOG_NOT_SYNC;
    LOG_ERROR("log not sync", KR(ret), "next_log_lsn", progress_.get_next_lsn(),
        "cur_log_lsn", group_entry_lsn, K(group_entry));
  } else {
    if (OB_SUCC(ret)) {
      palf::LSN next_lsn = group_entry_lsn + group_entry_serialize_size;

      if (OB_FAIL(progress_.update_log_progress(next_lsn, group_entry_serialize_size, submit_ts))) {
        LOG_ERROR("update log progress fail", KR(ret), K(next_lsn), K(group_entry_serialize_size), K(submit_ts),
            K(group_entry), K(progress_));
      }

      LOG_TRACE("read log and update progress success", K_(tls_id), K(group_entry), K_(progress));
    }
  }

  return ret;
}

// This function is called by the DEAD POOL and will clean up all unpublished tasks and then downlink them
//
// The implementation guarantees that tasks will only be produced when the log is read.
int LSFetchCtx::offline(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  return ret;
}

int LSFetchCtx::get_log_route_service_(logservice::ObLogRouteService *&log_route_service)
{
  int ret = OB_SUCCESS;
  IObLogFetcher *fetcher = static_cast<IObLogFetcher *>(ls_fetch_mgr_->get_fetcher_host());

  if (OB_ISNULL(fetcher)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fetcher is nullptr", KR(ret), K(fetcher));
  } else if (OB_FAIL(fetcher->get_log_route_service(log_route_service))) {
    LOG_ERROR("Fetcher get_log_route_service failed", KR(ret));
  } else if (OB_ISNULL(log_route_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("log_route_service is nullptr", KR(ret), K(log_route_service));
  }

  return ret;
}

int LSFetchCtx::get_large_buffer_pool(archive::LargeBufferPool *&large_buffer_pool)
{
  int ret = OB_SUCCESS;
  IObLogFetcher *fetcher = static_cast<IObLogFetcher *>(ls_fetch_mgr_->get_fetcher_host());

  if (OB_ISNULL(fetcher)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fetcher is nullptr", KR(ret), K(fetcher));
  } else if (OB_FAIL(fetcher->get_large_buffer_pool(large_buffer_pool))) {
    LOG_ERROR("Fetcher get_log_route_service failed", KR(ret));
  } else if (OB_ISNULL(large_buffer_pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("log_route_service is nullptr", KR(ret), K(large_buffer_pool));
  }

  return ret;
}

int LSFetchCtx::get_log_ext_handler(logservice::ObLogExternalStorageHandler *&log_ext_handler)
{
  int ret = OB_SUCCESS;
  IObLogFetcher *fetcher = static_cast<IObLogFetcher *>(ls_fetch_mgr_->get_fetcher_host());

  if (OB_ISNULL(fetcher)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fetcher is nullptr", KR(ret), K(fetcher));
  } else if (OB_FAIL(fetcher->get_log_ext_handler(log_ext_handler))) {
    LOG_ERROR("Fetcher get_log_ext_handler failed", KR(ret));
  } else if (OB_ISNULL(log_ext_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get_log_ext_handler is nullptr", KR(ret), K(log_ext_handler));
  }

  return ret;
}

int LSFetchCtx::get_fetcher_config(const ObLogFetcherConfig *&cfg)
{
  int ret = OB_SUCCESS;
  IObLogFetcher *fetcher = static_cast<IObLogFetcher *>(ls_fetch_mgr_->get_fetcher_host());

  if (OB_ISNULL(fetcher)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fetcher is nullptr", KR(ret), K(fetcher));
  } else if (OB_FAIL(fetcher->get_fetcher_config(cfg))) {
    LOG_ERROR("Fetcher get_fetcher_config failed", KR(ret));
  } else if (OB_ISNULL(cfg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObLogFetcherConfig is nullptr", KR(ret), K(cfg));
  }

  return ret;
}

bool LSFetchCtx::need_update_svr_list()
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  const int64_t cur_time = get_timestamp();
  int64_t avail_svr_count = 0;
  logservice::ObLogRouteService *log_route_service = nullptr;

  if (is_direct_fetching_mode(fetching_mode_)) {
    bool_ret = false;
  } else if(is_integrated_fetching_mode(fetching_mode_)) {
    if (REACH_TIME_INTERVAL_THREAD_LOCAL(SERVER_LIST_UPDATE_INTERVAL_SEC)) {
      if (OB_FAIL(get_log_route_service_(log_route_service))) {
        LOG_ERROR("get_log_route_service_ failed", KR(ret));
      } else if (OB_FAIL(log_route_service->get_server_count(tls_id_.get_tenant_id(), tls_id_.get_ls_id(),
              avail_svr_count))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_ERROR("ObLogRouteService get_server_count failed", KR(ret), K(tls_id_));
        } else {
          bool_ret = true;
        }
      } else {
        // If no server is available, or if a proactive update is requested, an update is required
        if (avail_svr_count <= 0 || svr_list_need_update_) {
          bool_ret = true;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls_fetch_ctx has invalid fetching mode", KR(ret), KPC(this), K(tls_id_), K_(fetching_mode));
  }

  LOG_TRACE("need_update_svr_list", KR(ret), K(bool_ret), K(tls_id_),
      K(svr_list_need_update_), K(avail_svr_count));

  return bool_ret;
}

bool LSFetchCtx::need_locate_start_lsn() const
{
  bool bool_ret = false;

  bool_ret = ! (progress_.get_next_lsn().is_valid());

  LOG_TRACE("need_locate_start_lsn", K(tls_id_), K(bool_ret), K(progress_));

  return bool_ret;
}

bool LSFetchCtx::need_locate_end_lsn() const
{
  bool bool_ret = false;

  // It should need locate when end_tstamp_ns is valid and end_ls is not valid
  bool_ret = (OB_INVALID_TIMESTAMP != start_parameters_.get_end_tstamp_ns())
    && ! start_parameters_.get_end_lsn().is_valid();

  LOG_TRACE("need_locate_end_lsn", K(tls_id_), K(bool_ret), K(start_parameters_));

  return bool_ret;
}

int LSFetchCtx::update_svr_list(const bool need_print_info)
{
  int ret = OB_SUCCESS;
  logservice::ObLogRouteService *log_route_service = nullptr;

  if (OB_FAIL(get_log_route_service_(log_route_service))) {
    LOG_ERROR("get_log_route_service_ failed", KR(ret));
  } else if (OB_FAIL(log_route_service->async_server_query_req(tls_id_.get_tenant_id(), tls_id_.get_ls_id()))) {
    if (OB_EAGAIN == ret) {
      LOG_INFO("log_route_service thread_pool is full while async_server_query_req, retry later", KR(ret), K_(tls_id));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("ObLogRouteService async_server_query_req failed", KR(ret), K(tls_id_));
    }
  } else {
    LOG_TRACE("async_server_query_req succ", K_(tls_id));
  }

  return ret;
}

int LSFetchCtx::locate_start_lsn(IObLogStartLSNLocator &start_lsn_locator)
{
  int ret = OB_SUCCESS;
  const int64_t start_tstamp_ns = serve_info_.start_serve_timestamp_;

  if (OB_FAIL(locate_lsn_(start_lsn_locate_req_, start_tstamp_ns, true, start_lsn_locator))) {
    LOG_ERROR("start_lsn_locate_req_ locate failed", KR(ret), K(start_lsn_locate_req_), K(start_tstamp_ns));
  }

  return ret;
}

int LSFetchCtx::locate_end_lsn(IObLogStartLSNLocator &start_lsn_locator)
{
  int ret = OB_SUCCESS;
  const int64_t end_tstamp_ns = start_parameters_.get_end_tstamp_ns();

  if (OB_FAIL(locate_lsn_(end_lsn_locate_req_, end_tstamp_ns, false, start_lsn_locator))) {
    LOG_ERROR("end_lsn_locate_req_ locate failed", KR(ret), K(end_lsn_locate_req_), K(end_tstamp_ns));
  }

  return ret;
}

int LSFetchCtx::set_end_lsn_and_init_dict_iter_(const LSN &start_lsn)
{
  int ret = OB_SUCCESS;
  start_parameters_.set_end_lsn(start_lsn);

  if (OB_FAIL(data_dict_iterator_.init(tls_id_.get_tenant_id()))) {
    LOG_ERROR("data_dict_iterator_ init failed", KR(ret), K(tls_id_));
  } else {
    LOG_INFO("data_dict_iterator_ init success", K(tls_id_), K(start_lsn), K(start_parameters_));
  }
  return ret;
}

int LSFetchCtx::locate_lsn_(
    StartLSNLocateReq &lsn_locate_req,
    const int64_t tstamp_ns,
    const bool is_start_tstamp,
    IObLogStartLSNLocator &start_lsn_locator)
{
  int ret = OB_SUCCESS;
  int state = lsn_locate_req.get_state();
  LocateSvrList svr_list;

  if (StartLSNLocateReq::IDLE == state) {
    const ObLogFetcherConfig *cfg = nullptr;

    if (OB_FAIL(get_fetcher_config(cfg))) {
      LOG_ERROR("get_fetcher_config_ failed", KR(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (is_integrated_fetching_mode(fetching_mode_)) {
      logservice::ObLogRouteService *log_route_service = nullptr;

      lsn_locate_req.reset(tls_id_, tstamp_ns, cfg);
      // set fetching mode after reset, otherwise fetching mode would be invalid
      lsn_locate_req.set_fetching_mode(fetching_mode_);

      if (OB_FAIL(get_log_route_service_(log_route_service))) {
        LOG_ERROR("get_log_route_service_ failed", KR(ret));
      } else if (OB_FAIL(log_route_service->get_server_array_for_locate_start_lsn(tls_id_.get_tenant_id(), tls_id_.get_ls_id(),
        svr_list))) {
        LOG_ERROR("ObLogRouteService get_server_array_for_locate_start_lsn failed", KR(ret), K(tls_id_));
      } else if (svr_list.count() <= 0) {
        LOG_INFO("server list is empty for locating start lsn, mark for updating server list");
        mark_svr_list_update_flag(true);
      } else if (OB_FAIL(init_locate_req_svr_list_(lsn_locate_req, svr_list))) {
        LOG_ERROR("init_locate_req_svr_list_ fail", KR(ret), K(svr_list));
      } else if (OB_FAIL(start_lsn_locator.async_start_lsn_req(&lsn_locate_req))) {
        LOG_ERROR("launch async start lsn request fail", KR(ret), K(lsn_locate_req));
      } else {
        LOG_INFO("start lsn locate request launched", K_(tls_id), K(is_start_tstamp),
            "start_tstamp", NTS_TO_STR(tstamp_ns),
            "svr_cnt", lsn_locate_req.svr_list_.count(),
            "svr_list", lsn_locate_req.svr_list_);
      }
    } else if (is_direct_fetching_mode(fetching_mode_)) {
      lsn_locate_req.reset(tls_id_, tstamp_ns, cfg);
      // set fetching mode after reset, otherwise fetching mode would be invalid
      lsn_locate_req.set_fetching_mode(fetching_mode_);

      if (OB_FAIL(start_lsn_locator.async_start_lsn_req(&lsn_locate_req))) {
        LOG_ERROR("launch async start lsn request fail", KR(ret), K(lsn_locate_req));
      } else {
        LOG_INFO("start lsn locate request launched in direct mode", K_(tls_id), K(is_start_tstamp),
            "start_tstamp", NTS_TO_STR(tstamp_ns));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid fetching mode", KR(ret), K_(fetching_mode));
    }
  } else if (StartLSNLocateReq::REQ == state) {
    // On request
  } else if (StartLSNLocateReq::DONE == state) {
    palf::LSN start_lsn;
    if (is_integrated_fetching_mode(fetching_mode_)) {
      common::ObAddr locate_svr;

      if (! lsn_locate_req.get_result(start_lsn, locate_svr)) {
        LOG_ERROR("start lsn locate fail", K_(start_lsn_locate_req));
      } else if (is_start_tstamp) {
        if (OB_FAIL(init_group_iterator_(start_lsn))) {
          LOG_ERROR("init_group_iterator_ failed", KR(ret), K_(tls_id), K(start_lsn));
        } else {
          progress_.set_next_lsn(start_lsn);

          LOG_INFO("start lsn located succ", K_(tls_id), K(start_lsn),
              "start_tstamp", NTS_TO_STR(tstamp_ns), K(locate_svr),
              "fetching_log_mode", print_fetching_mode(fetching_mode_),
              "svr_cnt", lsn_locate_req.svr_list_.count(),
              "svr_list", lsn_locate_req.svr_list_);
        }
      } else {
        if (OB_FAIL(set_end_lsn_and_init_dict_iter_(start_lsn))) {
          LOG_ERROR("set_end_lsn_and_init_dict_iter_ failed", KR(ret), K(start_lsn));
        }
      }
    } else if (is_direct_fetching_mode(fetching_mode_)) {
      int err = OB_SUCCESS;
      lsn_locate_req.get_direct_result(start_lsn, err);
      if (OB_UNLIKELY(OB_SUCCESS != err)) {
        LOG_ERROR("start lsn direct locate failed", K(err), K(lsn_locate_req), K_(start_lsn_locate_req));
      } else if (is_start_tstamp) {
        if (OB_FAIL(init_group_iterator_(start_lsn))) {
          LOG_ERROR("init_group_iterator_ failed", KR(ret), K_(tls_id), K(start_lsn));
        } else {
          progress_.set_next_lsn(start_lsn);
          LOG_INFO("start lsn located succ", K_(tls_id), K(start_lsn),
              "start_tstamp", NTS_TO_STR(tstamp_ns),
              "fetching_log_mode", print_fetching_mode(fetching_mode_));
        }
      } else {
        if (OB_FAIL(set_end_lsn_and_init_dict_iter_(start_lsn))) {
          LOG_ERROR("set_end_lsn_and_init_dict_iter_ failed", KR(ret), K(start_lsn));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid fetching mode", KR(ret), K_(fetching_mode), KPC(this));
    }

    // Reset the location request, whether successful or not
    lsn_locate_req.reset();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unknown start lsn locator request state", KR(ret), K(state),
        K(lsn_locate_req));
  }

  return ret;
}

int LSFetchCtx::next_server(common::ObAddr &request_svr)
{
  int ret = OB_SUCCESS;
  logservice::ObLogRouteService *log_route_service = nullptr;
  const palf::LSN &next_lsn = progress_.get_next_lsn();

  if (OB_FAIL(get_log_route_service_(log_route_service))) {
    LOG_ERROR("get_log_route_service_ failed", KR(ret));
  } else if (OB_FAIL(log_route_service->next_server(tls_id_.get_tenant_id(), tls_id_.get_ls_id(),
          next_lsn, request_svr))) {
    if (OB_ITER_END != ret) {
      LOG_ERROR("ObLogRouteService next_server failed", KR(ret), K(tls_id_), K(next_lsn), K(request_svr));
    }
  } else {
  }

  if (OB_ITER_END == ret) {
    // If the server is exhausted, ask to update the server list
    mark_svr_list_update_flag(true);
  }

  return ret;
}

int LSFetchCtx::init_locate_req_svr_list_(StartLSNLocateReq &req, LocateSvrList &locate_svr_list)
{
  int ret = OB_SUCCESS;

  // Locate start log ids preferably from the server with the latest logs to avoid inaccurate history tables that can lead to too many locate fallbacks.
  ARRAY_FOREACH_N(locate_svr_list, idx, count) {
    common::ObAddr &locate_addr = locate_svr_list.at(idx);
    StartLSNLocateReq::SvrItem start_lsn_locate_svr_item;
    start_lsn_locate_svr_item.reset(locate_addr);

    if (OB_FAIL(req.svr_list_.push_back(start_lsn_locate_svr_item))) {
      LOG_ERROR("StartLSNLocateReq::SvrList push_back failed", KR(ret));
    }
  }

  return ret;
}

void LSFetchCtx::mark_svr_list_update_flag(const bool need_update)
{
  ATOMIC_STORE(&svr_list_need_update_, need_update);
}

uint64_t LSFetchCtx::hash() const
{
  // hash by "PKEY + next_lsn"
  const LSN &next_lsn = progress_.get_next_lsn();
  return murmurhash(&next_lsn, sizeof(next_lsn), tls_id_.hash());
}

// Timeout conditions: (both satisfied)
// 1. the progress is not updated for a long time on the target server
// 2. progress is less than upper limit
int LSFetchCtx::check_fetch_timeout(const common::ObAddr &svr,
    const int64_t upper_limit,
    const int64_t fetcher_resume_tstamp,
    bool &is_fetch_timeout)                       // Is the log fetch timeout
{
  int ret = OB_SUCCESS;
  int64_t cur_time = get_timestamp();
  int64_t svr_start_fetch_tstamp = OB_INVALID_TIMESTAMP;
  is_fetch_timeout = false;
  const ObLogFetcherConfig *cfg = nullptr;

  if (OB_FAIL(get_fetcher_config(cfg))) {
    LOG_ERROR("get_fetcher_config_ failed", KR(ret));
  } else {
    // Partition timeout, after which time progress is not updated, it is considered to be a log fetch timeout
    const int64_t ls_fetch_progress_update_timeout = cfg->ls_fetch_progress_update_timeout_sec * _SEC_;


    // Get the starting log time on the current server
    if (OB_FAIL(fetch_info_.get_cur_svr_start_fetch_tstamp(svr, svr_start_fetch_tstamp))) {
      LOG_ERROR("get_cur_svr_start_fetch_tstamp fail", KR(ret), K(svr), K(fetch_info_));
    } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == svr_start_fetch_tstamp)) {
      ret = OB_INVALID_ERROR;
      LOG_ERROR("invalid start fetch tstamp", KR(ret), K(svr_start_fetch_tstamp), K(fetch_info_));
    } else {
      // Get the current progress and when the progress was last updated
      int64_t cur_progress = progress_.get_progress();
      int64_t progress_last_update_tstamp = progress_.get_touch_tstamp();

      if (OB_INVALID_TIMESTAMP != cur_progress && cur_progress >= upper_limit) {
        is_fetch_timeout = false;
      } else {
        // Consider the time at which logs start to be fetched on the server as a lower bound for progress updates
        // Ensure that the ls stays on a server for a certain period of time
        int64_t last_update_tstamp = OB_INVALID_TIMESTAMP;
        if ((OB_INVALID_TIMESTAMP == progress_last_update_tstamp)) {
          last_update_tstamp = svr_start_fetch_tstamp;
        } else {
          last_update_tstamp = std::max(progress_last_update_tstamp, svr_start_fetch_tstamp);
        }

        if (OB_INVALID_TIMESTAMP != fetcher_resume_tstamp) {
          // After a fetcher pause and restart, the fetcher pause time is also counted as ls fetch time,
          // a misjudgement may occur, resulting in a large number of ls timeouts being dispatched
          last_update_tstamp = std::max(last_update_tstamp, fetcher_resume_tstamp);
        }

        // Progress update interval
        const int64_t progress_update_interval = (cur_time - last_update_tstamp);

        // long periods of non-updating progress and progress timeouts, where it is no longer necessary to determine if the machine is behind in its backup
        if (progress_update_interval >= ls_fetch_progress_update_timeout) {
          is_fetch_timeout = true;
        }

        if (is_fetch_timeout) {
          LOG_INFO("[CHECK_PROGRESS_TIMEOUT]", K_(tls_id), K(svr),
              K(is_fetch_timeout),
              K(progress_update_interval),
              K(progress_),
              "update_limit", NTS_TO_STR(upper_limit),
              "last_update_tstamp", TS_TO_STR(last_update_tstamp),
              "svr_start_fetch_tstamp", TS_TO_STR(svr_start_fetch_tstamp));
        } else {
          LOG_TRACE("[CHECK_PROGRESS_TIMEOUT]", K_(tls_id), K(svr),
              K(is_fetch_timeout),
              K(progress_update_interval),
              K(progress_),
              "update_limit", NTS_TO_STR(upper_limit),
              "last_update_tstamp", TS_TO_STR(last_update_tstamp),
              "svr_start_fetch_tstamp", TS_TO_STR(svr_start_fetch_tstamp));
        }
      }
    }
  }

  return ret;
}

int LSFetchCtx::get_dispatch_progress(int64_t &dispatch_progress, PartTransDispatchInfo &dispatch_info)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ls_ctx_add_info_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("invalid part trans resolver", KR(ret), K(ls_ctx_add_info_));
  } else if (OB_FAIL(ls_ctx_add_info_->get_dispatch_progress(
      tls_id_.get_ls_id(),
      dispatch_progress,
      dispatch_info))) {
    LOG_ERROR("get_dispatch_progress from part trans resolver fail", KR(ret), K(tls_id_));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == dispatch_progress)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dispatch_progress is invalid", KR(ret), K(dispatch_progress), K(tls_id_), K(dispatch_info));
  }

  return ret;
}

bool LSFetchCtx::is_in_use() const
{
  // As long as there is an asynchronous request in progress, it is considered to be "in use"
  return start_lsn_locate_req_.is_state_req() || end_lsn_locate_req_.is_state_req();
}

void LSFetchCtx::print_dispatch_info() const
{
  LSProgress cur_progress;
  progress_.atomic_copy(cur_progress);

  int64_t progress = cur_progress.get_progress();

  if (fetch_info_.is_from_idle_to_idle()) {
    if (REACH_TIME_INTERVAL_THREAD_LOCAL(10 * _SEC_)) {
      _ISTAT("[DISPATCH_FETCH_TASK] LS=%s TO=%s FROM=%s REASON=\"%s\" "
          "DELAY=%s PROGRESS=%s DISCARDED=%d",
          to_cstring(tls_id_), to_cstring(fetch_info_.cur_mod_),
          to_cstring(fetch_info_.out_mod_), fetch_info_.out_reason_,
          NTS_TO_DELAY(progress),
          to_cstring(cur_progress),
          discarded_);
    }
  } else {
    _ISTAT("[DISPATCH_FETCH_TASK] LS=%s TO=%s FROM=%s REASON=\"%s\" "
        "DELAY=%s PROGRESS=%s DISCARDED=%d",
        to_cstring(tls_id_), to_cstring(fetch_info_.cur_mod_),
        to_cstring(fetch_info_.out_mod_), fetch_info_.out_reason_,
        NTS_TO_DELAY(progress),
        to_cstring(cur_progress),
        discarded_);
  }
}

void LSFetchCtx::dispatch_in_idle_pool()
{
  fetch_info_.dispatch_in_idle_pool();
  print_dispatch_info();
}

void LSFetchCtx::dispatch_in_fetch_stream(const common::ObAddr &svr, FetchStream &fs)
{
  ATOMIC_SET(&state_, FETCHING_LOG);
  set_fetch_stream_host(&fs);
  fetch_info_.dispatch_in_fetch_stream(svr, fs);
  print_dispatch_info();
}

void LSFetchCtx::dispatch_in_dead_pool()
{
  fetch_info_.dispatch_in_dead_pool();
  print_dispatch_info();
}

int LSFetchCtx::get_cur_svr_start_fetch_tstamp(const common::ObAddr &svr,
      int64_t &svr_start_fetch_tstamp) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(fetch_info_.get_cur_svr_start_fetch_tstamp(svr, svr_start_fetch_tstamp))) {
    LOG_ERROR("get_cur_svr_start_fetch_tstamp fail", KR(ret), K(fetch_info_),
        K(svr), K(svr_start_fetch_tstamp));
  }

  return ret;
}

int LSFetchCtx::add_into_blacklist(const common::ObAddr &svr,
    const int64_t svr_service_time,
    int64_t &survival_time)
{
  int ret = OB_SUCCESS;
  logservice::ObLogRouteService *log_route_service = nullptr;

  if (OB_FAIL(get_log_route_service_(log_route_service))) {
    LOG_ERROR("get_log_route_service_ failed", KR(ret));
  } else if (OB_FAIL(log_route_service->add_into_blacklist(tls_id_.get_tenant_id(), tls_id_.get_ls_id(),
          svr, svr_service_time, survival_time))) {
    LOG_ERROR("ObLogRouteService add_into_blacklist failed", KR(ret), K(tls_id_), K(svr),
        K(svr_service_time), K(survival_time));
  } else {}

  return ret;
}

bool LSFetchCtx::need_switch_server(const common::ObAddr &cur_svr)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  const palf::LSN &next_lsn = progress_.get_next_lsn();
  logservice::ObLogRouteService *log_route_service = nullptr;

  if (OB_FAIL(get_log_route_service_(log_route_service))) {
    LOG_ERROR("get_log_route_service_ failed", KR(ret));
  } else {
    bool_ret = log_route_service->need_switch_server(tls_id_.get_tenant_id(), tls_id_.get_ls_id(),
          next_lsn, cur_svr);
  }

  return bool_ret;
}

void LSFetchCtx::handle_error(const share::ObLSID &ls_id,
      const IObLogErrHandler::ErrType &err_type,
      share::ObTaskId &trace_id,
      const palf::LSN &lsn,
      const int err_no,
      const char *fmt, ...)
{
   if (OB_NOT_NULL(err_handler_)) {
     err_handler_->handle_error(ls_id, err_type, trace_id, lsn, err_no, fmt);
   }
}

/////////////////////////////////// LSProgress ///////////////////////////////////

void LSFetchCtx::LSProgress::reset()
{
  next_lsn_.reset();
  log_progress_ = OB_INVALID_TIMESTAMP;
  log_touch_tstamp_ = OB_INVALID_TIMESTAMP;
}

// start_lsn refers to the start LSN, which may not be valid
// start_tstamp_ns refers to the LS start timestamp, not the start_lsn log timestamp
//
// Therefore, this function sets start_tstamp_ns to the current progress
void LSFetchCtx::LSProgress::reset(const palf::LSN start_lsn, const int64_t start_tstamp_ns)
{
  // Update next_sn
  next_lsn_ = start_lsn;
  // Set start-up timestamp to progress
  log_progress_ = start_tstamp_ns;
  log_touch_tstamp_ = get_timestamp();
}

// If the progress is greater than the upper limit, the touch timestamp of the corresponding progress is updated
// NOTE: The purpose of this function is to prevent the touch timestamp from not being updated for a long time if the progress
// is greater than the upper limit, which could lead to a false detection of a progress timeout if the upper limit suddenly increases.
void LSFetchCtx::LSProgress::update_touch_tstamp_if_progress_beyond_upper_limit(const int64_t upper_limit)
{
  ObByteLockGuard guard(lock_);

  if (OB_INVALID_TIMESTAMP != log_progress_
      && OB_INVALID_TIMESTAMP != upper_limit
      && log_progress_ >= upper_limit) {
    log_touch_tstamp_ = get_timestamp();
  }
}

int LSFetchCtx::LSProgress::update_log_progress(const palf::LSN &new_next_lsn,
    const int64_t new_lsn_length,
    const int64_t new_log_progress)
{
  ObByteLockGuard guard(lock_);

  int ret = OB_SUCCESS;

  // Require next_lsn to be valid
  if (OB_UNLIKELY(! next_lsn_.is_valid())) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("invalid next_lsn", KR(ret), K(next_lsn_), K_(log_progress));
  }
  // Verifying log continuity
  else if (OB_UNLIKELY((next_lsn_ + new_lsn_length) != new_next_lsn)) {
    ret = OB_LOG_NOT_SYNC;
    LOG_ERROR("log not sync", KR(ret), K(next_lsn_), K(new_next_lsn), K(new_lsn_length));
  } else {
    next_lsn_ = new_next_lsn;

    // Update log progress if it is invalid, or if log progress has been updated
    if (OB_INVALID_TIMESTAMP == log_progress_ ||
        (OB_INVALID_TIMESTAMP != new_log_progress && new_log_progress > log_progress_)) {
      log_progress_ = new_log_progress;
    }

    // Log progress update, update the log_touch_tstamp_, the reason is:
    //
    // 1. Normally, if the log progress is updated, indicating that the log was fetched and that the progress was updated anyway
    // 2. At startup, if there is a log rollback and the progress is equal to the startup timestamp and cannot be rolled back,
    // so the fetched log progress is less than the start progress and the update of the log progress does not update the progress,
    // but the LS does fetched the log, in which case the "update timestamp of progress" needs to be updated
    log_touch_tstamp_ = get_timestamp();
  }

  return ret;
}

void LSFetchCtx::LSProgress::atomic_copy(LSProgress &prog) const
{
  // protected by lock
  ObByteLockGuard guard(lock_);

  prog.next_lsn_ = next_lsn_;
  prog.log_progress_ = log_progress_;
  prog.log_touch_tstamp_ = log_touch_tstamp_;
}

///////////////////////////////// FetchModule /////////////////////////////////
void LSFetchCtx::FetchModule::reset()
{
  module_ = FETCH_MODULE_NONE;
  svr_.reset();
  fetch_stream_ = NULL;
  start_fetch_tstamp_ = OB_INVALID_TIMESTAMP;
}

void LSFetchCtx::FetchModule::reset_to_idle_pool()
{
  reset();
  set_module(FETCH_MODULE_IDLE_POOL);
}

void LSFetchCtx::FetchModule::reset_to_fetch_stream(const common::ObAddr &svr, FetchStream &fs)
{
  set_module(FETCH_MODULE_FETCH_STREAM);
  svr_ = svr;
  fetch_stream_ = &fs;
  start_fetch_tstamp_ = get_timestamp();
}

void LSFetchCtx::FetchModule::reset_to_dead_pool()
{
  reset();
  set_module(FETCH_MODULE_DEAD_POOL);
}

int64_t LSFetchCtx::FetchModule::to_string(char *buffer, const int64_t size) const
{
  int64_t pos = 0;

  switch (module_) {
    case FETCH_MODULE_NONE: {
      (void)databuff_printf(buffer, size, pos, "NONE");
      break;
    }

    case FETCH_MODULE_IDLE_POOL: {
      (void)databuff_printf(buffer, size, pos, "IDLE_POOL");
      break;
    }

    case FETCH_MODULE_FETCH_STREAM: {
      (void)databuff_printf(buffer, size, pos, "[%s](%p)",
          to_cstring(svr_), fetch_stream_);
      break;
    }

    case FETCH_MODULE_DEAD_POOL: {
      (void)databuff_printf(buffer, size, pos, "DEAD_POOL");
      break;
    }

    default: {
      // Invalid module
      (void)databuff_printf(buffer, size, pos, "INVALID");
      break;
    }
  }

  return pos;
}

///////////////////////////////// FetchInfo /////////////////////////////////
void LSFetchCtx::FetchInfo::reset()
{
  cur_mod_.reset();
  out_mod_.reset();
  out_reason_ = "NONE";
  dispatched_count_from_idle_to_idle_ = 0;
}

void LSFetchCtx::FetchInfo::dispatch_in_idle_pool()
{
  cur_mod_.reset_to_idle_pool();
}

void LSFetchCtx::FetchInfo::dispatch_in_fetch_stream(const common::ObAddr &svr, FetchStream &fs)
{
  cur_mod_.reset_to_fetch_stream(svr, fs);
}

void LSFetchCtx::FetchInfo::dispatch_in_dead_pool()
{
  cur_mod_.reset_to_dead_pool();
}

void LSFetchCtx::FetchInfo::dispatch_out(const char *reason)
{
  out_mod_ = cur_mod_;

  if (is_from_idle_to_idle()) {
    ++dispatched_count_from_idle_to_idle_;
  }
  cur_mod_.reset();
  out_reason_ = reason;
}

int LSFetchCtx::FetchInfo::get_cur_svr_start_fetch_tstamp(const common::ObAddr &svr,
    int64_t &svr_start_fetch_ts) const
{
  int ret = OB_SUCCESS;

  svr_start_fetch_ts = OB_INVALID_TIMESTAMP;

  // Requires that the FetchStream module is currently in progress
  if (OB_UNLIKELY(FetchModule::FETCH_MODULE_FETCH_STREAM != cur_mod_.module_)) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("current module is not FetchStream", KR(ret), K(cur_mod_));
  }
  // verify server
  else if (OB_UNLIKELY(svr != cur_mod_.svr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("server does not match", KR(ret), K(svr), K(cur_mod_));
  } else {
    svr_start_fetch_ts = cur_mod_.start_fetch_tstamp_;
  }

  return ret;
}

//////////////////////////////////////// LSFetchInfoForPrint //////////////////////////////////

LSFetchInfoForPrint::LSFetchInfoForPrint() :
    tps_(0),
    is_discarded_(false),
    tls_id_(),
    progress_(),
    fetch_mod_(),
    dispatch_progress_(OB_INVALID_TIMESTAMP),
    dispatch_info_()
{
}

int LSFetchInfoForPrint::init(LSFetchCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ctx.get_dispatch_progress(dispatch_progress_, dispatch_info_))) {
    LOG_ERROR("get_dispatch_progress from LSFetchCtx fail", KR(ret), K(ctx));
  } else {
    tps_ = ctx.get_tps();
    is_discarded_ = ctx.is_discarded();
    tls_id_ = ctx.get_tls_id();
    ctx.get_progress_struct(progress_);
    fetch_mod_ = ctx.get_cur_fetch_module();
  }

  return ret;
}

void LSFetchInfoForPrint::print_fetch_progress(const char *description,
    const int64_t idx,
    const int64_t array_cnt,
    const int64_t cur_time) const
{
  _LOG_INFO("[STAT] %s idx=%ld/%ld tls_id=%s mod=%s "
      "discarded=%d delay=%s tps=%.2lf progress=%s",
      description, idx, array_cnt, to_cstring(tls_id_),
      to_cstring(fetch_mod_),
      is_discarded_, TVAL_TO_STR(cur_time * NS_CONVERSION - progress_.get_progress()),
      tps_, to_cstring(progress_));
}

void LSFetchInfoForPrint::print_dispatch_progress(const char *description,
    const int64_t idx,
    const int64_t array_cnt,
    const int64_t cur_time) const
{
  _LOG_INFO("[STAT] %s idx=%ld/%ld tls_id=%s delay=%s pending_task(queue/total)=%ld/%ld "
      "dispatch_progress=%s last_dispatch_log_lsn=%lu next_task=%s "
      "next_trans(log_lsn=%lu,committed=%d,ready_to_commit=%d,global_version=%s) checkpoint=%s",
      description, idx, array_cnt, to_cstring(tls_id_),
      TVAL_TO_STR(cur_time - dispatch_progress_/NS_CONVERSION),
      dispatch_info_.task_count_in_queue_,
      dispatch_info_.pending_task_count_,
      NTS_TO_STR(dispatch_progress_),
      dispatch_info_.last_dispatch_log_lsn_.val_,
      dispatch_info_.next_task_type_,
      dispatch_info_.next_trans_log_lsn_.val_,
      dispatch_info_.next_trans_committed_,
      dispatch_info_.next_trans_ready_to_commit_,
      NTS_TO_STR(dispatch_info_.next_trans_global_version_),
      NTS_TO_STR(dispatch_info_.current_checkpoint_));
}

}
}

#undef STAT
#undef _STAT
#undef ISTAT
#undef _ISTAT
#undef DSTAT
