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
#include "ob_log_config.h"                    // ObLogConfig
#include "ob_log_ls_fetch_mgr.h"              // IObLogLSFetchMgr
#include "ob_log_trace_id.h"                  // ObLogTraceIdGuard
#include "ob_log_instance.h"                  // TCTX
#include "ob_log_fetcher.h"                   // IObLogFetcher

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
namespace libobcdc
{

/////////////////////////////// LSFetchCtx /////////////////////////////////
LSFetchCtx::LSFetchCtx()
{
  reset();
}

LSFetchCtx::~LSFetchCtx()
{
  reset();
}

void LSFetchCtx::configure(const ObLogConfig &config)
{
}

void LSFetchCtx::reset()
{
  // Note: The default stream type for setting ls is hot stream
  stype_ = FETCH_STREAM_TYPE_HOT;
  state_ = STATE_NORMAL;
  discarded_ = false;
  tls_id_.reset();
  serve_info_.reset();
  progress_id_ = -1;
  ls_fetch_mgr_ = NULL;
  part_trans_resolver_ = NULL;
  last_sync_progress_ = OB_INVALID_TIMESTAMP;
  progress_.reset();
  fetch_info_.reset();
  //svr_list_need_update_ = true;
  //TODO tmp test
  svr_list_need_update_ = false;
  start_lsn_locate_req_.reset();
  FetchTaskListNode::reset();
  mem_storage_.destroy();
  group_iterator_.destroy();
  fetched_log_size_ = 0;
  ctx_desc_.reset();
}

int LSFetchCtx::init(const TenantLSID &tls_id,
    const int64_t start_tstamp_ns,
    const palf::LSN &start_lsn,
    const int64_t progress_id,
    IObCDCPartTransResolver &part_trans_resolver,
    IObLogLSFetchMgr &ls_fetch_mgr)
{
  int ret = OB_SUCCESS;
  // If the start lsn is 0, the service is started from creation
  bool start_serve_from_create = (palf::PALF_INITIAL_LSN_VAL == start_lsn.val_);

  reset();

  tls_id_ = tls_id;
  serve_info_.reset(start_serve_from_create, start_tstamp_ns);
  progress_.reset(start_lsn, start_tstamp_ns);
  progress_id_ = progress_id;
  ls_fetch_mgr_ = &ls_fetch_mgr;
  part_trans_resolver_ = &part_trans_resolver;
  fetched_log_size_ = 0;

  // Default is SYS LS type if it is a sys LS, otherwise it is a hot stream
  if (tls_id.is_sys_log_stream()) {
    //is_sys_ls
    stype_ = FETCH_STREAM_TYPE_SYS_LS;
  } else {
    stype_ = FETCH_STREAM_TYPE_HOT;
  }

  if (start_lsn.is_valid()) {
    // LSN is valid, init mem_storage; otherwise after need locate start_lsn success, we can init mem_storage
    if (OB_FAIL(init_group_iterator_(start_lsn))) {
      LOG_ERROR("init_group_iterator_ failed", KR(ret), K_(tls_id), K(start_lsn));
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
    palf::GetFileEndLSN group_iter_end_func = [&](){ return INT64_MAX; };

    if (OB_FAIL(mem_storage_.init(start_lsn))) {
      LOG_ERROR("init mem_storage_ failed", KR(ret), K_(tls_id), K(start_lsn));
    }
    // init palf iterator
    else if (OB_UNLIKELY(group_iterator_.is_inited())) {
      ret = OB_INIT_TWICE;
      LOG_ERROR("mem_storage_ or group_iterator_ already inited", KR(ret), K_(mem_storage), K_(group_iterator));
    } else if (OB_FAIL(group_iterator_.init(start_lsn, &mem_storage_, group_iter_end_func))) {
      LOG_ERROR("init group_iterator_ failed" ,KR(ret), K_(tls_id), K(start_lsn));
    }
  }

  return ret;
}

int LSFetchCtx::append_log(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(mem_storage_.append(buf, buf_len))) {
    LOG_ERROR("append log into mem_storage_ failed", KR(ret), K_(tls_id), KP(buf), K(buf_len), K_(mem_storage), K_(group_iterator));
  } else {
    ATOMIC_AAF(&fetched_log_size_, buf_len);
    LOG_DEBUG("append_log succ", K(buf_len), KPC(this));
  }

  return ret;
}

int LSFetchCtx::get_next_group_entry(palf::LogGroupEntry &group_entry, palf::LSN &lsn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!group_iterator_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_ERROR("group_iterator_ not init!");
  } else if (OB_FAIL(group_iterator_.next())) {
    if (OB_ITER_END != ret) {
      LOG_ERROR("iterate group_entry failed", KR(ret), K_(group_iterator));
    }
  } else if (OB_FAIL(group_iterator_.get_entry(group_entry, lsn))) {
    LOG_ERROR("get_next_group_entry failed", KR(ret), K_(group_iterator));
  } else { /* success */ }

  return ret;
}

int LSFetchCtx::get_log_entry_iterator(palf::LogGroupEntry &group_entry,
    palf::LSN &start_lsn,
    palf::MemPalfBufferIterator &entry_iter)
{
  int ret = OB_SUCCESS;
  palf::GetFileEndLSN entry_iter_end_func = [&](){ return start_lsn + group_entry.get_group_entry_size(); };

  if (OB_FAIL(entry_iter.init(start_lsn, &mem_storage_, entry_iter_end_func))) {
    LOG_ERROR("entry_iter init failed", KR(ret), K_(mem_storage), K_(group_iterator), K(group_entry), K(start_lsn));
  }

  return ret;
}

int LSFetchCtx::dispatch_heartbeat_if_need_()
{
  int ret = OB_SUCCESS;
  // Get current progress
  int64_t cur_progress = get_progress();
  if (OB_ISNULL(part_trans_resolver_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("invalid part_trans_resolver_", KR(ret), K_(part_trans_resolver));
  }
  // heartbeats are sent down only if progress updated
  else if (cur_progress != last_sync_progress_) {
    LOG_DEBUG("ls progress updated. generate HEARTBEAT task", K_(tls_id),
        "last_sync_progress", NTS_TO_STR(last_sync_progress_),
        "cur_progress", NTS_TO_STR(cur_progress));
    if (OB_FAIL(part_trans_resolver_->heartbeat(cur_progress))) {
      LOG_ERROR("generate HEARTBEAT task fail", KR(ret), K_(tls_id), K(cur_progress));
    } else {
      last_sync_progress_ = cur_progress;
    }
  }
  return ret;
}

int LSFetchCtx::sync(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  int64_t pending_trans_count = 0;
  // Heartbeat issued according to conditions
  if (OB_FAIL(dispatch_heartbeat_if_need_())) {
    LOG_ERROR("dispatch_heartbeat_if_need_ fail", KR(ret));
  } else {
    ret = dispatch_(stop_flag, pending_trans_count);
  }
  return ret;
}

int LSFetchCtx::dispatch_(volatile bool &stop_flag, int64_t &pending_trans_count)
{
  int ret = OB_SUCCESS;
  // get current state
  int cur_state = state_;

  if (OB_ISNULL(part_trans_resolver_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid part trans resolver", KR(ret), K(part_trans_resolver_));
  } else if (OB_FAIL(part_trans_resolver_->dispatch(stop_flag, pending_trans_count))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("resolver dispatch fail", KR(ret));
    }
  } else {
    LOG_DEBUG("dispatch trans task success", K_(tls_id), K(pending_trans_count),
        "state", print_state(state_));
  }
  return ret;
}

int LSFetchCtx::read_log(
    const palf::LogEntry &log_entry,
    const palf::LSN &lsn,
    IObCDCPartTransResolver::MissingLogInfo &missing,
    TransStatInfo &tsi,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const char *buf = log_entry.get_data_buf();
  const int64_t buf_len = log_entry.get_data_len();
  const int64_t submit_ts = log_entry.get_log_ts();
  int64_t pos = 0;
  logservice::ObLogBaseHeader log_base_header;

  if (OB_ISNULL(part_trans_resolver_)) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("invalid part trans resolver", KR(ret), K_(part_trans_resolver));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(0 >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid log_entry buf or buf_len", KR(ret), K(log_entry), K(lsn), K_(tls_id));
  }
  // deserialize palf log base header(to get log_type)
  else if (OB_FAIL(deserialize_log_entry_base_header_(buf, buf_len, pos, log_base_header))) {
    LOG_ERROR("deserialize_log_entry_base_header_ failed", KR(ret), K(log_entry), K(pos), K(lsn), K_(tls_id));
  } else {
    const logservice::ObLogBaseType &base_type = log_base_header.get_log_type();

    switch (base_type) {
      case logservice::ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE:
      {
        if (OB_FAIL(part_trans_resolver_->read(buf, buf_len, pos, lsn, submit_ts, serve_info_, missing, tsi))) {
          if (OB_ITEM_NOT_SETTED != ret && OB_IN_STOP_STATE != ret) {
            LOG_ERROR("resolve trans log failed", KR(ret), K(log_entry), K(log_base_header));
          }
        }
        break;
      }
      case logservice::ObLogBaseType::KEEP_ALIVE_LOG_BASE_TYPE:
      {
        // update progress while group_entry consumed (in fetch_stream)
        LOG_DEBUG("LOG_STREAM_KEEP_ALIVE", K_(tls_id), K(submit_ts));
        break;
      }
      case logservice::ObLogBaseType::GC_LS_LOG_BASE_TYPE:
      {
        // Processing OFFLINE logs
        if (OB_FAIL(handle_offline_ls_log_(log_entry, stop_flag))) {
          LOG_ERROR("handle_offline_ls_log_ failed", KR(ret), K(log_entry), K(log_base_header));
        }
        break;
      }
      default:
      {
        LOG_DEBUG("ignore palf log", K(log_base_header), K(log_entry));
        break;
      }
    }
  }

  return ret;
}

int LSFetchCtx::read_miss_tx_log(
    const palf::LogEntry &log_entry,
    const palf::LSN &lsn,
    TransStatInfo &tsi,
    IObCDCPartTransResolver::MissingLogInfo &missing)
{
  int ret = OB_SUCCESS;
  const char *buf = log_entry.get_data_buf();
  const int64_t buf_len = log_entry.get_data_len();
  const int64_t submit_ts = log_entry.get_log_ts();
  int64_t pos = 0;
  logservice::ObLogBaseHeader log_base_header;

  if (OB_ISNULL(part_trans_resolver_)) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("invalid part trans resolver", KR(ret), K(part_trans_resolver_));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(0 >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid log_entry buf or buf_len", KR(ret), K(log_entry), K(lsn), K_(tls_id));
  } else if (OB_FAIL(deserialize_log_entry_base_header_(buf, buf_len, pos, log_base_header))) {
    LOG_ERROR("deserialize_log_entry_base_header_ for miss_log fail", KR(ret), K(log_entry));
  } else if (logservice::ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE != log_base_header.get_log_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("expect trans log while reading miss_log", KR(ret), K(log_entry), K(log_base_header), K(lsn));
  } else {

    if (OB_FAIL(part_trans_resolver_->read(buf, buf_len, pos, lsn, submit_ts, serve_info_, missing, tsi))) {
      if (OB_ITEM_NOT_SETTED == ret) {
        // if found new generated miss log while resolving misslog, FetchStream will
        // found new_generated_missing_info not empty and goon with misslog process
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("resolve miss_log fail", KR(ret), K(log_entry), K(log_base_header), K(lsn), K(missing));
      }
    }
  }

  return ret;
}

int LSFetchCtx::update_progress(
    const palf::LogGroupEntry &group_entry,
    const palf::LSN &group_entry_lsn)
{
  int ret = OB_SUCCESS;
  const int64_t submit_ts = group_entry.get_log_ts();
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

      LOG_DEBUG("read log and update progress success", K_(tls_id), K(group_entry), K_(progress));
    }
  }

  return ret;
}

int LSFetchCtx::handle_offline_ls_log_(const palf::LogEntry &log_entry,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  // const uint64_t log_id = log_entry.get_header().get_log_id();
  // const int64_t tstamp = log_entry.get_header().get_submit_timestamp();

  ISTAT("[HANDLE_OFFLINE_LOG] begin", K_(tls_id), "state", print_state(state_));

  // For OFFLINE logs, only tasks in NORMAL state are processed
  // Tasks in other states will be deleted by other scenarios responsible for the ls
  //
  // Ensure that the discard recycling mechanism.
  //
  // 1. STATE_NORMAL: discard will be set when OFFLINE logging or ls deletion DDL is encountered
  //
  // Note: Mechanically, we have to take precautions in many ways and cannot rely on one mechanism to guarantee ls recovery.
  // There are two scenarios in which partitions need to be reclaimed.
  // 1. ls deletion by DDL: this includes deleting tables, deleting partitions, deleting DBs, deleting tenants, etc. This scenario relies on DDL deletion to be sufficient
  // The observer ensures that the ls is not iterated over in the schema after the DDL is deleted
  if (STATE_NORMAL == state_) {
    int64_t pending_trans_count = 0;
    // First ensure that all tasks in the queue are dispatched
    if (OB_FAIL(dispatch_(stop_flag, pending_trans_count))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("dispatch task fail", KR(ret), K(tls_id_));
      }
    }
    // Check if there are pending transactions to be output
    else if (OB_UNLIKELY(pending_trans_count > 0)) {
      ret = OB_INVALID_DATA;
      LOG_ERROR("there are still pending trans after dispatch when processing offline log, unexcept error",
          KR(ret), K(pending_trans_count), K(tls_id_), K(state_));
    } else {
      // Finally mark the ls as ready for deletion
      // Note: there is a concurrency situation here, after a successful setup, it may be dropped into the DEAD POOL for recycling by other threads immediately
      // Since all data is already output here, it doesn't matter if it goes to the DEAD POOL
      set_discarded();
    }
  }

  ISTAT("[HANDLE_OFFLINE_LOG] end", KR(ret), K_(tls_id), "state", print_state(state_));

  return ret;
}

int LSFetchCtx::deserialize_log_entry_base_header_(const char *buf, const int64_t buf_len, int64_t &pos,
                                                     logservice::ObLogBaseHeader &log_base_header)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || OB_UNLIKELY(0 != pos) || OB_UNLIKELY(0 >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid log args", KR(ret), K_(tls_id), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(log_base_header.deserialize(buf, buf_len, pos))) {
    LOG_ERROR("deserialize log_base_header failed", KR(ret), K_(tls_id), K(buf), K(buf_len), K(pos));
  } else { /* success */ }

  return ret;
}

// This function is called by the DEAD POOL and will clean up all unpublished tasks and then downlink them
//
// The implementation guarantees that tasks will only be produced when the log is read.
int LSFetchCtx::offline(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ISTAT("[OFFLINE_PART] begin", K_(tls_id), "state", print_state(state_), K_(discarded));
  if (OB_ISNULL(part_trans_resolver_)) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("invalid part trans resolver", KR(ret), K(part_trans_resolver_));
  }
  // launch offline_ls task
  else if (OB_FAIL(part_trans_resolver_->offline(stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("delete ls by part trans resolver fail", KR(ret));
    }
  } else {
    // success
    ISTAT("[OFFLINE_PART] end", K_(tls_id), "state", print_state(state_), K_(discarded));
  }
  return ret;
}

int LSFetchCtx::get_log_route_service_(logservice::ObLogRouteService *&log_route_service)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(TCTX.fetcher_->get_log_route_service(log_route_service))) {
    LOG_ERROR("Fetcher get_log_route_service failed", KR(ret));
  } else if (OB_ISNULL(log_route_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("log_route_service is nullptr", KR(ret), K(log_route_service));
  }

  return ret;
}

bool LSFetchCtx::need_update_svr_list()
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  int64_t cur_time = get_timestamp();
  int64_t avail_svr_count = 0;
  logservice::ObLogRouteService *log_route_service = nullptr;

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
    // if (avail_svr_count <= 0 || svr_list_need_update_) {
    if (avail_svr_count <= 0) {
      bool_ret = true;
    }
  }

  LOG_DEBUG("need_update_svr_list", K(bool_ret), KR(ret), K(tls_id_),
      K(svr_list_need_update_), K(avail_svr_count));

  return bool_ret;
}

bool LSFetchCtx::need_locate_start_lsn() const
{
  bool bool_ret = false;

  bool_ret = ! (progress_.get_next_lsn().is_valid());

  LOG_DEBUG("need_locate_start_lsn", K(tls_id_), K(bool_ret), K(progress_));

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
    LOG_DEBUG("async_server_query_req succ", K_(tls_id));
  }

  return ret;
}

int LSFetchCtx::locate_start_lsn(IObLogStartLSNLocator &start_lsn_locator)
{
  int ret = OB_SUCCESS;
  int state = start_lsn_locate_req_.get_state();
  int64_t start_tstamp_ns = serve_info_.start_serve_timestamp_;
  LocateSvrList svr_list;

  if (StartLSNLocateReq::IDLE == state) {
    start_lsn_locate_req_.reset(tls_id_, start_tstamp_ns);
    logservice::ObLogRouteService *log_route_service = nullptr;

    if (OB_FAIL(get_log_route_service_(log_route_service))) {
      LOG_ERROR("get_log_route_service_ failed", KR(ret));
    } else if (OB_FAIL(log_route_service->get_server_array_for_locate_start_lsn(tls_id_.get_tenant_id(), tls_id_.get_ls_id(),
       svr_list))) {
      LOG_ERROR("ObLogRouteService get_server_array_for_locate_start_lsn failed", KR(ret), K(tls_id_));
    } else if (svr_list.count() <= 0) {
      LOG_INFO("server list is empty for locating start lsn, mark for updating server list");
      mark_svr_list_update_flag(true);
    } else if (OB_FAIL(init_locate_req_svr_list_(start_lsn_locate_req_, svr_list))) {
      LOG_ERROR("init_locate_req_svr_list_ fail", KR(ret), K(svr_list));
    } else if (OB_FAIL(start_lsn_locator.async_start_lsn_req(&start_lsn_locate_req_))) {
      LOG_ERROR("launch async start lsn request fail", KR(ret), K(start_lsn_locate_req_));
    } else {
      LOG_INFO("start lsn locate request launched", K_(tls_id),
          "start_tstamp", NTS_TO_STR(start_tstamp_ns),
          "svr_cnt", start_lsn_locate_req_.svr_list_.count(),
          "svr_list", start_lsn_locate_req_.svr_list_);
    }
  } else if (StartLSNLocateReq::REQ == state) {
    // On request
  } else if (StartLSNLocateReq::DONE == state) {
    palf::LSN start_lsn;
    common::ObAddr locate_svr;

    if (! start_lsn_locate_req_.get_result(start_lsn, locate_svr)) {
      LOG_ERROR("start lsn locate fail", K_(start_lsn_locate_req));
    } else if (OB_FAIL(init_group_iterator_(start_lsn))) {
      LOG_ERROR("init_group_iterator_ failed", KR(ret), K_(tls_id), K(start_lsn));
    } else {
      progress_.set_next_lsn(start_lsn);

      LOG_INFO("start lsn located succ", K_(tls_id), K(start_lsn),
          "start_tstamp", NTS_TO_STR(start_tstamp_ns), K(locate_svr),
          "svr_cnt", start_lsn_locate_req_.svr_list_.count(),
          "svr_list", start_lsn_locate_req_.svr_list_);
    }

    // Reset the location request, whether successful or not
    start_lsn_locate_req_.reset();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unknown start lsn locator request state", KR(ret), K(state),
        K(start_lsn_locate_req_));
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
    bool &is_fetch_timeout,                       // Is the log fetch timeout
    bool &is_fetch_timeout_on_lagged_replica)     // Is the log fetch timeout on a lagged replica
{
  int ret = OB_SUCCESS;
  int64_t cur_time = get_timestamp();
  int64_t svr_start_fetch_tstamp = OB_INVALID_TIMESTAMP;
  // Partition timeout, after which time progress is not updated, it is considered to be a log fetch timeout
  const int64_t ls_fetch_progress_update_timeout = TCONF.ls_fetch_progress_update_timeout_sec * _SEC_;
  // Timeout period for partitions on lagging replica, compared to normal timeout period
  const int64_t ls_fetch_progress_update_timeout_for_lagged_replica = TCONF.ls_fetch_progress_update_timeout_sec_for_lagged_replica * _SEC_;

  is_fetch_timeout = false;
  is_fetch_timeout_on_lagged_replica = false;

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
      is_fetch_timeout_on_lagged_replica = false;
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
        is_fetch_timeout_on_lagged_replica = false;
      } else {
        // Before the progress timeout, verify that the server fetching the logs is a lagged replica, and if the logs are not fetched on the lagged replica for some time, then it is also considered a progress timeout
        // Generally, the timeout for a lagged copy is less than the progress timeout
        // ls_fetch_progress_update_timeout_for_lagged_replica < ls_fetch_progress_update_timeout
        //
        // How to define a long timeout for fetching logs on a lagged replica?
        // 1. lagged replica: the next log does exist, but this server can't fetch it, indicating that this server is most likely behind the replica
        // 2. When to start counting the timeout: from the time libobcdc confirms the existence of the next log
        if (progress_update_interval >= ls_fetch_progress_update_timeout_for_lagged_replica)  {  // Progress update time over lagging replica configuration item
          is_fetch_timeout = true;
          is_fetch_timeout_on_lagged_replica = true;
        }
      }

      if (is_fetch_timeout) {
        LOG_INFO("[CHECK_PROGRESS_TIMEOUT]", K_(tls_id), K(svr),
            K(is_fetch_timeout), K(is_fetch_timeout_on_lagged_replica),
            K(progress_update_interval),
            K(progress_),
            "update_limit", NTS_TO_STR(upper_limit),
            "last_update_tstamp", TS_TO_STR(last_update_tstamp),
            "svr_start_fetch_tstamp", TS_TO_STR(svr_start_fetch_tstamp));
      } else {
        LOG_DEBUG("[CHECK_PROGRESS_TIMEOUT]", K_(tls_id), K(svr),
            K(is_fetch_timeout), K(is_fetch_timeout_on_lagged_replica),
            K(progress_update_interval),
            K(progress_),
            "update_limit", NTS_TO_STR(upper_limit),
            "last_update_tstamp", TS_TO_STR(last_update_tstamp),
            "svr_start_fetch_tstamp", TS_TO_STR(svr_start_fetch_tstamp));
      }
    }
  }
  return ret;
}

int LSFetchCtx::get_dispatch_progress(int64_t &dispatch_progress, PartTransDispatchInfo &dispatch_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_trans_resolver_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("invalid part trans resolver", KR(ret), K(part_trans_resolver_));
  } else if (OB_FAIL(part_trans_resolver_->get_dispatch_progress(dispatch_progress,
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
  return start_lsn_locate_req_.is_state_req();
}

void LSFetchCtx::print_dispatch_info() const
{
  LSProgress cur_progress;
  progress_.atomic_copy(cur_progress);

  int64_t progress = cur_progress.get_progress();

  if (fetch_info_.is_from_idle_to_idle()) {
    if (REACH_TIME_INTERVAL(10 * _SEC_)) {
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
  } else if (OB_FAIL(log_route_service->need_switch_server(tls_id_.get_tenant_id(), tls_id_.get_ls_id(),
          next_lsn, cur_svr))) {
    LOG_ERROR("ObLogRouteService need_switch_server failed", KR(ret), K(tls_id_), K(next_lsn),
        K(cur_svr));
  } else {}

  return bool_ret;
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
