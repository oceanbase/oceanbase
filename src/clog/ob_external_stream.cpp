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

#define USING_LOG_PREFIX EXTLOG

#include "ob_external_stream.h"

#include "lib/stat/ob_diagnose_info.h"
#include "storage/ob_partition_service.h"   // ObPartitionService
#include "clog/ob_partition_log_service.h"  // ObIPartitionLogService

#include "ob_log_engine.h"
#include "ob_partition_log_service.h"

namespace oceanbase {
using namespace common;
using namespace clog;
using namespace storage;
using namespace obrpc;
namespace logservice {

// ************************ ObStreamItem ************************

// Do not support multithreading
// Verify whether need to continue fetching logs
//
// Follow one principle: in the same round of RPC, if it is determined that it is no longer
// necessary to continue to fetch logs, then it will no longer be necessary to fetch logs in the future.
int ObStreamItem::check_need_fetch_status(const ObLogRpcIDType fetch_rpc_id, const int64_t upper_limit_ts,
    storage::ObPartitionService& part_service, bool& status_changed, bool& need_fetch, bool& reach_upper_limit,
    bool& reach_max_log_id)
{
  need_fetch = true;       // need fetch by default
  status_changed = false;  // status not changed by default
  reach_upper_limit = false;
  reach_max_log_id = false;

  // Check whether it is a new round of fetching, if it is, clear the results of the previous round.
  if (fetch_rpc_id != fetch_rpc_id_) {
    // After reset, need to continue fetching logs by default
    need_fetch_status_.reset();
    fetch_rpc_id_ = fetch_rpc_id;
  }

  // If no longer need to fetch logs has been decided in a same round of PRC,
  // a fast path can be taken to determine fetch logs or not.
  if (!need_fetch_status_.need_fetch_) {
    need_fetch = need_fetch_status_.need_fetch_;
    reach_upper_limit = need_fetch_status_.reach_upper_limit_;
    reach_max_log_id = need_fetch_status_.reach_max_log_id_;
    status_changed = false;
  }

  // judgment "no need to fetch logs" not passed, continue judging
  if (need_fetch) {
    // Determine whether the upper limit is exceeded
    if (fetch_progress_ts_ >= upper_limit_ts) {
      reach_upper_limit = true;
    } else {
      // Check whether the maximum log is exceeded
      // Optimize fetching historical case, only when the max log is invalid
      // or the maximum log information has been exceeded, the maximum log information will be updated.
      if (OB_INVALID_ID == last_slide_log_id_ || next_log_id_ > last_slide_log_id_) {
        last_slide_log_id_ = get_last_slide_log_id_(pkey_, part_service);
      }

      if (OB_INVALID_ID != last_slide_log_id_ && next_log_id_ > last_slide_log_id_) {
        reach_max_log_id = true;
      }
    }

    // If upper bound or the maximum log is exceeded, it is no longer necessary to fetch log,
    // update the corresponding variable.
    if (reach_upper_limit || reach_max_log_id) {
      need_fetch = false;
      status_changed = true;

      // Update local status information, reuse in next loop
      need_fetch_status_.reset(need_fetch, reach_upper_limit, reach_max_log_id);
    }
  }

  return OB_SUCCESS;
}

uint64_t ObStreamItem::get_last_slide_log_id_(const ObPartitionKey& pkey, storage::ObPartitionService& part_service)
{
  uint64_t last_slide_log_id = OB_INVALID_ID;
  ObIPartitionGroupGuard guard;
  ObIPartitionLogService* pls = NULL;
  if (OB_SUCCESS != part_service.get_partition(pkey, guard) || NULL == guard.get_partition_group() ||
      NULL == (pls = guard.get_partition_group()->get_log_service())) {
  } else if (!(guard.get_partition_group()->is_valid())) {
    LOG_TRACE("partition is invalid when get_last_slide_log_id", K(pkey));
  } else {
    last_slide_log_id = pls->get_last_slide_log_id();
  }
  return last_slide_log_id;
}

// ************************ ObStream ************************
int ObStream::init(const ObLogOpenStreamReq::ParamArray& params, const int64_t lifetime,
    const LiboblogInstanceId& liboblog_instance_id)
{
  int ret = OB_SUCCESS;
  const int64_t count = params.count();
  if (OB_UNLIKELY(count <= 0) || OB_UNLIKELY(lifetime <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "invalid OpenStreamReq", K(ret), K(count), K(lifetime));
  } else {
    for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
      const ObLogOpenStreamReq::Param& param = params[i];
      if (OB_UNLIKELY(!param.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        EXTLOG_LOG(WARN, "invalid param", K(ret), K(param));
      } else {
        new (items_ + i) ObStreamItem(param.pkey_, param.start_log_id_);
        EXTLOG_LOG(TRACE, "Stream init item", K(i), K(param));
      }
    }
    if (OB_SUCC(ret)) {
      lifetime_ = lifetime;
      liboblog_instance_id_ = liboblog_instance_id;
      deadline_ts_ = ObTimeUtility::current_time() + lifetime;
      item_count_ = count;
      rr_pointer_ = 0;
      upper_limit_ts_ = common::OB_INVALID_TIMESTAMP;
    }
  }
  EXTLOG_LOG(INFO,
      "init fetch log stream",
      K(ret),
      K(lifetime),
      K(liboblog_instance_id),
      K(deadline_ts_),
      "part_count",
      count,
      K(params));
  return ret;
}

void ObStream::destroy()
{
  processing_ = false;
  liboblog_instance_id_.reset();
  lifetime_ = 0;
  deadline_ts_ = 0;
  item_count_ = 0;
  rr_pointer_ = 0;
  upper_limit_ts_ = common::OB_INVALID_TIMESTAMP;

  // FIXME: ObStreamItem will no longer traverse and execute destroy subsequently.
  // If the ObStreamItem needs to release memory in destroy, a memory leak may occur here.
}

// print verbose information
int64_t ObStream::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  for (int i = 0; i < item_count_ && pos < buf_len; i++) {
    pos += (items_ + i)->to_string(buf + pos, buf_len - pos);
  }
  return pos;
}

void ObStream::print_basic_info(const int64_t stream_idx, const ObStreamSeq& stream_seq) const
{
  int64_t min_ts = INT64_MAX;
  int64_t max_ts = 0;
  ObPartitionKey min_ts_pkey;
  ObPartitionKey max_ts_pkey;
  uint64_t max_ts_next_log_id = OB_INVALID_ID;
  uint64_t min_ts_next_log_id = OB_INVALID_ID;
  for (int i = 0; i < item_count_; i++) {
    const int64_t cur_pkey_progress_ts = items_[i].fetch_progress_ts_;
    const uint64_t cur_pkey_next_log_id = items_[i].next_log_id_;
    if (OB_LIKELY(OB_INVALID_TIMESTAMP != cur_pkey_progress_ts)) {
      if (cur_pkey_progress_ts < min_ts) {
        min_ts = cur_pkey_progress_ts;
        min_ts_pkey = items_[i].pkey_;
        min_ts_next_log_id = cur_pkey_next_log_id;
      }
      if (cur_pkey_progress_ts > max_ts) {
        max_ts = cur_pkey_progress_ts;
        max_ts_pkey = items_[i].pkey_;
        max_ts_next_log_id = cur_pkey_next_log_id;
      }
    }
  }
  EXTLOG_LOG(INFO,
      "[FETCH_LOG_STREAM] Print Stream Basic Information",
      K(stream_seq),
      K(stream_idx),
      K_(liboblog_instance_id),
      K_(item_count),
      K_(rr_pointer),
      K_(upper_limit_ts),
      K_(lifetime),
      K_(deadline_ts),
      K(min_ts_pkey),
      K(min_ts),
      K(min_ts_next_log_id),
      K(max_ts_pkey),
      K(max_ts),
      K(max_ts_next_log_id));
}

ObStreamItem* ObStream::cur_item()
{
  ObStreamItem* item = NULL;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(item_count_ <= 0) || OB_UNLIKELY(rr_pointer_ < 0) || OB_UNLIKELY(rr_pointer_ >= item_count_)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(WARN, "invalid ObStream", K(ret), K(item_count_), K(rr_pointer_));
  } else {
    item = items_ + rr_pointer_;
  }
  return item;
}

// it is possible to fetch part of log entries of this item and then
// exit the request (buf_full), the rr_pointer_ also advanced in such case,
// it does not matter.
ObStreamItem* ObStream::next_item()
{
  ObStreamItem* item = NULL;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(item_count_ <= 0) || OB_UNLIKELY(rr_pointer_ < 0) || OB_UNLIKELY(rr_pointer_ >= item_count_)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(WARN, "invalid ObStream", K(ret), K(item_count_), K(rr_pointer_));
  } else {
    // advance to next item
    rr_pointer_ = (rr_pointer_ + 1) % item_count_;
    item = items_ + rr_pointer_;
  }
  return item;
}

void ObStream::clear_progress_ts()
{
  const int64_t SMALL_TIMESTAMP_MAGIC = 123;  // small enough
  for (int i = 0; i < item_count_; i++) {
    items_[i].fetch_progress_ts_ = SMALL_TIMESTAMP_MAGIC;
  }
}

}  // namespace logservice
}  // namespace oceanbase
