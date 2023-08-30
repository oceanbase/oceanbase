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

#define USING_LOG_PREFIX PALF
#include "lib/stat/ob_session_stat.h"
#include "log_cache.h"
#include "palf_handle_impl.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{
LogHotCache::LogHotCache()
  : palf_id_(INVALID_PALF_ID),
    palf_handle_impl_(NULL),
    read_size_(0),
    hit_count_(0),
    read_count_(0),
    last_print_time_(0),
    is_inited_(false)
{}

LogHotCache::~LogHotCache()
{
  destroy();
}

void LogHotCache::destroy()
{
  reset();
}

void LogHotCache::reset()
{
  is_inited_ = false;
  palf_handle_impl_ = NULL;
  palf_id_ = INVALID_PALF_ID;
}

int LogHotCache::init(const int64_t palf_id, IPalfHandleImpl *palf_handle_impl)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (false == is_valid_palf_id(palf_id) || OB_ISNULL(palf_handle_impl)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    palf_id_ = palf_id;
    palf_handle_impl_ = palf_handle_impl;
    is_inited_ = true;
  }
  return ret;
}

int LogHotCache::read(const LSN &read_begin_lsn,
                      const int64_t in_read_size,
                      char *buf,
                      int64_t &out_read_size) const
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0, hit_cnt = 0, read_cnt = 0;
  out_read_size = 0;
  int64_t start_ts = ObTimeUtility::fast_current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!read_begin_lsn.is_valid() || in_read_size <= 0 || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K_(palf_id), K(read_begin_lsn), K(in_read_size),
        KP(buf));
  } else if (OB_FAIL(palf_handle_impl_->read_data_from_buffer(read_begin_lsn, in_read_size, \
          buf, out_read_size))) {
    if (OB_ERR_OUT_OF_LOWER_BOUND != ret) {
      PALF_LOG(WARN, "read_data_from_buffer failed", K(ret), K_(palf_id), K(read_begin_lsn),
          K(in_read_size));
    }
  } else {
    int64_t cost_ts = ObTimeUtility::fast_current_time() - start_ts;
    hit_cnt = ATOMIC_AAF(&hit_count_, 1);
    read_size = ATOMIC_AAF(&read_size_, out_read_size);
    EVENT_TENANT_INC(ObStatEventIds::PALF_READ_COUNT_FROM_CACHE, MTL_ID());
    EVENT_ADD(ObStatEventIds::PALF_READ_SIZE_FROM_CACHE, out_read_size);
    EVENT_ADD(ObStatEventIds::PALF_READ_TIME_FROM_CACHE, cost_ts);
    PALF_LOG(TRACE, "read_data_from_buffer success", K(ret), K_(palf_id), K(read_begin_lsn),
        K(in_read_size), K(out_read_size));
  }
  read_cnt = ATOMIC_AAF(&read_count_, 1);
  if (palf_reach_time_interval(PALF_STAT_PRINT_INTERVAL_US, last_print_time_)) {
    read_cnt = read_cnt == 0 ? 1 : read_cnt;
    PALF_LOG(INFO, "[PALF STAT HOT CACHE HIT RATE]", K_(palf_id), K(read_size), K(hit_cnt), K(read_cnt), "hit rate", hit_cnt * 1.0 / read_cnt);
    hit_count_ = 0;
    read_size_ = 0;
    read_count_ = 0;
  }
  return ret;
}

} // end namespace palf
} // end namespace oceanbase
