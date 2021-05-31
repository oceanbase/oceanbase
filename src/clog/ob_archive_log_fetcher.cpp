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

#include "ob_archive_log_fetcher.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::clog;

namespace oceanbase {
namespace logservice {

int ObArchiveLogFetcher::fetch_log(const ObPGKey& pg_key, const ObReadParam& param, ObReadBuf& rbuf, ObReadRes& res)
{
  int ret = OB_SUCCESS;
  int64_t end_ts = ObTimeUtility::current_time() + param.timeout_;
  ObReadCost read_cost;
  bool fetch_log_from_hot_cache = false;
  int64_t log_entry_size = 0;

  if (OB_UNLIKELY(!pg_key.is_valid() || !rbuf.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(pg_key), K(param), K(rbuf), KR(ret));
  } else {
    if (OB_FAIL(fetch_log_entry_(
            pg_key, param, rbuf.buf_, rbuf.buf_len_, end_ts, read_cost, fetch_log_from_hot_cache, log_entry_size))) {

      LOG_WARN("failed to fetch_log_entry_", K(pg_key), K(param), KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    res.buf_ = rbuf.buf_;
    res.data_len_ = log_entry_size;
  }
  return ret;
}

}  // namespace logservice
}  // end of namespace oceanbase
