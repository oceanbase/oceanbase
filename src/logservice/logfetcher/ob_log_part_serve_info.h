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
 *
 * Partition Service Information
 */

#ifndef OCEANBASE_LOG_FETCHER_DATA_FILTER_H__
#define OCEANBASE_LOG_FETCHER_DATA_FILTER_H__

#include "ob_log_utils.h"       // NTS_TO_STR

namespace oceanbase
{
namespace logfetcher
{

struct PartServeInfo
{
  PartServeInfo() : start_serve_from_create_(false), start_serve_timestamp_(0)
  {
  }
  ~PartServeInfo() {}

  void reset()
  {
    start_serve_from_create_ = false;
    start_serve_timestamp_ = 0;
  }

  void reset(const bool start_serve_from_create, const int64_t start_serve_timestamp)
  {
    start_serve_from_create_ = start_serve_from_create;
    start_serve_timestamp_ = start_serve_timestamp;
  }

  // Determine if a partitioned transaction is in service
  // 1. if the input parameter is prepare log timestamp, return no service must not be served; return service must be served
  // 2. If the input parameter is commit log timestamp, return no service must be no service; return service must be no service
  bool is_served(const int64_t tstamp) const
  {
    bool bool_ret = false;

    // If a partition is served from the moment it is created, all its partition transactions are served
    if (start_serve_from_create_) {
      bool_ret = true;
    } else {
      // Otherwise, require the prepare log timestamp to be greater than or equal to the start service timestamp before the partitioned transaction is serviced
      bool_ret  = (tstamp >= start_serve_timestamp_);
    }

    return bool_ret;
  }

  TO_STRING_KV(K_(start_serve_from_create),
      "start_serve_timestamp", NTS_TO_STR(start_serve_timestamp_));

  bool      start_serve_from_create_;   // If start service from partition creation or not
  uint64_t  start_serve_timestamp_;     // Timestamp of the starting service
};

}
}

#endif
