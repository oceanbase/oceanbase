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
 *  Fetch log stream type
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_FETCH_STREAM_TYPE_H__
#define OCEANBASE_LIBOBCDC_OB_LOG_FETCH_STREAM_TYPE_H__

namespace oceanbase
{
namespace libobcdc
{

// Fetch log stream type
//
// 1. Hot streams: streams that are written to more frequently and have a larger log volume
// 2. Cold streams: streams that have not been written to for a long time and rely on heartbeats to maintain progress
// 3. DDL streams: streams dedicated to serving DDL partitions
//
// Different streams with different Strategies
// 1. Hot streams fetch logs frequently and need to allocate more resources to fetch logs
// 2. Cold streams have no logs for a long time, so they can reduce the frequency of log fetching and heartbeats and use less resources
// 3. DDL streams are always of the hot stream type, ensuring sufficient resources, always real-time, and immune to pauses
enum FetchStreamType
{
  FETCH_STREAM_TYPE_UNKNOWN = -1,
  FETCH_STREAM_TYPE_HOT = 0,        // Hot stream
  FETCH_STREAM_TYPE_COLD = 1,       // Cold stream
  FETCH_STREAM_TYPE_SYS_LS = 2,     // SYS LS stream
  FETCH_STREAM_TYPE_MAX
};

bool is_fetch_stream_type_valid(const FetchStreamType type);
const char *print_fetch_stream_type(FetchStreamType type);

}
}

#endif
