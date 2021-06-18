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

#include "ob_log_external_monitor.h"

namespace oceanbase {
namespace clog {
int64_t ObExtReqStatistic::ReqByTsStatistic::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(req_count_), K(pkey_count_), K(err_count_), K(scan_file_count_), K(perf_total_));
  J_OBJ_END();
  return pos;
}
int64_t ObExtReqStatistic::ReqByIdStatistic::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(req_count_), K(pkey_count_), K(err_count_), K(scan_file_count_), K(perf_total_));
  J_OBJ_END();
  return pos;
}
int64_t ObExtReqStatistic::ReqFetchStatistic::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(req_count_), K(pkey_count_), K(err_count_), K(scan_file_count_), K(log_num_), K(offline_pkey_count_));
  J_OBJ_END();
  return pos;
}
int64_t ObExtReqStatistic::ReqHbStatistic::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(req_count_), K(pkey_count_), K(err_count_), K(disk_pkey_count_), K(disk_count_));
  J_OBJ_END();
  return pos;
}
ObExtReqStatistic::ReqByTsStatistic ObExtReqStatistic::ts;
ObExtReqStatistic::ReqByIdStatistic ObExtReqStatistic::id;
ObExtReqStatistic::ReqFetchStatistic ObExtReqStatistic::fetch;
ObExtReqStatistic::ReqHbStatistic ObExtReqStatistic::hb;
}  // namespace clog
}  // namespace oceanbase
