/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SQL
#include "share/external_table/ob_odps_table_utils.h"

namespace oceanbase
{
using namespace common;
using namespace sql;

namespace share
{

int ObOdpsTableUtils::resolve_odps_start_step(const ObOdpsScanTask *scan_task,
  int64_t &start,
  int64_t &step)
{
  int ret = OB_SUCCESS;
  start = scan_task->first_lineno_;
  int64_t end = scan_task->last_lineno_;
  if (end != INT64_MAX) {
    step = end - start;
  } else {
    step = INT64_MAX;
  }
  return ret;
}

int ObOdpsTableUtils::make_odps_scan_task(const common::ObString &file_url,
                                              const uint64_t part_id,
                                              const int64_t first_lineno,
                                              const int64_t last_lineno,
                                              const common::ObString &session_id,
                                              const int64_t first_split_idx,
                                              const int64_t last_split_idx,
                                              ObOdpsScanTask &scan_task)
{
  int ret = OB_SUCCESS;
  scan_task.file_url_ = file_url;
  scan_task.part_id_ = part_id;
  scan_task.session_id_ = session_id;
  scan_task.first_split_idx_ = first_split_idx;
  scan_task.last_split_idx_ = last_split_idx;
  scan_task.first_lineno_ = first_lineno;
  scan_task.last_lineno_ = last_lineno;
  return ret;
}

}  // namespace share
}  // namespace oceanbase
