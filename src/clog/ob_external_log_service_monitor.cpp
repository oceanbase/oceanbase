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

#include "ob_external_log_service_monitor.h"

namespace oceanbase {
namespace logservice {

int64_t ObExtLogServiceMonitor::locate_count_;
int64_t ObExtLogServiceMonitor::open_count_;
int64_t ObExtLogServiceMonitor::fetch_count_;
int64_t ObExtLogServiceMonitor::heartbeat_count_;

int64_t ObExtLogServiceMonitor::locate_time_;
int64_t ObExtLogServiceMonitor::open_time_;
int64_t ObExtLogServiceMonitor::fetch_time_;
int64_t ObExtLogServiceMonitor::l2s_time_;
int64_t ObExtLogServiceMonitor::svr_queue_time_;
int64_t ObExtLogServiceMonitor::heartbeat_time_;

int64_t ObExtLogServiceMonitor::enable_feedback_count_;
int64_t ObExtLogServiceMonitor::fetch_size_;
int64_t ObExtLogServiceMonitor::read_disk_count_;
int64_t ObExtLogServiceMonitor::fetch_log_count_;
int64_t ObExtLogServiceMonitor::get_cursor_batch_time_;
int64_t ObExtLogServiceMonitor::read_log_time_;
int64_t ObExtLogServiceMonitor::total_fetch_pkey_count_;
int64_t ObExtLogServiceMonitor::reach_upper_ts_pkey_count_;
int64_t ObExtLogServiceMonitor::reach_max_log_pkey_count_;
int64_t ObExtLogServiceMonitor::need_fetch_pkey_count_;
int64_t ObExtLogServiceMonitor::scan_round_count_;
int64_t ObExtLogServiceMonitor::round_rate_;

int64_t ObExtLogServiceMonitor::feedback_count_;
int64_t ObExtLogServiceMonitor::feedback_pkey_count_;
int64_t ObExtLogServiceMonitor::feedback_time_;

}  // namespace logservice
}  // namespace oceanbase
