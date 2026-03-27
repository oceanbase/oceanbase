/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_cdc_service_monitor.h"

namespace oceanbase
{
namespace cdc
{
int64_t ObCdcServiceMonitor::locate_count_ = 0;
int64_t ObCdcServiceMonitor::fetch_count_ = 0;

int64_t ObCdcServiceMonitor::locate_time_;
int64_t ObCdcServiceMonitor::fetch_time_;
int64_t ObCdcServiceMonitor::l2s_time_;
int64_t ObCdcServiceMonitor::svr_queue_time_;

int64_t ObCdcServiceMonitor::fetch_size_;
int64_t ObCdcServiceMonitor::fetch_log_count_;
int64_t ObCdcServiceMonitor::reach_upper_ts_pkey_count_;
int64_t ObCdcServiceMonitor::reach_max_log_pkey_count_;
int64_t ObCdcServiceMonitor::need_fetch_pkey_count_;
int64_t ObCdcServiceMonitor::scan_round_count_;
int64_t ObCdcServiceMonitor::round_rate_;

} // namespace cdc
} // namespace oceanbase

