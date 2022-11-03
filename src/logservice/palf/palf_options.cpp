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

#include "palf_options.h"
#include "log_define.h"

namespace oceanbase
{
namespace palf
{
void PalfDiskOptions::reset()
{
  log_disk_usage_limit_size_ = -1;
  log_disk_utilization_limit_threshold_ = -1;
  log_disk_utilization_threshold_ = -1;
}

bool PalfDiskOptions::is_valid() const
{
  return -1 != log_disk_usage_limit_size_ && log_disk_usage_limit_size_ >= 4 * PALF_PHY_BLOCK_SIZE
    && 1 <=log_disk_utilization_threshold_ && 100 >= log_disk_utilization_threshold_
    && 1 <=log_disk_utilization_limit_threshold_ && 100 >= log_disk_utilization_limit_threshold_
    && log_disk_utilization_limit_threshold_ > log_disk_utilization_threshold_;
}

bool PalfDiskOptions::operator==(const PalfDiskOptions &palf_disk_options) const
{
  return log_disk_usage_limit_size_ == palf_disk_options.log_disk_usage_limit_size_
    && log_disk_utilization_threshold_ == palf_disk_options.log_disk_utilization_threshold_
    && log_disk_utilization_limit_threshold_ == palf_disk_options.log_disk_utilization_limit_threshold_;
}
}
}
