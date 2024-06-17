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
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "log_define.h"
#include <cstdint>

namespace oceanbase
{
namespace palf
{
void PalfOptions::reset()
{
  disk_options_.reset();
  compress_options_.reset();
  rebuild_replica_log_lag_threshold_ = 0;
  enable_log_cache_ = false;
}

bool PalfOptions::is_valid() const
{
  return disk_options_.is_valid() && compress_options_.is_valid() && (rebuild_replica_log_lag_threshold_ >= 0);
}

void PalfDiskOptions::reset()
{
  log_disk_usage_limit_size_ = -1;
  log_disk_utilization_limit_threshold_ = -1;
  log_disk_utilization_threshold_ = -1;
  log_disk_throttling_percentage_ = -1;
  log_disk_throttling_maximum_duration_ = -1;
  log_writer_parallelism_ = -1;
}

bool PalfDiskOptions::is_valid() const
{
  const int64_t MIN_DURATION = 1 * 1000 * 1000L;
  const int64_t MAX_DURATION = 3 * 24 * 60 * 60 * 1000 * 1000L;
  return 0 <= log_disk_usage_limit_size_
    && 1 <=log_disk_utilization_threshold_ && 100 >= log_disk_utilization_threshold_
    && 1 <=log_disk_utilization_limit_threshold_ && 100 >= log_disk_utilization_limit_threshold_
    && log_disk_utilization_limit_threshold_ > log_disk_utilization_threshold_
    && log_disk_throttling_percentage_ >= MIN_WRITING_THTOTTLING_TRIGGER_PERCENTAGE
    && log_disk_throttling_percentage_ <= 100
    && log_disk_throttling_maximum_duration_ >= MIN_DURATION
    && log_disk_throttling_maximum_duration_ <= MAX_DURATION
    && log_writer_parallelism_ >= 1 && log_writer_parallelism_ <= 8;
}

bool PalfDiskOptions::operator==(const PalfDiskOptions &palf_disk_options) const
{
  return log_disk_usage_limit_size_ == palf_disk_options.log_disk_usage_limit_size_
    && log_disk_utilization_threshold_ == palf_disk_options.log_disk_utilization_threshold_
    && log_disk_utilization_limit_threshold_ == palf_disk_options.log_disk_utilization_limit_threshold_
    && log_disk_throttling_percentage_ == palf_disk_options.log_disk_throttling_percentage_
    && log_disk_throttling_maximum_duration_ == palf_disk_options.log_disk_throttling_maximum_duration_
    && log_writer_parallelism_ == palf_disk_options.log_writer_parallelism_;
}

bool PalfDiskOptions::operator!=(const PalfDiskOptions &palf_disk_options) const
{
  return !this->operator==(palf_disk_options);
}

PalfDiskOptions &PalfDiskOptions::operator=(const PalfDiskOptions &other)
{
  log_disk_usage_limit_size_ = other.log_disk_usage_limit_size_;
  log_disk_utilization_threshold_ = other.log_disk_utilization_threshold_;
  log_disk_utilization_limit_threshold_ = other.log_disk_utilization_limit_threshold_;
  log_disk_throttling_percentage_ = other.log_disk_throttling_percentage_;
  log_disk_throttling_maximum_duration_ = other.log_disk_throttling_maximum_duration_;
  log_writer_parallelism_ = other.log_writer_parallelism_;
  return *this;
}

void PalfTransportCompressOptions::reset()
{
  enable_transport_compress_ = false;
  transport_compress_func_ = ObCompressorType::INVALID_COMPRESSOR;
}

bool PalfTransportCompressOptions::is_valid() const
{
  return !enable_transport_compress_ || (ObCompressorType::INVALID_COMPRESSOR != transport_compress_func_);
}

//为了使用时可以无锁,需要考虑修改顺序
PalfTransportCompressOptions &PalfTransportCompressOptions::operator=(const PalfTransportCompressOptions &other)
{
  if (!other.enable_transport_compress_) {
    enable_transport_compress_ = other.enable_transport_compress_;
    MEM_BARRIER();
    transport_compress_func_ = other.transport_compress_func_;
  } else {
    transport_compress_func_ = other.transport_compress_func_;
    MEM_BARRIER();
    enable_transport_compress_ = other.enable_transport_compress_;
  }
  return *this;
}

static const char *access_mode_strs[] = {
  "INVALID_ACCESS_MODE",
  "APPEND",
  "RAW_WRITE",
  "FLASHBACK",
  "PREPARE_FLASHBACK"
};

int get_access_mode(const common::ObString &str, AccessMode &mode)
{
  int ret = OB_SUCCESS;
  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    mode = AccessMode::INVALID_ACCESS_MODE;
    for (int64_t i = 0; i < ARRAYSIZEOF(access_mode_strs); i++) {
      if (0 == str.case_compare(access_mode_strs[i])) {
        mode = static_cast<AccessMode>(i);
        break;
      }
    }
  }
  if (AccessMode::INVALID_ACCESS_MODE == mode) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

void PalfThrottleOptions::reset()
{
  total_disk_space_ = -1;
  stopping_writing_percentage_ = -1;
  trigger_percentage_ = -1;
  maximum_duration_ = -1;
  unrecyclable_disk_space_ = -1;
}

bool PalfThrottleOptions::is_valid() const
{
  return (total_disk_space_ > 0
  && stopping_writing_percentage_ > 0 && stopping_writing_percentage_ <= 100
  && trigger_percentage_ >= MIN_WRITING_THTOTTLING_TRIGGER_PERCENTAGE && trigger_percentage_ <= 100
  && maximum_duration_ > 0
  && unrecyclable_disk_space_ >= 0);
}

bool PalfThrottleOptions::operator==(const PalfThrottleOptions &other) const
{
  return total_disk_space_  == other.total_disk_space_
    && stopping_writing_percentage_ == other.stopping_writing_percentage_
    && trigger_percentage_ == other.trigger_percentage_
    && maximum_duration_ == other.maximum_duration_
    && unrecyclable_disk_space_ == other.unrecyclable_disk_space_;
}

}// end of namespace palf
}// end of namespace oceanbase
