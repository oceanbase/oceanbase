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
void PalfOptions::reset()
{
  disk_options_.reset();
  compress_options_.reset();
}

bool PalfOptions::is_valid() const
{
  return disk_options_.is_valid() && compress_options_.is_valid();
}

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

PalfDiskOptions &PalfDiskOptions::operator=(const PalfDiskOptions &other)
{
  log_disk_usage_limit_size_ = other.log_disk_usage_limit_size_;
  log_disk_utilization_threshold_ = other.log_disk_utilization_threshold_;
  log_disk_utilization_limit_threshold_ = other.log_disk_utilization_limit_threshold_;
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
}
}
