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

#include "ob_i_sample_iterator.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

ObISampleIterator::ObISampleIterator(const SampleInfo &sample_info)
  : sample_info_(&sample_info)
{
}

ObISampleIterator::~ObISampleIterator()
{
  sample_info_ = nullptr;
}

bool ObISampleIterator::return_this_sample(const int64_t num) const
{
  bool bret = false;
  int64_t seed = sample_info_->seed_;
  if (seed == -1) {
    // seed is not specified, generate random seed
    seed = ObTimeUtility::current_time();
  }
  uint64_t hash_value = murmurhash(&num, sizeof(num), static_cast<uint64_t>(seed));
  double cut_off_tmp = static_cast<double>(UINT64_MAX) * sample_info_->percent_ / 100.0;
  uint64_t cut_off = cut_off_tmp >= UINT64_MAX ? UINT64_MAX : static_cast<uint64_t>(cut_off_tmp);
  bret = hash_value <= cut_off;
  return bret;
}

}
}
