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
#define USING_LOG_PREFIX SHARE

#include "ob_throttling_utils.h"
#include "math.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_utility.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace share
{
int ObThrottlingUtils::calc_decay_factor(const int64_t available_size,
                                         const int64_t duration_us,
                                         const int64_t chunk_size,
                                         double &decay_fatctor)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(available_size <= 0 || duration_us <= 0 || chunk_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(available_size), K(duration_us), K(chunk_size));
  } else {
    double N = static_cast<double>(available_size) / static_cast<double>(chunk_size);
    decay_fatctor = static_cast<double>(duration_us) / static_cast<double>((pow(N, 2) * pow(N + 1, 2)) / 4);
  }
  return ret;
}

int ObThrottlingUtils::get_throttling_interval(const int64_t chunk_size,
                                               const int64_t request_size,
                                               const int64_t trigger_limit,
                                               const int64_t cur_hold,
                                               const double decay_factor,
                                               int64_t &interval_us)
{
  int ret = OB_SUCCESS;
  interval_us = 0;
  if (OB_UNLIKELY(chunk_size <= 0 || request_size <= 0 || trigger_limit <= 0
                  || cur_hold <= 0 || decay_factor <= 0.0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(chunk_size), K(request_size), K(trigger_limit), K(cur_hold), K(decay_factor));
  } else {
    double ret_interval = 0.0;
    const int64_t cur_overused_size = cur_hold - trigger_limit;

    const int64_t upper_complete_boundary = (cur_overused_size + request_size) / chunk_size * chunk_size;
    const int64_t lower_complete_boundary = (cur_overused_size + chunk_size - 1) / chunk_size * chunk_size;

    const int64_t complete_chunk_cnt = MAX(0, ((upper_complete_boundary - lower_complete_boundary) / chunk_size));


    const int64_t first_chunk_part_size = MIN(request_size, lower_complete_boundary - (cur_overused_size));
    int64_t base_chunk_seq = (cur_overused_size + chunk_size - 1) / chunk_size + 1;
    if (first_chunk_part_size > 0) {
      ret_interval += decay_factor * pow(base_chunk_seq, 3) * first_chunk_part_size / chunk_size; // add first part interval
    }

    int64_t cur_chunk_seq = upper_complete_boundary / chunk_size + 1;

    for (int64_t i = 1; i <= complete_chunk_cnt; ++i, cur_chunk_seq ++) {
      ret_interval += decay_factor * pow(cur_chunk_seq, 3);
    }

    const int64_t last_chunk_part_size = lower_complete_boundary > upper_complete_boundary ? 0 : (cur_overused_size + request_size - upper_complete_boundary);

    if (last_chunk_part_size > 0) {
      ret_interval += decay_factor * pow(cur_chunk_seq, 3) * last_chunk_part_size / chunk_size;
    }
    interval_us = MAX(0, static_cast<int64_t>(ret_interval));
  }
  return ret;
}

} // end of namespace share
}//end of namespace oceanbase
