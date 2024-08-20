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
 * OBCDC malloc sample info
 */

#define USING_LOG_PREFIX OBLOG

#include <algorithm>

#include "ob_cdc_malloc_sample_info.h"
#include "ob_log_utils.h"

namespace oceanbase
{
namespace libobcdc
{

int ObCDCMallocSampleInfo::init(const ObMallocSampleMap &sample_map)
{
  int ret = OB_SUCCESS;
  lib::ObMallocSampleMap::const_iterator it = sample_map.begin();

  for (; OB_SUCC(ret) && it != sample_map.end(); ++it) {
    ObCDCMallocSamplePair pair(it->first, it->second);
    if (OB_FAIL(samples_.push_back(pair))) {
      LOG_ERROR("push_back malloc sample into array failed", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    lib::ob_sort(samples_.begin(), samples_.end(), ObCDCMallocSamplePairCompartor());
  }

  return ret;
}

void ObCDCMallocSampleInfo::print_topk(int64_t k)
{
  if (OB_LIKELY(k > 0)) {
    const int64_t sample_cnt = samples_.count();

    for (int i = 0; i < std::min(sample_cnt, k); i++) {
      char bt_str[lib::MAX_BACKTRACE_LENGTH];
      ObCDCMallocSamplePair &pair = samples_.at(i);
      IGNORE_RETURN parray(bt_str, sizeof(bt_str), (int64_t*)pair.key_.bt_, lib::AOBJECT_BACKTRACE_COUNT);

      _LOG_INFO("[DUMP_MALLOC_SAMPLE][TOP %d/%ld][tenant_id: %ld][label: %s][ctx_id: %ld][alloc_size/alloc_cnt: %s/%ld][bt: %s]",
          i, k, pair.key_.tenant_id_, pair.key_.label_, pair.key_.ctx_id_, SIZE_TO_STR(pair.value_.alloc_bytes_), pair.value_.alloc_count_, bt_str);
    }
  }
}

void ObCDCMallocSampleInfo::print_with_filter(const char *label_str, const int64_t alloc_size)
{
  if (OB_UNLIKELY(alloc_size > 0) && OB_NOT_NULL(label_str)) {
    const int64_t sample_cnt = samples_.count();
    LOG_INFO("print_with_filter", KCSTRING(label_str), K(alloc_size));

    for (int i = 0; i < sample_cnt; i++) {
      const ObCDCMallocSamplePair &pair = samples_.at(i);

      if (pair.value_.alloc_bytes_ > alloc_size && ObLabel(pair.key_.label_) == ObLabel(label_str)) {
        char bt_str[lib::MAX_BACKTRACE_LENGTH];
        IGNORE_RETURN parray(bt_str, sizeof(bt_str), (int64_t*)pair.key_.bt_, lib::AOBJECT_BACKTRACE_COUNT);

        _LOG_INFO("[DUMP_MALLOC_SAMPLE][FILTER][tenant_id: %ld][label: %s][ctx_id: %ld][alloc_size/alloc_cnt: %s/%ld][bt: %s]",
            pair.key_.tenant_id_, pair.key_.label_, pair.key_.ctx_id_, SIZE_TO_STR(pair.value_.alloc_bytes_), pair.value_.alloc_count_, bt_str);
      }
    }
  }
}

} // end of namespace libobcdc
} // end of namespace oceanbase
