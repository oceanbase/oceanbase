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
 * KV store stat
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_STORE_SERVICE_STAT_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_STORE_SERVICE_STAT_H_

#include "lib/utility/ob_print_utils.h"         // TO_STRING_KV

namespace oceanbase
{
namespace libobcdc
{
struct StoreServiceStatInfo
{
  int64_t total_data_size_ CACHE_ALIGNED;
  int64_t last_total_data_size_ CACHE_ALIGNED;

  StoreServiceStatInfo() { reset(); }
  ~StoreServiceStatInfo() { reset(); }

  void reset();

  void do_data_stat(int64_t record_len);

  double calc_rate(const int64_t delta_time);

  double get_total_data_size() const;

  TO_STRING_KV(K_(total_data_size),
               K_(last_total_data_size));
};

}
}

#endif
