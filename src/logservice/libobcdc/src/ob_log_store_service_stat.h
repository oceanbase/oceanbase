/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
