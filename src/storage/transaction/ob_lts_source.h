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

#ifndef OCEANBASE_TRANSACTION_OB_LTS_SOURCE_
#define OCEANBASE_TRANSACTION_OB_LTS_SOURCE_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "ob_i_ts_source.h"

namespace oceanbase {
namespace transaction {
class ObLtsSource : public ObITsSource {
public:
  ObLtsSource()
  {}
  ~ObLtsSource()
  {}

public:
  int update_gts(const int64_t gts, bool& update);
  int update_local_trans_version(const int64_t version, bool& update);
  int get_gts(const MonotonicTs stc, ObTsCbTask* task, int64_t& gts, MonotonicTs& receive_gts_ts);
  int get_gts(ObTsCbTask* task, int64_t& gts);
  int get_local_trans_version(const MonotonicTs stc, ObTsCbTask* task, int64_t& version, MonotonicTs& receive_gts_ts);
  int get_local_trans_version(ObTsCbTask* task, int64_t& version);
  int wait_gts_elapse(const int64_t gts, ObTsCbTask* task, bool& need_wait);
  int wait_gts_elapse(const int64_t gts);
  int refresh_gts(const bool need_refresh);
  int update_base_ts(const int64_t base_ts, const int64_t publish_version);
  int get_base_ts(int64_t& base_ts, int64_t& publish_version);
  bool is_external_consistent()
  {
    return false;
  }
  int update_publish_version(const int64_t publish_version);
  int get_publish_version(int64_t& publish_version);

public:
  TO_STRING_KV("ts_source", "LTS");
};
}  // namespace transaction
}  // end of namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_LTS_SOURCE_
