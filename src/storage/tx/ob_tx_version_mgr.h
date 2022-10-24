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

#ifndef OCEANBASE_TRANSACTION_OB_TX_VERSION_MGR_
#define OCEANBASE_TRANSACTION_OB_TX_VERSION_MGR_

#include <stdint.h>
#include "lib/atomic/ob_atomic.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace transaction
{

class ObTxVersionMgr
{
public:
  ObTxVersionMgr() : max_commit_ts_(100), max_elr_commit_ts_(100), max_read_ts_(100) {}
  ~ObTxVersionMgr() {}
public:
  void update_max_commit_ts(const int64_t ts, const bool elr)
  {
    if (!elr) {
      (void)inc_update(&max_commit_ts_, ts);
    } else {
      (void)inc_update(&max_elr_commit_ts_, ts);
    }
    TRANS_LOG(TRACE, "update max commit ts", K(ts), K(elr));
  }
  void update_max_read_ts(const int64_t ts)
  {
    (void)inc_update(&max_read_ts_, ts);
    TRANS_LOG(TRACE, "update max read ts", K(ts));
  }
  int64_t get_max_commit_ts(const bool elr) const
  {
    int64_t max_commit_ts = ATOMIC_LOAD(&max_commit_ts_);
    if (elr) {
      const int64_t max_elr_commit_ts = ATOMIC_LOAD(&max_elr_commit_ts_);
      max_commit_ts = max(max_commit_ts, max_elr_commit_ts);
    }
    TRANS_LOG(TRACE, "get max commit ts", K(max_commit_ts), K(elr));
    return max_commit_ts;
  }
  int64_t get_max_read_ts() const
  {
    const int64_t max_read_ts = ATOMIC_LOAD(&max_read_ts_) + 1;
    TRANS_LOG(TRACE, "get max read ts", K(max_read_ts));
    return max_read_ts;
  }
private:
  int64_t max_commit_ts_ CACHE_ALIGNED;
  int64_t max_elr_commit_ts_ CACHE_ALIGNED;
  int64_t max_read_ts_ CACHE_ALIGNED;
};

}
}//end of namespace oceanbase

#endif //OCEANBASE_TRANSACTION_OB_TX_VERSION_MGR_
