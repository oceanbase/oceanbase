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
  ObTxVersionMgr()
  {
    max_commit_ts_.set_base();
    max_elr_commit_ts_.set_base();
    max_read_ts_.set_base();
  }
  ~ObTxVersionMgr() {}
public:
  void update_max_commit_ts(const palf::SCN &ts, const bool elr)
  {
    if (!elr) {
      max_commit_ts_.inc_update(ts);
    } else {
      max_elr_commit_ts_.inc_update(ts);
    }
    TRANS_LOG(TRACE, "update max commit ts", K(ts), K(elr));
  }
  void update_max_read_ts(const palf::SCN &ts)
  {
    max_read_ts_.inc_update(ts);
    TRANS_LOG(TRACE, "update max read ts", K(ts));
  }
  palf::SCN get_max_commit_ts(const bool elr) const
  {
    palf::SCN max_commit_ts = max_commit_ts_.atomic_get();
    if (elr) {
      const palf::SCN max_elr_commit_ts = max_elr_commit_ts_.atomic_get();
      max_commit_ts = palf::SCN::max(max_commit_ts, max_elr_commit_ts);
    }
    TRANS_LOG(TRACE, "get max commit ts", K(max_commit_ts), K(elr));
    return max_commit_ts;
  }
  palf::SCN get_max_read_ts() const
  {
    const palf::SCN max_read_ts = palf::SCN::scn_inc(max_read_ts_);
    TRANS_LOG(TRACE, "get max read ts", K(max_read_ts));
    return max_read_ts;
  }
private:
  palf::SCN max_commit_ts_ CACHE_ALIGNED;
  palf::SCN max_elr_commit_ts_ CACHE_ALIGNED;
  palf::SCN max_read_ts_ CACHE_ALIGNED;
};

}
}//end of namespace oceanbase

#endif //OCEANBASE_TRANSACTION_OB_TX_VERSION_MGR_
