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

#ifndef OB_TEST_OB_TEST_MOCK_ELECTION_MGR_H_
#define OB_TEST_OB_TEST_MOCK_ELECTION_MGR_H_

#include "election/ob_election_mgr.h"

namespace oceanbase {
namespace unittest {
class MockObElectionMgr : public election::ObElectionMgr {
public:
  int set_election_rpc(election::ObIElectionRpc* election_rpc)
  {
    int ret = common::OB_SUCCESS;

    if (NULL == election_rpc) {
      ret = common::OB_ERR_NULL_VALUE;
      ELECT_LOG(WARN, "set election rpc NULL", K(ret));
    } else {
      rpc_ = election_rpc;
    }

    return ret;
  }
  const common::ObAddr& get_addr() const
  {
    return addr_;
  }
  int64_t get_idx() const
  {
    return idx_;
  }

public:
  common::ObAddr addr_;
  int64_t idx_;
};

}  // namespace unittest
}  // namespace oceanbase
#endif
