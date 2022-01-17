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

#ifndef OB_UNITTEST_MOCK_ELECTION_MGR_H_
#define OB_UNITTEST_MOCK_ELECTION_MGR_H_

#include "election/ob_election_mgr.h"

namespace oceanbase {
namespace unittest {

class MockElectionMgr : public election::ObElectionMgr {
public:
  int set_election_rpc(election::ObIElectionRpc* election_rpc)
  {
    int ret = common::OB_SUCCESS;

    if (NULL == election_rpc) {
      ret = common::OB_INVALID_ARGUMENT;
      ELECT_LOG(WARN, "invalid argument", K(ret));
    } else {
      rpc_ = election_rpc;
    }

    return ret;
  }
  bool is_partition_exist(const common::ObPartitionKey& partition)
  {
    bool bool_ret = false;
    int ret = OB_SUCCESS;
    election::ObIElection* election = NULL;
    if (!partition.is_valid()) {
    } else if (OB_SUCCESS == (ret = get_election(partition, election)) && NULL != election) {
      bool_ret = true;
    }
    return bool_ret;
  }
};

}  // namespace unittest
}  // namespace oceanbase
#endif
