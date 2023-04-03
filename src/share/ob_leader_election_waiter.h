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

#ifndef OCEANBASE_SHARE_OB_LEADER_ELECTION_WAITER_H_
#define OCEANBASE_SHARE_OB_LEADER_ELECTION_WAITER_H_

#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/net/ob_addr.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
namespace schema
{
class ObSimpleTableSchemaV2;
class ObTablegroupSchema;
}

class ObLSTableOperator;
class ObLSLeaderElectionWaiter
{
public:
  struct ExpectedLeader
  {
    uint64_t tenant_id_;
    share::ObLSID ls_id_;
    common::ObAddr exp_leader_;
    common::ObAddr new_leader_;

    inline void reset();
    bool is_valid() const
    {
      return OB_INVALID_ID != tenant_id_
             && ls_id_.is_valid()
             && exp_leader_.is_valid();
    }
    int init(const uint64_t tenant_id,
        const share::ObLSID &ls_id,
        const common::ObAddr &expect_leader);
    int assgin(const ExpectedLeader &other);

    TO_STRING_KV(K_(tenant_id),
                 K_(ls_id),
                 K_(exp_leader),
                 K_(new_leader));
  };

  ObLSLeaderElectionWaiter(
      share::ObLSTableOperator &pt_operator,
      volatile bool &stop);
  virtual ~ObLSLeaderElectionWaiter();

  int wait(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int64_t timeout);

  int wait(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int64_t timeout,
      common::ObAddr &leader);
  
  int wait(
      common::ObIArray<ExpectedLeader> &expected_leaders,
      const int64_t timeout);
  share::ObLSTableOperator &get_lst_operator() { return lst_operator_; }
private:
  static const int64_t CHECK_LEADER_ELECT_INTERVAL_US = 500 * 1000L; //500ms
  static const int64_t CHECK_LEADER_CHANGE_INTERVAL_US = 3000 * 1000L; //3s

  int wait_elect_leader(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int64_t check_interval,
      const int64_t abs_timeout,
      common::ObAddr &leader);
  
  int check_sleep(
      const int64_t interval_us);

private:
  volatile bool &stop_;
  common::ObArenaAllocator allocator_;
  share::ObLSTableOperator &lst_operator_;

  DISALLOW_COPY_AND_ASSIGN(ObLSLeaderElectionWaiter);
};

}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_LEADER_ELECTION_WAITER_H_
