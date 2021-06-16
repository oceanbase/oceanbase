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
#include "common/ob_partition_key.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
namespace sqlclient {
class ObMySQLResult;
}
}  // namespace common
namespace share {
namespace schema {
class ObTableSchema;
class ObTablegroupSchema;
}  // namespace schema
class ObPartitionTableOperator;

class ObLeaderElectionWaiter {
public:
  struct ExpectedLeader {
    common::ObPartitionKey partition_;
    common::ObAddr exp_leader_;
    common::ObAddr old_leader_;
    common::ObAddr new_leader_;

    inline void reset();
    bool is_valid() const
    {
      return partition_.is_valid() && exp_leader_.is_valid() && old_leader_.is_valid();
    }

    TO_STRING_KV(K_(partition), K_(exp_leader), K_(old_leader), K_(new_leader));
  };

  ObLeaderElectionWaiter(share::ObPartitionTableOperator& pt_operator, volatile bool& stop);
  virtual ~ObLeaderElectionWaiter();

  int wait(const uint64_t table_id, const int64_t partition_id, const int64_t timeout);
  int wait(const uint64_t table_id, const int64_t partition_id, const int64_t timeout, common::ObAddr& leader);
  // when observer finish sending create table rpc to rs,
  // invoke following function to wait the new table's leader elected
  int wait(const share::schema::ObTableSchema& table_schema, const int64_t timeout);
  int wait(const share::schema::ObTablegroupSchema& tablegroup_schema, const int64_t timeout);
  int wait(
      const common::ObArray<uint64_t>& table_ids, const common::ObArray<int64_t>& part_nums, const int64_t timeout);
  // wait all partition's leader change
  int wait(const common::ObArray<uint64_t>& table_ids, const int64_t partition_id, const common::ObAddr& leader,
      const int64_t timeout);
  int wait(common::ObIArray<ExpectedLeader>& expected_leaders, const int64_t timeout);
  share::ObPartitionTableOperator& get_pt_operator()
  {
    return pt_operator_;
  }

private:
  static const int64_t CHECK_LEADER_ELECT_INTERVAL_US = 500 * 1000L;    // 500ms
  static const int64_t CHECK_LEADER_CHANGE_INTERVAL_US = 3000 * 1000L;  // 3s

  int wait_elect_leader(const uint64_t table_id, const int64_t partition_id, const int64_t check_interval,
      const int64_t abs_timeout, common::ObAddr& leader);
  int check_sleep(const int64_t interval_us);

private:
  volatile bool& stop_;
  common::ObArenaAllocator allocator_;
  share::ObPartitionTableOperator& pt_operator_;

  DISALLOW_COPY_AND_ASSIGN(ObLeaderElectionWaiter);
};

inline void ObLeaderElectionWaiter::ExpectedLeader::reset()
{
  partition_.reset();
  exp_leader_.reset();
  old_leader_.reset();
  new_leader_.reset();
}

class ObUserPartitionLeaderWaiter {
public:
  typedef ObLeaderElectionWaiter::ExpectedLeader ExpectedLeader;

public:
  ObUserPartitionLeaderWaiter(common::ObMySQLProxy& mysql_proxy, volatile bool& stop)
      : mysql_proxy_(mysql_proxy), stop_(stop)
  {}
  virtual ~ObUserPartitionLeaderWaiter()
  {}

public:
  // leader wait interface for user partition, invoked by leader coordinator
  int tenant_user_partition_wait(
      const uint64_t tenant_id, common::ObIArray<ExpectedLeader>& expected_leaders, const int64_t timeout);

private:
  static const int64_t CHECK_LEADER_CHANGE_INTERVAL_BASE_US = 100 * 1000;  // 100ms
  static const int64_t CHECK_LEADER_CHANGE_INTERVAL_MAX_US = 4 * 1000000;  // 4s
  typedef common::hash::ObHashMap<common::ObPartitionKey, ExpectedLeader*, common::hash::NoPthreadDefendMode>
      ExpectedLeaderMap;
  int build_pkey_expected_leader_map(
      common::ObIArray<ExpectedLeader>& expected_leaders, ExpectedLeaderMap& expected_leader_map);
  int append_batch_sql_fmt(common::ObSqlString& sql_string, int64_t& true_batch_cnt, const int64_t start,
      const int64_t end, const common::ObIArray<ExpectedLeader>& expected_leaders, const uint64_t tenant_id);
  int check_change_leader_result(const int64_t start, const int64_t end,
      common::ObIArray<ExpectedLeader>& expected_leaders, ExpectedLeaderMap& expected_leader_map,
      const int64_t tenant_id, const int64_t timeout);
  int do_check_change_leader_result(
      common::sqlclient::ObMySQLResult& result, ExpectedLeaderMap& expected_leader_map, const int64_t true_batch_cnt);
  void interval_sleep(const int64_t sleep_interval);
  int check_cancel();

private:
  common::ObMySQLProxy& mysql_proxy_;
  volatile bool& stop_;
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_LEADER_ELECTION_WAITER_H_
