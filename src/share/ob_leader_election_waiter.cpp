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

#define USING_LOG_PREFIX SHARE
#include "share/ob_leader_election_waiter.h"

#include "share/partition_table/ob_partition_table_operator.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_part_mgr_util.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace share;
namespace share {
ObLeaderElectionWaiter::ObLeaderElectionWaiter(ObPartitionTableOperator& pt_operator, volatile bool& stop)
    : stop_(stop), allocator_(ObModIds::OB_RS_PARTITION_TABLE_TEMP), pt_operator_(pt_operator)
{}

ObLeaderElectionWaiter::~ObLeaderElectionWaiter()
{}

#define LOG_WAIT_RESULT(start_time, ...) \
  LOG_INFO("wait leader elect finish", K(ret), "wait_time", ObTimeUtility::current_time() - start_time, ##__VA_ARGS__)
int ObLeaderElectionWaiter::wait(const uint64_t table_id, const int64_t partition_id, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t abs_timeout = start_time + timeout;
  ObAddr leader;
  if (!ObIPartitionTable::is_valid_key(table_id, partition_id) || timeout <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(timeout));
  } else if (OB_FAIL(wait(table_id, partition_id, abs_timeout, leader))) {
    LOG_WARN("wait failed", KT(table_id), K(partition_id), K(abs_timeout), K(ret));
  }
  LOG_WAIT_RESULT(start_time, KT(table_id), K(partition_id), K(timeout));
  return ret;
}

int ObLeaderElectionWaiter::wait(
    const uint64_t table_id, const int64_t partition_id, const int64_t timeout, ObAddr& leader)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t abs_timeout = start_time + timeout;
  if (!ObIPartitionTable::is_valid_key(table_id, partition_id) || timeout <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(timeout));
  } else if (OB_FAIL(wait_elect_leader(table_id, partition_id, CHECK_LEADER_ELECT_INTERVAL_US, abs_timeout, leader))) {
    LOG_WARN("wait_elect_leader failed", KT(table_id), K(partition_id), K(abs_timeout), K(ret));
  }
  LOG_WAIT_RESULT(start_time, KT(table_id), K(partition_id), K(timeout), K(leader));
  return ret;
}

int ObLeaderElectionWaiter::wait(const ObTablegroupSchema& tablegroup_schema, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t abs_timeout = start_time + timeout;
  const uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  if (!tablegroup_schema.get_binding()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), "tablegroup_id", tablegroup_schema.get_binding());
  } else if (timeout <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(timeout));
  } else {
    ObAddr server;
    bool check_dropped_schema = false;
    ObTablegroupPartitionKeyIter iter(tablegroup_schema, check_dropped_schema);
    int64_t partition_id = -1;
    while (OB_SUCC(ret) && OB_SUCC(iter.next_partition_id_v2(partition_id))) {
      server.reset();
      if (OB_FAIL(
              wait_elect_leader(tablegroup_id, partition_id, CHECK_LEADER_ELECT_INTERVAL_US, abs_timeout, server))) {
        LOG_WARN("fail to wait elect leader", K(tablegroup_id), K(partition_id), K(abs_timeout), K(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  LOG_WAIT_RESULT(start_time, K(tablegroup_schema), K(timeout));
  return ret;
}

int ObLeaderElectionWaiter::wait(const ObTableSchema& table_schema, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t abs_timeout = start_time + timeout;
  const uint64_t table_id = table_schema.get_table_id();
  if (!table_schema.is_valid() || timeout <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_schema), K(timeout));
  }
  ObAddr server;
  bool check_dropped_schema = false;
  ObTablePartitionKeyIter iter(table_schema, check_dropped_schema);
  int64_t partition_id = -1;
  while (OB_SUCC(ret) && OB_SUCC(iter.next_partition_id_v2(partition_id))) {
    server.reset();
    if (OB_FAIL(wait_elect_leader(table_id, partition_id, CHECK_LEADER_ELECT_INTERVAL_US, abs_timeout, server))) {
      LOG_WARN("wait_elect_leader failed", KT(table_id), K(partition_id), K(abs_timeout), K(ret));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  LOG_WAIT_RESULT(start_time, K(table_schema), K(timeout));
  return ret;
}

int ObLeaderElectionWaiter::wait(
    const ObArray<uint64_t>& table_ids, const ObArray<int64_t>& part_nums, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t abs_timeout = start_time + timeout;
  if (table_ids.empty() || table_ids.count() != part_nums.count() || timeout < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(ret),
        "table id count",
        table_ids.count(),
        "part number count",
        part_nums.count(),
        K(timeout));
  }
  ObAddr server;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < part_nums[i]; ++j) {
      server.reset();
      if (OB_FAIL(wait_elect_leader(table_ids[i], j, CHECK_LEADER_ELECT_INTERVAL_US, abs_timeout, server))) {
        LOG_WARN("wait_elect_leader failed", "table_id", table_ids[i], "partition_id", j, K(abs_timeout), K(ret));
      }
    }
  }
  LOG_WAIT_RESULT(start_time, K(part_nums), K(timeout));
  return ret;
}

int ObLeaderElectionWaiter::wait(
    const ObArray<uint64_t>& table_ids, const int64_t partition_id, const ObAddr& leader, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t abs_timeout = start_time + timeout;
  ObAddr expected_leader = leader;
  if (table_ids.empty() || partition_id < 0 || !leader.is_valid() || timeout <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "table id count", table_ids.count(), K(partition_id), K(leader), K(timeout));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
    if (OB_FAIL(wait_elect_leader(
            table_ids.at(i), partition_id, CHECK_LEADER_CHANGE_INTERVAL_US, abs_timeout, expected_leader))) {
      LOG_WARN("wait_elect_leader failed",
          "table_id",
          table_ids.at(i),
          K(partition_id),
          K(abs_timeout),
          K(expected_leader),
          K(ret));
    }
  }
  LOG_WAIT_RESULT(start_time, K(leader), K(timeout));
  return ret;
}

int ObLeaderElectionWaiter::wait(common::ObIArray<ExpectedLeader>& expected_leaders, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t abs_timeout = start_time + timeout;
  if (expected_leaders.empty() || timeout <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "expected leader count", expected_leaders.count(), K(timeout));
  }
  FOREACH_CNT_X(expected_leader, expected_leaders, OB_SUCCESS == ret)
  {
    if (!expected_leader->is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", "expected_leader", *expected_leader, K(ret));
    } else if (OB_FAIL(wait_elect_leader(expected_leader->partition_.get_table_id(),
                   expected_leader->partition_.get_partition_id(),
                   CHECK_LEADER_CHANGE_INTERVAL_US,
                   abs_timeout,
                   expected_leader->exp_leader_))) {
      LOG_WARN("wait_elect_leader failed", "expected_leader", *expected_leader, K(abs_timeout), K(ret));
    } else {
      expected_leader->new_leader_ = expected_leader->exp_leader_;
    }
  }
  LOG_WAIT_RESULT(start_time, K(timeout));
  return ret;
}

int ObLeaderElectionWaiter::wait_elect_leader(const uint64_t table_id, const int64_t partition_id,
    const int64_t check_interval, const int64_t abs_timeout, ObAddr& leader)
{
  int ret = OB_SUCCESS;
  ObPartitionInfo partition_info;
  partition_info.set_allocator(&allocator_);
  const ObPartitionReplica* leader_replica = nullptr;
  if (!ObIPartitionTable::is_valid_key(table_id, partition_id) || check_interval <= 0 || abs_timeout <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(check_interval), K(abs_timeout));
  } else {
    int64_t sleep_interval = std::max(1l, check_interval / 100);
    while (!stop_) {
      if (OB_FAIL(pt_operator_.get(table_id, partition_id, partition_info))) {
        LOG_WARN("get partition info failed", KT(table_id), K(partition_id), K(ret));
      } else if (OB_FAIL(partition_info.find_leader_by_election(leader_replica))) {
        // failure is normal, since leader may have not taked over
      } else if (nullptr == leader_replica) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL leader", K(ret));
      } else if (!leader.is_valid()) {
        leader = leader_replica->server_;
        break;
      } else if (leader == leader_replica->server_) {
        break;
      }
      if (OB_SUCCESS != ret || nullptr == leader_replica || leader != leader_replica->server_) {
        const int64_t now = ObTimeUtility::current_time();
        if (now < abs_timeout) {
          if (OB_FAIL(check_sleep(std::min(sleep_interval, abs_timeout - now)))) {
            LOG_WARN("check sleep failed", K(ret));
            break;
          }
        } else {
          ret = OB_WAIT_ELEC_LEADER_TIMEOUT;
          LOG_WARN("wait elect sys leader timeout", K(ret), K(abs_timeout), K(table_id), K(partition_id));
          break;
        }
      }
      sleep_interval = std::min(sleep_interval * 2, check_interval);
    }
    if (stop_ && OB_SUCC(ret)) {
      ret = OB_CANCELED;
      LOG_WARN("stop flag set, cancel task", K(ret));
    }
  }
  return ret;
}

int ObLeaderElectionWaiter::check_sleep(const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  int64_t escaped = 0;
  const static int64_t max_step = 10 * 1000;  // 10ms;
  while (!stop_ && escaped < interval_us) {
    const int32_t step = static_cast<int32_t>(std::min(max_step, interval_us - escaped));
    usleep(step);
    escaped += step;
  }
  if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("stop flag set, cancel task", K(ret));
  }
  return ret;
}

int ObUserPartitionLeaderWaiter::check_cancel()
{
  int ret = OB_SUCCESS;
  if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("stop flag is set, cancel task", K(ret));
  }
  return ret;
}

int ObUserPartitionLeaderWaiter::build_pkey_expected_leader_map(
    common::ObIArray<ExpectedLeader>& expected_leaders, ExpectedLeaderMap& expected_leader_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expected_leaders.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expected_leaders.count(); ++i) {
      const common::ObPartitionKey& pkey = expected_leaders.at(i).partition_;
      ExpectedLeader& expected_leader = expected_leaders.at(i);
      const int32_t overwrite = 0;
      if (OB_FAIL(expected_leader_map.set_refactored(pkey, &expected_leader, overwrite))) {
        LOG_WARN("fail to set map", K(ret), K(pkey));
      }
    }
  }
  return ret;
}

int ObUserPartitionLeaderWaiter::check_change_leader_result(const int64_t start, const int64_t end,
    common::ObIArray<ExpectedLeader>& expected_leaders, ExpectedLeaderMap& expected_leader_map, const int64_t tenant_id,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (start >= end || start >= expected_leaders.count() || end > expected_leaders.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start), K(end), "count", expected_leaders.count());
  } else {
    common::ObSqlString sql_string;
    bool finish = false;
    const int64_t abs_timeout = ObTimeUtility::current_time() + timeout;
    int64_t sleep_interval = CHECK_LEADER_CHANGE_INTERVAL_BASE_US;
    while (OB_SUCC(ret) && OB_SUCC(check_cancel()) && !finish) {
      sql_string.reset();
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        sqlclient::ObMySQLResult* result = NULL;
        int64_t true_batch_cnt = 0;
        if (OB_FAIL(append_batch_sql_fmt(sql_string, true_batch_cnt, start, end, expected_leaders, tenant_id))) {
          LOG_WARN("fail to append sub batch sql fmt", K(ret));
        } else if (OB_FAIL(mysql_proxy_.read(res, tenant_id, sql_string.ptr()))) {
          LOG_WARN("fail to execute sql", K(ret));
        } else if (OB_UNLIKELY(NULL == (result = res.get_result()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get mysql result failed", K(ret));
        } else {
          ret = do_check_change_leader_result(*result, expected_leader_map, true_batch_cnt);
          if (OB_SUCC(ret)) {
            finish = true;
          } else if (OB_EAGAIN == ret) {
            ret = OB_SUCCESS;
            const int64_t now = ObTimeUtility::current_time();
            if (now < abs_timeout) {
              interval_sleep(std::min(sleep_interval, abs_timeout - now));
              int64_t max_sleep_interval = CHECK_LEADER_CHANGE_INTERVAL_MAX_US;
              sleep_interval = std::min(2 * sleep_interval, max_sleep_interval);
            } else {
              ret = OB_WAIT_ELEC_LEADER_TIMEOUT;
              LOG_WARN("wait elect sys leader timeout", K(ret), K(abs_timeout));
            }
          } else {
            // this error is thrown to the upper layer
          }
        }
      }
    }
  }
  return ret;
}

int ObUserPartitionLeaderWaiter::do_check_change_leader_result(
    common::sqlclient::ObMySQLResult& result, ExpectedLeaderMap& expected_leader_map, const int64_t true_batch_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(true_batch_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(true_batch_cnt));
  } else {
    common::ObString svr_ip;
    common::ObAddr cur_leader;
    common::ObPartitionKey pkey;
    int64_t finish_cnt = 0;
    int64_t not_finish_cnt = 0;
    while (OB_SUCC(ret) && OB_SUCC(result.next()) && OB_SUCC(check_cancel())) {
      svr_ip.reset();
      cur_leader.reset();
      pkey.reset();
      int64_t table_id = OB_INVALID_ID;
      int64_t partition_id = OB_INVALID_INDEX;
      int64_t svr_port = 0;
      int64_t index = 0;
      const int64_t part_cnt = 0;  // hash do not care part_cnt
      GET_COL_IGNORE_NULL(result.get_int, index++, table_id);
      GET_COL_IGNORE_NULL(result.get_int, index++, partition_id);
      GET_COL_IGNORE_NULL(result.get_varchar, index++, svr_ip);
      GET_COL_IGNORE_NULL(result.get_int, index++, svr_port);
      ExpectedLeader* expected_leader = NULL;
      if (OB_FAIL(ret)) {
        // GET_COL_IGNORE_NULL may fail
      } else if (!cur_leader.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to set ip addr", K(ret));
      } else if (OB_FAIL(pkey.init(static_cast<uint64_t>(table_id), partition_id, part_cnt))) {
        LOG_WARN("fail to init pkey", K(ret));
      } else if (OB_FAIL(expected_leader_map.get_refactored(pkey, expected_leader))) {
        LOG_WARN("fail to get expected leader item", K(ret));
      } else if (OB_UNLIKELY(NULL == expected_leader)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expected leader", K(ret));
      } else if (cur_leader == expected_leader->exp_leader_) {
        expected_leader->new_leader_ = cur_leader;
        ++finish_cnt;
      } else {
        ++not_finish_cnt;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret) && (finish_cnt < true_batch_cnt || not_finish_cnt > 0)) {
      // the result read from partition table is less than the expected number, not finished
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

void ObUserPartitionLeaderWaiter::interval_sleep(const int64_t sleep_interval)
{
  int64_t escaped = 0;
  const static int64_t max_step = 10 * 1000;  // 10ms
  while (escaped < sleep_interval) {
    const int32_t step = static_cast<int32_t>(std::min(max_step, sleep_interval - escaped));
    usleep(step);
    escaped += step;
  }
  return;
}

int ObUserPartitionLeaderWaiter::append_batch_sql_fmt(common::ObSqlString& sql_string, int64_t& true_batch_cnt,
    const int64_t start, const int64_t end, const common::ObIArray<ExpectedLeader>& expected_leaders,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (start >= end || start >= expected_leaders.count() || end > expected_leaders.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start), K(end), "count", expected_leaders.count());
  } else {
    sql_string.reset();
    if (OB_FAIL(sql_string.append_fmt("SELECT table_id, partition_id, svr_ip, svr_port"
                                      " FROM %s WHERE tenant_id=%lu AND (role = %d or role = %d) AND"
                                      " (table_id, partition_id) IN (",
            OB_ALL_TENANT_META_TABLE_TNAME,
            tenant_id,
            common::LEADER,
            common::STANDBY_LEADER))) {
      LOG_WARN("fail to append format", K(ret));
    } else {
      true_batch_cnt = 0;
      bool first = true;
      for (int64_t index = start; OB_SUCC(ret) && index < end; ++index) {
        const ExpectedLeader& expected_leader = expected_leaders.at(index);
        const ObPartitionKey& pkey = expected_leaders.at(index).partition_;
        if (pkey.get_tenant_id() != tenant_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant_id not match", K(ret), K(tenant_id), "tenant_id_in_pkey", pkey.get_tenant_id());
        } else if (pkey.get_tenant_id() == common::OB_SYS_TENANT_ID) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sys tenant", K(ret), "tenant_id_in_key", pkey.get_tenant_id());
        } else if (is_sys_table(pkey.get_table_id())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sys table", K(pkey));
        } else if (expected_leader.exp_leader_ == expected_leader.new_leader_) {
          // bypass, this has check-in before
        } else if (OB_FAIL(sql_string.append_fmt(
                       "%s(%lu, %ld)", (first ? "" : ", "), pkey.get_table_id(), pkey.get_partition_id()))) {
          LOG_WARN("fail to append fmt", K(ret));
        } else {
          first = false;
          ++true_batch_cnt;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql_string.append_fmt(")"))) {
          LOG_WARN("fail to append sql", K(ret));
        }
      }
    }
  }
  return ret;
}

// leader wait interface for user partition, invoked by leader coordinator
int ObUserPartitionLeaderWaiter::tenant_user_partition_wait(
    const uint64_t tenant_id, common::ObIArray<ExpectedLeader>& expected_leaders, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  ExpectedLeaderMap expected_leader_map;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || common::OB_SYS_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(expected_leaders.count() <= 0 || timeout <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(timeout), "expected leader count", expected_leaders.count());
  } else if (OB_FAIL(expected_leader_map.create(expected_leaders.count() * 2, ObModIds::OB_RS_PARTITION_TABLE_TEMP))) {
    LOG_WARN("fail to create leader info map", K(ret));
  } else if (OB_FAIL(build_pkey_expected_leader_map(expected_leaders, expected_leader_map))) {
    LOG_WARN("fail to build pkey expected leader map", K(ret));
  } else {
    for (int64_t start = 0; OB_SUCC(ret) && OB_SUCC(check_cancel()) && start < expected_leaders.count(); /*nop*/) {
      const int64_t BATCH_CNT = 512;
      const int64_t bak_start = start;
      const int64_t bak_end =
          (BATCH_CNT + bak_start > expected_leaders.count()) ? expected_leaders.count() : BATCH_CNT + bak_start;
      start = bak_end;
      if (OB_FAIL(check_change_leader_result(
              bak_start, bak_end, expected_leaders, expected_leader_map, tenant_id, timeout))) {
        LOG_WARN("fail to check change leader result", K(ret));
      }
    }
  }
  LOG_WAIT_RESULT(start_time, K(tenant_id), K(timeout));
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
