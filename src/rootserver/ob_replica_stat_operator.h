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

#ifndef _OB_REPLICA_STAT_OPERATOR_H
#define _OB_REPLICA_STAT_OPERATOR_H 1
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/ob_partition_key.h"
#include "lib/net/ob_addr.h"
namespace oceanbase {
namespace common {
class ObMySQLProxy;
namespace sqlclient {
class ObMySQLResult;
}
}  // end namespace common
namespace rootserver {
// @see ObReplicaResourceUsage
struct ObReplicaStat {
  common::ObPartitionKey part_key_;
  common::ObAddr server_;
  // DISK IO
  double sstable_read_rate_;
  double sstable_read_bytes_rate_;
  double sstable_write_rate_;
  double sstable_write_bytes_rate_;
  double log_write_rate_;
  double log_write_bytes_rate_;
  // MEMORY
  int64_t memtable_bytes_;
  // CPU (not accurate)
  double cpu_utime_rate_;  // CPU time spent in user mode
  double cpu_stime_rate_;  // CPU time spent in kernel mode
  // NET (not accurate)
  double net_in_rate_;
  double net_in_bytes_rate_;
  double net_out_rate_;
  double net_out_bytes_rate_;
};

// Access class for oceanbase.__all_virtual_partition_info
class ObIReplicaStatIterator {
public:
  virtual ~ObIReplicaStatIterator()
  {}
  virtual int next(ObReplicaStat& replica_stat) = 0;
};

class ObReplicaStatIterator : public ObIReplicaStatIterator {
public:
  ObReplicaStatIterator();
  virtual ~ObReplicaStatIterator();
  int init(common::ObMySQLProxy& proxy);
  int open(uint64_t tenant_id, int64_t replica_cnt = 1);
  int next(ObReplicaStat& replica_stat) override;
  int close();

private:
  int cons_replica_stat(const common::sqlclient::ObMySQLResult& res, ObReplicaStat& replica_stat);

private:
  bool inited_;
  common::ObMySQLProxy* sql_proxy_;
  common::ObMySQLProxy::MySQLResult res_;
  common::sqlclient::ObMySQLResult* result_;
};

// For test only
// several classes are based on ObReplicaStatIterator,
// External fault injection case by case is tedious,
// it is good to depend on the input data from an external physical table
class ObReplicaStatUpdater {
public:
  static int insert_stat(common::ObISQLClient& sql_client, ObReplicaStat& replica_stat);
  static int migrate_stat(common::ObISQLClient& sql_client, const common::ObPartitionKey& p,
      const common::ObAddr& from_svr, const common::ObAddr& to_svr);
  static int delete_stat(
      common::ObISQLClient& sql_client, const common::ObPartitionKey& part_key, const common::ObAddr& server);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_REPLICA_STAT_OPERATOR_H */
