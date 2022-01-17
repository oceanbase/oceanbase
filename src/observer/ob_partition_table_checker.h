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

#ifndef OCEANBASE_OBSERVER_OB_PARTITION_TABLE_CHECKER_H_
#define OCEANBASE_OBSERVER_OB_PARTITION_TABLE_CHECKER_H_

#include "lib/net/ob_addr.h"
#include "lib/hash/ob_hashset.h"
#include "lib/queue/ob_dedup_queue.h"
#include "lib/task/ob_timer.h"
#include "lib/container/ob_array.h"
#include "common/ob_partition_key.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase {

namespace common {
class ObAddr;
class ObTimer;
class ObServerConfig;
}  // end namespace common

namespace obrpc {
class ObSrvRpcProxy;
}  // end of namespace obrpc

namespace share {
class ObPartitionTableOperator;
class ObPartitionReplica;
namespace schema {
class ObMultiVersionSchemaService;
}  // end namespace schema
}  // end namespace share

namespace storage {
class ObPartitionService;
}  // end namespace storage

namespace observer {
class ObPartitionTableChecker;
class ObCheckPartitionTableTask : public common::IObDedupTask {
public:
  typedef common::ObSArray<common::ObAddr> ServerList;
  ObCheckPartitionTableTask(ObPartitionTableChecker& pt_checker, volatile const bool& stopped);
  virtual ~ObCheckPartitionTableTask();

  inline bool is_valid() const;

  virtual int64_t hash() const;
  virtual bool operator==(const IObDedupTask& other) const;
  virtual int64_t get_deep_copy_size() const;
  virtual IObDedupTask* deep_copy(char* buf, const int64_t buf_size) const;
  virtual int64_t get_abs_expired_time() const
  {
    return 0;
  }
  virtual int process();

  TO_STRING_KV("pt_checker", reinterpret_cast<int64_t>(&pt_checker_));

private:
  ObPartitionTableChecker& pt_checker_;
  volatile const bool& stopped_;
};

class ObCheckDanglingReplicaTask : public common::IObDedupTask {
public:
  ObCheckDanglingReplicaTask(ObPartitionTableChecker& pt_checker, volatile const bool& stopped, int64_t version);
  virtual ~ObCheckDanglingReplicaTask();

  inline bool is_valid() const;

  virtual int64_t hash() const;
  virtual bool operator==(const IObDedupTask& other) const;
  virtual int64_t get_deep_copy_size() const;
  virtual IObDedupTask* deep_copy(char* buf, const int64_t buf_size) const;
  virtual int64_t get_abs_expired_time() const
  {
    return 0;
  }
  virtual int process();

  TO_STRING_KV("pt_checker", reinterpret_cast<int64_t>(&pt_checker_), K_(version));

private:
  ObPartitionTableChecker& pt_checker_;
  volatile const bool& stopped_;
  int64_t version_;
};

class ObPartitionTableChecker : public common::ObTimerTask {
public:
  ObPartitionTableChecker();
  virtual ~ObPartitionTableChecker();

  int init(share::ObPartitionTableOperator& pt_operator, share::schema::ObMultiVersionSchemaService& schema_service,
      storage::ObPartitionService& partition_service, int tg_id);
  inline bool is_inited()
  {
    return inited_;
  }
  inline void stop()
  {
    stopped_ = true;
  }
  int destroy();

  virtual void runTimerTask();

  int schedule_task();
  int schedule_task(int64_t version);
  int check_partition_table();
  int check_dangling_replica_exist(const int64_t version);

private:
  typedef common::hash::ObHashMap<common::ObPartitionKey, share::ObPartitionReplica, common::hash::NoPthreadDefendMode>
      ObReplicaMap;
  static const int64_t SERVER_REPLICA_MAP_BUCKET_NUM = 64 * 1024;
  static const int64_t PT_CHECK_THREAD_CNT = 1;
  static const int64_t PT_CHECK_QUEUE_SIZE = 64;
  static const int64_t PTTASK_MAP_SIZE = 256;
  static const int64_t PT_CHECK_THREAD_DEAD_THRESHOLD = 300L * 1000L * 1000L;  // 300s
private:
  // iterator partition table, and put replicas in current observer in hashset
  int build_replica_map(ObReplicaMap& server_replica_map);
  // check replica exist in partition table but not in observer
  int check_dangling_replicas(ObReplicaMap& server_replica_map, int64_t& dangling_count);
  int check_report_replicas(ObReplicaMap& server_replica_map, int64_t& report_count);
  int check_if_replica_equal(
      share::ObPartitionReplica& replica, share::ObPartitionReplica& local_replica, bool& is_equal);
  int member_list_is_equal(share::ObPartitionReplica::MemberList& a, share::ObPartitionReplica::MemberList& b);

private:
  bool inited_;
  volatile bool stopped_;
  share::ObPartitionTableOperator* pt_operator_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  storage::ObPartitionService* partition_service_;

  int tg_id_;
  common::ObDedupQueue queue_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionTableChecker);
};

bool ObCheckPartitionTableTask::is_valid() const
{
  return pt_checker_.is_inited();
}

bool ObCheckDanglingReplicaTask::is_valid() const
{
  return pt_checker_.is_inited();
}
}  // end namespace observer
}  // end namespace oceanbase
#endif  // OCEANBASE_OBSERVER_OB_PARTITION_TABLE_CHECKER_H_
