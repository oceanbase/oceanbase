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

#ifndef OCEANBASE_TRANSACTION_OB_GC_PARTITION_ADAPTER_
#define OCEANBASE_TRANSACTION_OB_GC_PARTITION_ADAPTER_

#include "share/ob_errno.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/container/ob_iarray.h"
#include "lib/time/ob_time_utility.h"
#include "common/ob_partition_key.h"
#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
class ObSqlString;
}  // namespace common
namespace transaction {
enum {
  GC_STATUS_UNKNOWN = -1,
  GC_STATUS_EXIST = 0,
  GC_STATUS_NOT_EXIST = 1,
};

inline bool is_partition_status_valid(const int status)
{
  return GC_STATUS_EXIST == status || GC_STATUS_NOT_EXIST == status;
}

typedef common::LinkHashNode<common::ObPartitionKey> GCInfoNode;
typedef common::LinkHashValue<common::ObPartitionKey> GCInfoValue;

class GCInfo : public GCInfoValue {
public:
  GCInfo() : access_ts_(common::ObTimeUtility::current_time()), update_ts_(0), status_(GC_STATUS_UNKNOWN)
  {}
  ~GCInfo()
  {}
  int64_t get_access_ts() const
  {
    return access_ts_;
  }
  int64_t get_update_ts() const
  {
    return ATOMIC_LOAD(&update_ts_);
  }
  int get_status() const
  {
    return ATOMIC_LOAD(&status_);
  }
  void set_status(const int status)
  {
    (void)ATOMIC_STORE(&status_, status);
    (void)ATOMIC_STORE(&update_ts_, common::ObTimeUtility::current_time());
  }
  void update_access_ts()
  {
    (void)ATOMIC_STORE(&access_ts_, common::ObTimeUtility::current_time());
  }

private:
  int64_t access_ts_;
  int64_t update_ts_;
  int status_;
};

class GCInfoAlloc {
public:
  static GCInfo* alloc_value()
  {
    return NULL;
  }
  static void free_value(GCInfo* info)
  {
    if (NULL != info) {
      op_free(info);
      info = NULL;
    }
  }
  static GCInfoNode* alloc_node(GCInfo* info)
  {
    UNUSED(info);
    return op_alloc(GCInfoNode);
  }
  static void free_node(GCInfoNode* node)
  {
    if (NULL != node) {
      op_free(node);
      node = NULL;
    }
  }
};

typedef common::ObLinkHashMap<common::ObPartitionKey, GCInfo, GCInfoAlloc> GCMap;

class ObGCPartitionAdapter : public share::ObThreadPool {
public:
  ObGCPartitionAdapter() : is_inited_(false), is_running_(false), sql_proxy_(NULL)
  {}
  ~ObGCPartitionAdapter()
  {}

public:
  int init(common::ObMySQLProxy* sql_proxy);
  int start();
  void stop();
  void wait();
  void destroy();
  void run1();

public:
  int check_partition_exist(const common::ObPartitionKey& pkey, bool& exist);

public:
  static ObGCPartitionAdapter& get_instance();

private:
  static const int64_t BATCH_GC_PARTITION_QUERY = 64;

private:
  int refresh_all_partition_status_();
  int refresh_partition_status_(const common::ObIArray<common::ObPartitionKey>& pkey_array);
  int clear_obsolete_partition_();
  int get_unknown_partition_array_(common::ObIArray<common::ObPartitionKey>& array);
  int query_status_(const common::ObIArray<common::ObPartitionKey>& pkey_array, common::ObIArray<int>& status_array);
  int fill_sql_(common::ObSqlString& sql, const common::ObIArray<common::ObPartitionKey>& pkey_array);
  int update_partition_status_(
      const common::ObIArray<common::ObPartitionKey>& pkey_array, const common::ObIArray<int>& status_array);

private:
  bool is_inited_;
  bool is_running_;
  common::ObMySQLProxy* sql_proxy_;
  GCMap gc_map_;
};

#define GC_PARTITION_ADAPTER (oceanbase::transaction::ObGCPartitionAdapter::get_instance())

class GetUnknownPartition {
public:
  explicit GetUnknownPartition(common::ObIArray<common::ObPartitionKey>& array) : array_(array)
  {}
  ~GetUnknownPartition()
  {}
  bool operator()(const common::ObPartitionKey& pkey, GCInfo* info);

private:
  static const int64_t MAX_STATUS_REFRESH_INTERVAL = 1 * 1000 * 1000;

private:
  common::ObIArray<common::ObPartitionKey>& array_;
};

class RemoveObsoletePartition {
public:
  RemoveObsoletePartition()
  {}
  ~RemoveObsoletePartition()
  {}
  bool operator()(const common::ObPartitionKey& pkey, GCInfo* info);

private:
  static const int64_t STATUS_OBSOLETE_TIME = 600 * 1000 * 1000;
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_GC_PARTITION_ADAPTER_
