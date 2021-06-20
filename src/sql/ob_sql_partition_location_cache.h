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

#ifndef OCEANBASE_SQL_OB_SQL_PARTITION_LOCATION_CACHE_
#define OCEANBASE_SQL_OB_SQL_PARTITION_LOCATION_CACHE_

#include "share/partition_table/ob_partition_location_cache.h"

namespace oceanbase {
namespace sql {
class ObTaskExecutorCtx;
class ObSqlPartitionLocationCache : public share::ObIPartitionLocationCache {
public:
  enum LocationDistributedMode {
    LOC_DIST_MODE_INVALID = 0,
    LOC_DIST_MODE_ONLY_LOCAL,
    LOC_DIST_MODE_DISTRIBUTED,
    LOC_DIST_MODE_ONLY_RS,
  };

public:
  ObSqlPartitionLocationCache();
  virtual ~ObSqlPartitionLocationCache(){};

  void init(share::ObIPartitionLocationCache* loc_cache, const common::ObAddr& self_addr,
      share::schema::ObSchemaGetterGuard* guard_schema);
  bool is_inited() const;

  virtual ObIPartitionLocationCache::PartitionLocationCacheType get_type() const
  {
    return ObIPartitionLocationCache::PART_LOC_CACHE_TYPE_SQL;
  }

  // get partition location of a partition
  // return OB_ENTRY_NOT_EXIST if not record in partition table
  virtual int get(const uint64_t table_id, const int64_t partition_id, share::ObPartitionLocation& location,
      const int64_t last_renew_time, bool& is_cache_hit, const bool auto_update = true) override;

  // get partition location of a partition
  // return OB_ENTRY_NOT_EXIST if not record in partition table
  virtual int get(const common::ObPartitionKey& partition, share::ObPartitionLocation& location,
      const int64_t last_renew_time, bool& is_cache_hit) override;

  // get all partition locations of a table, virtabe table should set partition_num to 1
  // return OB_ENTRY_NOT_EXIST if some partition has record in partition table
  virtual int get(const uint64_t table_id, common::ObIArray<share::ObPartitionLocation>& locations,
      const int64_t last_renew_time, bool& is_cache_hit, const bool auto_update = true) override;

  // get leader addr of a partition, return OB_ENTRY_NOT_EXIST if leader not exist
  virtual int get_strong_leader(
      const common::ObPartitionKey& partition, common::ObAddr& leader, const bool force_renew = false) override;

  // return OB_ENTRY_NOT_EXIST if partition's location not in cache
  virtual int nonblock_get(const uint64_t table_id, const int64_t partition_id, share::ObPartitionLocation& location,
      const int64_t cluster_id = common::OB_INVALID_ID) override;

  // return OB_ENTRY_NOT_EXIST if partition's location not in cache
  virtual int nonblock_get(const common::ObPartitionKey& partition, share::ObPartitionLocation& location,
      const int64_t cluster_id = common::OB_INVALID_ID) override;

  // return OB_ENTRY_NOT_EXIST if partition's location not in cache or leader not exist
  virtual int nonblock_get_strong_leader(const common::ObPartitionKey& partition, common::ObAddr& leader) override;

  // trigger a location update task and clear location in cache
  virtual int nonblock_renew(const common::ObPartitionKey& partition, const int64_t last_renew_time,
      const int64_t cluster_id = common::OB_INVALID_ID) override;

  // try to trigger a location update task and clear location in cache,
  // if it is limited by the limiter and not be done, is_limited will be set to true
  virtual int nonblock_renew_with_limiter(
      const common::ObPartitionKey& partition, const int64_t expire_renew_time, bool& is_limited) override;

  // link table.
  virtual int get_link_table_location(const uint64_t table_id, share::ObPartitionLocation& location);

  ObSqlPartitionLocationCache::LocationDistributedMode get_location_distributed_mode(const uint64_t table_id) const;

  inline void set_task_exec_ctx(ObTaskExecutorCtx* task_exec_ctx)
  {
    task_exec_ctx_ = task_exec_ctx;
  }
  static int get_phy_key(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t table_id,
      const int64_t partition_id, uint64_t& tg_id, int64_t& pg_id, common::ObPGKey& pg_key);
  share::schema::ObSchemaGetterGuard* get_schema_guard()
  {
    return schema_guard_;
  }
  void set_schema_guard(share::schema::ObSchemaGetterGuard* schema_guard)
  {
    schema_guard_ = schema_guard;
  }

private:
  int virtual_get(const uint64_t table_id, const int64_t partition_id, share::ObPartitionLocation& location,
      const int64_t expire_renew_time, bool& is_cache_hit);
  int build_local_location(uint64_t table_id, share::ObPartitionLocation& location);
  int build_distribute_location(uint64_t table_id, const int64_t partition_id, share::ObPartitionLocation& location);
  int get_phy_key(const common::ObPartitionKey& pkey, common::ObPGKey& pg_key);
  int get_phy_key(
      const uint64_t table_id, const int64_t partition_id, uint64_t& tg_id, int64_t& pg_id, common::ObPGKey& pg_key);

private:
  share::ObIPartitionLocationCache* loc_cache_;
  common::ObAddr self_addr_;
  ObTaskExecutorCtx* task_exec_ctx_;  // used for multi-partition virtual-table to get table_location from partition_id
  share::schema::ObSchemaGetterGuard* schema_guard_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlPartitionLocationCache);
};

inline void ObSqlPartitionLocationCache::init(share::ObIPartitionLocationCache* loc_cache,
    const common::ObAddr& self_addr, share::schema::ObSchemaGetterGuard* schema_guard)
{
  loc_cache_ = loc_cache;
  self_addr_ = self_addr;
  schema_guard_ = schema_guard;
}

inline bool ObSqlPartitionLocationCache::is_inited() const
{
  bool bool_ret = false;
  if (NULL != loc_cache_ && self_addr_.is_valid() && nullptr != schema_guard_) {
    bool_ret = true;
  }
  return bool_ret;
}

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_OB_SQL_PARTITION_LOCATION_CACHE_ */
