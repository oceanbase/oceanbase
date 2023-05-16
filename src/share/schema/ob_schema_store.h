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

#ifndef OCEANBASE_SCHEMA_OB_TENANT_SCHEMA_STORE_H_
#define OCEANBASE_SCHEMA_OB_TENANT_SCHEMA_STORE_H_

#include "lib/hash/ob_ptr_array_map.h"
#include "share/schema/ob_schema_mgr_cache.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaStore
{
public:
  static const int64_t MAX_VERSION_COUNT = 64;
  static const int64_t MAX_VERSION_COUNT_FOR_LIBOBLOG = 6;
  ObSchemaStore()
    : tenant_id_(0),
      refreshed_version_(0),
      received_version_(0),
      checked_sys_version_(0),
      baseline_schema_version_(common::OB_INVALID_VERSION),
      consensus_version_(0) {}
  ~ObSchemaStore() {}
  int init(const uint64_t tenant_id,
           const int64_t init_version_count,
           const int64_t init_version_count_for_liboblog);
  void reset_version();
  void update_refreshed_version(int64_t version);
  void update_received_version(int64_t version);
  void update_checked_sys_version(int64_t version);
  void update_baseline_schema_version(int64_t version);
  void update_consensus_version(int64_t version);
  int64_t get_refreshed_version() const { return ATOMIC_LOAD(&refreshed_version_); }
  int64_t get_received_version() const { return ATOMIC_LOAD(&received_version_); }
  int64_t get_checked_sys_version() const { return ATOMIC_LOAD(&checked_sys_version_); }
  int64_t get_baseline_schema_version() const { return ATOMIC_LOAD(&baseline_schema_version_); }
  int64_t get_consensus_version() const { return ATOMIC_LOAD(&consensus_version_); }

  int64_t tenant_id_;
  int64_t refreshed_version_;
  int64_t received_version_;
  int64_t checked_sys_version_;
  int64_t baseline_schema_version_;
  int64_t consensus_version_;
  ObSchemaMgrCache schema_mgr_cache_;
  ObSchemaMgrCache schema_mgr_cache_for_liboblog_;
};

class ObSchemaStoreMap
{
public:
  const static int TENANT_MAP_BUCKET_NUM = 1024;
  ObSchemaStoreMap() {}
  ~ObSchemaStoreMap() { destroy(); }
  int init(int64_t bucket_num);
  void destroy();
  int create(const uint64_t tenant_id,
             const int64_t init_version_count,
             const int64_t init_version_count_for_liboblog);
  const ObSchemaStore* get(uint64_t tenant_id) const;
  ObSchemaStore* get(uint64_t tenant_id);
  int get_all_tenant(common::ObIArray<uint64_t> &tenant_ids);
private:
  common::ObLinkArray map_;
};

}; // end namespace schema
}; // end namespace share
}; // end namespace oceanbase

#endif /* OCEANBASE_SCHEMA_OB_TENANT_SCHEMA_STORE_H_ */
