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

#ifndef OCEANBASE_SHARE_OB_TENANT_PARTITION_CONTAINER_H
#define OCEANBASE_SHARE_OB_TENANT_PARTITION_CONTAINER_H
#include "lib/ob_define.h"
#include "lib/container/ob_array.h"
#include "share/partition_table/ob_partition_info.h"
namespace oceanbase {
namespace share {
class ObTenantPartitionIterator;
namespace schema {
class ObSchemaGetterGuard;
class ObMultiVersionSchemaService;
}  // namespace schema

class ObTenantPartitionContainer {
public:
  struct Partition {
  public:
    Partition() : partition_info_(), tablegroup_id_(common::OB_INVALID_ID)
    {}
    void reset()
    {
      partition_info_.reuse();
      tablegroup_id_ = common::OB_INVALID_ID;
    }
    int assign(const Partition& other);
    TO_STRING_KV(K_(partition_info), K_(tablegroup_id));

  public:
    ObPartitionInfo partition_info_;
    int64_t tablegroup_id_;
  };

  struct ObPartitionGroupOrder {
    explicit ObPartitionGroupOrder(int& ret) : ret_(ret)
    {}
    bool operator()(const Partition* left, const Partition* right);

  private:
    template <typename T>
    int compare(const T& left, const T& right)
    {
      return left < right ? -1 : (left == right ? 0 : 1);
    }

  private:
    int& ret_;
  };

  ObTenantPartitionContainer();
  ~ObTenantPartitionContainer();
  int init(schema::ObMultiVersionSchemaService* schema_service);
  int fetch_partition(ObTenantPartitionIterator& iter);
  int get_sorted_partition(common::ObIArray<Partition*>*& sorted_partition);

private:
  bool inited_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObArray<Partition> all_partition_;
  // order by tablegroup_id, partition_id, table_id
  common::ObArray<Partition*> sorted_partition_;
};
}  // namespace share
}  // namespace oceanbase
#endif
