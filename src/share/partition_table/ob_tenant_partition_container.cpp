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

#define USING_LOG_PREFIX SHARE_PT
#include "lib/container/ob_array_iterator.h"
#include "share/partition_table/ob_tenant_partition_container.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace share {
int ObTenantPartitionContainer::Partition::assign(const Partition& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(partition_info_.assign(other.partition_info_))) {
    LOG_WARN("fail to assign partition", K(ret));
  } else {
    tablegroup_id_ = other.tablegroup_id_;
  }
  return ret;
}

bool ObTenantPartitionContainer::ObPartitionGroupOrder::operator()(const Partition* left, const Partition* right)
{
  int cmp = -1;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    // error happen, do nothing
  } else if (NULL == left || NULL == right) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K_(ret), KP(left), KP(right));
  } else {
    // (1) sort by tablegroup id
    // tablegroup_id
    cmp = compare(left->tablegroup_id_, right->tablegroup_id_);
    if (0 == cmp) {
      // (2) sort by table_id, partition_idx when these partitions are not in tablegroups
      if (OB_INVALID_ID == left->tablegroup_id_) {
        cmp = compare(left->partition_info_.get_table_id(), right->partition_info_.get_table_id());
        if (0 == cmp) {
          cmp = compare(left->partition_info_.get_partition_id(), right->partition_info_.get_partition_id());
        }
      } else {
        // (3) sort by tablegroup_id, partition_id, table_id for partitions in tablegroups
        cmp = compare(left->partition_info_.get_partition_id(), right->partition_info_.get_partition_id());
        if (0 == cmp) {
          cmp = compare(left->partition_info_.get_table_id(), right->partition_info_.get_table_id());
        }
      }  // end else
    }
  }
  return cmp < 0;
}

ObTenantPartitionContainer::ObTenantPartitionContainer()
    : inited_(false), schema_service_(NULL), all_partition_(), sorted_partition_()
{}

ObTenantPartitionContainer::~ObTenantPartitionContainer()
{}

int ObTenantPartitionContainer::init(schema::ObMultiVersionSchemaService* schema_service)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema_service));
  } else {
    schema_service_ = schema_service;
  }
  return ret;
}

int ObTenantPartitionContainer::fetch_partition(ObTenantPartitionIterator& iter)
{
  int ret = OB_SUCCESS;
  ObPartitionInfo partition_info;
  Partition partition;
  ObSchemaGetterGuard schema_guard;
  /* 1 when partition info is from a standlone partition,
   *   last_table_id and last_tablegroup_id
   *   are filled using the general values
   * 2 when partition info is from a binding tablegroup partition,
   *   last_table_id is set to tablegroup id,
   *   and last_tablegroup_id is set to -1.
   */
  int64_t last_table_id = OB_INVALID_ID;
  int64_t last_tablegroup_id = OB_INVALID_ID;
  all_partition_.reset();
  sorted_partition_.reset();

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!iter.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(iter));
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  }

  while (OB_SUCC(ret) && OB_SUCC(iter.next(partition_info))) {
    if (OB_INVALID_ID == last_table_id || last_table_id != partition_info.get_table_id()) {
      const uint64_t tid = partition_info.get_table_id();
      if (!is_tablegroup_id(tid)) {  // this partition info is from a standlone table
        const ObSimpleTableSchemaV2* table_schema = NULL;
        if (OB_FAIL(schema_guard.get_table_schema(partition_info.get_table_id(), table_schema))) {
          LOG_WARN("fail to get table schema", K(ret), "table_id", partition_info.get_table_id());
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("get invalid table schema", K(ret), K(table_schema));
        } else {
          last_table_id = partition_info.get_table_id();
          last_tablegroup_id = table_schema->get_tablegroup_id();
          partition.tablegroup_id_ = last_tablegroup_id;
        }
      } else {  // this partition info is from a binding tablegroup
        const ObSimpleTablegroupSchema* tg_schema = nullptr;
        if (OB_FAIL(schema_guard.get_tablegroup_schema(partition_info.get_table_id(), tg_schema))) {
          LOG_WARN("fail to get tablegroup schema", K(ret), "tg_id", partition_info.get_table_id());
        } else if (OB_UNLIKELY(nullptr == tg_schema)) {
          ret = OB_TABLEGROUP_NOT_EXIST;
          LOG_WARN("get invalid tablegroup schema", K(ret), K(tg_schema));
        } else {
          last_table_id = partition_info.get_table_id();
          last_tablegroup_id = -1;
          partition.tablegroup_id_ = -1;
        }
      }
    } else {
      partition.tablegroup_id_ = last_tablegroup_id;
    }
    if (OB_FAIL(ret)) {
      // nothing todo
    } else if (OB_FAIL(partition.partition_info_.assign(partition_info))) {
      LOG_WARN("fail to assign", K(ret), K(partition_info));
    } else if (OB_FAIL(all_partition_.push_back(partition))) {
      LOG_WARN("fail to push back", K(ret));
    }
    partition.reset();
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTenantPartitionContainer::get_sorted_partition(common::ObIArray<Partition*>*& sorted_partition)
{
  int ret = OB_SUCCESS;
  if (0 >= sorted_partition_.count()) {
    if (OB_FAIL(sorted_partition_.reserve(all_partition_.count()))) {
      LOG_WARN("fail to reserve", K(ret));
    } else {
      FOREACH_X(p, all_partition_, OB_SUCC(ret))
      {
        if (OB_FAIL(sorted_partition_.push_back(&(*p)))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      std::sort(sorted_partition_.begin(), sorted_partition_.end(), ObPartitionGroupOrder(ret));
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to compare", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    sorted_partition = &sorted_partition_;
  }
  return ret;
}
}  // namespace share
}  // namespace oceanbase
