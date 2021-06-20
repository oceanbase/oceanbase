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

#include "ob_partition_table_iterator.h"

#include "lib/profile/ob_trace_id.h"
#include "lib/container/ob_array_iterator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/config/ob_server_config.h"
#include "ob_partition_table_operator.h"

namespace oceanbase {
namespace share {

using namespace common;
using namespace schema;

void ObPartIdAscIterator::reset()
{
  sorted_part_id_array_.reset();
  part_level_ = PARTITION_LEVEL_MAX;
  cur_idx_ = 0;
}

int ObPartIdAscIterator::build(const uint64_t partition_entity_id,
    const share::schema::ObPartitionSchema& partition_schema, const bool filter_dropped_schema)
{
  int ret = OB_SUCCESS;
  reset();
  ObPartitionKeyIter part_key_iter(partition_entity_id, partition_schema, !filter_dropped_schema);
  int64_t phy_part_id = -1;
  int64_t partition_num = part_key_iter.get_partition_num();
  if (OB_FAIL(sorted_part_id_array_.reserve(partition_num))) {
    LOG_WARN("Failed to reserve array", K(ret), K(partition_num));
  }
  while (OB_SUCC(ret) && OB_SUCC(part_key_iter.next_partition_id_v2(phy_part_id))) {
    if (OB_FAIL(sorted_part_id_array_.push_back(phy_part_id))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    std::sort(sorted_part_id_array_.begin(), sorted_part_id_array_.end());
    part_level_ = partition_schema.get_part_level();
  }
  return ret;
}

int ObPartIdAscIterator::generate_next_part_id(int64_t& part_id) const
{
  int ret = OB_SUCCESS;
  if (cur_idx_ >= sorted_part_id_array_.count()) {
    ret = OB_ITER_END;
  } else {
    part_id = sorted_part_id_array_.at(cur_idx_);
  }
  return ret;
}

bool ObPartIdAscIterator::is_iter_end() const
{
  return cur_idx_ >= sorted_part_id_array_.count();
}

void ObPartIdAscIterator::inc_iter()
{
  ++cur_idx_;
}

int ObPartIdAscIterator::check_out_of_part_id_range(const int64_t partition_id, bool& out_of_range) const
{
  int ret = OB_SUCCESS;
  out_of_range = true;
  if (sorted_part_id_array_.count() <= 0) {
    out_of_range = true;
  } else {
    common::ObArray<int64_t>::const_iterator pos =
        std::lower_bound(sorted_part_id_array_.begin(), sorted_part_id_array_.end(), partition_id);
    if (pos != sorted_part_id_array_.end() && *pos == partition_id) {
      out_of_range = false;
    } else {
      out_of_range = true;
    }
  }
  return ret;
}

int ObTablePartitionIterator::ObPrefetchInfo::init(uint64_t table_id, ObPartitionTableOperator& pt_operator)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else {
    table_id_ = table_id;
    pt_operator_ = &pt_operator;
  }
  return ret;
}
int ObTablePartitionIterator::ObPrefetchInfo::get(ObPartitionInfo& partition)
{
  int ret = OB_SUCCESS;
  if (need_prefetch()) {
    if (OB_FAIL(prefetch())) {
      LOG_WARN("fail to prefetch", K(ret), K_(table_id));
    }
  }
  if (OB_SUCC(ret)) {
    if (prefetch_idx_ >= prefetch_count()) {
      prefetch_iter_end_ = true;
      ret = OB_ITER_END;
    } else if (OB_FAIL(partition.assign(prefetch_partitions_.at(prefetch_idx_)))) {
      LOG_WARN("fail to assgign partition", K(ret));
    }
  }
  return ret;
}

int ObTablePartitionIterator::ObPrefetchInfo::next(ObPartitionInfo& partition)
{
  int ret = OB_SUCCESS;
  if (prefetch_idx_ >= prefetch_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid next", K(prefetch_idx_), K(prefetch_count()));
  } else if (OB_FAIL(partition.assign(prefetch_partitions_.at(prefetch_idx_)))) {
    LOG_WARN("fail to assign partition", K(ret), K(prefetch_idx_));
  } else {
    prefetch_idx_++;
  }
  return ret;
}

int ObTablePartitionIterator::ObPrefetchInfo::prefetch()
{
  int ret = OB_SUCCESS;
  if (prefetch_idx_ < prefetch_partitions_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no need to do prefetch", K(ret), K(prefetch_idx_), K(prefetch_partitions_.count()));
  } else if (OB_ISNULL(pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(pt_operator_));
  } else {
    int64_t start_partition_id = 0;
    bool first_prefetch = true;
    if (prefetch_partitions_.count() > 0) {
      start_partition_id = prefetch_partitions_.at(prefetch_partitions_.count() - 1).get_partition_id();
      first_prefetch = false;
    }
    uint64_t tenant_id = extract_tenant_id(table_id_);
    prefetch_idx_ = 0;
    prefetch_partitions_.reuse();
    if (OB_FAIL(pt_operator_->prefetch_by_table_id(
            tenant_id, table_id_, start_partition_id, prefetch_partitions_, need_fetch_faillist_))) {
      LOG_WARN("fail to prefetch partitions", K(ret), K(table_id_), K(start_partition_id));
    } else if (!first_prefetch) {
      prefetch_idx_++;  // the first partition is duplicated, need to be filtered
    }
  }
  return ret;
}
int64_t ObTablePartitionIterator::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KT_(table_id));
  J_OBJ_END();
  return pos;
}

ObTablePartitionIterator::ObTablePartitionIterator()
    : inited_(false),
      table_id_(OB_INVALID_ID),
      part_level_(PARTITION_LEVEL_ZERO),
      asc_part_id_iterator_(),
      pt_operator_(NULL),
      allocator_(ObModIds::OB_RS_PARTITION_TABLE_TEMP),
      prefetch_info_(),
      filters_()
{}

ObTablePartitionIterator::~ObTablePartitionIterator()
{}

// check if we need to access the tenant level meta table by the mode
// when TablePartitionIterator::init is invoked
int ObTablePartitionIterator::init(
    const uint64_t table_id, ObSchemaGetterGuard& schema_guard, ObPartitionTableOperator& pt_operator)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = extract_tenant_id(table_id);
  if (OB_INVALID_ID == table_id || OB_INVALID_ID == tenant_id || OB_INVALID_ID == extract_pure_id(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(table_id));
  } else if (OB_FAIL(prefetch_info_.init(table_id, pt_operator))) {
    LOG_WARN("fail to init prefetch info", K(ret));
  } else {
    share::schema::ObSchemaType schema_type =
        (is_new_tablegroup_id(table_id) ? ObSchemaType::TABLEGROUP_SCHEMA : ObSchemaType::TABLE_SCHEMA);
    const share::schema::ObPartitionSchema* partition_schema = nullptr;
    const ObSimpleTenantSchema* tenant = NULL;
    if (OB_FAIL(share::schema::ObPartMgrUtils::get_partition_schema(
            schema_guard, table_id, schema_type, partition_schema))) {
      LOG_WARN("fail to get partition schema", K(ret), "schema_id", table_id);
    } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant))) {
      LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", K(ret), K(tenant_id));
    } else if (OB_FAIL(asc_part_id_iterator_.build(table_id, *partition_schema, tenant->is_restore()))) {
      LOG_WARN("fail to build asc part id iterator", K(ret));
    } else {
      table_id_ = table_id;
      pt_operator_ = &pt_operator;
      part_level_ = partition_schema->get_part_level();
      prefetch_info_.reset();
      prefetch_info_.set_need_fetch_faillist(need_fetch_faillist_);
      allocator_.reuse();
      inited_ = true;
    }
  }
  return ret;
}

int ObTablePartitionIterator::next(ObPartitionInfo& partition)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == partition.get_allocator()) {
    partition.set_allocator(&allocator_);
  }
  bool will_break = false;
  do {
    will_break = true;
    ObPartitionInfo tmp_partition;
    tmp_partition.set_allocator(&allocator_);
    bool prefetch_end = false;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(prefetch_info_.get(tmp_partition))) {
        if (OB_ITER_END == ret) {
          prefetch_end = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next prefetch partition info", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      CheckType type = LEGAL_REPLICA;
      if (prefetch_end) {
        if (asc_part_id_iterator_.is_iter_end()) {
          ret = OB_ITER_END;
        } else {
          if (OB_FAIL(mock_next_partition(partition))) {
            LOG_WARN("fail to mock next partition", K(ret));
          }
        }
      } else if (OB_FAIL(check_replica(tmp_partition, type))) {
        LOG_WARN("fail to check replica", K(ret), K(tmp_partition));
      } else if (LEGAL_REPLICA == type || REDUNDANT_REPLICA == type) {
        if (OB_FAIL(prefetch_info_.next(partition))) {
          LOG_WARN("fail to get next partition", K(ret));
        }
      } else if (LOST_REPLICA == type) {
        if (OB_FAIL(mock_next_partition(partition))) {
          LOG_WARN("fail to mock next partition", K(ret));
        }
      } else if (OUTOF_SCHEMA_REPLICA == type) {
        // do not care ret, just let it iterate
        ret = prefetch_info_.next(tmp_partition);
        will_break = false;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected replica type", K(type), K(ret));
      }
    }
  } while (OB_SUCC(ret) && !will_break);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(partition.filter(filters_))) {
      LOG_WARN("filter replica failed", K(ret), K(partition));
    } else {
      asc_part_id_iterator_.inc_iter();
    }
  }

  return ret;
}

int ObTablePartitionIterator::mock_next_partition(ObPartitionInfo& info)
{
  int ret = OB_SUCCESS;
  int64_t next_partition_id = -1;
  if (OB_FAIL(asc_part_id_iterator_.generate_next_part_id(next_partition_id))) {
    LOG_WARN("fail to get next part id", K(ret));
  } else {
    info.set_table_id(table_id_);
    info.set_partition_id(next_partition_id);
  }
  return ret;
}

int ObTablePartitionIterator::check_replica(ObPartitionInfo& info, CheckType& check_type)
{
  int ret = OB_SUCCESS;
  check_type = INVALID_REPLICA;

  bool out_of = false;
  int64_t expect_partition_id = 0;
  if (OB_FAIL(asc_part_id_iterator_.check_out_of_part_id_range(info.get_partition_id(), out_of))) {
    LOG_WARN("fail to check out of range", K(info.get_partition_id()), K(ret));
  } else if (out_of) {
    check_type = OUTOF_SCHEMA_REPLICA;
  } else if (asc_part_id_iterator_.is_iter_end()) {
    check_type = REDUNDANT_REPLICA;
  } else if (OB_FAIL(asc_part_id_iterator_.generate_next_part_id(expect_partition_id))) {
    LOG_WARN("fail to get next part id", K(ret));
  } else if (info.get_partition_id() == expect_partition_id) {
    check_type = LEGAL_REPLICA;
  } else if (info.get_partition_id() > expect_partition_id) {
    // expected <10>, got <11> or expected <1.5>, got <1.6>
    check_type = LOST_REPLICA;
  } else {
    // expected <18>, got <17>  or expected <1,6>, got <1,5>
    check_type = REDUNDANT_REPLICA;
  }
  return ret;
}

///////////////////

ObTenantPartitionIterator::ObTenantPartitionIterator()
    : inited_(false),
      pt_operator_(NULL),
      schema_service_(NULL),
      partition_entity_iterator_(),
      allocator_(ObModIds::OB_RS_PARTITION_TABLE_TEMP),
      filters_(),
      tenant_id_(OB_INVALID_ID),
      prefetch_partitions_(),
      prefetch_idx_(0),
      tenant_end_(false),
      partition_entity_id_(OB_INVALID_ID),
      asc_part_id_iterator_(),
      ignore_row_checksum_(true),
      partition_entity_filters_(),
      filter_dropped_schema_(false)
{}

ObTenantPartitionIterator::~ObTenantPartitionIterator()
{}

int ObTenantPartitionIterator::init(ObPartitionTableOperator& pt_operator, ObMultiVersionSchemaService& schema_service,
    const uint64_t tenant_id, bool ignore_row_checksum)
{
  int ret = OB_SUCCESS;
  // allow init twice
  const ObSimpleTenantSchema* tenant = NULL;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(filters_.set_valid_version())) {
    LOG_WARN("set valid version filter failed", K(ret));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard_))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard_.get_tenant_info(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", K(ret), K(tenant_id));
  } else if (OB_FAIL(partition_entity_iterator_.init(schema_guard_, tenant_id))) {
    LOG_WARN("table iterator init failed", K(tenant_id), K(ret));
  } else {
    pt_operator_ = &pt_operator;
    schema_service_ = &schema_service;
    tenant_id_ = tenant_id;
    prefetch_partitions_.reuse();
    prefetch_idx_ = 0;
    tenant_end_ = false;
    partition_entity_id_ = OB_INVALID_ID;
    ignore_row_checksum_ = ignore_row_checksum;
    filter_dropped_schema_ = tenant->is_restore();
    inited_ = true;
  }
  return ret;
}

int ObTenantPartitionIterator::next_partition_entity()
{
  int ret = OB_SUCCESS;
  uint64_t this_partition_entity_id = OB_INVALID_ID;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }
  ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  while (OB_SUCC(ret) && OB_SUCC(partition_entity_iterator_.next(this_partition_entity_id))) {
    ObSchema tmp_schema;
    ObPrimaryZone primary_zone_info(&tmp_schema);
    asc_part_id_iterator_.reset();
    zone_locality.reuse();
    const share::schema::ObPartitionSchema* partition_schema = nullptr;
    bool pass = true;
    const share::schema::ObSchemaType schema_type =
        (is_new_tablegroup_id(this_partition_entity_id) ? ObSchemaType::TABLEGROUP_SCHEMA : ObSchemaType::TABLE_SCHEMA);
    if (OB_FAIL(share::schema::ObPartMgrUtils::get_partition_schema(
            schema_guard_, this_partition_entity_id, schema_type, partition_schema))) {
      LOG_WARN("fail to get partition schema", K(ret), K(this_partition_entity_id));
    } else if (!partition_schema->has_self_partition()) {
      pass = false;  // has no partition
    } else if (filter_dropped_schema_ && partition_schema->is_dropped_schema()) {
      pass = false;  // filter dropped schema while restore
    } else if (OB_FAIL(partition_schema->get_primary_zone_inherit(schema_guard_, primary_zone_info))) {
      LOG_WARN("fail to get primary zone inherit", K(ret));
    } else if (OB_FAIL(partition_schema->get_zone_replica_attr_array_inherit(schema_guard_, zone_locality))) {
      LOG_WARN("fail to get zone replica attr array inherit", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (!pass) {
        // not pass, iterate the next one
      } else if (OB_FAIL(partition_entity_filters_.check(primary_zone_info, zone_locality, pass))) {
        LOG_WARN("fail to check table", K(ret), K(this_partition_entity_id));
      } else if (!pass) {
        // nothing todo
      } else if (OB_UNLIKELY(nullptr == partition_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition_schema ptr is null", K(ret));
      } else if (OB_FAIL(asc_part_id_iterator_.build(
                     this_partition_entity_id, *partition_schema, filter_dropped_schema_))) {
        LOG_WARN("fail to build asc part id iterator", K(ret));
      } else {
        partition_entity_id_ = this_partition_entity_id;
        break;
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObTenantPartitionIterator::inner_next(ObPartitionInfo& partition)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (asc_part_id_iterator_.is_iter_end()) {
      ret = OB_ITER_END;  // table partition iterator end
    } else if (prefetch_idx_ >= prefetch_partitions_.count()) {
      // prefetch partition iterator end
      if (!tenant_end_ && OB_FAIL(prefetch())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("prefetch failed", K(ret));
        } else {
          ret = OB_SUCCESS;
          tenant_end_ = true;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    // skip partition of table already deleted
    while (OB_SUCCESS == ret && !tenant_end_ &&
           partition_entity_id_ > prefetch_partitions_.at(prefetch_idx_).get_table_id()) {
      // table_id of the next table is greater then table invalid table_id prefetched before
      ++prefetch_idx_;
      if (prefetch_idx_ == prefetch_partitions_.count()) {
        // prefetch partition iterator end
        if (OB_FAIL(prefetch())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("prefetch failed", K(ret));
          } else {
            ret = OB_SUCCESS;
            tenant_end_ = true;
          }
        }
      }
    }
    int64_t expect_partition_id = -1;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(asc_part_id_iterator_.generate_next_part_id(expect_partition_id))) {
        LOG_WARN("fail to get next part id", K(ret));
      }
    }
    int64_t part_id_fetch = 0;
    uint64_t table_id_fetch = 0;
    if (OB_SUCC(ret) && !tenant_end_) {
      part_id_fetch = prefetch_partitions_.at(prefetch_idx_).get_partition_id();
      table_id_fetch = prefetch_partitions_.at(prefetch_idx_).get_table_id();
    }
    if (OB_FAIL(ret)) {
      // this is used to process the situation of null partition,
      // it depends on the successive increment of paritition_id_,
      // however the paritition_id_ is not successive increment for sub partition
    } else if (tenant_end_ || partition_entity_id_ < table_id_fetch ||
               (partition_entity_id_ == table_id_fetch && expect_partition_id < part_id_fetch)) {
      // 1. exist in schema, but not in meta_table
      // return empty partition here, do not set error ret.
      LOG_WARN("find hole in partition table",
          K_(tenant_end),
          K_(partition_entity_id),
          K(expect_partition_id),
          K(table_id_fetch),
          K(part_id_fetch));
      partition.set_table_id(partition_entity_id_);
      partition.set_partition_id(expect_partition_id);
      asc_part_id_iterator_.inc_iter();
    } else if (partition_entity_id_ == table_id_fetch && expect_partition_id > part_id_fetch) {
      // 2. not exist in schema, but exist in meta_table
      LOG_DEBUG("prefetched partition_id smaller than partition to iterated",
          KR(ret),
          K(partition_entity_id_),
          K(expect_partition_id),
          K(table_id_fetch),
          K(part_id_fetch));
      // 2.1 ignore the prefetch_partitions for table/tablegroup schemas do not exist
      while (OB_SUCC(ret) && prefetch_idx_ < prefetch_partitions_.count() &&
             partition_entity_id_ == prefetch_partitions_.at(prefetch_idx_).get_table_id() &&
             expect_partition_id > prefetch_partitions_.at(prefetch_idx_).get_partition_id()) {
        ++prefetch_idx_;
        if (prefetch_idx_ == prefetch_partitions_.count()) {
          // 2.2 prefetch ignore the partitions whose schema do not exist
          const uint64_t last_table_id = partition_entity_id_;
          const int64_t last_partition_id = expect_partition_id - 1;
          if (OB_FAIL(prefetch(last_table_id, last_partition_id))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("prefetch failed", KR(ret), K(last_table_id), K(last_partition_id));
            } else {
              ret = OB_SUCCESS;
              tenant_end_ = true;
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(inner_next(partition))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("inner next failed", KR(ret));
          }
        }
      }
    } else {
      if (OB_FAIL(partition.assign(prefetch_partitions_[prefetch_idx_]))) {
        LOG_WARN("failed to assign partition", K(ret));
      } else {
        ++prefetch_idx_;
        asc_part_id_iterator_.inc_iter();
      }
    }
  }
  return ret;
}

int ObTenantPartitionIterator::next(ObPartitionInfo& partition)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (NULL == partition.get_allocator()) {
      partition.set_allocator(&allocator_);
    }
    if (OB_INVALID_ID == partition_entity_id_) {
      if (OB_FAIL(next_partition_entity())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("switch to next table failed", K(ret));
        }
      }
    }
  }

  do {
    if (OB_SUCC(ret)) {
      partition.reuse();
      if (OB_FAIL(inner_next(partition))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("inner_next failed", K(ret));
        } else {
          if (OB_FAIL(next_partition_entity())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("switch to next table failed", K(ret));
            }
          } else if (OB_FAIL(inner_next(partition))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("inner next failed", K(ret));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition.filter(filters_))) {
        LOG_WARN("filter replica failed", K(ret), K(partition));
      }
    }
  } while (OB_SUCCESS == ret && 0 == partition.replica_count() && filters_.skip_empty_partition());
  return ret;
}

int ObTenantPartitionIterator::prefetch()
{
  int ret = OB_SUCCESS;
  uint64_t last_table_id = OB_INVALID_ID;
  int64_t last_partition_id = OB_INVALID_INDEX;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (prefetch_idx_ != prefetch_partitions_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prefetched partition infos have not been iterated to end",
        K_(prefetch_idx),
        "prefetch count",
        prefetch_partitions_.count(),
        K(ret));
  } else if (OB_ISNULL(pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt_operator is null", K(ret));
  } else {
    // intialize table_id and the start partition_id
    if (prefetch_partitions_.count() <= 0) {  // try to prefetch the first time
      last_table_id = 0;
      last_partition_id = 0;
    } else {  // prefetched before
      const int64_t last_idx = prefetch_partitions_.count() - 1;
      last_table_id = prefetch_partitions_.at(last_idx).get_table_id();
      last_partition_id = prefetch_partitions_.at(last_idx).get_partition_id();
    }

    // prefetch will assure that last partition info contains all replicas of that partition
    prefetch_idx_ = 0;
    prefetch_partitions_.reuse();
    if (OB_FAIL(pt_operator_->prefetch(
            tenant_id_, last_table_id, last_partition_id, prefetch_partitions_, ignore_row_checksum_))) {
      LOG_WARN("pt_operator prefetch failed", K_(tenant_id), KT(last_table_id), K(last_partition_id), K(ret));
    } else if (prefetch_partitions_.count() <= 0) {
      // if the prefetch result is empty, iter end
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObTenantPartitionIterator::prefetch(const uint64_t last_table_id, const int64_t last_partition_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (prefetch_idx_ != prefetch_partitions_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prefetched partition infos have not been iterated to end",
        K_(prefetch_idx),
        "prefetch count",
        prefetch_partitions_.count(),
        K(ret));
  } else if (OB_ISNULL(pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt_operator is null", K(ret));
  } else {
    // prefetch will assure that last partition info contains all replicas of that partition
    prefetch_idx_ = 0;
    prefetch_partitions_.reuse();
    if (OB_FAIL(pt_operator_->prefetch(
            tenant_id_, last_table_id, last_partition_id, prefetch_partitions_, ignore_row_checksum_))) {
      LOG_WARN("pt_operator prefetch failed", K_(tenant_id), KT(last_table_id), K(last_partition_id), K(ret));
    } else if (prefetch_partitions_.count() <= 0) {
      // if the prefetch result is empty, iter end
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int64_t ObTenantPartitionIterator::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();

  J_KV(K_(inited),
      K_(partition_entity_iterator),
      K_(tenant_id),
      "prefetch count",
      prefetch_partitions_.count(),
      K_(prefetch_idx),
      K_(tenant_end),
      K_(partition_entity_id));
  J_OBJ_END();
  return pos;
}

ObPartitionTableIterator::ObPartitionTableIterator()
    : inited_(false),
      pt_operator_(NULL),
      schema_service_(NULL),
      tenant_partition_iter_(),
      tenant_iter_(),
      ignore_row_checksum_(true)
{}

ObPartitionTableIterator::~ObPartitionTableIterator()
{}

int ObPartitionTableIterator::init(
    ObPartitionTableOperator& pt_operator, ObMultiVersionSchemaService& schema_service, bool ignore_row_checksum)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::TraceId* trace_id = ObCurTraceId::get_trace_id();
  if (OB_ISNULL(trace_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid trace id", K(ret));
  } else if (trace_id->is_invalid()) {
    ObCurTraceId::init(GCONF.self_addr_);
  }
  if (OB_SUCC(ret) && inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(tenant_iter_.init(schema_service))) {
    LOG_WARN("tenant iterator init failed", K(ret));
  } else {
    pt_operator_ = &pt_operator;
    schema_service_ = &schema_service;
    ignore_row_checksum_ = ignore_row_checksum;
    inited_ = true;
  }
  return ret;
}

int ObPartitionTableIterator::next(ObPartitionInfo& partition)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (!tenant_partition_iter_.is_inited()) {
      if (OB_FAIL(next_tenant())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("next_tenant failed", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tenant_partition_iter_.next(partition))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("tenant partition iterate failed", K(ret));
      } else {
        while (OB_ITER_END == ret) {
          if (OB_FAIL(next_tenant())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("next_tenant failed", K(ret));
            } else {
              break;
            }
          } else {
            if (OB_FAIL(tenant_partition_iter_.next(partition))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("tenant partition iterate failed", K(ret));
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int64_t ObPartitionTableIterator::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();

  J_KV(K_(inited), K_(tenant_partition_iter), K_(tenant_iter));
  J_OBJ_END();
  return pos;
}

int ObPartitionTableIterator::next_tenant()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(tenant_iter_.next(tenant_id))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("tenant iterate failed", K(ret));
    }
  } else if (OB_FAIL(tenant_partition_iter_.init(*pt_operator_, *schema_service_, tenant_id, ignore_row_checksum_))) {
    LOG_WARN("tenant partition iterator init failed", K(tenant_id), K(ret));
  }
  return ret;
}

ObPTPartPartitionIterator::ObPTPartPartitionIterator()
    : inited_(false),
      pt_operator_(NULL),
      pt_table_id_(OB_INVALID_ID),
      pt_partition_id_(OB_INVALID_INDEX),
      prefetch_partitions_(),
      prefetch_idx_(0),
      allocator_(ObModIds::OB_RS_PARTITION_TABLE_TEMP),
      filters_()
{}

ObPTPartPartitionIterator::~ObPTPartPartitionIterator()
{}

int ObPTPartPartitionIterator::init(
    ObPartitionTableOperator& pt_operator, const uint64_t pt_table_id, const int64_t pt_partition_id)
{
  int ret = OB_SUCCESS;
  // allow init twice
  if (OB_INVALID_ID == pt_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pt_table_id", K(pt_table_id), K(ret));
  } else if (pt_partition_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pt_partition_id", K(pt_partition_id), K(ret));
  } else {
    pt_operator_ = &pt_operator;
    pt_table_id_ = pt_table_id;
    pt_partition_id_ = pt_partition_id;
    prefetch_partitions_.reuse();
    prefetch_idx_ = 0;
    inited_ = true;
  }
  return ret;
}

int ObPTPartPartitionIterator::next(ObPartitionInfo& partition)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    partition.reuse();
    if (NULL == partition.get_allocator()) {
      partition.set_allocator(&allocator_);
    }

    if (prefetch_idx_ < prefetch_partitions_.count()) {
      if (OB_FAIL(partition.assign(prefetch_partitions_[prefetch_idx_]))) {
        LOG_WARN("failed to assign partition", K(ret));
      } else {
        ++prefetch_idx_;
      }
    } else if (OB_FAIL(prefetch())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("prefetch failed", K(ret));
      }
    } else {
      if (prefetch_idx_ >= prefetch_partitions_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected prefetch idx", K(ret), K_(prefetch_idx), "partition_count", prefetch_partitions_.count());
      } else {
        if (OB_FAIL(partition.assign(prefetch_partitions_[prefetch_idx_]))) {
          LOG_WARN("failed to assign partition", K(ret));
        } else {
          ++prefetch_idx_;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition.filter(filters_))) {
        LOG_WARN("filter replica failed", K(partition), K(ret));
      }
    }
  }
  return ret;
}

int ObPTPartPartitionIterator::prefetch()
{
  int ret = OB_SUCCESS;
  uint64_t last_table_id = 0;
  int64_t last_partition_id = 0;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (prefetch_idx_ != prefetch_partitions_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prefetched partition infos have not been iterated to end",
        K_(prefetch_idx),
        "prefetch count",
        prefetch_partitions_.count(),
        K(ret));
  } else if (OB_ISNULL(pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt_operator is null", K(ret));
  } else {
    if (prefetch_partitions_.count() > 0) {
      const int64_t last_idx = prefetch_partitions_.count() - 1;
      last_table_id = prefetch_partitions_.at(last_idx).get_table_id();
      last_partition_id = prefetch_partitions_.at(last_idx).get_partition_id();
    }
    prefetch_idx_ = 0;
    prefetch_partitions_.reuse();
    const int64_t start = ObTimeUtility::current_time();
    if (OB_FAIL(pt_operator_->prefetch(pt_table_id_,
            pt_partition_id_,
            last_table_id,
            last_partition_id,
            prefetch_partitions_,
            need_fetch_faillist_))) {
      LOG_WARN("pt_operator prefetch failed",
          KT_(pt_table_id),
          K_(pt_partition_id),
          KT(last_table_id),
          K(last_partition_id),
          K(ret));
    } else if (prefetch_partitions_.count() <= 0) {
      ret = OB_ITER_END;
    }
    const int64_t cost = ObTimeUtility::current_time() - start;
    LOG_DEBUG("prefetch cost", K(cost));
  }
  return ret;
}

ObPartitionTableIdIterator::ObPartitionTableIdIterator()
    : inited_(false),
      schema_service_(NULL),
      tenant_iter_(),
      pt_tables_(),
      pt_table_id_(OB_INVALID_ID),
      pt_partition_id_(OB_INVALID_INDEX)
{}

ObPartitionTableIdIterator::~ObPartitionTableIdIterator()
{}

int ObPartitionTableIdIterator::init(ObMultiVersionSchemaService& schema_service)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(tenant_iter_.init(schema_service))) {
    LOG_WARN("tenant iterator init failed", K(ret));
  } else {
    pt_tables_[0] = OB_ALL_VIRTUAL_CORE_META_TABLE_TID;
    pt_tables_[1] = OB_ALL_CORE_TABLE_TID;
    pt_tables_[2] = OB_ALL_ROOT_TABLE_TID;
    pt_tables_[3] = OB_ALL_TENANT_META_TABLE_TID;
    schema_service_ = &schema_service;
    inited_ = true;
  }
  return ret;
}

int ObPartitionTableIdIterator::get_next_partition(uint64_t& pt_table_id, int64_t& pt_partition_id)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if ((OB_INVALID_ID != pt_table_id_ && OB_INVALID_INDEX == pt_partition_id_) ||
             (OB_INVALID_ID == pt_table_id_ && OB_INVALID_INDEX != pt_partition_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pt_table_id or pt_partition_id", K(ret), K(pt_table_id_), K(pt_partition_id_));
  } else if (OB_INVALID_ID == pt_table_id_ && OB_INVALID_INDEX == pt_partition_id_) {
    pt_table_id_ = combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_CORE_META_TABLE_TID);
    pt_partition_id_ = 0;
  } else {
    int64_t part_num = 0;
    if (OB_FAIL(get_part_num(pt_table_id_, part_num))) {
      LOG_WARN("get_part_num failed", K(pt_table_id_), K(ret));
    } else if (pt_partition_id_ < part_num - 1) {
      // @note: all inner tables are non range partitions,
      // no need to process for range partitions
      // since the range partition id are successively incremental
      pt_partition_id_++;
    } else {
      int64_t index = -1;
      for (int64_t i = 0; i < ARRAYSIZEOF(pt_tables_); ++i) {
        if (extract_pure_id(pt_tables_[i]) == extract_pure_id(pt_table_id_)) {
          index = i;
        }
      }
      if (index < 0 || index >= ARRAYSIZEOF(pt_tables_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid index", K(ret), K(index), "pt_tables_count", ARRAYSIZEOF(pt_tables_));
      } else {
        uint64_t tenant_id = OB_INVALID_TENANT_ID;
        if (index == ARRAYSIZEOF(pt_tables_) - 1) {
          if (OB_ALL_TENANT_META_TABLE_TID == extract_pure_id(pt_tables_[index])) {
            // for __all_tenant_meta_table,need to get the next tenant
            if (OB_FAIL(tenant_iter_.next(tenant_id))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("tenant iterate failed", K(ret));
              }
            } else if (OB_INVALID_TENANT_ID != tenant_id) {
              pt_table_id_ = combine_id(tenant_id, pt_tables_[index]);
              pt_partition_id_ = 0;
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("tenant_id is invalid", K(ret), K(tenant_id));
            }
          }
        } else {
          if (OB_ALL_TENANT_META_TABLE_TID != extract_pure_id(pt_tables_[index + 1])) {
            pt_table_id_ = combine_id(OB_SYS_TENANT_ID, pt_tables_[index + 1]);
            pt_partition_id_ = 0;
          } else {
            if (OB_FAIL(tenant_iter_.next(tenant_id))) {
              // at lease sys tenant is iterated out
              LOG_WARN("should have at least one tenant", K(ret));
            } else if (OB_INVALID_TENANT_ID != tenant_id) {
              pt_table_id_ = combine_id(tenant_id, pt_tables_[index + 1]);
              pt_partition_id_ = 0;
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("tenant_id is invalid", K(ret), K(tenant_id));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    pt_table_id = pt_table_id_;
    pt_partition_id = pt_partition_id_;
  }
  return ret;
}

int ObPartitionTableIdIterator::get_part_num(const uint64_t table_id, int64_t& part_num)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObSimpleTableSchemaV2* table = NULL;
  const uint64_t tenant_id = extract_tenant_id(table_id);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", KT(table_id), K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(guard.get_table_schema(table_id, table))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_id));
  } else if (NULL == table) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KT(table_id), K(ret));
  } else if (table->has_self_partition()) {
    part_num = table->get_all_part_num();
  } else {
    // for all_virtual_core_meta_table
    part_num = 1;
  }
  return ret;
}

ObFullPartitionTableIterator::ObFullPartitionTableIterator()
    : inited_(false), pt_operator_(NULL), part_iter_(), pt_part_iter_()
{}

ObFullPartitionTableIterator::~ObFullPartitionTableIterator()
{}

// iteration sequence:__all_virtual_core_meta_table
//                    -> __all_core_table
//                    -> __all_root_table
//                    -> __all_tenant_meta_table
int ObFullPartitionTableIterator::init(
    ObPartitionTableOperator& pt_operator, ObMultiVersionSchemaService& schema_service)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(pt_part_iter_.init(schema_service))) {
    LOG_WARN("pt_part_iterator init failed", K(ret));
  } else {
    pt_operator_ = &pt_operator;
    inited_ = true;
  }
  return ret;
}

int ObFullPartitionTableIterator::next(ObPartitionInfo& partition)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (!part_iter_.is_inited()) {
      if (OB_FAIL(next_partition())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("next_partition failed", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(part_iter_.next(partition))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("partition iterate failed", K(ret));
      } else {
        while (OB_ITER_END == ret) {
          if (OB_FAIL(next_partition())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("next_partition failed", K(ret));
            } else {
              break;
            }
          } else if (OB_FAIL(part_iter_.next(partition))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("partition iterate failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObFullPartitionTableIterator::next_partition()
{
  int ret = OB_SUCCESS;
  uint64_t pt_table_id = OB_INVALID_ID;
  int64_t pt_partition_id = OB_INVALID_INDEX;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(pt_part_iter_.get_next_partition(pt_table_id, pt_partition_id))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next partition", K(ret), K(pt_table_id), K(pt_partition_id));
    }
  } else if (OB_FAIL(part_iter_.init(*pt_operator_, pt_table_id, pt_partition_id))) {
    LOG_WARN("ObPTPartPartitionIterator init failed", KT(pt_table_id), K(pt_partition_id), K(ret));
  }
  return ret;
}

ObFullMetaTableIterator::ObFullMetaTableIterator()
    : inited_(false),
      pt_operator_(NULL),
      schema_service_(NULL),
      part_iter_(),
      tenant_iter_(),
      pt_table_(OB_INVALID_INDEX)
{}

ObFullMetaTableIterator::~ObFullMetaTableIterator()
{}

int ObFullMetaTableIterator::init(ObPartitionTableOperator& pt_operator, ObMultiVersionSchemaService& schema_service)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(tenant_iter_.init(schema_service))) {
    LOG_WARN("tenant iterator init failed", K(ret));
  } else {
    pt_table_ = OB_ALL_TENANT_META_TABLE_TID;
    pt_operator_ = &pt_operator;
    schema_service_ = &schema_service;
    inited_ = true;
  }
  return ret;
}

int ObFullMetaTableIterator::next(ObPartitionInfo& partition)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (!part_iter_.is_inited()) {
      if (OB_FAIL(next_partition())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("next_partition failed", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(part_iter_.next(partition))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("partition iterate failed", K(ret));
      } else {
        while (OB_ITER_END == ret) {
          if (OB_FAIL(next_partition())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("next_partition failed", K(ret));
            } else {
              break;
            }
          } else if (OB_FAIL(part_iter_.next(partition))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("partition iterate failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObFullMetaTableIterator::next_partition()
{
  int ret = OB_SUCCESS;
  uint64_t pt_table_id = OB_INVALID_ID;
  int64_t pt_partition_id = OB_INVALID_INDEX;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ALL_TENANT_META_TABLE_TID != pt_table_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt_table_ is invalid", K(ret), K(pt_table_));
  } else {
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    if (!part_iter_.is_inited()) {
      if (OB_ALL_TENANT_META_TABLE_TID == extract_pure_id(pt_table_)) {
        if (OB_FAIL(tenant_iter_.next(tenant_id))) {
          LOG_WARN("at least has one tenant", K(ret));
        } else if (OB_INVALID_TENANT_ID != tenant_id) {
          pt_table_id = combine_id(tenant_id, pt_table_);
          pt_partition_id = 0;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant_id is invalid", K(ret), K(tenant_id));
        }
      }

    } else {
      int64_t part_num = 0;
      if (OB_FAIL(get_part_num(part_iter_.get_pt_table_id(), part_num))) {
        LOG_WARN("get_part_num failed", "pt_table_id", part_iter_.get_pt_table_id(), K(ret));
      } else if (part_iter_.get_pt_partition_id() < part_num - 1) {
        pt_table_id = part_iter_.get_pt_table_id();
        // @note: all inner tables are non range partitions,
        // no need to process for range partitions
        // since the range partition id are successively incremental
        pt_partition_id = part_iter_.get_pt_partition_id() + 1;
      } else {
        if (OB_ALL_TENANT_META_TABLE_TID == extract_pure_id(pt_table_)) {
          // for __all_tenant_meta_table,need to iterate the next tenant
          if (OB_FAIL(tenant_iter_.next(tenant_id))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("tenant iterate failed", K(ret));
            }
          } else if (OB_INVALID_TENANT_ID != tenant_id) {
            pt_table_id = combine_id(tenant_id, pt_table_);
            pt_partition_id = 0;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tenant_id is invalid", K(ret), K(tenant_id));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(part_iter_.init(*pt_operator_, pt_table_id, pt_partition_id))) {
      LOG_WARN("ObPTPartPartitionIterator init failed", KT(pt_table_id), K(pt_partition_id), K(ret));
    }
  }
  return ret;
}

int ObFullMetaTableIterator::get_part_num(const uint64_t table_id, int64_t& part_num)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObTableSchema* table = NULL;
  const uint64_t tenant_id = extract_tenant_id(table_id);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", KT(table_id), K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(guard.get_table_schema(table_id, table))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_id));
  } else if (NULL == table) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KT(table_id), K(ret));
  } else if (table->has_self_partition()) {
    part_num = table->get_all_part_num();
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
