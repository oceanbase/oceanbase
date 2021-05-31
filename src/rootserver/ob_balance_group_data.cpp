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

#define USING_LOG_PREFIX RS_LB

#include "ob_balance_group_data.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "common/ob_zone.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_primary_zone_util.h"
#include "observer/ob_server_struct.h"
#include "ob_balance_info.h"
#include "ob_balancer_interface.h"
#include "ob_root_service.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::rootserver::balancer;

SquareIdMap::SquareIdMap(ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder, ObIAllocator& allocator)
    : BalanceGroupBox(),
      inited_(false),
      schema_guard_(schema_guard),
      stat_finder_(stat_finder),
      allocator_(allocator),
      map_(NULL),
      tenant_id_(common::OB_INVALID_ID),
      row_size_(0),
      col_size_(0),
      is_valid_(true),
      ignore_leader_balance_(true),
      primary_zone_(),
      map_id_(0)
{
  tenant_id_ = stat_finder_.get_tenant_id();
  enable_dump_ = ObServerConfig::get_instance().enable_rich_error_msg;
}

SquareIdMapItem* SquareIdMap::alloc_id_map_item_array(ObIAllocator& allocator, int64_t size)
{
  SquareIdMapItem* arr = nullptr;
  arr = static_cast<SquareIdMapItem*>(allocator.alloc(size * sizeof(SquareIdMapItem)));
  if (OB_UNLIKELY(nullptr == arr)) {
    LOG_WARN("fail to alloc memory", KP(arr));
  } else {
    for (int i = 0; i < size; ++i) {
      void* ptr = reinterpret_cast<void*>(arr + i);
      (void)new (ptr) SquareIdMapItem();
    }
  }
  return arr;
}

int SquareIdMap::update_leader_balance_info(
    const common::ObZone& zone, const HashIndexCollection& hash_index_collection)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<share::ObRawPrimaryZoneUtil::ZoneScore, MAX_ZONE_NUM> zone_score;
  common::ObSEArray<share::ObRawPrimaryZoneUtil::RegionScore, MAX_ZONE_NUM> region_score;  // DUMMY
  ObRootService* root_service = GCTX.root_service_;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == root_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root service ptr is null", K(ret));
  } else {
    share::ObRawPrimaryZoneUtil builder(root_service->get_zone_mgr());
    if (OB_FAIL(builder.build(primary_zone_, zone_score, region_score))) {
      LOG_WARN("fail to build zone score array", K(ret));
    } else {
      for (iterator iter = begin(); OB_SUCC(ret) && iter != end(); ++iter) {
        HashIndexMapItem hash_index_item;
        if (!iter->pkey_.is_valid()) {
          // invalid pkey
        } else {
          int tmp_ret = hash_index_collection.get_partition_index(iter->pkey_, hash_index_item);
          if (OB_ENTRY_NOT_EXIST == tmp_ret) {
            // by pass
          } else if (OB_SUCCESS == tmp_ret) {
            ret = update_item_leader_balance_info(zone, *iter, zone_score, region_score, hash_index_item);
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get partition index unexpected", K(ret), "pkey", iter->pkey_);
          }
        }
      }
    }
  }
  return ret;
}

int SquareIdMap::update_item_leader_balance_info(const common::ObZone& zone, SquareIdMapItem& id_map_item,
    const common::ObIArray<share::ObRawPrimaryZoneUtil::ZoneScore>& zone_score,
    const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score,
    const HashIndexMapItem& hash_index_item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    UNUSED(region_score);
    common::ObZone balance_group_zone;
    common::ObSEArray<common::ObZone, MAX_ZONE_NUM> high_priority_zone_array;
    if (OB_FAIL(ObRawPrimaryZoneUtil::generate_high_priority_zone_array(zone_score, high_priority_zone_array))) {
      LOG_WARN("fail to generate priority zone array", K(ret));
    } else if (OB_FAIL(hash_index_item.get_balance_group_zone(high_priority_zone_array, balance_group_zone))) {
      LOG_WARN("fail to get balance group zone", K(ret));
    } else if (zone == balance_group_zone) {
      id_map_item.designated_leader_ = true;
    } else {
      id_map_item.designated_leader_ = false;
    }
  }
  return ret;
}

int SquareIdMap::set(const int64_t row, const int64_t col, const common::ObPartitionKey& pkey, const int64_t all_tg_idx,
    const int64_t all_pg_idx, const uint64_t tablegroup_id, const uint64_t table_id, const int64_t part_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (row >= row_size_ || col >= col_size_ || row < 0 || col < 0) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("row or col out of range", K(row), K(col), K_(row_size), K_(col_size), K(ret));
  } else if ((OB_INVALID_INDEX == all_tg_idx && OB_INVALID_INDEX == part_idx) || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index",
        K(row),
        K(col),
        K_(row_size),
        K_(col_size),
        K(pkey),
        K(tablegroup_id),
        K(table_id),
        K(all_tg_idx),
        K(part_idx),
        K(ret));
  } else {
    int64_t map_idx = col + row * col_size_;
    SquareIdMapItem& item = map_[map_idx];
    item.pkey_ = pkey;
    item.all_tg_idx_ = all_tg_idx;
    item.all_pg_idx_ = all_pg_idx;
    item.tablegroup_id_ = tablegroup_id;
    item.table_id_ = table_id;
    item.part_idx_ = part_idx;
  }
  return ret;
}

int SquareIdMap::get(
    const int64_t row, const int64_t col, common::ObPartitionKey& pkey, int64_t& all_tg_idx, int64_t& part_idx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (row >= row_size_ || col >= col_size_ || row < 0 || col < 0) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("row or col out of range", K(row), K(col), K_(row_size), K_(col_size), K(ret));
  } else {
    int64_t map_idx = col + row * col_size_;
    SquareIdMapItem& item = map_[map_idx];
    pkey = item.pkey_;
    all_tg_idx = item.all_tg_idx_;
    part_idx = item.part_idx_;
  }
  return ret;
}

int SquareIdMap::get(const int64_t row, const int64_t col, Item*& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (row >= row_size_ || col >= col_size_ || row < 0 || col < 0) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("row or col out of range", K(row), K(col), K_(row_size), K_(col_size), K(ret));
  } else {
    int64_t map_idx = col + row * col_size_;
    item = &map_[map_idx];
  }
  return ret;
}

int SquareIdMap::get(const int64_t row, const int64_t col, Item& item) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (row >= row_size_ || col >= col_size_ || row < 0 || col < 0) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("row or col out of range", K(row), K(col), K_(row_size), K_(col_size), K(ret));
  } else {
    int64_t map_idx = col + row * col_size_;
    item = map_[map_idx];
  }
  return ret;
}

int SquareIdMap::get(const int64_t map_idx, Item& item) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (map_idx >= size() || map_idx < 0) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("row or col out of range", K(map_idx), K_(row_size), K_(col_size), K(ret));
  } else {
    item = map_[map_idx];
  }
  return ret;
}

int SquareIdMap::get_all_tg_idx(uint64_t tablegroup_id, uint64_t table_id, int64_t& all_tg_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(stat_finder_.get_all_tg_idx(tablegroup_id, table_id, all_tg_idx))) {
    LOG_WARN("fail get_all_tg_idx", K(tablegroup_id), K(table_id), K(ret));
  }
  return ret;
}

int SquareIdMap::assign_unit(const IUnitProvider& unit_provider)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    uint64_t unit_id = OB_INVALID_ID;
    for (int64_t row = 0; OB_SUCC(ret) && row < row_size_; ++row) {
      for (int64_t col = 0; OB_SUCC(ret) && col < col_size_; ++col) {
        int64_t map_idx = col + row * col_size_;
        SquareIdMap::Item& item = map_[map_idx];
        // Take the unit where the primary table is located as the reference standard
        // Assuming that the load balancing algorithm believes that the primary table does not need to be migrated,
        // and the replicas of the rest of the tables in the table group are not on the same unit,
        // the TableGroupCoordinator will help with scheduling, and no load balancing is required
        if (OB_FAIL(unit_provider.find_unit(item.all_tg_idx_, item.part_idx_, unit_id))) {
          LOG_WARN("fail find_unit", K(*this), K(item), K(row), K(col), K(ret));
        } else {
          item.unit_id_ = item.dest_unit_id_ = unit_id;
          LOG_DEBUG("assign_unit", K(unit_id), K(item), K(row), K(col));
        }
      }
    }
  }
  return ret;
}

bool SquareIdMap::need_balance() const
{
  bool need_balance = false;
  for (int64_t row = 0; row < row_size_; ++row) {
    for (int64_t col = 0; col < col_size_; ++col) {
      int64_t map_idx = col + row * col_size_;
      SquareIdMap::Item& item = map_[map_idx];
      if (item.unit_id_ != item.dest_unit_id_) {
        need_balance = true;
        break;
      }
    }
  }
  return need_balance;
}

int SquareIdMap::dump(const char* dump_banner, bool force_dump)
{
  int ret = OB_SUCCESS;
  UNUSED(dump_banner);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (enable_dump_ || force_dump) {
    /*
    const int64_t buf_size = 10250000;
    char buf[buf_size];
    int64_t pos = 0;
    _OB_LOG(INFO, "%s\n", dump_banner);
    databuff_printf(buf, buf_size, pos, "\n");
    for (int64_t row = 0; row < row_size_; row++) {
      //_OB_LOG(INFO, "row %ld\n", row);
      for (int64_t col = 0; col < col_size_; ++col) {
        int64_t all_tg_idx = OB_INVALID_ID;
        int64_t part_idx = OB_INVALID_INDEX;
        common::ObPartitionKey pkey;
        if (OB_FAIL(get(row, col, pkey, all_tg_idx, part_idx))) {
          LOG_WARN("fail get item from map", K(row), K(col), K(ret));
        } else {
          databuff_printf(buf, buf_size, pos, "<%ld,%ld> ", all_tg_idx, part_idx);
        }
      }
      databuff_printf(buf, buf_size, pos, "\n");
    }
    _OB_LOG(INFO, "%s\n=============dump finish===========\n", buf);
    LOG_INFO("dump", "info", *this);
    */
  }
  return ret;
}

int SquareIdMap::dump2(const char* dump_banner, bool force_dump)
{
  int ret = OB_SUCCESS;
  UNUSED(dump_banner);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (enable_dump_ || force_dump) {
    /*
    const int64_t buf_size = 10250000;
    char buf[buf_size];
    int64_t pos = 0;
    databuff_printf(buf, buf_size, pos, "\n");
    for (int64_t row = 0; row < row_size_; row++) {
      //_OB_LOG(INFO, "row %ld\n", row);
      for (int64_t col = 0; col < col_size_; ++col) {
        Item item;
        if (OB_FAIL(get(row, col, item))) {
          LOG_WARN("fail get item from map", K(row), K(col), K(ret));
        } else {
          databuff_printf(buf, buf_size, pos, "<pkey:%lu,%ld; is_leader:%d; %ld, %lu, %ld; %lu, %lu> ",
                          item.pkey_.get_table_id(),
                          item.pkey_.get_partition_id(),
                          item.designated_leader_,
                          item.all_tg_idx_,
                          item.table_id_,
                          item.part_idx_,
                          item.unit_id_,
                          item.dest_unit_id_);
        }
      }
      databuff_printf(buf, buf_size, pos, "\n");
    }
    LOG_INFO("dump", "info", *this);
    _OB_LOG(INFO, "\n=============dump begin===========\n%s\n%s\n%s\n=============dump finish===========\n",
            dump_banner, get_comment(), buf);
    */
  }
  return ret;
}

int SquareIdMap::dump_unit(const char* dump_banner)
{
  int ret = OB_SUCCESS;
  UNUSED(dump_banner);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else {
    /*
    const int64_t buf_size = 10250000;
    char buf[buf_size];
    int64_t pos = 0;
    _OB_LOG(INFO, "%s\n", dump_banner);
    databuff_printf(buf, buf_size, pos, "before:\n");
    for (int64_t row = 0; row < row_size_; row++) {
      //_OB_LOG(INFO, "row %ld\n", row);
      for (int64_t col = 0; col < col_size_; ++col) {
        Item item;
        if (OB_FAIL(get(row, col, item))) {
          LOG_WARN("fail get item from map", K(row), K(col), K(ret));
        } else {
          databuff_printf(buf, buf_size, pos, "%04ld ", item.unit_id_);

        }
      }
      databuff_printf(buf, buf_size, pos, "\n");
    }
    databuff_printf(buf, buf_size, pos, "after:\n");
    for (int64_t row = 0; row < row_size_; row++) {
      for (int64_t col = 0; col < col_size_; ++col) {
        Item item;
        if (OB_FAIL(get(row, col, item))) {
          LOG_WARN("fail get item from map", K(row), K(col), K(ret));
        } else {
          databuff_printf(buf, buf_size, pos, "%04ld ", item.dest_unit_id_);
        }
      }
      databuff_printf(buf, buf_size, pos, "\n");
    }
    _OB_LOG(INFO, "%s\n=============dump finish===========\n", buf);
    LOG_INFO("dump", "info", *this);
    */
  }
  return ret;
}

int SquareIdMap::init(const int64_t row, const int64_t col)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(row <= 0 || col <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row), K(col));
  } else {
    int64_t map_size = row * col;
    if (OB_UNLIKELY(nullptr == (map_ = alloc_id_map_item_array(allocator_, map_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), K(map_size));
    } else {
      row_size_ = row;
      col_size_ = col;
      inited_ = true;
    }
  }
  return ret;
}

int SquareIdMap::set_item(const BalanceGroupBoxItem& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(set(item.row_,
                 item.col_,
                 item.pkey_,
                 item.all_tg_idx_,
                 item.all_pg_idx_,
                 item.tablegroup_id_,
                 item.table_id_,
                 item.part_idx_))) {
    LOG_WARN("fail to set element", K(ret));
  }
  return ret;
}

const char* ShardGroupIdMap::get_comment() const
{
  return balancer::TG_PG_SHARD_BALANCE;
}

BalanceGroupType ShardGroupIdMap::get_map_type() const
{
  return SHARD_GROUP_BALANCE_GROUP;
}

const char* ShardPartitionIdMap::get_comment() const
{
  return balancer::TG_PG_SHARD_PARTITION_BALANCE;
}

BalanceGroupType ShardPartitionIdMap::get_map_type() const
{
  return SHARD_PARTITION_BALANCE_GROUP;
}

const char* TableGroupIdMap::get_comment() const
{
  return balancer::TG_PG_GROUP_BALANCE;
}

BalanceGroupType TableGroupIdMap::get_map_type() const
{
  return TABLE_GROUP_BALANCE_GROUP;
}

const char* PartitionTableIdMap::get_comment() const
{
  return balancer::TG_PG_TABLE_BALANCE;
}

BalanceGroupType PartitionTableIdMap::get_map_type() const
{
  return PARTITION_TABLE_BALANCE_GROUP;
}

const char* NonPartitionTableIdMap::get_comment() const
{
  return balancer::TG_NON_PARTITION_TABLE_BALANCE;
}

BalanceGroupType NonPartitionTableIdMap::get_map_type() const
{
  return NON_PARTITION_TABLE_BALANCE_GROUP;
}

int SquareIdMapCollection::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
  } else {
    inited_ = true;
  }
  return ret;
}

int SquareIdMapCollection::collect_box(const BalanceGroupBox* balance_group_box)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == balance_group_box)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    SquareIdMap* square_id_map = nullptr;
    if (SQUARE_MAP_TYPE != balance_group_box->get_container_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), "container_type", balance_group_box->get_container_type());
    } else if (nullptr == (square_id_map = (SquareIdMap*)(balance_group_box))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("square id map ptr is null", K(ret));
    } else if (OB_FAIL(square_id_map_array_.push_back(square_id_map))) {
      LOG_WARN("fail to push back balance group box", K(ret));
    }
  }
  return ret;
}

int SquareIdMapCollection::calc_leader_balance_statistic(
    const common::ObZone& zone, const HashIndexCollection& hash_index_collection)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < square_id_map_array_.count(); ++i) {
      SquareIdMap* id_map = square_id_map_array_.at(i);
      if (OB_UNLIKELY(nullptr == id_map)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("id map ptr is null", K(ret));
      } else if (id_map->ignore_leader_balance()) {
        // no need leader balance, do not update related information
      } else if (OB_FAIL(id_map->update_leader_balance_info(zone, hash_index_collection))) {
        LOG_WARN("fail to update leader balance info", K(ret));
      }
    }
  }
  return ret;
}

int HashIndexMap::init(const int64_t index_map_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (index_map_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index_map_size));
  } else if (OB_FAIL(index_map_.create(index_map_size, ObModIds::OB_HASH_BUCKET_BALANCER))) {
    LOG_WARN("fail to create index map", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int HashIndexMap::set_item(const BalanceGroupBoxItem& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    HashIndexMapItem this_item(item.in_group_index_,
        item.group_count_,
        item.group_id_,
        item.balance_group_type_,
        item.pkey_.get_tenant_id(),
        item.balance_group_id_);
    const common::ObPartitionKey& pkey = item.pkey_;
    const int32_t overwrite = 0;
    if (OB_FAIL(index_map_.set_refactored(pkey, this_item, overwrite))) {
      LOG_WARN("fail to set index map", K(ret));
    }
  }
  return ret;
}

int HashIndexCollection::init(const int64_t item_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (item_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(index_map_.create(2 * item_size, ObModIds::OB_HASH_BUCKET_BALANCER))) {
    LOG_WARN("fail to create index map", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int HashIndexCollection::assign(const HashIndexCollection& that)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    index_map_.reuse();
    for (IndexMap::const_iterator iter = that.index_map_.begin(); OB_SUCC(ret) && iter != that.index_map_.end();
         ++iter) {
      const common::ObPartitionKey& pkey = iter->first;
      const HashIndexMapItem& item = iter->second;
      const int32_t overwrite = 0;
      if (OB_FAIL(index_map_.set_refactored(pkey, item, overwrite))) {
        LOG_WARN("fail to set index map item", K(ret));
      }
    }
  }
  return ret;
}

int HashIndexCollection::collect_box(const BalanceGroupBox* balance_group_box)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == balance_group_box)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    HashIndexMap* hash_index_map = nullptr;
    if (HASH_INDEX_TYPE != balance_group_box->get_container_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), "container_type", balance_group_box->get_container_type());
    } else if (nullptr == (hash_index_map = (HashIndexMap*)(balance_group_box))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hash index map ptr is null", K(ret));
    } else {
      for (IndexMap::const_iterator iter = hash_index_map->index_map_.begin();
           OB_SUCC(ret) && iter != hash_index_map->index_map_.end();
           ++iter) {
        const common::ObPartitionKey& pkey = iter->first;
        const HashIndexMapItem& item = iter->second;
        const int32_t overwrite = 0;
        if (OB_FAIL(index_map_.set_refactored(pkey, item, overwrite))) {
          LOG_WARN("fail to set index map item", K(ret));
        }
      }
    }
  }
  return ret;
}

int HashIndexCollection::get_partition_index(
    const common::ObPartitionKey& pkey, HashIndexMapItem& hash_index_item) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    HashIndexMapItem my_index;
    int tmp_ret = index_map_.get_refactored(pkey, my_index);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_SUCCESS == tmp_ret) {
      hash_index_item = my_index;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get shardgroup index", K(ret), K(pkey));
    }
  }
  return ret;
}
