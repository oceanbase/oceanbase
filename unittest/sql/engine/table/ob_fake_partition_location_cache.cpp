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

#include "ob_fake_partition_location_cache.h"
using namespace oceanbase::common;
namespace oceanbase {
namespace share {

ObFakePartitionLocationCache::ObFakePartitionLocationCache()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = partition_loc_map_.create(10000, ObModIds::OB_SQL_EXECUTOR))) {
    SQL_ENG_LOG(WARN, "fail to create location map", K(ret));
  }
}

ObFakePartitionLocationCache::~ObFakePartitionLocationCache()
{}

int ObFakePartitionLocationCache::get(const uint64_t table_id, const int64_t partition_id,
    ObPartitionLocation& location, const int64_t expire_renew_time, bool&)
{
  UNUSED(expire_renew_time);
  int ret = OB_SUCCESS;
  ObFakePartitionKey key;
  key.table_id_ = table_id;
  key.partition_id_ = partition_id;
  if (hash::HASH_EXIST == (ret = partition_loc_map_.get(key, location))) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObFakePartitionLocationCache::add_location(ObFakePartitionKey key, ObPartitionLocation location)
{
  int ret = OB_SUCCESS;
  location.set_table_id(key.table_id_);
  location.set_partition_id(key.partition_id_);
  if (location.get_partition_cnt() <= key.partition_id_) {
    location.set_partition_cnt(key.partition_id_ + 1);
  }
  location.is_valid();
  if (hash::HASH_INSERT_SUCC == (ret = partition_loc_map_.set(key, location))) {
    ret = OB_SUCCESS;
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
