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

#include "ob_location_adapter.h"

#include "share/partition_table/ob_partition_location_cache.h"
#include "ob_trans_ctx_mgr.h"

namespace oceanbase {
namespace transaction {

using namespace common;
using namespace share;

ObLocationAdapter::ObLocationAdapter() : is_inited_(false), location_cache_(NULL), schema_service_(NULL)
{
  reset_statistics();
}

int ObLocationAdapter::init(
    ObIPartitionLocationCache* location_cache, share::schema::ObMultiVersionSchemaService* schema_service)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ob location adapter inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(location_cache) || OB_ISNULL(schema_service)) {
    TRANS_LOG(WARN, "invalid argument", KP(location_cache), KP(schema_service));
    ret = OB_INVALID_ARGUMENT;
  } else {
    location_cache_ = location_cache;
    schema_service_ = schema_service;
    is_inited_ = true;
    TRANS_LOG(INFO, "ob location cache adapter inited success");
  }

  return ret;
}

void ObLocationAdapter::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    TRANS_LOG(INFO, "ob location cache adapter destroyed");
  }
}

void ObLocationAdapter::reset_statistics()
{
  renew_access_ = 0;
  total_access_ = 0;
  error_count_ = 0;
}

void ObLocationAdapter::statistics()
{
  if (REACH_TIME_INTERVAL(TRANS_ACCESS_STAT_INTERVAL)) {
    TRANS_LOG(INFO,
        "location adapter statistics",
        K_(renew_access),
        K_(total_access),
        K_(error_count),
        "renew_rate",
        static_cast<float>(renew_access_) / static_cast<float>(total_access_ + 1));
    reset_statistics();
  }
}

int ObLocationAdapter::get_strong_leader_(const ObPartitionKey& partition, const bool is_sync, ObAddr& server)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ob location adapter not inited");
    ret = OB_NOT_INIT;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_sync) {
    if (OB_FAIL(location_cache_->get_strong_leader(partition, server))) {
      TRANS_LOG(WARN, "get leader from locatition cache error", K(ret), K(partition));
      const bool force_renew = true;
      if (OB_SUCCESS != (ret = location_cache_->get_strong_leader(partition, server, force_renew))) {
        TRANS_LOG(WARN, "get leader from locatition cache error again", KR(ret), K(partition), K(force_renew));
      }
      renew_access_++;
    }
  } else {
    if (OB_FAIL(location_cache_->nonblock_get_strong_leader(partition, server))) {
      TRANS_LOG(DEBUG, "nonblock get leader from locatition cache error", K(ret), K(partition));
      int tmp_ret = OB_SUCCESS;
      // Failed to obtain the leader asynchronously,
      // the location cache will not be cleared temporarily
      const int64_t expire_renew_time = 0;
      if (OB_SUCCESS != (tmp_ret = location_cache_->nonblock_renew(partition, expire_renew_time))) {
        TRANS_LOG(WARN, "nonblock renew from location cache error", "ret", tmp_ret, K(partition));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!server.is_valid()) {
      TRANS_LOG(WARN, "invalid server", K(partition), K(server));
      ret = OB_ERR_UNEXPECTED;
    }
  }
  // statistics
  ++total_access_;
  if (OB_FAIL(ret)) {
    ++error_count_;
  }
  statistics();

  return ret;
}

/*
 * if get_strong_leader() and nonblock_get_strong_leader() error, both renew location cache
 */

int ObLocationAdapter::get_strong_leader(const ObPartitionKey& partition, ObAddr& server)
{
  int ret = OB_SUCCESS;
  const bool is_sync = true;
#ifdef TRANS_ERROR
  int64_t random = ObRandom::rand(0, 100);
  static int64_t total_alloc_cnt = 0;
  static int64_t random_cnt = 0;
  ++total_alloc_cnt;
  if (0 == random % 50) {
    ret = OB_LOCATION_NOT_EXIST;
    ++random_cnt;
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(INFO, "get error for random", K(partition), K(total_alloc_cnt), K(random_cnt));
    }
    return ret;
  }
#endif

  if (!is_inited_) {
    TRANS_LOG(WARN, "ob location adapter not inited");
    ret = OB_NOT_INIT;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = get_strong_leader_(partition, is_sync, server);
  }

  return ret;
}

int ObLocationAdapter::nonblock_get_strong_leader(const ObPartitionKey& partition, ObAddr& server)
{
  int ret = OB_SUCCESS;
  const bool is_sync = false;
#ifdef TRANS_ERROR
  int64_t random = ObRandom::rand(0, 100);
  static int64_t total_alloc_cnt = 0;
  static int64_t random_cnt = 0;
  ++total_alloc_cnt;
  if (0 == random % 50) {
    ret = OB_LOCATION_NOT_EXIST;
    ++random_cnt;
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(INFO, "get error for random", K(partition), K(total_alloc_cnt), K(random_cnt));
    }
    return ret;
  }
#endif

  if (!is_inited_) {
    TRANS_LOG(WARN, "ob location adapter not inited");
    ret = OB_NOT_INIT;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = get_strong_leader_(partition, is_sync, server);
  }

  return ret;
}

int ObLocationAdapter::nonblock_renew(const ObPartitionKey& partition, const int64_t expire_renew_time)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ob location adapter not inited");
    ret = OB_NOT_INIT;
  } else if (!partition.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (SCHE_PARTITION_ID == partition) {
    // do nothing
  } else if (OB_SUCCESS != (ret = location_cache_->nonblock_renew(partition, expire_renew_time))) {
    TRANS_LOG(WARN, "nonblock renew error", KR(ret), K(partition), K(expire_renew_time));
  } else {
    ++renew_access_;
  }

  return ret;
}

int ObLocationAdapter::nonblock_get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location)
{
  int ret = OB_SUCCESS;
#ifdef TRANS_ERROR
  int64_t random = ObRandom::rand(0, 100);
  static int64_t total_alloc_cnt = 0;
  static int64_t random_cnt = 0;
  ++total_alloc_cnt;
  if (0 == random % 50) {
    ret = OB_LOCATION_NOT_EXIST;
    ++random_cnt;
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(INFO, "get error for random", K(table_id), K(partition_id), K(total_alloc_cnt), K(random_cnt));
    }
    return ret;
  }
#endif
  if (!is_inited_) {
    TRANS_LOG(WARN, "ob location adapter not inited");
    ret = OB_NOT_INIT;
  } else if (0 == table_id || 0 > partition_id) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(table_id), K(partition_id));
  } else if (OB_FAIL(location_cache_->nonblock_get(table_id, partition_id, location))) {
    TRANS_LOG(WARN, "nonblock get failed", KR(ret), K(table_id), K(partition_id));
  } else {
    // do nothing
  }
  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
