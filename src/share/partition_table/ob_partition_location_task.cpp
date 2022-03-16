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

#include "lib/time/ob_time_utility.h"
#include "share/ob_errno.h"
#include "share/ob_task_define.h"
#include "share/partition_table/ob_partition_location_task.h"

namespace oceanbase {
using namespace common;
namespace share {

void TSILocationRateLimit::reset()
{
  cnt_ = 0;
  start_ts_ = OB_INVALID_TIMESTAMP;
}

int64_t TSILocationRateLimit::calc_wait_ts(const int64_t cnt, const int64_t exec_ts, const int64_t frequency)
{
  int64_t wait_ts = 0;
  int64_t current_ts = ObTimeUtility::current_time();
  if (current_ts - start_ts_ >= ONE_SECOND_US) {  // init or >= 1s
    cnt_ = cnt;
    start_ts_ = current_ts - exec_ts;
  } else {
    cnt_ += cnt;
  }
  if (cnt_ > frequency) {
    wait_ts = cnt_ / (double)frequency * ONE_SECOND_US - (current_ts - start_ts_);
  }
  return wait_ts > 0 ? wait_ts : 0;
}

void TSILocationStatistics::reset()
{
  suc_cnt_ = 0;
  fail_cnt_ = 0;
  total_exec_us_ = 0;
  total_wait_us_ = 0;
}

int64_t TSILocationStatistics::get_total_cnt() const
{
  return suc_cnt_ + fail_cnt_;
}

void TSILocationStatistics::calc(const int ret, const int64_t exec_us, const int64_t wait_us, const int64_t cnt)
{
  total_exec_us_ += static_cast<uint64_t>(exec_us);
  total_wait_us_ += static_cast<uint64_t>(wait_us);
  if (OB_SUCCESS == ret) {
    suc_cnt_ += cnt;
  } else {
    fail_cnt_ += cnt;
  }
}

int ObPartitionBroadcastTask::init(
    const uint64_t table_id, const int64_t partition_id, const int64_t partition_cnt, const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  table_id_ = table_id;
  partition_id_ = partition_id;
  partition_cnt_ = partition_cnt;
  timestamp_ = timestamp;
  return ret;
}

void ObPartitionBroadcastTask::reset()
{
  table_id_ = OB_INVALID_ID;
  partition_id_ = OB_INVALID_ID;
  partition_cnt_ = OB_INVALID_ID;
  timestamp_ = OB_INVALID_TIMESTAMP;
}

bool ObPartitionBroadcastTask::is_valid() const
{
  return OB_INVALID_ID != table_id_ && OB_INVALID_ID != partition_id_ && OB_INVALID_ID != partition_cnt_ &&
         OB_INVALID_TIMESTAMP != timestamp_;
}

int ObPartitionBroadcastTask::assign(const ObPartitionBroadcastTask& other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    table_id_ = other.table_id_;
    partition_id_ = other.partition_id_;
    partition_cnt_ = other.partition_cnt_;
    timestamp_ = other.timestamp_;
  }
  return ret;
}

bool ObPartitionBroadcastTask::need_process_alone() const
{
  return false;
}

int64_t ObPartitionBroadcastTask::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
  hash_val = murmurhash(&partition_id_, sizeof(partition_id_), hash_val);
  hash_val = murmurhash(&partition_cnt_, sizeof(partition_cnt_), hash_val);
  return hash_val;
}

bool ObPartitionBroadcastTask::operator==(const ObPartitionBroadcastTask& other) const
{
  bool equal = false;
  if (!is_valid() || !other.is_valid()) {
    LOG_WARN("invalid argument", "self", *this, K(other));
  } else if (this == &other) {
    equal = true;
  } else {
    equal = (table_id_ == other.table_id_ && partition_id_ == other.partition_id_ &&
             partition_cnt_ == other.partition_cnt_);
  }
  return equal;
}

bool ObPartitionBroadcastTask::compare_without_version(const ObPartitionBroadcastTask& other) const
{
  return (*this == other);
}

uint64_t ObPartitionBroadcastTask::get_group_id() const
{
  return extract_tenant_id(table_id_);
}

bool ObPartitionBroadcastTask::is_barrier() const
{
  return false;
}

bool ObPartitionBroadcastTask::need_assign_when_equal() const
{
  return true;
}

int ObPartitionBroadcastTask::assign_when_equal(const ObPartitionBroadcastTask& other)
{
  int ret = OB_SUCCESS;
  if (*this == other) {
    if (other.timestamp_ > timestamp_) {
      timestamp_ = other.timestamp_;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task should be equal", KR(ret), KPC(this), K(other));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObPartitionBroadcastTask, table_id_, partition_id_, timestamp_);

int ObPartitionUpdateTask::init(const uint64_t table_id, const int64_t partition_id, const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  table_id_ = table_id;
  partition_id_ = partition_id;
  timestamp_ = timestamp;
  return ret;
}

void ObPartitionUpdateTask::reset()
{
  table_id_ = OB_INVALID_ID;
  partition_id_ = OB_INVALID_ID;
  timestamp_ = OB_INVALID_TIMESTAMP;
}

bool ObPartitionUpdateTask::is_valid() const
{
  return OB_INVALID_ID != table_id_ && OB_INVALID_ID != partition_id_ && OB_INVALID_TIMESTAMP != timestamp_;
}

int ObPartitionUpdateTask::assign(const ObPartitionUpdateTask& other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    table_id_ = other.table_id_;
    partition_id_ = other.partition_id_;
    timestamp_ = other.timestamp_;
  }
  return ret;
}

bool ObPartitionUpdateTask::need_process_alone() const
{
  return false;
}

int64_t ObPartitionUpdateTask::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
  hash_val = murmurhash(&partition_id_, sizeof(partition_id_), hash_val);
  return hash_val;
}

bool ObPartitionUpdateTask::operator==(const ObPartitionUpdateTask& other) const
{
  bool equal = false;
  if (!is_valid() || !other.is_valid()) {
    LOG_WARN("invalid argument", "self", *this, K(other));
  } else if (this == &other) {
    equal = true;
  } else {
    equal = (table_id_ == other.table_id_ && partition_id_ == other.partition_id_);
  }
  return equal;
}

bool ObPartitionUpdateTask::compare_without_version(const ObPartitionUpdateTask& other) const
{
  return (*this == other);
}

uint64_t ObPartitionUpdateTask::get_group_id() const
{
  return extract_tenant_id(table_id_);
}

bool ObPartitionUpdateTask::is_barrier() const
{
  return false;
}

bool ObPartitionUpdateTask::need_assign_when_equal() const
{
  return true;
}

int ObPartitionUpdateTask::assign_when_equal(const ObPartitionUpdateTask& other)
{
  int ret = OB_SUCCESS;
  if (*this == other) {
    if (other.timestamp_ > timestamp_) {
      timestamp_ = other.timestamp_;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task should be equal", KR(ret), KPC(this), K(other));
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
