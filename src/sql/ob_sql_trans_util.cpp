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

#define USING_LOG_PREFIX SQL
#include "sql/ob_sql_trans_util.h"
#include "sql/session/ob_sql_session_info.h"
namespace oceanbase {
using namespace common;
namespace sql {
OB_SERIALIZE_MEMBER(TransResult, part_epoch_list_, response_partitions_, total_partitions_, max_sql_no_);

void TransResult::reset()
{
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  total_partitions_.reset();
  response_partitions_.reset();
  part_epoch_list_.reset();
  is_incomplete_ = false;
  max_sql_no_ = 0;
}

int TransResult::assign(const TransResult& other)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  OZ(total_partitions_.assign(other.total_partitions_));
  OZ(part_epoch_list_.assign(other.part_epoch_list_));
  OZ(response_partitions_.assign(other.response_partitions_));
  OX(is_incomplete_ = other.is_incomplete_);
  OX(max_sql_no_ = other.max_sql_no_);
  return ret;
}

int TransResult::merge_result(const TransResult& other)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  OZ(append_array_no_dup(total_partitions_, other.total_partitions_), other.total_partitions_);
  OZ(append_array_no_dup(part_epoch_list_, other.part_epoch_list_), other.part_epoch_list_);
  OZ(append_array_no_dup(response_partitions_, other.response_partitions_), other.response_partitions_);
  if (other.is_incomplete()) {
    OX(set_incomplete());
  }
  int64_t other_max_sql_no = other.max_sql_no_;
  if (OB_NOT_NULL(other.trans_desc_) && other_max_sql_no < other.trans_desc_->get_max_sql_no()) {
    other_max_sql_no = other.trans_desc_->get_max_sql_no();
  }
  OZ(set_max_sql_no(other_max_sql_no));
  if (OB_NOT_NULL(trans_desc_)) {
    OX(trans_desc_->set_max_sql_no(get_max_sql_no()));
  }
  return ret;
}

int TransResult::merge_total_partitions(const ObPartitionArray& partitions)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  OZ(append_array_no_dup(total_partitions_, partitions), partitions);
  return ret;
}

int TransResult::merge_response_partitions(const common::ObPartitionArray& partitions)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  OZ(append_array_no_dup(response_partitions_, partitions), partitions);
  return ret;
}

int TransResult::merge_part_epoch_list(const transaction::ObPartitionEpochArray& part_epoch_list)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  OZ(append_array_no_dup(part_epoch_list_, part_epoch_list), part_epoch_list);
  return ret;
}

void TransResult::clear_stmt_result()
{
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  total_partitions_.reset();
  response_partitions_.reset();
  part_epoch_list_.reset();
  is_incomplete_ = false;
  /*
  LOG_INFO("reset_sql_no", K(max_sql_no_), K(lbt()));
  */
  max_sql_no_ = 0;
}

int TransResult::set_max_sql_no(int64_t sql_no)
{
  int ret = OB_SUCCESS;
  if (max_sql_no_ < sql_no) {
    /*
    LOG_INFO("renew_sql_no", K(max_sql_no_), K(sql_no), K(lbt()));
    */
    max_sql_no_ = sql_no;
  } else {
    /*
    LOG_INFO("skip_sql_no", K(max_sql_no_), K(sql_no), K(lbt()));
    */
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
