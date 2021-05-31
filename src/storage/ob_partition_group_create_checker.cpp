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

#define USING_LOG_PREFIX STORAGE
#include "lib/oblog/ob_log_module.h"
#include "ob_partition_group_create_checker.h"

using namespace oceanbase::storage;
using namespace oceanbase::common;

void ObPartitionGroupCreateChecker::reset()
{
  creating_pgs_.reset();
}

int ObPartitionGroupCreateChecker::mark_pg_creating(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  TCWLockGuard lock_guard(lock_);

  if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey));
  } else if (is_pg_creating_(pkey)) {
    ret = OB_EAGAIN;
    LOG_WARN("partition group is creating, make sure not create it twice.", K(ret), K(pkey));
  } else if (OB_FAIL(creating_pgs_.push_back(pkey))) {
    LOG_WARN("push partition error", K(ret), K(pkey));
  }
  return ret;
}

int ObPartitionGroupCreateChecker::mark_pg_creating(
    const ObIArray<ObPartitionKey>& pgs, ObPartitionKey& create_twice_pg)
{
  int ret = OB_SUCCESS;
  TCWLockGuard lock_guard(lock_);

  for (int64_t i = 0; OB_SUCC(ret) && i < pgs.count(); ++i) {
    const ObPartitionKey& pkey = pgs.at(i);
    if (OB_UNLIKELY(!pkey.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(i), K(pgs));
    } else if (is_pg_creating_(pkey)) {
      ret = OB_EAGAIN;
      create_twice_pg = pkey;
      LOG_WARN("partition group is creating, make sure not create it twice.", K(ret), K(pkey));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < pgs.count(); ++i) {
      const ObPartitionKey& pkey = pgs.at(i);
      if (OB_FAIL(creating_pgs_.push_back(pkey))) {
        LOG_WARN("push partition error", K(ret), K(pkey));
      } else {
        count++;
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < count; ++i) {
        creating_pgs_.pop_back();
      }
    }
  }
  return ret;
}

int ObPartitionGroupCreateChecker::mark_pg_created(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  TCWLockGuard lock_guard(lock_);

  if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkey));
  } else if (OB_FAIL(mark_pg_created_(pkey))) {
    LOG_WARN("mark pg created failed.", K(ret), K(pkey));
  }
  return ret;
}

int ObPartitionGroupCreateChecker::mark_pg_created(const ObIArray<ObPartitionKey>& pgs)
{
  int ret = OB_SUCCESS;
  TCWLockGuard lock_guard(lock_);

  for (int64_t i = 0; OB_SUCC(ret) && i < pgs.count(); ++i) {
    const ObPartitionKey& pkey = pgs.at(i);
    if (OB_UNLIKELY(!pkey.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(pkey), K(i), K(pgs));
    } else if (OB_FAIL(mark_pg_created_(pkey))) {
      LOG_WARN("mark pg created failed.", K(ret), K(pkey), K(i), K(pgs));
    }
  }
  return ret;
}

bool ObPartitionGroupCreateChecker::is_pg_creating_(const ObPartitionKey& pkey)
{
  bool is_creating = false;
  const int64_t count = creating_pgs_.count();

  for (int64_t i = 0; !is_creating && (i < count); i++) {
    is_creating = (pkey == creating_pgs_[i]);
  }
  return is_creating;
}

int ObPartitionGroupCreateChecker::mark_pg_created_(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  bool found = false;
  const int64_t count = creating_pgs_.count();

  for (int64_t i = 0; OB_SUCC(ret) && !found && (i < count); i++) {
    if (pkey == creating_pgs_[i]) {
      found = true;
    }
    if (found && OB_FAIL(creating_pgs_.remove(i))) {
      LOG_ERROR("mark pg created failed.", K(ret), K(pkey));
    }
  }
  if (OB_UNLIKELY(!found) && OB_SUCC(ret)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the pg creating record not found, mark pg created failed.", K(ret), K(pkey));
  }
  return ret;
}
