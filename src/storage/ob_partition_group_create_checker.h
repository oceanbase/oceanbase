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

#ifndef OB_PATITION_GROUP_CREATE_CHECKER_H
#define OB_PATITION_GROUP_CREATE_CHECKER_H

#include "lib/lock/ob_tc_rwlock.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
using namespace oceanbase::common;

namespace storage {
class ObPartitionGroupCreateChecker {
public:
  ObPartitionGroupCreateChecker() : creating_pgs_()
  {}
  ~ObPartitionGroupCreateChecker()
  {
    reset();
  }
  int mark_pg_creating(const ObIArray<ObPartitionKey>& pgs, ObPartitionKey& create_twice_pg);
  int mark_pg_creating(const ObPartitionKey& pkey);
  int mark_pg_created(const ObIArray<ObPartitionKey>& pgs);
  int mark_pg_created(const ObPartitionKey& pkey);
  void reset();

private:
  bool is_pg_creating_(const ObPartitionKey& pkey);
  int mark_pg_created_(const ObPartitionKey& pkey);

private:
  common::TCRWLock lock_;
  ObPartitionArray creating_pgs_;
};

}  // namespace storage
}  // namespace oceanbase

#endif
