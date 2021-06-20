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

#include "share/partition_table/ob_partition_table_operator.h"
namespace oceanbase {
namespace share {
using namespace common;
class FakeRsListChangeCb : public ObIRsListChangeCb {
public:
  virtual int submit_update_rslist_task(const bool force_update)
  {
    UNUSED(force_update);
    return OB_SUCCESS;
  }
  virtual int submit_report_replica(const common::ObPartitionKey& key)
  {
    UNUSED(key);
    return OB_SUCCESS;
  }
  virtual int submit_report_replica()
  {
    return OB_SUCCESS;
  }
};
class FakeMergeErrorCb : public ObIMergeErrorCb {
public:
  FakeMergeErrorCb()
  {}
  ~FakeMergeErrorCb()
  {}
  virtual int submit_merge_error_task()
  {
    return OB_SUCCESS;
  }
};
}  // namespace share
}  // namespace oceanbase
