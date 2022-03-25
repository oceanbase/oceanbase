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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_PART_CALLBACK_H_
#define OCEANBASE_LIBOBLOG_OB_LOG_PART_CALLBACK_H_

#include "lib/container/ob_se_array.h"        // ObSEArray

namespace oceanbase
{
namespace common
{
struct ObPartitionKey;
}

namespace liboblog
{

struct PartAddCallback
{
public:
  virtual ~PartAddCallback() {}

public:
  // Add partition
  virtual int add_partition(const common::ObPartitionKey &pkey,
      const int64_t start_tstamp,
      const uint64_t start_log_id) = 0;
};

struct PartRecycleCallback
{
public:
  virtual ~PartRecycleCallback() {}

public:
  // Recycling partition
  virtual int recycle_partition(const common::ObPartitionKey &pkey) = 0;
};

typedef common::ObSEArray<int64_t, 4> PartCBArray;

}
}

#endif
