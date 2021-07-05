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

#ifndef OCEANBASE_COMMON_OB_I_RS_CB_H_
#define OCEANBASE_COMMON_OB_I_RS_CB_H_

#include "common/ob_partition_key.h"

namespace oceanbase {
namespace common {
class ObIRSCb {
public:
  virtual int submit_pt_update_task(const common::ObPartitionKey& part_key, const int64_t data_version) = 0;
};
}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_COMMON_OB_I_RS_CB_H_
