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

#ifndef OB_TEST_OB_TEST_ERROR_TRANSACTION_MAJOR_FREEZE_EXP_H_
#define OB_TEST_OB_TEST_ERROR_TRANSACTION_MAJOR_FREEZE_EXP_H_

#include "common/ob_partition_key.h"

namespace oceanbase {
namespace unittest {
class ObTransMajorFreezeException {
public:
  ObTransMajorFreezeException()
  {
    reset();
  }
  virtual ~ObTransMajorFreezeException()
  {}
  void reset();
  void init(const int64_t msg_type, const bool is_do_major_freeze);
  int check_major_freeze(const int64_t msg_type, const common::ObPartitionKey& partition);
  common::ObPartitionArray get_partitions() const
  {
    return partitions_;
  }
  TO_STRING_KV(K_(msg_type), K_(is_do_major_freeze), K_(partitions));

private:
  int64_t msg_type_;
  bool is_do_major_freeze_;
  common::ObPartitionArray partitions_;
};
}  // namespace unittest
}  // namespace oceanbase
#endif
