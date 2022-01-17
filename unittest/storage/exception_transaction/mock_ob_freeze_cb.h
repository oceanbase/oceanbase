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

#ifndef OB_TEST_OB_TEST_MOCK_FREEZE_CALLBACK_H_
#define OB_TEST_OB_TEST_MOCK_FREEZE_CALLBACK_H_

namespace oceanbase {
namespace storage {
class ObIFreezeCb;
}
namespace unittest {
class MockObFreezeTransCb : public storage::ObIFreezeCb {
public:
  MockObFreezeTransCb()
  {}
  virtual ~MockObFreezeTransCb()
  {}

  virtual int on_success(const common::ObPartitionKey& partition, const int64_t freeze_cmd)
  {
    UNUSED(partition);
    UNUSED(freeze_cmd);
    return OB_SUCCESS;
  }
  virtual int on_fail(const common::ObPartitionKey& partition, const int64_t freeze_cmd)
  {
    UNUSED(partition);
    UNUSED(freeze_cmd);
    return OB_SUCCESS;
  }
};
}  // namespace unittest
}  // namespace oceanbase

#endif
