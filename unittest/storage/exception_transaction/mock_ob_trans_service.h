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

#ifndef OB_TEST_OB_TEST_MOCK_TRANS_SERVICE_H_
#define OB_TEST_OB_TEST_MOCK_TRANS_SERVICE_H_

#include "mock_ob_clog_adapter.h"
#include "storage/transaction/ob_trans_service.h"

namespace oceanbase {
namespace unittest {
class MockObTransService : public transaction::ObTransService {
public:
  int set_clog_adapter(transaction::ObIClogAdapter* clog_adapter)
  {
    int ret = OB_SUCCESS;

    if (NULL == clog_adapter) {
      ret = OB_ERR_NULL_VALUE;
      TRANS_LOG(WARN, "set clog adapter NULL", K(ret));
    } else {
      clog_adapter_ = clog_adapter;
    }

    return ret;
  }
  int set_trans_rpc(transaction::ObITransRpc* trans_rpc)
  {
    int ret = OB_SUCCESS;

    if (NULL == trans_rpc) {
      ret = OB_ERR_NULL_VALUE;
      TRANS_LOG(WARN, "set trans rpc NULL", K(ret));
    } else {
      rpc_ = trans_rpc;
    }

    return ret;
  }
  const ObAddr& get_addr() const
  {
    return addr_;
  }

public:
  ObAddr addr_;
  int64_t idx_;
};

}  // namespace unittest
}  // namespace oceanbase
#endif
