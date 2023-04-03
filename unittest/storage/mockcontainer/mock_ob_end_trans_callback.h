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

#ifndef OCEANBASE_UNITTEST_MOCK_OB_END_TRANS_CALLBACK_H_
#define OCEANBASE_UNITTEST_MOCK_OB_END_TRANS_CALLBACK_H_

#include "sql/ob_i_end_trans_callback.h"
#include "storage/tx/ob_trans_result.h"

namespace oceanbase
{
namespace unittest
{
class MockObEndTransCallback : public sql::ObExclusiveEndTransCallback
{
public:
  virtual int wait()
  {
    int res = OB_SUCCESS;
    int ret = cond_.wait(WAIT_US, res);
    return (OB_SUCCESS == ret) ? res : ret;
  }
  virtual void callback(int cb_param, const transaction::ObTransID &trans_id)
  {
    UNUSED(trans_id);
    callback(cb_param);
  }
  virtual void callback(int cb_param)
  {
    cond_.notify(cb_param);
  }
  virtual const char *get_type() const { return "MockObEndTransCallback"; }
  virtual sql::ObEndTransCallbackType get_callback_type() const { return sql::MOCK_CALLBACK_TYPE; }
private:
  static const int64_t WAIT_US = 1000 * 1000 * 1000;
private:
  transaction::ObTransCond cond_;
};

} // namespace unittest
} // namespace oceanbase
#endif // OCEANBASE_UNITTEST_MOCK_OB_END_TRANS_CALLBACK_H_
