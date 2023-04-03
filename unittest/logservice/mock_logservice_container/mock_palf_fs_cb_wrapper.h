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

#ifndef OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_ROLE_CHANGE_CB_WRAPPER_
#define OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_ROLE_CHANGE_CB_WRAPPER_

#include "logservice/palf/palf_callback_wrapper.h"

namespace oceanbase
{
namespace palf
{

class MockPalfFSCbWrapper : public PalfFSCbWrapper {
public:
  MockPalfFSCbWrapper() {}
  ~MockPalfFSCbWrapper() {}

  int add_cb_impl(PalfFSCbNode *cb_impl)
  {
    int ret = OB_SUCCESS;
    UNUSED(cb_impl);
    return ret;
  }
  void del_cb_impl(PalfFSCbNode *cb_impl)
  {
    UNUSED(cb_impl);
  }
  int update_end_lsn(int64_t id, const LSN &end_lsn, const int64_t proposal_id)
  {
    int ret = OB_SUCCESS;
    UNUSED(id);
    UNUSED(end_lsn);
    UNUSED(proposal_id);
    return ret;
  }
};

} // end of palf
} // end of oceanbase

#endif
