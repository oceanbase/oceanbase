/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_ROLE_CHANGE_CB_WRAPPER_
#define OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_ROLE_CHANGE_CB_WRAPPER_

#include "logservice/palf/palf_callback_wrapper.h"

namespace oceanbase
{
namespace palf
{

class MockPalfRoleChangeCbWrapper : public PalfRoleChangeCbWrapper {
public:
  MockPalfRoleChangeCbWrapper() {}
  ~MockPalfRoleChangeCbWrapper() {}

  int add_cb_impl(PalfRoleChangeCbNode *cb_impl)
  {
    int ret = OB_SUCCESS;
    UNUSED(cb_impl);
    return ret;
  }
  void del_cb_impl(PalfRoleChangeCbNode *cb_impl)
  {
    UNUSED(cb_impl);
  }
  int on_role_change(int64_t id)
  {
    int ret = OB_SUCCESS;
    UNUSED(id);
    return ret;
  }
};

} // end of palf
} // end of oceanbase

#endif
