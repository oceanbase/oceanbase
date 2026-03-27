/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_MOCK_RS_MGR_H_
#define OCEANBASE_SHARE_OB_MOCK_RS_MGR_H_

#include "share/ob_rs_mgr.h"

namespace oceanbase
{
namespace share
{
class MockObRsMgr : public ObRsMgr
{
public:
  MOCK_CONST_METHOD1(get_master_root_server, int(common::ObAddr &));
  virtual ~MockObRsMgr() {}

  int get_master_root_server_wrapper(common::ObAddr &rs) { rs = global_rs(); return common::OB_SUCCESS; }

  static common::ObAddr &global_rs() { static common::ObAddr addr; return addr; }
};
}//end namespace share
}//end namespace oceanbase

#endif
