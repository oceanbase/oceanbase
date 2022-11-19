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
