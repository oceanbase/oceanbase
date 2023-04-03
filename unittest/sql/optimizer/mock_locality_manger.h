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

#ifndef _OB_MOCK_LOCALITY_MANAGER_H_
#define _OB_MOCK_LOCALITY_MANAGER_H_
#undef private
#undef protected
#include <gmock/gmock.h>
#define private public
#define protected public
#include "storage/ob_locality_manager.h"

using namespace oceanbase;
namespace test
{
class MockLocalityManager : public oceanbase::storage::ObLocalityManager
{
public:
  virtual int get_server_locality_array(
      common::ObIArray<share::ObServerLocality> &server_locality_array,
      bool &has_readonly_zone) const
  {
    int ret = OB_SUCCESS;
    UNUSED(server_locality_array);
    UNUSED(has_readonly_zone);
    return ret;
  }
};

}


#endif
