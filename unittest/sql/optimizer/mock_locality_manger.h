/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
