/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "mock_server.h"

using namespace oceanbase::unittest::pub;
using namespace oceanbase::common;
using namespace oceanbase::observer;

int main()
{
  MockServer s;
  if (OB_SUCCESS == s.init(25000)) {
    s.start();
    // s.wait_for();
  } else {
    OB_LOG(ERROR, "init fail");
  }
  sleep(5);
  return 0;
}

namespace oceanbase
{
namespace unittest
{
namespace pub
{



}
}
}
