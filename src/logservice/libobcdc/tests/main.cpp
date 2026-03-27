/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX OBLOG_TAILF

#include "obcdc_main.h"

using namespace oceanbase::libobcdc;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = OB_SUCCESS;
  ObLogMain &oblog_main = ObLogMain::get_instance();

  if (OB_FAIL(oblog_main.init(argc, argv))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("init oblog main fail", K(argc));
    }
  } else if (OB_FAIL(oblog_main.start())) {
    LOG_ERROR("start oblog main fail", K(ret));
  } else {
    oblog_main.run();
    oblog_main.stop();

    if (oblog_main.need_reentrant()) {
      LOG_INFO("oblog reentrant");

      if (OB_FAIL(oblog_main.start())) {
        LOG_ERROR("start oblog main twice fail", K(ret));
      } else {
        oblog_main.run();
        oblog_main.stop();
      }
    }
  }

  oblog_main.destroy();

  return 0;
}
