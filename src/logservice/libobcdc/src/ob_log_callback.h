/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_CALLBACK_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_CALLBACK_H_

namespace oceanbase
{
namespace libobcdc
{
class ObILogCallback
{
public:
  virtual int handle_log_callback() = 0;
};

}
}

#endif
