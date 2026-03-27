/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIBOBCDC_FAKE_COMMON_CONFIG_H__
#define OCEANBASE_LIBOBCDC_FAKE_COMMON_CONFIG_H__

#include "share/ob_define.h"
#include "share/config/ob_common_config.h"    // ObCommonConfig

namespace oceanbase
{
namespace libobcdc
{
class ObLogFakeCommonConfig : public common::ObCommonConfig
{
public:
  ObLogFakeCommonConfig() {}
  virtual ~ObLogFakeCommonConfig() {}

  virtual int check_all() const { return 0; }
  virtual void print() const  { /* do nothing */ }
  virtual common::ObServerRole get_server_type() const { return common::OB_OBLOG; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFakeCommonConfig);
};
} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_FAKE_COMMON_CONFIG_H__ */
