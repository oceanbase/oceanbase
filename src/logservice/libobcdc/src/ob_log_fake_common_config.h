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
 *
 * Fake Common Config
 * Only for public modules that depend on ObCommonConfig
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
