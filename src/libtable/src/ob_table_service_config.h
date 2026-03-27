/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_TABLE_SERVICE_CONFIG_H
#define _OB_TABLE_SERVICE_CONFIG_H 1
#include "share/config/ob_common_config.h"

namespace oceanbase
{
namespace table
{
/// Config for ObTableServiceClient
class ObTableServiceConfig : public common::ObCommonConfig
{
public:
  ObTableServiceConfig() {}
  virtual ~ObTableServiceConfig() {}

  virtual int check_all() const { return 0; }
  virtual void print() const  { /* do nothing */ }
  virtual common::ObServerRole get_server_type() const { return common::OB_OBLOG; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableServiceConfig);
};
} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_SERVICE_CONFIG_H */
