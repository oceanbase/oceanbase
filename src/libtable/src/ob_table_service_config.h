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
