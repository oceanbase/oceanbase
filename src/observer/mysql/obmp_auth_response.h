/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OBMP_AUTH_RESPONSE_H_
#define _OBMP_AUTH_RESPONSE_H_

#include "observer/mysql/obmp_base.h"

namespace oceanbase
{
namespace observer
{
class ObMPAuthResponse
    : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_AUTH_SWITCH_RESPONSE;

public:
  explicit ObMPAuthResponse(const ObGlobalContext &gctx)
      : ObMPBase(gctx),
        auth_data_()
  {}

  int deserialize();

protected:
  int process();

  common::ObString auth_data_;
}; // end of class ObMPStatistic
} // end of namespace observer
} // end of namespace oceanbase

#endif /* _OBMP_AUTH_RESPONSE_H_ */
