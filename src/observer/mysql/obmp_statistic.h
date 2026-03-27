/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OBMP_STATISTIC_H_
#define _OBMP_STATISTIC_H_

#include "observer/mysql/obmp_base.h"

namespace oceanbase
{
namespace observer
{
class ObMPStatistic
    : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_STATISTICS;

public:
  explicit ObMPStatistic(const ObGlobalContext &gctx)
      : ObMPBase(gctx)
  {}

  int deserialize() { return common::OB_SUCCESS; }

protected:
  int process();

}; // end of class ObMPStatistic
} // end of namespace observer
} // end of namespace oceanbase

#endif /* _OBMP_STATISTIC_H_ */
