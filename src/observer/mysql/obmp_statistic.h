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
