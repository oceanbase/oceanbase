/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OBMP_SET_OPTION_H_
#define _OBMP_SET_OPTION_H_

#include "observer/mysql/obmp_base.h"

namespace oceanbase
{
enum MysqlSetOptEnum {
  MYSQL_OPTION_INVALID = -1,
  MYSQL_OPTION_MULTI_STATEMENTS_ON = 0,
  MYSQL_OPTION_MULTI_STATEMENTS_OFF = 1
};

namespace observer
{
class ObMPSetOption
    : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_SET_OPTION;

public:
  explicit ObMPSetOption(const ObGlobalContext &gctx)
      : ObMPBase(gctx),
        set_opt_(MysqlSetOptEnum::MYSQL_OPTION_INVALID)
  {}
  virtual ~ObMPSetOption() {}

  int deserialize();

protected:
  int process();

  uint16_t set_opt_;
}; // end of class ObMPStatistic
} // end of namespace observer
} // end of namespace oceanbase

#endif /* _OBMP_SET_OPTION_H_ */
