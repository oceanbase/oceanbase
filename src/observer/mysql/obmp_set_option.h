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
