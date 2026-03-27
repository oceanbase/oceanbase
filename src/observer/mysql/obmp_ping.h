/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_PING_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_PING_H_

#include "observer/mysql/obmp_base.h"

namespace oceanbase
{
namespace observer
{

class ObMPPing : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_PING;
  explicit ObMPPing(const ObGlobalContext &gctx);
  virtual ~ObMPPing();

protected:
  int process();
  int deserialize();

private:
  common::ObString sql_;
  DISALLOW_COPY_AND_ASSIGN(ObMPPing);
}; // end of class ObMPPing

} // end of namespace observer
} // end of namespace oceanbase


#endif // OCEANBASE_OBSERVER_MYSQL_OBMP_PING_H_
