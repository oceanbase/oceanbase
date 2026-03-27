/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_DEBUG_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_DEBUG_H_

#include "observer/mysql/obmp_base.h"

namespace oceanbase
{
namespace observer
{

class ObMPDebug : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_DEBUG;
  explicit ObMPDebug(const ObGlobalContext &gctx);
  virtual ~ObMPDebug();

protected:
  int process();
  int deserialize();

private:
  DISALLOW_COPY_AND_ASSIGN(ObMPDebug);
}; // end of class ObMPDebug

} // end of namespace observer
} // end of namespace oceanbase


#endif // OCEANBASE_OBSERVER_MYSQL_OBMP_DEBUG_H_
