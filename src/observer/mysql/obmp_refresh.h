/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_REFRESH_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_REFRESH_H_

#include "observer/mysql/obmp_base.h"

namespace oceanbase
{
namespace observer
{

class ObMPRefresh : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_REFRESH;
  explicit ObMPRefresh(const ObGlobalContext &gctx);
  virtual ~ObMPRefresh();

protected:
  int process();
  int deserialize();

private:
  DISALLOW_COPY_AND_ASSIGN(ObMPRefresh);
}; // end of class ObMPRefresh

} // end of namespace observer
} // end of namespace oceanbase


#endif // OCEANBASE_OBSERVER_MYSQL_OBMP_REFRESH_H_
