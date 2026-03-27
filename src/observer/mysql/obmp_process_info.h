/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_PROCESS_INFO_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_PROCESS_INFO_H_

#include "observer/mysql/obmp_query.h"

namespace oceanbase
{
namespace observer
{

class ObMPProcessInfo : public ObMPQuery
{
public:
  //static const obmysql::ObMySQLCmd COM = obmysql::COM_PROCESS_INFO;
  explicit ObMPProcessInfo(const ObGlobalContext &gctx);
  virtual ~ObMPProcessInfo();
protected:
  int deserialize() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMPProcessInfo);
}; // end of class ObMPProcessInfo

} // end of namespace observer
} // end of namespace oceanbase


#endif // OCEANBASE_OBSERVER_MYSQL_OBMP_PROCESS_INFO_H_
