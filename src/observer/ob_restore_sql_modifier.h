/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_RS_RESTORE_SQL_MODIFIER_H_
#define _OB_RS_RESTORE_SQL_MODIFIER_H_

namespace oceanbase
{

namespace sql
{
class ObResultSet;
}

namespace observer
{
class ObRestoreSQLModifier
{
public:
  virtual int modify(sql::ObResultSet &rs) = 0;
};
}

}
#endif /* _OB_RS_RESTORE_SQL_MODIFIER_H_ */
//// end of header file
