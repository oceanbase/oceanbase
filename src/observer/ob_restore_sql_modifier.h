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
