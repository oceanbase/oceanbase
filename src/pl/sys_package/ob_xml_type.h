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

#ifndef OCEANBASE_SRC_PL_SYS_TYPE_DBMS_XML_H_
#define OCEANBASE_SRC_PL_SYS_TYPE_DBMS_XML_H_

#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "pl/ob_pl_user_type.h"

namespace oceanbase
{
namespace pl
{

class ObXmlType
{
public:
  static int transform(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int getclobval(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int getstringval(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int createxml(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int constructor(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

private:
  static int parse_xml(const ObString &input);
  static int get_val(sql::ObExecContext &ctx, sql::ParamStore &params,
                     common::ObObj &result, bool ret_clob = true);
  static int get_xmltype(sql::ObExecContext &ctx, ObObj &obj,
                         ObPLXmlType *&xmltype, bool need_new = false);
  static int make_xmltype(sql::ObExecContext &ctx, const ObString &str,
                          ObPLXmlType *&xmltype, const ObCollationType &cs_type);

private:
  static const ObString OB_XML_DEFAULT_HEAD;
};

} // end pl
} // end oceanbase

#endif
