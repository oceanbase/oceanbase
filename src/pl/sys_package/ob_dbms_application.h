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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_APPLICATION_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_APPLICATION_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSAppInfo
{
public:
  ObDBMSAppInfo() {}
  virtual ~ObDBMSAppInfo() {}
public:
  // reads the value of the client_info field of the current session.
  // param format like following:
  //READ_CLIENT_INFO(CLIENT_INFO OUT VARCHAR2);
  static int read_client_info(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  // reads the values of the module and action fields of the current session.
  // param format like following:
  //READ_MODULE(MODULE_NAME OUT VARCHAR2, ACTION_NAME OUT VARCHAR2);
  static int read_module(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  // sets the name of the current action within the current module.
  // param format like following:
  //SET_ACTION(ACTION_NAME IN VARCHAR2);
  static int set_action(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  // supplies additional information about the client application.
  // param format like following:
  //SET_CLIENT_INFO(CLIENT_INFO IN VARCHAR2);
  static int set_client_info(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  // sets the name of the current application or module.
  // param format like following:
  //SET_MODULE(MODULE_NAME IN VARCHAR2, ACTION_NAME IN VARCHAR2);
  static int set_module(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
};

} // end of pl
} // end of oceanbase

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_APPLICATION_H_ */
