/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_MYSQL_TRANSLATOR_H_
#define _OB_MYSQL_TRANSLATOR_H_

#include "rpc/frame/ob_req_translator.h"

// used when initializing processor table
namespace oceanbase
{
namespace obmysql
{

using rpc::frame::ObReqProcessor;

class ObMySQLTranslator
    : public rpc::frame::ObReqTranslator
{
public:
  ObMySQLTranslator() {}
  virtual ~ObMySQLTranslator() {}
}; // end of class ObMySQLTranslator

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OB_MYSQL_TRANSLATOR_H_ */
