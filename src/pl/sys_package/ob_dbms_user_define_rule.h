/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_QUERY_REWRITE_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_QUERY_REWRITE_H_
#include "sql/engine/ob_exec_context.h"
#include "share/object/ob_obj_cast.h"
#include "sql/udr/ob_udr_struct.h"

namespace oceanbase
{
namespace pl
{

#define DEF_UDR_PROCESSOR(name)                                                                   \
  class name##Processor : public ObUDRProcessor                                                   \
  {                                                                                               \
  public:                                                                                         \
    name##Processor(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)      \
    : ObUDRProcessor(ctx, params, result)                                                         \
    {}                                                                                            \
    virtual ~name##Processor() {}                                                                 \
    virtual int parse_request_param();                                                            \
    virtual int generate_exec_arg();                                                              \
    virtual int execute();                                                                        \
  private:                                                                                        \
    DISALLOW_COPY_AND_ASSIGN(name##Processor);                                                    \
  };

class ObUDRProcessor
{
public:
  ObUDRProcessor(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result)
    : is_inited_(false),
      ctx_(ctx),
      params_(params),
      result_(result),
      arg_()
  {}
  virtual ~ObUDRProcessor() {}
  int process();
  virtual int init();
  virtual int parse_request_param() = 0;
  virtual int generate_exec_arg() = 0;
  virtual int execute() = 0;

protected:
  int pre_execution_check();
  int sync_rule_from_inner_table();

protected:
  bool is_inited_;
  sql::ObExecContext &ctx_;
  sql::ParamStore &params_;
  common::ObObj &result_;
  ObUDRInfo arg_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUDRProcessor);
};

DEF_UDR_PROCESSOR(ObCreateRule);
DEF_UDR_PROCESSOR(ObRemoveRule);
DEF_UDR_PROCESSOR(ObEnableRule);
DEF_UDR_PROCESSOR(ObDisableRule);

class ObDBMSUserDefineRule
{
public:
  ObDBMSUserDefineRule() {}
  virtual ~ObDBMSUserDefineRule() {}
#define DEF_UDR_FUNC(name)  \
  static int name(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  DEF_UDR_FUNC(create_rule);
  DEF_UDR_FUNC(remove_rule);
  DEF_UDR_FUNC(enable_rule);
  DEF_UDR_FUNC(disable_rule);
};

} // end of pl
} // end of oceanbase
#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_QUERY_REWRITE_H_ */