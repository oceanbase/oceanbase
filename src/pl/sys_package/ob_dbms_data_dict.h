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
#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_DATA_DICT_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_DATA_DICT_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{
static const char* DATA_DICT_SCHEDULED_JOB_NAME = "SCHEDULED_TRIGGER_DUMP_DATA_DICT";
class ObDBMSDataDict
{
public:
  ObDBMSDataDict() {}
  virtual ~ObDBMSDataDict() {}

  static int trigger_dump_data_dict(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int enable_dump(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int disable_dump(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int modify_interval(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int modify_duration(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int modify_retention(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

private:
  static int check_op_allowed_(const sql::ObExecContext &ctx, uint64_t &tenant_id);
  static int check_scheduled_job_enabled_(const uint64_t tenant_id, bool &is_enabled);
  static int get_tenant_gts_(const uint64_t tenant_id, share::SCN &gts);
  static int send_trigger_dump_dict_rpc_(
      const uint64_t tenant_id,
      const obrpc::ObTriggerDumpDataDictArg &trigger_dump_data_dict_arg);
  static int modify_schedule_job_(
      const uint64_t tenant_id,
      const ObString &job_attribute_name,
      const ObObj &job_attribute_value);
 };
} // end of pl
} // end of oceanbase
#endif // OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_DATA_DICT_H_