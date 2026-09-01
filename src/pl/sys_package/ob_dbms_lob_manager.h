/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_LOB_MANAGER_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_LOB_MANAGER_H_

#include "common/object/ob_object.h"
#include "lib/charset/ob_charset.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_tree.h"

namespace oceanbase
{
namespace pl
{
typedef struct ObPLExecCtx ObPLExecCtx;
using common::ParamStore;
using common::ObObj;


class ObDbmsLobManager
{
public:
  ObDbmsLobManager() {}
  virtual ~ObDbmsLobManager() {}

  // LOB一致性校验和恢复相关接口
  static int check_lob(ObPLExecCtx &ctx, ParamStore &params, ObObj &result);
  static int cancel_job(ObPLExecCtx &ctx, ParamStore &params, ObObj &result);
  static int suspend_job(ObPLExecCtx &ctx, ParamStore &params, ObObj &result);
  static int resume_job(ObPLExecCtx &ctx, ParamStore &params, ObObj &result);
  static int repair_lob(ObPLExecCtx &ctx, ParamStore &params, ObObj &result);
  static int check_lob_inner(ObPLExecCtx &ctx, ParamStore &params, ObObj &result);
  static int reschedule_job(ObPLExecCtx &ctx, ParamStore &params, ObObj &result);

private:
  static int check_lob_task_exists(uint64_t tenant_id,
                                  ObISQLClient &sql_proxy,
                                  bool &task_exists,
                                  int64_t table_id);
  static int check_resource_manager_plan_exists(uint64_t tenant_id,
                                  ObISQLClient &sql_proxy,
                                  bool &resource_manager_exists);
  static int parse_table_ids_json(ObIAllocator &allocator,
                                  ObObj &json_obj,
                                  ObJsonBuffer &table_ids_json);
  static int push_table_with_tablet(ObIAllocator &allocator,
                                    int64_t table_id,
                                    ObIJsonBase *tablet_ids_tree,
                                    ObJsonObject &table_with_tablet_obj);

  static int get_table_id(ObObj &obj, int64_t &table_id);
};



} // end namespace pl
} // end namespace oceanbase
#endif // OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_LOB_MANAGER_H_