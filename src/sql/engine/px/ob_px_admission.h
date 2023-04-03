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

#ifndef __SQL_ENG_PX_ADMISSION_H__
#define __SQL_ENG_PX_ADMISSION_H__

#include "share/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/lock/ob_monitor.h"
#include "lib/lock/mutex.h"
#include "lib/rc/ob_rc.h"
#include "lib/hash/ob_hashset.h"
#include "sql/resolver/ob_stmt_type.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::common::hash;

namespace sql
{

class ObSQLSessionInfo;
class ObPhysicalPlan;
class ObExecContext;

class ObPxAdmission
{
public:
  ObPxAdmission() = default;
  ~ObPxAdmission() = default;
  static int64_t admit(ObSQLSessionInfo &session, ObExecContext &exec_ctx,
                       int64_t wait_time_us, int64_t session_target,
                       ObHashMap<ObAddr, int64_t> &worker_map,
                       int64_t req_cnt, int64_t &admit_cnt);
  static int enter_query_admission(sql::ObSQLSessionInfo &session,
                                   sql::ObExecContext &exec_ctx,
                                   sql::stmt::StmtType stmt_type,
                                   sql::ObPhysicalPlan &plan);
  static void exit_query_admission(sql::ObSQLSessionInfo &session,
                                   sql::ObExecContext &exec_ctx,
                                   sql::stmt::StmtType stmt_type,
                                   sql::ObPhysicalPlan &plan);
private:
  static int get_parallel_session_target(sql::ObSQLSessionInfo &session,
                                         int64_t minimal_session_target,
                                         int64_t &session_target);
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObPxAdmission);
};
class ObPxSubAdmission
{
public:
  ObPxSubAdmission() = default;
  ~ObPxSubAdmission() = default;
  static void acquire(int64_t max, int64_t min, int64_t &acquired_cnt);
  static void release(int64_t acquired_cnt);
private:
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObPxSubAdmission);
};

}
}
#endif /* __SQL_ENG_PX_ADMISSION_H__ */
//// end of header file
