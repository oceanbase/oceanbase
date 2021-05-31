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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_job_conf.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/ob_sql_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObJobConf::ObJobConf()
    : task_split_type_(ObTaskSpliter::INVALID_SPLIT), table_id_(OB_INVALID_ID), index_id_(OB_INVALID_ID)
{}

ObJobConf::~ObJobConf()
{}

void ObJobConf::reset()
{
  task_split_type_ = ObTaskSpliter::INVALID_SPLIT;
  table_id_ = OB_INVALID_ID;
  index_id_ = OB_INVALID_ID;
}
