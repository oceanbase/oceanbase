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

#define USING_LOG_PREFIX RS
#include "rootserver/parallel_ddl/ob_create_view_helper.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_rpc_struct.h"
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

ObCreateViewHelper::ObCreateViewHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    const obrpc::ObCreateTableArg &arg,
    obrpc::ObCreateTableRes &res)
  : ObDDLHelper(schema_service, tenant_id),
    arg_(arg),
    res_(res)
{}

ObCreateViewHelper::~ObCreateViewHelper()
{
}

int ObCreateViewHelper::execute()
{
  RS_TRACE(create_view_begin);
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(start_ddl_trans_())) {
    LOG_WARN("fail to start ddl trans", KR(ret));
  } else if (OB_FAIL(lock_objects_())) {
    LOG_WARN("fail to lock objects", KR(ret));
  } else if (OB_FAIL(generate_schemas_())) {
    LOG_WARN("fail to generate schemas", KR(ret));
  } else if (OB_FAIL(gen_task_id_and_schema_versions_())) {
    LOG_WARN("fail to gen task id and schema versions", KR(ret));
  } else if (OB_FAIL(create_schemas_())) {
    LOG_WARN("fail create schemas", KR(ret));
  } else if (OB_FAIL(serialize_inc_schema_dict_())) {
    LOG_WARN("fail to serialize inc schema dict", KR(ret));
  } else if (OB_FAIL(wait_ddl_trans_())) {
    LOG_WARN("fail to wait ddl trans", KR(ret));
  }
  if (OB_FAIL(end_ddl_trans_(ret))) { // won't overwrite ret
    LOG_WARN("fail to end ddl trans", KR(ret));
  }
  RS_TRACE(create_view_end);
  FORCE_PRINT_TRACE(THE_RS_TRACE, "[parallel create view]");
  return ret;
}

//TODO:(yanmu.ztl) to implement
int ObCreateViewHelper::lock_objects_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  return ret;
}

//TODO:(yanmu.ztl) to implement
int ObCreateViewHelper::generate_schemas_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  return ret;
}

//TODO:(yanmu.ztl) to implement
int ObCreateViewHelper::create_schemas_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  }
  return ret;
}
