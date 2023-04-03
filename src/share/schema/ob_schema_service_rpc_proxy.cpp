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

#include "share/schema/ob_schema_service_rpc_proxy.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace obrpc
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

/*
int ObGetLatestSchemaVersionP::process()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema_service_)) {
    ret = OB_INNER_STAT_ERROR;
    SHARE_SCHEMA_LOG(WARN, "inner stat error", K(ret), K(schema_service_));
  } else {
    result_ = schema_service_->get_refreshed_schema_version();
  }

  return ret;
}
*/

OB_SERIALIZE_MEMBER(ObAllSchema,
                    tenant_,
                    users_,
                    databases_,
                    tablegroups_,
                    tables_,
                    outlines_,
                    db_privs_,
                    table_privs_);


ObGetAllSchemaP::ObGetAllSchemaP(ObMultiVersionSchemaService *schema_service)
  : schema_service_(schema_service),
    buf_(NULL),
    buf_len_(0)
{
}

ObGetAllSchemaP::~ObGetAllSchemaP()
{
}

int ObGetAllSchemaP::before_process()
{
  return OB_NOT_SUPPORTED;
}

int ObGetAllSchemaP::process()
{
  int ret = OB_SUCCESS;

  static const int64_t BUF_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
  int64_t pos = 0;
  while (OB_SUCC(ret)) {
    if (pos >= buf_len_) {
      break;
    }
    int64_t buf_len = pos + BUF_SIZE <= buf_len_ ? BUF_SIZE : buf_len_ - pos;
    result_.reset();
    if (!result_.set_data(buf_ + pos, buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "set data failed", K(ret));
    } else if (FALSE_IT(result_.get_position() = buf_len)) {
      // not reach here
    } else if (OB_FAIL(flush())) {
      SHARE_SCHEMA_LOG(WARN, "flush failed");
    } else {
      pos += buf_len;
    }
  }

  return ret;
}

int ObGetAllSchemaP::after_process()
{
  int ret = OB_SUCCESS;
  if (NULL != buf_) {
    ob_free(buf_);
  }
  return ret;
}

} // namespace obrpc

} // namespace oceanbase
