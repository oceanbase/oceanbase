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

#define USING_LOG_PREFIX SERVER

#include "ob_table_direct_load_processor.h"
#include "observer/table_load/ob_table_load_client_service.h"

namespace oceanbase
{
namespace observer
{
using namespace table;

ObTableDirectLoadP::ObTableDirectLoadP(const ObGlobalContext &gctx)
  : ObTableRpcProcessor(gctx),
    allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    exec_ctx_(allocator_)
{
}

int ObTableDirectLoadP::check_arg()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(arg_.header_.operation_type_ == ObTableDirectLoadOperationType::MAX_TYPE ||
                  arg_.arg_content_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObTableDirectLoadP::try_process()
{
  int ret = OB_SUCCESS;
  exec_ctx_.set_tenant_id(credential_.tenant_id_);
  exec_ctx_.set_user_id(credential_.user_id_);
  exec_ctx_.set_database_id(credential_.database_id_);
  exec_ctx_.set_user_client_addr(user_client_addr_);
  if (OB_FAIL(ObTableLoadClientService::direct_load_operate(exec_ctx_, arg_, result_))) {
    LOG_WARN("fail to do direct load operate", KR(ret), K(arg_));
  }
  return ret;
}

ObTableAPITransCb *ObTableDirectLoadP::new_callback(rpc::ObRequest *req)
{
  UNUSED(req);
  return nullptr;
}

uint64_t ObTableDirectLoadP::get_request_checksum()
{
  uint64_t checksum = 0;
  checksum = ob_crc64(checksum, &result_.header_, sizeof(result_.header_));
  checksum = ob_crc64(checksum, result_.res_content_.ptr(), result_.res_content_.length());
  return checksum;
}

} // namespace observer
} // namespace oceanbase
