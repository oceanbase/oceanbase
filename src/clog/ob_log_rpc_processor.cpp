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

#include "ob_log_rpc_processor.h"
#include "storage/ob_partition_service.h"
#include "ob_clog_mgr.h"

namespace oceanbase {
using namespace storage;
namespace clog {
int ObLogRpcProcessor::process()
{
  int ret = OB_SUCCESS;
  ObAddr server;
  ObICLogMgr* clog_mgr = NULL;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogRpcProcessor not init", K(ret));
  } else if (OB_ISNULL(clog_mgr = partition_service_->get_clog_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "clog_mgr is null", K(ret), K(clog_mgr));
  } else {
    ret = clog_mgr->handle_packet(obrpc::OB_CLOG, arg_.data_, arg_.len_);
  }
  return ret;
}
};  // end namespace clog
};  // end namespace oceanbase
