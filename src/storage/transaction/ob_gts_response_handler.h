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

#ifndef OCEANBASE_TRANSACTION_OB_GTS_RESPONSE_HANDLER_
#define OCEANBASE_TRANSACTION_OB_GTS_RESPONSE_HANDLER_

#include "rpc/frame/ob_req_processor.h"
#include "observer/ob_srv_task.h"
#include "ob_gts_msg.h"
#include "ob_gts_define.h"
//#include "ob_gts_rpc.h"

namespace oceanbase {
namespace transaction {
class ObTsMgr;
class ObGtsResponseHandler : public rpc::frame::ObReqProcessor {
public:
  ObGtsResponseHandler()
  {
    reset();
  }
  ~ObGtsResponseHandler()
  {}
  int init(observer::ObSrvTask* task, ObTsMgr* ts_mgr);
  void reset();

protected:
  int deserialize()
  {
    return common::OB_SUCCESS;
  }
  int serialize()
  {
    return common::OB_SUCCESS;
  }
  int response(const int retcode)
  {
    UNUSED(retcode);
    return common::OB_SUCCESS;
  }
  int process();
  int after_process()
  {
    req_has_wokenup_ = true;
    return common::OB_SUCCESS;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObGtsResponseHandler);
  observer::ObSrvTask* task_;
  ObTsMgr* ts_mgr_;
};  // end of class ObGtsResponseHandler

class ObGtsResponseTask : public observer::ObSrvTask {
public:
  ObGtsResponseTask()
  {
    reset();
  }
  ~ObGtsResponseTask()
  {}
  int init(const uint64_t tenant_id, const int64_t queue_index, ObTsMgr* ts_mgr, int ts_type);
  void reset();
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_queue_index() const
  {
    return queue_index_;
  }
  rpc::frame::ObReqProcessor& get_processor()
  {
    return handler_;
  }
  int get_ts_type() const
  {
    return ts_type_;
  }
  TO_STRING_KV(KP(this), K_(queue_index), K_(ts_type));

private:
  uint64_t tenant_id_;
  int64_t queue_index_;
  ObGtsResponseHandler handler_;
  int ts_type_;
};

class ObGtsResponseTaskFactory {
public:
  static ObGtsResponseTask* alloc();
  static void free(ObGtsResponseTask* task);

private:
  static int64_t alloc_count_;
  static int64_t free_count_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif /* OCEANBASE_TRANSACTION_OB_GTS_RESPONSE_HANDLER_*/
