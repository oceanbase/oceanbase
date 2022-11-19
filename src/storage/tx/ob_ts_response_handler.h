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

#ifndef OCEANBASE_TRANSACTION_OB_TS_RESPONSE_HANDLER_
#define OCEANBASE_TRANSACTION_OB_TS_RESPONSE_HANDLER_

#include "rpc/frame/ob_req_processor.h"
#include "observer/ob_srv_task.h"
#include "ob_gts_msg.h"
#include "ob_gts_define.h"

namespace oceanbase
{
namespace transaction 
{
class ObTsMgr;
class ObTsResponseHandler : public rpc::frame::ObReqProcessor
{
public:
  ObTsResponseHandler() { reset(); }
  ~ObTsResponseHandler() {}
  int init(observer::ObSrvTask *task, ObTsMgr *ts_mgr);
  void reset();
protected:
  int run();
private:
  DISALLOW_COPY_AND_ASSIGN(ObTsResponseHandler);
  observer::ObSrvTask *task_;
  ObTsMgr *ts_mgr_;
}; // end of class ObTsResponseHandler

class ObTsResponseTask : public observer::ObSrvTask
{
public:
  ObTsResponseTask() { reset(); }
  ~ObTsResponseTask() {}
  int init(const uint64_t tenant_id,
           const int64_t arg1,
           ObTsMgr *ts_mgr,
           int ts_type);
  void reset();
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_arg1() const { return arg1_; }
  rpc::frame::ObReqProcessor &get_processor() { return handler_; }
  int get_ts_type() const { return ts_type_; }
  TO_STRING_KV(KP(this), K_(arg1), K_(ts_type));
private:
  uint64_t tenant_id_;
  int64_t arg1_;
  ObTsResponseHandler handler_;
  int ts_type_;
};

class ObTsResponseTaskFactory
{
public:
  static ObTsResponseTask *alloc();
  static void free(ObTsResponseTask *task);
private:
  static int64_t alloc_count_;
  static int64_t free_count_;
};

} // transaction 
} // oceanbase

#endif /* OCEANBASE_TRANSACTION_OB_TS_RESPONSE_HANDLER_*/
