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
#pragma once

#include "share/detect/ob_detect_callback.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "lib/hash/ob_hashset.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace obrpc {

struct ObTaskStateDetectReq
{
  OB_UNIS_VERSION(1);
public:
  ObTaskStateDetectReq()
  {
    peer_ids_.create(1);
  }

  ~ObTaskStateDetectReq()
  {
    destroy();
  }

  void destroy()
  {
    peer_ids_.destroy();
    uniq_ids_.reset();
  }

  hash::ObHashSet<ObDetectableId> peer_ids_; // serialize from hashset
  common::ObSEArray<ObDetectableId, 1> uniq_ids_; // deserialize into array
};

struct TaskInfo {
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(task_id), K_(task_state));
  ObDetectableId task_id_;
  common::ObTaskState task_state_;
};

struct ObTaskStateDetectResp
{
  OB_UNIS_VERSION(1);
public:
  ~ObTaskStateDetectResp()
  {
    destroy();
  }

  void destroy()
  {
    task_infos_.reset();
  }

  int assign(const ObTaskStateDetectResp &other)
  {
    int ret = OB_SUCCESS;
    ARRAY_FOREACH_X(other.task_infos_, idx, cnt, OB_SUCC(ret)) {
      if (OB_FAIL(task_infos_.push_back(other.task_infos_.at(idx)))) {
        COMMON_LOG(WARN, "[DM] failed to push back");
      }
    }
    return ret;
  }

  common::ObSEArray<TaskInfo, 1> task_infos_;
};

struct ObDetectRpcStatus
{
public:
  ObDetectRpcStatus(const common::ObAddr &dst)
      : dst_(dst), is_processed_(false), is_timeout_(false), is_visited_(false) {}
  void destroy()
  {
    response_.destroy();
  }
  bool is_processed() const { return ATOMIC_LOAD(&is_processed_); }
  void set_processed(bool value) { ATOMIC_SET(&is_processed_, value); }
  bool is_timeout() const { return ATOMIC_LOAD(&is_timeout_); }
  void set_is_timeout(bool value) { ATOMIC_SET(&is_timeout_, value); }
  bool is_visited() const { return is_visited_; }
  void set_visited(bool value) { is_visited_ = value; }

  inline int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, len, pos, "ObDetectRpcStatus: (%d, %d, %d)",
                    is_processed_, is_timeout_, is_visited_);
    return pos;
  }
public:
  common::ObAddr dst_;
  bool is_processed_;
  bool is_timeout_;
  bool is_visited_;
  ObTaskStateDetectResp response_;
};

class ObTaskStateDetectAsyncCB;
class ObDetectRpcProxy : public ObRpcProxy
{
private:
  static const int64_t DETECT_MSG_TIMEOUT = 10 * 1000 * 1000;
public:
  DEFINE_TO(ObDetectRpcProxy);
  RPC_AP(PR1 detect_task_state, OB_DETECT_RPC_CALL, (ObTaskStateDetectReq), ObTaskStateDetectResp);
};

class ObTaskStateDetectAsyncCB : public obrpc::ObDetectRpcProxy::AsyncCB<obrpc::OB_DETECT_RPC_CALL>
{
public:
  ObTaskStateDetectAsyncCB(ObThreadCond &cond, ObDetectRpcStatus *rpc_status, const common::ObAddr &dst)
    : cond_(cond), rpc_status_(rpc_status), dst_(dst) {}
  virtual ~ObTaskStateDetectAsyncCB() {}

  // const ObTaskStateDetectResp &get_result() const { return result_; }
  // const obrpc::ObRpcResultCode get_ret_code() const { return rcode_; }
  int process() override;
  virtual void on_timeout() override;
  rpc::frame::ObReqTransport::AsyncCB *clone(
      const rpc::frame::SPAlloc &alloc) const
  {
    void *buf = alloc(sizeof(*this));
    rpc::frame::ObReqTransport::AsyncCB *newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObTaskStateDetectAsyncCB(cond_, rpc_status_, dst_);
    }
    return newcb;
  }
  virtual void set_args(const Request &arg) { UNUSED(arg); }

private:
  ObThreadCond &cond_;
  ObDetectRpcStatus *rpc_status_;
  common::ObAddr dst_;
};




} // end namespace obrpc
} // end namespace oceanbase
