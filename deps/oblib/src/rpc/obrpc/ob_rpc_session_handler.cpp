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

#define USING_LOG_PREFIX RPC_OBRPC

#include "rpc/obrpc/ob_rpc_session_handler.h"

#include "lib/atomic/ob_atomic.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "rpc/ob_request.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_poc_rpc_server.h"
#include "rpc/obrpc/ob_rpc_reverse_keepalive_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obrpc;

ObRpcSessionHandler::ObRpcSessionHandler()
{
  sessid_ = ObTimeUtility::current_time();
  ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::OB_HASH_NODE_NEXT_WAIT_MAP);
  SET_USE_500(attr);
  next_wait_map_.create(MAX_COND_COUNT, attr, attr);
  max_waiting_thread_count_ = MAX_WAIT_THREAD_COUNT;
  waiting_thread_count_ = 0;

  for (int64_t i = 0; i < MAX_COND_COUNT; ++i) {
    next_cond_[i].init(ObWaitEventIds::RPC_SESSION_HANDLER_COND_WAIT);
  }
}

int64_t ObRpcSessionHandler::generate_session_id()
{
  return ATOMIC_AAF(&sessid_, 1);
}

bool ObRpcSessionHandler::wakeup_next_thread(ObRequest &req)
{
  bool bret = false;
  int64_t sessid = OB_INVALID_ID;
  if (OB_SUCCESS != (get_session_id(req, sessid))) {
    bret = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "Get session id failed", K(bret));
  } else {
    WaitObject wait_object;
    int hash_ret = next_wait_map_.get_refactored(sessid, wait_object);
    if (OB_SUCCESS != hash_ret) {
      // no thread wait for this packet;
      bret = false;
      LOG_WARN_RET(hash_ret, "session not found", K(sessid));
    } else {
      get_next_cond_(wait_object.thid_).lock();
      hash_ret = next_wait_map_.get_refactored(sessid, wait_object);
      if (OB_SUCCESS != hash_ret) {
        // no thread wait for this packet;
        LOG_WARN_RET(hash_ret, "wakeup session but no thread wait", K(req));
        bret = false;
      } else if (NULL != wait_object.req_) {
        const ObRpcPacket &pkt = reinterpret_cast<const ObRpcPacket&>(req.get_packet());
        ObRpcPacketCode pcode = pkt.get_pcode();
        bool is_stream_last = pkt.is_stream_last();
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "previous stream request hasn't been processed yet, "
                     "this might be an abort packet that indicates execution has timed out",
                     K(pcode), K(is_stream_last), "previous request", *wait_object.req_);
        bret = false;
      } else {
        // set packet to wait object
        wait_object.req_ = &req;
        int overwrite = 1;
        hash_ret = next_wait_map_.set_refactored(sessid, wait_object, overwrite);
        if (OB_SUCCESS != hash_ret) {
          LOG_WARN_RET(hash_ret, "rewrite wait object fail", K(*wait_object.req_), K(hash_ret));
        }
        //Wake up the corresponding worker thread to start work
        get_next_cond_(wait_object.thid_).signal();
        bret = true;
      }
      get_next_cond_(wait_object.thid_).unlock();
    }
  }
  return bret;
}

int ObRpcSessionHandler::prepare_for_next_request(int64_t sessid)
{
  int ret = OB_SUCCESS;

  WaitObject wait_object;
  wait_object.thid_ = get_itid();
  wait_object.req_ = NULL;

  if (wait_object.thid_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Thread id no valid", K(ret), "thread id", wait_object.thid_);
  } else {
    get_next_cond_(wait_object.thid_).lock();

    int hash_ret = next_wait_map_.set_refactored(sessid, wait_object);
    if (OB_SUCCESS != ret) {
      if (OB_HASH_EXIST == hash_ret) {
        hash_ret = next_wait_map_.get_refactored(sessid, wait_object);
        if (OB_SUCCESS == hash_ret) {
          // impossible , either last wait not set to NULL or next request
          // reached before pepare..
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("insert sessid exist", K(ret), K(sessid));
        }
      } else {
        ret = hash_ret;
        LOG_WARN("insert wait object fail", K(ret), K(sessid));
      }
    }

    get_next_cond_(wait_object.thid_).unlock();
  }

  return ret;
}

int ObRpcSessionHandler::get_session_id(const ObRequest &req, int64_t &session_id) const
{
  int ret = OB_SUCCESS;

  if(OB_UNLIKELY(ObRequest::OB_RPC != req.get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Request type should be OB_RPC", K(ret), "req type", req.get_type());
  } else {
    const ObRpcPacket &pkt = reinterpret_cast<const ObRpcPacket&>(req.get_packet());
    session_id = pkt.get_session_id();
  }

  return ret;
}


int ObRpcSessionHandler::destroy_session(int64_t sessid)
{
  int ret = OB_SUCCESS;
  WaitObject wait_object;
  int hash_ret = next_wait_map_.get_refactored(sessid, wait_object);
  if (OB_SUCCESS != hash_ret) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Can not find session id in next_wait_map", K(ret), K(sessid));
  } else {
    get_next_cond_(wait_object.thid_).lock();
    hash_ret = next_wait_map_.get_refactored(sessid, wait_object);
    if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Can not find session id in next_wait_map", K(ret), K(sessid));
    }
    if (OB_FAIL(next_wait_map_.erase_refactored(sessid))) {
      if (OB_HASH_NOT_EXIST == ret) {
        LOG_WARN("sessid not exist, destroy do nothing.", K(ret), K(sessid));
      } else {
        LOG_ERROR("erase wait map sessid fail in stream rpc", K(ret), K(sessid));
      }
    }
    if (wait_object.req_) {
      RPC_REQ_OP.response_result(wait_object.req_, NULL);
      LOG_INFO("destroy session with request hasn't processed",
               K_(wait_object.thid),
               K_(wait_object.req));
      wait_object.req_ = NULL;
    }
    get_next_cond_(wait_object.thid_).unlock();
  }

  return ret;
}

int ObRpcSessionHandler::wait_for_next_request(int64_t sessid,
                                               ObRequest *&req,
                                               const int64_t timeout,
                                               const ObRpcReverseKeepaliveArg& reverse_keepalive_arg)
{
  int ret = OB_SUCCESS;
  int hash_ret = 0;
  WaitObject wait_object;
  uint64_t oldv = 0;
  int64_t thid = get_itid();

  req = NULL;
  bool continue_loop = true;
  while (OB_SUCCESS == ret && continue_loop) {
    //waiting_thread_count_ The number of threads waiting for packets of the streaming interface
    oldv = waiting_thread_count_;
    if (oldv > max_waiting_thread_count_) {
      ret = OB_EAGAIN;
      LOG_INFO("current wait thread  >= max wait",
               K(ret), K(oldv), K_(max_waiting_thread_count));
    } else if (ATOMIC_BCAS(&waiting_thread_count_, oldv, oldv + 1)) {
      continue_loop = false;
      get_next_cond_(thid).lock();
      hash_ret = next_wait_map_.get_refactored(sessid, wait_object);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        ret = OB_ERR_UNEXPECTED; //NOT_PREPARED;
        LOG_WARN("sessid not exist, prepare first.", K(ret), K(sessid));
      } else if (wait_object.thid_ != thid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session wait thread id doesn't equal each other",
                 K(ret), K(sessid), K(thid), K(wait_object.thid_), K(ret));
      } else if (NULL == wait_object.req_) {
        int64_t timeout_ms = timeout / 1000;
        int64_t abs_timeout_us = ObTimeUtility::current_time() + timeout;
        int64_t keepalive_timeout_us = abs_timeout_us - timeout + MAX_WAIT_TIMEOUT_MS * 1000;
        if (timeout_ms < DEFAULT_WAIT_TIMEOUT_MS) {
          timeout_ms = DEFAULT_WAIT_TIMEOUT_MS;
        } else {
          //do nothing
        }

        //Waking up by IO thread
        int64_t current_time_us = 0;
        while ((current_time_us = ObTimeUtility::current_time()) < abs_timeout_us) {
          // Here we don't care about result of wait because:
          //
          //   1. If it return success, it may be caused by spurious wakeup.
          //   2. If it return fail, it may have gotten the signal.
          //
          // So we'll check if we've received the request for each
          // wait has finished.
          get_next_cond_(thid).wait(
              timeout_ms < DEFAULT_WAIT_TIMEOUT_MS
              ? timeout_ms
              : DEFAULT_WAIT_TIMEOUT_MS);
          if (OB_FAIL(next_wait_map_.get_refactored(sessid, wait_object))) {
            LOG_ERROR("wait object has been released", K(sessid), K(ret));
            break;
          } else if (OB_ISNULL(wait_object.req_)) {
            LOG_DEBUG("the stream request hasn't come");
            // when waiting for OB_REMOTE_EXECUTE/OB_REMOTE_SYNC_EXECUTE/OB_INNER_SQL_SYNC_TRANSMIT request more than 30s,
            // try to send reverse keepalive request.
            if (current_time_us >= keepalive_timeout_us && reverse_keepalive_arg.is_valid()) {
              get_next_cond_(wait_object.thid_).unlock();
              ret = stream_rpc_reverse_probe(reverse_keepalive_arg);
              get_next_cond_(wait_object.thid_).lock();
              if (OB_FAIL(ret)) {
                LOG_WARN("stream rpc sender has been aborted, unneed to wait", K(sessid), K(timeout), K(reverse_keepalive_arg));
                break;
              }
            }
          } else {
            req = wait_object.req_;
            break;
          }
          timeout_ms = (abs_timeout_us - current_time_us) / 1000;
        }

        if (current_time_us >= abs_timeout_us) {
          // Stop or time out
          req = NULL;
          ret = OB_WAIT_NEXT_TIMEOUT; //WAIT_TIMEOUT;
        }
      } else {
        // Already got the packet
        // check if already got the request..
        req = wait_object.req_;
      }

      // clear last packet, after receiving the streaming interface, set packet_ to NULL
      wait_object.req_ = NULL;
      int overwrite = 1;
      hash_ret = next_wait_map_.set_refactored(sessid, wait_object, overwrite);
      if (OB_SUCCESS != hash_ret) {
        LOG_WARN("rewrite clear session req error",
                  K(hash_ret), K(sessid), K(req));
      }

      get_next_cond_(wait_object.thid_).unlock();
      ATOMIC_DEC(&waiting_thread_count_);
    } else {
      //do nothing
    }
  }
  return ret;
}
