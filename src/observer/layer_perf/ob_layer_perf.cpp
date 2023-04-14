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

#define USING_LOG_PREFIX SERVER

#include "observer/layer_perf/ob_layer_perf.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace observer
{

#ifdef PERF_MODE

thread_local ObLS *ObLayerPerf::ls_ = nullptr;

int ObLayerPerf::process(bool &hit)
{
  int ret = OB_SUCCESS;
  hit = false;
  common::ObString &sql = query_->sql_;
  if (sql.prefix_match("[PM_")) {
    hit = true;
    bool need_sync_resp = true;
    r_ = query_->get_ob_request()->get_ez_req();
    LOG_INFO("ObLayerPerf::process", K(sql));

    if (sql.prefix_match("[PM_WORKER]")) {
      // do nothing
    } else if (sql.prefix_match("[PM_CLOG]")) {
      // clog layer perf
      need_sync_resp = false;
      if (OB_FAIL(do_clog_layer_perf())) {
        LOG_ERROR("do_clog_layer_perf", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (need_sync_resp) {
        ret = do_query_response();
      } else {
        // async response
        query_->packet_sender_.disable_response();
      }
    }
  }
  return ret;
}

int ObLayerPerf::do_response()
{
  //response
  int ret = OB_SUCCESS;
  void *buf = easy_pool_alloc(r_->ms->pool, obmysql::OB_MULTI_RESPONSE_BUF_SIZE + sizeof(easy_buf_t));
  easy_buf_t *b = reinterpret_cast<easy_buf_t*>(buf);
  init_easy_buf(b, reinterpret_cast<char*>(b + 1), r_, obmysql::OB_MULTI_RESPONSE_BUF_SIZE);
  obmysql::OMPKOK okp;
  obmysql::ObMySQLCapabilityFlags cap_flags(163553933);
  okp.set_seq(1);
  okp.set_capability(cap_flags);
  int64_t seri_size = 0;
  if (OB_FAIL(okp.encode(b->last, b->end - b->pos, seri_size))) {
    LOG_ERROR("okp.serialize", K(ret));
  } else {
    b->last += seri_size;
    //easy_request_addbuf(r_, b);
    r_->opacket = b;
    easy_request_wakeup(r_);
  }
  return ret;
}

int ObLayerPerf::do_query_response()
{
  obmysql::OMPKOK okp;
  okp.set_capability(query_->get_conn()->cap_flags_);
  query_->response_packet(okp, NULL);
  return query_->flush_buffer(true);
}

int ObLayerPerf::do_clog_layer_perf()
{
  int ret = OB_SUCCESS;

  ObLS *ls = nullptr;
  if (ObLayerPerf::ls_ != nullptr) {
    ls = ObLayerPerf::ls_;
  } else {
    ObLSService *ls_svr = MTL(ObLSService*);
    common::ObSharedGuard<ObLSIterator> iter;
    if (OB_FAIL(ls_svr->get_ls_iter(iter, ObLSGetMod::LOG_MOD))) {

    } else {
      while (OB_SUCC(ret)) {
        ls = nullptr;
        if (OB_FAIL(iter->get_next(ls))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("iter ls get next failed", K(ret));
          }
        } else if (OB_ISNULL(ls)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("ls is NULL", K(ret));
        } else if (ls->get_ls_id().id() > ObLSID::MIN_USER_LS_ID) {
          // find first user ls
          break;
        }
      }
    }
    if (OB_SUCC(ret) && ls == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("not found user ls");
    }
    if (OB_SUCC(ret)) {
      ObLayerPerf::ls_ = ls;
    }
  }

  if (OB_SUCC(ret)) {
    char *buf = (char*)"clog layer perf test";
    PerfLogCb *cb = nullptr;
    palf::LSN lsn;
    share::SCN zero, ts_ns;
    LOG_INFO("perf layer append", KP(r_), KP(buf));
    if (nullptr == (cb = static_cast<PerfLogCb*>(ob_malloc(sizeof(PerfLogCb), "PerfLogCb")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allo mem", K(ret));
    } else if (FALSE_IT(new (cb) PerfLogCb(r_))) {
    } else if (OB_FAIL(ls->get_log_handler()->append(buf, strlen(buf), zero, true, cb, lsn, ts_ns))) {
      LOG_ERROR("append fail", K(ret));
    }
  }
  return ret;
}


#endif
} // end observer
} // end oceanbase
