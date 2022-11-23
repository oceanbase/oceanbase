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

#include "observer/mysql/obmp_query.h"
#include "rpc/obmysql/packet/ompk_ok.h"

namespace oceanbase
{
namespace observer
{

#ifdef PERF_MODE
class ObLayerPerf
{
public:
  ObLayerPerf(ObMPQuery *query) :query_(query),
                                 r_(nullptr)
                                 {}
  int process(bool &hit);
  // use mpquery do response
  int do_query_response();
  // use easy request do response
  int do_response();
  void set_ez_req(easy_request_t *r) {
    r_ = r;
  }
public:
  static thread_local ObLS* ls_;
private:
  int do_clog_layer_perf();
  ObMPQuery *query_;
  easy_request_t *r_;
};

class PerfLogCb : public logservice::AppendCb
{
public:
  explicit PerfLogCb(easy_request_t *r) : layer_perf_(nullptr) {
    layer_perf_.set_ez_req(r);
  }
  ~PerfLogCb() {}
  int on_success() override {
    int ret = layer_perf_.do_response();
    this->~PerfLogCb();
    ob_free(this);
    return ret;
  }
  int on_failure() override {
    this->~PerfLogCb();
    ob_free(this);
    return OB_ERR_UNEXPECTED;
  }
private:
  ObLayerPerf layer_perf_;
};
#endif
} // end observer
} // end oceanbase
