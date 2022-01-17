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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_END_TRANS_CALLBACK_
#define OCEANBASE_TRANSACTION_OB_TRANS_END_TRANS_CALLBACK_

#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "sql/ob_end_trans_callback.h"

namespace oceanbase {

namespace transaction {
struct ObEndTransCallbackItem {
  ObEndTransCallbackItem() : cb_(NULL), retcode_(common::OB_SUCCESS)
  {}
  ~ObEndTransCallbackItem()
  {}
  TO_STRING_KV(KP_(cb), K_(retcode));
  sql::ObIEndTransCallback* cb_;
  int retcode_;
};

typedef common::ObArray<ObEndTransCallbackItem> ObEndTransCallbackArray;

class ObEndTransCallback {
public:
  ObEndTransCallback() : callback_count_(0), cb_(NULL), cb_type_(NULL), tenant_id_(common::OB_INVALID_TENANT_ID)
  {}
  ~ObEndTransCallback()
  {
    destroy();
  }
  int init(const uint64_t tenant_id, sql::ObIEndTransCallback* cb);
  void reset();
  void destroy();
  bool need_callback() const
  {
    return NULL != cb_;
  }
  int callback(const int cb_param);
  int64_t to_string(char* buf, const int64_t buf_len) const;
  const char* get_cb_type()
  {
    return cb_type_;
  }
  sql::ObIEndTransCallback* get_cb() const
  {
    return cb_;
  }

private:
  int64_t callback_count_;
  sql::ObIEndTransCallback* cb_;
  const char* cb_type_;
  uint64_t tenant_id_;
};

class EndTransCallbackTask : public ObTransTask {
public:
  EndTransCallbackTask()
  {
    reset();
  }
  ~EndTransCallbackTask()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  int make(const int64_t task_type, const ObEndTransCallback& end_trans_cb, const int param,
      const MonotonicTs receive_gts_ts, const int64_t need_wait_interval_us);
  int callback(bool& has_cb);
  int64_t get_need_wait_us() const;
  static const int64_t MAX_NEED_WAIT_US = 500;
  TO_STRING_KV(K_(param), K_(trans_need_wait_wrap));

private:
  ObEndTransCallback end_trans_cb_;
  int param_;
  ObTransNeedWaitWrap trans_need_wait_wrap_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_END_TRANS_CALLBACK_
