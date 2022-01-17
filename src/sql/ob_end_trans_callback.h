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

#ifndef __OB_SQL_END_TRANS_CALLBACK_H__
#define __OB_SQL_END_TRANS_CALLBACK_H__

#include "share/ob_define.h"
#include "lib/utility/utility.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/objectpool/ob_tc_factory.h"
#include "lib/allocator/ob_mod_define.h"
#include "sql/ob_i_end_trans_callback.h"
#include "storage/transaction/ob_trans_result.h"
#include "storage/transaction/ob_trans_define.h"
#include "observer/mysql/ob_mysql_end_trans_cb.h"

namespace oceanbase {
namespace sql {
class ObSharedEndTransCallback : public ObIEndTransCallback {
public:
  ObSharedEndTransCallback();
  virtual ~ObSharedEndTransCallback();

  virtual bool is_shared() const override final
  {
    return true;
  }
};

class ObExclusiveEndTransCallback : public ObIEndTransCallback {
public:
  enum EndTransType {
    END_TRANS_TYPE_INVALID,
    END_TRANS_TYPE_EXPLICIT,  // Explicit commit
    END_TRANS_TYPE_IMPLICIT,  // Implicit commit
  };

public:
  ObExclusiveEndTransCallback();
  virtual ~ObExclusiveEndTransCallback();

  virtual bool is_shared() const override final
  {
    return false;
  }

  void set_is_need_rollback(bool is_need_rollback)
  {
    has_set_need_rollback_ = true;
    is_need_rollback_ = is_need_rollback;
  }
  void set_end_trans_type(ObExclusiveEndTransCallback::EndTransType end_trans_type)
  {
    end_trans_type_ = end_trans_type;
  }
  void set_is_txs_end_trans_called(bool is_txs_end_trans_called)
  {
    is_txs_end_trans_called_ = is_txs_end_trans_called;
  }
  bool is_txs_end_trans_called() const
  {
    return is_txs_end_trans_called_;
  }
  void reset()
  {
    ObIEndTransCallback::reset();
    end_trans_type_ = END_TRANS_TYPE_INVALID;
    has_set_need_rollback_ = false;
    is_need_rollback_ = false;
    is_txs_end_trans_called_ = false;
  }

protected:
  ObExclusiveEndTransCallback::EndTransType end_trans_type_;
  bool has_set_need_rollback_;
  bool is_need_rollback_;
  bool is_txs_end_trans_called_;
};

class ObEndTransSyncCallback : public ObExclusiveEndTransCallback {
public:
  ObEndTransSyncCallback();
  virtual ~ObEndTransSyncCallback();
  virtual void callback(int cb_param);
  virtual void callback(int cb_param, const transaction::ObTransID& trans_id);
  virtual const char* get_type() const
  {
    return "ObEndTransSyncCallback";
  }
  virtual ObEndTransCallbackType get_callback_type() const
  {
    return SYNC_CALLBACK_TYPE;
  }
  int wait();
  int init(const transaction::ObTransDesc* trans_desc, ObSQLSessionInfo* session);
  void reset();

private:
  /* functions */

  /* variables */
  static const int64_t PUSH_POP_TIMEOUT = 1000 * 1000 * 10L;
  transaction::ObTransCond queue_;
  const transaction::ObTransDesc* trans_desc_;
  ObSQLSessionInfo* session_;
  /* macro */
  DISALLOW_COPY_AND_ASSIGN(ObEndTransSyncCallback);
};

class ObEndTransAsyncCallback : public ObExclusiveEndTransCallback {
public:
  ObEndTransAsyncCallback();
  virtual ~ObEndTransAsyncCallback();
  virtual void callback(int cb_param);
  virtual void callback(int cb_param, const transaction::ObTransID& trans_id);
  virtual const char* get_type() const
  {
    return "ObEndTransAsyncCallback";
  }
  virtual ObEndTransCallbackType get_callback_type() const
  {
    return ASYNC_CALLBACK_TYPE;
  }
  observer::ObSqlEndTransCb& get_mysql_end_trans_cb()
  {
    return mysql_end_trans_cb_;
  }
  void reset()
  {
    ObExclusiveEndTransCallback::reset();
    mysql_end_trans_cb_.reset();
  }

private:
  /* macro */
  observer::ObSqlEndTransCb mysql_end_trans_cb_;
  DISALLOW_COPY_AND_ASSIGN(ObEndTransAsyncCallback);
};

class ObNullEndTransCallback : public ObSharedEndTransCallback {
public:
  ObNullEndTransCallback() : ObSharedEndTransCallback()
  {}
  virtual ~ObNullEndTransCallback()
  {}
  virtual void callback(int cb_param)
  {
    UNUSED(cb_param);
  }
  virtual void callback(int cb_param, const transaction::ObTransID& trans_id)
  {
    UNUSED(cb_param);
    UNUSED(trans_id);
  }
  virtual const char* get_type() const
  {
    return "ObNullEndTransCallback";
  }
  virtual ObEndTransCallbackType get_callback_type() const
  {
    return NULL_CALLBACK_TYPE;
  }
  virtual int init(const transaction::ObTransID& trans_id);

private:
  DISALLOW_COPY_AND_ASSIGN(ObNullEndTransCallback);

private:
  transaction::ObTransID trans_id_;
};

}  // namespace sql
}  // namespace oceanbase

#endif /* __OB_SQL_END_TRANS_CALLBACK_H__ */
//// end of header file
