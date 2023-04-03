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
#include "storage/tx/ob_trans_result.h"
#include "storage/tx/ob_trans_define.h"
#include "observer/mysql/ob_mysql_end_trans_cb.h"

namespace oceanbase
{
namespace sql
{
// 同一个对象可能在同一时间被多个end trans函数共享，可能会发生并发地调用同一个对象的callback函数的情况
// deprecated, only used in NullEndTransCallback and will be removed in 4.0
class ObSharedEndTransCallback : public ObIEndTransCallback
{
public:
  ObSharedEndTransCallback();
  virtual ~ObSharedEndTransCallback();

  virtual bool is_shared() const override final { return true; }
};

// 一个对象同一时间只能被一个end trans函数独享，同一个对象只可能被串行地调用callback函数
class ObExclusiveEndTransCallback : public ObIEndTransCallback
{
public:
  enum EndTransType
  {
    END_TRANS_TYPE_INVALID,
    END_TRANS_TYPE_EXPLICIT, // 显式提交
    END_TRANS_TYPE_IMPLICIT, // 隐式提交
  };
public:
  ObExclusiveEndTransCallback();
  virtual ~ObExclusiveEndTransCallback();

  virtual bool is_shared() const override final { return false; }

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
  bool is_txs_end_trans_called() const { return is_txs_end_trans_called_; }
  void reset()
  {
    ObIEndTransCallback::reset();
    end_trans_type_ = END_TRANS_TYPE_INVALID;
    has_set_need_rollback_ = false;
    is_need_rollback_ = false;
    is_txs_end_trans_called_ = false;
  }
protected:
  ObExclusiveEndTransCallback::EndTransType end_trans_type_; // 表示是显式还是隐式的commit或rollback
  bool has_set_need_rollback_;
  bool is_need_rollback_;
  bool is_txs_end_trans_called_; // 事务模块的end trans接口是否已经调用了（正在调用过程中也算，并且调用参数不一定传的是本callback对象）
};

class ObEndTransAsyncCallback : public ObExclusiveEndTransCallback
{
public:
  ObEndTransAsyncCallback();
  virtual ~ObEndTransAsyncCallback();
  virtual void callback(int cb_param);
  virtual void callback(int cb_param, const transaction::ObTransID &trans_id);
  virtual const char *get_type() const { return "ObEndTransAsyncCallback"; }
  virtual ObEndTransCallbackType get_callback_type() const { return ASYNC_CALLBACK_TYPE; }
  observer::ObSqlEndTransCb &get_mysql_end_trans_cb() { return mysql_end_trans_cb_; }
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

}
}


#endif /* __OB_SQL_END_TRANS_CALLBACK_H__ */
//// end of header file
