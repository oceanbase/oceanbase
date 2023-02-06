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

#ifndef __OB_SQL_I_END_TRANS_CALLBACK_H__
#define __OB_SQL_I_END_TRANS_CALLBACK_H__

#include "share/ob_define.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/utility.h"
#include "storage/tx/ob_trans_end_trans_callback.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{

namespace transaction {
  class ObTransID;
  class ObITxCallback;
}

namespace sql
{

enum ObEndTransCallbackType
{
  SYNC_CALLBACK_TYPE = 0, /* 同步等待，如DDL语句 */
  ASYNC_CALLBACK_TYPE, /* 异步执行事务提交后的操作，如DML语句，COMMINT/ROLLBACK语句对客户端发送执行结果 */
  // NO_CALLBACK_TYPE, /* 用户中途主动断开链接时会rollback事务, 这种情况也采取SYNC模式较为方便 */
  NULL_CALLBACK_TYPE,
  //SQL_CALLBACK_TYPE,
  WRITE_FROZEN_STAT_CALLBACK_TYPE,
  MOCK_CALLBACK_TYPE,
  MAX_CALLBACK_TYPE
};


/* ObIEndTransCallback的生命周期：
 * 思路1.  在StartTrans的时候创建，在EndTrans调用结束后终止
 *   - disconnect模式下，采取同步等待。这个动作在StartTrans的时候无法预知
 * 思路2.  在调用EndTrans之前创建，在调用EndTrans之后终止
 *   - 需要知道当前操作类型(ac=0/1, commit/rollback, dml, disconnect)
 *
 * explicit_end_trans (commit/rollback), on_plan_end (ac=1 dml)两种情况需要从外部传入ObIEndTransCallback，
 * 其余情况都使用sync模式，内部构造和释放ObIEndTransCallback
 *
 * 调用end_trans后，回调发生前，是否还可以做【可能会产生错误】的操作？
 *  - 所谓【可能会产生错误】的操作，是指调用了函数，并获得一个错误码，影响SQL错误输出。
 *  - 假设做了这样的操作，产生了错误码，那么这个错误码必须被保存，否则就被吞掉了。
 *    只能保存到Callback中。但这存在多线程问题：保存错误码的时候Callback已经调用了怎么办？
 *  - 比较简单的方式：end_trans后什么都不做，其余操作全部都放到回调中执行。
 *
 */
class ObIEndTransCallback : public transaction::ObITxCallback
{
public:
  ObIEndTransCallback() { reset(); }
  virtual ~ObIEndTransCallback() {};
  /*
   * 部分场景下（如cmd触发隐式提交）需要同步等待callback调用成功后
   * 才能继续后面的操作，所以引入wait()操作
   */
  virtual int wait() { return common::OB_NOT_IMPLEMENT; }
  /*
   * 由事务完成后调用，具体动作由对象本身定义
   * 如：唤醒wait(); 向客户端回包; 向RPC调用者回包等
   */
  virtual void callback(int cb_param) = 0;
  virtual void callback(int cb_param, const transaction::ObTransID &trans_id) = 0;
  virtual const char *get_type() const = 0;
  virtual ObEndTransCallbackType get_callback_type() const = 0;
  // 表示是否可能同一时间同一个callback对象被多个end trans函数共享，可能并发地调用同一callback对象的callback函数
  virtual bool is_shared() const = 0;

  /*
   * 设置外部错误码，事务完成后回调时参考callback(cb_param)中cb_param
   * 指定的错误码，以及本外部错误码，返回首现错误码
   */
  void set_last_error(int last_err) { last_err_ = last_err; }

  /*
   * 调用ps->end_trans成功后调用
   */
  inline void handout() { ATOMIC_INC(&call_counter_); }
  /*
   * 事务层触发cb.callback()后立即在callback中调用
   */
  inline void handin() { ATOMIC_INC(&callback_counter_);}
  inline void reset()
  {
    last_err_ = common::OB_SUCCESS;
    call_counter_ = 0;
    callback_counter_ = 0;
  }
protected:
  // 为了检查是否存在重复调用callback，或漏调callback的问题
  inline void CHECK_BALANCE(const char *type) const
  {
    if (OB_UNLIKELY(callback_counter_ != call_counter_)) {
      SQL_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "Callback times mismatch. bug!!!",
              K(type), K(this), K(common::lbt()), K_(callback_counter), K_(call_counter));
    }
  }
protected:
  int last_err_;
  volatile uint64_t call_counter_; // 调用ps->end_trans成功的次数
  volatile uint64_t callback_counter_; // 调用callback的次数
};

} /*ns*/
}/*ns */

#endif /* __OB_SQL_I_END_TRANS_CALLBACK_H__ */
//// end of header file
