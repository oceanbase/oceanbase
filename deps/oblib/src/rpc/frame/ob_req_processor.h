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

#ifndef _OCEABASE_RPC_FRAME_OB_REQ_PROCESSOR_H_
#define _OCEABASE_RPC_FRAME_OB_REQ_PROCESSOR_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_string.h"
#include "lib/net/ob_addr.h"
#include "rpc/ob_request.h"

namespace oceanbase {

namespace common {
class ObDataBuffer;
}

namespace rpc {

namespace frame {
// All members of this class are thread separated, whereas the object
// of this class lives along with the server. Please ensure everything
// in the class works fine with work flows as follow:
//
// For each thread:
// ObReqProcessor processor;
// processor.init(...);
// processor.reuse(...);
// processor.process(...);
// processor.reuse(...);
// processor.process(...);
// ....
// processor.destory(...);
//
// todo(): shadow member ipacket_ for fear that it's been used
// after having response.
//
class ObReqProcessor {
public:
  ObReqProcessor();
  virtual ~ObReqProcessor()
  {}

  // called after the processor object is created
  virtual int init();
  // called when the processor object would be destroyed
  virtual void destroy()
  {}
  // called before the processor object would be reused
  virtual void reuse()
  {}

  virtual void set_ob_request(ObRequest& req);
  const ObRequest* get_ob_request() const
  {
    return req_;
  }

  void set_io_thread_mark()
  {
    io_thread_mark_ = true;
  }

  void set_task_mark()
  {
    task_mark_ = true;
  }

  bool has_task_mark()
  {
    return task_mark_;
  }

  int run();

  int get_process_ret() const
  {
    return process_ret_;
  }
  int64_t get_receive_timestamp() const;
  void set_receive_timestamp(int64_t receive_timestamp)
  {
    receive_timestamp_ = receive_timestamp;
  }
  int64_t get_run_timestamp() const;
  void set_run_timestamp(int64_t run_timestamp)
  {
    run_timestamp_ = run_timestamp;
  }
  int64_t get_enqueue_timestamp() const;
  void set_enqueue_timestamp(int64_t enqueue_timestamp)
  {
    enqueue_timestamp_ = enqueue_timestamp;
  }

protected:
  virtual int check_timeout()
  {
    return common::OB_SUCCESS;
  }
  virtual int deserialize() = 0;
  virtual int serialize() = 0;

  virtual int before_process();
  virtual int process() = 0;
  virtual int after_process();

  virtual int before_response();
  virtual int response(const int retcode) = 0;

  virtual void cleanup()
  {}

  virtual char* easy_alloc(int64_t size) const;

  common::ObAddr get_peer() const;

  virtual void wakeup_request();

  bool can_force_print(int process_ret) const
  {
    return (common::OB_SUCCESS != process_ret &&
            (common::OB_CONNECT_ERROR == process_ret || common::OB_IO_ERROR == process_ret ||
                common::OB_ERR_UNEXPECTED == process_ret || common::OB_ERR_DISTRIBUTED_NOT_SUPPORTED == process_ret ||
                common::OB_TIMEOUT == process_ret || common::OB_TRANS_TIMEOUT == process_ret ||
                common::OB_TRANS_STMT_TIMEOUT == process_ret || common::OB_TRANS_UNKNOWN == process_ret ||
                common::OB_TRANS_KILLED == process_ret || common::OB_TRANS_CTX_NOT_EXIST == process_ret));
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObReqProcessor);

protected:
  ObRequest* req_;
  // mark if request has wokenup
  bool req_has_wokenup_;
  bool conn_valid_;
  bool io_thread_mark_;
  bool task_mark_;
  // error may happen before process, ObRpcAPTaskSubmitP need return the error
  // code by setting before_process_ret_ to task_event.err_code_.
  int before_process_ret_;
  // share by all processors of this thread
  int process_ret_;

private:
  int64_t receive_timestamp_;
  int64_t run_timestamp_;
  int64_t enqueue_timestamp_;
};  // end of class ObReqProcessor

inline ObReqProcessor::ObReqProcessor()
    : req_(NULL),
      req_has_wokenup_(true),
      conn_valid_(true),
      io_thread_mark_(false),
      task_mark_(false),
      before_process_ret_(common::OB_SUCCESS),
      process_ret_(common::OB_SUCCESS),
      receive_timestamp_(0),
      run_timestamp_(0),
      enqueue_timestamp_(0)
{
  // empty
}

inline void ObReqProcessor::set_ob_request(ObRequest& req)
{
  if (!req_has_wokenup_) {
    RPC_FRAME_LOG(WARN, "previous request hasn't completed, terminate it now", K(req_));
    wakeup_request();
  }
  req_has_wokenup_ = false;
  req_ = &req;
  receive_timestamp_ = req.get_receive_timestamp();
  enqueue_timestamp_ = req.get_enqueue_timestamp();
}

inline int ObReqProcessor::init()
{
  return common::OB_SUCCESS;
}

inline int ObReqProcessor::before_process()
{
  return common::OB_SUCCESS;
}

inline int64_t ObReqProcessor::get_receive_timestamp() const
{
  return receive_timestamp_;
}

inline int64_t ObReqProcessor::get_run_timestamp() const
{
  return run_timestamp_;
}

inline int64_t ObReqProcessor::get_enqueue_timestamp() const
{
  return enqueue_timestamp_;
}

inline common::ObAddr ObReqProcessor::get_peer() const
{
  common::ObAddr peer;
  if (OB_ISNULL(req_)) {
    RPC_FRAME_LOG(ERROR, "req_ is invalid", K(req_));
  } else {
    if (conn_valid_) {
      peer = req_->get_peer();
    }
  }
  return peer;
}

}  // end of namespace frame
}  // end of namespace rpc
}  // end of namespace oceanbase

#endif /* _OCEABASE_RPC_FRAME_OB_REQ_PROCESSOR_H_ */
