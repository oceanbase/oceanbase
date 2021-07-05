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

#ifndef OCEANBASE_CLOG_OB_I_DISK_LOG_BUFFER_H_
#define OCEANBASE_CLOG_OB_I_DISK_LOG_BUFFER_H_
#include "ob_buffer_task.h"

namespace oceanbase {
namespace clog {
class ObDiskBufferTask : public ObIBufferTask {
public:
  ObDiskBufferTask() : proposal_id_(), buf_(NULL), len_(0), offset_(0)
  {
    need_callback_ = true;
  }

public:
  void reset();
  void set(char* buf, const int64_t len);
  common::ObProposalID get_proposal_id() const
  {
    return proposal_id_;
  }
  char* get_data_buffer() const
  {
    return buf_;
  }
  int64_t get_data_len() const
  {
    return len_;
  }
  int64_t get_entry_cnt() const
  {
    return 1;
  }
  // st == single thread
  virtual int st_after_consume(const int handle_err) = 0;

protected:
  int fill_buffer(char* buf, const offset_t offset);

protected:
  common::ObProposalID proposal_id_;
  char* buf_;
  int64_t len_;
  offset_t offset_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDiskBufferTask);
};

class ObIDiskLogBuffer {
public:
  ObIDiskLogBuffer()
  {}
  virtual ~ObIDiskLogBuffer()
  {}

private:
  virtual int submit(ObIBufferTask* task) = 0;
};
};  // end namespace clog
};  // end namespace oceanbase

#endif /* OCEANBASE_CLOG_OB_I_DISK_LOG_BUFFER_H_ */
