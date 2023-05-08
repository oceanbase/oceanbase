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

#ifndef OCEANBASE_LIBOBCDC_STORE_TASK_H_
#define OCEANBASE_LIBOBCDC_STORE_TASK_H_

#include "ob_log_batch_buffer_task.h"  // IObLogBufTask
#include "ob_log_callback.h"           // ObILogCallback
#include "ob_log_store_key.h"          // ObLogStoreKey

namespace oceanbase
{
namespace libobcdc
{
class ObLogStoreTask : public IObLogBufTask
{
public:
  ObLogStoreTask();
  ~ObLogStoreTask();
  void reset();
  int init(const logservice::TenantLSID &tenant_ls_id,
      const palf::LSN &log_lsn,
      const char *data_buf,
      const int64_t data_len,
      ObILogCallback *log_callback);
  void destroy() { reset(); }

  bool is_valid() const;
  int64_t get_data_len() const { return data_len_; }
  int64_t get_entry_cnt() const { return 1; }
  int fill_buffer(char *buf, const offset_t offset);
  int st_after_consume(const int handle_err);

public:
  static const int64_t RP_MAX_FREE_LIST_NUM = 10240;

  ObLogStoreKey &get_store_key() { return store_key_; }
  const char *get_data_buffer() const { return data_buf_; }
  offset_t get_offset() const { return offset_; }

  TO_STRING_KV(K_(store_key),
      K_(data_len),
      K_(offset));

private:
  bool is_inited_;
  ObLogStoreKey store_key_;
  const char *data_buf_;
  int64_t data_len_;
  offset_t offset_;  // Fill Batch buf offset
  ObILogCallback *log_callback_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStoreTask);
};

}; // end namespace libobcdc
}; // end namespace oceanbase
#endif
