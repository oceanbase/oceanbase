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
 *
 * Definition of K-V interface
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_STORE_SERVICE_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_STORE_SERVICE_H_

#include <string>
#include <vector>
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace libobcdc
{
struct ObSlice
{
public:
  ObSlice() : buf_(NULL), buf_len_(0) {}
  ObSlice(const char *buf, const int64_t buf_len) : buf_(buf), buf_len_(buf_len) {}
  ~ObSlice() { reset(); }

  void reset()
  {
    buf_ = NULL;
    buf_len_ = 0;
  }
  bool is_valid() { return (NULL != buf_ && buf_len_ > 0); }

public:
  const char *buf_;
  int64_t buf_len_;
};

class IObStoreService
{
public:
  virtual ~IObStoreService() {}
  virtual int init(const std::string &path) = 0;
  virtual void mark_stop_flag() = 0; // stop store service: won't handle more store task.
  virtual int close() = 0; // close store service: including actual storager

public:
  virtual int put(const std::string &key, const ObSlice &value) = 0;
  virtual int put(void *cf_handle, const std::string &key, const ObSlice &value) = 0;

  virtual int batch_write(void *cf_handle, const std::vector<std::string> &keys, const std::vector<ObSlice> &values) = 0;

  virtual int get(const std::string &key, std::string &value) = 0;
  virtual int get(void *cf_handle, const std::string &key, std::string &value) = 0;

  virtual int del(const std::string &key) = 0;
  virtual int del(void *cf_handle, const std::string &key) = 0;
  virtual int del_range(void *cf_handle, const std::string &begin_key, const std::string &end_key) = 0;
  virtual int compact_range(void *cf_handle, const std::string &begin_key, const std::string &end_key, const bool op_entire_cf = false) = 0;
  virtual int flush(void *cf_handle) = 0;

  virtual int create_column_family(const std::string& column_family_name,
      void *&cf_handle) = 0;
  virtual int drop_column_family(void *cf_handle) = 0;
  virtual int destory_column_family(void *cf_handle) = 0;

  virtual void get_mem_usage(const std::vector<uint64_t> ids,
      const std::vector<void *> cf_handles) = 0;
  virtual int get_mem_usage(void * cf_handle, int64_t &estimate_live_data_size, int64_t &estimate_num_keys) = 0;

};

}
}

#endif
