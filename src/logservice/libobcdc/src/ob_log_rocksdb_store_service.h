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
 * OBCDC Storage based on rocksdb
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_ROCKSDB_IMPL_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_ROCKSDB_IMPL_H_

#include "ob_log_store_service.h"
#include "lib/atomic/ob_atomic.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"

namespace oceanbase
{
namespace libobcdc
{
class RocksDbStoreService : public IObStoreService
{
public:
  RocksDbStoreService();
  virtual ~RocksDbStoreService();
  int init(const std::string &path);
  void destroy();

public:
  // Default ColumnFamily put/get/del
  // Assign ColumnFamily put/get/del
  virtual int put(const std::string &key, const ObSlice &value);
  virtual int put(void *cf_handle, const std::string &key, const ObSlice &value);

  virtual int batch_write(void *cf_handle, const std::vector<std::string> &keys, const std::vector<ObSlice> &values);

  virtual int get(const std::string &key, std::string &value);
  virtual int get(void *cf_handle, const std::string &key, std::string &value);

  virtual int del(const std::string &key);
  virtual int del(void *cf_handle, const std::string &key);
  virtual int del_range(void *cf_handle, const std::string &begin_key, const std::string &end_key);
  virtual int compact_range(
      void *cf_handle,
      const std::string &begin_key,
      const std::string &end_key,
      const bool op_entire_cf = false);
  virtual int flush(void *cf_handle);

  virtual int create_column_family(const std::string& column_family_name,
      void *&cf_handle);
  virtual int drop_column_family(void *cf_handle);
  virtual int destory_column_family(void *cf_handle);

  virtual void mark_stop_flag() override { ATOMIC_SET(&is_stopped_, true); }
  virtual int close() override;
  virtual void get_mem_usage(const std::vector<uint64_t> ids,
      const std::vector<void *> cf_handles);
  virtual int get_mem_usage(void * cf_handle, int64_t &estimate_live_data_size, int64_t &estimate_num_keys);
  OB_INLINE bool is_stopped() const { return ATOMIC_LOAD(&is_stopped_); }

private:
  int init_dir_(const char *dir_path);

private:
  bool is_inited_;
  bool is_stopped_;
  rocksdb::DB *m_db_;
  rocksdb::Options m_options_;
  std::string m_db_path_;
};

}
}

#endif
