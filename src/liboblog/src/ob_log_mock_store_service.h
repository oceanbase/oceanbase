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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_MOCK_STORE_SERVICE_H_
#define OCEANBASE_LIBOBLOG_OB_LOG_MOCK_STORE_SERVICE_H_

#include "ob_log_store_service.h"

namespace oceanbase
{
namespace liboblog
{
class MockObLogStoreService : public IObStoreService
{
public:
  MockObLogStoreService() {}
  void destroy() {}
  ~MockObLogStoreService() { destroy(); }
  int init(const std::string &path) { UNUSED(path); return 0; }
  int close() { return 0; }
public:
  int put(const std::string &key, const ObSlice &value)
  {
    UNUSED(key);
    UNUSED(value);
    return 0;
  }

  int put(void *cf_handle, const std::string &key, const ObSlice &value)
  {
    UNUSED(key);
    UNUSED(value);
    UNUSED(cf_handle);
    return 0;
  }

  int batch_write(void *cf_handle, const std::vector<std::string> &keys, const std::vector<ObSlice> &values)
  {
    UNUSED(cf_handle);
    UNUSED(keys);
    UNUSED(values);
    return 0;
  }

  int get(const std::string &key, std::string &value)
  {
    UNUSED(key);
    UNUSED(value);
    return 0;
  }

  int get(void *cf_handle, const std::string &key, std::string &value)
  {
    UNUSED(cf_handle);
    UNUSED(key);
    UNUSED(value);
    return 0;
  }

  int del(const std::string &key)
  {
    UNUSED(key);
    return 0;
  }

  int del(void *cf_handle, const std::string &key)
  {
    UNUSED(cf_handle);
    UNUSED(key);
    return 0;
  }

  int create_column_family(const std::string& column_family_name,
      void *&cf_handle)
  {
    UNUSED(column_family_name);
    cf_handle = this;
    return 0;
  }

  int drop_column_family(void *cf_handle)
  {
    UNUSED(cf_handle);
    return 0;
  }

  int destory_column_family(void *cf_handle)
  {
    UNUSED(cf_handle);
    return 0;
  }

  void get_mem_usage(const std::vector<uint64_t> ids,
      const std::vector<void *> cf_handles)
  {
    UNUSED(ids);
    UNUSED(cf_handles);
  }

};

}
}


#endif
