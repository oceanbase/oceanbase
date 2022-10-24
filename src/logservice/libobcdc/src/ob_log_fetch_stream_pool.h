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
 * FetchStream Pool
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_FETCH_STREAM_POOL_H__
#define OCEANBASE_LIBOBCDC_OB_LOG_FETCH_STREAM_POOL_H__

#include "lib/objectpool/ob_small_obj_pool.h"     // ObSmallObjPool

#include "ob_log_ls_fetch_stream.h"               // FetchStream

namespace oceanbase
{
namespace libobcdc
{

class IFetchStreamPool
{
public:
  virtual ~IFetchStreamPool() {}

public:
  virtual int alloc(FetchStream *&fs) = 0;
  virtual int free(FetchStream *fs) = 0;
};

////////////////////// FetchStreamPool ///////////////////
class FetchStreamPool : public IFetchStreamPool
{
  typedef common::ObSmallObjPool<FetchStream> PoolType;
  static const int64_t DEFAULT_BLOCK_SIZE = 1L << 24;

public:
  FetchStreamPool();
  virtual ~FetchStreamPool();

public:
  int alloc(FetchStream *&fs);
  int free(FetchStream *fs);
  void print_stat();

public:
  int init(const int64_t cached_fs_count);
  void destroy();

private:
  PoolType pool_;

private:
  DISALLOW_COPY_AND_ASSIGN(FetchStreamPool);
};

}
}

#endif
