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
 * Binlog Record Pool
 */

#ifndef OCEANBASE_SRC_LIBOBLOG_OB_LOG_BINLOG_RECORD_POOL_
#define OCEANBASE_SRC_LIBOBLOG_OB_LOG_BINLOG_RECORD_POOL_

#include "ob_log_binlog_record.h"               // ObLogBR
#include "lib/objectpool/ob_small_obj_pool.h"   // ObSmallObjPool

namespace oceanbase
{
namespace libobcdc
{
class IObLogBRPool
{
public:
  virtual ~IObLogBRPool() {}

public:
  // If host is valid, then set host to binlog record: ObLogBR::set_host()
  virtual int alloc(ObLogBR *&br, void *host = nullptr, void *stmt_task = nullptr) = 0;
  virtual void free(ObLogBR *br) = 0;
  virtual void print_stat_info() const = 0;
};

//////////////////////////////////////////////////////////////////////////////

class ObLogBRPool : public IObLogBRPool
{
  typedef common::ObSmallObjPool<ObLogUnserilizedBR> UnserilizedBRObjPool;

public:
  ObLogBRPool();
  virtual ~ObLogBRPool();

public:
  int alloc(ObLogBR *&br, void *host = nullptr, void *stmt_task = nullptr);
  void free(ObLogBR *br);
  void print_stat_info() const;

public:
  int init(const int64_t fixed_br_count);
  void destroy();

private:
  bool        inited_;
  UnserilizedBRObjPool   unserilized_pool_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogBRPool);
};

} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_SRC_LIBOBLOG_OB_LOG_BINLOG_RECORD_POOL_ */
