

/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_I_ASYNC_QUERY_ITER_H
#define _OB_I_ASYNC_QUERY_ITER_H

#include "observer/table/common/ob_table_common_struct.h"

namespace oceanbase
{
namespace table
{
class ObIAsyncQueryIter
{
public:
  ObIAsyncQueryIter() = default;
  virtual ~ObIAsyncQueryIter() = default;
  virtual int start(const ObTableQueryAsyncRequest &req, ObTableExecCtx &exec_ctx, ObTableQueryAsyncResult &result) = 0;
  virtual int next(ObTableExecCtx &exec_ctx, ObTableQueryAsyncResult &result) = 0;
  virtual int renew(ObTableQueryAsyncResult &result) = 0;
  virtual int end(ObTableQueryAsyncResult &result) = 0;
  virtual uint64_t get_session_time_out_ts() const = 0;
  virtual uint64_t get_lease_timeout_period() const = 0;
};

} // end of namespace table
} // end of namespace oceanbase

#endif // _OB_HBASE_COLUMN_FAMILY_SERVICE_H
