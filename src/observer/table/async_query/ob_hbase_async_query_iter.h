/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_I_HBASE_ASYNC_QUERY_ITER_H
#define _OB_I_HBASE_ASYNC_QUERY_ITER_H

#include "ob_i_async_query_iter.h"
#include "observer/table/cf_service/ob_hbase_column_family_service.h"
#include "observer/table/common/ob_hbase_common_struct.h"

namespace oceanbase
{
namespace table
{

class ObHbaseAsyncQueryIter : public ObIAsyncQueryIter
{
public:
  ObHbaseAsyncQueryIter();
  virtual ~ObHbaseAsyncQueryIter();
  virtual int start(const ObTableQueryAsyncRequest &req, ObTableExecCtx &exec_ctx, ObTableQueryAsyncResult &result) override;
  virtual int next(ObTableExecCtx &exec_ctx, ObTableQueryAsyncResult &result) override;
  virtual int renew(ObTableQueryAsyncResult &result) override;
  virtual int end(ObTableQueryAsyncResult &result) override;
  virtual uint64_t get_session_time_out_ts() const override;
  uint64_t get_lease_timeout_period() const override;
private:
  int init_query_and_sel_cols(const ObTableQueryAsyncRequest &req, ObTableExecCtx &exec_ctx);
private:
  common::ObArenaAllocator allocator_;
  uint64_t lease_timeout_period_;
  ObHbaseQueryResultIterator *result_iter_;
  ObHbaseCfServiceGuard *cf_service_guard_;
  ObTableQuery query_;
  ObHbaseQuery *hbase_query_;
};

} // end of namespace table
} // end of namespace oceanbase

#endif // _OB_HBASE_COLUMN_FAMILY_SERVICE_H
