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

#ifndef _OB_TABLE_QUERY_PROCESSOR_H
#define _OB_TABLE_QUERY_PROCESSOR_H 1
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/table/ob_table_rpc_proxy.h"
#include "ob_table_rpc_processor.h"
#include "ob_table_service.h"

namespace oceanbase
{
namespace observer
{
class ObTableQueryP: public ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_EXECUTE_QUERY> >
{
  typedef ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_EXECUTE_QUERY> > ParentType;
public:
  explicit ObTableQueryP(const ObGlobalContext &gctx);
  virtual ~ObTableQueryP() {}

  virtual int deserialize() override;
protected:
  virtual int check_arg() override;
  virtual int try_process() override;
  virtual void reset_ctx() override;
  virtual table::ObTableAPITransCb *new_callback(rpc::ObRequest *req) override;
  virtual void audit_on_finish() override;
  virtual uint64_t get_request_checksum() override;

private:
  int get_partition_ids(uint64_t table_id, common::ObIArray<int64_t> &part_ids);
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryP);
private:
  common::ObArenaAllocator allocator_;
  ObTableServiceQueryCtx table_service_ctx_;
  int64_t result_row_count_;
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_QUERY_PROCESSOR_H */
