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
 
#ifndef _OB_TABLE_QUERY_AND_MUTATE_PROCESSOR_H
#define _OB_TABLE_QUERY_AND_MUTATE_PROCESSOR_H 1
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/table/ob_table_rpc_proxy.h"
#include "ob_table_rpc_processor.h"
#include "ob_table_service.h"

namespace oceanbase
{
namespace observer
{
class ObTableQueryAndMutateP: public ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_QUERY_AND_MUTATE> >
{
  typedef ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_QUERY_AND_MUTATE> > ParentType;
public:
  explicit ObTableQueryAndMutateP(const ObGlobalContext &gctx);
  virtual ~ObTableQueryAndMutateP() {}

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
  int check_rowkey_and_generate_mutations(ObTableQueryResult &one_row, ObTableBatchOperation *&mutations);
  // rewrite htable query to avoid lock too much rows for update
  int rewrite_htable_query_if_need(const ObTableOperation &mutaion, ObTableQuery &query);
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryAndMutateP);
private:
  common::ObArenaAllocator allocator_;
  table::ObTableEntityFactory<table::ObTableEntity> default_entity_factory_;
  ObTableServiceQueryCtx query_ctx_;
  table::ObTableQueryResult one_result_;
};
} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_QUERY_AND_MUTATE_PROCESSOR_H */
