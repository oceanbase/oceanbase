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

#ifndef _OB_TABLE_RPC_IMPL_H
#define _OB_TABLE_RPC_IMPL_H 1
#include "ob_table.h"
#include "ob_table_service_client.h"
#include "share/table/ob_table_rpc_struct.h"
#include "share/table/ob_table_rpc_proxy.h"               // ObTableRpcProxy
namespace oceanbase
{
namespace table
{
class ObTableRpcImpl: public ObTable
{
public:
  int init(ObTableServiceClient &client, const ObString &table_name) override;
  void set_entity_factory(ObITableEntityFactory &entity_factory) override;
  /// executes an operation on a table
  int execute(const ObTableOperation &table_operation, const ObTableRequestOptions &request_options, ObTableOperationResult &result) override;
  /// executes a batch operation on a table as an atomic operation
  int batch_execute(const ObTableBatchOperation &batch_operation, const ObTableRequestOptions &request_options, ObITableBatchOperationResult &result) override;
  /// executes a query on a table
  int execute_query(const ObTableQuery &query, const ObTableRequestOptions &request_options, ObTableEntityIterator *&result) override;
  int execute_query_and_mutate(const ObTableQueryAndMutate &query_and_mutate, const ObTableRequestOptions &request_options, ObTableQueryAndMutateResult &result) override;
  /// executes a sync query on a table
  int query_start(const ObTableQuery& query, const ObTableRequestOptions &request_options, ObTableQuerySyncResult *&result) override;
  int query_next(const ObTableRequestOptions &request_options, ObTableQuerySyncResult *&result) override;
private:
  friend class ObTableServiceClientImpl;
  typedef ObSEArray<int64_t, 3> IdxArray;
  typedef common::hash::ObHashMap<int64_t, std::pair<int64_t, int64_t>, common::hash::NoPthreadDefendMode> BatchResultIdxMap;
  class GetServersTabletsFunc;
  class FreeIdxArrayFunc;
  typedef typename obrpc::ObTableRpcProxy::SSHandle<obrpc::OB_TABLE_API_EXECUTE_QUERY> QueryHandle;
  class QueryMultiResult: public ObTableEntityIterator
  {
  public:
    QueryMultiResult()
        :result_packet_count_(0)
    {}
    virtual ~QueryMultiResult();
    virtual int get_next_entity(const ObITableEntity *&entity) override;
    void reset();

    QueryHandle &get_handle() { return query_rpc_handle_; }
    ObTableQueryResult &get_one_result() { return one_result_; }
    int64_t get_result_count() const { return result_packet_count_; }
  private:
    QueryHandle query_rpc_handle_;
    ObTableQueryResult one_result_;
    int64_t result_packet_count_;
    // disallow copy
    DISALLOW_COPY_AND_ASSIGN(QueryMultiResult);
  };

  class QuerySyncMultiResult: public ObTableEntityIterator
  {
  public:
    QuerySyncMultiResult()
        :result_packet_count_(0)
    {}
    virtual ~QuerySyncMultiResult() { reset(); }
    virtual int get_next_entity(const ObITableEntity *&entity) override
    {
      int ret = OB_NOT_IMPLEMENT;
      UNUSED(entity);
      return ret;
    }
    void reset()
    {
      one_result_.reset();
      session_id_ = 0;
      has_more_ = false;
      result_packet_count_ = 0;
      server_pkt_ts_ = -1;
    }
    ObTableQuerySyncResult &get_one_result() { return one_result_; }
    int64_t get_result_count() const { return result_packet_count_; }
  public:
    ObTableQuerySyncResult one_result_;
    int64_t result_packet_count_;
    int64_t server_pkt_ts_; // for packet validation
    common::ObAddr server_addr_;
    int64_t session_id_;
    bool has_more_;
    DISALLOW_COPY_AND_ASSIGN(QuerySyncMultiResult);
  };

private:
  ObTableRpcImpl();
  virtual ~ObTableRpcImpl();
  int get_rowkeys(const ObTableBatchOperation &batch_operation, ObIArray<ObRowkey> &rowkeys);
  int aggregate_tablet_by_server(const ObIArray<share::ObTabletLocation> &tablets_locs,
                                 ObIArray<common::ObAddr> &servers,
                                 ObIArray<IdxArray> &tablets_per_server);
  int reorder_servers_results(const ObIArray<ObTableBatchOperationResult*> &servers_results,
                              const BatchResultIdxMap &result_idx_map,
                              int64_t results_count,
                              ObITableBatchOperationResult &result);
private:
  bool inited_;
  ObTableServiceClient *client_;
  char table_name_buf_[common::OB_MAX_TABLE_NAME_LENGTH];
  ObString table_name_;
  uint64_t table_id_;
  ObTableEntityFactory<ObTableEntity> default_entity_factory_;
  ObITableEntityFactory *entity_factory_;
  common::ObArenaAllocator arena_;
  obrpc::ObTableRpcProxy *rpc_proxy_;
  QueryMultiResult query_multi_result_;
  QuerySyncMultiResult query_sync_multi_result_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableRpcImpl);
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_RPC_IMPL_H */
