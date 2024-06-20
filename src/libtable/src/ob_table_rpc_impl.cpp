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

#define USING_LOG_PREFIX CLIENT
#include "ob_table_rpc_impl.h"
#include "share/table/ob_table_rpc_proxy.h"               // ObTableRpcProxy
#include "share/ob_tenant_mgr.h"
using namespace oceanbase::table;
using namespace oceanbase::common;
using namespace oceanbase::share;

ObTableRpcImpl::ObTableRpcImpl()
    :inited_(false),
     client_(NULL),
     table_id_(OB_INVALID_ID),
     default_entity_factory_(ObModIds::TEST),
     entity_factory_(&default_entity_factory_),
     arena_(ObModIds::TABLE_CLIENT),
     rpc_proxy_(NULL)
{
  table_name_buf_[0] = '\0';
}

ObTableRpcImpl::~ObTableRpcImpl()
{}

int ObTableRpcImpl::init(ObTableServiceClient &client, const ObString &table_name)
{
  int ret = OB_SUCCESS;
  ObDataBuffer buff(table_name_buf_, ARRAYSIZEOF(table_name_buf_));
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(client.get_table_id(table_name, table_id_))) {
    LOG_WARN("failed to get table id", K(ret), K(table_name));
  } else if (OB_FAIL(ob_write_string(buff, table_name, table_name_))) {
    LOG_WARN("failed to store table name", K(ret), K(table_name));
  } else {
    client_ = &client;
    rpc_proxy_ = &client.get_table_rpc_proxy();
    LOG_DEBUG("init table succ", K_(table_name), K_(table_id));
    inited_ = true;
  }
  return ret;
}

void ObTableRpcImpl::set_entity_factory(ObITableEntityFactory &entity_factory)
{
  entity_factory_ = &entity_factory;
}

int ObTableRpcImpl::execute(const ObTableOperation &table_operation, const ObTableRequestOptions &request_options, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObITableEntity &entity = const_cast<ObITableEntity&>(table_operation.entity());
    ObRowkey rowkey = entity.get_rowkey();
    ObTabletLocation tablet_loc;
    ObTabletReplicaLocation leader_loc;
    uint64_t table_id = OB_INVALID_ID;
    ObTabletID tablet_id;
    if (OB_FAIL(client_->get_tablet_location(table_name_, rowkey, tablet_loc, table_id, tablet_id))) {
      LOG_WARN("failed to get tablet", K(ret), K_(table_name), K(rowkey));
    } else if (OB_FAIL(tablet_loc.get_leader(leader_loc))) {
      LOG_WARN("failed to find leader location", K(ret), K(tablet_loc));
    } else {
      entity_factory_->free_and_reuse();
      arena_.reset_remain_one_page();
      ObITableEntity *result_entity = NULL;
      if (NULL == (result_entity = entity_factory_->alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no memory for result entity", K(ret));
      } else {
        result_entity->set_allocator(&arena_); // have to deep copy values from the rpc memory
        result.set_entity(*result_entity);
        result.set_type(table_operation.type());

        ObTableOperationRequest request;
        request.table_operation_ = table_operation;
        request.table_name_ = table_name_;
        request.table_id_ = table_id_;
        request.tablet_id_ = tablet_id;
        request.credential_ = client_->get_credential();
        request.entity_type_ = this->entity_type_;
        request.consistency_level_ = request_options.consistency_level();
        request.returning_affected_rows_ = request_options.returning_affected_rows();
        request.returning_affected_entity_ = request_options.returning_affected_entity();
        request.option_flag_ = request_options.get_option_flag();

        ret = rpc_proxy_->
              timeout(request_options.server_timeout())
              .to(leader_loc.get_server())
              .execute(request, result);
      }
    }
  }
  return ret;
}

int ObTableRpcImpl::get_rowkeys(const ObTableBatchOperation &batch_operation, ObIArray<ObRowkey> &rowkeys)
{
  int ret = OB_SUCCESS;
  const int64_t N = batch_operation.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    const ObTableOperation &table_op = batch_operation.at(i);
    ObRowkey rowkey = const_cast<ObITableEntity&>(table_op.entity()).get_rowkey();
    if (OB_FAIL(rowkeys.push_back(rowkey))) {
      LOG_WARN("failed to push back", K(ret));
    }
  } // end for
  return ret;
}

class ObTableRpcImpl::GetServersTabletsFunc
{
public:
  GetServersTabletsFunc(ObIArray<ObAddr> &servers, ObIArray<IdxArray> &tablets_per_server)
      :servers_(servers),
       tablets_per_server_(tablets_per_server)
  {}
  int operator()(const hash::HashMapPair<ObAddr, IdxArray*> &kv)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(servers_.push_back(kv.first))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(tablets_per_server_.push_back(*kv.second))) {
      LOG_WARN("failed to push back", K(ret));
    }
    return ret;
  }
private:
  ObIArray<ObAddr> &servers_;
  ObIArray<IdxArray> &tablets_per_server_;
};

class ObTableRpcImpl::FreeIdxArrayFunc
{
public:
  int operator()(const hash::HashMapPair<ObAddr, IdxArray*> &kv)
  {
    if (NULL != kv.second) {
      IdxArray *idx_array = kv.second;
      ob_delete(idx_array);
    }
    return OB_SUCCESS;
  }
};

int ObTableRpcImpl::aggregate_tablet_by_server(const ObIArray<ObTabletLocation> &tablets_locs, ObIArray<ObAddr> &servers, ObIArray<IdxArray> &tablets_per_server)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<ObAddr, IdxArray*, hash::NoPthreadDefendMode> server_tablet_map;
  static const int64_t MAP_BUCKET_SIZE = 107;
  if (OB_FAIL(server_tablet_map.create(MAP_BUCKET_SIZE, ObModIds::TABLE_CLIENT, ObModIds::TABLE_CLIENT))) {
    LOG_WARN("failed to init hash map", K(ret));
  }
  const int64_t N = tablets_locs.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    ObTabletReplicaLocation leader_loc;
    IdxArray* idx_array = NULL;
    if (OB_FAIL(tablets_locs.at(i).get_leader(leader_loc))) {
      LOG_WARN("failed to find leader location", K(ret), "tablet_loc", tablets_locs.at(i));
    } else if (OB_FAIL(server_tablet_map.get_refactored(leader_loc.get_server(), idx_array))) {
      if (OB_HASH_NOT_EXIST == ret) {
        idx_array = OB_NEW(IdxArray, ObModIds::TABLE_CLIENT,
                           ObModIds::TABLE_CLIENT, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
        if (NULL == idx_array) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("no memory", K(ret));
        } else {
          if (OB_FAIL(idx_array->push_back(i))) {
            LOG_WARN("failed to push back", K(ret));
          } else if (OB_FAIL(server_tablet_map.set_refactored(leader_loc.get_server(), idx_array, 0))) {
            LOG_WARN("failed to set hash", K(ret));
          }
          if (OB_FAIL(ret)) {
            ob_delete(idx_array);
            idx_array = NULL;
          }
        }
      } else {
        LOG_WARN("failed to get hash", K(ret));
      }
    } else if (OB_FAIL(idx_array->push_back(i))) {
      LOG_WARN("failed to push back", K(ret));
    }
  } // end for
  if (OB_SUCC(ret)) {
    GetServersTabletsFunc get_servers_tablets_fn(servers, tablets_per_server);
    if (OB_FAIL(server_tablet_map.foreach_refactored(get_servers_tablets_fn))) {
      LOG_WARN("failed to do foreach", K(ret));
    }
  }

  int tmp_ret = ret;
  FreeIdxArrayFunc free_idx_array_fn;
  if (OB_FAIL(server_tablet_map.foreach_refactored(free_idx_array_fn))) {
    //overwrite ret
    LOG_WARN("failed to do foreach", K(ret));
  }
  ret = OB_SUCCESS == tmp_ret ? ret : tmp_ret;
  return ret;
}

int ObTableRpcImpl::reorder_servers_results(const ObIArray<ObTableBatchOperationResult*> &servers_results,
                                            const BatchResultIdxMap &result_idx_map,
                                            int64_t results_count,
                                            ObITableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  // @todo Should I deal with partial results ?
  for (int64_t i = 0; OB_SUCCESS == ret && i < results_count; ++i)
  {
    std::pair<int64_t, int64_t> server_result_idx;
    if (OB_FAIL(result_idx_map.get_refactored(i, server_result_idx))) {
      LOG_WARN("result not exists", K(ret), K(i));
    } else if (server_result_idx.first < 0 || server_result_idx.first >= servers_results.count()) {
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_WARN("invalid server index", K(ret), "idx", server_result_idx.first, "total", servers_results.count());
    } else {
      ObTableBatchOperationResult *server_result = servers_results.at(server_result_idx.first);
      if (NULL == server_result) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("server_result is NULL", K(ret));
      } else if (server_result_idx.second < 0 || server_result_idx.second >= server_result->count()) {
        ret = OB_INDEX_OUT_OF_RANGE;
        LOG_WARN("invalid server result index", K(ret), "idx", server_result_idx.second, "total", server_result->count(),
                 "server_result", *server_result);
      } else if (OB_FAIL(result.push_back(server_result->at(server_result_idx.second)))) { // shallow copy
        LOG_WARN("failed to push back", K(ret));
      } else {
        LOG_DEBUG("[yzfdebug] one result", K(i), "one_result", server_result->at(server_result_idx.second), "count", result.count());
      }
    }
  } // end for
  return ret;
}

int ObTableRpcImpl::batch_execute(const ObTableBatchOperation &batch_operation, const ObTableRequestOptions &request_options, ObITableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  if (nullptr != THE_TRACE) {
    THE_TRACE->reset();
  }
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    NG_TRACE(calc_tablet_location_begin);
    uint64_t table_id = OB_INVALID_ID;
    ObSEArray<ObRowkey, 16> rowkeys(ObModIds::TABLE_CLIENT, OB_MALLOC_NORMAL_BLOCK_SIZE);
    ObSEArray<ObTabletLocation, 3> tablets_locs(ObModIds::TABLE_CLIENT, OB_MALLOC_NORMAL_BLOCK_SIZE);
    ObSEArray<ObTabletID, 3> tablet_ids(ObModIds::TABLE_CLIENT, OB_MALLOC_NORMAL_BLOCK_SIZE);
    ObSEArray<sql::RowkeyArray, 3> rowkeys_per_tablet(ObModIds::TABLE_CLIENT, OB_MALLOC_NORMAL_BLOCK_SIZE);
    ObSEArray<ObAddr, 3> servers(ObModIds::TABLE_CLIENT, OB_MALLOC_NORMAL_BLOCK_SIZE);
    ObSEArray<IdxArray, 3> tablets_per_server(ObModIds::TABLE_CLIENT, OB_MALLOC_NORMAL_BLOCK_SIZE);
    if (OB_FAIL(get_rowkeys(batch_operation, rowkeys))) {
      LOG_WARN("failed to get rowkeys from batch operation", K(ret));
    } else if (OB_FAIL(client_->get_tablets_locations(table_name_, rowkeys,
                                                      tablets_locs, table_id, tablet_ids, rowkeys_per_tablet))) {
      LOG_WARN("failed to get location", K(ret), K_(table_name), K(rowkeys));
    } else if (OB_FAIL(aggregate_tablet_by_server(tablets_locs, servers, tablets_per_server))) {
      LOG_WARN("failed to aggregate tablets", K(ret));
    } else {
      entity_factory_->free_and_reuse();
      arena_.reset_remain_one_page();
      NG_TRACE(calc_tablet_location_end);

      ObTableBatchOperationRequest request;
      request.credential_ = client_->get_credential();
      request.table_name_ = table_name_;
      request.table_id_ = table_id_;
      request.entity_type_ = this->entity_type_;
      request.consistency_level_ = request_options.consistency_level();
      request.returning_affected_rows_ = request_options.returning_affected_rows();
      request.returning_affected_entity_ = request_options.returning_affected_entity();
      request.option_flag_ = request_options.get_option_flag();
      request.batch_operation_as_atomic_ = request_options.batch_operation_as_atomic();
      if (REACH_TIME_INTERVAL(10*1000*1000)) {
        // TODO: we can not print the tenat memory usage now.
        // ObTenantManager::get_instance().print_tenant_usage();
        LOG_INFO("[yzfdebug] entity factory info", "addr", entity_factory_,
                 "free_count", entity_factory_->get_free_count(),
                 "used_count", entity_factory_->get_used_count(),
                 "total_mem", entity_factory_->get_total_mem(),
                 "used_mem", entity_factory_->get_used_mem(),
                 "server_num", servers.count(),
                 "batch_num", batch_operation.count());
      }
      const int64_t N = servers.count();
      if (1 == N && 1 == tablets_per_server.at(0).count()) {
        // single server and single tablet
        const ObAddr &server = servers.at(0);
        request.tablet_id_ = tablet_ids.at(tablets_per_server.at(0).at(0));
        request.batch_operation_ = batch_operation;  // @todo optimize to avoid copy
        ObTableBatchOperationResult &batch_result = dynamic_cast<ObTableBatchOperationResult&>(result); // @FIXME
        batch_result.set_entity_factory(entity_factory_);
        batch_result.set_allocator(&arena_);  // have to deep copy values from the rpc memory
        ret = rpc_proxy_->
              timeout(request_options.server_timeout())
              .to(server)
              .batch_execute(request, batch_result);
        NG_TRACE(tag1);
      } else {
        // multi tablets
        ObSEArray<ObTableBatchOperationResult*, 3> servers_results(ObModIds::TABLE_CLIENT, OB_MALLOC_NORMAL_BLOCK_SIZE);
        // table operation index -> (server index, server operation index)
        BatchResultIdxMap result_idx_map;
        static const int64_t MAP_BUCKET_SIZE = 512;
        if (OB_FAIL(result_idx_map.create(MAP_BUCKET_SIZE, ObModIds::TABLE_CLIENT, ObModIds::TABLE_CLIENT))) {
          LOG_WARN("failed to init map", K(ret));
        } else {
          result.reset();
        }
        LOG_DEBUG("begin multi batch operation", "server_num", N);
        for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)  // for each server
        {
          const ObAddr &server = servers.at(i);
          const IdxArray &tablets_idx_array = tablets_per_server.at(i);
          const int64_t tablet_num_of_server = tablets_idx_array.count();
          for (int64_t j = 0; OB_SUCCESS == ret && j < tablet_num_of_server; ++j)  // for each tablet of the server
          {
            const int64_t tablet_idx = tablets_idx_array.at(j);
            if (tablet_idx < 0 || tablet_idx >= tablet_ids.count()) {
              ret = OB_INDEX_OUT_OF_RANGE;
              LOG_WARN("invalid tablet index", K(ret), K(tablet_idx), "tablet_num", tablet_ids.count());
            } else {
              // send batch for each tablet of each server
              request.tablet_id_ = tablet_ids.at(tablet_idx);
              request.batch_operation_.reset();
              ObTableBatchOperationResult *server_batch_result = OB_NEW(ObTableBatchOperationResult, ObModIds::TABLE_CLIENT);
              if (NULL == server_batch_result) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("no memory", K(ret));
              } else if (OB_FAIL(servers_results.push_back(server_batch_result))) {
                LOG_WARN("failed to push back", K(ret));
              } else {
                server_batch_result->set_entity_factory(entity_factory_);
                server_batch_result->set_allocator(&arena_);  // have to deep copy values from the rpc memory
                // build batch operation for the tablet
                const sql::RowkeyArray &rowkey_array = rowkeys_per_tablet.at(tablet_idx);
                const int64_t rowkey_count = rowkey_array.count();
                for (int64_t k = 0; OB_SUCCESS == ret && k < rowkey_count; ++k)
                {
                  int64_t rowkey_idx = rowkey_array.at(k);
                  if (rowkey_idx < 0 || rowkey_idx >= batch_operation.count()) {
                    ret = OB_INDEX_OUT_OF_RANGE;
                    LOG_WARN("invalid rowkey index", K(ret), K(rowkey_idx), "total", rowkey_array.count());
                  } else if (OB_FAIL(request.batch_operation_.add(batch_operation.at(rowkey_idx)))) {
                    LOG_WARN("failed to push back", K(ret));
                  } else if (OB_FAIL(result_idx_map.set_refactored(rowkey_idx, std::make_pair(servers_results.count()-1, k), 0))) {
                    LOG_WARN("failed to record result idx", K(ret));
                  } else {
                  }
                } // end for each operation of this tablet

                if (OB_SUCC(ret)) {
                  LOG_DEBUG("multi batch operation", K(i), K(j), K(server), K(tablet_idx), K(rowkey_array));
                  // send batch for each tablet of each server
                  ret = rpc_proxy_->
                        timeout(request_options.server_timeout())
                        .to(server)
                        .batch_execute(request, *server_batch_result);
                }
              }
            }
          }  // end for each tablet
        } // end for each server

        // reorder results
        if (OB_SUCC(ret)) {
          LOG_WARN("finish multi batch operation", "results_num", servers_results.count());
          if (OB_FAIL(reorder_servers_results(servers_results, result_idx_map, batch_operation.count(), result))) {
            LOG_WARN("failed to reorder results", K(ret));
            LOG_WARN("fail tableapi test", K(batch_operation.count()), K(result.count()), K(tablet_ids.count()), K(rowkeys_per_tablet), K(tablets_per_server));
            LOG_WARN("fail tableapi ttest", K(tablets_locs.count()));
          }
        }
        // free server_result
        const int64_t results_count = servers_results.count();
        for (int64_t i = 0; OB_SUCCESS == ret && i < results_count; ++i)
        {
          ObTableBatchOperationResult *server_result = servers_results.at(i);
          if (NULL != server_result) {
            ob_delete(server_result);
          }
        } // end for
        NG_TRACE(tag2);
      }
    }
  }
  // FORCE_PRINT_TRACE(THE_TRACE, "[BATCH EXECUTE]");
  return ret;
}

ObTableRpcImpl::QueryMultiResult::~QueryMultiResult()
{
  reset();
}

int ObTableRpcImpl::QueryMultiResult::get_next_entity(const ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  ret = one_result_.get_next_entity(entity);
  while (OB_ITER_END == ret) {
    if (query_rpc_handle_.has_more()) {
      // request for the next result packet
      if (OB_FAIL(query_rpc_handle_.get_more(one_result_))) {
        LOG_WARN("failed to get more result", K(ret));
        int tmp_ret = ret;
        if (OB_FAIL(query_rpc_handle_.abort())) {
          LOG_WARN("failed to abort stream", K(ret));
        } else {
          ret = tmp_ret;
          LOG_INFO("abort the stream");
        }
        break;
      } else {
        ++result_packet_count_;
        ret = one_result_.get_next_entity(entity);
        LOG_INFO("[yzfdebug] get one more result", K(ret), K_(result_packet_count));
      }
    } else {
      break;
    }
  }
  return ret;
}

void ObTableRpcImpl::QueryMultiResult::reset()
{
  int ret = OB_SUCCESS;
  one_result_.reset();
  if (query_rpc_handle_.has_more()) {
    if (OB_FAIL(query_rpc_handle_.abort())) {
      LOG_WARN("failed to abort stream", K(ret));
    }
  }
  result_packet_count_ = 0;
}

int ObTableRpcImpl::execute_query(const ObTableQuery &query, const ObTableRequestOptions &request_options, ObTableEntityIterator *&result)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObTabletLocation tablet_loc;
    ObTabletReplicaLocation leader_loc;
    uint64_t table_id = OB_INVALID_ID;
    ObTabletID tablet_id;
    const ObString &index_name = query.get_index_name();
    const ObIArray<common::ObNewRange> &scan_ranges = query.get_scan_ranges();
    if (scan_ranges.count() <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("should have at least one scan range", K(ret), K(scan_ranges));
    } else if (OB_FAIL(client_->get_tablet_location(table_name_, index_name, scan_ranges.at(0),
                                                    tablet_loc, table_id, tablet_id))) {
      LOG_WARN("failed to get location", K(ret), K_(table_name), K(index_name), K(scan_ranges));
    } else if (OB_FAIL(tablet_loc.get_leader(leader_loc))) {
      LOG_WARN("failed to find leader location", K(ret), K(tablet_loc));
    } else {
      ObTableQueryRequest request;
      request.query_ = query;  // @todo FIXME
      request.table_name_ = table_name_;
      request.table_id_ = table_id_;
      request.tablet_id_ = tablet_id;
      request.credential_ = client_->get_credential();
      request.entity_type_ = this->entity_type_;
      request.consistency_level_ = request_options.consistency_level();
      query_multi_result_.reset();

      ret = rpc_proxy_->
            timeout(request_options.server_timeout())
            .to(leader_loc.get_server())
            .execute_query(request, query_multi_result_.get_one_result(), query_multi_result_.get_handle());
      if (OB_SUCC(ret)) {
        result = &query_multi_result_;
        LOG_INFO("[yzfdebug] query has more", "has_more", query_multi_result_.get_handle().has_more());
      }
    }
  }
  return ret;
}

int ObTableRpcImpl::execute_query_and_mutate(const ObTableQueryAndMutate &query_and_mutate, const ObTableRequestOptions &request_options, ObTableQueryAndMutateResult &result)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObTabletLocation tablet_loc;
    ObTabletReplicaLocation leader_loc;
    uint64_t table_id = OB_INVALID_ID;
    ObTabletID tablet_id;
    const ObTableQuery &query = query_and_mutate.get_query();
    const ObString &index_name = query.get_index_name();
    const ObIArray<common::ObNewRange> &scan_ranges = query.get_scan_ranges();
    if (scan_ranges.count() <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("should have at least one scan range", K(ret), K(scan_ranges));
    } else if (OB_FAIL(client_->get_tablet_location(table_name_, index_name, scan_ranges.at(0),
                                                    tablet_loc, table_id, tablet_id))) {
      LOG_WARN("failed to get location", K(ret), K_(table_name), K(index_name), K(scan_ranges));
    } else if (OB_FAIL(tablet_loc.get_leader(leader_loc))) {
      LOG_WARN("failed to find leader location", K(ret), K(tablet_loc));
    } else {
      ObTableQueryAndMutateRequest request;
      request.query_and_mutate_ = query_and_mutate;  // @todo FIXME
      request.table_name_ = table_name_;
      request.table_id_ = table_id_;
      request.tablet_id_ = tablet_id;
      request.credential_ = client_->get_credential();
      request.entity_type_ = this->entity_type_;
      query_multi_result_.reset();
      ret = rpc_proxy_->
            timeout(request_options.server_timeout())
            .to(leader_loc.get_server())
            .query_and_mutate(request, result);
    }
  }
  return ret;
}

int ObTableRpcImpl::query_start(const ObTableQuery& query, const ObTableRequestOptions &request_options, ObTableQueryAsyncResult *&result)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init already", K(ret));
  } else {
    ObTabletLocation tablet_loc;
    ObTabletReplicaLocation leader_loc;
    uint64_t table_id = OB_INVALID_ID;
    ObTabletID tablet_id;
    const ObString &index_name = query.get_index_name();
    const ObIArray<common::ObNewRange> &scan_ranges = query.get_scan_ranges();
    if (scan_ranges.count() <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("should have at least one scan range", K(ret), K(scan_ranges));
    } else if (OB_FAIL(client_->get_tablet_location(table_name_, index_name, scan_ranges.at(0),
                                                       tablet_loc, table_id, tablet_id))) {
      LOG_WARN("failed to get location", K(ret), K_(table_name), K(index_name), K(scan_ranges));
    } else if (OB_FAIL(tablet_loc.get_leader(leader_loc))) {
      LOG_WARN("failed to find leader location", K(ret), K(tablet_loc));
    } else {
      ObTableQueryAsyncRequest request;
      request.query_ = query;
      request.table_name_ = table_name_;
      request.table_id_ = table_id_;
      request.tablet_id_ = tablet_id;
      request.credential_ = client_->get_credential();
      request.entity_type_ = this->entity_type_;
      request.consistency_level_ = request_options.consistency_level();
      request.query_session_id_ = 0;
      request.query_type_ = ObQueryOperationType::QUERY_START;
      query_async_multi_result_.reset();
      result = &query_async_multi_result_.get_one_result();
      ret = rpc_proxy_->
            timeout(request_options.server_timeout())
            .to(leader_loc.get_server())
            .execute_query_async(request, *result);
      if (OB_SUCC(ret)) {
        query_async_multi_result_.server_addr_ = leader_loc.get_server();
        query_async_multi_result_.has_more_ = !result->is_end_;
        query_async_multi_result_.session_id_ = result->query_session_id_;
        query_async_multi_result_.result_packet_count_++;
        if (result->is_end_ && result->get_row_count() == 0) {
          ret = OB_ITER_END;
        }
      } else {
        LOG_WARN("failed to execute rpc call for query start", K(ret));
      }
    }
  }
  return ret;
}

int ObTableRpcImpl::query_next(const ObTableRequestOptions &request_options, ObTableQueryAsyncResult *&result) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init already", K(ret));
  } else if(!query_async_multi_result_.has_more_) {
    ret = OB_ITER_END;
    LOG_DEBUG("no more result or query_start hasn't been executed yet", K(ret));
  } else {
    ObTableQueryAsyncRequest request;
    request.credential_ = client_->get_credential();
    request.entity_type_ = this->entity_type_;
    request.consistency_level_ = request_options.consistency_level();
    request.query_session_id_ = query_async_multi_result_.session_id_;
    request.query_type_ = ObQueryOperationType::QUERY_NEXT;
    query_async_multi_result_.has_more_ = false;
    result = &query_async_multi_result_.get_one_result();
    result->reset();
    ret = rpc_proxy_->
          timeout(request_options.server_timeout())
          .to(query_async_multi_result_.server_addr_)
          .execute_query_async(request, *result);
    if (OB_SUCC(ret)) {
      query_async_multi_result_.has_more_ = !result->is_end_;
      query_async_multi_result_.result_packet_count_++;
      if (result->is_end_ && result->get_row_count() == 0) {
        ret = OB_ITER_END;
      }
    } else {
      LOG_WARN("failed to execute rpc call for query next", K(ret));
    }
  }
  return ret;
}
