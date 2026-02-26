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

#ifndef _OB_TABLE_QUERY_SESSION_MGR_H
#define _OB_TABLE_QUERY_SESSION_MGR_H

#include "ob_table_query_session.h"

namespace oceanbase
{
namespace obrpc
{
class ObTableSessIDRpcProxy;
} // namespace obrpc
namespace observer
{
class ObTableSessIDRequestRpc;
class ObTableQueryASyncMgr : public common::ObTimerTask
{
public:
  using ObQueryHashMap =
    common::hash::ObHashMap<uint64_t, table::ObITableQueryAsyncSession *, common::hash::SpinReadWriteDefendMode>;
  using QuerySessionPair = common::hash::HashMapPair<uint64_t, table::ObITableQueryAsyncSession*>;
  ~ObTableQueryASyncMgr() {}
  ObTableQueryASyncMgr();
  struct ObGetAllSessionIdOp {
    explicit ObGetAllSessionIdOp(common::ObIArray<uint64_t>& session_id_array) : session_id_array_(session_id_array)
    {}
    int operator()(QuerySessionPair& entry);
    common::ObIArray<uint64_t>& session_id_array_;
  };

public:
  template<typename T>
  T *alloc_query_session(uint64_t sess_id)
  {
    int ret = OB_SUCCESS;
    T *query_session = OB_NEWx(T, &allocator_, sess_id);
    if (OB_ISNULL(query_session)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "failed to allocate query session", K(ret));
    } else if (OB_FAIL(query_session->init())) {
      SERVER_LOG(WARN, "failed to init query session", K(ret));
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(query_session)) {
      free_query_session(query_session);
      query_session = nullptr;
    }

    return query_session;
  }
  int get_session_id(uint64_t &real_sessid,
                     uint64_t arg_sessid,
                     const table::ObQueryOperationType query_type);
  int get_query_session(uint64_t sessid, table::ObITableQueryAsyncSession *&query_sess_ctx);
  int set_query_session(uint64_t sessid, table::ObITableQueryAsyncSession *query_sess_ctx);
  void clean_timeout_query_session();
  void destroy_all_query_session();
  void free_query_session(table::ObITableQueryAsyncSession *query_session);
  virtual void runTimerTask() override;
  static int mtl_init(ObTableQueryASyncMgr *&query_async_mgr);
  int start();
  void stop();
  void wait();
  void destroy();

public:
  template<typename T>
  int get_query_session(uint64_t sessid, table::ObQueryOperationType query_type, table::ObITableQueryAsyncSession *&query_session);
  int destory_query_session(table::ObITableQueryAsyncSession *query_session);
  static int check_query_type(const table::ObQueryOperationType query_type);
private:
  ObQueryHashMap *get_query_session_map();
  int generate_query_sessid(uint64_t &sess_id);
  lib::ObMutex& get_locker(uint64_t sessid) { return locker_arr_[sessid % DEFAULT_LOCK_ARR_SIZE];}
  int generate_new_session_ids(const int64_t arg_session_count);
  int release_occupied_session_id(const int64_t sess_id);

private:
  int init();
  int alloc_request_rpc_proxy();
  int binary_search_sess_id_index(const int64_t session_id, int64_t &idx);
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryASyncMgr);

public:
  static const uint64_t INVALID_SESSION_ID = 0;
private:
  static const uint64_t DEFAULT_LOCK_ARR_SIZE = 2000;
  static const uint64_t QUERY_SESSION_EXTEND_BATCH_SIZE = 10000;
  static const uint64_t MIN_QUERY_SESSION_CLEAN_DELAY = 1 * 1000 * 1000;  // 1s
  static const uint64_t INIT_ARRAY_SIZE = 10000;

private:
  common::ObFIFOAllocator allocator_;
  int64_t session_id_;
  ObQueryHashMap query_session_map_;
  lib::ObMutex locker_arr_[DEFAULT_LOCK_ARR_SIZE];
  common::ObTimer timer_;
  bool is_inited_;
  uint64_t session_count_;
  common::SpinRWLock lock_;
  obrpc::ObTableSessIDRpcProxy *rpc_proxy_;
  ObTableSessIDRequestRpc *id_request_rpc_;
  ObSEArray<int64_t, INIT_ARRAY_SIZE> session_ids_; // start from 1
  ObSEArray<uint8_t, INIT_ARRAY_SIZE> session_id_in_use_;
};

} // end of namespace table
} // end of namespace oceanbase

#endif