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

#ifndef _OB_TABLE_QUERY_SYNC_PROCESSOR_H
#define _OB_TABLE_QUERY_SYNC_PROCESSOR_H 1
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/table/ob_table_rpc_proxy.h"
#include "ob_table_rpc_processor.h"
#include "ob_table_service.h"

namespace oceanbase
{
namespace observer
{

/**
 * ---------------------------------------- ObTableQuerySyncSession ----------------------------------------
 */
class ObTableQuerySyncSession final
{
public:
  explicit ObTableQuerySyncSession()
    : in_use_(true),
      timestamp_(0),
      query_(),
      result_iterator_(nullptr),
      allocator_(ObModIds::TABLE_PROC),
      table_service_ctx_(allocator_),
      iterator_mementity_(nullptr)
  {}
  ~ObTableQuerySyncSession();
  
  void set_timestamp(int64_t timestamp) { timestamp_ = timestamp; }
  void set_result_iterator(ObNormalTableQueryResultIterator* iter);
  int deep_copy_select_columns(const ObTableQuery &query);
  void set_in_use(bool in_use) {in_use_ = in_use;}
  bool is_in_use() {return in_use_;}
  int init();

  int64_t get_timestamp() { return timestamp_; }
  ObTableServiceQueryCtx *get_table_service_ctx() {return &table_service_ctx_;}
  ObNormalTableQueryResultIterator *get_result_iterator() { return result_iterator_; }
  ObArenaAllocator *get_allocator() {return &allocator_;}

public:
  ObPartitionLeaderArray* get_part_leader_list() {return &participants_;}
  sql::TransState* get_trans_state() {return &trans_state_;}
  transaction::ObTransDesc* get_trans_desc() {return &trans_desc_;}
  transaction::ObPartitionEpochArray* get_part_epoch_list() {return &part_epoch_list_;}

private:
  bool in_use_;
  uint64_t timestamp_;
  ObTableQuery query_; // only select_columns is correct
  ObNormalTableQueryResultIterator *result_iterator_;
  ObArenaAllocator allocator_;
  ObTableServiceQueryCtx table_service_ctx_;
  lib::MemoryContext iterator_mementity_;

private:
  // txn control
  ObPartitionLeaderArray participants_;
  sql::TransState trans_state_;
  transaction::ObTransDesc trans_desc_;
  transaction::ObPartitionEpochArray part_epoch_list_;
};

/**
 * ------------------------------------ ObQuerySyncSessionRecycle ------------------------------------
 */
class ObQuerySyncSessionRecycle : public common::ObTimerTask
{
public:
  ObQuerySyncSessionRecycle(){}
  virtual ~ObQuerySyncSessionRecycle(){}
  virtual void runTimerTask();

private:
  void query_session_recycle();
  DISALLOW_COPY_AND_ASSIGN(ObQuerySyncSessionRecycle);
};

/**
 * -----------------------------------Singleton ObQuerySyncMgr -------------------------------------
 */
class ObQuerySyncMgr final
{
  friend class ObTableQuerySyncP;

public:
  using ObQueryHashMap = 
    common::hash::ObHashMap<uint64_t, ObTableQuerySyncSession *, common::hash::SpinReadWriteDefendMode>;
  using QuerySessionPair = common::hash::HashMapPair<uint64_t, ObTableQuerySyncSession*>; 
  ~ObQuerySyncMgr() {}
  static ObQuerySyncMgr &get_instance();

  struct ObGetAllSessionIdOp {
    explicit ObGetAllSessionIdOp(common::ObIArray<uint64_t>& session_id_array) : session_id_array_(session_id_array)
    {}
    int operator()(QuerySessionPair& entry);
    common::ObIArray<uint64_t>& session_id_array_;
  };

public:
  int get_query_session(uint64_t sessid, ObTableQuerySyncSession *&query_sess_ctx);
  int set_query_session(uint64_t sessid, ObTableQuerySyncSession *query_sess_ctx);
  void clean_timeout_query_session();

public:
  ObQueryHashMap *get_query_session_map();
  ObTableQuerySyncSession *alloc_query_session();
  uint64_t generate_query_sessid();
  lib::ObMutex& get_locker(uint64_t sessid) { return locker_arr_[sessid % DEFAULT_LOCK_ARR_SIZE];}
  
private:
  int init();  
  ObQuerySyncMgr();
  DISALLOW_COPY_AND_ASSIGN(ObQuerySyncMgr);

private: 
  static const uint64_t INVALID_SESSION_ID = 0;
  static const uint64_t DEFAULT_LOCK_ARR_SIZE = 2000;
  static const uint64_t QUERY_SESSION_MAX_SIZE = 1000;
  static const uint64_t QUERY_SESSION_TIMEOUT = 120 * 1000 * 1000; // 120s
  static const uint64_t QUERY_SESSION_CLEAN_DELAY = 180 * 1000 * 1000; // 180s

private:
  static int64_t once_;  // for creating singleton instance
  static ObQuerySyncMgr *instance_;
  int64_t session_id_; 
  ObQueryHashMap query_session_map_;
  lib::ObMutex locker_arr_[DEFAULT_LOCK_ARR_SIZE];
  ObQuerySyncSessionRecycle query_session_recycle_; 
  common::ObTimer timer_;
};

/**
 * -------------------------------------- ObTableQuerySyncP ----------------------------------------
*/
class ObTableQuerySyncP : 
  public ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC> >
{
  typedef ObTableRpcProcessor<obrpc::ObTableRpcProxy::ObRpc<obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC>> 
    ParentType;
public:
  explicit ObTableQuerySyncP(const ObGlobalContext &gctx);
  virtual ~ObTableQuerySyncP() {}
  virtual int deserialize() override;

protected:
  virtual int check_arg() override;
  virtual int try_process() override;
  virtual void reset_ctx() override;
  virtual void audit_on_finish() override;
  virtual uint64_t get_request_checksum() override;
  virtual table::ObTableAPITransCb *new_callback(rpc::ObRequest *req) override;

private:
  int process_query_start();
  int process_query_next();
  int process_query_end();
  int destory_query_session(bool need_rollback_trans);
  DISALLOW_COPY_AND_ASSIGN(ObTableQuerySyncP);

private:
  int get_partition_ids(uint64_t table_id, ObIArray<int64_t> &part_ids);
  int get_session_id(uint64_t &real_sessid, uint64_t arg_sessid);
  int get_query_session(uint64_t sessid, ObTableQuerySyncSession *&query_session);
  int query_scan_with_init();
  int query_scan_without_init();
  int query_scan_with_old_context(const int64_t timeout);
  int query_scan_with_new_context(ObTableQuerySyncSession * session_ctx, table::ObTableQueryResultIterator *result_iterator, 
    const int64_t timeout);

private:
  void set_trans_from_session(ObTableQuerySyncSession *query_session);
  int check_query_type();

private:
  ObTableServiceQueryCtx *table_service_ctx_;
  int64_t result_row_count_;
  uint64_t query_session_id_;
  ObArenaAllocator allocator_;
  ObTableQuerySyncSession *query_session_;
  int64_t timeout_ts_;
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_QUERY_SYNC_PROCESSOR_H */