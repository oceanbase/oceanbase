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

#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_SERVICE_H_
#define OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_SERVICE_H_

#include <stdint.h>

#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/ob_occam_timer.h"
#include "sql/ob_sql_trans_control.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tablelock/ob_table_lock_rpc_proxy.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"

namespace oceanbase
{
namespace rpc
{
namespace frame
{
class ObReqTransport;
}
}

namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}

namespace transaction
{

namespace tablelock
{

class ObTableLockService final
{
private:
  static const int64_t OB_DEFAULT_LOCK_ID_COUNT = 10;
  typedef common::ObSEArray<ObLockID, OB_DEFAULT_LOCK_ID_COUNT> ObLockIDArray;
  typedef hash::ObHashMap<share::ObLSID, ObLockIDArray> ObLSLockMap;
  class ObTableLockCtx
  {
  public:
    ObTableLockCtx();
    ~ObTableLockCtx() {}
    int set_by_lock_req(const ObLockRequest &arg, const bool is_replace_task = false);
    int set_by_lock_req_common_part(const ObLockRequest &arg);
    int set_tablet_id(const common::ObIArray<common::ObTabletID> &tablet_ids);
    int set_tablet_id(const common::ObTabletID &tablet_id);
    int set_lock_id(const common::ObIArray<ObLockID> &lock_ids);
    int set_lock_id(const ObLockID &lock_id);
    int set_lock_id(const ObLockOBJType &obj_type, const uint64_t obj_id);
    bool is_try_lock() const { return 0 == timeout_us_; }
    bool is_deadlock_avoid_enabled() const;
    bool is_timeout() const;
    int64_t remain_timeoutus() const;
    int64_t get_rpc_timeoutus() const;
    int64_t get_tablet_cnt() const;
    const common::ObTabletID &get_tablet_id(const int64_t index) const;
    int add_touched_ls(const share::ObLSID &lsid);
    void clean_touched_ls();
    bool is_savepoint_valid() { return current_savepoint_.is_valid(); }
    void reset_savepoint() { current_savepoint_.reset(); }

    bool is_stmt_savepoint_valid() { return stmt_savepoint_.is_valid(); }
    void reset_stmt_savepoint() { stmt_savepoint_.reset(); }
    ObTableLockOpType get_lock_op_type() const { return lock_op_type_; }
    bool is_unlock_task() const { return tablelock::is_unlock_task(task_type_); }
    bool is_replace_task() const { return tablelock::is_replace_lock_task(task_type_); }
    bool is_tablet_lock_task() const { return tablelock::is_tablet_lock_task(task_type_); }

  public:
    ObTableLockTaskType task_type_; // current lock request type
    bool is_in_trans_;
    union {
      // used for table/partition
      struct {
        uint64_t table_id_;
        uint64_t partition_id_;          // set when lock or unlock specified partition
        share::ObLSID ls_id_;            // used for alone tablet lock and unlock
      };
    };

    ObTableLockOpType lock_op_type_;  // specify the lock op type

    int64_t origin_timeout_us_;  // the origin timeout us specified by user.
    int64_t timeout_us_;         // the timeout us for every retry times.
    int64_t abs_timeout_ts_;     // the abstract timeout us.
    sql::TransState trans_state_;
    transaction::ObTxDesc *tx_desc_;
    ObTxParam tx_param_;                      // the tx param for current tx
    transaction::ObTxSEQ current_savepoint_;  // used to rollback current sub tx.
    share::ObLSArray need_rollback_ls_;       // which ls has been modified after
                                              // the current_savepoint_ created.
    common::ObTabletIDArray tablet_list_;     // all the tablets need to be locked/unlocked
    ObLockIDArray obj_list_;

    ObTableLockMode lock_mode_;
    ObTableLockOwnerID lock_owner_;

    // TODO: yanyuan.cxf we need better performance.
    // share::ObLSArray ls_list_; // related ls list
    int64_t schema_version_;             // the schema version of the table to be locked
    bool tx_is_killed_;                  // used to kill a trans.
    bool is_from_sql_;
    int ret_code_before_end_stmt_or_tx_;  // used to mark this lock is still conflict while lock request exiting

    // use to kill the whole lock table stmt.
    transaction::ObTxSEQ stmt_savepoint_;
    bool is_for_replace_;

    TO_STRING_KV(K(task_type_), K(is_in_trans_), K(table_id_), K(partition_id_),
                 K(tablet_list_), K(obj_list_), K(lock_op_type_),
                 K(origin_timeout_us_), K(timeout_us_),
                 K(abs_timeout_ts_), KPC(tx_desc_), K(tx_param_),
                 K(current_savepoint_), K(need_rollback_ls_),
                 K(lock_mode_), K(lock_owner_),
                 K(schema_version_), K(tx_is_killed_),
                 K(is_from_sql_), K(ret_code_before_end_stmt_or_tx_),
                 K(stmt_savepoint_), K_(is_for_replace));
  };

  class ObReplaceTableLockCtx : public ObTableLockCtx
  {
  public:
    ObReplaceTableLockCtx() : ObTableLockCtx(), new_lock_mode_(NO_LOCK), new_lock_owner_() {}
    ~ObReplaceTableLockCtx() {}
    int get_lock_param(const ObLockID &lock_id, ObReplaceLockParam &lock_param) const;
    INHERIT_TO_STRING_KV("ObTableLockCtx", ObTableLockCtx, K_(new_lock_mode), K_(new_lock_owner));

  public:
    ObTableLockMode new_lock_mode_;
    ObTableLockOwnerID new_lock_owner_;
  };

  class ObRetryCtx
  {
  public:
    ObRetryCtx() : need_retry_(false),
                   send_rpc_count_(0),
                   rpc_ls_array_(),
                   retry_lock_ids_()
    {}
    ~ObRetryCtx()
    { reuse(); }
    void reuse();
  public:
    TO_STRING_KV(K_(need_retry), K_(send_rpc_count), K_(rpc_ls_array),
                 K_(retry_lock_ids));
    bool need_retry_;
    int64_t send_rpc_count_; // how many rpc we have send.
    ObArray<share::ObLSID> rpc_ls_array_;
    ObLockIDArray retry_lock_ids_;           // the lock id need to be retry.
  };
public:
  class ObOBJLockGarbageCollector
  {
    static const int OBJ_LOCK_GC_THREAD_NUM = 2;
  public:
    friend class ObLockTable;
    ObOBJLockGarbageCollector();
    ~ObOBJLockGarbageCollector();
  public:
    int start();
    void stop();
    void wait();
    void destroy();
    int garbage_collect_right_now();

    TO_STRING_KV(KP(this),
                 K_(last_success_timestamp));
  private:
    int garbage_collect_for_all_ls_();
    void check_and_report_timeout_();
    int check_is_leader_(ObLS *ls, bool &is_leader);
  public:
    static int64_t GARBAGE_COLLECT_PRECISION;
    static int64_t GARBAGE_COLLECT_EXEC_INTERVAL;
    static int64_t GARBAGE_COLLECT_TIMEOUT;
  private:
    common::ObOccamThreadPool obj_lock_gc_thread_pool_;
    common::ObOccamTimer timer_;
    common::ObOccamTimerTaskRAIIHandle timer_handle_;

    int64_t last_success_timestamp_;
  };

public:
  typedef hash::ObHashMap<ObLockID, share::ObLSID> LockMap;

  ObTableLockService()
    : location_service_(nullptr),
      sql_proxy_(nullptr),
      obj_lock_garbage_collector_(),
      is_inited_(false) {}
  ~ObTableLockService() {}
  int init();
  static int mtl_init(ObTableLockService* &lock_service);
  int start();
  void stop();
  void wait();
  void destroy();

  // generate a tenant unique owner id
  // this owner id can be used to link OUT_TRANS_LOCK and OUT_TRANS_UNLOCK operation.
  int generate_owner_id(ObTableLockOwnerID &owner_id);
  // ---------------------------- interface for OUT_TRANS lock ------------------------------/
  // lock and unlock with anonymous trans.

  // lock the table level lock and all the tablet level lock within an anonymous trans.
  // @param [in] table_id, specified the table which will be locked.
  // @param [in] lock_mode, may be ROW_SHARE/ROW_EXCLUSIVE/SHARE/SHARE_ROW_EXCLUSIVE/EXCLUSIVE
  // @param [in] lock_owner, who will lock the table, and who will unlock the table later.
  // @param [in] timeout_us, 0 means it is try lock, if there is some lock conflict will return immediately.
  //                         otherwise retry until timeout if there is some lock conflict.
  // @return
  int lock_table(const uint64_t table_id,
                 const ObTableLockMode lock_mode,
                 const ObTableLockOwnerID lock_owner,
                 const int64_t timeout_us = 0);
  int unlock_table(const uint64_t table_id,
                   const ObTableLockMode lock_mode,
                   const ObTableLockOwnerID lock_owner,
                   const int64_t timeout_us = 0);
  // lock the tablet level lock and corresponding table level lock within an anonymous trans.
  // @param [in] table_id, specified the table whose tablet will be locked.
  // @param [in] tablet_id, specified which tablet will be locked.
  // @param [in] lock_mode, may be ROW_SHARE/ROW_EXCLUSIVE/SHARE/SHARE_ROW_EXCLUSIVE/EXCLUSIVE
  // @param [in] lock_owner, who will lock the table, and who will unlock the table later.
  // @param [in] timeout_us, 0 means it is try lock, if there is some lock conflict will return immediately.
  //                         otherwise retry until timeout if there is some lock conflict.
  // @return
  int lock_tablet(const uint64_t table_id,
                  const common::ObTabletID &tablet_id,
                  const ObTableLockMode lock_mode,
                  const ObTableLockOwnerID lock_owner,
                  const int64_t timeout_us = 0);
  int unlock_tablet(const uint64_t table_id,
                    const common::ObTabletID &tablet_id,
                    const ObTableLockMode lock_mode,
                    const ObTableLockOwnerID lock_owner,
                    const int64_t timeout_us = 0);

  // ---------------------------- interface for IN_TRANS/OUT_TRANS lock ------------------------------/
  int lock_partition_or_subpartition(ObTxDesc &tx_desc,
                                     const ObTxParam &tx_param,
                                     ObLockPartitionRequest &arg);
  int lock(ObTxDesc &tx_desc,
           const ObTxParam &tx_param,
           const ObLockRequest &arg,
           const bool is_for_replace = false);
  int unlock(ObTxDesc &tx_desc,
             const ObTxParam &tx_param,
             const ObUnLockRequest &arg);
  // NOTICE: has the same restrictions as the lock interface mentioned above.
  int replace_lock(ObTxDesc &tx_desc,
                   const ObTxParam &tx_param,
                   const ObReplaceLockRequest &replace_req);
  int replace_lock(ObTxDesc &tx_desc,
                   const ObTxParam &tx_param,
                   const ObReplaceAllLocksRequest &replace_req);
  int garbage_collect_right_now();
  int get_obj_lock_garbage_collector(ObOBJLockGarbageCollector *&obj_lock_garbage_collector);

private:
  int check_cluster_version_after_(const uint64_t version);
  int check_data_version_after_(const uint64_t version);
  bool need_retry_trans_(const ObTableLockCtx &ctx,
                         const int64_t ret) const;
  bool need_retry_single_task_(const ObTableLockCtx &ctx,
                               const int64_t ret) const;
  bool need_retry_whole_rpc_task_(const int ret);
  bool need_retry_part_rpc_task_(const int ret,
                                 const ObTableLockTaskResult *result) const;
  bool need_renew_location_(const int64_t ret) const;
  int rewrite_return_code_(const int ret, const int ret_code_before_end_stmt_or_tx = OB_SUCCESS, const bool is_from_sql = false) const;
  bool is_lock_conflict_ret_code_(const int ret) const;
  bool is_timeout_ret_code_(const int ret) const;
  bool is_can_retry_err_(const int ret) const;
  int process_lock_task_(ObTableLockCtx &ctx);
  int process_obj_lock_task_(ObTableLockCtx &ctx);
  int process_table_lock_task_(ObTableLockCtx &ctx);
  int process_tablet_lock_task_(ObTableLockCtx &ctx,
                                const ObSimpleTableSchemaV2 *table_schema);
  int start_tx_(ObTableLockCtx &ctx);
  int end_tx_(ObTableLockCtx &ctx, const bool is_rollback);
  int start_sub_tx_(ObTableLockCtx &ctx);
  int end_sub_tx_(ObTableLockCtx &ctx, const bool is_rollback);
  int start_stmt_(ObTableLockCtx &ctx);
  int end_stmt_(ObTableLockCtx &ctx, const bool is_rollback);
  int check_op_allowed_(const uint64_t table_id,
                        const ObSimpleTableSchemaV2 *table_schema,
                        bool &is_allowed);
  int get_process_tablets_(const ObSimpleTableSchemaV2 *table_schema,
                           ObTableLockCtx &ctx);
  int get_ls_lock_map_(ObTableLockCtx &ctx,
                       const common::ObTabletIDArray &tablets,
                       LockMap &lock_map,
                       ObLSLockMap &ls_lock_map);
  int fill_ls_lock_map_(ObTableLockCtx &ctx,
                        const ObLockIDArray &lock_ids,
                        ObLSLockMap &ls_lock_map,
                        bool force_refresh_location);
  int fill_ls_lock_map_(ObTableLockCtx &ctx,
                        const common::ObTabletIDArray &tablets,
                        LockMap &lock_map,
                        ObLSLockMap &ls_lock_map);
  int get_tablet_ls_(const ObTableLockCtx &ctx,
                     const ObTabletID &tablet_id,
                     share::ObLSID &ls_id,
                     bool force_refresh = false);
  int get_lock_id_ls_(const ObTableLockCtx &ctx,
                      const ObLockID &lock_id,
                      share::ObLSID &ls_id,
                      bool force_refresh = false);
  int get_ls_leader_(const int64_t cluster_id,
                     const uint64_t tenant_id,
                     const share::ObLSID &ls_id,
                     const int64_t abs_timeout_ts,
                     ObAddr &addr);
  int pack_request_(ObTableLockCtx &ctx,
                    const ObTableLockTaskType task_type,
                    const ObLockID &lock_id,
                    const share::ObLSID &ls_id,
                    ObAddr &addr,
                    ObTableLockTaskRequest &request);
  int pack_batch_request_(ObTableLockCtx &ctx,
                          const ObTableLockTaskType task_type,
                          const share::ObLSID &ls_id,
                          const ObLockIDArray &lock_ids,
                          ObLockTaskBatchRequest<ObLockParam> &request);
  int pack_batch_request_(ObTableLockCtx &ctx,
                          const ObTableLockTaskType task_type,
                          const share::ObLSID &ls_id,
                          const ObLockIDArray &lock_ids,
                          ObLockTaskBatchRequest<ObReplaceLockParam> &request);
  template<class RpcProxy>
  int batch_rpc_handle_(RpcProxy &proxy_batch,
                        ObTableLockCtx &ctx,
                        const ObLSLockMap &lock_map);
  template<class RpcProxy>
  int batch_rpc_handle_(RpcProxy &proxy_batch,
                        ObTableLockCtx &ctx,
                        const ObLSLockMap &ls_lock_map,
                        bool &can_retry,
                        ObLSLockMap &retry_ls_lock_map);
  template<class RpcProxy>
  int handle_parallel_rpc_response_(RpcProxy &proxy_batch,
                                    ObTableLockCtx &ctx,
                                    const ObLSLockMap &ls_lock_map,
                                    bool &can_retry,
                                    ObRetryCtx &retry_ctx);
  template<class RpcProxy>
  int parallel_batch_rpc_handle_(RpcProxy &proxy_batch,
                                 ObTableLockCtx &ctx,
                                 const ObLSLockMap &ls_lock_map);
  template<class RpcProxy>
  int parallel_batch_rpc_handle_(RpcProxy &proxy_batch,
                                 ObTableLockCtx &ctx,
                                 const ObLSLockMap &ls_lock_map,
                                 bool &can_retry,
                                 ObLSLockMap &retry_ls_lock_map);
  template<class RpcProxy>
  int parallel_send_rpc_task_(RpcProxy &proxy_batch,
                              ObTableLockCtx &ctx,
                              const ObLSLockMap &ls_lock_map,
                              ObRetryCtx &retry_ctx);
  template<class RpcProxy>
  int send_one_rpc_task_(RpcProxy &proxy_batch,
                         ObTableLockCtx &ctx,
                         const share::ObLSID &ls_id,
                         const ObLockIDArray &lock_ids,
                         ObRetryCtx &retry_ctx);
  template<class RpcProxy>
  int send_rpc_task_(RpcProxy &proxy_batch,
                     ObTableLockCtx &ctx,
                     const share::ObLSID &ls_id,
                     const ObLockIDArray &lock_ids,
                     ObRetryCtx &retry_ctx);
  template<class RpcProxy, class LockRequest>
  int rpc_call_(RpcProxy &proxy_batch,
                const ObAddr &addr,
                const int64_t timeout_us,
                const LockRequest &request);
  template<class RpcProxy>
  int pack_and_call_rpc_(RpcProxy &proxy_batch,
                         ObTableLockCtx &ctx,
                         const share::ObLSID &ls_id,
                         const ObLockIDArray &lock_ids,
                         const ObAddr &addr,
                         ObRetryCtx &retry_ctx);
  template<>
  int pack_and_call_rpc_(obrpc::ObBatchReplaceLockProxy &proxy_batch,
                         ObTableLockCtx &ctx,
                         const share::ObLSID &ls_id,
                         const ObLockIDArray &lock_ids,
                         const ObAddr &addr,
                         ObRetryCtx &retry_ctx);
  int get_retry_lock_ids_(const ObLockIDArray &lock_ids,
                          const int64_t start_pos,
                          ObLockIDArray &retry_lock_ids);
  int get_retry_lock_ids_(const share::ObLSID &ls_id,
                          const ObLSLockMap &ls_lock_map,
                          const int64_t start_pos,
                          ObLockIDArray &retry_lock_ids);
  int collect_rollback_info_(const share::ObLSID &ls_id,
                             ObTableLockCtx &ctx);
  int collect_rollback_info_(const ObArray<share::ObLSID> &ls_array,
                             ObTableLockCtx &ctx);
  template<class RpcProxy>
  int collect_rollback_info_(const ObArray<share::ObLSID> &ls_array,
                             RpcProxy &proxy_batch,
                             ObTableLockCtx &ctx);
  int inner_process_obj_lock_(ObTableLockCtx &ctx,
                              const LockMap &lock_map,
                              const ObLSLockMap &ls_lock_map);
  int inner_process_obj_lock_batch_(ObTableLockCtx &ctx,
                                    const ObLSLockMap &ls_lock_map);
  int process_obj_lock_(ObTableLockCtx &ctx,
                        const share::ObLSID &ls_id,
                        const ObLockID &lock_id);
  int process_obj_lock_(ObTableLockCtx &ctx,
                        const share::ObLSID &ls_id,
                        const common::ObIArray<ObLockID> &lock_ids);
  static bool is_part_table_lock_(const ObTableLockTaskType task_type);
  int get_table_lock_mode_(const ObTableLockTaskType task_type,
                           const ObTableLockMode part_lock_mode,
                           ObTableLockMode &table_lock_mode);
  int process_table_lock_(ObTableLockCtx &ctx);
  int process_table_tablet_lock_(ObTableLockCtx &ctx, const LockMap &lock_map, const ObLSLockMap &ls_lock_map);
  // only useful in LOCK_TABLE/LOCK_PARTITION
  int pre_check_lock_(ObTableLockCtx &ctx,
                      const ObLSLockMap &ls_lock_map);
  int batch_pre_check_lock_(ObTableLockCtx &ctx,
                            const ObLSLockMap &ls_lock_map);
  int pre_check_lock_old_version_(ObTableLockCtx &ctx,
                                  const ObLSLockMap &ls_lock_map);
  // used by deadlock detector.
  int deal_with_deadlock_(ObTableLockCtx &ctx);
  int get_table_partition_level_(const ObTableID table_id, ObPartitionLevel &part_level);
  DISALLOW_COPY_AND_ASSIGN(ObTableLockService);
private:
  // TODO: yanyuan.cxf use parallel rpc and modify this to 5s.
  static const int64_t DEFAULT_TIMEOUT_US = 1500L * 1000L * 1000L; // 1500s
  static const int64_t DEFAULT_RPC_TIMEOUT_US = 2L * 1000L * 1000L; // 2s

  share::ObLocationService *location_service_;
  common::ObMySQLProxy *sql_proxy_;
  ObOBJLockGarbageCollector obj_lock_garbage_collector_;
  bool is_inited_;
};

}

}
}

#endif /* OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_SERVICE_H_ */
