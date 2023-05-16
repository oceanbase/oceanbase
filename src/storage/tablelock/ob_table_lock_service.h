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

namespace observer
{
struct ObGlobalContext;
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
    ObTableLockCtx(const ObTableLockTaskType task_type,
                   const uint64_t table_id,
                   const int64_t origin_timeout_us,
                   const int64_t timeout_us);
    ObTableLockCtx(const ObTableLockTaskType task_type,
                   const uint64_t table_id,
                   const uint64_t partition_id_,
                   const int64_t origin_timeout_us,
                   const int64_t timeout_us);
    ObTableLockCtx(const ObTableLockTaskType task_type,
                   const uint64_t table_id,
                   const common::ObTabletID &tablet_id,
                   const int64_t origin_timeout_us,
                   const int64_t timeout_us);
    ObTableLockCtx(const ObTableLockTaskType task_type,
                   const ObLockOBJType obj_type,
                   const uint64_t obj_id,
                   const int64_t origin_timeout_us,
                   const int64_t timeout_us);
    ~ObTableLockCtx() {}
    bool is_try_lock() const { return 0 == timeout_us_; }
    bool is_deadlock_avoid_enabled() const;
    bool is_timeout() const;
    int64_t remain_timeoutus() const;
    int64_t get_rpc_timeoutus() const;
    int64_t get_tablet_cnt() const;
    const common::ObTabletID &get_tablet_id(const int64_t index) const;
    int add_touched_ls(const share::ObLSID &lsid);
    void clean_touched_ls();
    bool is_savepoint_valid() { return -1 != current_savepoint_; }
    void reset_savepoint() { current_savepoint_ = -1; }

    bool is_stmt_savepoint_valid() { return -1 != stmt_savepoint_; }
    void reset_stmt_savepoint() { stmt_savepoint_ = -1; }
    ObTableLockOpType get_lock_op_type() const { return lock_op_type_; }
  public:
    ObTableLockTaskType task_type_; // current lock request type
    bool is_in_trans_;
    union {
      // used for table/partition/tablet
      struct {
        uint64_t table_id_;
        uint64_t partition_id_;        // set when lock or unlock specified partition
        common::ObTabletID tablet_id_; // set when lock or unlock specified tablet
      };
      // used for lock object
      struct {
        ObLockOBJType obj_type_;
        uint64_t obj_id_;
      };
    };

    ObTableLockOpType lock_op_type_; // specify the lock op type

    int64_t origin_timeout_us_;   // the origin timeout us specified by user.
    int64_t timeout_us_;          // the timeout us for every retry times.
    int64_t abs_timeout_ts_;      // the abstract timeout us.
    sql::TransState trans_state_;
    transaction::ObTxDesc *tx_desc_;
    ObTxParam tx_param_;           // the tx param for current tx
    int64_t current_savepoint_;    // used to rollback current sub tx.
    share::ObLSArray need_rollback_ls_; // which ls has been modified after
                                        // the current_savepoint_ created.
    common::ObTabletIDArray tablet_list_; // all the tablets need to be locked/unlocked
    // TODO: yanyuan.cxf we need better performance.
    // share::ObLSArray ls_list_; // related ls list
    int64_t schema_version_;             // the schema version of the table to be locked
    bool tx_is_killed_;                   // used to kill a trans.
    bool is_from_sql_;

    // use to kill the whole lock table stmt.
    int64_t stmt_savepoint_;

    TO_STRING_KV(K(is_in_trans_), K(table_id_), K(partition_id_),
                 K(tablet_id_), K(obj_type_), K(obj_id_), K(lock_op_type_),
                 K(origin_timeout_us_), K(timeout_us_),
                 K(abs_timeout_ts_), KPC(tx_desc_), K(tx_param_),
                 K(current_savepoint_), K(need_rollback_ls_),
                 K(tablet_list_), K(schema_version_), K(tx_is_killed_),
                 K(is_from_sql_), K(stmt_savepoint_));
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
    int garbage_collect_right_now();

    TO_STRING_KV(KP(this),
                 K_(last_success_timestamp));
  private:
    int garbage_collect_for_all_ls_();
    void check_and_report_timeout_();
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
  // arg.op_type_ specified the behavior after trans commit or abort:
  // lock will be unlocked if it is IN_TRANS_COMMON_LOCK,
  // lock will be left if it is OUT_TRANS_LOCK and the trans is commited and we need
  // another trans to unlock the lock.
  // lock the table level lock and all the tablet level lock of this table.
  int lock_table(ObTxDesc &tx_desc,
                 const ObTxParam &tx_param,
                 const ObLockTableRequest &arg);
  // arg.op_type_ should be OUT_TRANS_UNLOCK:
  // unlock the table level lock and all the tablet level lock of this table.
  int unlock_table(ObTxDesc &tx_desc,
                   const ObTxParam &tx_param,
                   const ObUnLockTableRequest &arg);
  // arg.op_type_ specified the behavior after trans commit or abort:
  // lock will be unlocked if it is IN_TRANS_COMMON_LOCK,
  // lock will be left if it is OUT_TRANS_LOCK and the trans is commited and we need
  // another trans to unlock the lock.
  // lock the tablet level lock and all RX or RS at table level lock.
  int lock_tablet(ObTxDesc &tx_desc,
                  const ObTxParam &tx_param,
                  const ObLockTabletRequest &arg);
  // arg.op_type_ should be OUT_TRANS_UNLOCK:
  // unlock the table level lock and the tablet level lock.
  int unlock_tablet(ObTxDesc &tx_desc,
                    const ObTxParam &tx_param,
                    const ObUnLockTabletRequest &arg);
  int lock_partition_or_subpartition(ObTxDesc &tx_desc,
                                     const ObTxParam &tx_param,
                                     const ObLockPartitionRequest &arg);
  int lock_partition(ObTxDesc &tx_desc,
                     const ObTxParam &tx_param,
                     const ObLockPartitionRequest &arg);
  int unlock_partition(ObTxDesc &tx_desc,
                       const ObTxParam &tx_param,
                       const ObUnLockPartitionRequest &arg);
  int lock_subpartition(ObTxDesc &tx_desc,
                        const ObTxParam &tx_param,
                        const ObLockPartitionRequest &arg);
  int unlock_subpartition(ObTxDesc &tx_desc,
                          const ObTxParam &tx_param,
                          const ObUnLockPartitionRequest &arg);
  int lock_obj(ObTxDesc &tx_desc,
               const ObTxParam &tx_param,
               const ObLockObjRequest &arg);
  int unlock_obj(ObTxDesc &tx_desc,
                 const ObTxParam &tx_param,
                 const ObUnLockObjRequest &arg);
  int garbage_collect_right_now();
  int get_obj_lock_garbage_collector(ObOBJLockGarbageCollector *&obj_lock_garbage_collector);
private:
  bool need_retry_single_task_(const ObTableLockCtx &ctx,
                               const int64_t ret) const;
  int rewrite_return_code_(const int ret) const;
  int process_lock_task_(ObTableLockCtx &ctx,
                         const ObTableLockMode lock_mode,
                         const ObTableLockOwnerID lock_owner);
  int process_obj_lock_task_(ObTableLockCtx &ctx,
                             const ObTableLockMode lock_mode,
                             const ObTableLockOwnerID lock_owner);
  int process_table_lock_task_(ObTableLockCtx &ctx,
                               const ObTableLockMode lock_mode,
                               const ObTableLockOwnerID lock_owner);
  int start_tx_(ObTableLockCtx &ctx);
  int end_tx_(ObTableLockCtx &ctx, const bool is_rollback);
  int start_sub_tx_(ObTableLockCtx &ctx);
  int end_sub_tx_(ObTableLockCtx &ctx, const bool is_rollback);
  int start_stmt_(ObTableLockCtx &ctx);
  int end_stmt_(ObTableLockCtx &ctx, const bool is_rollback);
  int check_op_allowed_(const uint64_t table_id,
                        const ObTableSchema *table_schema,
                        bool &is_allowed);
  int get_process_tablets_(const ObTableLockMode lock_mode,
                           const ObTableSchema *table_schema,
                           ObTableLockCtx &ctx);
  int get_ls_lock_map_(ObTableLockCtx &ctx,
                       LockMap &lock_map,
                       ObLSLockMap &ls_lock_map);
  int get_tablet_ls_(const ObTabletID &tablet_id,
                     share::ObLSID &ls_id);
  int get_ls_leader_(const int64_t cluster_id,
                     const uint64_t tenant_id,
                     const share::ObLSID &ls_id,
                     const int64_t abs_timeout_ts,
                     ObAddr &addr);
  int pack_request_(ObTableLockCtx &ctx,
                    const ObTableLockTaskType task_type,
                    const ObTableLockMode &lock_mode,
                    const ObTableLockOwnerID &lock_owner,
                    const ObLockID &lock_id,
                    const share::ObLSID &ls_id,
                    ObAddr &addr,
                    ObTableLockTaskRequest &request);
  int pack_batch_request_(ObTableLockCtx &ctx,
                          const ObTableLockTaskType task_type,
                          const ObTableLockMode &lock_mode,
                          const ObTableLockOwnerID &lock_owner,
                          const share::ObLSID &ls_id,
                          const ObLockIDArray &lock_ids,
                          ObLockTaskBatchRequest &request);
  template<class RpcProxy>
  int parallel_rpc_handle_(RpcProxy &proxy_batch,
                           ObTableLockCtx &ctx,
                           const LockMap &lock_map,
                           const ObTableLockMode lock_mode,
                           const ObTableLockOwnerID lock_owner);
  template<class RpcProxy>
  int batch_rpc_handle_(RpcProxy &proxy_batch,
                        ObTableLockCtx &ctx,
                        const ObLSLockMap &lock_map,
                        const ObTableLockMode lock_mode,
                        const ObTableLockOwnerID lock_owner);
  template<class RpcProxy>
  int handle_parallel_rpc_response_(int rpc_call_ret,
                                    int64_t rpc_count,
                                    RpcProxy &proxy_batch,
                                    ObTableLockCtx &ctx,
                                    ObArray<share::ObLSID> &ls_array);
  int inner_process_obj_lock_(ObTableLockCtx &ctx,
                              const LockMap &lock_map,
                              const ObLSLockMap &ls_lock_map,
                              const ObTableLockMode lock_mode,
                              const ObTableLockOwnerID lock_owner);
  int inner_process_obj_lock_old_version_(ObTableLockCtx &ctx,
                                          const LockMap &lock_map,
                                          const ObTableLockMode lock_mode,
                                          const ObTableLockOwnerID lock_owner);
  int inner_process_obj_lock_batch_(ObTableLockCtx &ctx,
                                    const ObLSLockMap &ls_lock_map,
                                    const ObTableLockMode lock_mode,
                                    const ObTableLockOwnerID lock_owner);
  int process_obj_lock_(ObTableLockCtx &ctx,
                        const share::ObLSID &ls_id,
                        const ObLockID &lock_id,
                        const ObTableLockMode lock_mode,
                        const ObTableLockOwnerID lock_owner);
  static bool is_part_table_lock_(const ObTableLockTaskType task_type);
  int get_table_lock_mode_(const ObTableLockTaskType task_type,
                           const ObTableLockMode part_lock_mode,
                           ObTableLockMode &table_lock_mode);
  int process_table_lock_(ObTableLockCtx &ctx,
                          const ObTableLockMode lock_mode,
                          const ObTableLockOwnerID lock_owner);
  int process_table_tablet_lock_(ObTableLockCtx &ctx,
                                 const ObTableLockMode lock_mode,
                                 const ObTableLockOwnerID lock_owner,
                                 const LockMap &lock_map,
                                 const ObLSLockMap &ls_lock_map);
  // only useful in LOCK_TABLE/LOCK_PARTITION
  int pre_check_lock_(ObTableLockCtx &ctx,
                      const ObTableLockMode lock_mode,
                      const ObTableLockOwnerID lock_owner,
                      const ObLSLockMap &ls_lock_map);
  int batch_pre_check_lock_(ObTableLockCtx &ctx,
                            const ObTableLockMode lock_mode,
                            const ObTableLockOwnerID lock_owner,
                            const ObLSLockMap &ls_lock_map);
  int pre_check_lock_old_version_(ObTableLockCtx &ctx,
                                  const ObTableLockMode lock_mode,
                                  const ObTableLockOwnerID lock_owner);
  // used by deadlock detector.
  int deal_with_deadlock_(ObTableLockCtx &ctx);
  int get_table_partition_level_(const ObTableID table_id, ObPartitionLevel &part_level);

  DISALLOW_COPY_AND_ASSIGN(ObTableLockService);
private:
  // TODO: yanyuan.cxf use parallel rpc and modify this to 5s.
  static const int64_t DEFAULT_TIMEOUT_US = 15L * 1000L * 1000L; // 15s
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
