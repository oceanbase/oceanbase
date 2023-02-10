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
#include "sql/ob_sql_trans_control.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tablelock/ob_table_lock_rpc_proxy.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"

#include "lib/utility/ob_macro_utils.h"

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
  class ObTableLockCtx
  {
  public:
    ObTableLockCtx(const uint64_t table_id,
                   const int64_t origin_timeout_us,
                   const int64_t timeout_us);
    ObTableLockCtx(const uint64_t table_id,
                   const common::ObTabletID &tablet_id,
                   const int64_t origin_timeout_us,
                   const int64_t timeout_us);
    ~ObTableLockCtx() {}
    bool is_try_lock() const { return 0 == timeout_us_; }
    bool is_deadlock_avoid_enabled() const;
    bool is_timeout() const;
    int64_t remain_timeoutus() const;
    int64_t get_rpc_timeoutus() const;
    uint64_t get_tenant_id() const { return tenant_id_; }
    int64_t get_tablet_cnt() const;
    const common::ObTabletID &get_tablet_id(const int64_t index) const;
    int add_touched_ls(const share::ObLSID &lsid);
    void clean_touched_ls();
    bool is_savepoint_valid() { return -1 != current_savepoint_; }
    void reset_savepoint() { current_savepoint_ = -1; }

    bool is_stmt_savepoint_valid() { return -1 != stmt_savepoint_; }
    void reset_stmt_savepoint() { stmt_savepoint_ = -1; }
  public:
    bool is_in_trans_;
    uint64_t tenant_id_;
    uint64_t table_id_;
    common::ObTabletID tablet_id_; // set when lock or unlock specified tablet

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

    // use to kill the whole lock table stmt.
    int64_t stmt_savepoint_;

    TO_STRING_KV(K(is_in_trans_), K(tenant_id_), K(table_id_), K(tablet_id_),
                 K(origin_timeout_us_), K(timeout_us_),
                 K(abs_timeout_ts_), KPC(tx_desc_), K(tx_param_),
                 K(tablet_list_), K(schema_version_), K(tx_is_killed_));
  };

public:
  typedef hash::ObHashMap<ObLockID, share::ObLSID> LockMap;

  ObTableLockService()
    : location_service_(nullptr),
      sql_proxy_(nullptr),
      is_inited_(false) {}
  ~ObTableLockService() {}
  int init();
  static int mtl_init(ObTableLockService* &lock_service);
  int start();
  void stop();
  void wait();
  void destroy();

  // interface for in trans lock
  int lock_table(ObTxDesc &tx_desc,
                 const ObTxParam &tx_param,
                 const uint64_t table_id,
                 const ObTableLockMode lock_mode,
                 const int64_t timeout_us = 0);
  int lock_tablet(ObTxDesc &tx_desc,
                  const ObTxParam &tx_param,
                  const uint64_t table_id,
                  const ObTabletID &tablet_id,
                  const ObTableLockMode lock_mode,
                  const int64_t timeout_us = 0);

  // interface for DDL mode
  int lock_table(const uint64_t table_id,
                 const ObTableLockMode lock_mode,
                 const ObTableLockOwnerID lock_owner,
                 const int64_t timeout_us = 0);
  int unlock_table(const uint64_t table_id,
                   const ObTableLockMode lock_mode,
                   const ObTableLockOwnerID lock_owner,
                   const int64_t timeout_us = 0);

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

private:
  bool need_retry_single_task_(const ObTableLockCtx &ctx,
                               const int64_t ret) const;
  int rewrite_return_code_(const int ret) const;
  int process_lock_task_(ObTableLockCtx &ctx,
                         const ObTableLockTaskType task_type,
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
  int get_process_tablets_(const ObTableLockTaskType task_type,
                           const ObTableSchema *table_schema,
                           ObTableLockCtx &ctx);
  int get_tablet_ls_(const ObTabletID &tablet_id,
                     share::ObLSID &ls_id);
  int get_ls_leader_(const int64_t cluster_id,
                     const uint64_t tenant_id,
                     const share::ObLSID &ls_id,
                     const int64_t abs_timeout_ts,
                     ObAddr &addr);
  int pack_request_(ObTableLockCtx &ctx,
                    const ObTableLockTaskType &task_type,
                    ObTableLockOpType &lock_op_type,
                    const ObTableLockMode &lock_mode,
                    const ObTableLockOwnerID &lock_owner,
                    const ObLockID &lock_id,
                    const share::ObLSID &ls_id,
                    ObAddr &addr,
                    ObTableLockTaskRequest &request);
  template<class RpcProxy>
  int parallel_rpc_handle_(RpcProxy &proxy_batch,
                           ObTableLockCtx &ctx,
                           LockMap &lock_map,
                           const ObTableLockTaskType task_type,
                           const ObTableLockMode lock_mode,
                           const ObTableLockOwnerID lock_owner);
  template<class RpcProxy>
  int handle_parallel_rpc_response_(int rpc_call_ret,
                                    int64_t rpc_count,
                                    RpcProxy &proxy_batch,
                                    ObTableLockCtx &ctx,
                                    ObArray<share::ObLSID> &ls_array);
  int inner_process_obj_lock_(ObTableLockCtx &ctx,
                              LockMap &lock_map,
                              const ObTableLockTaskType task_type,
                              const ObTableLockMode lock_mode,
                              const ObTableLockOwnerID lock_owner);
  int process_obj_lock_(ObTableLockCtx &ctx,
                        const share::ObLSID &ls_id,
                        const ObLockID &lock_id,
                        const ObTableLockTaskType task_type,
                        const ObTableLockMode lock_mode,
                        const ObTableLockOwnerID lock_owner);
  int get_table_lock_mode_(const ObTableLockTaskType task_type,
                           const ObTableLockMode tablet_lock_mode,
                           ObTableLockMode &table_lock_mode);
  int process_table_lock_(ObTableLockCtx &ctx,
                          const ObTableLockTaskType task_type,
                          const ObTableLockMode lock_mode,
                          const ObTableLockOwnerID lock_owner);
  int process_table_tablet_lock_(ObTableLockCtx &ctx,
                                 const ObTableLockTaskType task_type,
                                 const ObTableLockMode lock_mode,
                                 const ObTableLockOwnerID lock_owner);
  // only useful in LOCK_TABLE
  int pre_check_lock_(ObTableLockCtx &ctx,
                      const ObTableLockTaskType task_type,
                      const ObTableLockMode lock_mode,
                      const ObTableLockOwnerID lock_owner);
  // used by deadlock detector.
  int deal_with_deadlock_(ObTableLockCtx &ctx);

  DISALLOW_COPY_AND_ASSIGN(ObTableLockService);
private:
  // TODO: yanyuan.cxf use parallel rpc and modify this to 5s.
  static const int64_t DEFAULT_TIMEOUT_US = 15L * 1000L * 1000L; // 15s
  static const int64_t DEFAULT_RPC_TIMEOUT_US = 2L * 1000L * 1000L; // 2s

  share::ObLocationService *location_service_;
  common::ObMySQLProxy *sql_proxy_;
  bool is_inited_;
};

}

}
}

#endif /* OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_SERVICE_H_ */
