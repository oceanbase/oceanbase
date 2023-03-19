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

#ifndef OCEANBASE_STORAGE_OB_TRANS_TABLE
#define OCEANBASE_STORAGE_OB_TRANS_TABLE

#include "share/ob_rpc_struct.h"
#include "lib/worker.h"
#include "storage/ob_storage_struct.h"
#include "storage/tx_table/ob_tx_table_interface.h"
#include "storage/tx_table/ob_tx_data_table.h"
#include "storage/tx/ob_tx_data_functor.h"
#include "storage/tx_table/ob_tx_ctx_table.h"

namespace oceanbase
{

namespace share
{
namespace schema
{
class ObTableSchema;
} // schema
} // share

namespace storage
{
class ObLS;
class ObTxTable
{
  // Delay recycle tx data 5 minutes
  const static int64_t TX_DATA_DELAY_RECYCLE_TIME_NS = 5L * 60L * 1000L * 1000L * 1000L;

public:
  static const int64_t INVALID_READ_EPOCH = -1;
  static const int64_t CHECK_AND_ONLINE_PRINT_INVERVAL_US = 5 * 1000 * 1000; // 5 seconds

  enum TxTableState : int64_t
  {
    OFFLINE = 0,
    ONLINE,
    PREPARE_OFFLINE,
    STATE_CNT
  };

public:
  ObTxTable()
      : is_inited_(false),
        epoch_(INVALID_READ_EPOCH),
        state_(OFFLINE),
        ls_(nullptr),
        tx_data_table_(default_tx_data_table_) {}

  ObTxTable(ObTxDataTable &tx_data_table)
      : is_inited_(false),
        epoch_(INVALID_READ_EPOCH),
        state_(OFFLINE),
        ls_(nullptr),
    tx_data_table_(tx_data_table) {}
  ~ObTxTable() {}

  int init(ObLS *ls);
  int start();
  void stop();
  void destroy();
  int create_tablet(const lib::Worker::CompatMode compat_mode, const share::SCN &create_scn);
  int remove_tablet();
  int load_tx_table();
  int prepare_offline();
  int offline();
  int online();

  // In OB4 .0, transaction contexts are divided into exec_data and tx_data. Where exec_data
  // indicates the data required when the transaction is running,and tx_data indicates the data that
  // may still be required after the transaction commits. To avoid memory copying, the entire life
  // cycle of tx_data is maintained by tx data table.Therefore, when a transaction is started, the
  // memory of tx_data needs to be allocated by this function
  //
  // @param [out] tx_data, a tx data allocated by slice allocator
  int alloc_tx_data(ObTxDataGuard &tx_data_guard);

  int deep_copy_tx_data(const ObTxDataGuard &in_tx_data_guard, ObTxDataGuard &out_tx_data_guard);

  // insert a tx data to tx data memtable
  //
  // @param [in] tx_data, which to be inserted
  int insert(ObTxData *&tx_data);

  // =============== Interface for sstable to get txn information =====================

  /**
   * @brief do some checking with tx data user has to implement the check functor derived from ObITxDataCheckFunctor
   * 
   * @param[in] tx_id tx_id, the tx id of the transaction to be checked 
   * @param[in] fn the functor implemented by user
   * @param[in] read_epoch to make sure the version of tx data is what the callers want to be
   */
  int check_with_tx_data(const transaction::ObTransID tx_id, ObITxDataCheckFunctor &fn, const int64_t read_epoch);

  /**
   * @brief check whether the row key is locked by tx id
   * 
   * @param[in] read_trans_id 
   * @param[in] data_trans_id 
   * @param[in] sql_sequence 
   * @param[in] read_epoch 
   * @param[out] lock_state 
   */
  int check_row_locked(const transaction::ObTransID &read_tx_id,
                       const transaction::ObTransID &data_tx_id,
                       const int64_t sql_sequence,
                       const int64_t read_epoch,
                       storage::ObStoreRowLockState &lock_state);

  /**
   * @brief check whether transaction data_tx_id with sql_sequence is readable. (sql_sequence may be unreadable for txn or stmt rollback)
   * 
   * @param[in] data_tx_id 
   * @param[in] sql_sequence 
   * @param[in] read_epoch 
   * @param[out] can_read 
   */
  int check_sql_sequence_can_read(const transaction::ObTransID &data_tx_id,
                                  const int64_t sql_sequence,
                                  const int64_t read_epoch,
                                  bool &can_read);

  /**
   * @brief fetch the state of txn DATA_TRANS_ID when replaying to LOG_TS the requirement can be seen from
   * 
   *
   * @param[in] data_trans_id
   * @param[in] scn
   * @param[in] read_epoch
   * @param[out] state
   * @param[out] trans_version
   */
  int get_tx_state_with_scn(const transaction::ObTransID &data_trans_id,
                               const share::SCN scn,
                               const int64_t read_epoch,
                               int64_t &state,
                               share::SCN &trans_version);

  /**
   * @brief Try to get a tx data from tx_data_table. This function used in special situation when the trans service do
   * not be sure if the tx dat is existed or not. This function will not report error log if the tx data is not existed.
   *
   * @param[in] tx_id
   * @param[in] read_epoch
   * @param[out] state
   * @param[out] trans_version
   */
  int try_get_tx_state(const transaction::ObTransID tx_id,
                       const int64_t read_epoch,
                       int64_t &state,
                       share::SCN &trans_version);

  /**
   * @brief the txn READ_TRANS_ID use SNAPSHOT_VERSION to read the data, and check whether the data is locked, readable or unreadable by txn DATA_TRANS_ID. READ_LATEST is used to check whether read the data belong to the same txn
   * 
   * @param[in] lock_for_read_arg 
   * @param[in] read_epoch 
   * @param[out] can_read 
   * @param[out] trans_version 
   * @param[out] is_determined_state 
   * @param[in] op 
   */
  int lock_for_read(const transaction::ObLockForReadArg &lock_for_read_arg,
                    const int64_t read_epoch,
                    bool &can_read,
                    share::SCN &trans_version,
                    bool &is_determined_state,
                    const ObCleanoutOp &cleanout_op = ObCleanoutNothingOperation(),
                    const ObReCheckOp &recheck_op = ObReCheckNothingOperation());

  /**
   * @brief cleanout the tx state when encountering the uncommitted node. The node will be cleaned out if the state of
   * the txn is decided or prepared. You neeed notice that txn commit or abort is pereformed both on mvcc row and mvcc
   * txn node. And need row latch is used for lock_for_read to shorten critical path.
   *
   * @param[in] tx_id
   * @param[in] read_epoch
   * @param[in] value
   * @param[in] tnode
   * @param[in] need_row_latch
   */
  int cleanout_tx_node(const transaction::ObTransID &tx_id,
                       const int64_t read_epoch,
                       memtable::ObMvccRow &value,
                       memtable::ObMvccTransNode &tnode,
                       const bool need_row_latch);

  /**
   * @brief The tx data sstables need to be cleared periodically. This function returns a recycle_scn
   * to decide which tx data should be cleared.
   *
   * @param[out] recycle_scn the tx data whose end_scn is smaller or equals to the recycle_scn can
   * be cleared.
   */
  int get_recycle_scn(share::SCN &recycle_scn);

  /**
   * @brief Get the upper trans version for each given end_scn
   *
   * @param[in] sstable_end_scn the end_scn of the data sstable which is waitting to get the upper_trans_version
   * @param[out] upper_trans_version the upper_trans_version
   */
  int get_upper_trans_version_before_given_scn(const share::SCN sstable_end_scn, share::SCN &upper_trans_version);

  /**
   * @brief When a transaction is replayed in the middle, it will read tx data from tx data sstable
   * and fill in the undo actions if there is a tx data exist.
   *
   * @param[in & out] tx_data The pointer of tx data to be supplemented which is in tx ctx.
   */
  int supplement_undo_actions_if_exist(ObTxData *tx_data);

  int prepare_for_safe_destroy();

  /**
   * @brief the start_tx_scn used for deciding whether existed tx data sstable can be reused or not(in rebuild
   * situation)
   *
   * This scn can be simply interpreted as the end_scn of the oldest transaction in tx data sstables. For more details,
   * see 
   *
   * @param[out] start_tx_scn
   */
  int get_start_tx_scn(share::SCN &start_tx_scn);

  int dump_single_tx_data_2_text(const int64_t tx_id_int, const char *fname);

  const char* get_state_string(const int64_t state) const;

  TO_STRING_KV(KP(this), K_(is_inited), K_(epoch), "state", get_state_string(state_), KP_(ls), K_(tx_data_table));

public: // getter & setter
  ObTxDataTable *get_tx_data_table() { return &tx_data_table_; }
  ObTxCtxTable *get_tx_ctx_table() { return &tx_ctx_table_; }
  int get_tx_table_guard(ObTxTableGuard &guard);
  int64_t get_epoch() const { return ATOMIC_LOAD(&epoch_); }
  TxTableState get_state() const { return ATOMIC_LOAD(&state_); }

  static int64_t get_filter_col_idx();

private:
  int create_data_tablet_(
      const uint64_t tenant_id,
      const share::ObLSID ls_id,
      const lib::Worker::CompatMode compat_mode,
      const share::SCN &create_scn);
  int create_ctx_tablet_(
      const uint64_t tenant_id,
      const share::ObLSID ls_id,
      const lib::Worker::CompatMode compat_mode,
      const share::SCN &create_scn);
  int remove_tablet_(const common::ObTabletID &tablet_id);
  int remove_data_tablet_();
  int remove_ctx_tablet_();
  int get_data_table_schema_(
      const uint64_t tenant_id,
      share::schema::ObTableSchema &schema);
  int get_ctx_table_schema_(
      const uint64_t tenant_id,
      share::schema::ObTableSchema &schema);
  int gen_create_tablet_arg_(
      const common::ObTabletID &tablet_id,
      const uint64_t tenant_id,
      const share::ObLSID ls_id,
      const lib::Worker::CompatMode compat_mode,
      const share::schema::ObTableSchema &table_schema,
      obrpc::ObBatchCreateTabletArg &arg);
  int gen_remove_tablet_arg_(
      const common::ObTabletID &tablet_id,
      const uint64_t tenant_id,
      const share::ObLSID ls_id,
      obrpc::ObBatchRemoveTabletArg &arg);
  int restore_tx_ctx_table_(ObITable &trans_sstable);
  int load_tx_ctx_table_();
  int load_tx_data_table_();
  int offline_tx_ctx_table_();
  int offline_tx_data_table_();

  void check_state_and_epoch_(const transaction::ObTransID tx_id,
                              const int64_t read_epoch,
                              const bool need_log_error,
                              int &ret);
private:
  static const int64_t LS_TX_CTX_SCHEMA_VERSION = 0;
  static const int64_t LS_TX_CTX_SCHEMA_ROWKEY_CNT = 1;
  static const int64_t LS_TX_CTX_SCHEMA_COLUMN_CNT = 3;
  bool is_inited_;
  int64_t epoch_ CACHE_ALIGNED;
  TxTableState state_ CACHE_ALIGNED;
  ObLS *ls_;
  ObTxCtxTable tx_ctx_table_;
  // The Tx Data will be inserted into tx_data_table_ after transaction commit or abort
  ObTxDataTable default_tx_data_table_;
  ObTxDataTable &tx_data_table_;
};
}  // namespace storage
}  // namespace oceanbase
#endif
