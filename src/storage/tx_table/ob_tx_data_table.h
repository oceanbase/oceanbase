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

#ifndef OCEANBASE_STORAGE_OB_TX_DATA_TABLE
#define OCEANBASE_STORAGE_OB_TX_DATA_TABLE

#include "share/scn.h"
#include "share/ob_occam_timer.h"
#include "share/allocator/ob_tx_data_allocator.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tx_table/ob_tx_data_memtable_mgr.h"
#include "storage/tx_table/ob_tx_table_define.h"

namespace oceanbase
{

namespace common
{
class ObOccamTimer;
}

namespace storage
{
class ObLSTabletService;
class ObITxDataCheckFunctor;
class ObTxCtxTable;
class TxDataTableSelfFreezeGuard;

struct TxDataReadSchema
{
  ObTableIterParam iter_param_;
  ObTableReadInfo read_info_;
  ObRowkeyReadInfo full_read_info_;

  TxDataReadSchema() : iter_param_(), read_info_(), full_read_info_() {}
};

// In Ob4.0, transaction state table is divided into tx data table and tx
// context table. See details :
//
//
// All operatitons related to tx_data are implemented by ObTxDataTable.
// See details ::
//
class ObTxDataTable
{
public:
  struct MemtableHandlesCache
  {
    MemtableHandlesCache() { reuse(); }
    ~MemtableHandlesCache() { memtable_handles_.reset(); }
    int64_t memtable_head_;
    int64_t memtable_tail_;
    TCRWLock lock_;
    ObSEArray<ObTableHandleV2, MAX_MEMSTORE_CNT> memtable_handles_;

    void reuse()
    {
      memtable_head_ = -1;
      memtable_tail_ = -1;
      memtable_handles_.reuse();
    }

    void reset()
    {
      memtable_head_ = -1;
      memtable_tail_ = -1;
      memtable_handles_.reset();
    }

    TO_STRING_KV(K(memtable_head_), K(memtable_tail_), K(memtable_handles_));
  };


  using SliceAllocator = ObSliceAlloc;

  static const int64_t TX_DATA_MAX_CONCURRENCY = 32;
  // A tx data is 128 bytes, 128 * 262144 = 32MB
  static const int64_t SSTABLE_CACHE_MAX_RETAIN_CNT = 262144;
  // The max tps is 150w which means the cache can be inserted 15w tx data during 100ms. So once
  // cache cleaning task will delete at least 11w tx data.
  static const int64_t DEFAULT_CACHE_RETAINED_TIME = 100_ms; // 100ms

  // The tx data memtable will trigger a freeze if its memory use is more than 2%
  static constexpr double TX_DATA_FREEZE_TRIGGER_PERCENTAGE = 2;
  // TODO : @gengli.wzy The active & frozen tx data memtable can not use memory more than 10%
  static constexpr double TX_DATA_MEM_LIMIT_PERCENTAGE = 10;

  enum COLUMN_ID_LIST
  {
    TX_ID = common::OB_APP_MIN_COLUMN_ID,
    IDX,
    TOTAL_ROW_CNT,
    END_LOG_TS,
    VALUE
  };

public:  // ObTxDataTable
  ObTxDataTable()
    : is_inited_(false),
      is_started_(false),
      calc_upper_trans_is_disabled_(false),
      latest_transfer_scn_(),
      ls_id_(),
      tablet_id_(0),
      arena_allocator_(),
      tx_data_allocator_(nullptr),
      ls_(nullptr),
      ls_tablet_svr_(nullptr),
      memtable_mgr_(nullptr),
      tx_ctx_table_(nullptr),
      read_schema_(),
      calc_upper_trans_version_cache_(),
      memtables_cache_() {}
  ~ObTxDataTable() {}

  virtual int init(ObLS *ls, ObTxCtxTable *tx_ctx_table);
  virtual int start();
  virtual void stop();
  virtual void reset();
  virtual void destroy();
  int offline();
  int online();

  /**
   * @brief the same as ObTxTable::alloc_tx_data
   */
  virtual int alloc_tx_data(ObTxDataGuard &tx_data,
                            const bool enable_throttle = true,
                            const int64_t abs_expire_time = 0);

  /**
   * @brief allocate memory and deep copy tx data
   *
   * @param[in] in_tx_data input tx data
   * @param[out] out_tx_data output tx data
   */
  virtual int deep_copy_tx_data(const ObTxDataGuard &in_tx_data, ObTxDataGuard &out_tx_data);

  /**
   * @brief In order to reduce memory fragmentation and improve memory reuse rate,the variable
   * undo_status is divided into multiple fixed-length ObUndoAction arrays and concatenated with a
   * single linked list.The undo_status linked list is empty if the transaction does not rollback.
   * If the transaction rolls back some action, memory slices are dynamically allocated to store
   * undo action
   *
   * @param[out] undo_status_node allocated by slice allocator, which contains the next pointer
   * and ObUndoAction Array.
   */
  virtual int alloc_undo_status_node(ObUndoStatusNode *&undo_status_node);
  /**
   * @brief Free only one undo status node allocated by alloc_undo_status_node() function.
   *
   * @param undo_status_node the undo status node which is waiting for free
   */
  virtual int free_undo_status_node(ObUndoStatusNode *&undo_status_node);
  /**
   * @brief Insert tx data into tx data table
   *
   * @param[in] tx_data the tx data to insert
   */
  virtual int insert(ObTxData *&tx_data);

  /**
   * @brief Do some checking with tx data.
   * In order to reuse the logic, we implement the functor which can be use by tx ctx table and tx
   * data table to deal with tx data.
   *
   * @param[in] tx_id the tx id of the transaction to be checked
   * @param[in] fn the functor which is dealt with tx data
   */
  virtual int check_with_tx_data(const transaction::ObTransID tx_id,
                                 ObITxDataCheckFunctor &fn,
                                 ObTxDataGuard &tx_data_guard,
                                 share::SCN &recycled_scn);

  /**
   * @brief See ObTxTable::get_recycle_scn
   */
  int get_recycle_scn(share::SCN &recycle_scn);

  /**
   * @brief see ObTxTable::get_upper_trans_version_before_given_scn()
   */
  int get_upper_trans_version_before_given_scn(const share::SCN sstable_end_scn, share::SCN &upper_trans_version);

  /**
   * @brief see ObTxTable::supplement_tx_op_if_exist
   */
  int supplement_tx_op_if_exist(ObTxData *tx_data);

  int self_freeze_task();

  int update_memtables_cache();

  int prepare_for_safe_destroy();

  /**
   * @brief see ObTxTable::get_start_tx_scn
   */
  int get_start_tx_scn(share::SCN &start_tx_scn);

  void reuse_memtable_handles_cache();

  int dump_single_tx_data_2_text(const int64_t tx_id_int, FILE *fd);

  TO_STRING_KV(KP(this),
               K_(is_inited),
               K_(is_started),
               K_(ls_id),
               K_(tablet_id),
               K_(memtables_cache),
               KP_(ls),
               KP_(ls_tablet_svr),
               KP_(memtable_mgr),
               KP_(tx_ctx_table),
               KP_(&tx_data_allocator));

public: // getter and setter
  share::ObTenantTxDataAllocator *get_tx_data_allocator() { return tx_data_allocator_; }
  TxDataReadSchema &get_read_schema() { return read_schema_; };

  share::ObLSID get_ls_id();
  void disable_upper_trans_calculation();
  void enable_upper_trans_calculation(const share::SCN latest_transfer_scn);

private:
  virtual ObTxDataMemtableMgr *get_memtable_mgr_() { return memtable_mgr_; }

  int init_slice_allocator_();

  int init_arena_allocator_();

  int init_sstable_cache_();


  int register_clean_cache_task_();

  int check_tx_data_in_memtable_(const transaction::ObTransID tx_id, ObITxDataCheckFunctor &fn, ObTxDataGuard &tx_data_guard);

  int check_tx_data_with_cache_once_(const transaction::ObTransID tx_id, ObITxDataCheckFunctor &fn, ObTxDataGuard &tx_data_guard);

  int get_tx_data_from_cache_(const transaction::ObTransID tx_id, ObTxDataGuard &tx_data_guard, bool &find);

  int check_tx_data_in_sstable_(const transaction::ObTransID tx_id,
                                ObITxDataCheckFunctor &fn,
                                ObTxDataGuard &tx_data_guard,
                                share::SCN &recycled_scn);

  int get_tx_data_in_cache_(const transaction::ObTransID tx_id, ObTxData *&tx_data);

  int get_tx_data_in_sstable_(const transaction::ObTransID tx_id, ObTxData &tx_data, share::SCN &recycled_scn);

  int insert_(ObTxData *&tx_data, ObTxDataMemtableWriteGuard &write_guard);

  int insert_into_memtable_(ObTxDataMemtable *tx_data_memtable, ObTxData *&tx_data);

  // free the whole undo status list allocated by slice allocator

  int deep_copy_undo_status_list_(const ObUndoStatusList &in_list, ObUndoStatusList &out_list);
  int init_tx_data_read_schema_();

  int update_cache_if_needed_(bool &skip_calc);

  int update_calc_upper_trans_version_cache_(ObITable *table);

  int calc_upper_trans_scn_(const share::SCN sstable_end_scn, share::SCN &upper_trans_version);

  int update_freeze_trigger_threshold_();

  int check_need_update_memtables_cache_(bool &need_update);

  int get_tx_data_in_memtables_cache_(const transaction::ObTransID tx_id,
                                      ObTableHandleV2 &src_memtable_handle,
                                      ObTxDataGuard &tx_data_guard,
                                      bool &find);

  int clean_memtables_cache_();

  int dump_tx_data_in_memtable_2_text_(const transaction::ObTransID tx_id, FILE *fd);
  int dump_tx_data_in_sstable_2_text_(const transaction::ObTransID tx_id, FILE *fd);

  int DEBUG_slowly_calc_upper_trans_version_(const share::SCN &sstable_end_scn,
                                             share::SCN &tmp_upper_trans_version);

  int DEBUG_calc_with_all_sstables_(ObTableAccessContext &access_context,
                                    const share::SCN &sstable_end_scn,
                                    share::SCN &tmp_upper_trans_version);
  int DEBUG_calc_with_row_iter_(ObStoreRowIterator *row_iter,
                                const share::SCN &sstable_end_scn,
                                share::SCN &tmp_upper_trans_version);
  bool skip_this_sstable_end_scn_(const share::SCN &sstable_end_scn);
  int check_min_start_in_ctx_(const share::SCN &sstable_end_scn,
                              const share::SCN &max_decided_scn,
                              share::SCN &min_start_scn,
                              share::SCN &effective_scn,
                              bool &need_skip);
  int check_min_start_in_tx_data_(const share::SCN &sstable_end_scn,
                                  share::SCN &min_start_ts_in_tx_data_memtable,
                                  bool &need_skip);
  void print_alloc_size_for_test_();
  // free the whole undo status list allocated by slice allocator
  void free_undo_status_list_(ObUndoStatusNode *node_ptr);
private:
  static const int64_t LS_TX_DATA_SCHEMA_VERSION = 0;
  static const int64_t LS_TX_DATA_SCHEMA_ROWKEY_CNT = 2;
  static const int64_t LS_TX_DATA_SCHEMA_COLUMN_CNT = 5;
  bool is_inited_;
  bool is_started_;
  bool calc_upper_trans_is_disabled_;
  share::SCN latest_transfer_scn_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  // Allocator to allocate ObTxData and ObUndoStatus
  ObArenaAllocator arena_allocator_;
  share::ObTenantTxDataAllocator *tx_data_allocator_;
  ObLS *ls_;
  // Pointer to tablet service, used for get tx data memtable mgr
  ObLSTabletService *ls_tablet_svr_;
  // The tablet id of tx data table
  ObTxDataMemtableMgr *memtable_mgr_;
  ObTxCtxTable *tx_ctx_table_;
  TxDataReadSchema read_schema_;
  CalcUpperTransSCNCache calc_upper_trans_version_cache_;
  MemtableHandlesCache memtables_cache_;
};  // tx_table

}  // namespace storage

}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_TX_DATA_TABLE_
