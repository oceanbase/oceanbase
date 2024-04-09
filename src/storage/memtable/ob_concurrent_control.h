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

#ifndef OCEANBASE_STORAGE_MEMTABLE_OB_CONCURRENT_CONTROL
#define OCEANBASE_STORAGE_MEMTABLE_OB_CONCURRENT_CONTROL

#include "storage/tx/ob_trans_define.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace concurrent_control
{

// write flag is used for write flag modification
struct ObWriteFlag
{
  #define OBWF_BIT_TABLE_API        1
  #define OBWF_BIT_TABLE_LOCK       1
  #define OBWF_BIT_MDS              1
  #define OBWF_BIT_DML_BATCH_OPT    1
  #define OBWF_BIT_INSERT_UP        1
  #define OBWF_BIT_WRITE_ONLY_INDEX 1
  #define OBWF_BIT_CHECK_ROW_LOCKED 1
  #define OBWF_BIT_LOB_AUX          1
  #define OBWF_BIT_SKIP_FLUSH_REDO  1
  #define OBWF_BIT_RESERVED         55

  static const uint64_t OBWF_MASK_TABLE_API = (0x1UL << OBWF_BIT_TABLE_API) - 1;
  static const uint64_t OBWF_MASK_TABLE_LOCK = (0x1UL << OBWF_BIT_TABLE_LOCK) - 1;
  static const uint64_t OBWF_MASK_MDS = (0x1UL << OBWF_BIT_MDS) - 1;
  static const uint64_t OBWF_MASK_DML_BATCH_OPT = (0x1UL << OBWF_BIT_DML_BATCH_OPT) - 1;
  static const uint64_t OBWF_MASK_INSERT_UP = (0x1UL << OBWF_BIT_INSERT_UP) - 1;
  static const uint64_t OBWF_MASK_WRITE_ONLY_INDEX = (0x1UL << OBWF_BIT_WRITE_ONLY_INDEX) - 1;
  static const uint64_t OBWF_MASK_CHECK_ROW_LOCKED = (0x1UL << OBWF_BIT_CHECK_ROW_LOCKED) - 1;

  union
  {
    uint64_t flag_;
    struct
    {
      uint64_t is_table_api_        : OBWF_BIT_TABLE_API;        // 0: false(default), 1: true
      uint64_t is_table_lock_       : OBWF_BIT_TABLE_LOCK;       // 0: false(default), 1: true
      uint64_t is_mds_              : OBWF_BIT_MDS;              // 0: false(default), 1: true
      uint64_t is_dml_batch_opt_    : OBWF_BIT_DML_BATCH_OPT;    // 0: false(default), 1: true
      uint64_t is_insert_up_        : OBWF_BIT_INSERT_UP;        // 0: false(default), 1: true
      uint64_t is_write_only_index_ : OBWF_BIT_WRITE_ONLY_INDEX; // 0: false(default), 1: true
      uint64_t is_check_row_locked_ : OBWF_BIT_CHECK_ROW_LOCKED; // 0: false(default), 1: true
      uint64_t is_lob_aux_          : OBWF_BIT_LOB_AUX;          // 0: false(default), 1: true
      uint64_t is_skip_flush_redo_  : OBWF_BIT_SKIP_FLUSH_REDO;  // 0: false(default), 1: true
      uint64_t reserved_            : OBWF_BIT_RESERVED;
    };
  };

  ObWriteFlag() : flag_(0) {}
  void reset() { flag_ = 0; }
  inline bool is_table_api() const { return is_table_api_; }
  inline void set_is_table_api() { is_table_api_ = true; }
  inline bool is_table_lock() const { return is_table_lock_; }
  inline void set_is_table_lock() { is_table_lock_ = true; }
  inline bool is_mds() const { return is_mds_; }
  inline void set_is_mds() { is_mds_ = true; }
  inline bool is_dml_batch_opt() const { return is_dml_batch_opt_; }
  inline void set_is_dml_batch_opt() { is_dml_batch_opt_ = true; }
  inline bool is_insert_up() const { return is_insert_up_; }
  inline void set_is_insert_up() { is_insert_up_ = true; }
  inline bool is_write_only_index() const { return is_write_only_index_; }
  inline void set_is_write_only_index() { is_write_only_index_ = true; }
  inline bool is_check_row_locked() const { return is_check_row_locked_; }
  inline void set_check_row_locked() { is_check_row_locked_ = true; }
  inline bool is_lob_aux() const { return is_lob_aux_; }
  inline void set_lob_aux() { is_lob_aux_ = true; }
  inline bool is_skip_flush_redo() const { return is_skip_flush_redo_; }
  inline void set_skip_flush_redo() { is_skip_flush_redo_ = true; }
  inline void unset_skip_flush_redo() { is_skip_flush_redo_ = false; }

  TO_STRING_KV("is_table_api", is_table_api_,
               "is_table_lock", is_table_lock_,
               "is_mds", is_mds_,
               "is_dml_batch_opt", is_dml_batch_opt_,
               "is_insert_up", is_insert_up_,
               "is_write_only_index", is_write_only_index_,
               "is_check_row_locked", is_check_row_locked_,
               "is_lob_aux", is_lob_aux_,
               "is_skip_flush_redo", is_skip_flush_redo_);

  OB_UNIS_VERSION(1);
};

// TODO(handora.qc): Move more concurrent control related method into the function

// In Oceanbase 4.0, in order to prevent concurrent unique key insertion txns
// from inserting the same row at the same time, we decompose the unique key
// insertion into three actions: 1. existence verification, 2. data insertion
// and 3. lost update detection. That is, based on the snapshot isolation
// concurrency Control, the inherently guaranteed serial read and write
// capabilities on a single row of data solves the problem of concurrent unique
// key insertion. For two txns, T1 and T2, if T1's commit version is lower than
// T2's read version, T2 will see T1's insertion through existence verification;
// If T1's commit version is bigger than T2's read version, T2 will not see it
// under snapshot, while T2 will report TSC when found T1's update.
//
// After supporting the PDML, the txn has increased the ability to operate data
// concurrently in the same statement. Especially in the case of an index, it
// will introduce the situation of concurrently operating the same row.
// Therefore, the mentioned method cannot solve the concurrent unique key
// insertion of the same txn in this case(Because TSC will not help for the same
// txn).
//
// We consider that the essence of the problem is that tasks do not support
// serial read and write capabilities for single-row data. Therefore, under this
// idea, we consider whether it is possible to transplant concurrency control
// for serial read and write capabilities of single-row. Therefore, we consider
// To use the sequence number. Sequence number guarantees the following three
// principles.
// 1. The read sequence number is equal between all task in this statement.
// 2. The write sequence number is greater than the read sequence number of this
//    statement.
// 3. The reader seq no is bigger or equal than the seq no of the last
//    statement.
//
// With the above guarantees, we can realize whether there are concurrent
// operations of reading and writing the same data row in the same statement.
// That is, we will Find whether there is a write operation of the same
// transaction, whose write sequence number is greater or equal than the read
// sequence number of this operation. If there is a conflict, this task will be
// rolled back. This solves the common problem between B and C mentioned above
// It is guaranteed that only There is a concurrent modification of a task.
int check_sequence_set_violation(const concurrent_control::ObWriteFlag write_flag,
                                 const transaction::ObTxSEQ reader_seq_no,
                                 const transaction::ObTransID checker_tx_id,
                                 const blocksstable::ObDmlFlag checker_dml_flag,
                                 const transaction::ObTxSEQ checker_seq_no,
                                 const transaction::ObTransID locker_tx_id,
                                 const blocksstable::ObDmlFlag locker_dml_flag,
                                 const transaction::ObTxSEQ locker_seq_no);

} // namespace concurrent_control
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MEMTABLE_OB_CONCURRENT_CONTROL
