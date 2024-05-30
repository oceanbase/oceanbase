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

#include "storage/memtable/ob_concurrent_control.h"

namespace oceanbase
{
namespace concurrent_control
{

OB_SERIALIZE_MEMBER(ObWriteFlag, flag_);

// Currently, we consider the ability to implement the serial read and write
// capabilities for the single-row using the sequence number. Sequence number
// guarantees the following three principles.
// 1. The read sequence number is equal between all task in this statement.
// 2. The write sequence number is greater than the read sequence number of this
//    statement.
// 3. The reader seq no is bigger or equal than the seq no of the last
//    statement.
//
// With the above principles, we can prevent the concurrent operations of
// writing to the same data row in the same statement. That is, during writing
// to a mvcc row, we will check whether there is a data with the same txn id
// providing the lock semantic. And if its write sequence number is greater than
// or equal to the read sequence number of the current write operation, there
// may be a conflict.
//
// NB: Currently, there may be some cases that the txn sequentially increases
// the sequence number of the same row in the same task. And after paying great
// effort to sort out these posibilities(listed in the following cases), new
// possibility should be added carefully and remind the owner of the code.
int check_sequence_set_violation(const concurrent_control::ObWriteFlag write_flag,
                                 const transaction::ObTxSEQ reader_seq_no,
                                 const transaction::ObTransID writer_tx_id,
                                 const blocksstable::ObDmlFlag writer_dml_flag,
                                 const transaction::ObTxSEQ writer_seq_no,
                                 const transaction::ObTransID locker_tx_id,
                                 const blocksstable::ObDmlFlag locker_dml_flag,
                                 const transaction::ObTxSEQ locker_seq_no)
{
  int ret = OB_SUCCESS;
  // TODO(handora.qc): add flag to carefully screen out the different cases. For
  // example, add the lob flag for the case 2.1 to prevent the other scenes from
  // happening.

  if (writer_tx_id == locker_tx_id) {
    // For statements during sql and threads during PDML, the following rules is
    // guaranteed:
    // 1. reader seq no is bigger or equal than the seq no of the last statements
    if (reader_seq_no < locker_seq_no) {
      // Case 1: It may happens that two pdml unique index tasks insert the same
      // row concurrently or two px update with one update(one doesnot change
      // the rowkey) and one insert(one changes the rowkey), so we report
      // duplicate key under the case to prevent the insertion.
      if (blocksstable::ObDmlFlag::DF_INSERT == writer_dml_flag
          && (blocksstable::ObDmlFlag::DF_INSERT == locker_dml_flag
              || blocksstable::ObDmlFlag::DF_UPDATE == locker_dml_flag)) {
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        TRANS_LOG(WARN, "pdml duplicate primary key found", K(ret),
                  K(writer_tx_id), K(writer_dml_flag), K(writer_seq_no),
                  K(locker_tx_id), K(locker_dml_flag), K(locker_seq_no));
      // Case 2.1: For the case of the update in the storage layer, it may be
      // split into lock and update in a single statement and fail the check, so
      // we need bypass this case(Currently only the update of the lob will cause
      // it). We use the common idea that all operations split in the storage
      // layer will use same sequence number, so we bypass the check if the writer
      // sequence number is equal to the locker sequence number.

      // } else if (writer_seq_no == locker_seq_no &&
      //            (blocksstable::ObDmlFlag::DF_UPDATE == writer_dml_flag
      //             && blocksstable::ObDmlFlag::DF_LOCK == locker_dml_flag)) {
      //
      // Case 2.2: For the case of the self reference of the foreign key, it may
      // be split into lock and insert/update in a single statement, so we need
      // bypass this case(TODO(handora.qc): remove the requirement after yichang's
      // modification).
      } else if (blocksstable::ObDmlFlag::DF_LOCK == locker_dml_flag) {
        // bypass the case
      // Case 3: For the case of the update of the primary key in the sql layer,
      // it will be split into a delete of the old rowkey and an insert for the
      // new one which will fail to pass the check. And the two operations may
      // even be split into two sequentially ordered steps to follow the sql-layer
      // semantics, so we need bypass this case.
      } else if (blocksstable::ObDmlFlag::DF_INSERT == writer_dml_flag
                 && blocksstable::ObDmlFlag::DF_DELETE == locker_dml_flag) {
        // bypass the case
      // Case 4: For the case of the insert of two same rowkeys with insert onto
      // duplicate, it will be split into a insert of the rowkey and an update or
      // delete of the same one which will fail to pass the check. And the two
      // operations is split into two sequentially ordered steps so we need bypass
      // this case.
      } else if ((blocksstable::ObDmlFlag::DF_UPDATE == writer_dml_flag
                  || blocksstable::ObDmlFlag::DF_DELETE == writer_dml_flag)
                 && blocksstable::ObDmlFlag::DF_INSERT == locker_dml_flag) {
        // bypass the case
      // Case 5: For the case of table api, it inserts rows under the same stmt,
      // and so fail to pass the check. We must bypass the case.
      } else if (write_flag.is_table_api()) {
        // bypass the case
      // Case 6: For the case of deleting rows during building the unique index
      // concurrently, it may exist that two rows of the main table point to one
      // row of the newly created index, which means the unique index will abort
      // itself during consistency check. While because of the feature of the
      // online ddl, the concurrent delete will start to operate on the newly
      // created index, which causes these two delete operations and fail to pass
      // the check. So we need bypass this case.
      } else if (blocksstable::ObDmlFlag::DF_DELETE == writer_dml_flag
                 && blocksstable::ObDmlFlag::DF_DELETE == locker_dml_flag) {
        // bypass the case
      // Case 7: For the case of batch dml operation, it may operate the same row
      // concurrently if the first operation has no effects.(SQL layer will check
      // the modification of the row before the second operation, and report the
      // error if the row has been modified while the first row may have no effect
      // and the parallel insert may happen). So we need report the batched stmt
      // warning according to this case.
      } else if (write_flag.is_dml_batch_opt()) {
        ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
        TRANS_LOG(WARN, "batch multi stmt rollback found", K(ret),
                  K(writer_tx_id), K(writer_dml_flag), K(writer_seq_no),
                  K(locker_tx_id), K(locker_dml_flag), K(locker_seq_no));
        // Case 8: For the case of on duplicate key update, it may operate the
        // same row more than once if the sql insert onto duplicate with the
        // same row more than once. It may have no chance to batch the same row.
        // So we need bypass this case.
      } else if (write_flag.is_insert_up()) {
        // bypass the case
      // Case 9: For the case of the write only index, it may operate the same
      // row more than once under the case that main table has two rows pointing
      // to the same index which is building during the first stage(in which
      // stage update and insert should consider index while the index is not
      // readable). In the case, an update may update the same row on the index.
      // So we need report the batched stmt warning according to this case.
      } else if (write_flag.is_write_only_index()) {
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        TRANS_LOG(WARN, "write only index insert/update on the same row", K(ret),
                  K(writer_tx_id), K(writer_dml_flag), K(writer_seq_no),
                  K(locker_tx_id), K(locker_dml_flag), K(locker_seq_no));
      } else {
        // Others: It will never happen that two operaions on the same row for the
        // same txn except the above cases. So we should report unexpected error.
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "multiple modification on one row found", K(reader_seq_no),
                  K(writer_tx_id), K(writer_dml_flag), K(writer_seq_no),
                  K(locker_tx_id), K(locker_dml_flag), K(locker_seq_no));
      }
    }

    if (OB_SUCC(ret) && writer_seq_no < locker_seq_no) {
      // Under the PDML scenario, there exists a case where a row may be
      // concurrently modified by both a delete and an insert operation
      // (consider a scenario like 'a = a + 1'). In such a situation, there is a
      // possibility of sequence numbers becoming out of order on the row which
      // may break the promise of the memtable semantics. We need to be aware of
      // this situation and retry the entire SQL statement in such cases.
      ret = OB_SEQ_NO_REORDER_UNDER_PDML;
      TRANS_LOG(WARN, "wrong row of sequence on one row found", K(reader_seq_no),
                K(writer_tx_id), K(writer_dml_flag), K(writer_seq_no),
                K(locker_tx_id), K(locker_dml_flag), K(locker_seq_no));
    }
  }

  return ret;
}

} // namespace concurrent_control
} // namespace oceanbase
