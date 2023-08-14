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

#ifndef OB_UNCOMMITTED_TRANS_TEST_H_
#define OB_UNCOMMITTED_TRANS_TEST_H_

#include "lib/hash/ob_hashmap.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "mockcontainer/mock_ob_iterator.h"

namespace oceanbase
{
namespace unittest
{

class TestUncommittedMinorMergeScan : public transaction::ObPartitionTransCtxMgr
{
public:
  struct TestTransStatus
  {
    TestTransStatus()
     : status_(transaction::ObTransTableStatusType::RUNNING), commit_trans_version_(INT64_MAX)
    {

    }
    TestTransStatus(
        transaction::ObTransTableStatusType status,
        int64_t commit_trans_version)
        : status_(status), commit_trans_version_(commit_trans_version)
    {
    }
    transaction::ObTransTableStatusType status_;
    int64_t commit_trans_version_;
    TO_STRING_KV(K_(status), K_(commit_trans_version));
  };
  const static int64_t ROLLBACK_SQL_SEQUENCE = 38;
  const static int64_t ROLLBACK_SQL_SEQUENCE_2 = 48;
  const static int64_t ROLLBACK_SQL_SEQUENCE_3 = 58;
public:
  TestUncommittedMinorMergeScan()
      : peek_flag_(false),
        input_idx_(0),
        default_trans_id_(999)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(trans_status_map_.create(10, ObModIds::OB_HASH_BUCKET_DAG_MAP))) {
      STORAGE_LOG(WARN, "failed to create hash map", K(ret));
    } else {
      trans_status_map_.clear();
      state_ = State::F_WORKING;
    }
  }

  ~TestUncommittedMinorMergeScan()
  {
    trans_status_map_.destroy();
  }

  int get_transaction_status_with_log_ts(
      const transaction::ObTransID &data_trans_id,
      const int64_t log_ts,
      transaction::ObTransTableStatusType &status,
      int64_t &trans_version,
      uint64_t &cluster_version) override
  {
    UNUSEDx(log_ts);
    int ret = OB_SUCCESS;
    const transaction::ObTransID *trans_id_ptr = &data_trans_id;
    cluster_version = CLUSTER_VERSION_3200;
    if (peek_flag_) {
      trans_id_ptr = &default_trans_id_;
    }
    TestTransStatus *trans_status = nullptr;
    if (trans_status_map_.get_refactored(*trans_id_ptr, trans_status)){
      STORAGE_LOG(WARN, "status is not exists", K(ret), K(data_trans_id),
          K(trans_status_map_.size()), K(peek_flag_), K(this));
    } else {
      status = trans_status->status_;
      if (transaction::ObTransTableStatusType::COMMIT == status
          || transaction::ObTransTableStatusType::RUNNING == status) {
        trans_version = trans_status->commit_trans_version_;
      } else {
        trans_version = INT64_MAX;
      }
      STORAGE_LOG(INFO, "get trans status", K(data_trans_id), KPC(trans_status), K(cluster_version), K(this));
    }
    return ret;
  }

  // add status for one transaction (transaction rows connot cross micro block)
  int add_transaction_status(
      transaction::ObTransTableStatusType status,
      int64_t trans_version = INT64_MAX)
  {
    int ret = OB_SUCCESS;
    peek_flag_ = false;
    TestTransStatus *trans_status = OB_NEW(TestTransStatus, ObModIds::TEST);
    trans_status->status_ = status;
    trans_status->commit_trans_version_ = trans_version;
    if (OB_FAIL(trans_status_map_.set_refactored(
        ObMockIteratorBuilder::trans_id_list_[input_idx_], trans_status))) {
      STORAGE_LOG(WARN, "push error", K(ret), K(status), K(trans_version));
    } else {
      STORAGE_LOG(INFO, "push success", K(ObMockIteratorBuilder::trans_id_list_[input_idx_]),
          K(status), K(trans_version), K(this));
      ++input_idx_;
    }
    return ret;
  }

  // set status for all transactions
  int set_all_transaction_status(
      transaction::ObTransTableStatusType status,
      int64_t trans_version = INT64_MAX)
  {
    int ret = OB_SUCCESS;
    peek_flag_ = true;
    TestTransStatus *trans_status = OB_NEW(TestTransStatus, ObModIds::TEST);
    trans_status->status_ = status;
    trans_status->commit_trans_version_ = trans_version;
    if (OB_FAIL(trans_status_map_.set_refactored(
        default_trans_id_, trans_status, 1/*over write*/))) {
      STORAGE_LOG(WARN, "push error", K(status), K(trans_version));
    } else {
      STORAGE_LOG(INFO, "push success", K(default_trans_id_), K(status), K(trans_version), K(this));
    }
    return ret;
  }

  int check_sql_sequence_can_read(
      const transaction::ObTransID &data_trans_id,
      const transaction::ObTxSEQ &sql_sequence,
      const share::SCN scn,
      bool &can_read)
  {
    int ret = OB_SUCCESS;
    UNUSED(data_trans_id);
    UNUSED(scn);
    if (ROLLBACK_SQL_SEQUENCE != sql_sequence
        && ROLLBACK_SQL_SEQUENCE_2 != sql_sequence
        && ROLLBACK_SQL_SEQUENCE_3 != sql_sequence) {
      can_read = true;
    } else {
      can_read = false;
    }
    return ret;
  }

  virtual int lock_for_read(
      const transaction::ObLockForReadArg &lock_for_read_arg,
      bool &can_read,
      int64_t &trans_version,
      bool &is_determined_state) override
  {
    int ret = OB_SUCCESS;
    transaction::ObTransTableStatusType status;
    uint64_t cluster_version = 0;
    transaction::ObTransID data_trans_id = lock_for_read_arg.data_trans_id_;
    int32_t data_sql_sequence = lock_for_read_arg.data_sql_sequence_;
    if (OB_FAIL(get_transaction_status_with_log_ts(data_trans_id, 0, status, trans_version, cluster_version))) {
      STORAGE_LOG(WARN, "failed to clear trans status map", K(ret));
    } else if (ROLLBACK_SQL_SEQUENCE != data_sql_sequence
        && ROLLBACK_SQL_SEQUENCE_2 != data_sql_sequence
        && ROLLBACK_SQL_SEQUENCE_3 != data_sql_sequence) {
      can_read = true;
    } else {
      can_read = false;
    }
    is_determined_state = status != transaction::ObTransTableStatusType::RUNNING;
    return ret;
  }


  void clear_all()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(trans_status_map_.clear())) {
      STORAGE_LOG(WARN, "failed to clear trans status map", K(ret));
    }
    input_idx_ = 0;
    peek_flag_ = false;
  }
private:
  static const int TRANS_ID_NUM = 10;
  typedef common::hash::ObHashMap<transaction::ObTransID, TestTransStatus*> TransStatusMap;
  bool peek_flag_;
  int64_t input_idx_;
  transaction::ObTransID trans_id_list_[TRANS_ID_NUM];
  TransStatusMap trans_status_map_;
  transaction::ObTransID default_trans_id_;
};


}
}
#endif
