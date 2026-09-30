/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_CONCURRENCY_CONTROL_OB_TRANS_STAT_ROW
#define OCEANBASE_STORAGE_CONCURRENCY_CONTROL_OB_TRANS_STAT_ROW

#include "share/scn.h"
#include "share/datum/ob_datum.h"
#include "storage/access/ob_table_access_param.h"

namespace oceanbase
{
namespace concurrency_control
{

class ObTransStatRow
{
public:
  ObTransStatRow()
    : trans_version_(share::SCN::max_scn()),
    scn_(share::SCN::max_scn()),
    trans_id_(),
    seq_no_(),
    snapshot_(share::SCN::max_scn()),
    mvcc_row_(nullptr),
    first_trans_node_(nullptr) {}

  void set(const share::SCN trans_version,
           const share::SCN scn,
           const transaction::ObTransID trans_id,
           const transaction::ObTxSEQ &seq_no)
  {
    trans_version_ = trans_version;
    scn_ = scn;
    trans_id_ = trans_id;
    seq_no_ = seq_no;
  }

  void reset()
  {
    trans_version_ = share::SCN::max_scn();
    scn_ = share::SCN::max_scn();
    trans_id_.reset();
    seq_no_.reset();
    snapshot_ = share::SCN::max_scn();
    mvcc_row_ = nullptr;
    first_trans_node_ = nullptr;
  }

  TO_STRING_KV(K_(trans_version), K_(scn), K_(trans_id), K_(seq_no),
               K_(snapshot), KP_(mvcc_row), KP_(first_trans_node));
  share::SCN trans_version_;
  share::SCN scn_;
  transaction::ObTransID trans_id_;
  transaction::ObTxSEQ seq_no_;
  share::SCN snapshot_;
  const void *mvcc_row_;
  // Initial visible read position, not necessarily the physical version-chain head.
  const void *first_trans_node_;
public:
  static const int64_t MAX_TRANS_STRING_SIZE = 120;
};

void build_trans_stat_datum(const storage::ObTableIterParam *param,
                            const blocksstable::ObDatumRow &row,
                            const ObTransStatRow &trans_stat_row);

void build_trans_stat_(const ObTransStatRow &trans_stat_row,
                       const int64_t trans_stat_len,
                       char *trans_stat_ptr);

} // namespace concurrency_control
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_CONCURRENCY_CONTROL_OB_TRANS_STAT_ROW
