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

#pragma once

#include "lib/oblog/ob_log_print_kv.h"
#include "common/ob_tablet_id.h"
#include "storage/ddl/ob_direct_insert_define.h"
#include "storage/tx/ob_trans_id.h"
#include "storage/tx/ob_tx_seq.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace storage
{
class ObStorageSchema;

class ObDDLIncLogBasic final
{
  OB_UNIS_VERSION_V(1);
public:
  ObDDLIncLogBasic();
  ~ObDDLIncLogBasic() = default;
  int init(const ObTabletID &tablet_id,
           const ObTabletID &lob_meta_tablet_id,
           const ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL,
           const transaction::ObTransID &trans_id = transaction::ObTransID(),
           const transaction::ObTxSEQ &seq_no = transaction::ObTxSEQ(),
           const int64_t snapshot_version = 0,
           const uint64_t data_format_version = 0);
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  void reset()
  {
    tablet_id_.reset();
    lob_meta_tablet_id_.reset();
    direct_load_type_ = ObDirectLoadType::DIRECT_LOAD_INVALID;
    trans_id_.reset();
    seq_no_.reset();
    snapshot_version_ = 0;
    data_format_version_ = 0;
  }
  bool operator ==(const ObDDLIncLogBasic &other) const
  {
    return tablet_id_ == other.get_tablet_id()
              && lob_meta_tablet_id_ == other.get_lob_meta_tablet_id()
              && direct_load_type_ == other.get_direct_load_type()
              && trans_id_ == other.get_trans_id()
              && seq_no_ == other.get_seq_no();
  }
  bool is_valid() const
  {
    return tablet_id_.is_valid();
  }
  const ObTabletID &get_tablet_id() const { return tablet_id_; }
  const ObTabletID &get_lob_meta_tablet_id() const { return lob_meta_tablet_id_; }
  const ObDirectLoadType &get_direct_load_type() const { return direct_load_type_; }
  const transaction::ObTransID &get_trans_id() const { return trans_id_; }
  const transaction::ObTxSEQ &get_seq_no() const { return seq_no_; }
  TO_STRING_KV(K_(tablet_id), K_(lob_meta_tablet_id), K_(direct_load_type),
      K_(trans_id), K_(seq_no), K_(snapshot_version), K_(data_format_version));
private:
  ObTabletID tablet_id_;
  ObTabletID lob_meta_tablet_id_;
  ObDirectLoadType direct_load_type_;
  transaction::ObTransID trans_id_;
  transaction::ObTxSEQ seq_no_;
  int64_t snapshot_version_;
  uint64_t data_format_version_;
};

class ObDDLIncStartLog final
{
  OB_UNIS_VERSION_V(1);
public:
  ObDDLIncStartLog();
  ~ObDDLIncStartLog();
  int init(const ObDDLIncLogBasic &log_basic);
  bool is_valid() const { return log_basic_.is_valid(); }
  const ObDDLIncLogBasic &get_log_basic() const { return log_basic_; }
  TO_STRING_KV(K_(log_basic), K_(has_cs_replica), KP_(storage_schema));
private:
  ObDDLIncLogBasic log_basic_;
  bool has_cs_replica_;
  ObStorageSchema *storage_schema_;
  ObArenaAllocator allocator_;
};

class ObDDLIncCommitLog final
{
  OB_UNIS_VERSION_V(1);
public:
  ObDDLIncCommitLog();
  ~ObDDLIncCommitLog() = default;
  int init(const ObDDLIncLogBasic &log_basic);
  bool is_valid() const { return log_basic_.is_valid(); }
  const ObDDLIncLogBasic &get_log_basic() const { return log_basic_; }
  TO_STRING_KV(K_(log_basic));
private:
  ObDDLIncLogBasic log_basic_;
};

} // namespace storage
} // namespace oceanbase
