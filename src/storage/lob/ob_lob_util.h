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

#ifndef OCEABASE_STORAGE_OB_LOB_UTIL_
#define OCEABASE_STORAGE_OB_LOB_UTIL_
#include "lib/lock/ob_spin_lock.h"
#include "lib/task/ob_timer.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_table_param.h"
#include "common/object/ob_object.h"

namespace oceanbase
{

namespace storage
{

struct ObLobAccessParam {
  ObLobAccessParam()
    : tx_desc_(nullptr), snapshot_(), tx_id_(), sql_mode_(SMO_DEFAULT), is_total_quantity_log_(false),
      allocator_(nullptr), meta_table_schema_(nullptr), piece_table_schema_(nullptr),
      main_tablet_param_(nullptr), meta_tablet_param_(nullptr), piece_tablet_param_(nullptr),
      ls_id_(), tablet_id_(), coll_type_(common::ObCollationType::CS_TYPE_BINARY), lob_common_(nullptr),
      lob_data_(nullptr), byte_size_(0), handle_size_(0), timeout_(0),
      fb_snapshot_(transaction::ObTransVersion::INVALID_TRANS_VERSION),
      scan_backward_(false), asscess_ptable_(false), offset_(0), len_(0),
      seq_no_st_(-1), used_seq_cnt_(0), total_seq_cnt_(0), checksum_(0), update_len_(0)
  {}
  transaction::ObTxDesc *tx_desc_; // for write/update/delete
  transaction::ObTxReadSnapshot snapshot_; // for read
  transaction::ObTransID tx_id_;           // used when read-latest
  ObSQLMode sql_mode_;
  bool is_total_quantity_log_;
  ObIAllocator *allocator_;
  share::schema::ObTableSchema* meta_table_schema_; // for test
  share::schema::ObTableSchema* piece_table_schema_; // for test
  share::schema::ObTableParam *main_tablet_param_; // for test
  share::schema::ObTableParam *meta_tablet_param_; // for test
  share::schema::ObTableParam *piece_tablet_param_; // for test
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObCollationType coll_type_;
  common::ObLobCommon *lob_common_; // lob common
  common::ObLobData *lob_data_; // lob data
  int64_t byte_size_;
  int64_t handle_size_;
  int64_t timeout_;
  int64_t fb_snapshot_;
  bool scan_backward_;
  bool asscess_ptable_;
  uint64_t offset_; // is_char为true时 offset代表字符长度
  uint64_t len_; // is_char为true时 len代表字符长度
  // runtime
  int64_t seq_no_st_;
  uint32_t used_seq_cnt_;
  uint32_t total_seq_cnt_;
  int64_t checksum_;
  uint64_t update_len_;
  TO_STRING_KV(K_(ls_id), K_(tablet_id), KPC_(lob_common), KPC_(lob_data), K_(byte_size), K_(handle_size),
      K_(coll_type), K_(offset), K_(len), K_(seq_no_st), K_(used_seq_cnt), K_(total_seq_cnt), K_(checksum),
      K_(update_len));
};

struct ObLobMetaInfo {
  ObLobMetaInfo()
    : lob_id_(), seq_id_(), char_len_(0), byte_len_(0), piece_id_(0), lob_data_()
  {}
  ObLobId lob_id_;
  ObString seq_id_;
  uint32_t char_len_;
  uint32_t byte_len_;
  // blocksstable::MacroBlockId macro_id_;
  uint64_t piece_id_;
  ObString lob_data_;
  TO_STRING_KV(K_(lob_id), K_(seq_id), K_(char_len), K_(byte_len), K_(piece_id), K_(lob_data));
};

struct ObLobPieceInfo {
  ObLobPieceInfo()
    : piece_id_(0), len_(0), macro_id_()
  {}
  uint64_t piece_id_;
  uint32_t len_;
  blocksstable::MacroBlockId macro_id_;
  TO_STRING_KV(K_(piece_id), K_(len), K_(macro_id));
};

class ObInsertLobColumnHelper final
{
public:
  static const uint64_t LOB_ACCESS_TX_TIMEOUT = 60000000; // 60s
  static const uint64_t LOB_ALLOCATOR_RESET_CYCLE = 128;
public:
  static int start_trans(const share::ObLSID &ls_id,
                         const bool is_for_read,
                         const int64_t timeout_ts,
                         transaction::ObTxDesc *&tx_desc);
  static int end_trans(transaction::ObTxDesc *tx_desc,
                       const bool is_rollback,
                       const int64_t timeout_ts);

  static int insert_lob_column(ObIAllocator &allocator,
                               const share::ObLSID ls_id,
                               const common::ObTabletID tablet_id,
                               const share::schema::ObColDesc &column,
                               blocksstable::ObStorageDatum &datum,
                               const int64_t timeout_ts);


};


} // storage
} // oceanbase

#endif


