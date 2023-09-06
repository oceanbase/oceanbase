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
#include "storage/access/ob_dml_param.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_table_param.h"
#include "common/object/ob_object.h"
#include "storage/lob/ob_lob_seq.h"

namespace oceanbase
{

namespace storage
{

struct ObLobAccessParam {
  ObLobAccessParam()
    : tx_desc_(nullptr), snapshot_(), tx_id_(), sql_mode_(SMO_DEFAULT), allocator_(nullptr),
      dml_base_param_(nullptr), column_ids_(),
      meta_table_schema_(nullptr), piece_table_schema_(nullptr),
      main_tablet_param_(nullptr), meta_tablet_param_(nullptr), piece_tablet_param_(nullptr),
      tenant_id_(MTL_ID()), src_tenant_id_(MTL_ID()),
      ls_id_(), tablet_id_(), coll_type_(), lob_locator_(nullptr),
      lob_common_(nullptr), lob_data_(nullptr), byte_size_(0), handle_size_(0), timeout_(0),
      fb_snapshot_(),
      scan_backward_(false), asscess_ptable_(false), offset_(0), len_(0),
      parent_seq_no_(), seq_no_st_(), used_seq_cnt_(0), total_seq_cnt_(0), checksum_(0), update_len_(0),
      op_type_(ObLobDataOutRowCtx::OpType::SQL), is_fill_zero_(false), from_rpc_(false),
      inrow_read_nocopy_(false)
  {}
  ~ObLobAccessParam() {
    if (OB_NOT_NULL(dml_base_param_)) {
      dml_base_param_->~ObDMLBaseParam();
    }
  }
public:
  int set_lob_locator(common::ObLobLocatorV2 *lob_locator);
  TO_STRING_KV(K_(tenant_id), K_(src_tenant_id), K_(ls_id), K_(tablet_id), KPC_(lob_locator), KPC_(lob_common),
    KPC_(lob_data), K_(byte_size), K_(handle_size), K_(coll_type), K_(scan_backward), K_(offset), K_(len),
    K_(parent_seq_no), K_(seq_no_st), K_(used_seq_cnt), K_(total_seq_cnt), K_(checksum), K_(update_len), K_(op_type),
    K_(is_fill_zero), K_(from_rpc), K_(snapshot), K_(tx_id), K_(inrow_read_nocopy));
public:
  transaction::ObTxDesc *tx_desc_; // for write/update/delete
  transaction::ObTxReadSnapshot snapshot_; // for read
  transaction::ObTransID tx_id_;           // used when read-latest
  ObSQLMode sql_mode_;
  bool is_total_quantity_log_;
  ObIAllocator *allocator_;
  ObDMLBaseParam* dml_base_param_;
  ObSEArray<uint64_t, 6> column_ids_;
  share::schema::ObTableSchema* meta_table_schema_; // for test
  share::schema::ObTableSchema* piece_table_schema_; // for test
  share::schema::ObTableParam *main_tablet_param_; // for test
  share::schema::ObTableParam *meta_tablet_param_; // for test
  share::schema::ObTableParam *piece_tablet_param_; // for test
  uint64_t tenant_id_;
  // some lob manager func will access other lob for data
  // other lob can read from other tenant
  uint64_t src_tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObCollationType coll_type_;
  common::ObLobLocatorV2 *lob_locator_; // should set by set_lob_locator
  common::ObLobCommon *lob_common_; // lob common
  common::ObLobData *lob_data_; // lob data
  int64_t byte_size_;
  int64_t handle_size_;
  int64_t timeout_;
  share::SCN fb_snapshot_;
  bool scan_backward_;
  bool asscess_ptable_;
  uint64_t offset_; // is_char == true, offset means char offset
  uint64_t len_; // is_char == true, len means char len
  // runtime
  transaction::ObTxSEQ parent_seq_no_; // the parent tablet write seq_no
  transaction::ObTxSEQ seq_no_st_; // start seq_no of lob tablet write
  uint32_t used_seq_cnt_;
  uint32_t total_seq_cnt_;
  int64_t checksum_;
  int64_t update_len_;
  ObLobDataOutRowCtx::OpType op_type_;
  // dbms lob
  bool is_fill_zero_; // fill zero when erase
  bool from_rpc_;
  bool inrow_read_nocopy_;
};

struct ObLobMetaInfo {
  ObLobMetaInfo()
    : lob_id_(), seq_id_(), char_len_(0), byte_len_(0), piece_id_(0), lob_data_()
  {}

  int deep_copy(ObIAllocator &allocator, ObLobMetaInfo& src)
  {
    int ret = OB_SUCCESS;
    lob_id_ = src.lob_id_;
    char_len_ = src.char_len_;
    byte_len_ = src.byte_len_;
    piece_id_ = src.piece_id_;
    char *buf = reinterpret_cast<char*>(allocator.alloc(src.seq_id_.length()));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buf, src.seq_id_.ptr(), src.seq_id_.length());
      seq_id_.assign_ptr(buf, src.seq_id_.length());

      // deep copy lob data
      buf = reinterpret_cast<char*>(allocator.alloc(src.lob_data_.length()));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(buf, src.lob_data_.ptr(), src.lob_data_.length());
        lob_data_.assign_ptr(buf, src.lob_data_.length());
      }
    }
    return ret;
  }

  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    oceanbase::common::databuff_print_kv(buf, buf_len, pos, K_(lob_id));
    ObString tmp_seq = seq_id_;
    size_t len = tmp_seq.length();
    const uint32_t* ori_dig = reinterpret_cast<const uint32_t*>(tmp_seq.ptr());
    const uint32_t ori_len = static_cast<uint32_t>(len / sizeof(uint32_t)); //TODO(yuanzhi.zy): check is len int32 enough
    uint32_t cur_pos = 0;
    common::databuff_printf(buf, buf_len, pos, ", seq_id:[");
    while (cur_pos < ori_len) {
      uint32_t val = ObLobSeqId::load32be(tmp_seq.ptr() + sizeof(uint32_t) * cur_pos);
      common::databuff_printf(buf, buf_len, pos, "%u.", val);
      cur_pos++;
    } // end while
    common::databuff_printf(buf, buf_len, pos, "], ");
    oceanbase::common::databuff_print_kv(buf, buf_len, pos, K_(char_len), K_(byte_len), K_(piece_id));
    common::databuff_printf(buf, buf_len, pos, ", ");
    ObString lob_data = lob_data_;
    if (lob_data.length() > 500) {
      lob_data.assign_ptr(lob_data.ptr(), 500);
    }
    oceanbase::common::databuff_print_kv(buf, buf_len, pos, K(lob_data));
    J_OBJ_END();
    return pos;
  }

  ObLobId lob_id_;
  ObString seq_id_;
  uint32_t char_len_;
  uint32_t byte_len_;
  // blocksstable::MacroBlockId macro_id_;
  uint64_t piece_id_;
  ObString lob_data_;
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
                               const int64_t timeout_ts,
                               const bool has_lob_header,
                               const uint64_t src_tenant_id);
  static int insert_lob_column(ObIAllocator &allocator,
                               const share::ObLSID ls_id,
                               const common::ObTabletID tablet_id,
                               const share::schema::ObColDesc &column,
                               ObObj &obj,
                               const int64_t timeout_ts);
};

struct ObLobDiffFlags
{
  ObLobDiffFlags() : can_do_append_(0), reserve_(0)
  {}
  TO_STRING_KV(K_(can_do_append), K_(reserve));
  uint64_t can_do_append_ : 1; // can do append in write situation
  uint64_t reserve_ : 63;
};

struct ObLobDiff
{
  enum DiffType
  {
    INVALID = 0,
    APPEND = 1,
    WRITE = 2,
    ERASE = 3,
    ERASE_FILL_ZERO = 4,
  };
  ObLobDiff()
    : type_(DiffType::INVALID), ori_offset_(0), ori_len_(0), offset_(0), byte_len_(0), dst_offset_(0), dst_len_(0),
      flags_()
  {}
  TO_STRING_KV(K_(type), K_(ori_offset), K_(ori_len), K_(offset), K_(byte_len), K_(dst_offset), K_(dst_len),
               K_(flags));
  DiffType type_;
  uint64_t ori_offset_;
  uint64_t ori_len_; // for diff, char_len
  uint64_t offset_;
  uint64_t byte_len_; // byte len
  uint64_t dst_offset_;
  uint64_t dst_len_; // for diff, char_len
  ObLobDiffFlags flags_;
};

struct ObLobDiffHeader
{
  ObLobDiffHeader()
    : diff_cnt_(0), persist_loc_size_(0)
  {}
  ObLobCommon* get_persist_lob()
  {
    return reinterpret_cast<ObLobCommon*>(data_);
  }
  char* get_inline_data_ptr()
  {
    return data_ + persist_loc_size_ + sizeof(ObLobDiff) * diff_cnt_;
  }
  ObLobDiff *get_diff_ptr()
  {
    return reinterpret_cast<ObLobDiff*>(data_ + persist_loc_size_);
  }
  TO_STRING_KV(K_(diff_cnt), K_(persist_loc_size));
  uint32_t diff_cnt_;
  uint32_t persist_loc_size_;
  char data_[0];
};

} // storage
} // oceanbase

#endif


