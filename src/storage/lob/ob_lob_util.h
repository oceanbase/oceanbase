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
#include "storage/lob/ob_ext_info_callback.h"
#include "storage/lob/ob_lob_access_param.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{

namespace storage
{

class ObLobCharsetUtil
{
public:
  static ObCollationType get_collation_type(ObObjType type, ObCollationType ori_coll_type);
  static void transform_query_result_charset(
      const common::ObCollationType& coll_type,
      const char* data,
      uint32_t len,
      uint32_t &byte_len,
      uint32_t &byte_st);
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

  void reset()
  {
    lob_id_.reset();
    seq_id_.reset();
    char_len_ = 0;
    byte_len_ = 0;
    piece_id_ = 0;
    lob_data_.reset();
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

class ObLobMetaWriteIter;

class ObInsertLobColumnHelper final
{
public:
  static const uint64_t LOB_TX_TIMEOUT = 86400000000; // 1 day
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
                               const ObObjType &obj_type,
                               const ObCollationType &cs_type,
                               const ObLobStorageParam &lob_storage_param,
                               blocksstable::ObStorageDatum &datum,
                               const int64_t timeout_ts,
                               const bool has_lob_header,
                               const uint64_t src_tenant_id);
  static int delete_lob_column(ObIAllocator &allocator,
                               const share::ObLSID ls_id,
                               const common::ObTabletID tablet_id,
                               const ObCollationType& collation_type,
                               blocksstable::ObStorageDatum &datum,
                               transaction::ObTxDesc *tx_desc,
                               transaction::ObTxReadSnapshot &snapshot,
                               const int64_t timeout_ts,
                               const bool has_lob_header);
  static int insert_lob_column(ObIAllocator &allocator,
                               const share::ObLSID ls_id,
                               const common::ObTabletID tablet_id,
                               const ObObjType &obj_type,
                               const ObCollationType &cs_type,
                               const ObLobStorageParam &lob_storage_param,
                               ObObj &obj,
                               const int64_t timeout_ts);

  // lob_allocator is mainly used for outrow lob read and write memory allocation,
  // that can be released after lob inset to avoid hold too many memory
  // and res_allocator is mainly used to alloc lob result datum memory in main table
  // should call iter.close outter
  static int insert_lob_column(ObIAllocator &res_allocator,
                               ObIAllocator &lob_allocator,
                               transaction::ObTxDesc *tx_desc,
                               share::ObTabletCacheInterval &lob_id_geneator,
                               const share::ObLSID ls_id,
                               const common::ObTabletID tablet_id,
                               const common::ObTabletID lob_meta_tablet_id,
                               const ObObjType &obj_type,
                               const ObCollationType collation_type,
                               const ObLobStorageParam &lob_storage_param,
                               blocksstable::ObStorageDatum &datum,
                               const int64_t timeout_ts,
                               const bool has_lob_header,
                               const uint64_t src_tenant_id,
                               ObLobMetaWriteIter &iter);
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
    WRITE_DIFF = 5,
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

  bool is_mutli_diff() { return diff_cnt_ > 0; }
  TO_STRING_KV(K_(diff_cnt), K_(persist_loc_size));
  uint32_t diff_cnt_;
  uint32_t persist_loc_size_;
  char data_[0];
};


class ObLobChunkIndex
{
  OB_UNIS_VERSION(1);
public:
  ObLobChunkIndex()
    : seq_id_(), offset_(0), pos_(0), byte_len_(0), flag_(0), data_idx_(0), old_data_idx_(-1)
  {}

  ObLobChunkIndex(uint64_t offset_, const ObLobMetaInfo& meta_info)
    : seq_id_(meta_info.seq_id_), offset_(offset_), byte_len_(meta_info.byte_len_), flag_(0), data_idx_(0), old_data_idx_(-1)
  {}

  int init(const uint64_t offset, const ObLobMetaInfo& meta_info);

  TO_STRING_KV(K(offset_), K(is_add_), K(is_modified_), K(byte_len_), K(pos_), K(data_idx_), K(old_data_idx_), K(seq_id_));

public:
  ObString seq_id_;
  uint64_t offset_;
  uint64_t pos_;
  uint32_t byte_len_;
  union {
    struct {
      uint32_t is_add_ : 1;
      uint32_t is_modified_ : 1;
      uint32_t reserved_ : 30;
    };
    uint32_t flag_;
  };
  uint32_t data_idx_;
  int32_t old_data_idx_;
};

class ObLobChunkData
{
  OB_UNIS_VERSION(1);
public:
  ObLobChunkData()
    : data_()
  {}

  explicit ObLobChunkData(const ObString &data)
    : data_(data)
  {}

  TO_STRING_KV(K(data_));

public:
	ObString data_;
};

struct ObLobPartialData
{
  OB_UNIS_VERSION(1);
public:
  ObLobPartialData(): chunk_size_(0), data_length_(0) {}

  int init();
  int push_chunk_index(const ObLobChunkIndex &chunk_index);
  int get_ori_data_length(int64_t &len) const;
  int sort_index();
  bool is_full_mode();
  // include new add chunk
  int64_t get_modified_chunk_cnt() const;

public:
  TO_STRING_KV(K(chunk_size_), K(data_length_));
  int64_t chunk_size_;
  // newest data length, include append data
  int64_t data_length_;
  ObString locator_;
  hash::ObHashMap<int, int, hash::NoPthreadDefendMode> search_map_;
  // must order by offset
	ObSEArray<ObLobChunkIndex, 10> index_;
	ObSEArray<ObLobChunkData, 1> data_;
	ObSEArray<ObLobChunkData, 5> old_data_;
};

} // storage
} // oceanbase

#endif


