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

#ifndef OCEANBASE_STORAGE_OB_LOB_ACCESS_PARAM_H_
#define OCEANBASE_STORAGE_OB_LOB_ACCESS_PARAM_H_

#include "storage/tx/ob_trans_define_v4.h"

namespace oceanbase
{

namespace share {
class ObTabletCacheInterval;
}

namespace blocksstable {
class ObDatumRow;
}

namespace storage
{

class ObDMLBaseParam;
class ObLobAccessCtx;
class ObLobMetaInfo;

struct ObLobAccessParam {

public:
  static const int32_t DEFAULT_QUERY_CACHE_THRESHOLD = 256 * 1024;
public:

  ObLobAccessParam()
    : tmp_allocator_(nullptr), allocator_(nullptr),
      tx_desc_(nullptr), snapshot_(), tx_id_(),
      sql_mode_(SMO_DEFAULT), dml_base_param_(nullptr),
      tenant_id_(MTL_ID()), src_tenant_id_(MTL_ID()),
      ls_id_(), tablet_id_(), lob_meta_tablet_id_(), lob_piece_tablet_id_(),
      coll_type_(), lob_locator_(nullptr), lob_common_(nullptr),
      lob_data_(nullptr), byte_size_(0), handle_size_(0), timeout_(0),
      fb_snapshot_(), offset_(0), len_(0),
      parent_seq_no_(), seq_no_st_(), used_seq_cnt_(0), total_seq_cnt_(0), checksum_(0), update_len_(0),
      op_type_(ObLobDataOutRowCtx::OpType::SQL), is_total_quantity_log_(true),
      read_latest_(false), scan_backward_(false), is_fill_zero_(false), from_rpc_(false),
      inrow_read_nocopy_(false), is_store_char_len_(true), need_read_latest_(false), no_need_retry_(false), is_mlog_(false), try_flush_redo_(false),
      main_table_rowkey_col_(false), is_index_table_(false),
      inrow_threshold_(OB_DEFAULT_LOB_INROW_THRESHOLD), schema_chunk_size_(OB_DEFAULT_LOB_CHUNK_SIZE),
      access_ctx_(nullptr), addr_(), lob_id_geneator_(nullptr), data_row_(nullptr)
  {}
  ~ObLobAccessParam();

public:
  int assign(const ObLobAccessParam &other);

  bool is_full_read() const { return op_type_ == ObLobDataOutRowCtx::OpType::SQL && 0 == offset_ && (len_ == byte_size_ || INT64_MAX == len_ || UINT64_MAX == len_); }
  bool is_full_delete() const { return op_type_ == ObLobDataOutRowCtx::OpType::SQL && 0 == offset_ && len_ >= byte_size_; }
  bool is_full_insert() const { return op_type_ == ObLobDataOutRowCtx::OpType::SQL && 0 == offset_ && 0 == byte_size_; }

  bool enable_block_cache() const;

  /**
   * parameter completion
  */
  int prepare();
  int set_lob_locator(common::ObLobLocatorV2 *lob_locator);
  int check_handle_size() const;

  int is_timeout() const;
  bool is_char() const { return coll_type_ != common::ObCollationType::CS_TYPE_BINARY; }
  bool is_blob() { return coll_type_ == common::ObCollationType::CS_TYPE_BINARY; }

  bool is_remote() const;

  // chunk size can be changed online.
  // that means lob data that has been writed may have different chunk size with schema
  // so here need use different function to get chunk size
  int64_t get_schema_chunk_size() const;
  bool has_store_chunk_size() const;
  int get_store_chunk_size(int64_t &chunk_size) const;
  bool has_single_chunk() const;

  ObLobDataOutRowCtx* get_data_outrow_ctx() { return reinterpret_cast<ObLobDataOutRowCtx*>(lob_data_->buffer_); }
  int64_t get_inrow_threshold() const;

  /**
   * return true if char_len filed is exist
   */
  bool lob_handle_has_char_len_field() const;

  /**
   * return true if char_len filed is exist and is valid
  */
  bool lob_handle_has_char_len() const;
  int64_t* get_char_len_ptr() const;

  // used to update byte_size and char_len in disk lob locator when update lob meta row
  int update_handle_data_size(const ObLobMetaInfo *old_info, const ObLobMetaInfo *new_info);

  int set_tx_read_snapshot(ObLobLocatorV2 &locator);
  int get_tx_read_snapshot(ObLobLocatorV2 &locator, transaction::ObTxReadSnapshot &read_snapshot);

  // for outrow ctx
  int init_out_row_ctx(uint64_t len);
  int update_out_row_ctx(const ObLobMetaInfo *old_info, const ObLobMetaInfo& new_info);
  int init_seq_no(const uint64_t modified_len);

  void set_tmp_allocator(ObIAllocator *tmp_allocator) { tmp_allocator_ = tmp_allocator; }
  ObIAllocator* get_tmp_allocator() { return  nullptr != tmp_allocator_ ? tmp_allocator_ : allocator_; }

  bool skip_flush_redo() const { return !try_flush_redo_; }

  TO_STRING_KV(KP(this), K_(tenant_id), K_(src_tenant_id), K_(ls_id), K_(tablet_id), K_(lob_meta_tablet_id), K_(lob_piece_tablet_id),
    KPC_(lob_locator), KPC_(lob_common), KPC_(lob_data), K_(byte_size), K_(handle_size), K_(timeout), KP_(allocator), KP_(tmp_allocator),
    K_(coll_type), K_(scan_backward), K_(offset), K_(len), K_(parent_seq_no), K_(seq_no_st), K_(used_seq_cnt), K_(total_seq_cnt), K_(checksum),
    K_(update_len), K_(op_type), K_(is_fill_zero), K_(from_rpc), K_(snapshot), K_(tx_id), K_(read_latest), K_(is_total_quantity_log),
    K_(inrow_read_nocopy), K_(schema_chunk_size), K_(inrow_threshold), K_(is_store_char_len), K_(need_read_latest), K(no_need_retry_), K_(is_mlog), K_(try_flush_redo),
    K_(main_table_rowkey_col), K_(is_index_table), KP_(access_ctx), K_(addr), KPC_(lob_id_geneator), KPC_(data_row));

private:
  ObIAllocator *tmp_allocator_;

public:
  ObIAllocator *allocator_;
  // transaction
  transaction::ObTxDesc *tx_desc_; // for insert/update/delete
  transaction::ObTxReadSnapshot snapshot_; // for read
  // TODO real need?
  transaction::ObTransID tx_id_; // used when read-latest
  ObSQLMode sql_mode_;
  ObDMLBaseParam* dml_base_param_;
  uint64_t tenant_id_;
  // some lob manager func will access other lob for data
  // other lob can read from other tenant
  uint64_t src_tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObTabletID lob_meta_tablet_id_;
  common::ObTabletID lob_piece_tablet_id_;
  common::ObCollationType coll_type_;
  // lob locator
  common::ObLobLocatorV2 *lob_locator_; // should set by set_lob_locator
  common::ObLobCommon *lob_common_; // lob common
  common::ObLobData *lob_data_; // lob data
  // real lob data byte size
  int64_t byte_size_;
  // disk lob locator total size
  // inrow :
  //    ObLobCommon + data
  // outrow :
  //    ObLobCommon + ObLobData + ObLobDataOutRowCtx + char_len
  int64_t handle_size_;
  int64_t timeout_;
  // TODO real need?
  share::SCN fb_snapshot_;
  // read/write offset and len
  uint64_t offset_; // is_char == true, offset means char offset
  uint64_t len_; // is_char == true, len means char len

  // for ObLobDataOutRowCtx
  transaction::ObTxSEQ parent_seq_no_; // the parent tablet write seq_no
  transaction::ObTxSEQ seq_no_st_; // start seq_no of lob tablet write
  uint32_t used_seq_cnt_;
  uint32_t total_seq_cnt_;
  // used calc checksum in ObLobDataOutRowCtx::check_sum_
  // currently not used actualy by obcdc
  int64_t checksum_;
  // record new lob data byte len when update outrow lob in dml
  // to calculate the number of seq_no that need to be pre-allocated
  int64_t update_len_;
  // write operation type
  //  1. if is normal dml , should be SQL
  //  2. if is json partial update, should be DIFF
  //  3. other is used for dbms_lob partial update
  //  4. and EMPTY_SQL should not be used
  ObLobDataOutRowCtx::OpType op_type_;
  // bool field

  // used for dml
  // true is full mode of binlog_row_image
  // default should be true, beacuse obcdc need old_row when delete
  bool is_total_quantity_log_;
  // means main table is read_latest, not lob aux table
  bool read_latest_;
  bool scan_backward_;
  bool is_fill_zero_; // fill zero when dbms lob erase
  bool from_rpc_;
  bool inrow_read_nocopy_;
  bool is_store_char_len_;
  bool need_read_latest_;
  // whether need retry when some error occur
  bool no_need_retry_;
  bool is_mlog_;
  // before 4.3.4 lob meta tablet is writed before main tablet,
  // to avoid rollback cost too much there is a flag in ObWriteFlag to control skip flush redo.
  // but after 4.3.4, lob meta tablet is writed after main table, so no need skip flush redo
  // so add this flag to control this behavior for upgrade compatibility.
  bool try_flush_redo_;
  bool main_table_rowkey_col_; // true: main table rowkey column
  bool is_index_table_;

  int64_t inrow_threshold_;
  int64_t schema_chunk_size_;
  ObObj ext_info_log_;
  ObLobAccessCtx *access_ctx_;
  ObAddr addr_;
  share::ObTabletCacheInterval *lob_id_geneator_;
  const blocksstable::ObDatumRow *data_row_; // for tablet split
};


struct ObLobCompareParams {

  ObLobCompareParams()
    : collation_left_(CS_TYPE_INVALID),
      collation_right_(CS_TYPE_INVALID),
      offset_left_(0),
      offset_right_(0),
      compare_len_(0),
      timeout_(0),
      tx_desc_(nullptr)
  {
  }

  TO_STRING_KV(K(collation_left_),
               K(collation_right_),
               K(offset_left_),
               K(offset_right_),
               K(compare_len_),
               K(timeout_),
               K(tx_desc_));

  ObCollationType collation_left_;
  ObCollationType collation_right_;
  uint64_t offset_left_;
  uint64_t offset_right_;

  // compare length
  uint64_t compare_len_;
  int64_t timeout_;
  transaction::ObTxDesc *tx_desc_;
};

struct ObLobStorageParam
{
  ObLobStorageParam():
    inrow_threshold_(OB_DEFAULT_LOB_INROW_THRESHOLD),
    is_rowkey_col_(false),
    is_index_table_(false)
  {}

  TO_STRING_KV(K_(inrow_threshold), K_(is_rowkey_col), K_(is_index_table));

  int64_t inrow_threshold_;
  bool is_rowkey_col_;
  bool is_index_table_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_ACCESS_PARAM_H_
