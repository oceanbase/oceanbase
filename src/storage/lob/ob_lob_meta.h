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

#ifndef OCEABASE_STORAGE_OB_LOB_META_
#define OCEABASE_STORAGE_OB_LOB_META_
#include "lib/lock/ob_spin_lock.h"
#include "lib/task/ob_timer.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/access/ob_dml_param.h"
#include "ob_lob_util.h"
#include "ob_lob_seq.h"
#include "ob_lob_persistent_adaptor.h"

namespace oceanbase
{
namespace storage
{

class ObLobMetaSingleGetter;

class ObLobMetaUtil {
public:
  static const uint64_t LOB_META_COLUMN_CNT = 6;
  static const uint64_t LOB_META_SCHEMA_ROWKEY_COL_CNT = 2;
  static const uint64_t LOB_ID_COL_ID = 0;
  static const uint64_t SEQ_ID_COL_ID = 1;
  static const uint64_t BYTE_LEN_COL_ID = 2;
  static const uint64_t CHAR_LEN_COL_ID = 3;
  static const uint64_t PIECE_ID_COL_ID = 4;
  static const uint64_t LOB_DATA_COL_ID = 5;
  static const uint64_t LOB_META_INLINE_PIECE_ID = UINT64_MAX - 1;
  static const uint64_t LOB_OPER_PIECE_DATA_SIZE = 256 * 1024; // 256K
  static const uint64_t SKIP_INVALID_COLUMN = 2;
public:
  static int transform_from_info_to_row(ObLobMetaInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey);
  static int transform_from_row_to_info(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info, bool with_extra_rowkey);
  static int construct(
      ObLobAccessParam &param,
      const ObLobId &lob_id,
      const ObString &seq_id,
      const uint32_t &byte_len,
      const uint32_t &char_len,
      const ObString &lob_data,
      ObLobMetaInfo &info)
  {
    int ret = OB_SUCCESS;
    info.lob_id_ = lob_id;
    info.seq_id_ = seq_id;
    info.byte_len_ = byte_len;
    info.char_len_ = char_len;
    info.piece_id_ = ObLobMetaUtil::LOB_META_INLINE_PIECE_ID;
    info.lob_data_ = lob_data;
    return ret;
  }

private:
  // from_row_to_info.
  static int transform_lob_id(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info);
  static int transform_seq_id(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info);
  static int transform_byte_len(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info, bool with_extra_rowkey);
  static int transform_char_len(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info, bool with_extra_rowkey);
  static int transform_piece_id(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info, bool with_extra_rowkey);
  static int transform_lob_data(const blocksstable::ObDatumRow *row, ObLobMetaInfo &info, bool with_extra_rowkey);

  // from_info_to_row.
  static int transform_lob_id(ObLobMetaInfo &info, blocksstable::ObDatumRow *row);
  static int transform_seq_id(ObLobMetaInfo &info, blocksstable::ObDatumRow *row);
  static int transform_byte_len(ObLobMetaInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey);
  static int transform_char_len(ObLobMetaInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey);
  static int transform_piece_id(ObLobMetaInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey);
  static int transform_lob_data(ObLobMetaInfo &info, blocksstable::ObDatumRow *row, bool with_extra_rowkey);
};

struct ObLobMetaScanResult {
  ObLobMetaScanResult() : info_(), st_(0), len_(0) {}
  ObLobMetaInfo info_;
  uint32_t st_;
  uint32_t len_;
  TO_STRING_KV(K_(info), K_(st), K_(len));
};

class ObLobMetaScanIter {
public:
  ObLobMetaScanIter();
  ~ObLobMetaScanIter() { reset(); }
  int open(ObLobAccessParam &param, ObILobApator* lob_adapter);
  int get_next_row(ObLobMetaInfo &row);
  int get_next_row(ObLobMetaScanResult &result);
  uint64_t get_cur_pos() { return cur_pos_; }
  uint64_t get_cur_byte_pos() { return cur_byte_pos_; }
  ObLobMetaInfo get_cur_info() { return cur_info_; }
  void reset();
  bool is_range_begin(const ObLobMetaInfo& info);
  bool is_range_end(const ObLobMetaInfo& info);
  bool is_range_over(const ObLobMetaInfo& info);
  void set_not_calc_char_len(bool not_calc_char_len) { not_calc_char_len_ = not_calc_char_len; }
  bool not_calc_char_len() const { return not_calc_char_len_; }
  void set_not_need_last_info(bool not_need_last_info) { not_need_last_info_ = not_need_last_info;}
  bool not_need_last_info() const { return not_need_last_info_; }
  TO_STRING_KV(K_(cur_pos), K_(cur_byte_pos), K_(cur_info), K_(not_calc_char_len), K_(not_need_last_info));
private:
  bool is_in_range(const ObLobMetaInfo& info);
private:
  ObILobApator* lob_adatper_;
  common::ObNewRowIterator *meta_iter_; // lob meta tablet scan iter
  int64_t byte_size_; // param.byte_size
  uint64_t offset_; // param.offset
  uint64_t len_; // param.len
  ObCollationType coll_type_; // param.coll_type
  bool scan_backward_; // param.scan_backward
  ObIAllocator *allocator_;
  ObLobAccessCtx *access_ctx_;
  ObTableScanParam scan_param_;
  uint64_t cur_pos_;
  uint64_t cur_byte_pos_;
  ObLobMetaInfo cur_info_;
  bool not_calc_char_len_;
  bool not_need_last_info_;
};

class ObLobWriteBuffer;
class ObLobQueryIter;
class ObLobMetaManager;

struct ObLobMetaWriteResult {
  ObLobMetaWriteResult() : info_(), data_(), need_alloc_macro_id_(false), is_update_(false), old_info_(), seq_no_(0) {}
  ObLobMetaInfo info_;
  ObString data_;
  bool need_alloc_macro_id_;
  bool is_update_;
  ObLobMetaInfo old_info_;
  int64_t seq_no_;
  TO_STRING_KV(K_(is_update), K_(seq_no), K_(info), K_(old_info), K_(data));
};

class ObLobMetaWriteIter {
public:
  ObLobMetaWriteIter(ObIAllocator* allocator, uint32_t piece_block_size);
  ObLobMetaWriteIter(const ObString& data, ObIAllocator* allocator, uint32_t piece_block_size);
  ~ObLobMetaWriteIter() { close(); }
  int open(ObLobAccessParam &param,
           ObString &data,
           uint64_t padding_size,
           ObString &post_data,
           ObString &remain_buf,
           ObString &seq_id_st,
           ObString &seq_id_end,
           ObLobMetaManager* meta_manager = nullptr);
  int open(ObLobAccessParam &param,
           void *iter, // ObLobQueryIter
           ObString &read_buf,
           uint64_t padding_size,
           ObString &post_data,
           ObString &remain_buf,
           ObString &seq_id_st,
           ObString &seq_id_end,
           ObLobMetaManager* meta_manager = nullptr);
  int open(ObLobAccessParam &param,
           ObString &data,
           ObLobMetaManager* meta_manager = nullptr);
  int open(ObLobAccessParam &param,
           void *iter, // ObLobQueryIter
           ObString &read_buf,
           ObLobMetaManager* meta_manager = nullptr);
  int open(ObLobAccessParam &param,
           void *iter, // ObLobQueryIter
           ObLobAccessParam *read_param, // ObLobAccessParam
           ObString &read_buf);
  int get_next_row(ObLobMetaWriteResult &row);
  int close();
  void set_end() { is_end_ = true; }
  void reuse();

  TO_STRING_KV(K_(seq_id), K_(offset), K_(lob_id), K_(piece_id), K_(coll_type), K_(piece_block_size),
               K_(scan_iter), K_(padding_size), K_(seq_id_end), K_(last_info), K_(is_store_char_len));
private:
  int try_fill_data(
      ObLobWriteBuffer& write_buffer,
      ObString &data,
      uint64_t base,
      bool is_padding);
  int try_fill_data(
      ObLobWriteBuffer& write_buffer,
      ObLobQueryIter *iter);
  int get_last_meta_info(ObLobAccessParam &param, ObLobMetaManager* meta_manager);
  int try_update_last_info(ObLobMetaWriteResult &row, ObLobWriteBuffer& write_buffer);

  int update_disk_lob_locator(ObLobMetaWriteResult &new_info);

private:
  ObLobSeqId seq_id_;       // seq id
  uint64_t offset_;       // write or append offset in macro block
  ObLobId lob_id_;
  uint64_t piece_id_; // TODO: for test
  ObString data_;     // write data
  common::ObCollationType coll_type_;
  uint32_t piece_block_size_;
  ObLobMetaScanIter scan_iter_; // use scan iter directly
  uint64_t padding_size_;
  ObLobSeqId seq_id_end_;
  ObString post_data_;
  ObString remain_buf_;
  ObString inner_buffer_;
  ObIAllocator* allocator_;
  ObLobMetaInfo last_info_;
  void *iter_; // ObLobQueryIter
  uint64_t iter_fill_size_;
  ObLobAccessParam *read_param_;
  ObLobCommon* lob_common_;
  bool is_end_;
  bool is_store_char_len_;
};
 
class ObLobMetaManager {
public:
  explicit ObLobMetaManager(const uint64_t tenant_id) :
    persistent_lob_adapter_(tenant_id)
  {}
  ~ObLobMetaManager() {}
  // write one lob meta row
  int write(ObLobAccessParam& param, ObLobMetaInfo& in_row);
  int batch_insert(ObLobAccessParam& param, ObNewRowIterator &iter);
  int batch_delete(ObLobAccessParam& param, ObNewRowIterator &iter);
  // append
  int append(ObLobAccessParam& param, ObLobMetaWriteIter& iter);
  // return ObLobMetaWriteResult
  int insert(ObLobAccessParam& param, ObLobMetaWriteIter& iter);
  // specified range rebuild
  int rebuild(ObLobAccessParam& param);
  // specified range LobMeta scan
  int scan(ObLobAccessParam& param, ObLobMetaScanIter &iter);
  // specified range erase
  int erase(ObLobAccessParam& param, ObLobMetaInfo& in_row);
  // specified range update
  int update(ObLobAccessParam& param, ObLobMetaInfo& old_row, ObLobMetaInfo& new_row);
  // fetch lob id
  int fetch_lob_id(ObLobAccessParam& param, uint64_t &lob_id);

  int open(ObLobAccessParam &param, ObLobMetaSingleGetter* getter);

  TO_STRING_KV("[LOB]", "meta mngr");
private:
  // lob adaptor
  ObPersistentLobApator persistent_lob_adapter_;
};

OB_INLINE int64_t ob_lob_writer_length_validation(const common::ObCollationType &coll_type,
                                                  const int64_t &data_len,
                                                  const int64_t &write_len,
                                                  int64_t &write_char_len)
{
  int64_t len_ret = write_len;
  if (write_len > data_len) {
    // if (coll_type == CS_TYPE_BINARY) {
      OB_ASSERT(0); // ToDo: Debug only
    // }
  }
  if (write_char_len > len_ret) {
    if (coll_type == CS_TYPE_BINARY) {
      write_char_len = len_ret;
    } else {
      OB_ASSERT(0); // ToDo: Debug only
    }
  }
  return len_ret;
}


class ObLobMetaWriteRowIter: public ObNewRowIterator
{
public:
  ObLobMetaWriteRowIter() : param_(nullptr), meta_iter_(nullptr), new_row_(), row_cell_(), result_() {}
  ObLobMetaWriteRowIter(ObLobAccessParam *param, ObLobMetaWriteIter *meta_iter)
    : param_(param), meta_iter_(meta_iter), new_row_(), row_cell_(), result_()
  {}
  virtual ~ObLobMetaWriteRowIter() {}
  virtual int get_next_row(ObNewRow *&row);
	virtual void reset() { new_row_.reset(); }
private:
  int update_seq_no();

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLobMetaWriteRowIter);
private:
  // data members
  ObLobAccessParam *param_;
  ObLobMetaWriteIter *meta_iter_;
  ObNewRow new_row_;
  ObObj row_cell_[ObLobMetaUtil::LOB_META_COLUMN_CNT];
  ObLobMetaWriteResult result_;
};

class ObLobMetaSingleGetter
{
public:
  ObLobMetaSingleGetter():
    param_(nullptr),
    scan_param_(),
    row_objs_(nullptr),
    table_id_(0),
    lob_adatper_(nullptr),
    scan_iter_(nullptr)
  {}

  ~ObLobMetaSingleGetter();

  int open(ObLobAccessParam &param, ObILobApator* lob_adatper);

  int get_next_row(ObString &seq_id, ObLobMetaInfo &info);

private:
  ObLobAccessParam *param_;
  ObTableScanParam scan_param_;
  ObObj *row_objs_;
  uint64_t table_id_;
public:
  ObILobApator *lob_adatper_;
  ObTableScanIterator *scan_iter_;
};


} // storage
} // oceanbase

#endif


