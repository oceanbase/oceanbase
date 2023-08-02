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

class ObLobMetaUtil {
public:
  static const uint64_t LOB_META_COLUMN_CNT = 6;
  static const uint64_t LOB_ID_COL_ID = 0;
  static const uint64_t SEQ_ID_COL_ID = 1;
  static const uint64_t BYTE_LEN_COL_ID = 2;
  static const uint64_t CHAR_LEN_COL_ID = 3;
  static const uint64_t PIECE_ID_COL_ID = 4;
  static const uint64_t LOB_DATA_COL_ID = 5;
  static const uint64_t LOB_META_INLINE_PIECE_ID = UINT64_MAX - 1;
  static const uint64_t LOB_OPER_PIECE_DATA_SIZE = 256 * 1024; // 256K
public:
  static int transform(common::ObNewRow* row, ObLobMetaInfo &info);
private:
  static int transform_lob_id(common::ObNewRow* row, ObLobMetaInfo &info);
  static int transform_seq_id(common::ObNewRow* row, ObLobMetaInfo &info);
  static int transform_byte_len(common::ObNewRow* row, ObLobMetaInfo &info);
  static int transform_char_len(common::ObNewRow* row, ObLobMetaInfo &info);
  static int transform_piece_id(common::ObNewRow* row, ObLobMetaInfo &info);
  static int transform_lob_data(common::ObNewRow* row, ObLobMetaInfo &info);
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
  TO_STRING_KV(K_(cur_pos), K_(cur_byte_pos), K_(cur_info));
private:
  bool is_in_range(const ObLobMetaInfo& info);
private:
  ObILobApator* lob_adatper_;
  common::ObNewRowIterator *meta_iter_; // lob meta tablet scan iter
  ObLobAccessParam param_;
  ObTableScanParam scan_param_;
  uint64_t cur_pos_;
  uint64_t cur_byte_pos_;
  ObLobMetaInfo cur_info_;
};

struct ObLobMetaWriteResult {
  ObLobMetaWriteResult() : info_(), data_(), need_alloc_macro_id_(false), is_update_(false), old_info_() {}
  ObLobMetaInfo info_;
  ObString data_;
  bool need_alloc_macro_id_;
  bool is_update_;
  ObLobMetaInfo old_info_;
};

class ObLobMetaWriteIter {
public:
  ObLobMetaWriteIter(const ObString& data, ObIAllocator* allocator, uint32_t piece_block_size);
  int open(ObLobAccessParam &param,
           uint64_t padding_size,
           ObString &post_data,
           ObString &remain_buf,
           ObString &seq_id_st,
           ObString &seq_id_end);
  int open(ObLobAccessParam &param, ObILobApator* adapter);
  int open(ObLobAccessParam &param,
           void *iter, // ObLobQueryIter
           ObString &read_buf,
           uint64_t padding_size,
           ObString &post_data,
           ObString &remain_buf,
           ObString &seq_id_st,
           ObString &seq_id_end);
  int get_next_row(ObLobMetaWriteResult &row);
  int close();
  TO_STRING_KV(K_(seq_id), K_(offset), K_(lob_id), K_(piece_id), K_(coll_type), K_(piece_block_size),
               K_(scan_iter), K_(padding_size), K_(seq_id_end), K_(last_info));
private:
  int try_fill_data(
      ObLobMetaWriteResult& row,
      ObString &data,
      uint64_t base,
      bool is_padding,
      bool &use_inner_buffer,
      bool &fill_full);
  int try_fill_data(
      ObLobMetaWriteResult& row,
      bool &use_inner_buffer,
      bool &fill_full);
  int try_update_last_info(ObLobMetaWriteResult &row);
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
};
 
class ObLobMetaManager {
public:
  ObLobMetaManager() {}
  ~ObLobMetaManager() {}
  // write one lob meta row
  int write(ObLobAccessParam& param, ObLobMetaInfo& in_row);
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

} // storage
} // oceanbase

#endif


