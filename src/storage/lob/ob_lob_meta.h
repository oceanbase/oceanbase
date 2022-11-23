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
  void reset();
  bool is_range_begin(const ObLobMetaInfo& info);
  bool is_range_end(const ObLobMetaInfo& info);
  bool is_range_over(const ObLobMetaInfo& info);
private:
  bool is_in_range(const ObLobMetaInfo& info);
private:
  ObILobApator* lob_adatper_;
  common::ObNewRowIterator *meta_iter_; // lob meta tablet scan iter
  ObLobAccessParam param_;
  ObTableScanParam scan_param_;
  uint64_t cur_pos_;
};

struct ObLobMetaWriteResult {
  ObLobMetaInfo info_;
  ObString data_;
  bool need_alloc_macro_id_;
};

class ObLobMetaWriteIter {
public:
  ObLobMetaWriteIter(const ObString& data, ObIAllocator* allocator, uint32_t piece_block_size);
  int open(ObLobAccessParam &param, ObILobApator* adapter);
  int get_next_row(ObLobMetaWriteResult &row);
  int close();
private:
  ObLobSeqId seq_id_;       // seq id
  uint64_t offset_;       // write or append offset in macro block
  ObLobId lob_id_;
  uint64_t piece_id_; // TODO: for test
  ObString data_;     // write data
  common::ObCollationType coll_type_;
  uint32_t piece_block_size_;
  ObLobMetaScanIter scan_iter_; // use scan iter directly
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
  int fetch_lob_id(const ObLobAccessParam& param, uint64_t &lob_id);
  TO_STRING_KV("[LOB]", "meta mngr");
private:
  // lob adaptor
  ObPersistentLobApator persistent_lob_adapter_;
};


} // storage
} // oceanbase

#endif


