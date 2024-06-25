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

#ifndef OCEANBASE_STORAGE_OB_EXT_INFO_CALLBACK_
#define OCEANBASE_STORAGE_OB_EXT_INFO_CALLBACK_

#include "storage/memtable/mvcc/ob_mvcc_trans_ctx.h"
#include "lib/json_type/ob_json_diff.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace storage
{
class ObLobAccessParam;
class ObLobQueryIter;

enum ObExtInfoLogType
{
  OB_INVALID_EXT_INFO_LOG = 0,
  OB_JSON_DIFF_EXT_INFO_LOG = 1,
};


struct ObExtInfoLogHeader
{
  static int64_t get_header_size() { return sizeof(ObExtInfoLogHeader); }

  ObExtInfoLogHeader():
    type_(0)
  {}

  ObExtInfoLogType get_type() const { return static_cast<ObExtInfoLogType>(type_); }
  bool is_json_diff() const { return get_type() == OB_JSON_DIFF_EXT_INFO_LOG; }

  uint8_t type_;

  NEED_SERIALIZE_AND_DESERIALIZE;

public:
  TO_STRING_KV(K(type_));

};

class ObJsonDiffLog
{
public:
  ObJsonDiffLog():
    diff_header_(),
    diffs_()
  {}

  ~ObJsonDiffLog();
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  int to_string(ObIAllocator &allocator, ObString &result);
  TO_STRING_KV(K(diff_header_), K(diffs_));

private:
  ObJsonDiffHeader diff_header_;

  ObJsonDiffArray diffs_;

};

class ObExtInfoCallback : public memtable::ObITransCallback
{
public:
  static const int32_t OB_EXT_INFO_MUTATOR_ROW_COUNT = 2;
  static const int32_t OB_EXT_INFO_MUTATOR_ROW_KEY_IDX = 0;
  static const int32_t OB_EXT_INFO_MUTATOR_ROW_KEY_CNT = 1;
  static const int32_t OB_EXT_INFO_MUTATOR_ROW_VALUE_IDX = 1;

public:
  ObExtInfoCallback() :
      ObITransCallback(),
      allocator_(nullptr),
      seq_no_cur_(),
      dml_flag_(blocksstable::ObDmlFlag::DF_MAX),
      key_obj_(),
      key_(),
      rowkey_(),
      mutator_row_buf_(nullptr),
      mutator_row_len_(0)
  {}

  virtual ~ObExtInfoCallback();

  virtual memtable::MutatorType get_mutator_type() const override;
  virtual transaction::ObTxSEQ get_seq_no() const override { return seq_no_cur_; }
  virtual int64_t get_data_size() override { return mutator_row_len_; };

  virtual int log_submitted(const SCN scn, storage::ObIMemtable *&last_mt);
  virtual int release_resource();

  int get_redo(memtable::RedoDataNode &redo_node);
  int set(
      ObIAllocator &allocator,
      const blocksstable::ObDmlFlag dml_flag,
      const transaction::ObTxSEQ &seq_no_cur,
      ObString &data);

public:
  TO_STRING_KV(K(seq_no_cur_), K(dml_flag_), K(mutator_row_len_), KP(mutator_row_buf_));

private:
  ObIAllocator *allocator_;
  transaction::ObTxSEQ seq_no_cur_;
  blocksstable::ObDmlFlag dml_flag_;
  ObObj key_obj_;
  memtable::ObMemtableKey key_;
  common::ObStoreRowkey rowkey_;
  char *mutator_row_buf_;
  int32_t mutator_row_len_;
};

class ObExtInfoCbRegister {
public:
  static const int32_t OB_EXT_INFO_LOG_HEADER_LEN = 1;
  static const int64_t OB_EXT_INFO_LOG_BLOCK_MAX_SIZE;

public:
  ObExtInfoCbRegister():
      tmp_allocator_(lib::ObMemAttr(MTL_ID(), "ExtInfoLogReg")),
      mvcc_ctx_(nullptr),
      header_(),
      ext_info_data_(),
      lob_param_(nullptr),
      data_iter_(nullptr),
      data_buffer_(),
      timeout_(0),
      data_size_(0),
      seq_no_st_(),
      seq_no_cnt_(0),
      header_writed_(false)
  {}

  ~ObExtInfoCbRegister();

  int register_cb(
    memtable::ObIMvccCtx *ctx,
    const int64_t timeout,
    const blocksstable::ObDmlFlag dml_flag,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxSEQ &parent_seq_no,
    ObObj &index_data,
    ObObj &ext_info_data);

private:

  static ObExtInfoLogType get_type(ObObjType obj_type)
  {
    ObExtInfoLogType res = OB_INVALID_EXT_INFO_LOG;
    switch (obj_type)
    {
    case ObJsonType:
      res = OB_JSON_DIFF_EXT_INFO_LOG;
      break;
    default:
      break;
    }
    return res;
  }

  int build_data_iter(ObObj &ext_info_data);

  int set_index_data(ObObj &index_data);
  int set_outrow_ctx_seq_no(ObObj& index_data);

  int get_data(ObString &data);

  int init_header(ObObj& index_data, ObObj &ext_info_data);

public:
  TO_STRING_KV(K(timeout_), K(data_size_), K(seq_no_st_), K(seq_no_cnt_), K(header_writed_));

private:
  ObArenaAllocator tmp_allocator_;
  memtable::ObIMvccCtx *mvcc_ctx_;
  ObExtInfoLogHeader header_;
  ObObj ext_info_data_;
  ObLobAccessParam *lob_param_;
  ObLobQueryIter *data_iter_;
  ObString data_buffer_;
  int64_t timeout_;
  int64_t data_size_;
  transaction::ObTxSEQ seq_no_st_;
  uint64_t seq_no_cnt_;
  bool header_writed_;
};

}; // end namespace memtable
}; // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_EXT_INFO_CALLBACK_ */
