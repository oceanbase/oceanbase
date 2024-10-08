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

#ifndef OCEANBASE_STORAGE_OB_LOB_LOCATOR_STRUCT_H_
#define OCEANBASE_STORAGE_OB_LOB_LOCATOR_STRUCT_H_


namespace oceanbase
{
namespace storage
{


class ObLobDiskLocatorWrapper
{
public:
  static int get_char_len(const ObLobLocatorV2 &src_data_locator, uint64_t &char_len);

public:
  ObLobDiskLocatorWrapper():
    lob_common_(nullptr),
    lob_data_(nullptr),
    outrow_ctx_(nullptr),
    char_len_ptr_(nullptr)
  {}


  int init(char *ptr, const int64_t len);
  int reset_for_dml();

  bool is_valid() const;

  ObLobCommon* get_lob_common() { return lob_common_; }
  ObLobId get_lob_id() const { return lob_data_->id_; }
  uint64_t get_byte_size() const { return lob_data_->byte_size_; }
  ObLobDataOutRowCtx *get_outrow_ctx() { return outrow_ctx_; }
  uint64_t get_char_len() const { return *char_len_ptr_; }

  transaction::ObTxSEQ get_seq_no_st() const { return transaction::ObTxSEQ::cast_from_int(outrow_ctx_->seq_no_st_); }
  int64_t get_seq_no_cnt() const { return outrow_ctx_->seq_no_cnt_; }
  bool is_ext_info_log() const { return ObLobDataOutRowCtx::OpType::EXT_INFO_LOG == outrow_ctx_->op_; }

  int check_for_dml(ObLobDiskLocatorWrapper &other) const;

private:
  int check_disk_locator_length(const int64_t len) const;

public:
  TO_STRING_KV(
    KPC_(lob_common),
    KPC_(lob_data),
    KPC_(outrow_ctx),
    KP_(char_len_ptr),
    "char_len", (nullptr == char_len_ptr_ ? 0 : *char_len_ptr_)
  );

private:
  ObLobCommon* lob_common_;
  ObLobData *lob_data_;
  ObLobDataOutRowCtx *outrow_ctx_;
  uint64_t *char_len_ptr_;

};

class ObLobDiskLocatorBuilder
{
public:
  ObLobDiskLocatorBuilder():
    ptr_(nullptr),
    len_(0),
    lob_common_(nullptr),
    lob_data_(nullptr),
    outrow_ctx_(nullptr),
    char_len_ptr_(nullptr)
  {}

  int init(ObIAllocator &allocator);
  int set_lob_id(const ObLobId& lob_id);
  int set_byte_len(const uint64_t &byte_len);
  int set_char_len(const uint64_t &char_len);
  int set_seq_no(const ObLobDataOutRowCtx::OpType type, transaction::ObTxSEQ &seq_no_st, const uint32_t seq_no_cnt);
  int set_chunk_size(const int64_t lob_chunk_size);
  int set_ext_info_log_length(const int64_t len);
  int to_locator(ObLobLocatorV2 &locator) const;

public:
  TO_STRING_KV(
    KP_(ptr),
    K_(len),
    KPC_(lob_common),
    KPC_(lob_data),
    KPC_(outrow_ctx),
    KP_(char_len_ptr),
    "char_len", (nullptr == char_len_ptr_ ? 0 : *char_len_ptr_)
  );

private:
  // locator data
  char *ptr_;
  int64_t len_;
  // locator field
  ObLobCommon* lob_common_;
  ObLobData *lob_data_;
  ObLobDataOutRowCtx *outrow_ctx_;
  uint64_t *char_len_ptr_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_LOCATOR_STRUCT_H_