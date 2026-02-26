/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_CG_BLOCK_TMP_FILE_H_
#define OCEANBASE_STORAGE_OB_CG_BLOCK_TMP_FILE_H_

#include "share/rc/ob_tenant_base.h"
#include "storage/tmp_file/ob_tmp_file_io_info.h"

namespace oceanbase
{
namespace storage
{

class ObCGBlock final
{
public:
  enum StoredVersion
  {
    UNKNOWN_VERSION = 0,
    DATA_VERSION_4_4_0_0_AFTER = 1
  };

  class StoreHeader final
  {
  public:
    StoreHeader() :
      store_version_(UNKNOWN_VERSION),
      is_complete_macro_block_(false),
      reserved_(0),
      macro_buffer_size_(0) { }
    ~StoreHeader() = default;
    void reset();
    bool is_valid() const { return UNKNOWN_VERSION != store_version_ && macro_buffer_size_ > 0; }
    NEED_SERIALIZE_AND_DESERIALIZE;
    TO_STRING_KV(K(store_version_), K(is_complete_macro_block_), K(macro_buffer_size_));

  public:
    union {
      int64_t header_;
      struct {
        int64_t store_version_ : 8;
        int64_t is_complete_macro_block_ : 1;
        int64_t reserved_ : 7;
        int64_t macro_buffer_size_ : 48;
      };
    };
  };

public:
  ObCGBlock() :
    is_inited_(false),
    store_header_(),
    macro_block_buffer_(nullptr),
    cg_block_offset_(0),
    micro_block_idx_(0),
    allocator_(ObMemAttr(MTL_ID(), "cg_block")) { }
  ~ObCGBlock() = default;
  // for write tmp file to initialize
  int init(const char *macro_block_buffer,
           const StoredVersion store_version,
           const bool is_complete_macro_block,
           const int64_t macro_buffer_size);
  // for read tmp file to initialize
  int init(const StoreHeader &store_header);
  void reset();
  OB_INLINE bool is_valid() const
  {
    return store_header_.is_valid() &&
           nullptr != macro_block_buffer_ &&
           cg_block_offset_ < store_header_.macro_buffer_size_ &&
           (is_all_micro_blocks_unconsumed() || has_consumed_micro_blocks());
  }
  OB_INLINE StoredVersion get_store_version() const
  {
    return static_cast<StoredVersion>(store_header_.store_version_);
  }

  OB_INLINE bool is_complete_macro_block() const { return store_header_.is_complete_macro_block_; }
  OB_INLINE bool is_all_micro_blocks_unconsumed() const { return 0 == cg_block_offset_  && 0 == micro_block_idx_; }
  OB_INLINE bool has_consumed_micro_blocks() const { return cg_block_offset_ != 0 && micro_block_idx_ != 0; }
  OB_INLINE int64_t get_macro_buffer_size() const { return store_header_.macro_buffer_size_; }
  OB_INLINE const char *get_macro_block_buffer() const { return macro_block_buffer_; }
  OB_INLINE int64_t get_cg_block_offset() const { return cg_block_offset_; }
  OB_INLINE void set_cg_block_offset(const int64_t cg_block_offset) { cg_block_offset_ = cg_block_offset; }
  OB_INLINE int64_t get_micro_block_idx() const { return micro_block_idx_; }
  OB_INLINE void set_micro_block_idx(const int64_t micro_block_idx) { micro_block_idx_ = micro_block_idx; }
  OB_INLINE const StoreHeader &get_store_header() const { return store_header_; }
  OB_INLINE int64_t get_total_size() const
  {
    return store_header_.get_serialize_size() + store_header_.macro_buffer_size_;
  }
  TO_STRING_KV(K(is_inited_), K(store_header_), KP(macro_block_buffer_), K(cg_block_offset_), K(micro_block_idx_));

private:
  DISALLOW_COPY_AND_ASSIGN(ObCGBlock);

private:
  bool is_inited_;
  StoreHeader store_header_;
  const char *macro_block_buffer_;
  int64_t cg_block_offset_;
  int64_t micro_block_idx_;
  ObArenaAllocator allocator_;
};

class ObCGBlockFile final
{
public:
  class BlockStore final
  {
  public:
    BlockStore() :
      is_inited_(false),
      fd_(-1),
      file_dir_(-1),
      file_size_(0),
      file_offset_(0),
      remain_data_size_(0),
      curr_cg_block_offset_(0),
      curr_cg_block_micro_idx_(0),
      store_header_() { }
    ~BlockStore() = default;
    int open();
    int close();
    int append_cg_block(const ObCGBlock &cg_block);
    int get_next_cg_block(ObCGBlock &cg_block);
    int put_cg_block_back(const ObCGBlock &cg_block);
    OB_INLINE int64_t get_data_size() const { return remain_data_size_; }
    TO_STRING_KV(K(is_inited_), K(fd_), K(file_dir_), K(file_size_),
                 K(file_offset_), K(remain_data_size_), K(curr_cg_block_offset_),
                 K(curr_cg_block_micro_idx_), K(store_header_));

  private:
    int write_cg_block_header(const ObCGBlock::StoreHeader &cg_block_store_header);
    // used if and only if it is the first iteration
    int read_cg_block_header(ObCGBlock::StoreHeader &cg_block_store_header);
    int take_cg_block_header(const ObCGBlock &cg_block);
    int get_io_info(const char *buf, const int64_t size, const int64_t timeout_ms, tmp_file::ObTmpFileIOInfo &io_info);
    DISALLOW_COPY_AND_ASSIGN(BlockStore);

  private:
    bool is_inited_;
    int64_t fd_;
    int64_t file_dir_;
    int64_t file_size_;
    int64_t file_offset_;
    int64_t remain_data_size_;
    int64_t curr_cg_block_offset_;
    int64_t curr_cg_block_micro_idx_;
    ObCGBlock::StoreHeader store_header_;
  };

public:
  ObCGBlockFile() :
    is_inited_(false),
    tablet_id_(ObTabletID::INVALID_TABLET_ID),
    slice_idx_(-1),
    scan_idx_(-1),
    cg_idx_(-1),
    block_store_() { }
  ~ObCGBlockFile()
  {
    if (is_inited_) {
      IGNORE_RETURN close();
    }
  }
  int open(const ObTabletID tablet_id,
           const int64_t slice_idx,
           const int64_t scan_idx,
           const int64_t cg_idx);
  int close();
  int append_cg_block(const ObCGBlock &cg_block);
  int get_next_cg_block(ObCGBlock &cg_block);
  int put_cg_block_back(const ObCGBlock &cg_block);
  OB_INLINE int64_t get_data_size() const { return block_store_.get_data_size(); }
  OB_INLINE ObTabletID get_tablet_id() const { return tablet_id_; }
  OB_INLINE int64_t get_slice_idx() const { return slice_idx_; }
  OB_INLINE int64_t get_scan_idx() const { return scan_idx_; }
  OB_INLINE int64_t get_cg_idx() const { return cg_idx_; }
  OB_INLINE bool is_opened() const { return is_inited_; }
  TO_STRING_KV(K(is_inited_), K(tablet_id_), K(slice_idx_),
               K(scan_idx_), K(cg_idx_), K(block_store_));

private:
  DISALLOW_COPY_AND_ASSIGN(ObCGBlockFile);

private:
  bool is_inited_;
  common::ObTabletID tablet_id_;
  int64_t slice_idx_;
  int64_t scan_idx_;
  int64_t cg_idx_;
  BlockStore block_store_;
};

// for macro writer to write cg block tmp file
class ObCGBlockFileWriter final
{
public:
  ObCGBlockFileWriter() :
    is_inited_(false),
    cg_block_file_(nullptr) { }
  ~ObCGBlockFileWriter() = default;
  int init(ObCGBlockFile *cg_block_file);
  void reset();
  int write(const char *buf, const bool is_complete_macro_block, const int64_t buf_size);
  TO_STRING_KV(K(is_inited_), KPC(cg_block_file_));

private:
  bool is_inited_;
  ObCGBlockFile *cg_block_file_;
};

} //end storage
} // end oceanbase

#endif //OCEANBASE_STORAGE_OB_CG_BLOCK_TMP_FILE_H_