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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_WRITE_CACHE_PAGE_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_WRITE_CACHE_PAGE_H_

#include "lib/list/ob_dlink_node.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/tmp_file/ob_tmp_file_global.h"

namespace oceanbase
{
namespace tmp_file
{

struct ObTmpFilePageId
{
public:
  ObTmpFilePageId()
    : page_index_in_block_(-1),
      block_index_(ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX) {}
  ObTmpFilePageId(const int64_t page_idx_in_blk, const int64_t block_index)
    : page_index_in_block_(page_idx_in_blk), block_index_(block_index) {}
  void reset();
  bool operator==(const ObTmpFilePageId &other) const;
  TO_STRING_KV(K(page_index_in_block_), K(block_index_));
public:
  int32_t page_index_in_block_;
  int64_t block_index_;
};

enum class PageType : int64_t
{
  INVALID = -1,
  DATA = 0,
  META = 1
};

struct ObTmpFileWriteCacheKey
{
public:
  ObTmpFileWriteCacheKey()
    : type_(PageType::INVALID),
      fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
      virtual_page_id_(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID) {}
  ObTmpFileWriteCacheKey(const PageType type, const int64_t fd, const int64_t vid)
    : type_(type), fd_(fd), virtual_page_id_(vid) {}
  ObTmpFileWriteCacheKey(const int64_t fd, const int64_t tree_level, const int64_t level_page_index) :
               type_(PageType::META),
               fd_(fd),
               tree_level_(tree_level),
               level_page_index_ (level_page_index) {}
  bool is_valid() const;
  void reset();
  ObTmpFileWriteCacheKey &operator=(const ObTmpFileWriteCacheKey &other);
  bool operator==(const ObTmpFileWriteCacheKey &other) const;
  int hash(uint64_t &hash_val) const;
  TO_STRING_KV(K(type_), K(fd_), K(virtual_page_id_), K(tree_level_), K(level_page_index_));
public:
  PageType type_;
  int64_t fd_;
  union {
    int64_t virtual_page_id_; // page_offset / page_size
    struct {
      int64_t tree_level_:16;
      int64_t level_page_index_:48;
    };
  };
};

class ObTmpFilePage
{
public:
  struct PageListNode : public common::ObDLinkBase<PageListNode>
  {
    PageListNode(ObTmpFilePage &page) : page_(page) {}
    ObTmpFilePage &page_;
  };
public:
  ObTmpFilePage();
  ObTmpFilePage(char *buf, const uint32_t arr_idx);
  ~ObTmpFilePage() { reset(); }
  void reset();
  int assign(const ObTmpFilePage &other);
  bool is_valid();
  bool is_full() const;
  bool is_loading() const;
  void set_loading(bool is_loading);
  void set_is_full(bool is_full);
  void set_array_index(uint32_t idx);
  void set_page_key(const ObTmpFileWriteCacheKey &key);
  void set_page_id(const ObTmpFilePageId &page_id);
  void set_ref(uint32_t ref);
  uint32_t inc_ref();
  uint32_t dec_ref();
  uint32_t get_ref() const;
  uint32_t get_array_index() const;
  char *get_buffer() const;
  ObTmpFilePageId get_page_id() const;
  ObTmpFileWriteCacheKey get_page_key() const;
  PageListNode &get_list_node();
  void replace_node(ObTmpFilePage &other);

  TO_STRING_KV(K(is_full()), K(is_loading()), K(get_ref()), K(get_array_index()),
               K(page_id_), K(page_key_), KP(buf_), KP(list_node_.get_prev()), KP(list_node_.get_next()));
private:
  static const int32_t PG_FULL_BIT = 1;
  static const int32_t PG_LOADING_BIT = 1;
  static const int32_t PG_TYPE_RESERVE_BIT = 14;
  static const int32_t PG_REF_BIT = 16;
  static const int32_t PG_ARRAY_INDEX_BIT = 32;
  static const uint64_t PG_FULL_MASK     = 0x0000000000000001ULL; // bit 0
  static const uint64_t PG_LOADING_MASK  = 0x0000000000000002ULL; // bit 1
  static const uint64_t PG_TYPE_MASK     = 0x000000000000FFFCULL; // bit [2, 16)
  static const uint64_t PG_REF_MASK      = 0x00000000FFFF0000ULL; // bit [16, 32)
  static const uint64_t PG_ARRAY_MASK    = 0xFFFFFFFF00000000ULL; // bit [32, 64)
private:
  // | 32 bits (array idx) | 16 bits (ref) | 14 bits (reserve for type and new flags) | 2 bits (flags) |
  union {
    uint64_t flag_;
    struct{
      bool is_full_ : PG_FULL_BIT;
      bool is_loading_ : PG_LOADING_BIT;
      uint16_t type_reserve_ : PG_TYPE_RESERVE_BIT;
      uint16_t ref_ : PG_REF_BIT;
      uint32_t array_index_ : PG_ARRAY_INDEX_BIT;
    };
  };
  char *buf_;
  PageListNode list_node_;
  ObTmpFilePageId page_id_;
  ObTmpFileWriteCacheKey page_key_;
};

class ObTmpFilePageHandle
{
public:
  ObTmpFilePageHandle();
  ObTmpFilePageHandle(ObTmpFilePage *page);
  ObTmpFilePageHandle(const ObTmpFilePageHandle &other);
  ~ObTmpFilePageHandle();
  int init(ObTmpFilePage *page);
  int reset();
  bool is_valid() const { return OB_NOT_NULL(page_); }
  int assign(const ObTmpFilePageHandle &other);
  ObTmpFilePageHandle& operator=(const ObTmpFilePageHandle &other);
  ObTmpFilePage *get_page() const { return page_; }
  TO_STRING_KV(KPC(page_));
private:
  ObTmpFilePage *page_;
};

typedef ObTmpFilePage::PageListNode PageNode;

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif
