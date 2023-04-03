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

#ifndef OCEANBASE_ENCODING_OB_MULTI_PREFIX_TREE_H_
#define OCEANBASE_ENCODING_OB_MULTI_PREFIX_TREE_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "ob_encoding_hash_util.h"
#include "ob_icolumn_encoder.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace blocksstable
{

static const int64_t MOVE_STEP = 16L;

class ObMultiPrefixTree
{
public:
  struct TreeNode;

  struct CellNode
  {
    CellNode() { reset(); }

    void reset() { MEMSET(this, 0, sizeof(*this)); }

    inline void add_after(CellNode *node)
    {
      add(this, node, next_);
    }

    inline void add(CellNode *prev, CellNode *node, CellNode *next)
    {
      prev->next_ = node;
      node->prev_ = prev;
      next->prev_ = node;
      node->next_ = next;
    }

    inline void unlink()
    {
      prev_->next_ = next_;
      next_->prev_ = prev_;
      prev_ = NULL;
      next_ = NULL;
    }

    inline CellNode *get_next() const
    {
      return next_;
    }

    TO_STRING_KV(KPC_(datum), K_(len), K_(ref));

    const ObDatum *datum_;
    // length of the common part with prefix
    int64_t len_;
    int64_t ref_;
    CellNode *next_;
    CellNode *prev_;
  };

  struct CellList
  {
    CellList() { reset(); }

    void reset()
    {
      size_ = 0;
      header_.next_ = &header_;
      header_.prev_ = &header_;
    }

    inline void merge(CellList &other)
    {
      other.header_.prev_->next_ = header_.next_;
      header_.next_->prev_ = other.header_.prev_;
      header_.next_ = other.header_.next_;
      other.header_.next_->prev_ = &header_;

      size_ += other.get_size();
      other.reset();
    }

    inline void add_first(CellNode *node)
    {
      header_.add_after(node);
      ++size_;
    }

    inline void remove(CellNode *node)
    {
      node->unlink();
      --size_;
    }

    inline CellNode *get_first()
    {
      return header_.next_;
    }

    inline CellNode *get_header()
    {
      return &header_;
    }

    inline int64_t get_size() const { return size_; }

    TO_STRING_KV(K_(size));

    int64_t size_;
    CellNode header_;
  };

  struct TreeNode
  {
    TreeNode() : prefix_(NULL), length_(0), children_(NULL),
    children_cnt_(0), cells_(), inactive_(false), ref_(0)
    {
    }

    void reset()
    {
      prefix_ = NULL;
      length_ = 0;
      children_ = NULL;
      children_cnt_ = 0;
      cells_.reset();
      inactive_ = false;
      ref_ = 0;
    }

    inline void set_active() { inactive_ = false; }
    inline void set_inactive() { inactive_ = true; }
    inline bool is_active() const { return false == inactive_; }
    inline bool is_leaf() const { return 0 == children_cnt_; }
    inline const ObDatum *get_prefix() const { return prefix_; }

    inline bool is_valid()
    {
      return 0 <= length_ && 0 <= cells_.get_size();
    }

    TO_STRING_KV(KP_(prefix), K_(length), KP_(children), K_(children_cnt),
        K_(cells), K_(inactive), K_(ref));

    const common::ObDatum *prefix_;
    int64_t length_; // length of common string
    TreeNode *children_;
    int64_t children_cnt_;
    CellList cells_;
    bool inactive_;
    int64_t ref_;
  };

  struct HashNode
  {
    const char *step_str_;
    CellList cells_;

    HashNode() { reset(); }

    void reset()
    {
      step_str_ = NULL;
      cells_.reset();
    }

    TO_STRING_KV(K_(step_str), K_(cells));
  };

  class HashTable
  {
  public:
    HashTable() : alloc_(NULL), buckets_(NULL), size_(0), hash_nodes_(NULL),
    created_(false), bucket_cnt_limit_(0), hnode_cnt_limit_(0)
    {
    }

    ~HashTable()
    {
    }

    void reuse()
    {
      MEMSET(buckets_, 0, sizeof(HashNode *) * bucket_cnt_limit_);
      // hash node will be reset when used
      size_ = 0;
    }

    int64_t size() const { return size_; }

    int create(const int64_t bucket_cnt_limit, const int64_t hnode_cnt_limit,
        common::ObArenaAllocator *alloc);
    int build(TreeNode &tnode, const int64_t level);

    HashNode *get_nodes() const { return hash_nodes_; }
    inline int64_t get_bucket_cnt_limit() const { return bucket_cnt_limit_; }
    inline void set_hnode_cnt_limit(const int64_t hnode_cnt_limit)
    {
      hnode_cnt_limit_ = hnode_cnt_limit;
    }

    inline uint64_t hash(const void *input, const int64_t length)
    {
      const int64_t seed = 0;
      return xxhash64(input, length, seed);
    }

    TO_STRING_KV(KP_(buckets), K_(size), KP_(hash_nodes), K_(created),
        K_(bucket_cnt_limit), K_(hnode_cnt_limit));

  private:
    common::ObArenaAllocator *alloc_;
    HashNode **buckets_;
    int64_t size_;
    HashNode *hash_nodes_;
    bool created_;
    int64_t bucket_cnt_limit_;
    int64_t hnode_cnt_limit_;
  };

  ObMultiPrefixTree();
  ~ObMultiPrefixTree();

  static const int64_t MAX_TNODE_CNT = 10L;
  static const int64_t SPLIT_PCT_THRESHOLD = 5L; // 1 / 5
  static const int64_t CNODE_LIMIT_RATIO = 5L;

public:
  int init(const int64_t cell_cnt, const int64_t bucket_cnt_limit);
  int build_tree(
      const ObColDatums *col_datums,
      int64_t &last_prefix_length,
      bool &suitable,
      int64_t &prefix_count,
      int64_t &prefix_length);
  void reuse();

  inline int64_t get_null_cnt() const { return null_cnt_; }
  inline int64_t get_nope_cnt() const { return nope_cnt_; }
  inline TreeNode *get_tree_nodes() const { return tree_nodes_; }
  inline int64_t get_tnode_cnt() const { return tnode_cnt_; }
  inline CellNode *get_cell_nodes() const { return cell_nodes_; }
  inline int64_t get_cnode_cnt() const { return cnode_cnt_; }
  inline int64_t get_bucket_cnt_limit() const { return ht_.get_bucket_cnt_limit(); }
  inline int64_t get_cnode_cnt_limit() const { return cnode_cnt_limit_; }
  inline bool is_inited() const { return is_inited_; }

  inline void set_cnode_cnt(const int64_t cnode_cnt)
  {
    cnode_cnt_ = cnode_cnt;
    // have to reset the hnode_cnt_limit_ of ht_
    ht_.set_hnode_cnt_limit(cnode_cnt / CNODE_LIMIT_RATIO + 1);
  }

  TO_STRING_KV(K_(is_inited), KP_(tree_nodes), KP_(cell_nodes),
      K_(tnode_cnt), K_(cnode_cnt), K_(cnode_cnt_limit), K_(ht),
      K_(null_cnt), K_(nope_cnt));
private:
  int init_root_node(const ObColDatums *col_datums);
  int traverse_by_level(const int64_t level, bool &stop, const int64_t move_step = MOVE_STEP);
  int try_previous_length(const int64_t last_prefix_length, bool &stop);
  int add_node(TreeNode &par_tnode, CellList &cells, const int64_t move_step);
  int complete_build(int64_t &prefix_count, int64_t &prefix_length,
      int64_t &last_prefix_length);
  OB_INLINE int64_t find_prefix(const char *left,
      const char *right, const int64_t len);

private:
  bool is_inited_;
  common::ObArenaAllocator alloc_;
  TreeNode *tree_nodes_; // the first tnode is root
  CellNode *cell_nodes_;
  int64_t tnode_cnt_;
  int64_t cnode_cnt_;
  int64_t cnode_cnt_limit_;
  HashTable ht_;
  int64_t null_cnt_;
  int64_t nope_cnt_;
};

class ObMultiPrefixTreeFactory
{
public:
  ObMultiPrefixTreeFactory();
  virtual ~ObMultiPrefixTreeFactory();

  int create(const int64_t cell_cnt, const int64_t bucket_cnt_limit,
      ObMultiPrefixTree *&prefix_tree);
  int recycle(ObMultiPrefixTree *prefix_tree);
private:
  void clear();

  common::ObPooledAllocator<ObMultiPrefixTree> allocator_;
  common::ObArray<ObMultiPrefixTree *> prefix_trees_;
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_MULTI_PREFIX_TREE_H_
