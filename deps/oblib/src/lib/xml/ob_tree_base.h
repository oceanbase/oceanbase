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
 * This file contains interface define for the tree base abstraction.
 */

#ifndef OCEANBASE_SQL_OB_TREE_BASE
#define OCEANBASE_SQL_OB_TREE_BASE

#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_vector.h"
#include "lib/number/ob_number_v2.h" // for number::ObNumber
#include "lib/xml/ob_multi_mode_interface.h"


namespace oceanbase {
namespace common {
class ObLibTreeNodeBase;
class ObIMulModeBase;

const int64_t DEFAULT_PAGE_SIZE = 4096L; // 4kb

static const int32_t MEMBER_SCALAR_FLAG = 0x1;
static const int32_t MEMBER_SEQUENT_FLAG = 0x2;
static const int32_t MEMBER_IN_SET_FLAG = 0x4;
static const int32_t MEMBER_USING_BUFFER = 0x8;
static const int32_t MEMBER_LAZY_SORTED = 0x10;

typedef std::pair<int64_t, int64_t> IndexRange;
#define HAS_CONTAINER_MEMBER(node) ((node)->has_sequent_member() && (node)->has_sorted_member())

#pragma pack(4)
class ObLibTreeNodeBase {
public:
  ObLibTreeNodeBase(int32_t flags, ObNodeDataType type)
    : type_(type),
      flags_(flags),
      pos_(-1),
      parent_(nullptr)
  {}

  virtual ~ObLibTreeNodeBase() {}
  // tree node interface
  ObLibTreeNodeBase* get_parent() { return parent_; }
  void set_parent(ObLibTreeNodeBase* parent) { parent_ = parent; }
  void set_index(int64_t pos) { pos_ = static_cast<int32_t>(pos); }
  void increase_index() { pos_++; }
  void decrease_index() { pos_--; }
  virtual void set_flags(uint32_t flags) { flags_ |= flags; }

  // same meaning
  virtual int64_t size() const { return 1; }
  virtual int64_t count() const { return 1; }

  bool is_leaf_node() const { return static_cast<bool>(MEMBER_SCALAR_FLAG & flags_); }
  bool is_lazy_sort() const { return static_cast<bool>(MEMBER_LAZY_SORTED & flags_); }
  bool has_sorted_member() const { return static_cast<bool>(MEMBER_IN_SET_FLAG & flags_); }
  bool has_sequent_member() const { return static_cast<bool>(MEMBER_SEQUENT_FLAG & flags_); }
  bool is_using_child_buffer() const { return static_cast<bool>(MEMBER_USING_BUFFER & flags_); }

  int get_key(ObString& key);

  int insert_prev(ObLibTreeNodeBase* new_node);
  int insert_after(ObLibTreeNodeBase* new_node);

  // 返回节点具体类型
  // 例如：json返回jsonInt，jsonDouble
  // xml 返回xmlElment, XmlAttribute
  virtual int node_type() { return type_; }
  // 数据修改接口, 修改的是孩子
  virtual int append(ObLibTreeNodeBase* node) = 0;
  virtual int insert(int64_t pos, ObLibTreeNodeBase* node) = 0;
  virtual int remove(int64_t pos) = 0;
  virtual int remove(ObLibTreeNodeBase* node) = 0;
  virtual int update(int64_t pos, ObLibTreeNodeBase* new_node) = 0;
  virtual int update(ObLibTreeNodeBase* old_node, ObLibTreeNodeBase* new_node) = 0;
  int32_t get_flags() const { return flags_; }
  int64_t get_index() const { return pos_; }
  int insert_slibing(ObLibTreeNodeBase* new_node, int64_t relative_index);

  int64_t to_string(char *buf, const int64_t buf_len) const {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "type = %d, flags_=%d, pos_=%d", type_, flags_, pos_);
    return pos;
  }
protected:
  ObNodeDataType type_;
  int32_t flags_;
  int32_t pos_;
  /* 父节点，公共 */
  ObLibTreeNodeBase* parent_;
};

#pragma pack()

// need order and set both
// orginal document order and sorted order for query quickly
class ObLibContainerNode : public ObLibTreeNodeBase {
public:

  ObLibContainerNode(ObNodeDataType type, ObMulModeMemCtx *ctx)
    : ObLibTreeNodeBase(MEMBER_SEQUENT_FLAG | MEMBER_IN_SET_FLAG, type),
      ctx_(ctx),
      children_(nullptr),
      sorted_children_(nullptr)
  {
    flags_ |= MEMBER_USING_BUFFER;
  }

  ObLibContainerNode(ObNodeDataType type, int32_t flags, ObMulModeMemCtx *ctx)
    : ObLibTreeNodeBase(flags, type),
      ctx_(ctx),
      children_(nullptr),
      sorted_children_(nullptr)
  {
    if (flags & MEMBER_SEQUENT_FLAG) {
      flags_ |= MEMBER_USING_BUFFER;
    }

    if (flags & MEMBER_IN_SET_FLAG) {
      flags_ |= MEMBER_USING_BUFFER;
    }
  }

  ObLibContainerNode(ObNodeDataType type)
    : ObLibTreeNodeBase(MEMBER_SCALAR_FLAG, type),
      ctx_(nullptr),
      children_(nullptr),
      sorted_children_(nullptr)
  {
  }

  ~ObLibContainerNode() {}

  virtual int64_t size() const;
  virtual int64_t count() const;

  // 数据修改接口, 修改的是孩子
  int append(ObLibTreeNodeBase* node) override;
  int insert(int64_t pos, ObLibTreeNodeBase* node) override;
  int remove(int64_t pos) override;
  int remove(ObLibTreeNodeBase* node) override;
  int update(int64_t pos, ObLibTreeNodeBase* new_node) override;
  int update(ObLibTreeNodeBase* old_node, ObLibTreeNodeBase* new_node) override;
  int get_range(int64_t start, int64_t end, ObIArray<ObLibTreeNodeBase*>& res);
  int get_children(ObIArray<ObLibTreeNodeBase*>& res);
  int get_children(const ObString& key, ObIArray<ObLibTreeNodeBase*>& res);
  virtual void set_flags(uint32_t flags) { ObLibTreeNodeBase::set_flags(flags); }
  virtual void del_flags(uint32_t flags) { flags_ &= (~flags); }
  ObLibTreeNodeBase* member(size_t pos);
  ObMulModeMemCtx* get_mem_ctx() { return ctx_; }
  int alter_member_sort_policy(bool enable);

  class iterator {
  public:
    friend class ObLibContainerNode;
    friend class tree_iterator;
    iterator()
      : is_eval_current_(false),
        cur_pos_(-1),
        total_(-1),
        vector_(nullptr),
        cur_node_(nullptr)
    {}
    // construct
    iterator(const iterator& iter)
      : is_eval_current_(iter.is_eval_current_),
        cur_pos_(iter.cur_pos_),
        total_(iter.total_),
        vector_(iter.vector_),
        cur_node_(iter.cur_node_)
    {}

    // construct
    iterator(ObLibContainerNode* node, bool is_eval_current)
    {
      cur_node_ = node;
      is_eval_current_ = is_eval_current;
      if (node->is_leaf_node()) {
        cur_pos_ = 0;
        total_ = 1;
        vector_ = nullptr;
      } else if (node->is_using_child_buffer()) {
        cur_pos_ = 0;
        total_ = node->child_[0] == nullptr ? 0 : 1;
        vector_ = nullptr;
      } else {
        ObLibTreeNodeVector* data_vector = nullptr;
        if (node->has_sequent_member()) {
          data_vector = node->children_;
        } else {
          data_vector = node->sorted_children_;
        }
        cur_pos_ = 0;
        total_ = data_vector->size();
        vector_ = data_vector;
      }
    }

    iterator(ObLibContainerNode* node)
      : iterator(node, false) {}

    ObLibContainerNode* current();
    ObLibContainerNode* operator*();
    ObLibContainerNode* operator[](int64_t pos);

    bool end();
    iterator next();
    iterator operator++();
    iterator operator--();
    iterator operator++(int);
    iterator operator--(int);
    bool operator<(const iterator& iter);
    bool operator>(const iterator& iter);
    iterator operator-(int size);
    iterator operator+(int size);
    iterator operator+=(int size);
    iterator operator-=(int size);
    int64_t operator-(const iterator& iter);
    bool operator==(const iterator& rhs);
    bool operator!=(const iterator& rhs);
    bool operator<=(const iterator& rhs);
    void set_range(int64_t start, int64_t finish);

    int64_t to_string(char *buf, const int64_t buf_len) const {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "cur_pos = %ld, total_=%ld", cur_pos_, total_);
      return pos;
    }
  private:
    bool is_eval_current() { return is_eval_current_; }
    void set_eval_current() { is_eval_current_ = true; }

  private:
    bool is_eval_current_;
    int64_t cur_pos_;
    int64_t total_;
    ObLibTreeNodeVector* vector_;
    ObLibContainerNode* cur_node_;
  };

  iterator begin();
  iterator end();
  iterator sorted_begin();
  iterator sorted_end();

  typedef std::pair<iterator, iterator> IterRange;
  int get_children(const ObString& key, IterRange& range);

protected:
  class tree_iterator {
    friend class ObLibContainerNode;
  public:
    tree_iterator(ObLibContainerNode* root, ObIAllocator* allocator)
      : type_(POST_ORDER),
        root_(root),
        stack_(allocator) {}

    tree_iterator(ObLibContainerNode* root, scan_type type, ObIAllocator* allocator)
      : type_(type),
        root_(root),
        stack_(allocator) {}

    int start();
    int next(ObLibContainerNode*& res);

  private:
    scan_type type_;
    ObLibContainerNode* root_;
    ObStack<iterator> stack_;
  };
  IndexRange get_effective_range(int64_t start, int64_t end);

protected:
  int64_t get_member_index(ObLibTreeNodeVector& container, ObLibTreeNodeBase* node);
  int insert_slibing(ObLibTreeNodeBase* new_node, int is_after_cur);
  void increase_index_after(int64_t pos);
  void decrease_index_after(int64_t pos);
  void sort();
  void do_sort();
  int remove_from_sequent_container(int64_t pos);
  void remove_from_sequent_container(ObLibTreeNodeBase* node);
  void remove_from_sorted_container(int64_t pos);
  int remove_from_sorted_container(ObLibTreeNodeBase* node);
  int append_into_sorted_container(ObLibTreeNodeBase* node);
  bool check_container_valid();
  int append_into_sequent_container(ObLibTreeNodeBase* node);
  int insert_into_sequent_container(int64_t pos, ObLibTreeNodeBase* node);
  int extend();

protected:
  ObMulModeMemCtx* ctx_;
  union {
    ObLibContainerNode* child_[2];
    struct {
      ObLibTreeNodeVector *children_; // document order array
      ObLibTreeNodeVector *sorted_children_; // sorted array
    };
  };
};

typedef ObLibContainerNode::IterRange IterRange;

}
}

#endif  // OCEANBASE_SQL_OB_TREE_BASE