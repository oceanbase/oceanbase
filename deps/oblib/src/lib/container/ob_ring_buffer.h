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

#ifndef OCEANBASE_LIB_CONTAINER_OB_RING_BUFFER_
#define OCEANBASE_LIB_CONTAINER_OB_RING_BUFFER_

#include <algorithm>
#include "lib/allocator/ob_allocator.h"
#include "lib/ob_define.h"
#include "lib/queue/ob_link.h"

namespace oceanbase
{
namespace common
{
class ObRingBuffer
{
public:
  typedef ObLink Link;

  class Node: public Link
  {
  public:
    static Node* const LOCKED_ADDR;
    friend class ObRingBuffer;
    Node(int64_t blk_size, int64_t start, int64_t end) :
        next_purge_child_(0),
        child_size_((end - start) / (blk_size / sizeof(void *))),
        start_(start),
        end_(end)
    {
      memset(children_, 0, blk_size);
    }
    ~Node() {}
    int64_t start() const { return start_; }
    int64_t end() const { return end_; }
    int64_t size() const { return end_ - start_; }
    int64_t compare(Node *that)
    {
      return this->end_  - that->end_;
    }

    void purge(int64_t purge_pos, Link *&purge_list)
    {
      if (is_leaf()) {
        // do nothing
      } else {
        // many threads may interleave purge operations on child list
        for (int64_t idx = ATOMIC_LOAD(&next_purge_child_);
             get_child_start(idx) < std::min(end_, purge_pos);
             idx = std::max(idx + 1, ATOMIC_LOAD(&next_purge_child_))) {
          Node **pnode = children_ + idx;
          Node *node = NULL;
          if (get_child_end(idx) <= purge_pos) {
            // the whole range can be purged
            if (LOCKED_ADDR == (node = ATOMIC_TAS(pnode, LOCKED_ADDR))) {
              // another thread has been purging this node now
            } else if (NULL == node) {
              // alloc on demand, no request for this range ever
            } else {
              // recursively add all children to purge_list
              inc_update(&next_purge_child_, idx + 1);
              node->add_to_purge_list(purge_list);
              node->purge(purge_pos, purge_list);
            }
          } else {
            // purge part of the range that pnode responses for
            if (LOCKED_ADDR == (node = ATOMIC_LOAD(pnode))) {
            } else if (NULL == node) {
            } else {
              // wait all purge requests that begin before pruge_list being freed
              // request that begin after free operation will not access node by testing LOCKED_ADDR
              node->purge(purge_pos, purge_list);
            }
          }
        }
      }
    }

    int get(ObRingBuffer *host, int64_t end, char *&buf)
    {
      int ret = OB_SUCCESS;
      if (end <= start_) {
        ret = OB_ERR_OUT_OF_LOWER_BOUND;
      } else if (is_leaf()) {
        // my children_[0] is mapping to byte range:start_ ~ end_
        buf = ((char *)children_) + (end - start_);
      } else {
        // mutli-level node
        int64_t idx = get_child_idx(end);
        int64_t child_start = get_child_start(idx);
        // pnode repsonses for the wanted byte range
        Node **pnode = children_ + idx;
        Node *node = NULL;
        if (LOCKED_ADDR == (node = ATOMIC_LOAD(pnode))) {
          // purged, no one response for the range
          node = NULL;
          ret = OB_ERR_OUT_OF_LOWER_BOUND;
        } else if (NULL != node) {
          // get from this node
        } else if (NULL == host) {
          ret = OB_ENTRY_NOT_EXIST;
        } else if (OB_UNLIKELY(NULL == (node = host->alloc_node(child_start,
                                                                child_start + child_size_)))) {
          // alloc on demand
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (!ATOMIC_BCAS(pnode, NULL, node)) {
          // another thread is modifying child list, give up and retry
          ret = OB_EAGAIN;
        }
        if (OB_UNLIKELY(OB_SUCCESS != ret && NULL != host && NULL != node)) {
          host->free_node(node);
          node = NULL;
        }
        if (NULL != node) {
          // recursively get
          ret = node->get(host, end, buf);
        }
      }
      return ret;
    }

    void add_to_purge_list(Link *&purge_list)
    {
      retire_link_.next_ = purge_list;
      purge_list = &retire_link_;
    }

  private:
    bool is_leaf() { return child_size_ <= (int64_t)sizeof(void *); }
    int64_t get_child_start(int64_t idx) { return start_ + idx * child_size_; }
    int64_t get_child_end(int64_t idx) { return start_ + (idx + 1) * child_size_; }
    int64_t get_child_idx(int64_t end) { return (end - start_ - 1) / child_size_; }

    Link retire_link_;
    int64_t next_purge_child_;
    int64_t child_size_;
    int64_t start_;
    int64_t end_;
    Node *children_[0];
  };

  ObRingBuffer(): alloc_(NULL), blk_size_(0), start_(0), end_(0), head_(sizeof(void *), -1, -1) {}
  ~ObRingBuffer() {}

  int init(common::ObIAllocator *alloc, int64_t blk_size)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(NULL == alloc) || OB_UNLIKELY(blk_size <= (int64_t)sizeof(void *))) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      alloc_ = alloc;
      blk_size_ = blk_size;
    }
    return ret;
  }

  void destroy()
  {
    Link *del_list = NULL;
    int ret = OB_SUCCESS;
    while (OB_EAGAIN == truncate(INT64_MAX))
      ;
    while (OB_EAGAIN == (ret = purge(del_list))
           || OB_SUCCESS == ret)
      ;
    destroy_list(del_list);
    alloc_ = NULL;
    start_ = 0;
    end_ = 0;
  }

  void destroy_list(Link *del_list)
  {
    if (OB_UNLIKELY(NULL != alloc_)) {
      for (Link *p = del_list, *next = NULL; NULL != p; p = next) {
        Node *node = CONTAINER_OF(p, Node, retire_link_);
        next = link_next(p);
        alloc_->free(node);
        node = NULL;
      }
    }
  }

  int64_t size() const { return ATOMIC_LOAD(&end_) - ATOMIC_LOAD(&start_); }
  int purge(Link *&purge_list)
  {
    int ret = OB_SUCCESS;
    int64_t garbage_pos = ATOMIC_LOAD(&start_);
    Node *p = NULL;
    if (OB_UNLIKELY(!is_inited())) {
      ret = OB_NOT_INIT;
    } else if (NULL == (p = (Node *)link_next((Link *)&head_))) {
      ret = OB_ERR_OUT_OF_LOWER_BOUND;
    } else if (p->start() >= garbage_pos) {
      ret = OB_ERR_OUT_OF_LOWER_BOUND;
    } else {
      p->purge(garbage_pos, purge_list);
      if (p->end() <= garbage_pos) {
        if (0 == del_node(p)) {
          p->add_to_purge_list(purge_list);
        }
        ret = OB_EAGAIN;
      }
    }
    return ret;
  }

  int truncate(int64_t start)
  {
    // just adjust start_/end_, purge later
    int ret = OB_SUCCESS;
    int64_t cur_start = ATOMIC_LOAD(&start_);
    int64_t cur_end = ATOMIC_LOAD(&end_);
    if (OB_UNLIKELY(!is_inited())) {
      ret = OB_NOT_INIT;
    } else if (start <= cur_start) {
      // already truncated or purged
      // do nothing
    } else if (start > cur_end && !ATOMIC_BCAS(&end_, cur_end, start)) {
      ret = OB_EAGAIN;
    } else if (!ATOMIC_BCAS(&start_, cur_start, start)) {
      ret = OB_EAGAIN;
    }
    return ret;
  }

  int extend(int64_t end)
  {
    int ret = OB_SUCCESS;
    Node *node = NULL;
    int64_t cur_end = ATOMIC_LOAD(&end_);
    if (OB_UNLIKELY(!is_inited())) {
      ret = OB_NOT_INIT;
    } else if (end <= cur_end) {
      // already mapped
      // do nothing
    } else if (OB_UNLIKELY(NULL == (node = alloc_node(end_, end + size())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (!ATOMIC_BCAS(&end_, cur_end, cur_end + node->size())) {
      ret = OB_EAGAIN;
    } else {
      insert_node(node);
    }
    if (OB_FAIL(ret)) {
      free_node(node);
      node = NULL;
    }
    return ret;
  }

  int get(int64_t end, char *&buf, bool for_write)
  {
    int ret = OB_SUCCESS;
    Node *node = NULL;
    if (OB_UNLIKELY(!is_inited())) {
      ret = OB_NOT_INIT;
    } else if (end <= ATOMIC_LOAD(&start_)) {
      ret = OB_ERR_OUT_OF_LOWER_BOUND;
    } else if (end > ATOMIC_LOAD(&end_)) {
      ret = OB_ERR_OUT_OF_UPPER_BOUND;
    } else if (NULL == (node = search_node(end))) {
      ret = OB_EAGAIN;
    } else if (OB_ERR_OUT_OF_LOWER_BOUND == (ret = node->get(for_write ? this : NULL, end, buf))) {
      ret = OB_EAGAIN;
    }
    return ret;
  }

private:
  bool is_inited() { return NULL != alloc_ && blk_size_ > 0; }

  static int64_t calc_node_size(int64_t blk_size, int64_t req_size)
  {
    // raise the size to the node size of some depth,
    int64_t size = blk_size;
    while (size < req_size) {
      size *= (blk_size / sizeof(void *));
    }
    return size;
  }

  Node *alloc_node(int64_t start, int64_t end)
  {
    Node *p = NULL;
    if (NULL != (p = (Node *)alloc_->alloc(blk_size_ + sizeof(Node)))) {
      int64_t node_size = calc_node_size(blk_size_, end - start);
      new(p)Node(blk_size_, start, start + node_size);
    }
    return p;
  }

  void free_node(Node *node)
  {
    if (NULL != alloc_ && NULL != node) {
      alloc_->free(node);
      node = NULL;
    }
  }

  Node *search_node(int64_t end)
  {
    Node *prev = NULL;
    Node key(sizeof(void *), end, end);
    return ol_search(&head_, &key, prev);
  }

  int insert_node(Node *node)
  {
    return ol_insert(&head_, node);
  }

  int del_node(Node *node)
  {
    Node *target = NULL;
    return ol_del(&head_, node, target);
  }
private:
  common::ObIAllocator *alloc_;
  int64_t blk_size_;
  int64_t start_;
  int64_t end_;
  Node head_; // sentinel node
  DISALLOW_COPY_AND_ASSIGN(ObRingBuffer);
};

}; // end namespace container
}; // end namespace oceanbase

#endif // OCEANBASE_LIB_CONTAINER_OB_RING_BUFFER_
