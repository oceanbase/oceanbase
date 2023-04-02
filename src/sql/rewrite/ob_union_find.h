// Copyright (c) 2015, 2016 Alibaba Inc. All Rights Reserved.
// Author:
//   zhenling.zzg


#ifndef OCEANBASE_SQL_REWRITE_OB_UNION_FIND_
#define OCEANBASE_SQL_REWRITE_OB_UNION_FIND_
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{

  /**
 * @brief
 * Union-Find: To efficiently check if two nodes in a graph are connected.
 * We use this algorithm to construct a graph for all the tables within a stmt
 * Once the graph is constructed, the connected relations of these tables are
 * also constructed.
 */
  struct UnionFind {
    UnionFind()
      : count_(0),
        is_inited_(false) {}
    UnionFind(int64_t n)
      : count_(n),
        is_inited_(false) {}
    virtual ~UnionFind() {}
    int64_t count_;
    ObSEArray<int64_t, 8> parent_;
    ObSEArray<int64_t, 8> tree_size_;

    bool is_inited_;
    int connect(int64_t p, int64_t q);
    int find_root(int64_t x, int64_t &root);
    int is_connected(int64_t p, int64_t q, bool &is_found);
    int init();
    void reset() {
      count_ = 0;
      parent_.reset();
      tree_size_.reset();
      is_inited_ = false;
    }
    TO_STRING_KV(K(count_),
                 K(parent_),
                 K(tree_size_),
                 K(is_inited_));
  };

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_REWRITE_OB_UNION_FIND_
