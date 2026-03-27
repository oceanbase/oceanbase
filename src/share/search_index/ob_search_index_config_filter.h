/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_SEARCH_INDEX_CONFIG_FILTER_H_
#define OCEANBASE_SHARE_OB_SEARCH_INDEX_CONFIG_FILTER_H_

#include <stdint.h>
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h" // ObArenaAllocator
#include "lib/hash/ob_hashmap.h"
#include "lib/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/string/ob_sql_string.h"
#include "lib/json_type/ob_json_path.h"
#include "lib/json_type/ob_json_base.h" // common::ObJsonNodeType
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "ob_search_index_encoder.h"
#include "sql/parser/parse_node.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace share
{

class ObSearchIndexJsonPathMatcher final
{
public:
  explicit ObSearchIndexJsonPathMatcher(const uint64_t tenant_id = OB_SERVER_TENANT_ID);
  ~ObSearchIndexJsonPathMatcher();

  void reset();

  // Initialize from a single string spec.
  int init(const ObString &spec, int64_t &idx);

  // Segment-aware prefix match. Returns true if any include path is a prefix of target_path.
  int match(const common::ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
            const bool index_sub_path, bool &matched, bool &is_terminal) const;


private:
  struct Node final
  {
    Node() : terminal_(false), children_(), next_all_(nullptr) {}
    bool terminal_;
    common::hash::ObHashMap<ObString, Node*> children_;
    Node *next_all_;
  };

private:
  int new_node(Node *&node);
  int get_or_create_child(Node &node, const ObString &seg, Node *&child);
  int insert_path(const ObString &path);
  int parse_and_insert(const ObString &spec, int64_t &pos);

private:
  uint64_t tenant_id_;
  mutable ObArenaAllocator allocator_;
  bool inited_;
  Node *root_;
  Node *all_nodes_;

  DISALLOW_COPY_AND_ASSIGN(ObSearchIndexJsonPathMatcher);
};

// ============================================================================
// ObSearchIndexConfigFilter - Column-level filter for JSON search index
// ============================================================================

class ObSearchIndexConfigFilter
{
public:
  static const uint8_t TYPE_MASK_STRING = (1 << 0);
  static const uint8_t TYPE_MASK_NUMBER = (1 << 1);

  explicit ObSearchIndexConfigFilter(const uint64_t tenant_id);

  // Initialize from a single string comment.
  int init_from_comment(const ObString &raw);
  void reset();

  // Combine path + type checks for JSON scalar nodes.
  int is_indexed(const common::ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
                 const common::ObJsonNodeType json_type, bool &passed) const;
  int is_indexed(const common::ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
                 const ObItemType pick_type, bool &passed, const bool index_sub_path) const;
  static bool is_type_indexed(const uint8_t type_mask, const common::ObJsonNodeType json_type);

  // Build search index configuration string from parse node
  static int print_comment(const ParseNode *param_node, common::ObSqlString &out);
  // Print search index column config (INCLUDE_PATHS/EXCLUDE_PATHS/INCLUDE_TYPES) to buf for SHOW CREATE TABLE.
  static int print_schema(const common::ObString &comment, char *buf, int64_t buf_len, int64_t &pos);
  static bool is_valid_config_path(common::ObJsonPath *json_path);
  bool has_types() const { return type_mask_ != 0; }
  uint8_t get_type_mask() const { return type_mask_; }

  TO_STRING_KV(K_(has_include_paths), K_(has_exclude_paths), K_(type_mask));
private:
  bool has_paths() const { return has_include_paths_ || has_exclude_paths_; }
  static common::ObJsonNodeType get_json_type(const ObItemType pick_type);
  int is_path_indexed(const common::ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
                      bool &passed, const bool index_sub_path = false) const;

private:
  bool has_include_paths_;
  bool has_exclude_paths_;
  uint8_t type_mask_;
  ObSearchIndexJsonPathMatcher include_matcher_;
  ObSearchIndexJsonPathMatcher exclude_matcher_;
};

}
}

#endif // OCEANBASE_SHARE_OB_SEARCH_INDEX_CONFIG_FILTER_H_
