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

#define USING_LOG_PREFIX SHARE

#include "ob_search_index_config_filter.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/charset/ob_charset.h"
#include "sql/parser/parse_node.h"
#include "share/ob_define.h" // OZ macro

namespace oceanbase
{
namespace share
{
using namespace common;

ObSearchIndexJsonPathMatcher::ObSearchIndexJsonPathMatcher(const uint64_t tenant_id)
  : tenant_id_(tenant_id),
    allocator_(ObModIds::OB_JSON_PARSER, tenant_id_),
    inited_(false),
    root_(nullptr),
    all_nodes_(nullptr)
{
}

ObSearchIndexJsonPathMatcher::~ObSearchIndexJsonPathMatcher()
{
  reset();
}

void ObSearchIndexJsonPathMatcher::reset()
{
  Node *n = all_nodes_;
  while (n != nullptr) {
    (void)n->children_.destroy();
    n = n->next_all_;
  }
  root_ = nullptr;
  inited_ = false;
  allocator_.reset();
}

int ObSearchIndexJsonPathMatcher::init(const ObString &spec, int64_t &pos)
{
  int ret = OB_SUCCESS;
  reset();
  if (spec.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty spec provided", K(ret));
  } else if (OB_ISNULL(root_) && OB_FAIL(new_node(root_))) {
    LOG_WARN("failed to new root node", K(ret));
  } else if (OB_FAIL(parse_and_insert(spec, pos))) {
    LOG_WARN("failed to parse and insert spec", K(ret), K(spec));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObSearchIndexJsonPathMatcher::match(
    const ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items, const bool is_range_cmp,
    bool &matched, bool &is_terminal) const
{
  int ret = OB_SUCCESS;
  matched = false;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized or root is null", K(ret));
  } else {
    const Node *cur = root_;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < path_items.count(); ++idx) {
      Node *child = nullptr;
      if (OB_SUCCESS == (cur->children_.get_refactored("*", child))) {
        matched = true;
        is_terminal = false;
        break;
      }
      const ObSearchIndexPathEncoder::JsonPathItem &item = path_items.at(idx);
      if (item.is_array_path()) {
        matched = cur->terminal_;
        is_terminal = cur->terminal_;
        break;
      } else if (OB_FAIL(cur->children_.get_refactored(item.path, child)) || OB_ISNULL(child)) {
        ret = OB_SUCCESS;
        break;
      } else if ((is_range_cmp || child->terminal_) && idx == path_items.count() - 1) {
        matched = true;
        is_terminal = true;
        break;
      } else {
        cur = child;
      }
    }
  }
  return ret;
}

int ObSearchIndexJsonPathMatcher::new_node(Node *&node)
{
  int ret = OB_SUCCESS;
  node = OB_NEWx(Node, (&allocator_));
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    node->next_all_ = all_nodes_;
    all_nodes_ = node;

    const ObMemAttr bucket_attr(tenant_id_, ObModIds::OB_HASH_BUCKET);
    const ObMemAttr node_attr(tenant_id_, ObModIds::OB_HASH_NODE);
    if (OB_FAIL(node->children_.create(8 /*bucket_num*/, bucket_attr, node_attr))) {
      LOG_WARN("failed to create hashmap for node children", K(ret));
    }
  }
  return ret;
}

int ObSearchIndexJsonPathMatcher::get_or_create_child(Node &node, const ObString &seg, Node *&child)
{
  int ret = OB_SUCCESS;
  child = nullptr;
  ret = const_cast<common::hash::ObHashMap<ObString, Node*>&>(node.children_).get_refactored(seg, child);
  if (ret == OB_HASH_NOT_EXIST) {
    ret = OB_SUCCESS;
    if (OB_FAIL(new_node(child))) {
      LOG_WARN("failed to new node", K(ret));
    } else if (OB_FAIL(node.children_.set_refactored(seg, child))) {
      LOG_WARN("failed to set refactored", K(ret));
    }
  }
  return ret;
}

int ObSearchIndexJsonPathMatcher::insert_path(const ObString &path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root node not initialized", K(ret));
  } else {
    ObJsonPath json_path(path, &allocator_);
    if (OB_FAIL(json_path.parse_path())) {
      LOG_WARN("failed to parse path", K(ret), K(path));
    } else {
      Node *cur = root_;
      JsonPathIterator it = json_path.begin();
      if (it == json_path.end()) {
        ObString wildcard(ObString::make_string("*"));
        Node *child = nullptr;
        if (OB_FAIL(get_or_create_child(*cur, wildcard, child))) {
          LOG_WARN("failed to get or create child for wildcard", K(ret));
        } else {
          cur = child;
        }
      } else {
        for (; OB_SUCC(ret) && it != json_path.end(); ++it) {
          ObJsonPathNode *node = *it;
          if (OB_ISNULL(node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null path node", K(ret));
            break;
          }
          ObJsonPathNodeType node_type = node->get_node_type();
          if (node_type == JPN_ROOT) {
          } else if (node_type == JPN_MEMBER) {
            ObJsonPathBasicNode *basic_node = static_cast<ObJsonPathBasicNode *>(node);
            ObString key_name(basic_node->get_object().len_, basic_node->get_object().object_name_);
            Node *child = nullptr;
            if (OB_FAIL(get_or_create_child(*cur, key_name, child))) {
              LOG_WARN("failed to get or create child", K(ret));
            } else {
              cur = child;
            }
          } else if (node_type == JPN_MEMBER_WILDCARD) {
            ObString wildcard(ObString::make_string("*"));
            Node *child = nullptr;
            if (OB_FAIL(get_or_create_child(*cur, wildcard, child))) {
              LOG_WARN("failed to get or create child for wildcard", K(ret));
            } else {
              cur = child;
            }
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not supported node type", K(ret), K(node_type));
            break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        cur->terminal_ = true;
      }
    }
  }
  return ret;
}

int ObSearchIndexJsonPathMatcher::parse_and_insert(const ObString &spec, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const char *p = spec.ptr();
  int64_t len = spec.length();

  if (pos >= len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid spec format", K(ret));
  } else {
    while (OB_SUCC(ret) && pos < len && isdigit(p[pos])) {
      int64_t path_len = 0;
      if (OB_FAIL(extract_int(spec, 0, pos, path_len))) {
        LOG_WARN("failed to extract path length", K(ret));
      } else if (pos >= len || pos + path_len > len) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid path format or insufficient data", K(ret), K(pos), K(path_len), K(len));
      } else {
        ObString path(path_len, p + pos);
        if (OB_FAIL(insert_path(path))) {
          LOG_WARN("failed to insert path", K(ret));
        }
        pos += path_len;
      }
    }
  }
  return ret;
}

ObSearchIndexConfigFilter::ObSearchIndexConfigFilter(const uint64_t tenant_id)
  : has_include_paths_(false),
    has_exclude_paths_(false),
    type_mask_(0),
    include_matcher_(tenant_id),
    exclude_matcher_(tenant_id)
{
}

void ObSearchIndexConfigFilter::reset()
{
  has_include_paths_ = false;
  has_exclude_paths_ = false;
  type_mask_ = 0;
  include_matcher_.reset();
  exclude_matcher_.reset();
}

// Checks whether the given path may use the search index according to INCLUDE_PATHS or EXCLUDE_PATHS.
//
// INCLUDE_PATHS:
//   - Exact path (e.g. "$.a.b.c"): only equality on that path uses the index; range on $.a.b.c does not.
//   - Path with wildcard (e.g. "$.a.b.c.*"): paths under that prefix use the index, same as defined semantics.
//
// EXCLUDE_PATHS (for consistent behavior when excluded subtree has no index rows):
//   - Exact path (e.g. "$.a.b.c"): $.a.b.c uses no index; ancestor paths $.a, $.a.b also cannot use range index.
//     Example: row {"a":{"b":{"c":2}}} has no index data when $.a.b.c is excluded, so json_extract(c1,'$.a')>1
//     must not use the index or results would differ from a non-index scan.
//   - Path with wildcard (e.g. "$.a.b.c.*"): $.a.b.c.* uses no index; $.a, $.a.b, $.a.b.c cannot use range index.
int ObSearchIndexConfigFilter::is_path_indexed(
    const ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
    bool &passed, const bool is_range_cmp) const
{
  int ret = OB_SUCCESS;
  passed = true;
  bool is_terminal = false;
  if (has_include_paths_) {
    if (OB_FAIL(include_matcher_.match(path_items, false, passed, is_terminal))) {
      LOG_WARN("failed to match include paths", K(ret), K(path_items));
    } else if (is_terminal && is_range_cmp) {
      passed = false;
    }
  } else if (has_exclude_paths_) {
    if (OB_FAIL(exclude_matcher_.match(path_items, is_range_cmp, passed, is_terminal))) {
      LOG_WARN("failed to match exclude paths", K(ret), K(path_items));
    } else {
      passed = !passed;
    }
  }
  return ret;
}

int ObSearchIndexConfigFilter::is_indexed(
    const ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
    const ObJsonNodeType json_type, bool &passed, const bool is_range_cmp) const
{
  int ret = OB_SUCCESS;
  passed = true;
  if (has_types() && !is_type_indexed(json_type)) {
    passed = false;
  } else if (has_paths() && OB_FAIL(is_path_indexed(path_items, passed, is_range_cmp))) {
    LOG_WARN("failed to check path", K(ret));
  }
  LOG_DEBUG("check_path_and_type", K(is_range_cmp), K(passed), K(has_types()), K(has_paths()), K(json_type));
  return ret;
}

bool ObSearchIndexConfigFilter::is_type_indexed(const ObJsonNodeType json_type) const
{
  bool bret = true;
  if (type_mask_ == 0) {
  } else if (json_type == ObJsonNodeType::J_STRING) {
    bret = ((type_mask_ & TYPE_MASK_STRING) != 0);
  } else if (ObIJsonBase::is_json_number_type(json_type)) {
    bret = ((type_mask_ & TYPE_MASK_NUMBER) != 0);
  } else {
    bret = false;
  }
  return bret;
}

int append_search_index_list_items(const ParseNode *node, ObSqlString &out, const bool is_path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected search index column params", K(ret));
  } else if (node->type_ == T_LINK_NODE) {
    if (node->num_child_ != 2 || OB_ISNULL(node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected search index column params", K(ret));
    } else if (OB_FAIL(append_search_index_list_items(node->children_[0], out, is_path))) {
      LOG_WARN("failed to append search index list items", K(ret));
    } else if (OB_FAIL(append_search_index_list_items(node->children_[1], out, is_path))) {
      LOG_WARN("failed to append search index list items", K(ret));
    }
  } else {
    if (is_path) {
      ObString path_str(static_cast<int32_t>(node->str_len_), node->str_value_);
      ObArenaAllocator tmp_alloc("JsonPathValid");
      void *buf = tmp_alloc.alloc(sizeof(ObJsonPath));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc path", K(ret));
      } else {
        ObJsonPath *json_path = new (buf) ObJsonPath(path_str, &tmp_alloc);
        if (OB_FAIL(json_path->parse_path())) {
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "search index config");
        } else if (!ObSearchIndexConfigFilter::is_valid_config_path(json_path)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported jsonpath expression", K(ret), K(path_str));
        }
      }
    }
    OZ (out.append_fmt("%d%.*s", static_cast<int32_t>(node->str_len_), static_cast<int32_t>(node->str_len_), node->str_value_));
  }
  return ret;
}

int build_search_index_single_opt(const ParseNode *opt,
                                         bool &has_inc_paths,
                                         bool &has_exc_paths,
                                         bool &has_inc_types,
                                         ObSqlString &inc_paths,
                                         ObSqlString &exc_paths,
                                         ObSqlString &inc_types)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(opt) || opt->num_child_ != 1 || OB_ISNULL(opt->children_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const ParseNode *list = opt->children_[0];
    if (opt->type_ == T_SEARCH_INDEX_INCLUDE_PATHS) {
      if (has_inc_paths) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        has_inc_paths = true;
        OZ (inc_paths.append("INCLUDE_PATHS="));
        OZ (append_search_index_list_items(list, inc_paths, true));
      }
    } else if (opt->type_ == T_SEARCH_INDEX_EXCLUDE_PATHS) {
      if (has_exc_paths) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        has_exc_paths = true;
        OZ (exc_paths.append("EXCLUDE_PATHS="));
        OZ (append_search_index_list_items(list, exc_paths, true));
      }
    } else if (opt->type_ == T_SEARCH_INDEX_INCLUDE_TYPES) {
      if (has_inc_types) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        has_inc_types = true;
        OZ (inc_types.append("INCLUDE_TYPES="));
        OZ (append_search_index_list_items(list, inc_types, false));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
  }
  return ret;
}

int build_search_index_opt_list(const ParseNode *node,
                                       bool &has_inc_paths,
                                       bool &has_exc_paths,
                                       bool &has_inc_types,
                                       ObSqlString &inc_paths,
                                       ObSqlString &exc_paths,
                                       ObSqlString &inc_types)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    // empty
  } else if (node->type_ == T_LINK_NODE) {
    if (node->num_child_ != 2 || OB_ISNULL(node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected search index column params", K(ret));
    } else if (OB_FAIL(build_search_index_opt_list(node->children_[0], has_inc_paths, has_exc_paths,
                                                   has_inc_types, inc_paths, exc_paths,
                                                   inc_types))) {
      LOG_WARN("failed to build search index opt list", K(ret));
    } else if (OB_FAIL(build_search_index_opt_list(node->children_[1], has_inc_paths, has_exc_paths,
                                                   has_inc_types, inc_paths, exc_paths,
                                                   inc_types))) {
      LOG_WARN("failed to build search index opt list", K(ret));
    }
  } else if (OB_FAIL(build_search_index_single_opt(node, has_inc_paths, has_exc_paths,
                                                   has_inc_types, inc_paths, exc_paths,
                                                   inc_types))) {
    LOG_WARN("failed to build search index single opt", K(ret));
  }
  return ret;
}

int print_length_prefixed_values(const char *data, int64_t len, int64_t &idx,
                                        char *buf, int64_t buf_len, int64_t &pos,
                                        bool with_quotes)
{
  int ret = OB_SUCCESS;
  bool first = true;
  ObString str;
  str.assign_ptr(data, static_cast<int32_t>(len));
  while (OB_SUCC(ret) && idx < len && isdigit(data[idx])) {
    int64_t value_len = 0;
    if (OB_FAIL(extract_int(str, 0, idx, value_len))) {
      LOG_WARN("failed to extract path length", K(ret));
    } else if (idx + value_len > len) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid config format", K(ret), K(idx), K(value_len), K(len));
    } else if (!first && OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
      LOG_WARN("fail to print comma", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, with_quotes ? "'%.*s'" : "%.*s",
                                static_cast<int>(value_len), data + idx))) {
      LOG_WARN("fail to print value", K(ret));
    }
    idx += value_len;
    first = false;
  }
  return ret;
}

int ObSearchIndexConfigFilter::init_from_comment(const ObString &raw)
{
  int ret = OB_SUCCESS;
  reset();
  if (raw.empty()) {
  } else {
    const char *data = raw.ptr();
    const int64_t len = raw.length();
    int64_t pos = 0;

    const char *include_paths_key = "INCLUDE_PATHS=";
    const char *exclude_paths_key = "EXCLUDE_PATHS=";
    const char *include_types_key = "INCLUDE_TYPES=";
    const int64_t paths_key_len = strlen(include_paths_key);
    const int64_t types_key_len = strlen(include_types_key);

    if (pos + paths_key_len > len) {
      // not enough data to match any path key, skip
    } else if (0 == MEMCMP(data + pos, include_paths_key, paths_key_len)) {
      has_include_paths_ = true;
      pos += paths_key_len;
      if (OB_FAIL(include_matcher_.init(raw, pos))) {
        LOG_WARN("failed to init include matcher", K(ret), K(raw));
      }
    } else if (0 == MEMCMP(data + pos, exclude_paths_key, paths_key_len)) {
      has_exclude_paths_ = true;
      pos += paths_key_len;
      if (OB_FAIL(exclude_matcher_.init(raw, pos))) {
        LOG_WARN("failed to init exclude matcher", K(ret), K(raw));
      }
    }
    if (OB_SUCC(ret) && pos + types_key_len <= len
        && 0 == MEMCMP(data + pos, include_types_key, types_key_len)) {
      pos += types_key_len;
      ObString types_value_str(len - pos, data + pos);
      int64_t idx = 0;
      const int64_t types_len = types_value_str.length();
      while (OB_SUCC(ret) && idx < types_len && isdigit(types_value_str.ptr()[idx])) {
        int64_t token_len = 0;
        if (OB_FAIL(extract_int(types_value_str, 0, idx, token_len))) {
          LOG_WARN("failed to extract type token length", K(ret));
        } else if (idx + token_len > types_len) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid INCLUDE_TYPES format", K(ret), K(idx), K(token_len), K(types_len));
        } else {
          ObString token;
          token.assign_ptr(types_value_str.ptr() + idx, static_cast<int32_t>(token_len));
          if (token == ObString::make_string("json_string")) {
            type_mask_ |= ObSearchIndexConfigFilter::TYPE_MASK_STRING;
          } else if (token == ObString::make_string("json_number")) {
            type_mask_ |= ObSearchIndexConfigFilter::TYPE_MASK_NUMBER;
          }
          idx += token_len;
        }
      }
    }
  }
  return ret;
}

int ObSearchIndexConfigFilter::print_comment(const ParseNode *param_node,
                                             common::ObSqlString &out)
{
  using namespace sql;
  int ret = OB_SUCCESS;
  out.reset();
  if (OB_ISNULL(param_node) || param_node->type_ != T_SEARCH_INDEX_COLUMN_PARAMS) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid search index column params", K(ret));
  } else if (param_node->num_child_ != 1 || OB_ISNULL(param_node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected search index column params", K(ret));
  } else {
    const ParseNode *opt_list = param_node->children_[0];
    bool has_inc_paths = false;
    bool has_exc_paths = false;
    bool has_inc_types = false;
    ObSqlString inc_paths;
    ObSqlString exc_paths;
    ObSqlString inc_types;
    OZ (build_search_index_opt_list(opt_list, has_inc_paths, has_exc_paths, has_inc_types, inc_paths, exc_paths, inc_types));
    if (OB_SUCC(ret) && has_inc_paths && has_exc_paths) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "search index: cannot set both include and exclude paths.");
      LOG_WARN("invalid search index column params", K(ret));
    } else {
      OZ(out.append(inc_paths.string()));
      OZ(out.append(exc_paths.string()));
      OZ(out.append(inc_types.string()));
    }
  }
  return ret;
}

int ObSearchIndexConfigFilter::print_schema(const ObString &comment,
                                            char *buf,
                                            int64_t buf_len,
                                            int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (!comment.empty()) {
    const char *data = comment.ptr();
    const int64_t len = comment.length();
    int64_t idx = 0;
    const char *paths_key = NULL;
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " WITH ("))) {
      LOG_WARN("fail to print WITH (", K(ret));
    } else if (idx + 14 <= len && 0 == MEMCMP(data + idx, "INCLUDE_PATHS=", 14)) {
      paths_key = "INCLUDE_PATHS";
      idx += 14;
    } else if (idx + 14 <= len && 0 == MEMCMP(data + idx, "EXCLUDE_PATHS=", 14)) {
      paths_key = "EXCLUDE_PATHS";
      idx += 14;
    }

    if (OB_SUCC(ret) && paths_key != NULL) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s = (", paths_key))) {
        LOG_WARN("fail to print paths key", K(ret));
      } else if (OB_FAIL(print_length_prefixed_values(data, len, idx, buf, buf_len, pos, true))) {
        LOG_WARN("fail to print paths value", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
        LOG_WARN("fail to print closing paren", K(ret));
      }
    }

    if (OB_SUCC(ret) && idx + 14 <= len && 0 == MEMCMP(data + idx, "INCLUDE_TYPES=", 14)) {
      idx += 14;
      if (paths_key != NULL && OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
        LOG_WARN("fail to print comma", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "INCLUDE_TYPES = ("))) {
        LOG_WARN("fail to print INCLUDE_TYPES", K(ret));
      } else if (OB_FAIL(print_length_prefixed_values(data, len, idx, buf, buf_len, pos, false))) {
        LOG_WARN("fail to print types value", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
        LOG_WARN("fail to print closing paren", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
      LOG_WARN("fail to print )", K(ret));
    }
  }
  return ret;
}

// Returns true if the path is valid for search index config: each node must be a member (key),
// and the last node may be a member wildcard (*).
// Example: "$.a.b.c" is valid; "$.a.b.*" is valid (wildcard only at the last segment).
bool ObSearchIndexConfigFilter::is_valid_config_path(common::ObJsonPath *json_path)
{
  bool ret_bool = true;
  int i = 0;
  int total_nodes = json_path->path_node_cnt();

  JsonPathIterator it = json_path->begin();
  for (; i < total_nodes && ret_bool == true && it != json_path->end(); ++i, ++it) {
    ObJsonPathNode *node = *it;
    if (OB_ISNULL(node) || node->get_node_type() != JPN_MEMBER) {
      ret_bool = false;
      break;
    }
  }

  if (ret_bool == false && i == total_nodes - 1 && it != json_path->end()) {
    ObJsonPathNode *node = *it;
    if (OB_NOT_NULL(node) && node->get_node_type() == JPN_MEMBER_WILDCARD) {
      ret_bool = true;
    }
  }

  return ret_bool;
}

} // namespace share
} // namespace oceanbase
