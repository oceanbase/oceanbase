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

// This file contains interface support for the JSON path abstraction.
#ifndef OCEANBASE_SQL_OB_JSON_PATH
#define OCEANBASE_SQL_OB_JSON_PATH

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_vector.h"
#include "lib/json_type/ob_json_common.h"

namespace oceanbase {
namespace common {


enum ObJsonPathNodeType {
  JPN_MEMBER = 0,               // member, example:.key
  JPN_MEMBER_WILDCARD,          // member wildcard, example:.*
  JPN_ARRAY_CELL,               // single array ele, example:[index]
  JPN_ARRAY_RANGE,              // array range, example:[8 to 16]
  JPN_ARRAY_CELL_WILDCARD,      // array ele wildcatd, example:[*]
  JPN_ELLIPSIS                  // ellipsis, example:**
};

typedef struct ObPathMember
{
  const char* object_name_;
  uint64_t   len_;
} ObPathMember;

typedef struct ObPathArrayCell
{
  uint64_t index_;
  bool is_index_from_end_;
} ObPathArrayCell;

typedef struct ObPathArrayRange
{
  uint64_t first_index_;
  uint64_t  last_index_;
  bool    is_first_index_from_end_;
  bool    is_last_index_from_end_;
} ObPathArrayRange;

typedef union ObPathNodeContent
{
  ObPathMember member_;           // JPN_MEMBER
  ObPathArrayCell array_cell_;    // JPN_ARRAY_CELL
  ObPathArrayRange array_range_;  // JPN_ARRAY_RANGE
  bool is_had_wildcard_;
} ObPathNodeContent;

// for [*] and [ to ], log actual range
// example: for size 5 array, [*] should change to [0, 5)
typedef struct ObArrayRange  
{
  uint64_t array_begin_;
  uint64_t  array_end_;
} ObArrayRange;


class ObJsonArrayIndex
{
 public:
  ObJsonArrayIndex() {}
  ObJsonArrayIndex(uint64_t index, bool is_from_end, size_t array_length);
  bool is_within_bounds() const;
  uint64_t get_array_index() const;
private:
  uint64_t array_index_;
  // True if the array index is within the bounds of the array. 
  bool is_within_bounds_;
};

// Basic class for JsonNode
// Here will be same class for oracle/mysql
// According to filters or function of oracle, it may has more class to produce.
class ObJsonPathNode 
{
public:
  ObJsonPathNode() {}
  virtual ~ObJsonPathNode();
  virtual ObJsonPathNodeType get_node_type() const;
  virtual ObPathNodeContent get_node_content() const;
  virtual bool is_autowrap() const = 0;
  virtual int node_to_string(ObJsonBuffer&) = 0;
protected:
  ObJsonPathNodeType node_type_;
  ObPathNodeContent node_content_;
};

class ObJsonPathBasicNode : public ObJsonPathNode
{
public:
  ObJsonPathBasicNode() {}
  virtual ~ObJsonPathBasicNode() {}
  explicit ObJsonPathBasicNode(ObString &keyname);                  // JPN_MEMBER
  ObJsonPathBasicNode(const char* name, uint64_t len);             // JPN_MEMBER
  explicit ObJsonPathBasicNode(uint64_t index);                    // JPN_ARRAY_CELL
  ObJsonPathBasicNode(uint64_t idx, bool is_from_end);
  ObJsonPathBasicNode(uint64_t first_idx, uint64_t last_idx);      // JPN_ARRAY_RANGE
  ObJsonPathBasicNode(uint64_t, bool, uint64_t, bool);
  int init(ObJsonPathNodeType cur_node_type);
  bool is_autowrap() const;
  // Get first_index according to from_end
  int get_first_array_index(uint64_t array_length, ObJsonArrayIndex &array_index) const;
  // Get last_index according to from_end when JPN_RANGE
  int get_last_array_index(uint64_t array_length, ObJsonArrayIndex &array_index) const;
  int get_array_range(uint64_t array_length, ObArrayRange &array_range) const;
  ObPathMember get_object() const;
  int node_to_string(ObJsonBuffer& str);
};

using JsonPathModuleArena = PageArena<ObJsonPathNode*, ModulePageAllocator>;
using ObJsonPathNodePointers = ObVector<ObJsonPathNode*, JsonPathModuleArena>;
using JsonPathIterator = ObJsonPathNodePointers::const_iterator;

class ObJsonPath
{
public:
  ObJsonPath(common::ObIAllocator* allocator);
  explicit ObJsonPath(const ObString& path, common::ObIAllocator *allocator);
  virtual ~ObJsonPath();
  common::ObIAllocator* get_allocator();
  int path_node_cnt();          // path length
  JsonPathIterator begin() const;   // iterator on path_nodes_
  JsonPathIterator end() const;     // iterator on path_nodes_
  int append(ObJsonPathNode* json_node);    // add to the tail of path_nodes_
  int to_string(ObJsonBuffer& str);          // transfer all pathnodes to string
  int parse_path();                         // do parse 
  bool can_match_many() const;
  bool is_contained_wildcard_or_ellipsis() const;
  ObString& get_path_string();
  ObJsonPathBasicNode* path_node(int index);
  ObJsonPathBasicNode* last_path_node();
private:
  common::ObIAllocator *allocator_;
  ModulePageAllocator page_allocator_;
  JsonPathModuleArena mode_arena_;
  ObJsonPathNodePointers path_nodes_;

  // obstring is not deepcopy, if allocator is not set, expression_ may be invalid, 
  // if expression_ pointer is released, the resource is hold outside
  ObString expression_;

  // if use_heap_expr_ is set none zero means: 
  // expression string is from heap-expr- which is allocated from allocator_,
  // has whole lifetime while the current class exists 
  ObJsonBuffer heap_expr_;
  int use_heap_expr_;

  bool is_contained_wildcard_or_ellipsis_; // for json_length, wildcards were forbidden
  
  uint64_t index_;
  int64_t bad_index_;
  int parse_path_node();
  int parse_array_node();
  int parse_member_node();
  int parse_ellipsis_node();
  int parse_array_index(uint64_t& array_index, bool& from_end);
  int add_array_node(bool is_cell_type, uint64_t& index1, uint64_t& index2,
      bool& from_end1, bool& from_end2);
  int get_origin_key_name(char* &str, uint64_t& length, bool is_quoted);
  int parse_name_with_rapidjson(char*& str, uint64_t& len);
};


// ObJsonPathCache
// used in json expression
class ObJsonPathCache {
public:
  enum ObPathParseStat{
    UNINITIALIZED,
    OK_NOT_NULL,
    OK_NULL,
    ERROR,
  };

  struct ObPathCacheStat{
    ObPathParseStat state_;
    int index_;
    ObPathCacheStat() : state_(UNINITIALIZED), index_(-1) {}
    ObPathCacheStat(ObPathParseStat state, int idx) : state_(state), index_(idx) {};
    ObPathCacheStat(const ObPathCacheStat& stat) : state_(stat.state_), index_(stat.index_) {}
  };

  typedef ObVector<ObJsonPath*> ObJsonPathPointers;
  typedef ObVector<ObPathCacheStat> ObPathCacheStatArr;

public:
  ObJsonPathCache() : allocator_(NULL) {};
  ObJsonPathCache(common::ObIAllocator *allocator);
  ~ObJsonPathCache() {};

  ObJsonPath* path_at(size_t idx);

  ObPathParseStat path_stat_at(size_t idx);
  
  size_t size();
  void reset();

  int find_and_add_cache(ObJsonPath*& parse_path, ObString& path_str, int arg_idx);
  void set_allocator(common::ObIAllocator *allocator);
  common::ObIAllocator* get_allocator();
private:
  int set_path(ObJsonPath* path, ObPathParseStat stat, int arg_idx, int index);
  bool is_match(ObString& path_str, size_t idx); 

  int fill_empty(size_t reserve_size);

private:
  ObJsonPathPointers path_arr_ptr_;
  ObPathCacheStatArr stat_arr_;
  common::ObIAllocator *allocator_;
};

using ObPathCacheStat = ObJsonPathCache::ObPathCacheStat;
using ObPathParseStat = ObJsonPathCache::ObPathParseStat;

class ObJsonPathUtil
{
public:
  static bool is_whitespace(char ch);
  static bool is_ecmascript_identifier(const char* name, uint64_t length);
  // add quote and 
  static int  double_quote(ObString &name, ObJsonBuffer* tmp_name);
  static bool is_terminator(char ch);
  static void skip_whitespace(const ObString& path, uint64_t& idx);
  static int append_array_index(uint64_t index, bool from_end, ObJsonBuffer& str);
  static int get_index_num(const ObString& expression, uint64_t& idx, uint64_t& array_idx);
  static int string_cmp_skip_charactor(ObString& lhs, ObString& rhs, char ch);

  // judge if has duplicate path
  //
  // @param [in] begin      path_node_beginner
  // @param [in] end        path_node_end
  // @param [in] auto_wrap  if need auto wrap to array
  // @return ret_bool       return true for it has duplicate path
  static bool path_gives_duplicates(const JsonPathIterator &begin,
                                    const JsonPathIterator &end,
                                    bool auto_wrap);

  static const uint32_t MAX_LENGTH = 4294967295;
  static const uint16_t MAX_PATH_NODE_CNT = 100;
};

} // namespace common
} // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_JSON_PATH