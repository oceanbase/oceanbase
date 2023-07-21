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
 * This file contains interface support for the JSON path abstraction.
 */

#ifndef OCEANBASE_SQL_OB_JSON_PATH
#define OCEANBASE_SQL_OB_JSON_PATH

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_vector.h"
#include "lib/json_type/ob_json_common.h"
#include "src/sql/parser/parse_node.h"

namespace oceanbase {
namespace common {

enum ObJsonPathNodeType {
  JPN_ERROR = 0,
  JPN_ROOT = 1,

  JPN_BEGIN_BASIC_FLAG,
  JPN_MEMBER,              // member, example：.key                 -> mysql & oracle
  JPN_MEMBER_WILDCARD,     // member wildcard, example：.*          -> mysql & oracle
  JPN_ARRAY_CELL,          // single array ele, example：[index]    -> mysql
  JPN_ARRAY_RANGE,         // array range, example：[8 to 16]       -> mysql
  JPN_ARRAY_CELL_WILDCARD, // array ele wildcatd, example：[*]      -> mysql & oracle
  JPN_WILDCARD_ELLIPSIS,   // ellipsis for mysql, example：**       -> mysql
  JPN_MULTIPLE_ARRAY,      // array for oracle, example:[3, 4 to 6] -> oracle
  JPN_DOT_ELLIPSIS,        // ellipsis, example：..                 -> oracle
  JPN_END_BASIC_FLAG,

  // item method - oracle
  JPN_BEGIN_FUNC_FLAG,
  JPN_ABS, // abs()
  JPN_BOOLEAN, // boolean()
  JPN_BOOL_ONLY, // booleanOnly()
  JPN_CEILING,
  JPN_DATE,
  JPN_DOUBLE,
  JPN_FLOOR,
  JPN_LENGTH,
  JPN_LOWER,
  JPN_NUMBER,
  JPN_NUM_ONLY,
  JPN_SIZE,
  JPN_STRING,
  JPN_STR_ONLY,
  JPN_TIMESTAMP,
  JPN_TYPE,
  JPN_UPPER,
  JPN_END_FUNC_FLAG,

  // filter 25
  JPN_BEGIN_FILTER_FLAG,
  JPN_EQUAL, // predicate ==
  JPN_UNEQUAL, // !=
  JPN_LARGGER, // >
  JPN_LARGGER_EQUAL, // >=
  JPN_SMALLER, // <
  JPN_SMALLER_EQUAL, // <=
  JPN_SUBSTRING, // has substring
  JPN_STARTS_WITH, // starts with
  JPN_LIKE, // like
  JPN_LIKE_REGEX, // like_regex
  JPN_EQ_REGEX, // eq_regex
  JPN_NOT_EXISTS,
  JPN_EXISTS, // exist
  JPN_AND_COND, // cond1 && cond2
  JPN_OR_COND, // cond1 || cond2
  JPN_NOT_COND, // !(cond)
  JPN_END_FILTER_FLAG,

  JPN_BEGIN_FILTER_CONTENT_FLAG,
  JPN_SQL_VAR,	// sql identifier
  JPN_BOOL_TRUE,
  JPN_BOOL_FALSE,
  JPN_NULL,
  JPN_SCALAR,	// quoted
  JPN_SUB_PATH,
  JPN_END_FILTER_CONTENT_FLAG,

  JPN_MAX
};

enum {
  OPT_JSON_VALUE,
  OPT_JSON_QUERY,
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

class ObJsonPath;
class ObJsonPathFilterNode;

typedef struct ObPathScalar {
  char* scalar_;
  uint64_t s_length_;
} ObPathScalar;

typedef struct ObPathVar {
  char* var_;
  uint64_t v_length_;
} ObPathVar;

// >, ==, like, has substring are comp_node(JPN_EQUAL - JPN_EXISTS)
// except for exists && !exists, comp_node have both left and right arg
// ObCompContent is arg of comp_node, the arg could be: scalar, subpath or sql_var
typedef union ObCompContent
{
  ObJsonPath* filter_path_;
  ObPathScalar path_scalar_;
  ObPathVar path_var_;
} ObCompContent;

typedef struct ObPathComparison
{
  // content of compare_node
  ObCompContent comp_left_;
  ObCompContent comp_right_;
  ObJsonPathNodeType left_type_;
  ObJsonPathNodeType right_type_;
} ObPathComparison;

// Condition：&&, ||, !()
// the left and right arg must be comp_node or  cond_node
// eg：$?(@.LineItems.Part.Description=="Nixon") ——> legal
//      $?(@.LineItems.Part.Description && true) ——> illegal
// the node_content_ of cond_node must be two FilterNode
typedef struct ObPathCondition
{
  ObJsonPathFilterNode* cond_left_;
  ObJsonPathFilterNode* cond_right_;
}ObPathCondition;

typedef PageArena<ObPathArrayRange*, ModulePageAllocator> PathArrayRangeArena;
using ObMultiArrayPointers = ObVector<ObPathArrayRange*, PathArrayRangeArena>;
using MultiArrayIterator = ObMultiArrayPointers::const_iterator;
class ObPathNodeContent
{
private:
  static const int64_t DEFAULT_PAGE_SIZE = (1LL << 10); // 1kb
public:
  ObPathNodeContent(ObIAllocator *allocator)
      : page_allocator_(*allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
        mode_arena_(DEFAULT_PAGE_SIZE, page_allocator_),
        multi_array_(&mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR) {}

  ModulePageAllocator page_allocator_;
  PathArrayRangeArena mode_arena_;
  ObPathMember member_;              // JPN_MEMBER
  ObPathArrayCell array_cell_;       // JPN_ARRAY_CELL
  ObPathArrayRange array_range_;     // JPN_ARRAY_RANGE
  bool is_had_wildcard_;
  ObMultiArrayPointers multi_array_;   //JPN_MULTIPLE_ARRAY,
  ObPathComparison comp_;
  ObPathCondition cond_;
};

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
  bool is_within_bounds() { return is_within_bounds_; }
  uint64_t get_array_index() { return array_index_; }
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
  ObJsonPathNode(ObIAllocator *allocator) : node_content_(allocator) {}
  virtual ~ObJsonPathNode();
  virtual ObJsonPathNodeType get_node_type() const;
  virtual bool is_autowrap() const = 0;
  virtual int node_to_string(ObJsonBuffer&,  bool is_mysql, bool is_next_array) = 0;
  TO_STRING_KV(K_(node_type));
  ObJsonPathNodeType node_type_;
  ObPathNodeContent node_content_;
};

class ObJsonPathBasicNode : public ObJsonPathNode
{
public:
  ObJsonPathBasicNode(ObIAllocator *allocator): ObJsonPathNode(allocator) {}
  virtual ~ObJsonPathBasicNode() {}
  explicit ObJsonPathBasicNode(ObIAllocator *allocator, ObString &keyname);                  // JPN_MEMBER
  ObJsonPathBasicNode(ObIAllocator *allocator, const char* name, uint64_t len);             // JPN_MEMBER
  explicit ObJsonPathBasicNode(ObIAllocator *allocator, uint64_t index);                    // JPN_ARRAY_CELL
  ObJsonPathBasicNode(ObIAllocator *allocator, uint64_t idx, bool is_from_end);
  ObJsonPathBasicNode(ObIAllocator *allocator, uint64_t first_idx, uint64_t last_idx);      // JPN_ARRAY_RANGE
  ObJsonPathBasicNode(ObIAllocator *allocator, uint64_t, bool, uint64_t, bool);
  ObJsonPathBasicNode(ObIAllocator *allocator, ObPathArrayRange* o_array);
  int init(ObJsonPathNodeType cur_node_type, bool is_mysql);
  bool is_autowrap() const;
  bool is_multi_array_autowrap() const;
  // Get first_index according to from_end
  int get_first_array_index(uint64_t array_length, ObJsonArrayIndex &array_index) const;
  // Get last_index according to from_end when JPN_RANGE
  int get_last_array_index(uint64_t array_length, ObJsonArrayIndex &array_index) const;
  int get_array_range(uint64_t array_length, ObArrayRange &array_range) const;
  int get_multi_array_size() const;
  int get_multi_array_range(uint32_t idx, uint64_t array_length, ObArrayRange &array_range) const;
  ObPathMember get_object() const;
  int add_multi_array(ObPathArrayRange* o_array);
  int node_to_string(ObJsonBuffer& str,  bool is_mysql, bool is_next_array);
  int mysql_to_string(ObJsonBuffer& str);
  int oracle_to_string(ObJsonBuffer& str, bool is_next_array);
};

class ObJsonPathFuncNode : public ObJsonPathNode
{
  public:
  ObJsonPathFuncNode(ObIAllocator *allocator): ObJsonPathNode(allocator) {}
  virtual ~ObJsonPathFuncNode() {}
  int init(const char* name, uint64_t len);
  bool is_autowrap() const;
  int node_to_string(ObJsonBuffer& str,  bool is_mysql, bool is_next_array);
};

typedef PageArena<char, ModulePageAllocator> JsonCharArrayArena;
typedef ObVector<char, JsonCharArrayArena> ObCharArrayPointers;
typedef PageArena<ObJsonPathFilterNode*, ModulePageAllocator> JsonPathFilterArena;
typedef ObVector<ObJsonPathFilterNode*, JsonPathFilterArena> ObFilterArrayPointers;
using ObFilterArrayIterator = ObFilterArrayPointers::iterator;
using ObCharArrayIterator = ObCharArrayPointers::iterator;

class ObJsonPathFilterNode : public ObJsonPathNode
{
  public:
  ObJsonPathFilterNode(ObIAllocator *allocator): ObJsonPathNode(allocator) {}
  virtual ~ObJsonPathFilterNode() {}
  bool is_autowrap() const;
  int comp_half_to_string(ObJsonBuffer& str, bool is_left);
  int comp_to_string(ObJsonBuffer& str);
  int cond_to_string(ObJsonBuffer& str);
  int node_to_string(ObJsonBuffer& str,  bool is_mysql, bool is_next_array);
  int init_left_comp_path(ObJsonPath* spath);
  int init_right_comp_path(ObJsonPath* spath);
  void init_left_scalar(char* str, uint64_t len);
  int init_right_scalar(char* str, uint64_t len);
  void init_left_var(char* str, uint64_t len);
  int init_right_var(char* str, uint64_t len);
  void init_bool_or_null(ObJsonPathNodeType comp_type, bool left);
  int init_comp_type(ObJsonPathNodeType comp_type);
  void init_cond_left(ObJsonPathFilterNode* node);
  int init_cond_right(ObJsonPathFilterNode* node);
  int init_cond_type(ObJsonPathNodeType cond_type);
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
  void set_subpath_arg(bool is_lax);
  int path_node_cnt() { return path_nodes_.size(); }  // path length
  ObJsonPathNodeType get_last_node_type();
  bool is_last_func();
  bool path_not_str();
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
  static int change_json_expr_res_type_if_need(common::ObIAllocator &allocator, ObString &str, ParseNode &ret_node, int8_t json_expr_flag);
  static int get_path_item_method_str(common::ObIAllocator &allocator, ObString &str, char*& res, uint64_t &len, bool &has_fun);
  bool is_lax_ = true;
  bool is_mysql_;
  bool is_sub_path_;
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
  int parse_mysql_path();
  int parse_oracle_path();
  int parse_mysql_path_node();
  int parse_oracle_path_node();
  int parse_member_wildcard_node();
  int parse_array_wildcard_node();
  int parse_single_array_node();
  int parse_mysql_member_node();
  int parse_wildcard_ellipsis_node();
  int parse_dot_ellipsis_node();
  int parse_dot_node();
  int parse_oracle_member_node();
  int init_multi_array(ObPathArrayRange* o_array, uint64_t index1, uint64_t index2,
      bool from_end1, bool from_end2);
  int parse_multiple_array_node();
  int parse_single_array_index(uint64_t& array_index, bool& from_end);
  int parse_multiple_array_index(uint64_t& index1, uint64_t& index2,
      bool& from_end1, bool& from_end2);
  int add_single_array_node(bool is_cell_type, uint64_t& index1, uint64_t& index2,
      bool& from_end1, bool& from_end2);
  int get_mysql_origin_key_name(char* &str, uint64_t& length, bool is_quoted);
  int get_oracle_origin_key_name(char* &str, uint64_t& length, bool is_quoted, bool& is_func);
  int parse_name_with_rapidjson(char*& str, uint64_t& len);
  int parse_func_node(char*& name, uint64_t& len);
  int get_char_comparison_type(ObJsonPathFilterNode* filter_comp_node);
  int get_func_comparison_type(ObJsonPathFilterNode* filter_comp_node);
  int get_comparison_type(ObJsonPathFilterNode* filter_comp_node, bool not_exists);
  int jump_over_dot();
  int jump_over_double_quote();
  int get_sub_path(char*& sub_path, uint64_t& len);
  int get_var_name(char*& name, uint64_t& len);
  int get_num_str(char*& num, uint64_t& len);
  int parse_comp_exist(ObJsonPathFilterNode* filter_comp_node);
  int parse_comp_var(ObJsonPathFilterNode* filter_comp_node, bool left);
  int parse_comp_string_num(ObJsonPathFilterNode* filter_comp_node, bool left);
  int parse_comp_half(ObJsonPathFilterNode* filter_comp_node, bool left);
  int parse_comparison(ObFilterArrayPointers& filter_stack, bool not_exists);
  int is_legal_comparison(ObJsonPathFilterNode* filter_comp_node);
  bool is_illegal_comp_for_func(const ObJsonPathNodeType last_path_node_type,
                                const ObJsonPathNodeType scalar_type, const ObPathScalar scalar);
  int parse_filter_node();
  int parse_condition(ObFilterArrayPointers& filter_stack, ObCharArrayPointers& char_stack, char in);
  int push_filter_char_in(char in, ObFilterArrayPointers& filter_stack, ObCharArrayPointers& char_stack);
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
  typedef PageArena<ObJsonPath*, ModulePageAllocator> JsonPathArena;
  typedef PageArena<ObPathCacheStat, ModulePageAllocator> PathCacheStatArena;
  typedef ObVector<ObJsonPath*, JsonPathArena> ObJsonPathPointers;
  typedef ObVector<ObPathCacheStat, PathCacheStatArena> ObPathCacheStatArr;
  static const int64_t DEFAULT_PAGE_SIZE = (1LL << 10); // 1kb

public:
  ObJsonPathCache(common::ObIAllocator *allocator) :
        allocator_(allocator),
        page_allocator_(*allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
        path_cache_arena_(DEFAULT_PAGE_SIZE, page_allocator_),
        path_arena_(DEFAULT_PAGE_SIZE, page_allocator_),
        path_arr_ptr_(&path_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
        stat_arr_(&path_cache_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR) {}
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
  common::ObIAllocator *allocator_;
  ModulePageAllocator page_allocator_;
  PathCacheStatArena path_cache_arena_;
  JsonPathArena      path_arena_;
  ObJsonPathPointers path_arr_ptr_;
  ObPathCacheStatArr stat_arr_;
};

using ObPathCacheStat = ObJsonPathCache::ObPathCacheStat;
using ObPathParseStat = ObJsonPathCache::ObPathParseStat;

class ObJsonPathUtil
{
public:
  static bool is_whitespace(char ch);
  static bool is_ecmascript_identifier(const char* name, uint64_t length);
  static bool is_oracle_keyname(const char* name, uint64_t length);
  // add quote and 
  static int  double_quote(ObString &name, ObJsonBuffer* tmp_name);
  static bool is_mysql_terminator(char ch);
  static bool is_begin_field_name(char ch);
  static bool is_end_of_comparission(char ch);
  static bool letter_or_not(char ch);
  static bool is_digit(char ch);
  static bool is_scalar(const ObJsonPathNodeType node_type);
  static void skip_whitespace(const ObString& path, uint64_t& idx);
  static int append_array_index(uint64_t index, bool from_end, ObJsonBuffer& str);
  static int get_index_num(const ObString& expression, uint64_t& idx, uint64_t& array_idx);
  static int string_cmp_skip_charactor(ObString& lhs, ObString& rhs, char ch);
  static char priority(char top, char in);
  static int pop_char_stack(ObCharArrayPointers& char_stack);
  static int pop_filter_stack(ObFilterArrayPointers& filter_stack);

  // judge if has duplicate path
  //
  // @param [in] begin      path_node_beginner
  // @param [in] end        path_node_end
  // @param [in] auto_wrap  if need auto wrap to array
  // @return ret_bool       return true for it has duplicate path
  static bool path_gives_duplicates(const JsonPathIterator &begin,
                                    const JsonPathIterator &end,
                                    bool auto_wrap);

  static bool is_letter(unsigned codepoint, const char* ori, uint64_t start, uint64_t end);
  static bool is_connector_punctuation(unsigned codepoint);
  static bool unicode_combining_mark(unsigned codepoint);
  static bool is_utf8_unicode_charator(const char* ori, uint64_t& start, int64_t len);

  static const uint32_t MAX_LENGTH = 4294967295;
  static const uint16_t MAX_PATH_NODE_CNT = 100;
};

} // namespace common
} // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_JSON_PATH