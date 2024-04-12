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
 * This file contains interface support for the XML Path abstraction.
 */

#ifndef OCEANBASE_SQL_OB_XPATH
#define OCEANBASE_SQL_OB_XPATH

#include "ob_tree_base.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_vector.h"
#include "src/share/datum/ob_datum.h"
#include "lib/json_type/ob_json_path.h"
#include "lib/xml/ob_mul_mode_reader.h"
#include "lib/xml/ob_multi_mode_interface.h"
#include "lib/allocator/ob_pooled_allocator.h"

namespace oceanbase {
namespace common {

enum ObParserType {
  PARSER_ERROR = 0,
  PARSER_JSON_PATH_STRICT = 1,
  PARSER_JSON_PATH_LAX,
  PARSER_XML_PATH,
  PARSER_MAX
};
enum ObPathNodeClass {
  PN_ERROR = 0,
  PN_ROOT = 1,
  PN_LOCATION,  // location node
  PN_FILTER,    // filter node
  PN_FUNCTION,  // function node
  PN_ARG,  // argument node
  PN_LOCATION_FILTER,
  PN_MAX
};

enum ObLocationType {
  PN_LOCATION_ERROR = 0,
  PN_KEY,              // seek by keyname
  PN_ARRAY,              // seek by keyname
  PN_ELLIPSIS,
  PN_LOCATION_MAX
};

enum ObFilterType {
  PN_FILTER_ERROR = 0,
  PN_NOT_COND,        // !(cond)
  PN_CMP_UNION = 2,   // union |, same with ObXpathFilterChar::UNION, don't change the position from union to mod
  PN_OR_COND,         // cond1 || cond2
  PN_AND_COND,        // cond1 && cond2
  PN_CMP_EQUAL,       // predicate ==
  PN_CMP_UNEQUAL,     // !=
  PN_CMP_LE,          // <=
  PN_CMP_LT,          // <
  PN_CMP_GE,          // >=
  PN_CMP_GT,          // >
  PN_CMP_ADD,         // +
  PN_CMP_SUB,         // -
  PN_CMP_MUL,         // *
  PN_CMP_DIV,         // div
  PN_CMP_MOD,         // mod
  PN_SUBSTRING,       // has substring
  PN_STARTS_WITH,     // starts with
  PN_LIKE,            // like
  PN_LIKE_REGEX,      // like_regex
  PN_EQ_REGEX,        // eq_regex
  PN_NOT_EXISTS,
  PN_EXISTS,          // exist
  PN_PURE_ARG,
  PN_FILTER_MAX
};

static constexpr char* filter_type_str_map[ObFilterType::PN_FILTER_MAX] = {
  const_cast<char*>("!("),
  const_cast<char*>(" | "),
  const_cast<char*>(" or "),  // json is ||
  const_cast<char*>(" and "), // json is &&
  const_cast<char*>(" = "),   // json is ==
  const_cast<char*>(" != "),
  const_cast<char*>(" <= "),
  const_cast<char*>(" < "),
  const_cast<char*>(" >= "),
  const_cast<char*>(" > "),
  const_cast<char*>(" + "),
  const_cast<char*>(" - "),
  const_cast<char*>(" * "),
  const_cast<char*>(" div "),
  const_cast<char*>(" mod "),
};

enum ObFuncType {
  PN_FUNC_ERROR = 0,
  PN_ABS,               // abs()
  PN_BOOLEAN_FUNC,      // boolean()
  PN_BOOL_ONLY,         // booleanOnly()
  PN_CEILING,
  PN_DATE_FUNC,
  PN_DOUBLE_FUNC,
  PN_FLOOR,
  PN_LENGTH,
  PN_LOWER,
  PN_NUMBER_FUNC,
  PN_NUM_ONLY,
  PN_SIZE,
  PN_STRING_FUNC,
  PN_STR_ONLY,
  PN_TIMESTAMP,
  PN_TYPE,
  PN_UPPER,
  PN_POSITION,
  PN_LAST,
  PN_COUNT,
  PN_CONCAT,
  PN_CONTAINS,
  PN_FALSE,
  PN_TRUE,
  PN_LOCAL_NAME,
  PN_LANG,
  PN_NOT_FUNC,
  PN_SUM,
  PN_NAME,
  PN_NS_URI,
  PN_NORMALIZE_SPACE,
  PN_SUBSTRING_FUNC,
  PN_ROUND,
  PN_FUNC_MAX
};

static constexpr char* func_str_map[PN_FUNC_MAX - PN_ABS] = {
  const_cast<char*>("abs("),
  const_cast<char*>("boolean("),
  const_cast<char*>("booleanOnly("),
  const_cast<char*>("ceiling("),
  const_cast<char*>("date("),
  const_cast<char*>("double("),
  const_cast<char*>("floor("),
  const_cast<char*>("length("),
  const_cast<char*>("lower("),
  const_cast<char*>("number("),
  const_cast<char*>("numberOnly("),
  const_cast<char*>("size("),
  const_cast<char*>("string("),
  const_cast<char*>("stringOnly("),
  const_cast<char*>("timestamp("),
  const_cast<char*>("type("),
  const_cast<char*>("upper("),
  const_cast<char*>("position("),
  const_cast<char*>("last("),
  const_cast<char*>("count("),
  const_cast<char*>("concat("),
  const_cast<char*>("contains("),
  const_cast<char*>("false("),
  const_cast<char*>("true("),
  const_cast<char*>("local-name("),
  const_cast<char*>("lang("),
  const_cast<char*>("not("),
  const_cast<char*>("sum("),
  const_cast<char*>("name("),
  const_cast<char*>("namespace-uri("),
  const_cast<char*>("normalize-space("),
  const_cast<char*>("substring("),
  const_cast<char*>("round("),
};

static constexpr int func_arg_num[PN_FUNC_MAX][2] = {
  /* PN_ABS            */ {0, 0},
  /* PN_BOOLEAN_FUNC   */ {1, 1},
  /* PN_BOOL_ONLY      */ {0, 0},
  /* PN_CEILING        */ {0, 0},
  /* PN_DATE_FUNC      */ {0, 0},
  /* PN_DOUBLE_FUNC    */ {0, 0},
  /* PN_FLOOR          */ {0, 0},
  /* PN_LENGTH         */ {0, 0},
  /* PN_LOWER          */ {0, 0},
  /* PN_NUMBER_FUNC    */ {0, 0},
  /* PN_NUM_ONLY       */ {0, 0},
  /* PN_SIZE           */ {0, 0},
  /* PN_STRING_FUNC    */ {0, 0},
  /* PN_STR_ONLY       */ {0, 0},
  /* PN_TIMESTAMP      */ {0, 0},
  /* PN_TYPE           */ {0, 0},
  /* PN_UPPER          */ {0, 0},
  /* PN_POSITION       */ {0, 0},
  /* PN_LAST           */ {0, 0}, // following func, todo
  /* PN_COUNT          */ {1, 1},
  /* PN_CONCAT         */ {0, 0},
  /* PN_CONTAINS       */ {0, 0},
  /* PN_FALSE          */ {0, 0},
  /* PN_TRUE           */ {0, 0},
  /* PN_LOCAL_NAME     */ {0, 0},
  /* PN_LANG           */ {0, 0},
  /* PN_NOT_FUN        */ {1, 1},
  /* PN_SUM            */ {0, 0},
  /* PN_NAME           */ {0, 0},
  /* PN_NS_URI         */ {0, 0},
  /* PN_NORMALIZE_SPACE*/ {0, 0},
  /* PN_SUBSTRING_FUNC */ {0, 0},
  /* PN_ROUND          */ {0, 0},
  /* PN_FUNC_MAX       */ {0, 0},
};

static constexpr int func_name_len[PN_FUNC_MAX] = {
  /* PN_ABS            */ 3,
  /* PN_BOOLEAN_FUNC   */ 7,
  /* PN_BOOL_ONLY      */ 11,
  /* PN_CEILING        */ 7,
  /* PN_DATE_FUNC      */ 4,
  /* PN_DOUBLE_FUNC    */ 6,
  /* PN_FLOOR          */ 5,
  /* PN_LENGTH         */ 6,
  /* PN_LOWER          */ 5,
  /* PN_NUMBER_FUNC    */ 6,
  /* PN_NUM_ONLY       */ 10,
  /* PN_SIZE           */ 4,
  /* PN_STRING_FUNC    */ 6,
  /* PN_STR_ONLY       */ 10,
  /* PN_TIMESTAMP      */ 5,
  /* PN_TYPE           */ 4,
  /* PN_UPPER          */ 5,
  /* PN_POSITION       */ 8,
  /* PN_LAST           */ 4, // following func, todo
  /* PN_COUNT          */ 5,
  /* PN_CONCAT         */ 6,
  /* PN_CONTAINS       */ 8,
  /* PN_FALSE          */ 5,
  /* PN_TRUE           */ 4,
  /* PN_LOCAL_NAME     */ 10,
  /* PN_LANG           */ 4,
  /* PN_NOT_FUN        */ 3,
  /* PN_SUM            */ 3,
  /* PN_NAME           */ 4,
  /* PN_NS_URI         */ 13,
  /* PN_NORMALIZE_SPACE*/ 15,
  /* PN_SUBSTRING_FUNC */ 9,
  /* PN_ROUND          */ 5,
  /* PN_FUNC_MAX       */ 0,
};

enum ObArgType {
  PN_ARG_ERROR = 0,
  PN_BOOLEAN,
  PN_STRING,
  PN_INT,
  PN_DOUBLE,
  PN_SQL_VAR,
  PN_SUBPATH,
  PN_ARG_MAX
};

enum ObSeekType {
  ERROR_SEEK = 0,
  OBJECT = 1,  // JSON OBJECT
  ARRAY,
  NODES, // 3
  // ObSeekType (from element to pi) is corresponding to ObMulModeNodeType, don't change the position
  ELEMENT, // 4
  TEXT,
  COMMENT,
  PROCESSING_INSTRUCTION,
  MAX_SEEK
};


static constexpr char* nodetest_str_map[ObSeekType::MAX_SEEK - ObSeekType::NODES] = {
  const_cast<char*>("node()"),
  const_cast<char*>(""), // element
  const_cast<char*>("text()"),
  const_cast<char*>("comment()"),
  const_cast<char*>("processing-instruction(")
};

enum ObPathNodeAxis {
  ERROR_AXIS = 0, // wrong axis, only used in ellipsis node
  SELF = 1,
  PARENT,
  ANCESTOR,
  ANCESTOR_OR_SELF,
  CHILD,
  DESCENDANT,
  DESCENDANT_OR_SELF,
  FOLLOWING_SIBLING,
  FOLLOWING,
  PRECEDING_SIBLING,
  PRECEDING,
  ATTRIBUTE,
  NAMESPACE,
  MAX_AXIS
};

static constexpr char* axis_str_map[ObPathNodeAxis::NAMESPACE] = {
  const_cast<char*>("self::"),
  const_cast<char*>("parent::"),
  const_cast<char*>("ancestor::"),
  const_cast<char*>("ancestor-or-self::"),
  const_cast<char*>("child::"),
  const_cast<char*>("descendant::"),
  const_cast<char*>("descendant-or-self::"),
  const_cast<char*>("following-sibling::"),
  const_cast<char*>("following::"),
  const_cast<char*>("preceding-sibling::"),
  const_cast<char*>("preceding::"),
  const_cast<char*>("@"),
  const_cast<char*>("namespace::")
};

// characters that will be pushed into char_stack when parse filter
// all characters should in priority comparision array
enum ObXpathFilterChar {
  CHAR_BEGIN_FILTER = 0, /* 0  [   */
  CHAR_LEFT_BRACE = 1,   /* 1  (   */
  CHAR_UNION = 2,        /* 2  |   */  // equal to ObFilterType::PN_CMP_UNION
  CHAR_OR,               /* 3  or  */
  CHAR_AND,              /* 4  and */
  CHAR_EQUAL,            /* 5  =   */
  CHAR_UNEQUAL,          /* 6  !=  */
  CHAR_LESS_EQUAL,       /* 7  <=  */
  CHAR_LESS,             /* 8  <   */
  CHAR_GREAT_EQUAL,      /* 9  >=  */
  CHAR_GREAT,            /* 10 >   */
  CHAR_ADD,              /* 11 +   */
  CHAR_SUB,              /* 12 -   */
  CHAR_MULTI,            /* 13 *   */
  CHAR_DIV,              /* 14 div */
  CHAR_MOD,              /* 15 mod */
  CHAR_RIGHT_BRACE,      /* 16 )   */
  CHAR_END_FILTER,       /* 17 ]   */
  CMP_CHAR_MAX
};

enum ObPathArgType {
  NOT_SUBPATH = 0,
  IN_FILTER = 1,
  IN_FUNCTION = 2
};

enum ObFilterOpAns {
  NOT_FILTERED = 0,
  FILTERED_TRUE = 1,
  FILTERED_FALSE = 2
};

class ObPathNode;
class ObPathArgNode;
class ObSeekResult;
class ObNodeTypeAndContent;
class ObSeekIterator;
class ObSeekComplexIterator;
class ObSeekAncestorIterator;
class ObXmlPathFilter;

typedef struct ObPathStr
{
  const char* name_;
  uint64_t   len_;
} ObPathStr;

typedef union ObArgNodeContent
{
  ObPathStr str_;
  ObPathNode* subpath_;
  double double_;
  bool boolean_;
} ObArgNodeContent;

class ObNodeTypeAndContent
{
public:
  ObNodeTypeAndContent() {}
  ObNodeTypeAndContent(ObArgType type) : type_(type) {}
  int64_t to_string(char *buf, const int64_t buf_len) const {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "data_type = %d", type_);
    return pos;
  }
  ObArgNodeContent *content_;
  ObArgType type_;
};
typedef common::ObArray<ObNodeTypeAndContent*> ObNodeSetVector;

typedef union ObPathResult
{
  ObIMulModeBase* base_;
  ObPathArgNode* scalar_;
} ObPathResult;

typedef ObJsonPathUtil ObXPathUtil;

using ObPathVectorArena = PageArena<ObPathNode*, ModulePageAllocator>;
using ObPathVectorPointers = ObVector<ObPathNode*, ObPathVectorArena>;
using ObPathVecotorIterator = ObPathVectorPointers::const_iterator;
using ObFilterCharVectorArena = PageArena<ObXpathFilterChar, ModulePageAllocator>;
using ObFilterCharPointers = ObVector<ObXpathFilterChar, ObFilterCharVectorArena>;

typedef PageArena<ObSeekResult, ModulePageAllocator> SeekVectorModuleArena;
typedef common::ObVector<ObSeekResult, SeekVectorModuleArena> ObSeekVector;
typedef PageArena<ObIMulModeBase*, ModulePageAllocator> ModeBaseModuleArena;
typedef common::ObSortedVector<ObIMulModeBase*, ModeBaseModuleArena> ObIBaseSortedVector;
typedef PageArena<ObSeekIterator*, ModulePageAllocator> SeekIterModuleArena;
typedef common::ObVector<ObSeekIterator*, SeekIterModuleArena> ObSeekIterVector;

class ObLocationNodeContent
{
public:
  ObPathStr key_;
  ObPathStr namespace_;
  bool has_prefix_ns_ = false;
  bool is_default_prefix_ns_ = false;
  bool has_wildcard_ = false;
};

class ObSeekResult
{
public:
  ObSeekResult() : is_scalar_(false) {}
  ObSeekResult(bool is_scalar) : is_scalar_(is_scalar) {}
  ObSeekResult(const ObSeekResult& from) : result_(from.result_), is_scalar_(from.is_scalar_) {}
  int64_t to_string(char *buf, const int64_t buf_len) const {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "data_type = %d", is_scalar_);
    return pos;
  }
  ObPathResult result_;
  bool is_scalar_;
};

class ObPathNodeType {
public:
  ObPathNodeType () :
    path_type_(PARSER_ERROR), node_class_(PN_ERROR), reserved_(0) {}
  ObPathNodeType (const ObParserType& path_type) :
    path_type_(path_type), node_class_(PN_ERROR), reserved_(0) {}
  ObPathNodeType (const ObParserType& path_type, const ObPathNodeClass& node_class) :
    path_type_(path_type), node_class_(node_class), reserved_(0) {}
  ObPathNodeType (const ObParserType& path_type, const ObPathNodeClass& node_class, const ObLocationType& name) :
    path_type_(path_type), node_class_(node_class), node_subtype_(name), reserved_(0) {}
  ObPathNodeType (const ObParserType& path_type, const ObPathNodeClass& node_class, const ObFilterType& name) :
    path_type_(path_type), node_class_(node_class), node_subtype_(name), reserved_(0) {}
  ObPathNodeType (const ObParserType& path_type, const ObPathNodeClass& node_class, const ObFuncType& name) :
    path_type_(path_type), node_class_(node_class), node_subtype_(name), reserved_(0) {}
  OB_INLINE void set_location_type (const ObLocationType& name) {node_subtype_ = name;}
  OB_INLINE void set_filter_type (const ObFilterType& name) {node_subtype_ = name;}
  OB_INLINE void set_func_type (const ObFuncType& name) {node_subtype_ = name;}
  OB_INLINE void set_arg_type (const ObArgType& name) {node_subtype_ = name;}

  OB_INLINE bool is_strict_json_path () {return path_type_ == ObParserType::PARSER_JSON_PATH_STRICT;}
  OB_INLINE bool is_lax_json_path () {return path_type_ == ObParserType::PARSER_JSON_PATH_LAX;}
  OB_INLINE bool is_xml_path () {return path_type_ == ObParserType::PARSER_XML_PATH;}
  OB_INLINE bool is_root () {return node_class_ == PN_ROOT;}
  OB_INLINE bool is_location () {return node_class_ == PN_LOCATION;}
  OB_INLINE bool is_filter () {return node_class_ == PN_FILTER;}
  OB_INLINE bool is_location_filter () {return node_class_ == PN_LOCATION_FILTER;}
  OB_INLINE bool is_func () {return node_class_ == PN_FUNCTION;}
  OB_INLINE bool is_arg () {return node_class_ == PN_ARG;}

  OB_INLINE ObParserType get_path_type () {return (ObParserType)path_type_;}
  OB_INLINE ObLocationType get_location_type () {return (ObLocationType)node_subtype_;}
  OB_INLINE ObFilterType get_filter_type () {return (ObFilterType)node_subtype_;}
  OB_INLINE ObFuncType get_func_type () {return (ObFuncType)node_subtype_;}
  OB_INLINE ObArgType get_arg_type () {return (ObArgType)node_subtype_;}

  uint32_t path_type_ : 3;
  uint32_t node_class_ : 5;
  uint32_t node_subtype_ : 12;
  uint32_t reserved_ : 12;
};

class ObPathVarPair final
{
public:
  ObPathVarPair() : key_(nullptr), value_(nullptr) {}
  explicit ObPathVarPair(const ObString &key, ObDatum *value)
      : key_(key),
        value_(value)
  {
  }
  ~ObPathVarPair() {}
  OB_INLINE common::ObString get_key() const { return key_; }
  OB_INLINE void set_xml_key(const common::ObString &new_key)
  {
    key_.assign_ptr(new_key.ptr(), new_key.length());
  }
  OB_INLINE void set_value(ObDatum *value) { value_ = value; }
  OB_INLINE ObDatum *get_value() const { return value_; }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "key = %s", key_.ptr());
    return pos;
  }
private:
  common::ObString key_;
  ObDatum *value_;
};

using ObPathPairArena = PageArena<ObPathVarPair, ModulePageAllocator>;
typedef common::ObVector<ObPathVarPair, ObPathPairArena> ObPathVarArray;
static const int64_t PATH_DEFAULT_PAGE_SIZE = (1LL << 10); // 1k

struct ObPathKeyCompare {
  int operator()(const ObPathVarPair &left, const ObPathVarPair &right)
  {
    INIT_SUCC(ret);
    common::ObString left_key = left.get_key();
    common::ObString right_key = right.get_key();
    // first compare length
    if (left_key.length() != right_key.length()) {
      ret = (left_key.length() < right_key.length());
    } else { // do Lexicographic order when length equals
      ret = (left_key.compare(right_key) < 0);
    }
    return ret;
  }

  int compare(const ObString &left, const ObString &right)
  {
    int result = 0;
    // first compare length
    if (left.length() != right.length()) {
      result = left.length() - right.length();
    } else { // do Lexicographic order when length equals
      result = left.compare(right);
    }
    return result;
  }
};

class ObPathVarObject
{
  public:
    ObPathVarObject(ObIAllocator &allocator)
      : page_allocator_(allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
        pair_arena_(PATH_DEFAULT_PAGE_SIZE, page_allocator_),
        object_array_(&pair_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR)
    {}
    ~ObPathVarObject() {}
    OB_INLINE void reset() { object_array_.clear(); }
    OB_INLINE uint64_t element_count() const { return object_array_.size(); }
    int add(const common::ObString &key, ObDatum *value, bool with_unique_key = false);
    ObDatum *get_value(const common::ObString &key) const;
  private:
    ModulePageAllocator page_allocator_;
    ObPathPairArena pair_arena_;
    ObPathVarArray object_array_;
};

class ObPathCtx
{
public:
  friend class ObPathNode;
  ObPathCtx (common::ObIAllocator *alloc) : alloc_(alloc), ancestor_record_(alloc), is_inited_(0) {}
  ObPathCtx(ObMulModeMemCtx* ctx) :
    ctx_(ctx), alloc_(ctx->allocator_), doc_root_(nullptr), cur_doc_(nullptr), cur_doc_position_(-1),
    ancestor_record_(ctx->allocator_), is_inited_(0), is_auto_wrap_(0),
    check_duplicate_(1), need_boolean_(0), need_record_(0), reserved_(0) {}
  ~ObPathCtx() {bin_pool_.reset();}
  int init(ObMulModeMemCtx* ctx, ObIMulModeBase *doc_root, ObIMulModeBase *cur_doc,
           ObIAllocator *tmp_alloc, bool is_auto_wrap, bool need_record, bool add_ns);
  int init_extend();
  int push_ancestor(ObIMulModeBase*& base_node);
  int alloc_new_bin(ObIMulModeBase*& base_node);
  int alloc_new_bin(ObXmlBin*& base_node, ObMulModeMemCtx* ctx);
  ObIMulModeBase* top_ancestor();
  int pop_ancestor();
  OB_INLINE void set_cur_doc_to_root() {cur_doc_ = doc_root_;}
  bool if_need_record() const;
  bool is_inited() const;
  int record_if_need();
  void reset() {ancestor_record_.reset(); bin_pool_.reset();}
  int reinit(ObIMulModeBase* doc, ObIAllocator *tmp_alloc);
  common::ObIAllocator* get_tmp_alloc() { return tmp_alloc_; }
  ObMulModeMemCtx* ctx_;
  common::ObIAllocator *alloc_;
  common::ObIAllocator *tmp_alloc_;
  ObIMulModeBase *doc_root_;
  ObIMulModeBase *cur_doc_;
  ObIMulModeBase* extend_;
  ObPathPool bin_pool_;
  int cur_doc_position_;
  ObStack<ObIMulModeBase*> ancestor_record_;
  int defined_ns_;
  union {
    struct {
      uint16_t is_inited_ : 1;
      uint16_t is_auto_wrap_ : 1;
      uint16_t check_duplicate_ : 1;
      uint16_t need_boolean_ : 1;
      uint16_t need_record_ : 1;
      uint16_t add_ns_ : 1;
      uint16_t reserved_ : 10;
    };

    uint16_t flags_;
  };
};

class ObPathNode : public ObLibContainerNode
{
public:
  ObPathNode(ObMulModeMemCtx* ctx, const ObParserType& parser_type) :
    ObLibContainerNode(OB_PATH_TYPE, MEMBER_SEQUENT_FLAG, ctx),
    need_cache_(false), contain_relative_path_(false) { node_type_.path_type_ = parser_type; node_type_.node_class_ = ObPathNodeClass::PN_ERROR;}
  ObPathNode(ObMulModeMemCtx* ctx, const ObParserType& parser_type, const ObPathNodeClass& node_class) :
    ObLibContainerNode(OB_PATH_TYPE, MEMBER_SEQUENT_FLAG, ctx),
    need_cache_(false), contain_relative_path_(false) { node_type_.path_type_ = parser_type; node_type_.node_class_ = node_class;} // root
  virtual ~ObPathNode() {}
  virtual ObPathNodeType get_node_type() {return node_type_;}
  virtual int eval_node(ObPathCtx &ctx, ObSeekResult& res);
  virtual int node_to_string(ObStringBuffer& str);
  ObPathNodeType node_type_;
  bool need_cache_;
  bool contain_relative_path_;
  bool is_seeked_ = false;
};

class ObPathRootNode : public ObPathNode
{
public:
  ObPathRootNode(ObMulModeMemCtx* ctx, const ObParserType& parser_type) :
    ObPathNode(ctx, parser_type, ObPathNodeClass::PN_ROOT),
    arena_(PATH_DEFAULT_PAGE_SIZE, ctx->page_allocator_),
    adapt_(&arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
    need_trans_(0), iter_pos_(-1), is_abs_path_(true) {}
  virtual ~ObPathRootNode() {}
  int reuse(ObPathCtx &ctx, ObIMulModeBase*& ans);
  int next_adapt(ObPathCtx &ctx, ObIMulModeBase*& ans);
  int init_adapt(ObPathCtx &ctx, ObIMulModeBase*& ans);
  virtual int node_to_string(ObStringBuffer& str);
  virtual int eval_node(ObPathCtx &ctx, ObSeekResult& res);
  bool is_abs_subpath();
  SeekIterModuleArena arena_;
  ObSeekIterVector adapt_;
  int need_trans_;
  int iter_pos_;
  bool is_abs_path_;
};

class ObPathLocationNode : public ObPathNode
{
public:
  ObPathLocationNode(ObMulModeMemCtx* ctx, const ObParserType& parser_type) :
    ObPathNode(ctx, parser_type, ObPathNodeClass::PN_LOCATION), is_absolute_(false),
     has_filter_(false), check_namespace_(false),
    seek_type_(ObSeekType::ERROR_SEEK), node_axis_(ObPathNodeAxis::ERROR_AXIS) {}
  int init(const ObLocationType& location_type);
  int init(const ObLocationType& location_type, const ObSeekType& seek_type);
  int init(const ObLocationType& location_type, const ObPathNodeAxis& axis_type);
  int init(const ObLocationType& location_type, const ObSeekType& seek_type, const ObPathNodeAxis& axis_type);
  virtual ~ObPathLocationNode() {}
  OB_INLINE void set_axis(ObPathNodeAxis axis) {node_axis_ = axis;}
  OB_INLINE void set_nodetest(ObSeekType seek_type) {seek_type_ = seek_type;}
  OB_INLINE void set_wildcard_info(bool wildcard) {node_content_.has_wildcard_ = wildcard;}
  OB_INLINE void set_prefix_ns_info(bool has_prefix_ns) {node_content_.has_prefix_ns_ = has_prefix_ns;}
  OB_INLINE void set_default_prefix_ns(bool default_prefix_ns) {node_content_.is_default_prefix_ns_ = default_prefix_ns;}
  OB_INLINE void set_check_ns_info(bool check_ns) {check_namespace_ = check_ns;}
  OB_INLINE ObSeekType get_seek_type() {return seek_type_;}
  OB_INLINE ObPathNodeAxis get_axis() {return node_axis_;}
  OB_INLINE bool get_check_ns() {return check_namespace_;}
  OB_INLINE ObString get_key_name() {return ObString(node_content_.key_.len_, node_content_.key_.name_);}
  OB_INLINE ObString get_ns_name() {return ObString(node_content_.namespace_.len_, node_content_.namespace_.name_);}
  OB_INLINE bool is_key_null() {return node_content_.key_.name_ == nullptr;}
  OB_INLINE bool get_wildcard_info() {return node_content_.has_wildcard_;}
  OB_INLINE bool get_prefix_ns_info() {return node_content_.has_prefix_ns_;}
  OB_INLINE bool get_default_prefix_ns_info() {return node_content_.is_default_prefix_ns_;}
  OB_INLINE bool has_filter() {return has_filter_;}
  OB_INLINE bool is_ancestor_axis() {return node_axis_ == ANCESTOR || node_axis_ == ANCESTOR_OR_SELF;}
  OB_INLINE bool is_complex_seektype() {return  node_type_.get_location_type() == PN_ELLIPSIS || node_axis_ == DESCENDANT || node_axis_ == DESCENDANT_OR_SELF;}
  OB_INLINE void set_ns_info(const char* name, uint64_t len) {node_content_.namespace_.name_ = name; node_content_.namespace_.len_ = len;}
  OB_INLINE void set_key_info(const char* name, uint64_t len) {node_content_.key_.name_ = name;node_content_.key_.len_ = len;}
  void set_nodetest_by_name(ObSeekType seek_type, const char* name, uint64_t len);
  void set_nodetest_by_axis();
  int set_seek_info(ObPathSeekInfo& seek_info);
  int set_check_ns_by_nodetest(ObIAllocator *allocator, ObString& default_ns);
  // Get first_index according to from_end
  virtual int node_to_string(ObStringBuffer& str);
  virtual int eval_node(ObPathCtx &ctx, ObSeekResult& res);
  int filter_location_res(ObPathCtx &ctx, ObIBaseSortedVector &dup, ObSeekVector &res);
  int axis_to_string(ObStringBuffer& str);
  int nodetest_to_string(ObStringBuffer& str);
  int get_valid_attribute(ObIMulModeBase *doc, ObArray<ObIMulModeBase*> &hit);
  int get_valid_namespace(ObIMulModeBase *doc, ObArray<ObIMulModeBase*> &hit);
  bool is_filter_nodetest();
  bool is_absolute_;
  bool has_filter_;
  private:
  bool check_namespace_;
  ObSeekType seek_type_;
  ObPathNodeAxis node_axis_;
  ObLocationNodeContent node_content_;
};

class ObPathFilterNode : public ObPathNode
{
public:
  ObPathFilterNode(ObMulModeMemCtx* ctx, const ObParserType& parser_type) :
    ObPathNode(ctx, parser_type, ObPathNodeClass::PN_FILTER), ans_(NOT_FILTERED),
    in_predication_(false), is_boolean_(false) {}
  ObPathFilterNode(ObMulModeMemCtx* ctx, const ObParserType& parser_type, bool in_predication) :
    ObPathNode(ctx, parser_type, ObPathNodeClass::PN_FILTER) ,ans_(NOT_FILTERED),
    in_predication_(in_predication), is_boolean_(false) {}
  virtual ~ObPathFilterNode() {}
  int init(const ObXpathFilterChar& filter_char, ObPathNode* left, ObPathNode* right, bool pred);
  int init(ObFilterType type);
  virtual int node_to_string(ObStringBuffer& str);
  int filter_arg_to_string(ObStringBuffer& str, bool is_left);
  int filter_type_to_string(ObStringBuffer& str);
  virtual int eval_node(ObPathCtx &ctx, ObSeekResult& res);
  ObSeekResult ans_;
  bool in_predication_;
  bool is_boolean_;
  bool filtered_ = false;
};

// complex filter, a collection of several filter operators
// left arg is location before filter operators
// right arg is location after filter operators
class ObPathFilterOpNode : public ObPathNode
{
public:
  ObPathFilterOpNode(ObMulModeMemCtx* ctx, const ObParserType& parser_type) :
    ObPathNode(ctx, parser_type, ObPathNodeClass::PN_LOCATION_FILTER),
    ans_(NOT_FILTERED), left_(nullptr), right_(nullptr) {}
  virtual ~ObPathFilterOpNode() {}
  virtual int node_to_string(ObStringBuffer& str);
  virtual int eval_node(ObPathCtx &ctx, ObSeekResult& res);
  int append_filter(ObPathNode* filter);
  void init_left(ObPathNode* left) {left_ = left;}
  void init_right(ObPathNode* right) {right_ = right;}
  int filter_op_arg_to_str(bool is_left, ObStringBuffer& str);
  int get_filter_ans(ObFilterOpAns& ans, ObPathCtx& filter_ctx);
  int get_valid_res(ObPathCtx &ctx, ObSeekResult& res, bool is_left);
  int init_right_without_filter(ObPathCtx &ctx, ObSeekResult& res);
  int init_right_with_filter(ObPathCtx &ctx, ObSeekResult& res);
  ObFilterOpAns ans_;
  ObPathNode* left_;
  ObPathNode* right_;
};

class ObPathFuncNode : public ObPathNode
{
public:
  ObPathFuncNode(ObMulModeMemCtx* ctx, const ObParserType& parser_type) :
    ObPathNode(ctx, parser_type, ObPathNodeClass::PN_FUNCTION),
    max_arg_num_(-1), min_arg_num_(0), ans_(nullptr) {}
  virtual ~ObPathFuncNode() {}
  int init(ObFuncType& func_type);
  virtual int node_to_string(ObStringBuffer& str);
  int func_arg_to_string(ObStringBuffer& str);
  virtual int eval_node(ObPathCtx &ctx, ObSeekResult& res);
  int check_is_all_location_without_filter(ObPathNode* arg_root);
  int check_is_legal_arg();
  int check_is_legal_count_arg();
  int checek_cache_and_abs();
  OB_INLINE int get_max_arg_num() {return max_arg_num_;}
  OB_INLINE int get_min_arg_num() {return min_arg_num_;}
  // Get first_index according to from_end
private:
  int eval_position_or_last(ObPathCtx &ctx, bool is_last,  ObSeekResult& res);
  int eval_count(ObPathCtx &ctx, ObSeekResult&res);
  int eval_true_or_false(ObPathCtx &ctx, bool is_true, ObSeekResult& res);
  int max_arg_num_;
  int min_arg_num_;
  ObPathArgNode* ans_;
};

class ObPathArgNode : public ObPathNode
{
public:
  ObPathArgNode(ObMulModeMemCtx* ctx, const ObParserType& parser_type) :
    ObPathNode(ctx, parser_type, ObPathNodeClass::PN_ARG) {}
  virtual ~ObPathArgNode() {}
  int init(char* str, uint64_t len, bool pred);
  int init(double num, bool pred);
  int init(bool boolean, bool pred);
  int init(ObPathNode* node, bool pred);
  virtual int node_to_string(ObStringBuffer& str);
  virtual int eval_node(ObPathCtx &ctx, ObSeekResult& res);
  ObArgNodeContent arg_;
  bool in_predication_;
  bool is_seeked_ = false;
};

class ObPathUtil
{
public:
  static bool is_filter_nodetest(const ObSeekType& seek_type);
  static bool is_upper_axis(const ObPathNodeAxis& axis);
  static bool is_down_axis(const ObPathNodeAxis& axis);
  static bool include_self_axis(const ObPathNodeAxis& axis);
  static bool check_contain_relative_path(ObPathNode* path);
  static bool check_need_cache(ObPathNode* path);
  static int add_dup_if_missing(ObIAllocator *allocator, ObIMulModeBase*& path_res, ObIBaseSortedVector &dup, bool& end_seek);
  static int add_scalar(ObIAllocator *allocator, ObPathArgNode* arg, ObSeekVector &res);
  static int get_parser_type(ObIMulModeBase *doc, ObParserType& parser_type);
  static int char_to_filter_type(const ObXpathFilterChar& ch, ObFilterType& type);
  static int pop_char_stack(ObFilterCharPointers& char_stack);
  static int pop_node_stack(ObPathVectorPointers& node_stack, ObPathNode*& top_node);
  static int alloc_seek_result(common::ObIAllocator *allocator, ObIMulModeBase* base, ObSeekResult*& result);
  static int alloc_seek_result(ObIAllocator *allocator, ObPathArgNode* arg, ObSeekResult*& res);
  static int alloc_num_arg(ObMulModeMemCtx *ctx, ObPathArgNode*& arg, ObParserType parser_type, double num);
  static int alloc_boolean_arg(ObMulModeMemCtx *ctx, ObPathArgNode*& arg, ObParserType parser_type, bool ans);
  static int trans_scalar_to_base(ObIAllocator *allocator, ObPathArgNode* arg, ObIMulModeBase*& base);
  static int filter_compare(ObPathCtx &ctx,
                            ObNodeSetVector &left, ObArgType left_type,
                            ObNodeSetVector &right, ObArgType right_type,
                            ObFilterType op, ObSeekResult& res);
  static int filter_calculate(ObPathCtx &ctx,
                            ObNodeSetVector &left, ObArgType left_type,
                            ObNodeSetVector &right, ObArgType right_type,
                            ObFilterType op, ObSeekVector &res);
  static int logic_compare_rule(ObPathCtx &ctx, ObPathNode *path_node, bool &ret_bool);
  static int filter_logic_compare(ObPathCtx &ctx, ObPathNode* left_node, ObPathNode* right_node, ObFilterType op, ObSeekResult &res);
  static int filter_union(ObPathCtx &ctx, ObPathNode* left_node, ObPathNode* right_node, ObFilterType op, ObSeekVector &res);
  static int filter_single_node(ObPathCtx &ctx, ObPathNode* filter_node, ObSeekVector &res);
  static int get_seek_vec(ObPathCtx &ctx, ObPathNode *from_node, ObSeekVector &res);
  static int get_filter_node_result(ObPathCtx &ctx, ObLibTreeNodeBase* filter_node_base, ObPathNode* &res);
  static int seek_res_to_boolean(ObSeekResult& filter, bool &res);
  static int get_seek_iterator(common::ObIAllocator *allocator, ObPathLocationNode* loc, ObSeekIterator*& ada);
  static int alloc_binary(common::ObIAllocator *allocator, ObXmlBin*& ada);
  static int alloc_iterator(common::ObIAllocator *allocator, ObSeekIterator*& ada);
  static int alloc_complex_iterator(common::ObIAllocator *allocator, ObSeekComplexIterator*& ada);
  static int alloc_ancestor_iterator(common::ObIAllocator *allocator, ObSeekAncestorIterator*& ada);
  static int alloc_path_node(common::ObIAllocator *allocator, ObPathNode*& node);
  static int alloc_node_content_info(common::ObIAllocator *allocator, ObArgNodeContent *content, ObArgType type, ObNodeTypeAndContent *&res);
  static int alloc_node_content_info(common::ObIAllocator *allocator, ObString *str, ObNodeTypeAndContent *&res);
  static int alloc_node_set_vector(ObPathCtx &ctx, ObPathNode *path_node, ObArgType& arg_type, ObNodeSetVector &node_vec);
  static int get_arg_type(ObArgType& arg_type, ObPathNode *path_node);
  static int release_seek_vector(ObPathCtx &ctx, ObSeekVector& seek_vector);
  static int add_ns_if_need(ObPathCtx &ctx, ObIMulModeBase*& res);
  static int collect_ancestor_ns(ObIMulModeBase* extend, ObStack<ObIMulModeBase*> &ancestor_record, ObXmlElement::NsMap &ns_map, ObArray<ObXmlAttribute*> &ns_vec, common::ObIAllocator* tmp_alloc);
};

class ObXmlPathFilter : public ObMulModeFilter
{
public:
  ObXmlPathFilter() {}
  ObXmlPathFilter(ObPathLocationNode* path, ObPathCtx* path_ctx) : path_(path), path_ctx_(path_ctx) {}
  ObXmlPathFilter(const ObXmlPathFilter& from) : path_(from.path_), path_ctx_(from.path_ctx_){}
  ~ObXmlPathFilter() {}
  int operator()(ObIMulModeBase* doc, bool& filtered);
  ObPathLocationNode* path_;
  ObPathCtx* path_ctx_;
};


// basic Iterator, used for following axis: child, parent, attribute, namespace
class ObSeekIterator
{
public:
  ObSeekIterator() : iter_(nullptr), is_seeked_(false) {}
  ObSeekIterator(const ObSeekIterator& src) : ada_root_(src.ada_root_),
    seek_info_(src.seek_info_), axis_(src.axis_), iter_(src.iter_), is_seeked_(src.is_seeked_) {}
  virtual ~ObSeekIterator() {}
  int init(ObPathCtx &ctx, ObPathLocationNode* location, ObIMulModeBase* ada_root);
	virtual int next(ObPathCtx &ctx, ObIMulModeBase*& res);
  virtual void reset() {is_seeked_ = false;}
	void reset(ObIMulModeBase* new_ada_root);
  void set_root(ObIMulModeBase* new_ada_root);
	int close();
protected:
  int next_child(ObPathCtx &ctx, ObIMulModeBase*& res);
  int next_self(ObPathCtx &ctx, ObIMulModeBase*& res);
  int next_parent(ObPathCtx &ctx, ObIMulModeBase*& res);
  int next_attribute(ObPathCtx &ctx, ObIMulModeBase*& res);
  int next_namespace(ObPathCtx &ctx, ObIMulModeBase*& res);
  int filter_ans(ObIMulModeBase* ans, bool& filtered);
  ObIMulModeBase* ada_root_;       // root node
	ObPathSeekInfo seek_info_;       // node filter for reaser
	ObPathNodeAxis axis_;            // axis info
	ObMulModeReader iter_;           // iter
	bool is_seeked_;
};

// complex Iterator, used for following axis: descendant, descendant-or-self and '//'
class ObSeekComplexIterator : public ObSeekIterator
{
public:
  ObSeekComplexIterator (ObIAllocator *alloc): ObSeekIterator() , iter_stack_(alloc) {}

  ObSeekComplexIterator(const ObSeekComplexIterator& src) :
    ObSeekIterator(src), iter_stack_(src.iter_stack_) {}
  ~ObSeekComplexIterator() {}
  virtual int next(ObPathCtx &ctx, ObIMulModeBase*& res);
  virtual void reset() {is_seeked_ = false; iter_stack_.reset();}
protected:
  int next_descendant(ObPathCtx &ctx, bool include_self, ObIMulModeBase*& res);
  int ellipsis_inner_next(ObPathCtx &ctx, ObIMulModeBase*& res);
  ObStack<ObMulModeReader> iter_stack_;
};

// Ancestor Iterator, used for following axis: ancestor, ancestor-or-self
class ObSeekAncestorIterator : public ObSeekIterator
{
public:
  ObSeekAncestorIterator (ObIAllocator *alloc): ObSeekIterator() , anc_stack_(alloc) {}

  ObSeekAncestorIterator(const ObSeekAncestorIterator& src) :
    ObSeekIterator(src), anc_stack_(src.anc_stack_) {}
  ~ObSeekAncestorIterator() {}
  virtual int next(ObPathCtx &ctx, ObIMulModeBase*& res);
  virtual void reset() {is_seeked_ = false; anc_stack_.reset();}
protected:
  int next_ancestor(ObPathCtx &ctx, bool include_self, ObIMulModeBase*& res);
  int ancestor_inner_next(ObPathCtx &ctx, ObIMulModeBase*& res);
  int anc_stack_push(ObPathCtx &ctx, ObIMulModeBase* push_node);
  void anc_stack_pop(ObPathCtx &ctx);
  ObStack<ObIMulModeBase*> anc_stack_;
};

static const int64_t DEFAULT_DUP_SIZE = 8;
class ObPathExprIter
{
public:
  ObPathExprIter(ObIAllocator *allocator, ObIAllocator *tmp_allocator = nullptr)
  : page_allocator_(*allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
    mode_arena_(PATH_DEFAULT_PAGE_SIZE, page_allocator_),
    path_ctx_(allocator),
    dup_(DEFAULT_DUP_SIZE, &mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
    is_inited_(0), need_record_(0), is_open_(0), add_ns_(0), reserved_(0)
  {
    tmp_allocator_ = (tmp_allocator == nullptr) ? allocator : tmp_allocator;
  }
  ~ObPathExprIter() {close();}
  int init(ObMulModeMemCtx* ctx, ObString& path, ObString& default_ns, ObIMulModeBase* doc, ObPathVarObject* pass_var, bool add_namespace = true);
  int open(); // begin to parse and seek
  int get_next_node(ObIMulModeBase*& res);

  int get_first_node(ObPathNode*& loc);
  int get_first_axis(ObPathNodeAxis& first_axis);
  int get_first_seektype(ObSeekType& first_seektype);
  ObIMulModeBase* get_cur_res_parent();
  bool is_first_init() { return !is_open_; }
  bool get_add_ns() {return add_ns_;}
  void set_add_ns(bool add_ns);
  int set_tmp_alloc(ObIAllocator *tmp_allocator);
  ObString& get_path_str() { return path_; }
  int close();
  int reset();
  int reset(ObIMulModeBase* doc, ObIAllocator *tmp_allocator);
private:
  common::ObIAllocator *allocator_;
  common::ObIAllocator *tmp_allocator_;
  ModulePageAllocator page_allocator_;
  ModeBaseModuleArena mode_arena_;
  ObMulModeMemCtx* ctx_;
  ObPathCtx path_ctx_;
  ObIMulModeBase* doc_;
  ObPathVarObject* pass_var_;
  ObPathNode* path_node_;
  ObString path_;
  ObString default_ns_;
  ObIBaseSortedVector dup_;
  uint8_t is_inited_ : 1;
  uint8_t need_record_ : 1;
  uint8_t is_open_ : 1;
  uint8_t add_ns_ : 1;
  uint8_t reserved_ : 4;
};

}
}
#endif  // OCEANBASE_SQL_OB_XPATH