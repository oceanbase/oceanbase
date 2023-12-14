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
 * This file contains interface support for the json tree abstraction.
 */

#ifndef OCEANBASE_SQL_OB_JSON_SCHEMA
#define OCEANBASE_SQL_OB_JSON_SCHEMA
#include "lib/xml/ob_multi_mode_interface.h"
#include "ob_json_base.h"
#include "ob_json_tree.h"

namespace oceanbase {
namespace common {

enum ObJsonSchemaAns {
  JS_NOT_CHCECKED = 0,
  JS_TRUE = 1,
  JS_FALSE = 2
};

enum ObJsonSchemaComp {
  JS_COMP_DEP = 0,
  JS_COMP_ALLOF = 1,
  JS_COMP_ONEOF = 2,
  JS_COMP_ANYOF = 3,
  JS_COMP_NOT = 4,
  JS_COMP_MAX = 5
};

// use uint64, cause ObJsonUint is uint64
typedef union ObJsonSchemaType {
  struct {
    uint64_t null_ : 1;
    uint64_t boolean_ : 1;
    uint64_t string_ : 1;
    uint64_t number_ : 1;
    uint64_t integer_ : 1;
    uint64_t object_ : 1;
    uint64_t array_ : 1;
    uint64_t error_type_ : 1;
    uint64_t reserved_ : 56;
  };

  uint64_t flags_;
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "falgs_ = %ld", flags_);
    return pos;
  }
} ObJsonSchemaType;

typedef union ObJsonSubSchemaKeywords {
  struct {
    uint16_t dep_schema_ : 1;
    uint16_t all_of_ : 1;
    uint16_t one_of_ : 1;
    uint16_t any_of_ : 1;
    uint16_t not_ : 1;
    uint16_t properties_ : 1;
    uint16_t pattern_pro_ : 1;
    uint16_t additional_pro_ : 1;
    uint16_t items_ : 1;
    uint16_t tuple_items_ : 1;
    uint16_t additional_items_ : 1;
    uint16_t reserved_ : 5;
  };

  uint16_t flags_;
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "falgs_ = %d", flags_);
    return pos;
  }
} ObJsonSubSchemaKeywords;

struct ObJsonContentCmp {
  bool operator()(const ObIJsonBase *a, const ObIJsonBase *b) {
    bool is_smaller = false;
    ObJsonNodeType a_type = a->json_type();
    ObJsonNodeType b_type = b->json_type();
    if (a_type == b_type) {
      int res = 0;
      a->compare(*b, res);
      is_smaller = (res < 0);
    } else {
      is_smaller = (a->json_type() < b->json_type());
    }
    return is_smaller;
  }
};

struct ObStringCmp {
  int operator()(const ObString a, const ObString b) {
    return a < b;
  }
};

struct ObStringUnique {
  int operator()(const ObString a, const ObString b) {
    return a == b;
  }
};

struct ObJsonContentUnique {
  bool operator()(const ObIJsonBase *a, const ObIJsonBase *b) {
    bool is_eq = false;
    ObJsonNodeType a_type = a->json_type();
    ObJsonNodeType b_type = b->json_type();
    if (a_type == b_type) {
      int res = 0;
      a->compare(*b, res);
      is_eq = (res == 0);
    } else {
      is_eq = false;
    }
    return is_eq;
  }
};

static const int64_t SCHEMA_DEFAULT_PAGE_SIZE = (1LL << 10); // 1k

class ObJsonSchemaTree
{
public:
  explicit ObJsonSchemaTree(ObIAllocator *allocator)
      : allocator_(allocator),
        root_doc_(nullptr),
        ref_(nullptr),
        schema_map_(nullptr),
        cur_schema_stk_(allocator_),
        typeless_(nullptr),
        str_buf_(allocator),
        serial_num_(0),
        json_ptr_("#")
  {
  }
  explicit ObJsonSchemaTree(ObIAllocator *allocator, ObJsonObject *root_doc)
      : allocator_(allocator),
        root_doc_(root_doc),
        ref_(nullptr),
        schema_map_(nullptr),
        cur_schema_stk_(allocator_),
        typeless_(nullptr),
        str_buf_(allocator),
        serial_num_(0),
        json_ptr_("#")
  {
  }
  explicit ObJsonSchemaTree(ObIAllocator *allocator, ObJsonObject *root_doc, ObString json_ptr)
      : allocator_(allocator),
        root_doc_(root_doc),
        ref_(nullptr),
        schema_map_(nullptr),
        cur_schema_stk_(allocator_),
        typeless_(nullptr),
        str_buf_(allocator),
        serial_num_(0),
        json_ptr_(json_ptr)
  {
  }
  virtual ~ObJsonSchemaTree() {}
  int build_schema_tree(ObIJsonBase *json_doc);
  void set_root_doc(ObJsonObject *root_doc) {root_doc_ = root_doc;}
  OB_INLINE ObJsonArray* get_schema_map() {return schema_map_;}
private:
  int inner_build_schema_tree(ObJsonObject* origin_schema,        // origin json schema，must be object
                              bool is_composition,                // true: add schema in composition
                                                                  // false: add schema in schema
                              ObJsonArray* comp_array = nullptr); // array of composition
  int get_schema_vec(ObIArray<ObJsonNode*> &schema_vec_stk, bool is_composition);
  bool if_have_ref(ObJsonObject* origin_schema);
  int get_ref_pointer_value(const ObString ref_str, ObJsonObject*& ref_value);
  int handle_ref_keywords(ObJsonObject* origin_schema, ObIArray<ObJsonNode*> &schema_vec_stk,
                          const bool& is_composition, ObJsonArray* comp_array);
  int generate_schema_and_record(const ObString& key_word, ObJsonNode* value, ObIArray<ObJsonNode*> &schema_vec_stk,
                                const bool& is_composition, ObJsonArray* comp_array);
  int generate_schema_info(const ObString& key_word, ObJsonNode* value, ObIArray<ObJsonNode*> &schema_vec_stk);
  int union_schema_def(const ObString& key_word, ObJsonNode*& value, ObJsonNode* old_key_value, bool& update_old_key);
  int union_type(ObJsonNode*& new_value, ObJsonNode* old_value, bool& update_old_key);
  int union_array_key_words_value(ObJsonNode*& new_value, ObJsonNode* old_value, bool& update_old_key, bool get_merge = false);
  int union_scalar_key_words_value(ObJsonNode*& new_value, ObJsonNode* old_value, bool& update_old_key);
  int union_add_pro_value(ObJsonNode*& new_value, ObJsonNode* old_value);
  int generate_comp_and_record(const ObString& key_word, ObJsonNode* value,
                               ObIArray<ObJsonNode*> &schema_vec_stk,
                               ObJsonArray* comp_array);
  int handle_keywords_with_specific_type(const ObString& key_word, const ObJsonNodeType& expect_type,
                                         ObJsonObject* origin_schema, ObIArray<ObJsonNode*> &schema_vec_stk,
                                         const bool& is_composition, ObJsonArray* comp_array);
  // for keywords:maxLength, minLength, maxProperties, minProperties, maxItems, minItems
  // these keywords must be positive integer, Otherwise, ignore it and it will not take effect.
  int handle_positive_int_keywords(const ObString& key_word, ObJsonObject* origin_schema,
                                    ObIArray<ObJsonNode*> &schema_vec_stk,
                                    const bool& is_composition, ObJsonArray* comp_array);
  int handle_keywords_with_number_value(const ObString& key_word, ObJsonObject* origin_schema,
                                        ObIArray<ObJsonNode*> &schema_vec_stk,
                                        const bool& is_composition, ObJsonArray* comp_array,
                                        bool must_be_positive = false);
  int handle_keywords_with_subschemas(ObJsonSubSchemaKeywords& key_words, ObJsonObject* json_schema,
                                      ObIArray<ObJsonNode*> &schema_vec_stk,
                                      bool is_composition, ObJsonArray* comp_array);
  int handle_properties(ObJsonObject*& json_schema, bool is_composition, ObJsonArray*comp_array, ObIArray<ObJsonObject*>& pro_array);
  int handle_pattern_properties(ObJsonObject* json_schema, ObJsonObject* pro_schema, bool is_composition,
                                ObJsonArray* comp_array, const ObIArray<ObJsonObject*>& pro_array);
  int add_required_key(ObJsonNode* pro, ObJsonNode* required, ObJsonArray* pro_key_array);
  int handle_additional_properties(ObJsonSubSchemaKeywords& key_words, ObJsonObject* json_schema, bool is_composition, ObJsonArray* comp_array);
  int handle_array_schema(ObJsonObject* json_schema, bool is_composition, ObJsonArray*comp_array, bool is_additonal);
  int handle_array_tuple_schema(ObJsonObject* json_schema, bool is_composition, ObJsonArray*comp_array);
  int handle_unnested_dependencies(ObJsonObject* json_schema);

  int handle_nested_dependencies(ObJsonObject* json_schema, ObJsonArray*comp_array);
  int handle_unnested_composition(const ObString& key_word, ObJsonObject* json_schema);

  int handle_nested_composition(const ObString& key_word, ObJsonObject* json_schema, ObJsonArray*comp_array);
  int handle_unnested_not(ObJsonObject* json_schema);
  int handle_nested_not(ObJsonObject* json_schema, ObJsonArray*comp_array);
  int get_difined_type(ObJsonObject* origin_schema, ObIArray<ObJsonNode*> &schema_vec_stk,
                      ObJsonSchemaType& s_type, const bool& is_composition, ObJsonArray* comp_array);
  int get_dep_schema_if_defined(ObJsonObject* origin_schema, ObIArray<ObJsonNode*> &schema_vec_stk, ObJsonSubSchemaKeywords& key_words,
                                const bool& is_composition, ObJsonArray* comp_array);
  int check_keywords_by_type(const ObJsonSchemaType& s_type, ObJsonObject* origin_schema,
                             ObIArray<ObJsonNode*> &schema_vec_stk, ObJsonSubSchemaKeywords& key_words,
                             const bool& is_composition, ObJsonArray* comp_array);
  int check_keywords_of_string(ObJsonObject* origin_schema, ObIArray<ObJsonNode*> &schema_vec_stk,
                               const bool& is_composition, ObJsonArray* comp_array);
  int check_keywords_of_number(ObJsonObject* origin_schema, ObIArray<ObJsonNode*> &schema_vec_stk,
                              const bool& is_composition, ObJsonArray* comp_array);
  int check_keywords_of_object(ObJsonObject* origin_schema, ObIArray<ObJsonNode*> &schema_vec_stk,
                               ObJsonSubSchemaKeywords& key_words,
                               const bool& is_composition, ObJsonArray* comp_array);
  int check_keywords_of_array(ObJsonObject* origin_schema, ObIArray<ObJsonNode*> &schema_vec_stk,
                              ObJsonSubSchemaKeywords& key_words,
                              const bool& is_composition, ObJsonArray* comp_array);
  int all_move_to_key(const ObString& key_word, ObJsonObject*& json_schema);
  int add_pattern_pro_to_schema(ObJsonObject* pro_schema, const ObIArray<ObJsonObject*>& pro_array, const ObString& key);
  int json_schema_move_to_key(const ObString& key_word);
  int json_schema_move_to_array(const ObString& key_word, ObJsonArray* array_val);
  int json_schema_add_comp_value(const ObString& key, ObJsonArray* new_array_val);
  int json_schema_add_dep_value(ObJsonObject* dep_val);
  int json_schema_back_to_parent();
  int json_schema_back_to_grandpa();
  int get_addition_pro_value(const ObJsonSubSchemaKeywords& key_words, ObJsonObject* origin_schema, ObJsonArray*& add_array, bool check_pattern = true);
public:
  static const int DEFAULT_PREVIOUS_NUMBER = -1;
  static const int ADDITIONAL_PRO_ARRAY_COUNT = 2;
private:
  ObIAllocator *allocator_;
  ObJsonObject *root_doc_;                 // record root of original json doc
  ObJsonObject *ref_;                      // record ref definition
  ObJsonArray* schema_map_;                // array of schema_map, index 0: schema_root_, index i: schema of number i
  ObStack<ObJsonObject*> cur_schema_stk_;  // value of cur_root, including infomations like: schema, compositon, properties, items, allOf...
  ObJsonInt*   typeless_;                  // used for copy schema
  ObJsonBuffer str_buf_;
  int serial_num_;
  ObString json_ptr_;                      // cur schema ptr, default '#'
};

class ObJsonSchemaValidator
{
public:
  explicit ObJsonSchemaValidator(ObIAllocator *allocator, ObIJsonBase *schema_map)
      : allocator_(allocator),
        schema_map_(schema_map),
        failed_keyword_(ObString::make_empty_string()),
        json_pointer_(allocator_),
        schema_pointer_(allocator_),
        str_buf_(allocator),
        composition_ans_recorded_(false)
  {
  }
  virtual ~ObJsonSchemaValidator() {}
  int schema_validator(ObIJsonBase *json_doc, bool& is_valid);
  int get_json_or_schema_point(ObJsonBuffer& pointer, bool is_schema_pointer);
  OB_INLINE ObString get_failed_keyword() { return failed_keyword_;}
  typedef int (*ObCheckCompSchema)(ObIJsonBase *json_doc, ObIJsonBase* dep_schema, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid);

private:
  int inner_schema_validator(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid);
  int object_recursive_validator(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid);
  int collect_schema_by_key(const ObString& key, ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObIJsonBase*> &recursive_vec);
  int collect_schema_by_pattern_key(const ObString& key, ObIJsonBase* schema_vec, ObIArray<ObIJsonBase*> &recursive_vec);
  int collect_schema_by_add_key(const ObString& key, ObIJsonBase *json_doc, ObIJsonBase* schema_vec, ObIArray<ObIJsonBase*> &recursive_vec);
  int array_recursive_validator(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid);
  int collect_schema_by_idx(const int& idx, const ObString& idx_str, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObIJsonBase*> &recursive_vec);
  int collect_schema_by_add_idx(const int& idx, ObIJsonBase* schema_vec, ObIArray<ObIJsonBase*> &recursive_vec);
  int get_schema_composition_ans(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid);
  // check compositon in all scheme vec
  int get_vec_schema_composition_ans(ObIJsonBase *json_doc, ObJsonSchemaComp comp_idx, const ObString& key, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid);
  // check compositon in each scheme vec
  ObCheckCompSchema get_comp_schema_validation_func(ObJsonSchemaComp comp_type);
  int check_dep_schema(ObIJsonBase *json_doc, ObIJsonBase* dep_schema, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid);
  int check_allof_schema(ObIJsonBase *json_doc, ObIJsonBase* dep_schema, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid);
  int check_oneof_schema(ObIJsonBase *json_doc, ObIJsonBase* dep_schema, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid);
  int check_anyof_schema(ObIJsonBase *json_doc, ObIJsonBase* dep_schema, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid);
  int check_not_schema(ObIJsonBase *json_doc, ObIJsonBase* dep_schema, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid);
  int get_single_element_and_check(ObIJsonBase *json_doc, const int& idx, ObIJsonBase* dep_schema, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid);
  int check_single_comp_array(ObIJsonBase *json_doc, ObIJsonBase* allof_val, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid);
  int check_single_composition_schema(ObIJsonBase *json_doc, ObIJsonBase* single_schema, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid);
  int get_ans_by_id(ObIJsonBase* j_id, ObIArray<ObJsonSchemaAns> &ans_map, bool& ans);
  int check_all_schema_def(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, bool& is_valid);
  int check_all_composition_def(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObJsonSchemaAns> &ans_map);
  // if in composition, record ans; if not in composition, check ans ,when false raise error
  int record_comp_ans(const int& def_id, const bool& ans, ObIArray<ObJsonSchemaAns> &ans_map);
  int check_single_schema(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid);
  int check_null_or_boolean(ObIJsonBase *json_doc, ObIJsonBase *schema, bool is_null, bool& is_valid);
  // keywords in composition
  int check_public_key_words(ObIJsonBase *json_doc, ObIJsonBase *schema, const ObJsonSchemaType& valid_type, bool& is_valid);
  // key_words_in_schema
  int check_public_key_words(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, ObJsonSchemaType valid_type, bool& is_valid);
  int check_public_key_words(const char key_start, ObJsonSchemaType &valid_type, ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid);
  int check_number_and_integer(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid);
  int check_number_and_integer(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, bool& is_valid);
  int check_boundary_key_words(const ObString& key, ObIJsonBase *json_doc, ObIJsonBase *schema, int& res);
  int check_string_type(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, bool& is_valid);
  int check_string_type(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid);
  int check_object_type(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, bool& is_valid);
  int check_object_type(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid);
  int check_array_type(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, bool& is_valid);
  int check_array_type(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid);
  bool check_type(const ObJsonSchemaType& valid_type, ObIJsonBase *schema);
  bool check_multiple_of(ObIJsonBase *json_doc, ObIJsonBase *schema);
  bool check_pattern_keywords(ObIJsonBase *json_doc, ObIJsonBase *schema);
  int check_ref(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid);
  int check_single_ref(ObIJsonBase *json_doc, const ObString& ref_key, bool& is_valid);
  int check_enum(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid);
  int check_required(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid);
  int check_dep_required(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid);
  int check_add_pro_in_schema(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid, ObString defined_key = ObString::make_empty_string());
  int check_pattern_key_in_add_pro(const ObString& key, ObIJsonBase *schema, bool& found);
  int check_unique_items(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid);
  int get_composition_schema_def(int idx, ObIJsonBase *schema_vec, ObIJsonBase *&schema, ObIArray<ObJsonSchemaAns> &ans_map, int& schema_id);
public:
  ObIAllocator *allocator_;
  ObIJsonBase *schema_map_;
  ObString failed_keyword_;
  ObStack<ObString> json_pointer_;
  ObStack<ObString> schema_pointer_;
  ObJsonBuffer str_buf_;
  // if did't record any composition ans, don't need check anyOf, oneOf, allOf and dependent schema, their default ans is true
  // but need check not, which default ans if false
  bool composition_ans_recorded_;
};

// ObJsonSchemaCache
// used in json schema expression
class ObJsonSchemaCache {
public:
  enum ObSchemaParseStat{
    UNINITIALIZED,
    INITIALIZED,
    ERROR,
  };

  struct ObSchemaCacheStat{
    ObSchemaParseStat state_;
    int index_;
    ObSchemaCacheStat() : state_(UNINITIALIZED), index_(-1) {}
    ObSchemaCacheStat(ObSchemaParseStat state, int idx) : state_(state), index_(idx) {};
    ObSchemaCacheStat(const ObSchemaCacheStat& stat) : state_(stat.state_), index_(stat.index_) {}
  };
  typedef PageArena<ObString, ModulePageAllocator> JsonSchemaStrArena;
  typedef PageArena<ObIJsonBase*, ModulePageAllocator> JsonSchemaArena;
  typedef PageArena<ObSchemaCacheStat, ModulePageAllocator> SchemaCacheStatArena;
  typedef ObVector<ObString, JsonSchemaStrArena> ObJsonSchemaStr;
  typedef ObVector<ObIJsonBase*, JsonSchemaArena> ObJsonSchemaPointers;
  typedef ObVector<ObSchemaCacheStat, SchemaCacheStatArena> ObSchemaCacheStatArr;
  static const int64_t DEFAULT_PAGE_SIZE = (1LL << 10); // 1kb

public:
  ObJsonSchemaCache(common::ObIAllocator *allocator) :
        allocator_(allocator),
        page_allocator_(*allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
        schema_cache_arena_(DEFAULT_PAGE_SIZE, page_allocator_),
        schema_arena_(DEFAULT_PAGE_SIZE, page_allocator_),
        str_arena_(DEFAULT_PAGE_SIZE, page_allocator_),
        schema_str_(&str_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
        schema_arr_ptr_(&schema_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
        stat_arr_(&schema_cache_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR) {}
  ~ObJsonSchemaCache() {};

  ObIJsonBase* schema_at(size_t idx);

  ObSchemaParseStat schema_stat_at(size_t idx);

  size_t size() { return schema_arr_ptr_.size(); }
  void reset() {stat_arr_.clear(); schema_arr_ptr_.clear(); schema_str_.clear();}

  int find_and_add_cache(ObIJsonBase*& out_schema, ObString& in_str, int arg_idx, const ObJsonInType& in_type);
  void set_allocator(common::ObIAllocator *allocator);
  common::ObIAllocator* get_allocator() {return allocator_;}
private:
  int set_schema(ObIJsonBase* j_schema, const ObString& in_str, int arg_idx, int index);
  bool is_match(ObString& in_str, size_t idx);

  int fill_empty(size_t reserve_size);

private:
  common::ObIAllocator *allocator_;
  ModulePageAllocator page_allocator_;
  SchemaCacheStatArena schema_cache_arena_;
  JsonSchemaArena      schema_arena_;
  JsonSchemaStrArena str_arena_;
  ObJsonSchemaStr schema_str_;           // array of schema str, for varchar, its json str; for json, its raw binary
  ObJsonSchemaPointers schema_arr_ptr_;  // array of parsed json schema;
  ObSchemaCacheStatArr stat_arr_;        // stat of json schema;
};

class ObJsonSchemaUtils
{
public:
  static int is_all_children_subschema(ObJsonNode* array_of_subschema);
  static int check_if_composition_legal(ObJsonObject* origin_schema, ObJsonSubSchemaKeywords& key_words);
  static int check_composition_by_name(const ObString& key_word, ObJsonObject* origin_schema, bool& is_legal);
  static int json_doc_move_to_key(const ObString& key_word, ObJsonObject*& json_schema);
  static int json_doc_move_to_array(const ObString& key_word, ObJsonObject* json_schema, ObJsonArray*& array_schema);
  static int record_schema_array(ObStack<ObJsonObject*>& stk, ObIArray<ObJsonObject*>& array);
  static int get_index_str(const int& idx, ObStringBuffer& buf);
  static int get_single_key_value(ObIJsonBase *single_schema, ObString& key_words, ObIJsonBase *&value);
  static int collect_key(ObJsonNode* obj, ObIAllocator *allocator, ObJsonArray* array, ObJsonBuffer& buf, bool pattern_key = false);
  static int get_specific_type_of_child(const ObString& key, ObJsonNodeType expect_type, ObIArray<ObIJsonBase*> &src, ObIArray<ObIJsonBase*> &res);
  static int get_all_composition_def(ObIArray<ObIJsonBase*> &src, ObIArray<ObIJsonBase*> &res);
  static int get_json_number(ObIJsonBase* json_doc, double& res);
  static int set_type_by_string(const ObString& str, ObJsonSchemaType& s_type);
  static int is_valid_pattern(const ObString& regex_text, ObJsonBuffer& buf, bool& ans);
  static int if_regex_match(const ObString& text, const ObString& regex_text, ObJsonBuffer& buf, bool& ans);
  static bool check_multiple_of(const double& json_val, const double& multi_val);
  static ObString get_pointer_key(ObString& ref_str, bool& end_while);
  static int set_valid_number_type_by_mode(ObIJsonBase *json_doc, ObJsonSchemaType& valid_type);
  static bool is_legal_json_pointer_name(const ObString& name);
  static bool is_legal_json_pointer_start(const char& ch);
  static bool is_legal_json_pointer_char(const char& ch);
  // for array, recursive checking is only required if there are keywords: ITEMS, TUPLE_ITEMS, ADDITIONAL_ITEMS
  // for object, recursive checking is only required if there are keywords: PROPERTIES, PATTERN_PRO, ADDITIONAL_PRO
  static int need_check_recursive(ObIArray<ObIJsonBase*> &schema_vec, bool& need_recursive, bool is_array_keywords);
};

} // namespace common
} // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_JSON_SCHEMA