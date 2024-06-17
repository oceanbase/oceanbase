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
 * This file contains implementation support for the json base abstraction.
 */

#define USING_LOG_PREFIX SQL
#include "ob_json_schema.h"
#include <regex>
#include "ob_json_bin.h"
#include "ob_json_parse.h"
#include "lib/encode/ob_base64_encode.h" // for ObBase64Encoder
#include "lib/utility/ob_fast_convert.h" // ObFastFormatInt::format_unsigned
#include "lib/charset/ob_dtoa.h" // ob_gcvt_opt
#include "rpc/obmysql/ob_mysql_global.h" // DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE
#include "lib/charset/ob_charset.h" // for strntod
#include "common/ob_smart_var.h" // for SMART_VAR
#include <rapidjson/internal/regex.h>

namespace oceanbase {
namespace common {
class ObJsonSchemaItem
{
public:
  static constexpr char* ROOT = const_cast<char*>("#");
  static constexpr char* REF = const_cast<char*>("$ref");
  static constexpr char* SCHEMA = const_cast<char*>("schema");
  static constexpr char* COMPOSITION = const_cast<char*>("composition");
  static constexpr char* PROPERTIES = const_cast<char*>("properties");// 10
  static constexpr char* PATTERN_PRO = const_cast<char*>("patternProperties");  //17
  static constexpr char* ADDITIONAL_PRO = const_cast<char*>("additionalProperties"); // 20
  static constexpr char* ALLOF = const_cast<char*>("allOf");
  static constexpr char* ANYOF = const_cast<char*>("anyOf");
  static constexpr char* ONEOF = const_cast<char*>("oneOf");
  static constexpr char* NOT = const_cast<char*>("not");
  static constexpr char* DEPENDENCIES = const_cast<char*>("dependencies");
  // DEPENDENTREQUIRED and DEPENDENTSCHEMAS are values of "dependencies" keyword
  // when value type is object, record as DEPENDENTSCHEMAS
  static constexpr char* DEPENDENTREQUIRED = const_cast<char*>("dependentRequired");// 17
  static constexpr char* DEPENDENTSCHEMAS = const_cast<char*>("dependentSchemas");
  static constexpr char* ENUM = const_cast<char*>("enum");// 4
  static constexpr char* TYPE = const_cast<char*>("type");// 4
  static constexpr char* TYPE_STRING = const_cast<char*>("string");
  static constexpr char* TYPE_NUMBER = const_cast<char*>("number");
  static constexpr char* TYPE_INTEGER = const_cast<char*>("integer");
  static constexpr char* TYPE_BOOLEAN = const_cast<char*>("boolean");
  static constexpr char* TYPE_NULL = const_cast<char*>("null");
  static constexpr char* TYPE_OBJECT = const_cast<char*>("object");
  static constexpr char* TYPE_ARRAY = const_cast<char*>("array");
  static constexpr char* MIN_LEN = const_cast<char*>("minLength"); // 9
  static constexpr char* MAX_LEN = const_cast<char*>("maxLength"); // 9
  static constexpr char* PATTERN = const_cast<char*>("pattern");
  static constexpr char* MULTIPLE_OF = const_cast<char*>("multipleOf"); // 10
  static constexpr char* MINMUM = const_cast<char*>("minimum"); // 7
  static constexpr char* MAXMUM = const_cast<char*>("maximum"); // 7
  static constexpr char* EXCLUSIVE_MINMUM = const_cast<char*>("exclusiveMinimum"); // 16
  static constexpr char* EXCLUSIVE_MAXMUM = const_cast<char*>("exclusiveMaximum"); // 16
  static constexpr char* REQUIRED = const_cast<char*>("required"); // 8
  static constexpr char* MIN_PROPERTIES = const_cast<char*>("minProperties"); // 13
  static constexpr char* MAX_PROPERTIES = const_cast<char*>("maxProperties"); // 13
  static constexpr char* ITEMS = const_cast<char*>("items");
  static constexpr char* TUPLE_ITEMS = const_cast<char*>("tupleItems");
  static constexpr char* ADDITIONAL_ITEMS = const_cast<char*>("additionalItems");
  static constexpr char* UNIQUE_ITEMS = const_cast<char*>("uniqueItems"); // 11
  static constexpr char* MIN_ITEMS = const_cast<char*>("minItems"); // 8
  static constexpr char* MAX_ITEMS = const_cast<char*>("maxItems"); // 8
};

static const int  JS_TYPE_LEN = 4;          // strlen(ObJsonSchemaItem::TYPE)
static const int  JS_PATTERN_LEN = 7;       // strlen(ObJsonSchemaItem::PATTERN),
static const int  JS_REQUIRED_LEN = 8;      //strlen(ObJsonSchemaItem::REQUIRED),
static const int  JS_STRMAX_LEN = 9;        // strlen(ObJsonSchemaItem::MAX_LEN),
static const int  JS_MULTIPLE_LEN = 10;     // strlen(ObJsonSchemaItem::MULTIPLE_OF),
static const int  JS_UNIQUE_ITEMS_LEN = 11; //strlen(ObJsonSchemaItem::UNIQUE_ITEMS)
static const int  JS_PROMAX_LEN = 13;       // strlen(ObJsonSchemaItem::MAX_PROPERTIES),
static const int  JS_ADD_ITEMS_LEN = 15;    //strlen(ObJsonSchemaItem::ADDITIONAL_ITEMS),
static const int  JS_EXCLUSIVE_LEN = 16;    // strlen(ObJsonSchemaItem::EXCLUSIVE_MAXMUM),
static const int  JS_DEP_REQUIRED_LEN = 17; //strlen(ObJsonSchemaItem::DEPENDENTREQUIRED),
static const int  JS_ADD_PRO_LEN = 20;      // strlen(ObJsonSchemaItem::ADDITIONAL_PRO),

int ObJsonSchemaTree::build_schema_tree(ObIJsonBase *json_doc)
{
  INIT_SUCC(ret);
  ObJsonObject* origin_json = nullptr;
  ObJsonObject* cur_root = nullptr;
  serial_num_ = 0;

  if (OB_ISNULL(json_doc) || OB_ISNULL(allocator_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("shouldn't be null", K(ret), KPC(json_doc));
  } else if (json_doc->json_type() != ObJsonNodeType::J_OBJECT) {
    // json schema must be object
    ret = OB_ERR_TYPE_OF_JSON_SCHEMA;
    LOG_WARN("json schema must be object", K(ret), K(json_doc->json_type()));
  } else if (json_doc->is_bin()) {
    ObJsonBin *j_bin = static_cast<ObJsonBin *>(json_doc);
    ObJsonNode *j_tree = nullptr;
    if (OB_FAIL(j_bin->to_tree(j_tree))) {
      LOG_WARN("fail to change bin to tree", K(ret));
    } else {
      origin_json = static_cast<ObJsonObject*>(j_tree);
    }
  } else {
    origin_json = static_cast<ObJsonObject*>(json_doc);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(root_doc_) && OB_FALSE_IT(root_doc_ = origin_json)) {
  } else if (OB_ISNULL(schema_map_ = OB_NEWx(ObJsonArray, allocator_, allocator_))
          || OB_ISNULL(cur_root = OB_NEWx(ObJsonObject, allocator_, allocator_))
          || OB_ISNULL(ref_ = OB_NEWx(ObJsonObject, allocator_, allocator_))
          || OB_ISNULL(typeless_ = OB_NEWx(ObJsonInt, allocator_, DEFAULT_PREVIOUS_NUMBER))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to init schema tree.", K(ret));
  } else if (OB_FAIL(schema_map_->append(cur_root))) {
    LOG_WARN("fail to push schema root.", K(ret));
  } else if (OB_FALSE_IT(++serial_num_) || OB_FALSE_IT(cur_schema_stk_.reset())) {
  } else if (OB_FAIL(cur_schema_stk_.push(cur_root))) {
    LOG_WARN("fail to push cur schema.", K(ret));
  } else if (OB_FAIL(schema_map_->append(ref_))) {
    LOG_WARN("fail to push schema root.", K(ret));
  } else if (OB_FALSE_IT(++serial_num_)) {
  } else if (OB_FAIL(inner_build_schema_tree(origin_json, false))) {
    LOG_WARN("fail to build schema.", K(ret));
  }
  return ret;
}
bool ObJsonSchemaTree::if_have_ref(ObJsonObject* origin_schema) {
  bool ret_bool = false;
  ObJsonNode* node = origin_schema->get_value(ObJsonSchemaItem::REF);
  if (OB_ISNULL(node)) {
  // didn't define, its normal
  } else if (node->json_type() == ObJsonNodeType::J_STRING) {
    ObString ref_str = ObString(node->get_data_length(), node->get_data());
    bool end_while = false;
    bool is_legal_name = true;
    while (!ref_str.empty() && !end_while && is_legal_name) {
      ObString key_str = ObJsonSchemaUtils::get_pointer_key(ref_str, end_while);
      if (key_str == ObJsonSchemaItem::ROOT) {
      } else if (!ObJsonSchemaUtils::is_legal_json_pointer_name(key_str)) {
        is_legal_name = false;
      }
    }
    ret_bool = end_while && is_legal_name;
  }
  return ret_bool;
}

int ObJsonSchemaTree::get_ref_pointer_value(const ObString origin_ref_str, ObJsonObject*& ref_value)
{
  INIT_SUCC(ret);
  ObString ref_str = origin_ref_str;
  if (ref_str.length() > 1) {
    ObIJsonBase *new_doc = nullptr;
    bool end_while = false;
    new_doc = root_doc_;
    while (!ref_str.empty() && !end_while && OB_NOT_NULL(new_doc) && OB_SUCC(ret)) {
      ObString key_str = ObJsonSchemaUtils::get_pointer_key(ref_str, end_while);
      ObIJsonBase *tmp_doc = nullptr;
      if (key_str.empty()) {
        new_doc = nullptr;
      } else if (key_str == ObJsonSchemaItem::ROOT) {
        new_doc = root_doc_;
      } else if (OB_FAIL(new_doc->get_object_value(key_str, tmp_doc)) || OB_ISNULL(tmp_doc)) {
        // didn't find, its normal
        ret = OB_SUCCESS;
        end_while = true;
        new_doc = nullptr;
      } else {
        new_doc = tmp_doc;
      }
    }
    if (OB_NOT_NULL(new_doc) && new_doc->json_type() == ObJsonNodeType::J_OBJECT) {
      ref_value = static_cast<ObJsonObject*>(new_doc);
    } else {
      ref_value = nullptr;
    }
  } else if (ref_str.length() == 1) { //ref_str = "#"
    ref_value = root_doc_;
  }
  return ret;
}

int ObJsonSchemaTree::handle_ref_keywords(ObJsonObject* origin_schema, ObIArray<ObJsonNode*> &schema_vec_stk,
                                          const bool& is_composition, ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  ObJsonNode* node = origin_schema->get_value(ObJsonSchemaItem::REF);
  ObString origin_ref = ObString(node->get_data_length(), node->get_data());
  ObJsonObject* ref_val = nullptr;
  if (OB_FAIL(get_ref_pointer_value(origin_ref, ref_val))) {
    LOG_WARN("fail to get ref value.", K(ret));
  } else if (OB_ISNULL(ref_val)) {
    // didn't find, its normal
  } else if (OB_FAIL(generate_schema_and_record(ObJsonSchemaItem::REF, node, schema_vec_stk, is_composition, comp_array))) {
    LOG_WARN( "fail to add type schema", K(ret));
  } else {
    // record and parse ref value
    ObJsonNode* ref_schema = ref_->get_value(origin_ref);
    if (OB_ISNULL(ref_schema)) {
      if (origin_ref.compare(json_ptr_) == 0) {
        if (OB_FAIL(ref_->add(origin_ref, typeless_, true, false, false))) {
          LOG_WARN("fail to add ref schema.", K(ret));
        }
      } else {
        ObJsonSchemaTree ref_schema_tree(allocator_, root_doc_, origin_ref);
        if (OB_FAIL(ref_schema_tree.build_schema_tree(ref_val))) {
          LOG_WARN("fail to build schema.", K(ret));
        } else if (OB_ISNULL(ref_schema_tree.schema_map_)) {
        } else if (OB_FAIL(ref_->add(origin_ref, ref_schema_tree.schema_map_, true, false, false))) {
          LOG_WARN("fail to add ref schema.", K(ret));
        }
      }
    } // not null, already parsed, do nothing
  }
  return ret;
}

int ObJsonSchemaTree::inner_build_schema_tree(ObJsonObject* origin_schema, bool is_composition, ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  ObArray<ObJsonNode*> schema_vec_stk;
  schema_vec_stk.set_block_size(SCHEMA_DEFAULT_PAGE_SIZE);
  ObJsonNode* type = nullptr;
  ObJsonSchemaType schema_type;
  ObJsonSubSchemaKeywords key_words;
  // record keywords that have subschema
  key_words.flags_ = 0;

  if (cur_schema_stk_.size() < 1 || OB_ISNULL(schema_map_) || OB_ISNULL(allocator_)
      || OB_ISNULL(origin_schema) || (is_composition && OB_ISNULL(comp_array))) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("shouldn't be null.", K(ret), K(cur_schema_stk_.size()), KPC(schema_map_));
  } else if (OB_FAIL(get_schema_vec(schema_vec_stk, is_composition))) {
    // all schema add to schema_vec:
    // if in composition, add to composition_vec, when check, just record result
    // if not in composition, add to schema_vec, when check and if illegal, raise error report and stop validation
    LOG_WARN("fail to get schema vec.", K(ret));
    // check public schema key words: type
  } else if (if_have_ref(origin_schema)) {
    if (OB_FAIL(handle_ref_keywords(origin_schema, schema_vec_stk, is_composition, comp_array))) {
      LOG_WARN("fail to handle ref.", K(ret));
    }
  } else if (OB_FAIL(get_difined_type(origin_schema, schema_vec_stk, schema_type, is_composition, comp_array))) {
    LOG_WARN("fail to get schema type.", K(ret));
    // check public schema key words: enum
  } else if (schema_type.error_type_ == 1) {
    // wrong type, the schema must be false, do not need check other keywords
    // but its legal, don't raise error
  } else if (OB_FAIL(handle_keywords_with_specific_type(ObJsonSchemaItem::ENUM, ObJsonNodeType::J_ARRAY,
                                                        origin_schema, schema_vec_stk, is_composition, comp_array))) {
    LOG_WARN("fail to get schema enum.", K(ret));
  // check keywords by defined type, if not define, check each key
  } else if (OB_FAIL(check_keywords_by_type(schema_type, origin_schema, schema_vec_stk, key_words, is_composition, comp_array))) {
    LOG_WARN("fail to check schema by type.", K(ret));
  } else if (OB_FAIL(ObJsonSchemaUtils::check_if_composition_legal(origin_schema, key_words))) {
    LOG_WARN("fail to check if_composition_legal.", K(ret));
  } else if (OB_FALSE_IT(schema_vec_stk.destroy())) { // useless now
  } else if (key_words.flags_ != 0 && OB_FAIL(handle_keywords_with_subschemas(key_words, origin_schema, schema_vec_stk, is_composition, comp_array))) {
    LOG_WARN("fail to handle key words with subschema.", K(ret));
  }
  return ret;
}

int ObJsonSchemaTree::get_schema_vec(ObIArray<ObJsonNode*> &schema_vec_stk, bool is_composition)
{
  INIT_SUCC(ret);
  int size = cur_schema_stk_.size();
  ObJsonNode* res_vec = nullptr;
  for (int i = 0; OB_SUCC(ret) && i < size; ++i) {
    ObJsonObject* cur_schema = cur_schema_stk_.at(i);
    ObJsonNode* tmp_json = nullptr;
    if (is_composition) {
      ObJsonArray* schema_vec = nullptr;
      if (OB_ISNULL(tmp_json = cur_schema->get_value(ObJsonSchemaItem::COMPOSITION))) {
        if (OB_ISNULL(schema_vec = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc composition node.", K(ret));
        } else if (OB_FAIL(cur_schema->add(ObJsonSchemaItem::COMPOSITION, schema_vec, true, false, false))) {
          LOG_WARN("fail to add composition node.", K(ret));
        } else {
          res_vec = schema_vec;
        }
      } else if (tmp_json->json_type() == ObJsonNodeType::J_ARRAY) {
        res_vec = tmp_json;
      } else {
        ret = OB_ERR_INVALID_JSON_TYPE;
        LOG_WARN("must be array.", K(ret));
      }
    } else if (OB_ISNULL(tmp_json = cur_schema->get_value(ObJsonSchemaItem::SCHEMA))) {
      ObJsonObject* schema_vec = nullptr;
      if (OB_ISNULL(schema_vec = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc schema node.", K(ret));
      } else if (OB_FAIL(cur_schema->add(ObJsonSchemaItem::SCHEMA, schema_vec, true, false, false))) {
        LOG_WARN("fail to add schema node.", K(ret));
      } else {
        res_vec = schema_vec;
      }
    } else if (tmp_json->json_type() == ObJsonNodeType::J_OBJECT) {
      res_vec = tmp_json;
    } else {
      ret = OB_ERR_INVALID_JSON_TYPE;
      LOG_WARN("must be array.", K(ret));
    }

    // the schema need record to all members in schma_vec_stk
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_vec_stk.push_back(res_vec))) {
      LOG_WARN("fail to push schema_vec node.", K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaTree::generate_schema_and_record(const ObString& key_word,
                                                ObJsonNode* value,
                                                ObIArray<ObJsonNode*> &schema_vec_stk,
                                                const bool& is_composition,
                                                ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  if (is_composition && OB_FAIL(generate_comp_and_record(key_word, value, schema_vec_stk, comp_array))) {
    LOG_WARN("fail to add comp node.", K(ret));
  } else if (!is_composition && OB_FAIL(generate_schema_info(key_word, value, schema_vec_stk))) {
    LOG_WARN("fail to add schema node.", K(ret));
  }
  return ret;
}

int ObJsonSchemaTree::generate_schema_info(const ObString& key_word, ObJsonNode* value, ObIArray<ObJsonNode*> &schema_vec_stk)
{
  INIT_SUCC(ret);
  int size = schema_vec_stk.count();
  for (int i = 0; OB_SUCC(ret) && i < size; ++i) {
    ObJsonNode* cur_schema_node = schema_vec_stk.at(i);
    ObJsonObject* cur_schema_vec = nullptr;
    ObJsonNode* old_key_value = nullptr;
    if (cur_schema_node->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_ERR_WRONG_VALUE;
      LOG_WARN("must be object type.", K(ret), K(i));
    } else if (OB_FALSE_IT(cur_schema_vec = static_cast<ObJsonObject*>(cur_schema_node))) {
    } else if (OB_ISNULL(old_key_value = cur_schema_vec->get_value(key_word))) {
      if (OB_FAIL(cur_schema_vec->add(key_word, value))) {
        LOG_WARN("fail to add.", K(ret), K(i));
      }
    } else {
      int res = 0;
      bool update_old_key = false;
      if (OB_SUCC(old_key_value->compare(*value, res)) && res == 0) {
        // same value, do nothing
      } else if (OB_FAIL(union_schema_def(key_word, value, old_key_value, update_old_key))) {
        LOG_WARN("fail to get union.", K(key_word), K(ret));
      } else if (update_old_key && OB_FAIL(cur_schema_vec->add(key_word, value, false, true, true))) {
        LOG_WARN("fail to update value.", K(key_word), K(ret));
      }
    }
  } // end for
  return ret;
}

int ObJsonSchemaTree::union_type(ObJsonNode*& new_value, ObJsonNode* old_value, bool& update_old_key)
{
  INIT_SUCC(ret);
  if (new_value->json_type() != ObJsonNodeType::J_UINT || old_value->json_type() != ObJsonNodeType::J_UINT) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be array type.", K(ret));
  } else {
    ObJsonSchemaType old_val;
    old_val.flags_ = old_value->get_uint();
    ObJsonSchemaType new_val;
    new_val.flags_ = new_value->get_uint();
    ObJsonSchemaType final_val;
    final_val.flags_ = 0;
    if (old_val.error_type_ == 1 || new_val.error_type_ == 1) {
      final_val.error_type_ = 1;
    } else {
      if ((old_val.integer_ == 1 && new_val.integer_)
        || (old_val.integer_ == 1 && new_val.number_)
        || (old_val.number_ == 1 && new_val.integer_)) {
        final_val.integer_ = 1;
      }
      final_val.null_ = old_val.null_ & new_val.null_;
      final_val.boolean_ = old_val.boolean_ & new_val.boolean_;
      final_val.string_ = old_val.string_ & new_val.string_;
      final_val.number_ = old_val.number_ & new_val.number_;
      final_val.object_ = old_val.object_ & new_val.object_;
      final_val.array_ = old_val.array_ & new_val.array_;
      if (final_val.flags_ == 0) {
        final_val.error_type_ = 1;
      }
      if (final_val.flags_ == 0) {
        final_val.error_type_ = 1;
      }
    }

    // set new type
    update_old_key = false;
    ObJsonUint* final_value = static_cast<ObJsonUint*>(old_value);
    final_value->set_value(final_val.flags_);
  }
  return ret;
}


int ObJsonSchemaTree::union_array_key_words_value(ObJsonNode*& new_value, ObJsonNode* old_value, bool& update_old_key, bool get_merge)
{
  INIT_SUCC(ret);
  ObSortedVector<ObJsonNode*> res;
  ObJsonContentCmp cmp;
  ObJsonContentUnique unique;
  if (new_value->json_type() != ObJsonNodeType::J_ARRAY || old_value->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be array type.", K(ret));
  } else {
    ObJsonArray* new_val = static_cast<ObJsonArray*>(new_value);
    ObJsonArray* old_val = static_cast<ObJsonArray*>(old_value);
    int old_size = old_val->element_count();
    int new_size = new_val->element_count();
    if (old_size == 0) {
      update_old_key = true;
    } else if (new_size == 0) {
    } else {
      for (int i = 0; i < old_size && OB_SUCC(ret); ++i) {
        ObSortedVector<ObJsonNode*>::iterator pos = res.end();
        ObJsonNode* node = (*old_val)[i];
        if (OB_FAIL(res.insert_unique(node, pos, cmp, unique))) {
          if (ret == OB_CONFLICT_VALUE) {
            ret = OB_SUCCESS; // confilict means found duplicated nodes, it is not an error.
          }
        }
      }
      old_val->clear();
      for (int i = 0; i < new_size && OB_SUCC(ret); ++i) {
        ObSortedVector<ObJsonNode*>::iterator pos = res.end();
        ObJsonNode* node = (*new_val)[i];
        if (node->json_type() != ObJsonNodeType::J_STRING) {
          // ignore
        } else if (OB_FAIL(res.insert_unique(node, pos, cmp, unique))) {
          if (ret == OB_CONFLICT_VALUE) {
            ret = OB_SUCCESS;
            if (!get_merge && OB_FAIL(old_val->append(node))) { // get union, only need confict value
              LOG_WARN("fail to append.", K(ret));
            }
          }
        } else if (get_merge && OB_FAIL(old_val->append(node))) {
          // get merge, add values that not in old_val
          LOG_WARN("fail to append.", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJsonSchemaTree::union_scalar_key_words_value(ObJsonNode*& new_value, ObJsonNode* old_value, bool& update_old_key)
{
  INIT_SUCC(ret);
  if (old_value->json_type() == ObJsonNodeType::J_ARRAY) {
    if (OB_FAIL(old_value->array_append(new_value))) {
      LOG_WARN("fail to append.", K(ret));
    }
  } else {
    update_old_key = true;
    ObJsonArray* array_val = nullptr;
    if (OB_ISNULL(array_val = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc array node.", K(ret));
    } else if (OB_FAIL(array_val->array_append(old_value))) {
      LOG_WARN("fail to append.", K(ret));
    } else if (OB_FAIL(array_val->array_append(new_value))) {
      LOG_WARN("fail to append.", K(ret));
    } else {
      new_value = array_val;
    }
  }
  return ret;
}

int ObJsonSchemaTree::union_add_pro_value(ObJsonNode*& new_value, ObJsonNode* old_value)
{
  INIT_SUCC(ret);
  if (new_value->json_type() != ObJsonNodeType::J_ARRAY || old_value->json_type() != ObJsonNodeType::J_ARRAY
    || new_value->element_count() != ADDITIONAL_PRO_ARRAY_COUNT || (old_value->element_count()) % ADDITIONAL_PRO_ARRAY_COUNT != 0) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be object type.", K(ret));
  } else {
    int size = new_value->element_count();
    ObJsonArray* new_val = static_cast<ObJsonArray*>(new_value);
    ObJsonArray* old_val = static_cast<ObJsonArray*>(old_value);
    for (int i = 0; i < ADDITIONAL_PRO_ARRAY_COUNT && OB_SUCC(ret); ++i) {
      ObJsonNode* new_node = (*new_val)[i];
      if (OB_FAIL(old_val->append(new_node))) {
        LOG_WARN("fail to get union.", K(ret));
      }
    }
  }
  return ret;
}

int ObJsonSchemaTree::union_schema_def(const ObString& key_word, ObJsonNode*& value, ObJsonNode* old_key_value, bool& update_old_key)
{
  INIT_SUCC(ret);
  int len = key_word.length();
  update_old_key = false;
  int res = 0;
  switch (len) {
    case JS_TYPE_LEN : { // type or enum
      if (key_word.compare(ObJsonSchemaItem::TYPE) == 0 && OB_SUCC(union_type(value, old_key_value, update_old_key))) {
      } else if (key_word.compare(ObJsonSchemaItem::ENUM) == 0 && OB_SUCC(union_array_key_words_value(value, old_key_value, update_old_key))) {
      } else if (key_word.compare(ObJsonSchemaItem::REF) == 0 && OB_SUCC(union_scalar_key_words_value(value, old_key_value, update_old_key))) {
      } else if (OB_SUCC(ret)) {
        ret = OB_ERR_WRONG_VALUE;
      }
      break;
    }
    case JS_STRMAX_LEN : {
      if (key_word.compare(ObJsonSchemaItem::MAX_LEN) == 0 && OB_SUCC(old_key_value->compare(*value, res))) {
        update_old_key = res > 0 ? true : false;
      } else if (key_word.compare(ObJsonSchemaItem::MIN_LEN) == 0 && OB_SUCC(old_key_value->compare(*value, res))) {
        update_old_key = res < 0 ? true : false;
      } else if (OB_SUCC(ret)) {
        ret = OB_ERR_WRONG_VALUE;
      }
      break;
    }
    case JS_PATTERN_LEN : { // pattern, maximum, minimun
      if (key_word.compare(ObJsonSchemaItem::MAXMUM) == 0 && OB_SUCC(old_key_value->compare(*value, res))) {
        update_old_key = res > 0 ? true : false;
      } else if (key_word.compare(ObJsonSchemaItem::MINMUM) == 0 && OB_SUCC(old_key_value->compare(*value, res))) {
        update_old_key = res < 0 ? true : false;
      } else if (key_word.compare(ObJsonSchemaItem::PATTERN) == 0
                && OB_SUCC(union_scalar_key_words_value(value, old_key_value, update_old_key))) {
      } else if (OB_SUCC(ret)) {
        ret = OB_ERR_WRONG_VALUE;
      }
      break;
    }
    case JS_EXCLUSIVE_LEN : {
      if (key_word.compare(ObJsonSchemaItem::EXCLUSIVE_MAXMUM) == 0 && OB_SUCC(old_key_value->compare(*value, res))) {
        update_old_key = res > 0 ? true : false;
      } else if (key_word.compare(ObJsonSchemaItem::EXCLUSIVE_MINMUM) == 0 && OB_SUCC(old_key_value->compare(*value, res))) {
        update_old_key = res < 0 ? true : false;
      } else if (OB_SUCC(ret)) {
        ret = OB_ERR_WRONG_VALUE;
      }
      break;
    }
    case JS_MULTIPLE_LEN : {
      if (key_word.compare(ObJsonSchemaItem::MULTIPLE_OF) == 0
        && OB_FAIL(union_scalar_key_words_value(value, old_key_value, update_old_key))) {
        LOG_WARN("fail to union matiple.", K(ret));
      }
      break;
    }
    case JS_PROMAX_LEN : {
      if (key_word.compare(ObJsonSchemaItem::MAX_PROPERTIES) == 0 && OB_SUCC(old_key_value->compare(*value, res))) {
        update_old_key = res > 0 ? true : false;
      } else if (key_word.compare(ObJsonSchemaItem::MIN_PROPERTIES) == 0 && OB_SUCC(old_key_value->compare(*value, res))) {
        update_old_key = res < 0 ? true : false;
      } else if (OB_SUCC(ret)) {
        ret = OB_ERR_WRONG_VALUE;
      }
      break;
    }
    case JS_REQUIRED_LEN : { // required, maxitems, minitems
      if (key_word.compare(ObJsonSchemaItem::MAX_ITEMS) == 0 && OB_SUCC(old_key_value->compare(*value, res))) {
        update_old_key = res > 0 ? true : false;
      } else if (key_word.compare(ObJsonSchemaItem::MIN_ITEMS) == 0 && OB_SUCC(old_key_value->compare(*value, res))) {
        update_old_key = res < 0 ? true : false;
      } else if (key_word.compare(ObJsonSchemaItem::REQUIRED) == 0 && OB_SUCC(union_array_key_words_value(value, old_key_value, update_old_key, true))) {
      } else if (OB_SUCC(ret)) {
        ret = OB_ERR_WRONG_VALUE;
      }
      break;
    }
    case JS_DEP_REQUIRED_LEN : {
      if (key_word.compare(ObJsonSchemaItem::DEPENDENTREQUIRED) == 0
          && OB_FAIL(union_scalar_key_words_value(value, old_key_value, update_old_key))) {
        LOG_WARN("fail to union matiple.", K(ret));
      }
      break;
    }
    case JS_ADD_PRO_LEN : {
      if (key_word.compare(ObJsonSchemaItem::ADDITIONAL_PRO) == 0 && OB_FAIL(union_add_pro_value(value, old_key_value))) {
        LOG_WARN("fail to union additional pro.", K(ret));
      }
      break;
    }
    case JS_ADD_ITEMS_LEN : {
      if (key_word.compare(ObJsonSchemaItem::ADDITIONAL_ITEMS) == 0 && OB_SUCC(old_key_value->compare(*value, res))) {
        update_old_key = res > 0 ? true : false;
      } else if (OB_SUCC(ret)) {
        ret = OB_ERR_WRONG_VALUE;
      }
      break;
    }
    default: {
      ret = OB_ERR_WRONG_VALUE;
      LOG_WARN("wrong type.", K(ret));
    }
  }
  return ret;
}


int ObJsonSchemaTree::generate_comp_and_record(const ObString& key_word,
                                                ObJsonNode* value,
                                                ObIArray<ObJsonNode*> &schema_vec_stk,
                                                ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  ObJsonObject* key_word_schema = nullptr;
  int size = schema_vec_stk.count();
  if (OB_ISNULL(key_word_schema = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc key_word_schema node.", K(ret));
  } else if (OB_FAIL(key_word_schema->add(key_word, value, true, false, false))) {
    LOG_WARN("fail to add schema node.", K(key_word), K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < size; ++i) {
    ObJsonNode* cur_schema_node = schema_vec_stk.at(i);
    ObJsonArray* cur_schema_vec = nullptr;
    ObJsonNode* record_schema = nullptr;
    ObJsonInt* record_schema_idx = nullptr;
    if (i == 0) {
      record_schema = key_word_schema;
    } else {
      record_schema = typeless_;
    }
    if (cur_schema_node->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_ERR_WRONG_VALUE;
      LOG_WARN("must be array type.", K(ret), K(i));
    } else if (OB_FALSE_IT(cur_schema_vec = static_cast<ObJsonArray*>(cur_schema_node))) {
    } else if (OB_FAIL(schema_map_->append(record_schema))) { // append type to schema_map_
      LOG_WARN("fail to push schema map.", K(ret));
    } else if (OB_ISNULL(record_schema_idx = OB_NEWx(ObJsonInt, allocator_, serial_num_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN( "fail to alloc memory for array json node", K(ret));
    } else if (OB_FAIL(cur_schema_vec->append(record_schema_idx))) { // append to cur_schema_vec
      LOG_WARN("fail to push into schema_vec.", K(ret));
    } else if (OB_ISNULL(record_schema_idx = OB_NEWx(ObJsonInt, allocator_, serial_num_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN( "fail to alloc memory for array json node", K(ret));
    } else if (OB_FAIL(comp_array->append(record_schema_idx))) {
      LOG_WARN("fail to push composition array.", K(ret));
    } else {
      // num of schema_map++
      ++serial_num_;
    }
  }
  return ret;
}

int ObJsonSchemaTree::handle_keywords_with_specific_type(const ObString& key_word,
                                                         const ObJsonNodeType& expect_type,
                                                         ObJsonObject* origin_schema,
                                                         ObIArray<ObJsonNode*> &schema_vec_stk,
                                                         const bool& is_composition,
                                                         ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  ObJsonNode* node = origin_schema->get_value(key_word);
  if (OB_ISNULL(node)) {
  // didn't define, its normal
  } else if (node->element_count() == 0) {
  } else if (node->json_type() == expect_type) {
    if (OB_FAIL(generate_schema_and_record(key_word, node, schema_vec_stk, is_composition, comp_array))) {
      LOG_WARN( "fail to add type schema", K(key_word), K(ret));
    }
  } /*else if (lib::is_oracle_mode()) {
    if implement oracle json schema, check each keyword and its expect type, because:
     a.in mysql mode:
        1. if the keywords and its expect_type didn't match, mysql would ignore the keyword;
        2. but if a composition/item keywords is array type, but its children aren't object, mysql would coredump;
        (in this situation, raise error anyway)
     b.in oracle mode:
        1. if the keywords and its expect_type didn't match, oracle would return error "invalid JSON schema document";
     c.but if an objects which key doesn't match key_words, both oracle and mysql would ignore.
  }*/
  return ret;
}

int ObJsonSchemaTree::handle_positive_int_keywords(const ObString& key_word, ObJsonObject* origin_schema,
                                                  ObIArray<ObJsonNode*> &schema_vec_stk,
                                                  const bool& is_composition, ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  ObJsonNode* node = origin_schema->get_value(key_word);
  if (OB_ISNULL(node)) {
  // didn't define, its normal
  } else if (node->element_count() == 0) {
  } else if (node->json_type() == ObJsonNodeType::J_INT) {
    int num = node->get_int();
    if (num < 0) { // illegal value, ignore and not take effect
    } else if (OB_FAIL(generate_schema_and_record(key_word, node, schema_vec_stk, is_composition, comp_array))) {
      LOG_WARN( "fail to add type schema", K(key_word), K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaTree::handle_keywords_with_number_value(const ObString& key_word,
                                                        ObJsonObject* origin_schema,
                                                        ObIArray<ObJsonNode*> &schema_vec_stk,
                                                        const bool& is_composition,
                                                        ObJsonArray* comp_array,
                                                        bool must_be_positive /*= false*/)
{
  INIT_SUCC(ret);
  ObJsonNode* node = origin_schema->get_value(key_word);
  if (OB_ISNULL(node)) {
  // didn't define, its normal
  } else if (node->is_number()) {
    bool is_valid = true;
    if (must_be_positive) {
      double val = 0.0;
      if (OB_FAIL(ObJsonSchemaUtils::get_json_number(node, val))) {
        LOG_WARN( "fail to get num", K(node->json_type()), K(val));
      } else if (val < 0) { // illegal value, ignore and not take effect
        is_valid = false;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_valid && OB_FAIL(generate_schema_and_record(key_word, node, schema_vec_stk, is_composition, comp_array))) {
      LOG_WARN( "fail to add type schema", K(key_word), K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaUtils::set_type_by_string(const ObString& str, ObJsonSchemaType& s_type)
{
  INIT_SUCC(ret);
  if (str.length() < strlen(ObJsonSchemaItem::TYPE_NULL)) {
      s_type.error_type_ = 1;
    } else {
      switch (str[0]) {
        case 's': {
          if (str.compare(ObJsonSchemaItem::TYPE_STRING) == 0) {
            s_type.string_ = 1;
          } else {
            s_type.error_type_ = 1;
          }
          break;
        }
        case 'n': {
          if (str.compare(ObJsonSchemaItem::TYPE_NULL) == 0) {
            s_type.null_ = 1;
          } else if (str.compare(ObJsonSchemaItem::TYPE_NUMBER) == 0) {
            s_type.number_ = 1;
          } else {
            s_type.error_type_ = 1;
          }
          break;
        }
        case 'i': {
          if (str.compare(ObJsonSchemaItem::TYPE_INTEGER) == 0) {
            s_type.integer_ = 1;
          } else {
            s_type.error_type_ = 1;
          }
          break;
        }
        case 'b': {
          if (str.compare(ObJsonSchemaItem::TYPE_BOOLEAN) == 0) {
            s_type.boolean_ = 1;
          } else {
            s_type.error_type_ = 1;
          }
          break;
        }
        case 'o': {
          if (str.compare(ObJsonSchemaItem::TYPE_OBJECT) == 0) {
            s_type.object_ = 1;
          } else {
            s_type.error_type_ = 1;
          }
          break;
        }
        case 'a': {
          if (str.compare(ObJsonSchemaItem::TYPE_ARRAY) == 0) {
            s_type.array_ = 1;
          } else {
            s_type.error_type_ = 1;
          }
          break;
        }
        default : {
          s_type.error_type_ = 1;
          break;
        }
      }
    }
  return ret;
}

int ObJsonSchemaTree::get_difined_type(ObJsonObject* origin_schema,
                                       ObIArray<ObJsonNode*> &schema_vec_stk,
                                       ObJsonSchemaType& s_type,
                                       const bool& is_composition,
                                       ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  s_type.flags_ = 0;
  ObJsonNode* node = origin_schema->get_value(ObJsonSchemaItem::TYPE);
  if (OB_ISNULL(node)) {
  } else if (node->json_type() == ObJsonNodeType::J_STRING) {
    ObJsonString* j_str = static_cast<ObJsonString*>(node);
    ObString str = j_str->get_str();
    if (OB_FAIL(ObJsonSchemaUtils::set_type_by_string(str, s_type))) {
      LOG_WARN( "fail to get type", K(str), K(ret));
    }
  } else if (node->json_type() == ObJsonNodeType::J_ARRAY) {
    int array_size = node->element_count();
    for (int i = 0; i < array_size && OB_SUCC(ret); ++i) {
      ObIJsonBase* tmp_node = nullptr;
      if (OB_FAIL(node->get_array_element(i, tmp_node))) {
        LOG_WARN( "fail to get node", K(i), K(array_size), K(ret));
      } else if (tmp_node->json_type() == ObJsonNodeType::J_STRING) {
        ObString str(tmp_node->get_data_length(), tmp_node->get_data());
        if (OB_FAIL(ObJsonSchemaUtils::set_type_by_string(str, s_type))) {
          LOG_WARN( "fail to get type", K(i), K(array_size), K(ret));
        } else if (s_type.error_type_ == 1) {
          s_type.error_type_ = 0;
        }
      } // not string, ignore
    }
    if (OB_SUCC(ret) && s_type.flags_ == 0) {
      s_type.error_type_ = 1;
    }
  } else { // key word "type" is special, if defined type, it must be checked, even is meaningless definition(such as wrong type or error string);
    s_type.error_type_ = 1;
  }

  if (OB_SUCC(ret) && s_type.flags_ != 0) {
    ObJsonUint* type_value = nullptr;
    if (OB_ISNULL(type_value = OB_NEWx(ObJsonUint, allocator_, s_type.flags_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN( "fail to alloc memory for array json node", K(ret));
    } else if (OB_FAIL(generate_schema_and_record(ObJsonSchemaItem::TYPE, type_value,
                                                  schema_vec_stk, is_composition, comp_array))) {
      LOG_WARN( "fail to add type schema", K(ret));
    }
  }
  return ret;
}

// Previously to Draft 2019-09, dependentRequired and dependentSchemas were one keyword called dependencies.
// If the dependency value was an array, it would behave like dependentRequired.
// If the dependency value was a object, it would behave like dependentSchemas.
int ObJsonSchemaTree::get_dep_schema_if_defined(ObJsonObject* json_schema,
                                                ObIArray<ObJsonNode*> &schema_vec_stk,
                                                ObJsonSubSchemaKeywords& key_words,
                                                const bool& is_composition,
                                                ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  ObJsonNode* node = json_schema->get_value(ObJsonSchemaItem::DEPENDENCIES);
  if (OB_ISNULL(node)) {
    // didn't define, its normal
  } else if (node->json_type() == ObJsonNodeType::J_OBJECT && node->element_count() > 0) {
    json_schema = static_cast<ObJsonObject*>(node);
    ObJsonObject* deps_require_node = nullptr;
    int count_schema_required = 0;
    for (int i = 0; OB_SUCC(ret) && i < json_schema->element_count() && i >= 0; ++i) {
      ObString key;
      ObJsonNode* value = nullptr;
      ObJsonObject* origin_schema = nullptr;
      if (OB_FAIL(json_schema->get_value_by_idx(i, key, value))) {
        LOG_WARN("fail to get key-value.",  K(i), K(json_schema->element_count()), K(ret));
      } else if (key_words.dep_schema_ == 0 && value->json_type() == ObJsonNodeType::J_OBJECT && value->element_count() > 0) {
        // value is not subschema, ignore in mysql, raise error in oracle
        key_words.dep_schema_ = 1;
      } else if (value->json_type() == ObJsonNodeType::J_ARRAY && value->element_count() > 0) {
        if (OB_ISNULL(deps_require_node)
            && OB_ISNULL(deps_require_node = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_FAIL(deps_require_node->add(key, value, true, true))) {
          LOG_WARN("fail to add key-value.", K(key), K(ret));
        } else if (OB_FAIL(json_schema->remove(key))) {
          LOG_WARN("fail to remove origin key-value.", K(key), K(ret));
        } else {
          ++count_schema_required;
          --i;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (count_schema_required > 0
              && OB_FAIL(generate_schema_and_record(ObJsonSchemaItem::DEPENDENTREQUIRED, deps_require_node,
                                                    schema_vec_stk, is_composition, comp_array))) {
      LOG_WARN( "fail to add type schema", K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaTree::check_keywords_by_type(const ObJsonSchemaType& schema_type,
                                             ObJsonObject* origin_schema,
                                             ObIArray<ObJsonNode*> &schema_vec_stk,
                                             ObJsonSubSchemaKeywords& key_words,
                                             const bool& is_composition,
                                             ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  if (schema_type.flags_ == 0) {
    if (OB_FAIL(check_keywords_of_string(origin_schema, schema_vec_stk, is_composition, comp_array))
        || OB_FAIL(check_keywords_of_number(origin_schema, schema_vec_stk, is_composition, comp_array))
        || OB_FAIL(check_keywords_of_object(origin_schema, schema_vec_stk, key_words, is_composition, comp_array))
        || OB_FAIL(check_keywords_of_array(origin_schema, schema_vec_stk, key_words, is_composition, comp_array))) {
        LOG_WARN("fail to get check keywords", K(ret));
      }
  } else {
    if (OB_SUCC(ret) && schema_type.string_ == 1) {
      ret = check_keywords_of_string(origin_schema, schema_vec_stk, is_composition, comp_array);
    }
    if (OB_SUCC(ret) && (schema_type.number_ == 1 || schema_type.integer_ == 1)) {
      ret = check_keywords_of_number(origin_schema, schema_vec_stk, is_composition, comp_array);
    }
    if (OB_SUCC(ret) && schema_type.object_ == 1) {
      ret = check_keywords_of_object(origin_schema, schema_vec_stk, key_words, is_composition, comp_array);
    }
    if (OB_SUCC(ret) && schema_type.array_ == 1) {
      ret = check_keywords_of_array(origin_schema, schema_vec_stk, key_words, is_composition, comp_array);
    }
  }
  return ret;
}

int ObJsonSchemaTree::check_keywords_of_string(ObJsonObject* origin_schema,
                                               ObIArray<ObJsonNode*> &schema_vec_stk,
                                               const bool& is_composition,
                                               ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  if (OB_FAIL(handle_positive_int_keywords(ObJsonSchemaItem::MAX_LEN, origin_schema, schema_vec_stk, is_composition, comp_array))
   || OB_FAIL(handle_positive_int_keywords(ObJsonSchemaItem::MIN_LEN, origin_schema, schema_vec_stk, is_composition, comp_array))) {
      LOG_WARN( "fail to add type max/min length", K(ret));
  } else {
    ObJsonNode* node = origin_schema->get_value(ObJsonSchemaItem::PATTERN);
    if (OB_ISNULL(node)) {
    // didn't define, its normal
    } else if (node->element_count() == 0) {
    } else if (node->json_type() == ObJsonNodeType::J_STRING) {
      ObString pattern = ObString(node->get_data_length(), node->get_data());
      bool valid_pattern = false;
      if (OB_FAIL(ObJsonSchemaUtils::is_valid_pattern(pattern, str_buf_, valid_pattern))) {
      } else if (!valid_pattern) {
        // invalid, do not record
      } else if (OB_FAIL(generate_schema_and_record(ObJsonSchemaItem::PATTERN, node, schema_vec_stk, is_composition, comp_array))) {
        LOG_WARN( "fail to add type schema", K(ret));
      }
    }
  }
  return ret;
}

int ObJsonSchemaTree::check_keywords_of_number(ObJsonObject* origin_schema,
                                               ObIArray<ObJsonNode*> &schema_vec_stk,
                                               const bool& is_composition,
                                               ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  ObJsonNode* max = origin_schema->get_value(ObJsonSchemaItem::MAXMUM);
  ObJsonNode* min = origin_schema->get_value(ObJsonSchemaItem::MINMUM);
  // for json_schema draft 4, exclusive_maxmum is boolean
  // so, if didn't define maxmun, even there is exclusive_maxmum definition, the definition is meaningless
  if (OB_NOT_NULL(max) && max->is_number()) {
    ObJsonNode* exclusive_max = nullptr;
    if (OB_NOT_NULL(exclusive_max = origin_schema->get_value(ObJsonSchemaItem::EXCLUSIVE_MAXMUM))
      && exclusive_max->json_type() == ObJsonNodeType::J_BOOLEAN
      && exclusive_max->get_boolean() == true) {
      if (OB_FAIL(generate_schema_and_record(ObJsonSchemaItem::EXCLUSIVE_MAXMUM, max, schema_vec_stk, is_composition, comp_array))) {
        LOG_WARN( "fail to add type schema", K(ret));
      }
    } else if (OB_FAIL(generate_schema_and_record(ObJsonSchemaItem::MAXMUM, max, schema_vec_stk, is_composition, comp_array))) {
      LOG_WARN( "fail to add type schema", K(ret));
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(min) && min->is_number()) {
    ObJsonNode* exclusive_min = nullptr;
    if (OB_NOT_NULL(exclusive_min = origin_schema->get_value(ObJsonSchemaItem::EXCLUSIVE_MINMUM))
      && exclusive_min->json_type() == ObJsonNodeType::J_BOOLEAN
      && exclusive_min->get_boolean() == true) {
      if (OB_FAIL(generate_schema_and_record(ObJsonSchemaItem::EXCLUSIVE_MINMUM, min, schema_vec_stk, is_composition, comp_array))) {
        LOG_WARN( "fail to add type schema", K(ret));
      }
    } else if (OB_FAIL(generate_schema_and_record(ObJsonSchemaItem::MINMUM, min, schema_vec_stk, is_composition, comp_array))) {
      LOG_WARN( "fail to add type schema", K(ret));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(handle_keywords_with_number_value(ObJsonSchemaItem::MULTIPLE_OF,
                              origin_schema, schema_vec_stk, is_composition, comp_array, true))) {
      LOG_WARN( "fail to add type maxmum/minmum/mutiple", K(ret));
  }
  return ret;
}

int ObJsonSchemaTree::check_keywords_of_object(ObJsonObject* origin_schema,
                                               ObIArray<ObJsonNode*> &schema_vec_stk,
                                               ObJsonSubSchemaKeywords& key_words,
                                               const bool& is_composition,
                                               ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  if (OB_FAIL(handle_positive_int_keywords(ObJsonSchemaItem::MAX_PROPERTIES, origin_schema,
                                           schema_vec_stk, is_composition, comp_array))
   || OB_FAIL(handle_positive_int_keywords(ObJsonSchemaItem::MIN_PROPERTIES, origin_schema,
                                           schema_vec_stk, is_composition, comp_array))) {
      LOG_WARN( "fail to add type max/min properties", K(ret));
  } else if (OB_FAIL(get_dep_schema_if_defined(origin_schema, schema_vec_stk, key_words,
                                               is_composition, comp_array))) {
    LOG_WARN("fail to get schema dependencies.", K(ret));
  // in mysql mode, required could be anytype, but ignore the values if not string
  // but in oracle mode, it must be string, or else is illegal, should raise error
  // todo: oracle mode adaptation
  } else if (OB_FAIL(handle_keywords_with_specific_type(ObJsonSchemaItem::REQUIRED,
                                                        ObJsonNodeType::J_ARRAY,
                                                        origin_schema, schema_vec_stk,
                                                        is_composition, comp_array))) {
    LOG_WARN( "fail to add type required", K(ret));
  }
  // property with sub_schema, just record, deal with it later
  if (OB_SUCC(ret)) {
    ObJsonNode* node = nullptr;
    if (OB_NOT_NULL(node = origin_schema->get_value(ObJsonSchemaItem::PROPERTIES))
       && node->json_type() == ObJsonNodeType::J_OBJECT && node->element_count() > 0) {
      key_words.properties_ = 1;
    }
    if (OB_NOT_NULL(node = origin_schema->get_value(ObJsonSchemaItem::PATTERN_PRO))
       && node->json_type() == ObJsonNodeType::J_OBJECT && node->element_count() > 0) {
      key_words.pattern_pro_ = 1;
    }
    if (OB_NOT_NULL(node = origin_schema->get_value(ObJsonSchemaItem::ADDITIONAL_PRO))) {
      if (node->json_type() == ObJsonNodeType::J_BOOLEAN) {
        ObJsonArray* add_array = nullptr;
        if (node->get_boolean()) {
        } else if (OB_FAIL(get_addition_pro_value(key_words, origin_schema, add_array))) {
          LOG_WARN( "fail to add type add_pro", K(key_words), K(ret));
        } else if (OB_FAIL(generate_schema_and_record(ObJsonSchemaItem::ADDITIONAL_PRO, add_array,
                                                      schema_vec_stk, is_composition, comp_array))) {
          LOG_WARN( "fail to add additonal schema", K(key_words), K(ret));
        }
      } else if (node->json_type() == ObJsonNodeType::J_OBJECT && node->element_count() > 0) {
        // property with sub_schema, just record, deal with it later
        key_words.additional_pro_ = 1;
      }
    }
  }
  return ret;
}


int ObJsonSchemaTree::add_required_key(ObJsonNode* pro, ObJsonNode* required, ObJsonArray* pro_key_array)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(pro_key_array) || OB_ISNULL(required) || required->json_type() != ObJsonNodeType::J_ARRAY) {
    // didn't define, its normal, do not need to add, ignore
  } else if (OB_ISNULL(pro)) {
    ObJsonArray* array_node = static_cast<ObJsonArray*>(required);
    int size = array_node->element_count();
    for (int i = 0; i < size && OB_SUCC(ret); ++i) {
      ObJsonNode* tmp_node = (*array_node)[i];
      bool valid_pattern = false;
      if (OB_ISNULL(tmp_node) || tmp_node->json_type() !=  ObJsonNodeType::J_STRING) {
        // not string, ignore
      } else {
        ObJsonString* tmp_str = static_cast<ObJsonString*>(tmp_node);
        ObJsonString* str_node = nullptr;
        if (OB_ISNULL(str_node = OB_NEWx(ObJsonString, allocator_, tmp_str->get_str()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc failed.", K(i), K(size), K(ret));
        } else if (OB_FAIL(pro_key_array->append(str_node))) {
          LOG_WARN("append failed.", K(i), K(size), K(ret));
        }
      }
    }
  } else if (pro->json_type() == ObJsonNodeType::J_OBJECT) {
    ObJsonArray* array_node = static_cast<ObJsonArray*>(required);
    ObJsonObject* object_node = static_cast<ObJsonObject*>(pro);
    int size = array_node->element_count();
    for (int i = 0; i < size && OB_SUCC(ret); ++i) {
      ObJsonNode* tmp_node = (*array_node)[i];
      bool valid_pattern = false;
      if (OB_ISNULL(tmp_node) || tmp_node->json_type() !=  ObJsonNodeType::J_STRING) {
        // not string, ignore
      } else {
        ObJsonString* str_node = static_cast<ObJsonString*>(tmp_node);
        if (OB_FAIL(object_node->add(str_node->get_str(), typeless_, true, false, false, true))) {
          if (ret == OB_ERR_DUPLICATE_KEY) {
            ret = OB_SUCCESS;
          } // ignore dup key
        }
      }
    } // add required key definition into properties
  }
  return ret;
}

/*
  mysql adaptation, bugfix: 53161405
  in oracle mode and standard json schema, the keyword additionalProperties is relative properties and patternProperties.
  other properties are both additionalProperties, when additionalProperties is false, these definition are illegal.
  but in mysql mode, properties (string type) defined in the required keyword are also considered legal definition.
*/
int ObJsonSchemaTree::get_addition_pro_value(const ObJsonSubSchemaKeywords& key_words, ObJsonObject* origin_schema, ObJsonArray*& add_array, bool check_pattern)
{
  INIT_SUCC(ret);
  add_array = nullptr;
  ObJsonArray* pro_key_array = nullptr;
  ObJsonArray* pattern_key_array = nullptr;
  ObJsonNode* dep_node = origin_schema->get_value(ObJsonSchemaItem::DEPENDENCIES);
  ObJsonNode* required_node = lib::is_mysql_mode() ? origin_schema->get_value(ObJsonSchemaItem::REQUIRED) : nullptr;
  if (OB_ISNULL(add_array = OB_NEWx(ObJsonArray, allocator_, allocator_))
    || OB_ISNULL(pro_key_array = OB_NEWx(ObJsonArray, allocator_, allocator_))
    || OB_ISNULL(pattern_key_array = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc comp_array failed.", K(ret));
  } else if (OB_NOT_NULL(required_node) && required_node->json_type() != ObJsonNodeType::J_ARRAY
            && OB_FALSE_IT(required_node = nullptr)) {
  } else if (OB_NOT_NULL(dep_node) && dep_node->json_type() != ObJsonNodeType::J_OBJECT
            && OB_FALSE_IT(dep_node = nullptr)) {
  } else if (key_words.properties_ == 0 && OB_ISNULL(dep_node) && OB_NOT_NULL(required_node)) {
    if (OB_FAIL(add_required_key(nullptr, required_node, pro_key_array))) {
      LOG_WARN("add required key failed.", K(ret));
    }
  } else if ((key_words.properties_ == 1 && OB_ISNULL(dep_node))
            || (OB_NOT_NULL(dep_node) && key_words.properties_ == 0)) {
    // if properties and dependencies only defined one
    ObJsonNode* node = (dep_node == nullptr) ? origin_schema->get_value(ObJsonSchemaItem::PROPERTIES) : dep_node;
    if (OB_FAIL(add_required_key(node, required_node, pro_key_array))) {
      LOG_WARN("add required key failed.", K(ret));
    } else if (OB_FAIL(ObJsonSchemaUtils::collect_key(node, allocator_, pro_key_array, str_buf_))) {
      LOG_WARN("add key failed.", K(ret));
    }
  } else if (key_words.properties_ == 1 && OB_NOT_NULL(dep_node)) {
    // if defined properties and dependencies at the same time
    ObJsonNode* pro_node = origin_schema->get_value(ObJsonSchemaItem::PROPERTIES);
    if (OB_FAIL(add_required_key(pro_node, required_node, pro_key_array))) {
      LOG_WARN("add required key failed.", K(ret));
    } else if (OB_FAIL(ObJsonSchemaUtils::collect_key(pro_node, allocator_, pro_key_array, str_buf_))) {
      LOG_WARN("add key failed.", K(ret));
    } else {
      ObJsonObject* dep_obj = static_cast<ObJsonObject*>(dep_node);
      ObJsonObject* pro_obj = static_cast<ObJsonObject*>(pro_node);
      int size = dep_obj->element_count();
      for (int i = 0; i < size && OB_SUCC(ret); ++i) {
        ObJsonNode* tmp_node = nullptr;
        ObString key;
        if (OB_FAIL(dep_obj->get_key_by_idx(i, key))) {
          LOG_WARN("get key failed.", K(size), K(i), K(key), K(ret));
        } else if (OB_NOT_NULL(tmp_node = pro_obj->get_value(key))) { // duplicate key, do nothing
        } else {
          ObJsonString* str_node = nullptr;
          if (OB_ISNULL(str_node = OB_NEWx(ObJsonString, allocator_, key))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc failed.", K(size), K(i), K(key), K(ret));
          } else if (OB_FAIL(pro_key_array->append(str_node))) {
            LOG_WARN("append failed.", K(size), K(i), K(key), K(ret));
          }
        }
      } // add dep key
    } // collect pro key first
  }

  // add pattern node
  if (OB_FAIL(ret)) {
  } else if (key_words.pattern_pro_ == 1) {
    ObJsonNode* patter_node = origin_schema->get_value(ObJsonSchemaItem::PATTERN_PRO);
    if (OB_FAIL(ObJsonSchemaUtils::collect_key(patter_node, allocator_, pattern_key_array, str_buf_, check_pattern))) {
      LOG_WARN("add key failed.", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(add_array->append(pro_key_array))) {
    LOG_WARN("append failed.", K(ret));
  } else if (OB_FAIL(add_array->append(pattern_key_array))) {
    LOG_WARN("append failed.", K(ret));
  }
  return ret;
}

int ObJsonSchemaTree::check_keywords_of_array(ObJsonObject* origin_schema,
                                              ObIArray<ObJsonNode*> &schema_vec_stk,
                                              ObJsonSubSchemaKeywords& key_words,
                                              const bool& is_composition,
                                              ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  ObJsonNode* node = nullptr;
  int tuple_items_size = 0;
  if (OB_FAIL(handle_positive_int_keywords(ObJsonSchemaItem::MAX_ITEMS, origin_schema,
                                           schema_vec_stk, is_composition, comp_array))
   || OB_FAIL(handle_positive_int_keywords(ObJsonSchemaItem::MIN_ITEMS, origin_schema,
                                           schema_vec_stk, is_composition, comp_array))) {
      LOG_WARN( "fail to add type max/min items", K(ret));
  } else if (OB_NOT_NULL(node = origin_schema->get_value(ObJsonSchemaItem::ITEMS))) {
    // property with sub_schema, just record, deal with it later
    if (node->json_type() == ObJsonNodeType::J_OBJECT && node->element_count() > 0) {
      // list item, valid for all element in array
      key_words.items_ = 1;
    } else if (node->json_type() == ObJsonNodeType::J_ARRAY && node->element_count() > 0) {
      if (OB_FAIL(ObJsonSchemaUtils::is_all_children_subschema(node))) {
        // tuple items, each element has its own schema, check if all schema element
        // if not, raise error
        LOG_WARN("illegal subschema in item array", K(ret));
      } else {
        key_words.tuple_items_ = 1;
        tuple_items_size = node->element_count();
      }
    } // not object or array, ignore in mysql, raise error in oracle
  } // check keyword: items

  if (OB_FAIL(ret)) {
  } else if (key_words.tuple_items_ != 1) {
    // if not tuple items, all items in array shoud obey item schema, there is no additonal items
    // so, the key_word: additional_items is meaningless, ignore it
    key_words.additional_items_ = 0;
  } else if (OB_NOT_NULL(node = origin_schema->get_value(ObJsonSchemaItem::ADDITIONAL_ITEMS))) {
    if (node->json_type() == ObJsonNodeType::J_BOOLEAN) {
      // if ADDITIONAL_ITEMS == true, do not need check
      ObJsonInt* tuple_size = nullptr;
      if (node->get_boolean()) {
      } else if (OB_ISNULL(tuple_size = OB_NEWx(ObJsonInt, allocator_, tuple_items_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to init schema tree.", K(ret));
      } else if (OB_FAIL(generate_schema_and_record(ObJsonSchemaItem::ADDITIONAL_ITEMS, tuple_size,
                                                    schema_vec_stk, is_composition, comp_array))) {
        LOG_WARN( "fail to add additonal schema", K(ret));
      }
    } else if (node->json_type() == ObJsonNodeType::J_OBJECT && node->element_count() > 0) {
      // property with sub_schema, just record, deal with it later
      key_words.additional_items_ = 1;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(node = origin_schema->get_value(ObJsonSchemaItem::UNIQUE_ITEMS))) {
    if (node->json_type() == ObJsonNodeType::J_BOOLEAN && node->get_boolean()) {
      if (OB_FAIL(generate_schema_and_record(ObJsonSchemaItem::UNIQUE_ITEMS, node, schema_vec_stk, is_composition, comp_array))) {
        LOG_WARN( "fail to add type schema", K(ret));
      }
    }
  }
  return ret;
}

int ObJsonSchemaTree::handle_keywords_with_subschemas(ObJsonSubSchemaKeywords& key_words, ObJsonObject* json_schema,
                                                      ObIArray<ObJsonNode*> &schema_vec_stk,
                                                      bool is_composition, ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  ObArray<ObJsonObject*> pro_array;
  pro_array.set_block_size(SCHEMA_DEFAULT_PAGE_SIZE);

  // deal with property
  ObJsonObject* pro_schema = nullptr;
  if (key_words.properties_ == 1) {
    pro_schema = json_schema;
    if (OB_FAIL(handle_properties(pro_schema, is_composition, comp_array, pro_array))) {
      LOG_WARN("fail to handle properties", K(ret));
    }
  }
  if (OB_SUCC(ret) && key_words.pattern_pro_ == 1) {
    if (OB_FAIL(handle_pattern_properties(json_schema, pro_schema, is_composition, comp_array, pro_array))) {
      LOG_WARN("fail to handle properties", K(ret));
    }
  }
  if (OB_SUCC(ret) && key_words.additional_pro_ == 1) {
    if (OB_FAIL(handle_additional_properties(key_words, json_schema, is_composition, comp_array))) {
      LOG_WARN("fail to handle properties", K(ret));
    }
  }
  pro_array.destroy();
  // the key_words.items_ and key_words.tuple_items_ wouldn't be true at the same time
  if (OB_FAIL(ret)) {
  } else if (key_words.items_ == 1) {
    if (OB_FAIL(handle_array_schema(json_schema, is_composition, comp_array, false))) {
      LOG_WARN("fail to handle items", K(ret));
    }
  } else if (key_words.tuple_items_ == 1) {
    if (OB_FAIL(handle_array_tuple_schema(json_schema, is_composition, comp_array))) {
      LOG_WARN("fail to handle tuple items", K(ret));
    } else if (key_words.additional_items_ == 1
      && OB_FAIL(handle_array_schema(json_schema, is_composition, comp_array, true))) {
      LOG_WARN("fail to handle additional items", K(ret));
    }
  }
  if (OB_SUCC(ret) && key_words.dep_schema_ == 1) {
    if (!is_composition
        && OB_FAIL(handle_unnested_dependencies(json_schema))) {
      LOG_WARN("fail to handle unnested_dependent_schemas.", K(ret));
    } else if (is_composition
      && OB_FAIL(handle_nested_dependencies(json_schema, comp_array))) {
      LOG_WARN("fail to handle nested_dependent_schemas.", K(ret));
    }
  }
  if (OB_SUCC(ret) && key_words.all_of_ == 1) {
    if (!is_composition
        && OB_FAIL(handle_unnested_composition(ObJsonSchemaItem::ALLOF, json_schema))) {
      LOG_WARN("fail to handle unnested_composition_allOf.", K(ret));
    } else if (is_composition
      && OB_FAIL(handle_nested_composition(ObJsonSchemaItem::ALLOF, json_schema, comp_array))) {
      LOG_WARN("fail to handle nested_composition_allOf.", K(ret));
    }
  }
  if (OB_SUCC(ret) && key_words.any_of_ == 1) {
    if (!is_composition
        && OB_FAIL(handle_unnested_composition(ObJsonSchemaItem::ANYOF, json_schema))) {
      LOG_WARN("fail to handle unnested_composition_anyOf.", K(ret));
    } else if (is_composition
      && OB_FAIL(handle_nested_composition(ObJsonSchemaItem::ANYOF, json_schema, comp_array))) {
      LOG_WARN("fail to handle nested_composition_anyOf.", K(ret));
    }
  }
  if (OB_SUCC(ret) && key_words.one_of_ == 1) {
    if (!is_composition
        && OB_FAIL(handle_unnested_composition(ObJsonSchemaItem::ONEOF, json_schema))) {
      LOG_WARN("fail to handle unnested_composition_oneOf.", K(ret));
    } else if (is_composition
      && OB_FAIL(handle_nested_composition(ObJsonSchemaItem::ONEOF, json_schema, comp_array))) {
      LOG_WARN("fail to handle nested_composition_oneOf.", K(ret));
    }
  }
  if (OB_SUCC(ret) && key_words.not_ == 1) {
    if (!is_composition
        && OB_FAIL(handle_unnested_not(json_schema))) {
      LOG_WARN("fail to handle unnested_composition_not.", K(ret));
    } else if (is_composition
      && OB_FAIL(handle_nested_not(json_schema, comp_array))) {
      LOG_WARN("fail to handle nested_composition_not.", K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaTree::handle_unnested_dependencies(ObJsonObject* json_schema)
{
  INIT_SUCC(ret);
  ObJsonObject* dep_schema_value = nullptr;
  if (OB_FAIL(ObJsonSchemaUtils::json_doc_move_to_key(ObJsonSchemaItem::DEPENDENCIES, json_schema))) {
    LOG_WARN("json doc move to key failed", K(ret));
  } else if (OB_ISNULL(dep_schema_value = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc nested_value failed.", K(ret));
  } else {
    int key_size = json_schema->element_count();
    ObJsonNode* value = nullptr;
    ObJsonArray* comp_array = nullptr;
    for (int i = 0; OB_SUCC(ret) && i < key_size; ++i) {
      ObString key;
      ObJsonObject* origin_schema = nullptr;
      if (OB_FAIL(json_schema->get_value_by_idx(i, key, value))) {
        LOG_WARN("fail to get key-value.", K(i), K(ret));
      } else if (value->json_type() != ObJsonNodeType::J_OBJECT) {
        // value is not subschema, ignore in mysql, raise error in oracle
      } else if (OB_FALSE_IT(origin_schema = static_cast<ObJsonObject*>(value))) {
      } else if (OB_ISNULL(comp_array = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc comp_array failed.", K(i), K(ret));
      } else if (OB_FAIL(inner_build_schema_tree(origin_schema, true, comp_array))) {
        LOG_WARN("recursive failed. ", K(i), K(ret));
      } else if (OB_FAIL(dep_schema_value->add(key, comp_array, true, true, false))) {
        LOG_WARN("fail to add dep_schema_value.", K(i), K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(json_schema_add_dep_value(dep_schema_value))) {
      LOG_WARN("fail to add dependentSchemas.", K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaTree::handle_nested_dependencies(ObJsonObject* json_schema, ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  ObJsonObject* dep_key = nullptr;
  ObJsonObject* dep_value = nullptr;
  if (OB_FAIL(ObJsonSchemaUtils::json_doc_move_to_key(ObJsonSchemaItem::DEPENDENCIES, json_schema))) {
    LOG_WARN("json doc move to key failed", K(ret));
  } else if (OB_ISNULL(dep_key = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc nested_key failed.", K(ret));
  } else if (OB_ISNULL(dep_value = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc nested_value failed.", K(ret));
  } else if (OB_FAIL(dep_key->add(ObJsonSchemaItem::DEPENDENTSCHEMAS, dep_value, true, false, false))) {
    LOG_WARN("fail to add nested node.", K(ret));
  } else {
    int key_size = json_schema->element_count();
    ObJsonNode* value = nullptr;
    for (int i = 0; OB_SUCC(ret) && i < key_size; ++i) {
      ObString key;
      ObJsonObject* origin_schema = nullptr;
      ObJsonArray* sub_dep_array = nullptr;
      if (OB_FAIL(json_schema->get_value_by_idx(i, key, value))) {
        LOG_WARN("fail to get key-value.", K(i), K(key), K(ret));
      } else if (value->json_type() != ObJsonNodeType::J_OBJECT) {
        // value is not subschema, ignore in mysql, raise error in oracle
      } else if (OB_FALSE_IT(origin_schema = static_cast<ObJsonObject*>(value))) {
      } else if (OB_ISNULL(sub_dep_array = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc comp_array failed.", K(i), K(ret));
      } else if (OB_FAIL(inner_build_schema_tree(origin_schema, true, sub_dep_array))) {
        LOG_WARN("recursion failed. ", K(i), K(ret));
      } else if (OB_FAIL(dep_value->add(key, sub_dep_array, true, false, false))) {
        LOG_WARN("json schema stk move to key failed", K(i), K(key), K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(comp_array->append(dep_key))) {
      LOG_WARN("fail to add nested dependencies key.", K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaTree::handle_properties(ObJsonObject*& json_schema, bool is_composition,
                                        ObJsonArray* comp_array, ObIArray<ObJsonObject*>& pro_array)
{
  INIT_SUCC(ret);
  if (OB_FAIL(all_move_to_key(ObJsonSchemaItem::PROPERTIES, json_schema))) {
    LOG_WARN("fail to move to properties.", K(ret));
  } else if (OB_FAIL(ObJsonSchemaUtils::record_schema_array(cur_schema_stk_, pro_array))) {
    LOG_WARN("fail to record properties.", K(ret));
  } else {
    int key_size = json_schema->element_count();
    ObString key;
    ObJsonNode* value = nullptr;
    for (int i = 0; OB_SUCC(ret) && i < key_size; ++i) {
      ObJsonObject* origin_schema = nullptr;
      if (OB_FAIL(json_schema->get_value_by_idx(i, key, value))) {
        LOG_WARN("fail to get key-value.", K(i), K(ret));
      } else if (value->json_type() != ObJsonNodeType::J_OBJECT) {
        // value is not subschema, ignore in mysql, raise error in oracle
      } else if (OB_FALSE_IT(origin_schema = static_cast<ObJsonObject*>(value))) {
      } else if (OB_FAIL(json_schema_move_to_key(key))) {
        LOG_WARN("json schema stk move to key failed.", K(i), K(key), K(ret));
      } else if (OB_FAIL(inner_build_schema_tree(origin_schema, is_composition, comp_array))) {
        LOG_WARN("recursion failed. ", K(i), K(ret));
      } else if (OB_FAIL(json_schema_back_to_parent())) {
        LOG_WARN("fail to back. ", K(i), K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(json_schema_back_to_parent())) {
      LOG_WARN("fail to back.", K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaTree::handle_pattern_properties(ObJsonObject* json_schema, ObJsonObject* pro_schema, bool is_composition,
                                                ObJsonArray* comp_array, const ObIArray<ObJsonObject*>& pro_array)
{
  INIT_SUCC(ret);
  if (OB_FAIL(all_move_to_key(ObJsonSchemaItem::PATTERN_PRO, json_schema))) {
    LOG_WARN("fail to move to properties.", K(ret));
  } else {
    int key_size = json_schema->element_count();
    ObString key;
    ObJsonNode* value = nullptr;
    for (int i = 0; OB_SUCC(ret) && i < key_size && i >= 0; ++i) {
      ObJsonObject* origin_schema = nullptr;
      int origin_schema_stk_size = cur_schema_stk_.size();
      bool valid_pattern = false;
      if (OB_FAIL(json_schema->get_value_by_idx(i, key, value))) {
        LOG_WARN("fail to get key-value.", K(i), K(ret));
      } else if (value->json_type() != ObJsonNodeType::J_OBJECT) {
        // value is not subschema, ignore in mysql, raise error in oracle
      } else if (OB_FAIL(ObJsonSchemaUtils::is_valid_pattern(key, str_buf_, valid_pattern))) {
      } else if (!valid_pattern) {
        if (OB_FAIL(json_schema->remove(key))) {
          LOG_WARN("fail to remove illegal pattern.", K(i), K(key), K(ret));
        } else {
          --key_size;
          --i;
        }
      } else if (OB_FALSE_IT(origin_schema = static_cast<ObJsonObject*>(value))) {
      } else if (OB_FAIL(json_schema_move_to_key(key))) {
        LOG_WARN("json schema stk move to key failed.", K(i), K(key), K(ret));
      } else if (OB_NOT_NULL(pro_schema) && pro_array.count() > 0
                && OB_FAIL(add_pattern_pro_to_schema(pro_schema, pro_array, key))) {
        LOG_WARN("fail to add patter properties.", K(i), K(ret));
      } else if (OB_FAIL(inner_build_schema_tree(origin_schema, is_composition, comp_array))) {
        LOG_WARN("recursion failed.", K(i), K(ret));
      } else {
        while (origin_schema_stk_size < cur_schema_stk_.size()) {
          cur_schema_stk_.pop();
        }
        if (OB_FAIL(json_schema_back_to_parent())) {
          LOG_WARN("fail to back. ", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(json_schema_back_to_parent())) {
      LOG_WARN("fail to back.", K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaTree::handle_additional_properties(ObJsonSubSchemaKeywords& key_words, ObJsonObject* json_schema,
                                                   bool is_composition, ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  ObJsonArray* pro_array = nullptr;
  if (OB_FAIL(get_addition_pro_value(key_words, json_schema, pro_array, false))) {
    LOG_WARN("fail to get add_pro_value.", K(ret));
  } else if (OB_FAIL(ObJsonSchemaUtils::json_doc_move_to_key(ObJsonSchemaItem::ADDITIONAL_PRO, json_schema))) {
    LOG_WARN("json doc move to key failed", K(ret));
  } else if (OB_FAIL(json_schema_move_to_array(ObJsonSchemaItem::ADDITIONAL_PRO, pro_array))) {
    LOG_WARN("json schema stk move to key failed", K(ret));
  } else if (OB_FAIL(inner_build_schema_tree(json_schema, is_composition, comp_array))) {
    LOG_WARN("recursion failed.", K(ret));
  } else if (OB_FAIL(json_schema_back_to_grandpa())) {
    LOG_WARN("fail to back.", K(ret));
  }
  return ret;
}

int ObJsonSchemaTree::handle_array_schema(ObJsonObject* json_schema, bool is_composition,
                                          ObJsonArray*comp_array, bool is_additonal)
{
  INIT_SUCC(ret);
  ObString key_word = is_additonal ? ObJsonSchemaItem::ADDITIONAL_ITEMS : ObJsonSchemaItem::ITEMS;
  if (!is_additonal) {
    if (OB_FAIL(all_move_to_key(key_word, json_schema))) {
      LOG_WARN("fail to move to properties.", K(ret));
    } else if (OB_FAIL(inner_build_schema_tree(json_schema, is_composition, comp_array))) {
      LOG_WARN("recursion failed.", K(ret));
    } else if (OB_FAIL(json_schema_back_to_parent())) {
      LOG_WARN("fail to back.", K(ret));
    }
  } else {
    ObJsonArray* array_schema = nullptr;
    ObJsonBuffer buf(allocator_);
    if (OB_FAIL(ObJsonSchemaUtils::json_doc_move_to_array(ObJsonSchemaItem::ITEMS, json_schema, array_schema))) {
      LOG_WARN("json doc move to key failed", K(ret));
    } else if (OB_FAIL(ObJsonSchemaUtils::json_doc_move_to_key(key_word, json_schema))) {
      LOG_WARN("json doc move to key failed", K(key_word), K(ret));
    } else if (OB_FAIL(json_schema_move_to_key(key_word))) {
      LOG_WARN("json schema stk move to key failed", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(array_schema)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("shouldn't be null.", K(ret));
    } else {
      int size = array_schema->element_count();
      buf.reset();
      if (OB_FAIL(ObJsonSchemaUtils::get_index_str(size, buf))) {
        LOG_WARN("fail to get key", K(buf), K(ret));
      } else if (OB_FAIL(json_schema_move_to_key(ObString(buf.length(), buf.ptr())))) {
        LOG_WARN("json schema stk move to key failed.", K(buf), K(ret));
      } else if (OB_FAIL(inner_build_schema_tree(json_schema, is_composition, comp_array))) {
        LOG_WARN("recursion failed. ", K(ret));
      } else if (OB_FAIL(json_schema_back_to_parent())) {
        LOG_WARN("fail to back. ", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(json_schema_back_to_parent())) {
      LOG_WARN("fail to back.", K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaTree::handle_array_tuple_schema(ObJsonObject* json_schema, bool is_composition, ObJsonArray*comp_array)
{
  INIT_SUCC(ret);
  ObJsonArray* array_schema = nullptr;
  ObJsonBuffer buf(allocator_);
  if (OB_FAIL(ObJsonSchemaUtils::json_doc_move_to_array(ObJsonSchemaItem::ITEMS, json_schema, array_schema))) {
    LOG_WARN("json doc move to key failed", K(ret));
  } else if (OB_FAIL(json_schema_move_to_key(ObJsonSchemaItem::TUPLE_ITEMS))) {
    LOG_WARN("json schema stk move to key failed", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(array_schema)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("shouldn't be null.", K(ret));
  } else {
    int size = array_schema->element_count();
    for (int i = 0; OB_SUCC(ret) && i < size; ++i) {
      ObJsonObject* origin_schema = nullptr;
      ObJsonNode* value = (*array_schema)[i];
      buf.reset();
      if (value->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_ERR_TYPE_OF_JSON_SCHEMA;
        LOG_WARN("json schema must be object", K(ret), K(i), K(value->json_type()));
      } else if (OB_FALSE_IT(origin_schema = static_cast<ObJsonObject*>(value))) {
      } else if (OB_FAIL(ObJsonSchemaUtils::get_index_str(i, buf))) {
        LOG_WARN("fail to get key", K(i), K(buf), K(ret));
      } else if (OB_FAIL(json_schema_move_to_key(ObString(buf.length(), buf.ptr())))) {
        LOG_WARN("json schema stk move to key failed.", K(i), K(buf), K(ret));
      } else if (OB_FAIL(inner_build_schema_tree(origin_schema, is_composition, comp_array))) {
        LOG_WARN("recursion failed. ", K(i), K(ret));
      } else if (OB_FAIL(json_schema_back_to_parent())) {
        LOG_WARN("fail to back. ", K(i), K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(json_schema_back_to_parent())) {
    LOG_WARN("fail to back.", K(ret));
  }
  return ret;
}

// if is unnested compisiton, add Composition key word and its value
int ObJsonSchemaTree::handle_unnested_composition(const ObString& key_word, ObJsonObject* json_schema)
{
  INIT_SUCC(ret);
  ObJsonArray* array_schema = nullptr;
  if (OB_FAIL(ObJsonSchemaUtils::json_doc_move_to_array(key_word, json_schema, array_schema))) {
    LOG_WARN("json doc move to key failed", K(key_word), K(ret));
  } else if (OB_ISNULL(array_schema)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("shouldn't be null.", K(ret));
  } else {
    int size = array_schema->element_count();
    for (int i = 0; OB_SUCC(ret) && i < size; ++i) {
      ObJsonObject* origin_schema = nullptr;
      ObJsonArray* comp_array = nullptr;
      ObJsonNode* value = (*array_schema)[i];
      ObString idx_str;
      if (value->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_ERR_TYPE_OF_JSON_SCHEMA;
        LOG_WARN("json schema must be object", K(ret), K(i), K(value->json_type()));
      } else if (OB_FALSE_IT(origin_schema = static_cast<ObJsonObject*>(value))) {
      } else if (OB_ISNULL(comp_array = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc comp_array failed.", K(i), K(ret));
      } else if (OB_FAIL(inner_build_schema_tree(origin_schema, true, comp_array))) {
        LOG_WARN("recursion failed.", K(i), K(ret));
      } else if (OB_FAIL(json_schema_add_comp_value(key_word, comp_array))) {
        LOG_WARN("json schema stk move to key failed", K(i), K(key_word), K(ret));
      }
    }
  }
  return ret;
}

int ObJsonSchemaTree::handle_unnested_not(ObJsonObject* json_schema)
{
  INIT_SUCC(ret);
  if (OB_FAIL(ObJsonSchemaUtils::json_doc_move_to_key(ObJsonSchemaItem::NOT, json_schema))) {
    LOG_WARN("fail to move to properties.", K(ret));
  } else {
    ObJsonArray* comp_array = nullptr;
    if (OB_ISNULL(comp_array = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc comp_array failed.", K(ret));
    } else if (OB_FAIL(inner_build_schema_tree(json_schema, true, comp_array))) {
      LOG_WARN("recursion failed.", K(ret));
    } else {
      int size = cur_schema_stk_.size();
      for (int i = 0; i < size && OB_SUCC(ret); ++i) {
        ObJsonObject* cur_schema = cur_schema_stk_.at(i);
        ObJsonNode* tmp_node = nullptr;
        bool update_old_key = false;
        if (OB_ISNULL(tmp_node = cur_schema->get_value(ObJsonSchemaItem::NOT))) {
          if (OB_FAIL(cur_schema->add(ObJsonSchemaItem::NOT, comp_array, true, false, false))) {
            LOG_WARN("fail to add composition node.", K(i), K(ret));
          }
        } else if (tmp_node->json_type() != ObJsonNodeType::J_ARRAY) {
          ret = OB_ERR_WRONG_VALUE;
          LOG_WARN("must be object type.", K(ret), K(i));
        } else {
          int arr_size = comp_array->element_count();
          ObJsonArray* old_arr = static_cast<ObJsonArray*>(tmp_node);
          for (int i = 0; i < arr_size; ++i) {
            if (OB_FAIL(old_arr->append((*comp_array)[i]))) {
              LOG_WARN("fail to append.", K(ret), K(i));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObJsonSchemaTree::handle_nested_composition(const ObString& key_word, ObJsonObject* json_schema, ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  ObJsonArray* array_schema = nullptr;
  ObJsonObject* nested_key = nullptr;
  ObJsonArray* nested_value = nullptr;
  if (OB_FAIL(ObJsonSchemaUtils::json_doc_move_to_array(key_word, json_schema, array_schema))) {
    LOG_WARN("json doc move to key failed", K(ret));
  } else if (OB_ISNULL(array_schema)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("shouldn't be null.", K(ret));
  } else if (OB_ISNULL(nested_key = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc nested_key failed.", K(ret));
  } else if (OB_ISNULL(nested_value = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc nested_value failed.", K(ret));
  } else if (OB_FAIL(nested_key->add(key_word, nested_value, true, false, false))) {
    LOG_WARN("fail to add nested node.", K(key_word), K(ret));
  } else {
    int size = array_schema->element_count();
    for (int i = 0; OB_SUCC(ret) && i < size; ++i) {
      ObJsonObject* origin_schema = nullptr;
      ObJsonArray* sub_comp_array = nullptr;
      ObJsonNode* value = (*array_schema)[i];
      ObString idx_str;
      if (value->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_ERR_TYPE_OF_JSON_SCHEMA;
        LOG_WARN("json schema must be object", K(ret), K(i), K(value->json_type()));
      } else if (OB_FALSE_IT(origin_schema = static_cast<ObJsonObject*>(value))) {
      } else if (OB_ISNULL(sub_comp_array = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc comp_array failed.", K(i), K(ret));
      } else if (OB_FAIL(inner_build_schema_tree(origin_schema, true, sub_comp_array))) {
        LOG_WARN("recursion failed. ", K(i), K(ret));
      } else if (OB_FAIL(nested_value->append(sub_comp_array))) {
        LOG_WARN("json schema stk move to key failed", K(i), K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(comp_array->append(nested_key))) {
      LOG_WARN("fail to add nested key.", K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaTree::handle_nested_not(ObJsonObject* json_schema, ObJsonArray* comp_array)
{
  INIT_SUCC(ret);
  ObJsonArray* array_schema = nullptr;
  ObJsonObject* nested_key = nullptr;
  ObJsonArray* nested_value = nullptr;
  if (OB_FAIL(ObJsonSchemaUtils::json_doc_move_to_key(ObJsonSchemaItem::NOT, json_schema))) {
    LOG_WARN("fail to move to properties.", K(ret));
  } else if (OB_ISNULL(nested_key = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc nested_key failed.", K(ret));
  } else if (OB_ISNULL(nested_value = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc nested_value failed.", K(ret));
  } else if (OB_FAIL(nested_key->add(ObJsonSchemaItem::NOT, nested_value, true, false, false))) {
    LOG_WARN("fail to add nested node.", K(ret));
  } else if (OB_FAIL(inner_build_schema_tree(json_schema, true, nested_value))) {
    LOG_WARN("recursion failed. ", K(ret));
  } else if (OB_FAIL(comp_array->append(nested_key))) {
    LOG_WARN("fail to add nested key.", K(ret));
  }
  return ret;
}

int ObJsonSchemaTree::all_move_to_key(const ObString& key, ObJsonObject*& json_schema)
{
  INIT_SUCC(ret);
  if (OB_FAIL(ObJsonSchemaUtils::json_doc_move_to_key(key, json_schema))) {
    LOG_WARN("json doc move to key failed", K(key), K(ret));
  } else if (OB_FAIL(json_schema_move_to_key(key))) {
    LOG_WARN("json schema stk move to key failed", K(key), K(ret));
  }
  return ret;
}

int ObJsonSchemaTree::json_schema_move_to_key(const ObString& key)
{
  INIT_SUCC(ret);
  int size = cur_schema_stk_.size();
  for (int i = 0; i < size && OB_SUCC(ret); ++i) {
    ObJsonObject* cur_schema = cur_schema_stk_.at(i);
    ObJsonNode* tmp_node = nullptr;
    if (OB_ISNULL(tmp_node = cur_schema->get_value(key))) {
      ObJsonObject* key_value = nullptr;
      if (OB_ISNULL(key_value = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc composition node.", K(key), K(i), K(size), K(ret));
      } else if (OB_FAIL(cur_schema->add(key, key_value, true, false, false))) {
        LOG_WARN("fail to add composition node.", K(key), K(i), K(size), K(ret));
      } else {
        cur_schema = key_value;
      }
    } else if (tmp_node->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_ERR_WRONG_VALUE;
      LOG_WARN("must be object type.", K(ret), K(size), K(i), K(key));
    } else {
      cur_schema = static_cast<ObJsonObject*>(tmp_node);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(cur_schema_stk_.set(i, cur_schema))) {
      LOG_WARN("fail to move.", K(ret), K(size), K(i), K(key));
    }
  }
  return ret;
}

int ObJsonSchemaTree::json_schema_move_to_array(const ObString& key, ObJsonArray* array_val)
{
  INIT_SUCC(ret);
  int size = cur_schema_stk_.size();
  for (int i = 0; i < size && OB_SUCC(ret); ++i) {
    ObJsonObject* cur_schema = cur_schema_stk_.at(i);
    ObJsonArray* key_value = nullptr;
    ObJsonNode* tmp_node = nullptr;
    ObJsonObject* object_val = nullptr;
    if (OB_ISNULL(tmp_node = cur_schema->get_value(key))) {
      if (OB_ISNULL(key_value = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc composition node.", K(key), K(i), K(size), K(ret));
      } else if (OB_FAIL(cur_schema->add(key, key_value, true, false, false))) {
        LOG_WARN("fail to add composition node.", K(key), K(i), K(size), K(ret));
      }
    } else if (tmp_node->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_ERR_WRONG_VALUE;
      LOG_WARN("must be object type.", K(ret), K(i), K(key));
    } else {
      key_value = static_cast<ObJsonArray*>(tmp_node);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(key_value->append(array_val))) {
      LOG_WARN("fail to append array val.", K(ret), K(i), K(key));
    } else if (OB_ISNULL(object_val = OB_NEWx(ObJsonObject, allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc composition node.", K(key), K(i), K(size), K(ret));
    } else if (OB_FAIL(key_value->append(object_val))) {
      LOG_WARN("fail to append obj val.", K(ret), K(key), K(i), K(size));
    } else if (OB_FAIL(cur_schema_stk_.set(i, object_val))) {
      LOG_WARN("fail to move.", K(key), K(i), K(size), K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaTree::json_schema_add_comp_value(const ObString& key, ObJsonArray* new_array_val)
{
  INIT_SUCC(ret);
  int size = cur_schema_stk_.size();
  for (int i = 0; i < size && OB_SUCC(ret); ++i) {
    ObJsonObject* cur_schema = cur_schema_stk_.at(i);
    ObJsonNode* tmp_node = nullptr;
    ObJsonArray* comp_array = nullptr;
    if (OB_ISNULL(tmp_node = cur_schema->get_value(key))) {
      ObJsonArray* key_value = nullptr;
      if (OB_ISNULL(key_value = OB_NEWx(ObJsonArray, allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc composition node.", K(key), K(i), K(size), K(ret));
      } else if (OB_FAIL(cur_schema->add(key, key_value, true, false, false))) {
        LOG_WARN("fail to add composition node.", K(key), K(i), K(size), K(ret));
      } else {
        comp_array = key_value;
      }
    } else if (tmp_node->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_ERR_WRONG_VALUE;
      LOG_WARN("must be object type.", K(ret), K(i), K(key));
    } else {
      comp_array = static_cast<ObJsonArray*>(tmp_node);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(comp_array->append(new_array_val))) {
      LOG_WARN("fail to move.", K(ret), K(i), K(key));
    }
  }
  return ret;
}

int ObJsonSchemaTree::json_schema_add_dep_value(ObJsonObject* dep_val)
{
  INIT_SUCC(ret);
  int size = cur_schema_stk_.size();
  for (int i = 0; i < size && OB_SUCC(ret); ++i) {
    ObJsonObject* cur_schema = cur_schema_stk_.at(i);
    ObJsonNode* tmp_node = nullptr;
    ObJsonObject* origin_dep = nullptr;
    if (OB_ISNULL(tmp_node = cur_schema->get_value(ObJsonSchemaItem::DEPENDENTSCHEMAS))) {
      if (OB_FAIL(cur_schema->add(ObJsonSchemaItem::DEPENDENTSCHEMAS, dep_val, true, false, false))) {
        LOG_WARN("fail to add composition node.", K(i), K(size), K(ret));
      }
    } else if (tmp_node->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_ERR_WRONG_VALUE;
      LOG_WARN("must be object type.", K(i), K(size), K(ret));
    } else {
      origin_dep = static_cast<ObJsonObject*>(tmp_node);
      int new_key_size = dep_val->element_count();
      for (int i = 0; i < new_key_size && OB_SUCC(ret); ++i) {
        ObString key;
        ObJsonNode* val = nullptr;
        ObJsonNode* origin_val = nullptr;
        ObJsonArray* arr_val;
        if (OB_FAIL(dep_val->get_value_by_idx(i, key, val))) {
          LOG_WARN("fail to get object.", K(i), K(size), K(ret));
        } else if (OB_ISNULL(origin_val = origin_dep->get_value(key))) {
          if (OB_FAIL(origin_dep->add(key, val, true, false, false))) {
            LOG_WARN("fail to add key-value.", K(ret), K(i), K(key), K(val));
          }
        } else if (origin_val->json_type() != ObJsonNodeType::J_ARRAY) {
          ret = OB_ERR_WRONG_VALUE;
          LOG_WARN("must be array type.", K(ret), K(i));
        } else {
          arr_val = static_cast<ObJsonArray*>(origin_val);
          if (OB_FAIL(arr_val->append(val))) {
             LOG_WARN("fail to add key-value.", K(ret), K(i), K(key), K(val));
          }
        }
      }
    }
  }
  return ret;
}

int ObJsonSchemaTree::json_schema_back_to_parent()
{
  INIT_SUCC(ret);
  int size = cur_schema_stk_.size();
  for (int i = 0; i < size && OB_SUCC(ret); ++i) {
    ObJsonObject* cur_schema = cur_schema_stk_.at(i);
    ObJsonNode* parent = cur_schema->get_parent();
    if (OB_ISNULL(parent)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("should have parent", K(ret), K(i));
    } else if (parent->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_ERR_WRONG_VALUE;
      LOG_WARN("must be object type.", K(ret), K(parent->json_type()));
    } else if (OB_FAIL(cur_schema_stk_.set(i, static_cast<ObJsonObject*>(parent)))) {
      LOG_WARN("fail to move.", K(ret), K(i));
    }
  }
  return ret;
}

int ObJsonSchemaTree::json_schema_back_to_grandpa()
{
  INIT_SUCC(ret);
  int size = cur_schema_stk_.size();
  for (int i = 0; i < size && OB_SUCC(ret); ++i) {
    ObJsonObject* cur_schema = cur_schema_stk_.at(i);
    ObJsonNode* parent = cur_schema->get_parent();
    ObJsonNode* grandpa = nullptr;
    if (OB_ISNULL(parent)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("should have parent", K(ret), K(i));
    } else if (OB_FALSE_IT(grandpa = parent->get_parent())) {
    } else if (OB_ISNULL(grandpa)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("should have parent", K(ret), K(i));
    } else if (grandpa->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_ERR_WRONG_VALUE;
      LOG_WARN("must be object type.", K(ret), K(parent->json_type()));
    } else if (OB_FAIL(cur_schema_stk_.set(i, static_cast<ObJsonObject*>(grandpa)))) {
      LOG_WARN("fail to move.", K(ret), K(i));
    }
  }
  return ret;
}

// if a key is valid for pattern properties and properties at the same time,
// the key should check  the schemas both defined on pattern properties and properties
int ObJsonSchemaTree::add_pattern_pro_to_schema(ObJsonObject* pro_schema, const ObIArray<ObJsonObject*>& pro_array, const ObString& pattern_text)
{
  INIT_SUCC(ret);
  int arr_size  = pro_array.count();
  int pro_schema_size = pro_schema->element_count();
  for (int i = 0; i < pro_schema_size && OB_SUCC(ret); ++i) {
    ObString key;
    bool regex_ans = false;
    if (OB_FAIL(pro_schema->get_key_by_idx(i, key))) {
      LOG_WARN("fail to get key-value.", K(i), K(key), K(ret));
    } else if (OB_FAIL(ObJsonSchemaUtils::if_regex_match(key, pattern_text, str_buf_, regex_ans)) || !regex_ans) {
      // doesn't match, do nothing
    } else {
      // search match key, add its value to cur_schema_stk_
      for (int i = 0; i < arr_size && OB_SUCC(ret); ++i) {
        ObJsonObject* json_schema = pro_array.at(i);
        ObJsonNode* match_val = nullptr;
        if (OB_ISNULL(match_val = json_schema->get_value(key))) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("shouldn't be null", K(ret), K(i));
        } else if (match_val->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_ERR_WRONG_VALUE;
          LOG_WARN("must be object type.", K(ret), K(match_val->json_type()));
        } else if (OB_FAIL(cur_schema_stk_.push(static_cast<ObJsonObject*>(match_val)))) {
          LOG_WARN("fail to push.", K(i), K(arr_size), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJsonSchemaValidator::get_json_or_schema_point(ObJsonBuffer& json_pointer, bool is_schema_pointer)
{
  INIT_SUCC(ret);
  ObString res = ObString::make_empty_string();
  ObStack<ObString>* stack_ptr = nullptr;
  if (is_schema_pointer) {
    stack_ptr =  &schema_pointer_;
  } else {
    stack_ptr = &json_pointer_;
  }
  int size = stack_ptr->size();
  for (int i = 0; i < size && OB_SUCC(ret); ++i) {
    if (OB_FAIL(json_pointer.append(stack_ptr->at(i)))) {
      LOG_WARN("fail to append.", K(i), K(json_pointer), K(ret));
    } else if (i + 1 < size && OB_FAIL(json_pointer.append("/"))) {
      LOG_WARN("fail to append.", K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaValidator::schema_validator(ObIJsonBase *json_doc, bool& is_valid)
{
  INIT_SUCC(ret);
  ObIJsonBase *json_schema = nullptr;
  ObArray<ObIJsonBase*> schema_vec;
  schema_vec.set_block_size(SCHEMA_DEFAULT_PAGE_SIZE);
  ObArray<ObJsonSchemaAns> ans_map;
  ans_map.set_block_size(SCHEMA_DEFAULT_PAGE_SIZE);
  is_valid = true;

  if (OB_ISNULL(schema_map_) || OB_ISNULL(json_doc)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("shouldn't be null.", KPC(schema_map_), KPC(json_doc), K(ret));
  } else if (schema_map_->json_type() != ObJsonNodeType::J_ARRAY || schema_map_->element_count() <= 1) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be array.", K(ret), K(schema_map_->json_type()), K(schema_map_->element_count()));
  } else if (OB_FAIL(schema_map_->get_array_element(0, json_schema))) {
    LOG_WARN("fail to get json schema.", K(ret));
  } else if (OB_ISNULL(json_schema) || json_schema->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("shouldn't be null or other type.", KPC(json_schema), K(ret));
  } else if (OB_FAIL(json_pointer_.push(ObJsonSchemaItem::ROOT))) {
    LOG_WARN("fail to push root.", K(ret));
  } else if (OB_FAIL(schema_pointer_.push(ObJsonSchemaItem::ROOT))) {
    LOG_WARN("fail to push root.", K(ret));
  } else if (OB_FAIL(schema_vec.push_back(json_schema))) {
    LOG_WARN("fail to push.", K(ret));
  } else {
    int size = schema_map_->element_count();
    // schema validation only seek, do not need reserve parent stack
    if (json_doc->is_bin()) {
      ObJsonBin* j_bin = static_cast<ObJsonBin*>(json_doc);
      j_bin->set_seek_flag(true);
    }
    for (int i = 0; i < size && OB_SUCC(ret); ++i) {
      ans_map.push_back(ObJsonSchemaAns::JS_NOT_CHCECKED);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(inner_schema_validator(json_doc, schema_vec, ans_map, is_valid))) {
      LOG_WARN("fail to validate.", K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaValidator::get_ans_by_id(ObIJsonBase* j_id, ObIArray<ObJsonSchemaAns> &ans_map, bool& ans)
{
  INIT_SUCC(ret);
  int size = ans_map.count();
  int id = j_id->get_int();
  if (id < size && id > 1) {
    if (ans_map.at(id) == ObJsonSchemaAns::JS_FALSE) {
      ans = false;
    } else {
      ans = true;
    }
  } else {
    ret = OB_INVALID_INDEX;
  }
  return ret;
}

int ObJsonSchemaValidator::inner_schema_validator(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid)
{
  INIT_SUCC(ret);
  ObJsonNodeType json_type = ObJsonNodeType::J_ERROR;
  bool need_recursive = false;
  if (!is_valid) {
  } else if (OB_ISNULL(json_doc) || schema_vec.count() < 1) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("shouldn't be null.", KPC(json_doc), K(ret), K(schema_vec.count()));
  } else if (OB_FAIL(check_all_schema_def(json_doc, schema_vec, is_valid))) {
    LOG_WARN("fail in check schema.", K(ret));
  } else if (!is_valid) {
  } else if (OB_FAIL(check_all_composition_def(json_doc, schema_vec, ans_map))) { // check and fill composition ans
    LOG_WARN("fail in check composition.", K(ret));
  } else if (OB_FALSE_IT(json_type = json_doc->json_type())) {
  } else if (json_type == ObJsonNodeType::J_OBJECT) { // recursion
    if (OB_FAIL(ObJsonSchemaUtils::need_check_recursive(schema_vec, need_recursive, false))) {
      LOG_WARN("fail to check recursive keywords.", K(ret));
    } else if (!need_recursive) {
      // didn't define recursive keywords
    } else if (OB_FAIL(schema_pointer_.push(ObJsonSchemaItem::PROPERTIES))) {
      LOG_WARN("fail to push schema pointer.", K(ret));
    } else if (OB_FAIL(object_recursive_validator(json_doc, schema_vec, ans_map, is_valid))) {
      LOG_WARN("fail in check object child.", K(ret));
    } else if (failed_keyword_.empty()) {
      schema_pointer_.pop();
    }
  } else if (json_type == ObJsonNodeType::J_ARRAY) {
    if (OB_FAIL(ObJsonSchemaUtils::need_check_recursive(schema_vec, need_recursive, true))) {
      LOG_WARN("fail to check recursive keywords.", K(ret));
    } else if (!need_recursive) {
      // didn't define recursive keywords
    } else if (OB_FAIL(schema_pointer_.push(ObJsonSchemaItem::ITEMS))) {
      LOG_WARN("fail to push schema pointer.", K(ret));
    } else if (OB_FAIL(array_recursive_validator(json_doc, schema_vec, ans_map, is_valid))) {
      LOG_WARN("fail in check array child.", K(ret));
    } else if (failed_keyword_.empty()) {
      schema_pointer_.pop();
    }
  } // else: is scalar, end of validation

  // get composition ans
  if (OB_FAIL(ret) || !is_valid) {
  } else if (OB_FAIL(get_schema_composition_ans(json_doc, schema_vec, ans_map, is_valid))) {
    LOG_WARN("fail to get_schema_composition_ans.", K(ret));
  }
  return ret;
}

int ObJsonSchemaValidator::get_schema_composition_ans(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid)
{
  INIT_SUCC(ret);
  // check dep_schema, if is object
  if (!is_valid) {
  } else if (OB_FAIL(composition_ans_recorded_
            && get_vec_schema_composition_ans(json_doc, ObJsonSchemaComp::JS_COMP_DEP, ObJsonSchemaItem::DEPENDENTSCHEMAS, schema_vec, ans_map, is_valid))) {
    LOG_WARN("fail to check comp.", K(ret));
  } else if (!is_valid) {
    failed_keyword_ = ObJsonSchemaItem::DEPENDENCIES;
  } else if (composition_ans_recorded_
            && OB_FAIL(get_vec_schema_composition_ans(json_doc, ObJsonSchemaComp::JS_COMP_ALLOF, ObJsonSchemaItem::ALLOF, schema_vec, ans_map, is_valid))) {
    LOG_WARN("fail to check comp.", K(ret));
  } else if (!is_valid) {
    failed_keyword_ = ObJsonSchemaItem::ALLOF;
  } else if (composition_ans_recorded_
            && OB_FAIL(get_vec_schema_composition_ans(json_doc, ObJsonSchemaComp::JS_COMP_ANYOF, ObJsonSchemaItem::ANYOF, schema_vec, ans_map, is_valid))) {
    LOG_WARN("fail to check comp.", K(ret));
  } else if (!is_valid) {
    failed_keyword_ = ObJsonSchemaItem::ANYOF;
  } else if (OB_FAIL(composition_ans_recorded_
            && get_vec_schema_composition_ans(json_doc, ObJsonSchemaComp::JS_COMP_ONEOF, ObJsonSchemaItem::ONEOF, schema_vec, ans_map, is_valid))) {
    LOG_WARN("fail to check comp.", K(ret));
  } else if (!is_valid) {
    failed_keyword_ = ObJsonSchemaItem::ONEOF;
  } else if (OB_FAIL(get_vec_schema_composition_ans(json_doc, ObJsonSchemaComp::JS_COMP_NOT, ObJsonSchemaItem::NOT, schema_vec, ans_map, is_valid))) {
    LOG_WARN("fail to check comp.", K(ret));
  } else if (!is_valid) {
    failed_keyword_ = ObJsonSchemaItem::NOT;
  }
  return ret;
}

int ObJsonSchemaValidator::get_vec_schema_composition_ans(ObIJsonBase *json_doc, ObJsonSchemaComp comp_idx, const ObString& key, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid)
{
  INIT_SUCC(ret);
  // check dep_schema, if is object
  if (!is_valid) {
  } else {
    int dep_size = 0;
    int vec_size = schema_vec.count();
    for (int i = 0; i < vec_size && OB_SUCC(ret) && is_valid; ++i) {
      ObIJsonBase *tmp_schema = schema_vec.at(i);
      ObIJsonBase *dep_schema = nullptr;
      if (OB_FAIL(tmp_schema->get_object_value(key, dep_schema))) {
        if (ret == OB_SEARCH_NOT_FOUND) { // didn't define property, its normal
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(dep_schema)) {
      } else if (dep_schema->element_count() == 0) {
        if (comp_idx == ObJsonSchemaComp::JS_COMP_NOT) { // not: default result is false
          is_valid = false;
        }
      } else {
        switch (comp_idx) {
          case ObJsonSchemaComp::JS_COMP_DEP : {
            if (OB_FAIL(check_dep_schema(json_doc, dep_schema, ans_map, is_valid))) {
              LOG_WARN("fail to check dep.", K(ret));
            }
            break;
          }
          case ObJsonSchemaComp::JS_COMP_ALLOF : {
            if (OB_FAIL(check_allof_schema(json_doc, dep_schema, ans_map, is_valid))) {
              LOG_WARN("fail to check allof.", K(ret));
            }
            break;
          }
          case ObJsonSchemaComp::JS_COMP_ONEOF : {
            if (OB_FAIL(check_oneof_schema(json_doc, dep_schema, ans_map, is_valid))) {
              LOG_WARN("fail to check oneof.", K(ret));
            }
            break;
          }
          case ObJsonSchemaComp::JS_COMP_ANYOF : {
            if (OB_FAIL(check_anyof_schema(json_doc, dep_schema, ans_map, is_valid))) {
              LOG_WARN("fail to check anyof.", K(ret));
            }
            break;
          }
          case ObJsonSchemaComp::JS_COMP_NOT : {
            if (OB_FAIL(check_not_schema(json_doc, dep_schema, ans_map, is_valid))) {
              LOG_WARN("fail to check not.", K(ret));
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            break;
          }
        } // end switch
      } // need to check comp
    }
  }
  return ret;
}

int ObJsonSchemaValidator::check_dep_schema(ObIJsonBase *json_doc, ObIJsonBase* dep_schema, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid)
{
  INIT_SUCC(ret);
  if (dep_schema->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be object.", K(ret));
  } else {
    int size = dep_schema->element_count();
    for (int i = 0; i < size && OB_SUCC(ret) && is_valid; ++i) {
      ObIJsonBase *tmp_schema = nullptr;
      int tmp_schema_size = 0;
      ObString dep_key;
      ObIJsonBase *dep_value = nullptr;
      if (OB_FAIL(dep_schema->get_object_value(i, dep_key, tmp_schema))) {
        LOG_WARN("fail to get schema array.", K(i), K(ret));
      } else if (OB_ISNULL(tmp_schema)) {
        ret = OB_BAD_NULL_ERROR;
      } else if (OB_FAIL(json_doc->get_object_value(dep_key, dep_value))) {
        if (ret == OB_SEARCH_NOT_FOUND) {
          ret = OB_SUCCESS; // didn't found, do not need check
        }
      } else if (tmp_schema->json_type() != ObJsonNodeType::J_ARRAY) {
        ret = OB_ERR_WRONG_VALUE;
        LOG_WARN("must be array.", K(i), K(ret));
      } else if (OB_FALSE_IT(tmp_schema_size = tmp_schema->element_count())) {
      } else if (tmp_schema_size == 0) {
      } else if (OB_FAIL(check_single_comp_array(json_doc, tmp_schema, ans_map, is_valid))) {
        LOG_WARN("fail to check schema array.", K(i), K(ret));
      } else if (!is_valid) {
        failed_keyword_ = ObJsonSchemaItem::DEPENDENCIES;
      }
    }
  }
  return ret;
}

int ObJsonSchemaValidator::get_single_element_and_check(ObIJsonBase *json_doc, const int& idx, ObIJsonBase* dep_schema, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid)
{
  INIT_SUCC(ret);
  ObIJsonBase *tmp_schema = nullptr;
  int tmp_schema_size = 0;
  if (OB_FAIL(dep_schema->get_array_element(idx, tmp_schema))) {
    LOG_WARN("fail to get schema array.", K(ret));
  } else if (OB_ISNULL(tmp_schema)) {
    ret = OB_BAD_NULL_ERROR;
  } else if (tmp_schema->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be array.", K(tmp_schema->json_type()), K(ret));
  } else if (OB_FALSE_IT(tmp_schema_size = tmp_schema->element_count())) {
  } else if (tmp_schema_size == 0) {
  } else if (OB_FAIL(check_single_comp_array(json_doc, tmp_schema, ans_map, is_valid))) {
    LOG_WARN("fail to chekc schema array.", K(ret));
  }
  return ret;
}


int ObJsonSchemaValidator::check_allof_schema(ObIJsonBase *json_doc, ObIJsonBase* allof_schema, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid)
{
  INIT_SUCC(ret);
  if (allof_schema->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be array.", K(ret));
  } else {
    int size = allof_schema->element_count();
    for (int i = 0; i < size && OB_SUCC(ret) && is_valid; ++i) {
      if (OB_FAIL(get_single_element_and_check(json_doc, i, allof_schema, ans_map, is_valid))) {
        LOG_WARN("fail to check.", K(i), K(ret));
      } else if (!is_valid) {
        failed_keyword_ = ObJsonSchemaItem::ALLOF;
      }
    }
  }
  return ret;
}

int ObJsonSchemaValidator::check_anyof_schema(ObIJsonBase *json_doc, ObIJsonBase* anyof_schema, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid)
{
  INIT_SUCC(ret);
  if (anyof_schema->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be array.", K(anyof_schema->json_type()), K(ret));
  } else {
    int size = anyof_schema->element_count();
    bool anyof = true;
    bool end_seek = false;
    for (int i = 0; i < size && OB_SUCC(ret) && !end_seek; ++i) {
      anyof = true;
      if (OB_FAIL(get_single_element_and_check(json_doc, i, anyof_schema, ans_map, anyof))) {
        LOG_WARN("fail to check.", K(i), K(ret));
      } else if (anyof) {
        end_seek = true;
      }
    }
    is_valid = anyof;
    if (OB_FAIL(ret)) {
    } else if (!is_valid) {
      failed_keyword_ = ObJsonSchemaItem::ANYOF;
    }
  }
  return ret;
}

int ObJsonSchemaValidator::check_oneof_schema(ObIJsonBase *json_doc, ObIJsonBase* oneof_schema, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid)
{
  INIT_SUCC(ret);
  if (oneof_schema->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be array.", K(ret));
  } else {
    int size = oneof_schema->element_count();
    bool oneof = false;
    bool end_seek = false;
    for (int i = 0; i < size && OB_SUCC(ret) && !end_seek; ++i) {
      bool tmp_ans = true;
      if (OB_FAIL(get_single_element_and_check(json_doc, i, oneof_schema, ans_map, tmp_ans))) {
        LOG_WARN("fail to check.", K(i), K(ret));
      } else if (tmp_ans) {
        if (oneof) {
          is_valid = false;
          failed_keyword_ = ObJsonSchemaItem::ONEOF;
          end_seek = true;
        } else {
          oneof = true; // already found one
        }
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      is_valid = oneof; // prevent the situation where all conditions are false
    }
  }
  return ret;
}

int ObJsonSchemaValidator::check_not_schema(ObIJsonBase *json_doc, ObIJsonBase* not_schema, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid)
{
  INIT_SUCC(ret);
  bool tmp_ans = true;
  if (not_schema->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be array.", K(not_schema->json_type()), K(ret));
  } else if (OB_FAIL(check_single_comp_array(json_doc, not_schema, ans_map, tmp_ans))) {
    LOG_WARN("fail to chekc schema array.", K(ret));
  } else {
    is_valid = !tmp_ans;
  }
  return ret;
}

int ObJsonSchemaValidator::check_single_composition_schema(ObIJsonBase *json_doc, ObIJsonBase* single_schema, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid)
{
  INIT_SUCC(ret);
  if (single_schema->element_count() != 1) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be one element.", K(single_schema->element_count()), K(ret));
  } else {
    ObString key;
    ObIJsonBase* ele = nullptr;
    if (OB_FAIL(single_schema->get_object_value(0, key, ele))) {
      LOG_WARN("fail to get value.", K(ret));
    } else if (OB_ISNULL(ele)) {
      ret = OB_BAD_NULL_ERROR;
    } else if (ele->element_count() == 0) {
    } else if (key.length() < strlen(ObJsonSchemaItem::NOT)) {
      ret = OB_ERR_WRONG_VALUE;
    } else {
      switch(key[2]) {
        case 'l': {
          // allOf
          if (OB_FAIL(check_allof_schema(json_doc, ele, ans_map, is_valid))) {
            LOG_WARN("fail to check of.", K(key), K(ret));
          }
          break;
        }
        case 'y': {
          // anyOf
          if (OB_FAIL(check_anyof_schema(json_doc, ele, ans_map, is_valid))) {
            LOG_WARN("fail to check of.", K(key), K(ret));
          }
          break;
        }
        case 'e': {
          // oneOf
          if (OB_FAIL(check_oneof_schema(json_doc, ele, ans_map, is_valid))) {
            LOG_WARN("fail to check of.", K(key), K(ret));
          }
          break;
        }
        case 'p': {
          // dependentSchema
          if (OB_FAIL(check_dep_schema(json_doc, ele, ans_map, is_valid))) {
            LOG_WARN("fail to check of.", K(key), K(ret));
          }
          break;
        }
        case 't': {
          // not
          if (OB_FAIL(check_not_schema(json_doc, ele, ans_map, is_valid))) {
            LOG_WARN("fail to check of.", K(key), K(ret));
          }
          break;
        }
        default: {
          ret = OB_ERR_WRONG_VALUE;
          break;
        }
      }
    }
  }
  return ret;
}

int ObJsonSchemaValidator::check_single_comp_array(ObIJsonBase *json_doc, ObIJsonBase* allof_val, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid)
{
  INIT_SUCC(ret);
  int size = allof_val->element_count();
  for (int i = 0; i < size && OB_SUCC(ret) && is_valid; ++i) {
    ObIJsonBase* tmp_val = nullptr;
    bool ans = true;
    if (OB_FAIL(allof_val->get_array_element(i, tmp_val))) {
      LOG_WARN("fail to get value.", K(i), K(size), K(ret));
    } else if (OB_ISNULL(tmp_val)) {
      ret = OB_BAD_NULL_ERROR;
    } else if (tmp_val->json_type() == ObJsonNodeType::J_INT) {
      if (OB_FAIL(get_ans_by_id(tmp_val, ans_map, ans))) {
        LOG_WARN("fail to get ans.", K(i), K(size), K(ret));
      }
    } else if (tmp_val->json_type() == ObJsonNodeType::J_OBJECT) {
      if (OB_FAIL(check_single_composition_schema(json_doc, tmp_val, ans_map, ans))) {
        LOG_WARN("fail to get ans.", K(i), K(size), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_valid = (is_valid && ans);
    }
  }
  return ret;
}

int ObJsonSchemaValidator::object_recursive_validator(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid)
{
  INIT_SUCC(ret);
  int object_size = json_doc->element_count();
  ObIJsonBase *tmp_value = nullptr;
  ObArray<ObIJsonBase*> recursive_schema_vec;
  recursive_schema_vec.set_block_size(SCHEMA_DEFAULT_PAGE_SIZE);
  for (int i = 0; i < object_size && OB_SUCC(ret) && is_valid; ++i) {
    ObString key;
    if (OB_FAIL(json_doc->get_object_value(i, key, tmp_value))) {
      LOG_WARN("fail to get object value.", K(i), K(object_size), K(key), K(ret));
    } else if (OB_FAIL(collect_schema_by_key(key, json_doc, schema_vec, recursive_schema_vec))) {
      LOG_WARN("fail to collect schema.", K(i), K(object_size), K(key), K(ret));
    } else if (recursive_schema_vec.count() > 0) {
      if (OB_FAIL(json_pointer_.push(key))) {
        LOG_WARN("fail to push schema.", K(i), K(object_size), K(key), K(ret));
      } else if (OB_FAIL(schema_pointer_.push(key))) {
        LOG_WARN("fail to push schema.", K(i), K(object_size), K(key), K(ret));
      } else if (OB_FAIL(inner_schema_validator(tmp_value, recursive_schema_vec, ans_map, is_valid))) {
        LOG_WARN("fail to validate.", K(i), K(object_size), K(key), K(ret));
      } else {
        if (failed_keyword_.empty()) {
          json_pointer_.pop();
          schema_pointer_.pop();
        }
        recursive_schema_vec.reuse();
      }
    } // recursive_schema_vec.count() == 0, no schema definition, do not need check
  }
  return ret;
}

int ObJsonSchemaValidator::collect_schema_by_key(const ObString& key, ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObIJsonBase*> &recursive_vec)
{
  INIT_SUCC(ret);
  int size = schema_vec.count();
  for (int i = 0; i < size && OB_SUCC(ret); ++i) {
    ObIJsonBase* tmp_schema = schema_vec.at(i);
    ObIJsonBase* property = nullptr;
    ObIJsonBase* tmp_value = nullptr;
    if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::PROPERTIES, property))) {
      if (ret == OB_SEARCH_NOT_FOUND) { // didn't define property, its normal
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(property->get_object_value(key, tmp_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) { // didn't define key, its normal
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(tmp_value)) {
    } else if (tmp_value->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_ERR_WRONG_VALUE;
    } else if (OB_FAIL(recursive_vec.push_back(tmp_value))) {
      LOG_WARN("fail to push back.", K(i), K(size), K(key), K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(tmp_value)) { // found in properties, don't need to check pattern properties
    } else if (OB_FALSE_IT(property = nullptr)) {
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::PATTERN_PRO, property)) || OB_ISNULL(property)) {
      if (ret == OB_SEARCH_NOT_FOUND) {  // didn't define pattern property, its normal
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(collect_schema_by_pattern_key(key, property, recursive_vec))) {
      LOG_WARN("fail to collect.", K(i), K(size), K(key), K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FALSE_IT(property = nullptr)) {
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::ADDITIONAL_PRO, property))) {
      if (ret == OB_SEARCH_NOT_FOUND) {  // didn't define addtional property, its normal
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(collect_schema_by_add_key(key, json_doc, property, recursive_vec))) {
      LOG_WARN("fail to collect.", K(i), K(size), K(key), K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaValidator::collect_schema_by_idx(const int& idx, const ObString& idx_str, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObIJsonBase*> &recursive_vec)
{
  INIT_SUCC(ret);
  int size = schema_vec.count();
  for (int i = 0; i < size && OB_SUCC(ret); ++i) {
    ObIJsonBase* tmp_schema = schema_vec.at(i);
    ObIJsonBase* items = nullptr;
    ObIJsonBase* tmp_value = nullptr;
    if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::ITEMS, items))) {
      if (ret == OB_SEARCH_NOT_FOUND) { // didn't define property, its normal
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(items)) {
    } else if (items->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_ERR_WRONG_VALUE;
    } else if (OB_FAIL(recursive_vec.push_back(items))) {
      LOG_WARN("fail to push back.", K(i), K(size), K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FALSE_IT(items = nullptr)) {
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::TUPLE_ITEMS, items)) || OB_ISNULL(items)) {
      if (ret == OB_SEARCH_NOT_FOUND) { // didn't define tuple items, its normal
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(items->get_object_value(idx_str, tmp_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) { // didn't define idx i in tuple, its normal
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(tmp_value)) {
    } else if (tmp_value->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_ERR_WRONG_VALUE;
    } else if (OB_FAIL(recursive_vec.push_back(tmp_value))) {
      LOG_WARN("fail to push back.", K(i), K(size), K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FALSE_IT(items = nullptr)) {
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::ADDITIONAL_ITEMS, items)) || OB_ISNULL(items)) {
      if (ret == OB_SEARCH_NOT_FOUND) {  // didn't define addtional property, its normal
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(collect_schema_by_add_idx(idx, items, recursive_vec))) {
      LOG_WARN("fail to collect.", K(i), K(size), K(ret));
    }
  }
  return ret;
}

int ObJsonSchemaValidator::collect_schema_by_pattern_key(const ObString& key, ObIJsonBase* schema_vec, ObIArray<ObIJsonBase*> &recursive_vec)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(schema_vec)) { // didn't define pattern, its normal
  } else if (schema_vec->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("should be object.", K(ret));
  } else {
    int size = schema_vec->element_count();
    for (int i = 0; i < size && OB_SUCC(ret); ++i) {
      ObString pattern;
      ObIJsonBase* tmp_value = nullptr;
      bool regex_ans = false;
      if (OB_FAIL(schema_vec->get_object_value(i, pattern, tmp_value))) {
        LOG_WARN("fail to get.",K(i), K(size),  K(ret));
      } else if (OB_ISNULL(tmp_value)) {
      } else if (tmp_value->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_ERR_WRONG_VALUE;
      } else if (OB_SUCC(ObJsonSchemaUtils::if_regex_match(key, pattern, str_buf_, regex_ans)) && regex_ans) {
        if (OB_FAIL(recursive_vec.push_back(tmp_value))) {
          LOG_WARN("fail to collect.", K(i), K(size), K(ret));
        }
      } // if not match, ignore
    }
  }
  return ret;
}

int ObJsonSchemaValidator::collect_schema_by_add_key(const ObString& key, ObIJsonBase *json_doc, ObIJsonBase* schema_vec, ObIArray<ObIJsonBase*> &recursive_vec)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(schema_vec)) { // didn't define add_pro, its normal
  } else if (schema_vec->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("should be array.", K(ret));
  } else {
    int size = schema_vec->element_count();
    if (size % ObJsonSchemaTree::ADDITIONAL_PRO_ARRAY_COUNT != 0) {
      ret = OB_ERR_WRONG_VALUE;
    }
    for (int i = 0; i + 1 < size && OB_SUCC(ret); i += 2) {
      ObIJsonBase* tmp_value = nullptr;
      bool is_valid = true;
      if (OB_FAIL(schema_vec->get_array_element(i, tmp_value))) {
        LOG_WARN("fail to get.", K(i), K(size), K(ret));
      } else if (OB_FAIL(check_add_pro_in_schema(json_doc, tmp_value, is_valid, key))) {
        LOG_WARN("fail to check.", K(i), K(size), K(ret));
      } else if (!is_valid) {
        tmp_value = nullptr;
        if (OB_FAIL(schema_vec->get_array_element(i + 1, tmp_value))) {
          LOG_WARN("fail to get.", K(i), K(size), K(ret));
        } else if (OB_ISNULL(tmp_value) || tmp_value->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_ERR_WRONG_VALUE;
        } else if (OB_FAIL(recursive_vec.push_back(tmp_value))) {
          LOG_WARN("fail to collect.", K(i), K(size), K(ret));
        }
      } // if node add_pro, ignore
    }
  }
  return ret;
}

int ObJsonSchemaValidator::array_recursive_validator(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObJsonSchemaAns> &ans_map, bool& is_valid)
{
  INIT_SUCC(ret);
  int array_size = json_doc->element_count();
  ObIJsonBase *tmp_value = nullptr;
  ObArray<ObIJsonBase*> recursive_schema_vec;
  recursive_schema_vec.set_block_size(SCHEMA_DEFAULT_PAGE_SIZE);
  ObJsonBuffer buf(allocator_);
  for (int i = 0; i < array_size && OB_SUCC(ret) && is_valid; ++i) {
    ObString idx_key;
    buf.reuse();
    if (OB_FAIL(json_doc->get_array_element(i, tmp_value))) {
      LOG_WARN("fail to get object value.", K(i), K(array_size), K(ret));
    } else if (OB_FAIL(ObJsonSchemaUtils::get_index_str(i, buf))) {
      LOG_WARN("fail get index.", K(i), K(array_size), K(ret));
    } else if (OB_FALSE_IT(idx_key = ObString(buf.length(), buf.ptr()))) {
    } else if (OB_FAIL(collect_schema_by_idx(i, idx_key, schema_vec, recursive_schema_vec))) {
      LOG_WARN("fail to collect schema.", K(i), K(array_size), K(ret));
    } else if (recursive_schema_vec.count() > 0) {
      if (OB_FAIL(json_pointer_.push(idx_key))) {
        LOG_WARN("fail to push schema.", K(i), K(array_size), K(idx_key), K(ret));
      } else if (OB_FAIL(schema_pointer_.push(idx_key))) {
        LOG_WARN("fail to push schema.", K(i), K(array_size), K(idx_key), K(ret));
      } else if (OB_FAIL(inner_schema_validator(tmp_value, recursive_schema_vec, ans_map, is_valid))) {
        LOG_WARN("fail to validate.", K(i), K(array_size), K(idx_key), K(ret));
      } else {
        if (failed_keyword_.empty()) {
          json_pointer_.pop();
          schema_pointer_.pop();
        }
        recursive_schema_vec.reuse();
      }
    } // recursive_schema_vec.count() == 0, no schema definition, do not need check
  }
  return ret;
}

int ObJsonSchemaValidator::collect_schema_by_add_idx(const int& idx, ObIJsonBase* schema_vec, ObIArray<ObIJsonBase*> &recursive_vec)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(schema_vec)) { // didn't define add_pro, its normal
  } else if (schema_vec->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("should be array.", K(ret));
  } else {
    int size = schema_vec->element_count();
    for (int i = 0; i < size && OB_SUCC(ret); ++i) {
      ObIJsonBase* tmp_value = nullptr;
      bool is_valid = true;
      ObString add_val;
      if (OB_FAIL(schema_vec->get_object_value(i, add_val, tmp_value))) {
        LOG_WARN("fail to get.", K(i), K(size), K(ret));
      } else {
        int64_t add_idx = strtoll(add_val.ptr(), NULL, 10);
        if (add_idx <= idx && OB_FAIL(recursive_vec.push_back(tmp_value))) {
          LOG_WARN("fail to push schema.", K(i), K(size), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJsonSchemaValidator::check_all_schema_def(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, bool& is_valid)
{
  INIT_SUCC(ret);
  if (!is_valid) {
  } else {
    ObArray<ObIJsonBase*> schema_def;
    schema_def.set_block_size(SCHEMA_DEFAULT_PAGE_SIZE);
    if (OB_FAIL(ObJsonSchemaUtils::get_specific_type_of_child(ObJsonSchemaItem::SCHEMA, ObJsonNodeType::J_OBJECT, schema_vec, schema_def))) {
      if (ret == OB_ERR_JSON_KEY_NOT_FOUND) { // there is no child in schema, do not need check
        ret = OB_SUCCESS;
      }
    } else {
      ObIJsonBase *schema = nullptr;
      ObJsonSchemaType valid_type;
      valid_type.flags_ = 0;
      switch (json_doc->json_type()) {
        case ObJsonNodeType::J_NULL:
        case ObJsonNodeType::J_BOOLEAN: { // null or boolean, only check type and enum
          if (json_doc->json_type() == ObJsonNodeType::J_NULL) {
            valid_type.null_ = 1;
          } else {
            valid_type.boolean_ = 1;
          }
          if (OB_FAIL(check_public_key_words(json_doc, schema_def, valid_type, is_valid))) {
            LOG_WARN("fail in check null/boolean.", K(valid_type), K(ret));
          }
          break;
        }
        // number or integer
        case ObJsonNodeType::J_DECIMAL:
        case ObJsonNodeType::J_INT:
        case ObJsonNodeType::J_UINT:
        case ObJsonNodeType::J_DOUBLE:
        case ObJsonNodeType::J_OFLOAT:
        case ObJsonNodeType::J_ODOUBLE:
        case ObJsonNodeType::J_ODECIMAL:
        case ObJsonNodeType::J_OINT:
        case ObJsonNodeType::J_OLONG: {
          if (OB_FAIL(ObJsonSchemaUtils::set_valid_number_type_by_mode(json_doc, valid_type))) {
            LOG_WARN("fail in set valid type.", K(valid_type), K(json_doc->json_type()));
          } else if (OB_FAIL(check_public_key_words(json_doc, schema_def, valid_type, is_valid))) {
            LOG_WARN("fail in check public key words.", K(valid_type), K(ret));
          } else if (is_valid && OB_FAIL(check_number_and_integer(json_doc, schema_def, is_valid))) {
            LOG_WARN("fail in check number/integer key words.", K(valid_type), K(ret));
          }
          break;
        }
        // type enclosed in double quotes treat as string
        case ObJsonNodeType::J_DATE:
        case ObJsonNodeType::J_TIME:
        case ObJsonNodeType::J_DATETIME:
        case ObJsonNodeType::J_TIMESTAMP:
        case ObJsonNodeType::J_STRING:
        case ObJsonNodeType::J_OBINARY:
        case ObJsonNodeType::J_OOID:
        case ObJsonNodeType::J_ORAWHEX:
        case ObJsonNodeType::J_ORAWID:
        case ObJsonNodeType::J_ODAYSECOND:
        case ObJsonNodeType::J_OYEARMONTH: {
          valid_type.string_ = 1;
          if (OB_FAIL(check_public_key_words(json_doc, schema_def, valid_type, is_valid))) {
            LOG_WARN("fail in check public key words.", K(valid_type), K(ret));
          } else if (is_valid && OB_FAIL(check_string_type(json_doc, schema_def, is_valid))) {
            LOG_WARN("fail in check number/integer key words.", K(valid_type), K(ret));
          }
          break;
        }
        case ObJsonNodeType::J_OBJECT: {
          valid_type.object_ = 1;
          if (OB_FAIL(check_public_key_words(json_doc, schema_def, valid_type, is_valid))) {
            LOG_WARN("fail in check public key words.", K(valid_type), K(ret));
          } else if (is_valid && OB_FAIL(check_object_type(json_doc, schema_def, is_valid))) {
            LOG_WARN("fail in check number/integer key words.", K(valid_type), K(ret));
          }
          break;
        }
        case ObJsonNodeType::J_ARRAY: {
          valid_type.array_ = 1;
          if (OB_FAIL(check_public_key_words(json_doc, schema_def, valid_type, is_valid))) {
            LOG_WARN("fail in check public key words.", K(valid_type), K(ret));
          } else if (is_valid && OB_FAIL(check_array_type(json_doc, schema_def, is_valid))) {
            LOG_WARN("fail in check number/integer key words.", K(valid_type), K(ret));
          }
          break;
        }
        default: {
          // not one of type definition, check only public keywords,
          // other type-related keywords return default ans——true
          if (OB_FAIL(check_public_key_words(json_doc, schema_def, valid_type, is_valid))) {
            LOG_WARN("fail in check public key words.", K(valid_type), K(ret));
          }
          break;
        }
      }
    }
  }
  return ret;
}

int ObJsonSchemaValidator::get_composition_schema_def(int idx, ObIJsonBase *schema_vec, ObIJsonBase *&schema,
                                                      ObIArray<ObJsonSchemaAns> &ans_map, int& schema_id)
{
  INIT_SUCC(ret);
  schema_id = 0;
  bool found = false;
  schema = nullptr;
  schema_id = schema_vec->get_int();
  int schema_def_id = schema_id;
  if (ans_map.at(schema_id) == ObJsonSchemaAns::JS_FALSE) {
    ret = OB_ITER_END;
  }
  while (schema_def_id > 0 && OB_SUCC(ret) && !found) {
    if (OB_SUCC(schema_map_->get_array_element(schema_def_id, schema))) {
      if (OB_NOT_NULL(schema) && schema->json_type() == ObJsonNodeType::J_OBJECT) {
        found = true;
      } else {
        --schema_def_id;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!found) {
    ret = OB_SEARCH_NOT_FOUND;
    LOG_WARN("didn't found schema.", K(ret));
  }
  return ret;
}

int ObJsonSchemaValidator::record_comp_ans(const int& def_id, const bool& ans, ObIArray<ObJsonSchemaAns> &ans_map)
{
  INIT_SUCC(ret);
  int size = ans_map.count();
  composition_ans_recorded_ = true;
  if (def_id > 0 && def_id < size) {
    if (ans_map.at(def_id) == ObJsonSchemaAns::JS_FALSE) {
    } else {
      ans_map.at(def_id) = ans ? ObJsonSchemaAns::JS_TRUE : ObJsonSchemaAns::JS_FALSE;
    }
  } else {
    ret = OB_ERROR_OUT_OF_RANGE;
  }
  return ret;
}

int ObJsonSchemaValidator::check_all_composition_def(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, ObIArray<ObJsonSchemaAns> &ans_map)
{
  INIT_SUCC(ret);
  ObArray<ObIJsonBase*> composition_def;
  composition_def.set_block_size(SCHEMA_DEFAULT_PAGE_SIZE);
  bool is_valid = true;
  if (OB_FAIL(ObJsonSchemaUtils::get_all_composition_def(schema_vec, composition_def))) {
    if (ret == OB_ERR_JSON_KEY_NOT_FOUND) { // there is no child in schema, do not need check
      ret = OB_SUCCESS;
    }
  } else if (composition_def.size() > 0) {
    is_valid = true;
    int size = composition_def.count();
    ObIJsonBase *comp_schema = nullptr;
    int def_id = 0;
    switch (json_doc->json_type()) {
      case ObJsonNodeType::J_NULL:
      case ObJsonNodeType::J_BOOLEAN: { // null or boolean, only check type and enum
        bool is_null = (json_doc->json_type() == ObJsonNodeType::J_NULL);
        for (int i = 0; i < size && OB_SUCC(ret); ++i) {
          ObIJsonBase *tmp_comp = composition_def.at(i);
          is_valid = true;
          if (OB_FAIL(get_composition_schema_def(i, tmp_comp, comp_schema, ans_map, def_id))) {
            if (ret == OB_ITER_END) { // already checked, and ans is false, escape
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(check_null_or_boolean(json_doc, comp_schema, is_null, is_valid))) {
            LOG_WARN("fail to check null.", K(ret));
          } else if (OB_FAIL(record_comp_ans(def_id, is_valid, ans_map))) {
            LOG_WARN("fail to deal with ans.", K(ret));
          }
        }
        break;
      }
      // number or integer
      case ObJsonNodeType::J_DECIMAL:
      case ObJsonNodeType::J_INT:
      case ObJsonNodeType::J_UINT:
      case ObJsonNodeType::J_DOUBLE:
      case ObJsonNodeType::J_OFLOAT:
      case ObJsonNodeType::J_ODOUBLE:
      case ObJsonNodeType::J_ODECIMAL:
      case ObJsonNodeType::J_OINT:
      case ObJsonNodeType::J_OLONG: {
        for (int i = 0; i < size && OB_SUCC(ret); ++i) {
          ObIJsonBase *tmp_comp = composition_def.at(i);
          is_valid = true;
          if (OB_FAIL(get_composition_schema_def(i, tmp_comp, comp_schema, ans_map, def_id))) {
            if (ret == OB_ITER_END) { // already checked, and ans is false, escape
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(check_number_and_integer(json_doc, comp_schema, is_valid))) {
            LOG_WARN("fail to check number.", K(ret));
          } else if (OB_FAIL(record_comp_ans(def_id, is_valid, ans_map))) {
            LOG_WARN("fail to deal with ans.", K(ret));
          }
        }
        break;
      }
      // type enclosed in double quotes treat as string
      case ObJsonNodeType::J_DATE:
      case ObJsonNodeType::J_TIME:
      case ObJsonNodeType::J_DATETIME:
      case ObJsonNodeType::J_TIMESTAMP:
      case ObJsonNodeType::J_STRING:
      case ObJsonNodeType::J_OBINARY:
      case ObJsonNodeType::J_OOID:
      case ObJsonNodeType::J_ORAWHEX:
      case ObJsonNodeType::J_ORAWID:
      case ObJsonNodeType::J_ODAYSECOND:
      case ObJsonNodeType::J_OYEARMONTH: {
        for (int i = 0; i < size && OB_SUCC(ret); ++i) {
          ObIJsonBase *tmp_comp = composition_def.at(i);
          is_valid = true;
          if (OB_FAIL(get_composition_schema_def(i, tmp_comp, comp_schema, ans_map, def_id))) {
            if (ret == OB_ITER_END) { // already checked, and ans is false, escape
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(check_string_type(json_doc, comp_schema, is_valid))) {
            LOG_WARN("fail to check string.", K(ret));
          } else if (OB_FAIL(record_comp_ans(def_id, is_valid, ans_map))) {
            LOG_WARN("fail to deal with ans.", K(ret));
          }
        }
        break;
      }
      case ObJsonNodeType::J_OBJECT: {
        for (int i = 0; i < size && OB_SUCC(ret); ++i) {
          ObIJsonBase *tmp_comp = composition_def.at(i);
          is_valid = true;
          if (OB_FAIL(get_composition_schema_def(i, tmp_comp, comp_schema, ans_map, def_id))) {
            if (ret == OB_ITER_END) { // already checked, and ans is false, escape
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(check_object_type(json_doc, comp_schema, is_valid))) {
            LOG_WARN("fail to check object.", K(ret));
          } else if (OB_FAIL(record_comp_ans(def_id, is_valid, ans_map))) {
            LOG_WARN("fail to deal with ans.", K(ret));
          }
        }
        break;
      }
      case ObJsonNodeType::J_ARRAY: {
        for (int i = 0; i < size && OB_SUCC(ret); ++i) {
          ObIJsonBase *tmp_comp = composition_def.at(i);
          is_valid = true;
          if (OB_FAIL(get_composition_schema_def(i, tmp_comp, comp_schema, ans_map, def_id))) {
            if (ret == OB_ITER_END) { // already checked, and ans is false, escape
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(check_array_type(json_doc, comp_schema, is_valid))) {
            LOG_WARN("fail to check array.", K(ret));
          } else if (OB_FAIL(record_comp_ans(def_id, is_valid, ans_map))) {
            LOG_WARN("fail to deal with ans.", K(ret));
          }
        }
        break;
      }
      default: {
        // not one of type definition, check only public keywords,
        // ObJsonSchemaType set 0
        ObJsonSchemaType comp_type;
        comp_type.flags_ = 0;
        for (int i = 0; i < size && OB_SUCC(ret); ++i) {
          ObIJsonBase *tmp_comp = composition_def.at(i);
          is_valid = true;
          if (OB_FAIL(get_composition_schema_def(i, tmp_comp, comp_schema, ans_map, def_id))) {
            if (ret == OB_ITER_END) { // already checked, and ans is false, escape
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(check_public_key_words(json_doc, comp_schema, comp_type, is_valid))) {
            LOG_WARN("fail to check null.", K(ret));
          } else if (OB_FAIL(record_comp_ans(def_id, is_valid, ans_map))) {
            LOG_WARN("fail to deal with ans.", K(ret));
          }
        }
        break;
      }
    }
  }
  return ret;
}

// only check one of them: type, enum

int ObJsonSchemaValidator::check_public_key_words(ObIJsonBase *json_doc, ObIJsonBase *schema, const ObJsonSchemaType& valid_type, bool& is_valid)
{
  INIT_SUCC(ret);
  ObIJsonBase* value = nullptr;
  ObString key_word;
  if (OB_FAIL(ObJsonSchemaUtils::get_single_key_value(schema, key_word, value))) {
    LOG_WARN("fail to get key value.", K(key_word), K(ret));
  } else if (key_word.length() == strlen(ObJsonSchemaItem::TYPE)) {
    if (key_word[0] == '$') {
      // ref
      if (OB_FAIL(check_ref(json_doc, value, is_valid))) {
        LOG_WARN("fail to check enum.", K(key_word), K(ret));
      }
    } else if (key_word[0] == 't') {
      // type : value must be int
      is_valid = check_type(valid_type, value);
    } else if (key_word[0] == 'e') {
      // enum
      if (OB_FAIL(check_enum(json_doc, value, is_valid))) {
        LOG_WARN("fail to check enum.", K(key_word), K(ret));
      }
    }
  } // no other key words need to check
  return ret;
}

int ObJsonSchemaValidator::check_null_or_boolean(ObIJsonBase *json_doc, ObIJsonBase *schema, bool is_null, bool& is_valid)
{
  INIT_SUCC(ret);
  ObJsonSchemaType valid_type;
  valid_type.flags_ = 0;
  if (is_null) {
    valid_type.null_ = 1;
  } else {
    valid_type.boolean_ = 1;
  }
  if (OB_FAIL(check_public_key_words(json_doc, schema, valid_type, is_valid))) {
    LOG_WARN("fail to check type/enum.", K(ret));
  }
  return ret;
}

// check both of them: type, enum
int ObJsonSchemaValidator::check_public_key_words(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, ObJsonSchemaType valid_type, bool& is_valid)
{
  INIT_SUCC(ret);
  is_valid = true;
  int size = schema_vec.count();
  for (int i = 0; i < size && OB_SUCC(ret) && is_valid; ++i) {
    ObIJsonBase* tmp_schema = schema_vec.at(i);
    ObIJsonBase* def_value = nullptr;
    bool has_ref = false;
    if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::REF, def_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(def_value)) { // didn't define, ignore
    } else if (OB_FAIL(check_ref(json_doc, def_value, is_valid))) {
    } else if (!is_valid) {
      failed_keyword_ = ObJsonSchemaItem::REF;
    }
    if (has_ref || OB_FAIL(ret) || !is_valid) {
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::TYPE, def_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(def_value)) { // didn't define, ignore
    } else if (check_type(valid_type, def_value)) {// type is true, do nothing
    } else {
      is_valid = false;
      failed_keyword_ = ObJsonSchemaItem::TYPE;
    }
    if (has_ref || OB_FAIL(ret) || !is_valid) {
    } else if (OB_FALSE_IT(def_value = nullptr)) {
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::ENUM, def_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(def_value)) {
    } else if (OB_FAIL(check_enum(json_doc, def_value, is_valid))) {
    } else if (!is_valid) {
      failed_keyword_ = ObJsonSchemaItem::ENUM;
    }
  }
  return ret;
}

// upper bound: maximum, exclusiveMaximum
// lower bound: maximum, exclusiveMaximum
int ObJsonSchemaValidator::check_boundary_key_words(const ObString& key, ObIJsonBase *json_doc,
                                                    ObIJsonBase *schema, int& res)
{
  INIT_SUCC(ret);
  ObIJsonBase* def_value = nullptr;
  res = 0;
  if (OB_FAIL(schema->get_object_value(key, def_value))) {
  } else if (OB_ISNULL(def_value)) {
    ret = OB_SEARCH_NOT_FOUND;
  } else if (OB_FAIL(json_doc->compare(*def_value, res))) {
    LOG_WARN("fail to compare.", K(ret));
  }
  return ret;
}

// check both of them:
// minimum/maximum
// multipleOf
// exclusiveMinimum/exclusiveMaximum
int ObJsonSchemaValidator::check_number_and_integer(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, bool& is_valid)
{
  INIT_SUCC(ret);
  int size = schema_vec.count();
  for (int i = 0; i < size && OB_SUCC(ret) && is_valid; ++i) {
    ObIJsonBase* tmp_schema = schema_vec.at(i);
    int res = 0;
    if (OB_FAIL(check_boundary_key_words(ObJsonSchemaItem::MAXMUM, json_doc, tmp_schema, res))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS;
      }
    } else if (res > 0) {
      is_valid = false;
      failed_keyword_ = ObJsonSchemaItem::MAXMUM;
    }
    if (OB_FAIL(ret) || !is_valid) {
    } else if (OB_FAIL(check_boundary_key_words(ObJsonSchemaItem::MINMUM, json_doc, tmp_schema, res))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS;
      }
    } else if (res < 0) {
      is_valid = false;
      failed_keyword_ = ObJsonSchemaItem::MINMUM;
    }
    if (OB_FAIL(ret) || !is_valid) {
    } else if (OB_FAIL(check_boundary_key_words(ObJsonSchemaItem::EXCLUSIVE_MAXMUM, json_doc, tmp_schema, res))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS;
      }
    } else if (res >= 0) {
      is_valid = false;
      failed_keyword_ = ObJsonSchemaItem::MAXMUM;
    }
    if (OB_FAIL(ret) || !is_valid) {
    } else if (OB_FAIL(check_boundary_key_words(ObJsonSchemaItem::EXCLUSIVE_MINMUM, json_doc, tmp_schema, res))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS;
      }
    } else if (res <= 0) {
      is_valid = false;
      failed_keyword_ = ObJsonSchemaItem::MINMUM;
    }

    if (OB_FAIL(ret) || !is_valid) {
    } else {
      ObIJsonBase* def_value = nullptr;
      if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::MULTIPLE_OF, def_value))) {
        if (ret == OB_SEARCH_NOT_FOUND) {
          ret = OB_SUCCESS; // didn't found, its normal
        }
      } else if (OB_ISNULL(def_value)) {
      } else if (def_value->is_json_number(def_value->json_type())) {
        if (!check_multiple_of(json_doc, def_value)) {
          is_valid = false;
          failed_keyword_ = ObJsonSchemaItem::MULTIPLE_OF;
        }
      } else if (def_value->json_type() == ObJsonNodeType::J_ARRAY) {
        int array_size = def_value->element_count();
        for (int i = 0; i < array_size && OB_SUCC(ret) && is_valid; ++i) {
          ObIJsonBase* tmp_array = nullptr;
          if (OB_FAIL(def_value->get_array_element(i, tmp_array))) {
            LOG_WARN("fail to get array value.", K(i), K(array_size), K(ret));
          } else if (OB_ISNULL(tmp_array)) {
            ret = OB_BAD_NULL_ERROR;
            LOG_WARN("shouldn't be null.", K(i), K(array_size), K(ret));
          } else if (!check_multiple_of(json_doc, def_value)) {
            is_valid = false;
            failed_keyword_ = ObJsonSchemaItem::MULTIPLE_OF;
          }
        }
      } else {
        ret = OB_ERR_WRONG_VALUE;
        LOG_WARN("must be object type.", K(ret), K(def_value->json_type()));
      }
    }
  }
  return ret;
}

// only check one of them:
// type/enum
// minimum/maximum
// multipleOf
// exclusiveMinimum/exclusiveMaximum
int ObJsonSchemaValidator::check_number_and_integer(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid)
{
  INIT_SUCC(ret);
  is_valid = true;
  ObIJsonBase* value = nullptr;
  ObString key_word;
  if (OB_FAIL(ObJsonSchemaUtils::get_single_key_value(schema, key_word, value))) {
    LOG_WARN("fail to get key value.", K(ret));
  } else {
    ObJsonSchemaType valid_type;
    valid_type.flags_ = 0;
    switch (key_word.length()) {
      case JS_TYPE_LEN: {
        if (key_word[0] == 't' && OB_FAIL(ObJsonSchemaUtils::set_valid_number_type_by_mode(json_doc, valid_type))) {
          LOG_WARN("fail to set valid type.", K(key_word), K(ret));
        } else if (OB_FAIL(check_public_key_words(key_word[0], valid_type, json_doc, value, is_valid))) {
          LOG_WARN("fail to check key value.", K(key_word), K(ret));
        }
        break;
      }
      case JS_PATTERN_LEN: {
        int res = 0;
        if (key_word[2] == 'x') { // maximum
          if (OB_FAIL(json_doc->compare(*value, res))) {
            LOG_WARN("fail to compare.", K(key_word), K(ret));
          } else if (res > 0) {
            is_valid = false;
          }
        } else if (key_word[2] == 'n') {
          if (OB_FAIL(json_doc->compare(*value, res))) {
            LOG_WARN("fail to compare.", K(key_word), K(ret));
          } else if (res < 0) {
            is_valid = false;
          }
        }
        break;
      }
      case JS_EXCLUSIVE_LEN: {
        int res = 0;
        if (key_word[11] == 'a') { // exclusiveMaximum
          if (OB_FAIL(json_doc->compare(*value, res))) {
            LOG_WARN("fail to compare.", K(key_word), K(ret));
          } else if (res >= 0) {
            is_valid = false;
          }
        } else if (key_word[11] == 'i') { //exclusiveMinimum
          if (OB_FAIL(json_doc->compare(*value, res))) {
            LOG_WARN("fail to compare.", K(key_word), K(ret));
          } else if (res <= 0) {
            is_valid = false;
          }
        }
        break;
      }
      case JS_MULTIPLE_LEN: {
        if (value->is_json_number(value->json_type())) {
          is_valid = check_multiple_of(json_doc, value);
        } else if (value->json_type() == ObJsonNodeType::J_ARRAY) {
          int array_size = value->element_count();
          for (int i = 0; i < array_size && OB_SUCC(ret) && is_valid; ++i) {
            ObIJsonBase* tmp_array = nullptr;
            if (OB_FAIL(value->get_array_element(i, tmp_array))) {
              LOG_WARN("fail to get array value.", K(key_word), K(ret), K(i), K(array_size));
            } else if (OB_ISNULL(tmp_array)) {
              ret = OB_BAD_NULL_ERROR;
              LOG_WARN("shouldn't be null.", K(ret), K(i), K(array_size));
            } else if (tmp_array->is_json_number(tmp_array->json_type())) {
              is_valid = check_multiple_of(json_doc, tmp_array);
            }
          }
        }
        break;
      }
      default: {
        // not key_words for number type, its normal, ignore
        break;
      }
    }
  }
  // no other key words need to check
  return ret;
}

// minLength/maxLength
// pattern
int ObJsonSchemaValidator::check_string_type(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, bool& is_valid)
{
  INIT_SUCC(ret);
  int size = schema_vec.count();
  for (int i = 0; i < size && OB_SUCC(ret) && is_valid; ++i) {
    ObIJsonBase* tmp_schema = schema_vec.at(i);
    ObIJsonBase* def_value = nullptr;
    if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::MAX_LEN, def_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(def_value)) {
    } else if (json_doc->get_data_length() > def_value->get_int()) {
      is_valid = false;
      failed_keyword_ = ObJsonSchemaItem::MAX_LEN;
    }

    if (OB_FAIL(ret) || !is_valid) {
    } else if (OB_FALSE_IT(def_value = nullptr)) {
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::MIN_LEN, def_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(def_value)) {
    } else if (json_doc->get_data_length() < def_value->get_int()) {
      is_valid = false;
      failed_keyword_ = ObJsonSchemaItem::MIN_LEN;
    }

    if (OB_FAIL(ret) || !is_valid) {
    } else if (OB_FALSE_IT(def_value = nullptr)) {
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::PATTERN, def_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(def_value)) {
    } else if (def_value->json_type() == ObJsonNodeType::J_STRING) {
      if (!check_pattern_keywords(json_doc, def_value)) {
        is_valid = false;
        failed_keyword_ = ObJsonSchemaItem::PATTERN;
      }
    } else if (def_value->json_type() == ObJsonNodeType::J_ARRAY) {
      int array_size = def_value->element_count();
      for (int i = 0; i < array_size && OB_SUCC(ret) && is_valid; ++i) {
        ObIJsonBase* tmp_array = nullptr;
        if (OB_FAIL(def_value->get_array_element(i, tmp_array))) {
          LOG_WARN("fail to get array value.", K(i), K(array_size), K(ret));
        } else if (OB_ISNULL(tmp_array)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("shouldn't be null.", K(i), K(array_size), K(ret));
        } else if (!check_pattern_keywords(json_doc, tmp_array)) {
          is_valid = false;
          failed_keyword_ = ObJsonSchemaItem::PATTERN;
        }
      }
    } else {
      ret = OB_ERR_WRONG_VALUE;
      LOG_WARN("must be object type.", K(ret), K(def_value->json_type()));
    }
  }
  return ret;
}

int ObJsonSchemaValidator::check_public_key_words(const char key_start, ObJsonSchemaType &valid_type, ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid)
{
  INIT_SUCC(ret);
  if (key_start == '$') {
    // ref
    if (OB_FAIL(check_ref(json_doc, schema, is_valid))) {
      LOG_WARN("fail to check ref.", K(key_start), K(valid_type), K(ret));
    }
  } else if (key_start == 't') {
    // type : value must be int
    is_valid = check_type(valid_type, schema);
  } else if (key_start == 'e') {
    // enum
    if (OB_FAIL(check_enum(json_doc, schema, is_valid))) {
      LOG_WARN("fail to check enum.", K(key_start), K(valid_type), K(ret));
    }
  }
  return ret;
}

// only check one of them:
// type/enum
// minLength/maxLength
// pattern
int ObJsonSchemaValidator::check_string_type(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid)
{
  INIT_SUCC(ret);
  is_valid = true;
  ObIJsonBase* value = nullptr;
  ObString key_word;
  ObJsonSchemaType valid_type;
  if (OB_FAIL(ObJsonSchemaUtils::get_single_key_value(schema, key_word, value))) {
    LOG_WARN("fail to get key value.", K(key_word), K(valid_type), K(ret));
  } else if (OB_NOT_NULL(value)) {
    valid_type.flags_ = 0;
    switch (key_word.length()) {
      case JS_TYPE_LEN: {
        valid_type.string_ = 1;
        if (OB_FAIL(check_public_key_words(key_word[0], valid_type, json_doc, value, is_valid))) {
          LOG_WARN("fail to check key value.", K(key_word), K(valid_type), K(ret));
        }
        break;
      }
      case JS_PATTERN_LEN: {
        if (key_word[0] == 'p') {
          if (value->json_type() == ObJsonNodeType::J_STRING) {
            is_valid = check_pattern_keywords(json_doc, value);
          } else if (value->json_type() == ObJsonNodeType::J_ARRAY) {
            int array_size = value->element_count();
            for (int i = 0; i < array_size && OB_SUCC(ret) && is_valid; ++i) {
              ObIJsonBase* tmp_array = nullptr;
              if (OB_FAIL(value->get_array_element(i, tmp_array))) {
                LOG_WARN("fail to get array value.", K(key_word), K(valid_type), K(ret), K(i), K(array_size));
              } else if (OB_ISNULL(tmp_array)) {
                ret = OB_BAD_NULL_ERROR;
                LOG_WARN("shouldn't be null.", K(key_word), K(valid_type), K(ret), K(i), K(array_size));
              } else if (tmp_array->json_type() == ObJsonNodeType::J_STRING) {
                is_valid = check_pattern_keywords(json_doc, tmp_array);
              }
            }
          }
        }
        break;
      }
      case JS_STRMAX_LEN: {
        if (key_word[1] == 'a') { // maxLength
          int data_len = json_doc->get_data_length();
          int schema_len = value->get_int();  // must be int
          is_valid = (data_len <= schema_len);
        } else if (key_word[1] == 'i') { // minLength
          int data_len = json_doc->get_data_length();
          int schema_len = value->get_int();
          is_valid = (data_len >= schema_len);
        }
        break;
      }
      default: {
        // not key_words for number type, its normal, ignore
        break;
      }
    }
  }
  // no other key words need to check
  return ret;
}

// check:
// minProperties/maxProperties
// dependencies
// required
// additionalProperties
int ObJsonSchemaValidator::check_object_type(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, bool& is_valid)
{
  INIT_SUCC(ret);
  int size = schema_vec.count();
  for (int i = 0; i < size && OB_SUCC(ret) && is_valid; ++i) {
    ObIJsonBase* tmp_schema = schema_vec.at(i);
    ObIJsonBase* def_value = nullptr;
    if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::MAX_PROPERTIES, def_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(def_value)) {
    } else if (json_doc->element_count() > def_value->get_int()) {
      is_valid = false;
      failed_keyword_ = ObJsonSchemaItem::MAX_PROPERTIES;
    }

    if (OB_FAIL(ret) || !is_valid) {// minProperties
    } else if (OB_FALSE_IT(def_value = nullptr)) {
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::MIN_PROPERTIES, def_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(def_value)) {
    } else if (json_doc->element_count() < def_value->get_int()) {
      is_valid = false;
      failed_keyword_ = ObJsonSchemaItem::MIN_PROPERTIES;
    }

    if (OB_FAIL(ret) || !is_valid) {// required
    } else if (OB_FALSE_IT(def_value = nullptr)) {
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::REQUIRED, def_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(def_value)) {
    } else if (OB_FAIL(check_required(json_doc, def_value, is_valid))) {
      LOG_WARN("fail to check key value.", K(ret));
    } else if (!is_valid) {
      failed_keyword_ = ObJsonSchemaItem::REQUIRED;
    }

    if (OB_FAIL(ret) || !is_valid) {// dep_required
    } else if (OB_FALSE_IT(def_value = nullptr)) {
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::DEPENDENTREQUIRED, def_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(def_value)) {
    } else if (def_value->json_type() == ObJsonNodeType::J_OBJECT) {
      if (OB_FAIL(check_dep_required(json_doc, def_value, is_valid))) {
        LOG_WARN("fail to check key value.", K(ret));
      } else if (!is_valid) {
        failed_keyword_ = ObJsonSchemaItem::DEPENDENCIES;
      }
    } else if (def_value->json_type() == ObJsonNodeType::J_ARRAY) {
      int array_size = def_value->element_count();
      for (int i = 0; i < array_size && OB_SUCC(ret) && is_valid; ++i) {
        ObIJsonBase* tmp_array = nullptr;
        if (OB_FAIL(def_value->get_array_element(i, tmp_array))) {
          LOG_WARN("fail to get array value.", K(i), K(array_size), K(ret));
        } else if (OB_ISNULL(tmp_array)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("shouldn't be null.", K(i), K(array_size), K(ret));
        } else if (OB_FAIL(check_dep_required(json_doc, def_value, is_valid))) {
          LOG_WARN("fail to check key value.", K(i), K(array_size), K(ret));
        } else if (!is_valid) {
          failed_keyword_ = ObJsonSchemaItem::DEPENDENCIES;
        }
      }
    } else {
      ret = OB_ERR_WRONG_VALUE;
      LOG_WARN("must be object type.", K(ret), K(def_value->json_type()));
    }

    if (OB_FAIL(ret) || !is_valid) {// additional properties
    } else if (OB_FALSE_IT(def_value = nullptr)) {
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::ADDITIONAL_PRO, def_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(def_value)) {
    } else if (OB_FAIL(check_add_pro_in_schema(json_doc, def_value, is_valid))) {
      LOG_WARN("fail to check key value.", K(ret));
    } else if (!is_valid) {
      failed_keyword_ = ObJsonSchemaItem::ADDITIONAL_PRO;
    }
  }

  return ret;
}

// only check one of them:
// minProperties/maxProperties
// dependencies
// required
// additionalProperties
int ObJsonSchemaValidator::check_object_type(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid)
{
  INIT_SUCC(ret);
  is_valid = true;
  ObIJsonBase* value = nullptr;
  ObString key_word;
  ObJsonSchemaType valid_type;
  if (OB_FAIL(ObJsonSchemaUtils::get_single_key_value(schema, key_word, value))) {
    LOG_WARN("fail to get key value.", K(ret));
  } else {
    valid_type.flags_ = 0;
    switch (key_word.length()) {
      case JS_TYPE_LEN: {
        valid_type.object_ = 1;
        if (OB_FAIL(check_public_key_words(key_word[0], valid_type, json_doc, value, is_valid))) {
          LOG_WARN("fail to check key value.", K(key_word), K(ret));
        }
        break;
      }
      case JS_PROMAX_LEN: {
        int res = 0;
        if (key_word[1] == 'a') { // maxProperties
          int data_size = json_doc->element_count();
          int schema_size = value->get_int(); // must be int
          is_valid = (data_size <= data_size);
        } else if (key_word[1] == 'i') { // minProperties
          int data_size = json_doc->element_count();
          int schema_size = value->get_int();
          is_valid = (data_size >= data_size);
        }
        break;
      }
      case JS_REQUIRED_LEN: {
        if (key_word[0] != 'r') {
        } else if (OB_FAIL(check_required(json_doc, value, is_valid))) {
          LOG_WARN("fail to check required.", K(key_word), K(ret));
        }
        break;
      }
      case JS_DEP_REQUIRED_LEN: {
        if (value->json_type() == ObJsonNodeType::J_OBJECT) {
          if (OB_FAIL(check_dep_required(json_doc, value, is_valid))) {
            LOG_WARN("fail to check dep required.", K(key_word), K(ret));
          }
        } else if (value->json_type() == ObJsonNodeType::J_ARRAY) {
          int array_size = value->element_count();
          for (int i = 0; i < array_size && OB_SUCC(ret) && is_valid; ++i) {
            ObIJsonBase* tmp_array = nullptr;
            if (OB_FAIL(value->get_array_element(i, tmp_array))) {
              LOG_WARN("fail to get array value.", K(i), K(array_size), K(ret));
            } else if (OB_ISNULL(tmp_array)) {
              ret = OB_BAD_NULL_ERROR;
              LOG_WARN("shouldn't be null.", K(ret), K(i));
            } else if (tmp_array->json_type() == ObJsonNodeType::J_OBJECT) {
              if (OB_FAIL(check_dep_required(json_doc, value, is_valid))) {
                LOG_WARN("fail to check dep required.", K(i), K(array_size), K(ret));
              }
            }
          }
        }
        break;
      }
      case JS_ADD_PRO_LEN: {
        if (OB_FAIL(check_add_pro_in_schema(json_doc, value, is_valid))) {
          LOG_WARN("fail to check dep required.", K(ret));
        }
        break;
      }
      default: {
        // not key_words for number type, its normal, ignore
        break;
      }
    }
  }
  // no other key words need to check
  return ret;
}

// check:
// additionalItems
// uniqueItems
// minItems/ maxItems
int ObJsonSchemaValidator::check_array_type(ObIJsonBase *json_doc, ObIArray<ObIJsonBase*> &schema_vec, bool& is_valid)
{
  INIT_SUCC(ret);
  int size = schema_vec.count();
  for (int i = 0; i < size && OB_SUCC(ret) && is_valid; ++i) {
    ObIJsonBase* tmp_schema = schema_vec.at(i);
    ObIJsonBase* def_value = nullptr;
    if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::MAX_ITEMS, def_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(def_value)) {
    } else if (json_doc->element_count() > def_value->get_int()) {
      is_valid = false;
      failed_keyword_ = ObJsonSchemaItem::MAX_ITEMS;
    }

    if (OB_FAIL(ret) || !is_valid) {// minItems
    } else if (OB_FALSE_IT(def_value = nullptr)) {
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::MIN_ITEMS, def_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(def_value)) {
    } else if (json_doc->element_count() < def_value->get_int()) {
      is_valid = false;
      failed_keyword_ = ObJsonSchemaItem::MIN_ITEMS;
    }

    if (OB_FAIL(ret) || !is_valid) {// addItems
    } else if (OB_FALSE_IT(def_value = nullptr)) {
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::ADDITIONAL_ITEMS, def_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(def_value)) {
    } else if (json_doc->element_count() > def_value->get_int()) {
      is_valid = false;
      failed_keyword_ = ObJsonSchemaItem::ADDITIONAL_ITEMS;
    }

    if (OB_FAIL(ret) || !is_valid) {// uniqueItems
    } else if (OB_FALSE_IT(def_value = nullptr)) {
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::UNIQUE_ITEMS, def_value))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(def_value)) {
    } else if (OB_FAIL(check_unique_items(json_doc, def_value, is_valid))) {
      LOG_WARN("fail to check key value.", K(ret));
    } else if (!is_valid) {
      failed_keyword_ = ObJsonSchemaItem::UNIQUE_ITEMS;
    }
  }
  return ret;
}


// only check one of them:
// additionalItems
// uniqueItems
// minItems/ maxItems
int ObJsonSchemaValidator::check_array_type(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid)
{
  INIT_SUCC(ret);
  is_valid = true;
  ObIJsonBase* value = nullptr;
  ObString key_word;
  ObJsonSchemaType valid_type;
  if (OB_FAIL(ObJsonSchemaUtils::get_single_key_value(schema, key_word, value))) {
    LOG_WARN("fail to get key value.", K(key_word), K(ret));
  } else {
    valid_type.flags_ = 0;
    switch (key_word.length()) {
      case JS_TYPE_LEN: {
        valid_type.array_ = 1;
        if (OB_FAIL(check_public_key_words(key_word[0], valid_type, json_doc, value, is_valid))) {
          LOG_WARN("fail to check key value.", K(key_word), K(ret));
        }
        break;
      }
      case JS_STRMAX_LEN: {
        int res = 0;
        if (key_word[1] == 'a') { // maxItems
          int data_size = json_doc->element_count();
          int schema_size = value->get_int();
          is_valid = (data_size <= data_size);
        } else if (key_word[1] == 'i') {
          int data_size = json_doc->element_count();
          int schema_size = value->get_int();
          is_valid = (data_size >= data_size);
        }
        break;
      }
      case JS_ADD_ITEMS_LEN: {
        int data_size = json_doc->element_count();
        int schema_size = value->get_int();
        is_valid = (data_size <= data_size);
        break;
      }
      case JS_UNIQUE_ITEMS_LEN: {
        if (OB_FAIL(check_unique_items(json_doc, value, is_valid))) {
          LOG_WARN("fail to check dep required.", K(ret));
        }
        break;
      }
      default: {
        // not key_words for number type, its normal, ignore
        break;
      }
    }
  }
  // no other key words need to check
  return ret;
}

bool ObJsonSchemaValidator::check_type(const ObJsonSchemaType& real_type, ObIJsonBase *schema_value)
{
  bool ret_bool = false;
  ObJsonSchemaType defined_type;
  defined_type.flags_ = schema_value->get_uint();
  if ((defined_type.flags_ & real_type.flags_) != 0) {
    ret_bool = true;
  }
  return ret_bool;
}

bool ObJsonSchemaValidator::check_multiple_of(ObIJsonBase *json_doc, ObIJsonBase *schema)
{
  bool ret_bool = true;
  double json_val = 0.0;
  double multi_val = 0.0;
  int ret = OB_SUCCESS;
  double ans;
  if (OB_FAIL(ObJsonSchemaUtils::get_json_number(json_doc, json_val))) {
    ret_bool = false;
  } else if (OB_FAIL(ObJsonSchemaUtils::get_json_number(schema, multi_val))) {
    ret_bool = false;
  } else {
    ret_bool = ObJsonSchemaUtils::check_multiple_of(json_val, multi_val);
  }
  return ret_bool;
}

bool ObJsonSchemaUtils::check_multiple_of(const double& json_val, const double& multi_val)
{
  bool ret_bool = true;
  double abs_json = abs(json_val);
  double abs_multi = abs(multi_val);
  double mod_ans = floor(abs_json / abs_multi);
  double res = abs_json - mod_ans * abs_multi;
  if (res > 0.0) {
    ret_bool = false;
  }
  return ret_bool;
}

int ObJsonSchemaUtils::set_valid_number_type_by_mode(ObIJsonBase *json_doc, ObJsonSchemaType& valid_type)
{
  INIT_SUCC(ret);
  switch (json_doc->json_type()) {
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_OINT: {
      // both oracle mode and mysql mode will be valid for integer and number type
      valid_type.integer_ = 1;
      valid_type.number_ = 1;
    }
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_OFLOAT:
    case ObJsonNodeType::J_ODOUBLE:
    case ObJsonNodeType::J_ODECIMAL:
    case ObJsonNodeType::J_OLONG: {
      if (lib::is_mysql_mode()) {
        // in mysql mode, only check nodetype
        // all double is not integer, which does not meet the standards of json schema
        valid_type.number_ = 1;
      } else {
        // in oracle mode and json schema standard, will check the value of double
        // for example, 1.0 is integer, but 1.1 is not integer
        double json_val = 0.0;
        if (OB_FAIL(ObJsonSchemaUtils::get_json_number(json_doc, json_val))) {
        } else if (fmod(json_val, 1.0) == 0) {
          valid_type.integer_ = 1;
          valid_type.number_ = 1;
        } else {
          valid_type.number_ = 1;
        }
      }
      break;
    }
    default: { // not number, set nothing
      break;
    }
  }
  return ret;
}

bool ObJsonSchemaValidator::check_pattern_keywords(ObIJsonBase *json_doc, ObIJsonBase *schema)
{
  bool ret_bool = true;
  ObString text(json_doc->get_data_length(), json_doc->get_data());
  ObString pattern(schema->get_data_length(), schema->get_data());
  ObJsonSchemaUtils::if_regex_match(text, pattern, str_buf_, ret_bool);
  return ret_bool;
}

int ObJsonSchemaValidator::check_ref(ObIJsonBase *json_doc, ObIJsonBase *schema_value, bool& is_valid)
{
  INIT_SUCC(ret);
  if (schema_value->json_type() == ObJsonNodeType::J_STRING) {
    ObString ref_str = ObString(schema_value->get_data_length(), schema_value->get_data());
    if (OB_FAIL(check_single_ref(json_doc, ref_str, is_valid))) {
      LOG_WARN("fail to check ref.", K(ret));
    }
  } else if (schema_value->json_type() == ObJsonNodeType::J_ARRAY) {
    int size = schema_value->element_count();
    for (int i = 0; i < size && OB_SUCC(ret) && is_valid; ++i) {
      ObIJsonBase* tmp_array = nullptr;
      if (OB_FAIL(schema_value->get_array_element(i, tmp_array))) {
        LOG_WARN("fail to get array value.", K(i), K(size), K(ret));
      } else if (OB_ISNULL(tmp_array)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("shouldn't be null.", K(ret), K(i));
      } else if (tmp_array->json_type() == ObJsonNodeType::J_STRING) {
        ObString ref_str = ObString(tmp_array->get_data_length(), tmp_array->get_data());
        if (OB_FAIL(check_single_ref(json_doc, ref_str, is_valid))) {
          LOG_WARN("fail to check ref.", K(i), K(size), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObJsonSchemaValidator::check_single_ref(ObIJsonBase *json_doc, const ObString& ref_key, bool& is_valid)
{
  INIT_SUCC(ret);
  ObIJsonBase * ref = nullptr;
  ObIJsonBase * ref_value = nullptr;
  if (ref_key.compare(ObJsonSchemaItem::ROOT) == 0) {
    if (json_pointer_.count() == 1) { // means check root, then current ans is final ans, return true
      ref_value = nullptr;
      is_valid = true;
    } else {
      ref_value = schema_map_;
    }
  } else if (OB_FAIL(schema_map_->get_array_element(1, ref))) {
    LOG_WARN("fail to get ref.", K(ret));
  } else if (ref->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be OBJECT.", K(ref->json_type()), K(ret));
  } else if (OB_FAIL(ref->get_object_value(ref_key, ref_value)) || OB_ISNULL(ref_value)) {
    // didn't found, invalid, but not raise error
    ret = OB_SUCCESS;
  } else if (ref_value->json_type() != ObJsonNodeType::J_ARRAY) {
    if (ref_value->json_type() == ObJsonNodeType::J_INT) {
      // typeless_, means check self now
      if (json_pointer_.count() == 1) { // means check root, then current ans is final ans, return true
        ref_value = nullptr;
        is_valid = true;
      } else {
        ref_value = schema_map_;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should be OBJECT.", K(ref_value->json_type()), K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(ref_value)) {
    ObJsonSchemaValidator ref_validator(allocator_, ref_value);
    if (OB_FAIL(ref_validator.schema_validator(json_doc, is_valid))) {
      LOG_WARN("fail to check.", K(ret));
    }
  }
  return ret;
}

// for enum, schema value must be array, checked in parsing schema tree
int ObJsonSchemaValidator::check_enum(ObIJsonBase *json_doc, ObIJsonBase *schema_value, bool& is_valid)
{
  INIT_SUCC(ret);
  is_valid = false;
  int size = schema_value->element_count();
  if (size == 0) {
    is_valid = true;
  } else {
    for (int i = 0; i < size && OB_SUCC(ret) && !is_valid; ++i) {
      ObIJsonBase* node = nullptr;
      int res = -1;
      if (OB_FAIL(schema_value->get_array_element(i, node))) {
        LOG_WARN("fail to get array.", K(ret), K(i));
      } else if (OB_ISNULL(node)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("shouldn't be null.", K(ret), K(i));
      } else if (OB_FAIL(json_doc->compare(*node, res))) {
        LOG_WARN("fail to compare.", K(ret), K(i));
      } else {
        is_valid = (res == 0);
      }
    }
  }
  return ret;
}

int ObJsonSchemaValidator::check_required(ObIJsonBase *json_doc, ObIJsonBase *schema_value, bool& is_valid)
{
  INIT_SUCC(ret);
  is_valid = true;
  if (schema_value->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be object type.", K(ret), K(schema_value->json_type()));
  } else {
    int size = schema_value->element_count();
    for (int i = 0; i < size && OB_SUCC(ret) && is_valid; ++i) {
      ObIJsonBase * required_key = nullptr;
      if (OB_FAIL(schema_value->get_array_element(i, required_key))) {
        LOG_WARN("fail to get required key.", K(ret), K(i));
      } else if (OB_ISNULL(required_key)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("shouldn't be null.", K(ret), K(i));
      } else if (required_key->json_type() == ObJsonNodeType::J_STRING) {
        ObIJsonBase *value = nullptr;
        ObString key(required_key->get_data_length(), required_key->get_data());
        if (OB_FAIL(json_doc->get_object_value(key, value)) || OB_ISNULL(value)) {
          // didn't found, invalid, but not raise error
          ret = OB_SUCCESS;
          is_valid = false;
        }
      } // if not string, don't check key
    }
  }
  return ret;
}

int ObJsonSchemaValidator::check_dep_required(ObIJsonBase *json_doc, ObIJsonBase *schema_value, bool& is_valid)
{
  INIT_SUCC(ret);
  is_valid = true;
  if (schema_value->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be object type.", K(ret), K(schema_value->json_type()));
  } else {
    int size = schema_value->element_count();
    for (int i = 0; i < size && OB_SUCC(ret) && is_valid; ++i) {
      ObIJsonBase * dep_required_value = nullptr;
      ObIJsonBase * tmp_value = nullptr;
      ObString dep_key;
      if (OB_FAIL(schema_value->get_object_value(i, dep_key, dep_required_value))) {
        LOG_WARN("fail to get required key.", K(ret), K(i));
      } else if (OB_ISNULL(dep_required_value)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("shouldn't be null.", K(ret), K(i));
      } else if (OB_FAIL(json_doc->get_object_value(dep_key, tmp_value))) {
        // didn't found, do not need to check dependencies
        ret = OB_SUCCESS;
      } else if (dep_required_value->json_type() != ObJsonNodeType::J_ARRAY) {
      } else if (OB_FAIL(check_required(json_doc, dep_required_value, is_valid))) {
        LOG_WARN("fail to check dep required.", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObJsonSchemaValidator::check_add_pro_in_schema(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid, ObString defined_key)
{
  INIT_SUCC(ret);
  // collect all key, add and of conflict, if not conflict, check pattern
  is_valid = true;
  ObSortedVector<ObString> properties_vec;
  ObStringCmp cmp;
  ObStringUnique unique;
  int array_size = schema->element_count();
  int size = json_doc->element_count();
  if (array_size % ObJsonSchemaTree::ADDITIONAL_PRO_ARRAY_COUNT != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong number.", K(ret));
  } else {
    for (int i = 0; i < array_size && OB_SUCC(ret); i += 2) {
      ObIJsonBase *tmp_value = nullptr;
      if (OB_FAIL(schema->get_array_element(i, tmp_value))) {
        LOG_WARN("fail to get.", K(i), K(array_size), K(ret));
      } else if (OB_ISNULL(tmp_value) || tmp_value->json_type() != ObJsonNodeType::J_ARRAY) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("wrong number.", K(i), K(array_size), K(ret));
      } else {
        int pro_num = tmp_value->element_count();
        for (int i = 0; i < pro_num && OB_SUCC(ret); ++i) {
          ObIJsonBase *tmp_str = nullptr;
          if (OB_FAIL(tmp_value->get_array_element(i, tmp_str))) {
            LOG_WARN("fail to get.", K(i), K(pro_num), K(ret));
          } else if (OB_ISNULL(tmp_str) || tmp_str->json_type() != ObJsonNodeType::J_STRING) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("wrong number.", K(i), K(pro_num), K(ret));
          } else {
            ObString str = ObString(tmp_str->get_data_length(), tmp_str->get_data());
            ObSortedVector<ObString>::iterator pos = properties_vec.end();
            if (OB_FAIL(properties_vec.insert_unique(str, pos, cmp, unique))) {
              if (ret == OB_CONFLICT_VALUE) {
                ret = OB_SUCCESS; // confilict means found duplicated nodes, it is not an error.
              }
            }
          }
        } // add all properties key
      }
    } // check all properties vector

    if (OB_FAIL(ret)) {
    } else if (defined_key.empty()) {
      for (int i = 0; i < size && OB_SUCC(ret) && is_valid; ++i) {
        ObIJsonBase * tmp_value = nullptr;
        ObString doc_key;
        bool found = false;
        ObSortedVector<ObString>::iterator pos = properties_vec.end();
        if (OB_FAIL(json_doc->get_object_value(i, doc_key, tmp_value))) {
          LOG_WARN("fail to get key.", K(ret), K(i));
        } else if (OB_FAIL(properties_vec.insert_unique(doc_key, pos, cmp, unique))) {
          if (ret == OB_CONFLICT_VALUE) {
            ret = OB_SUCCESS; // confilict means found duplicated nodes, it is not an error.
          }
        } else if (OB_FAIL(check_pattern_key_in_add_pro(doc_key, schema, found))) {
          LOG_WARN("fail to check pattern key.", K(ret), K(i));
        } else if (!found) {
          is_valid = false;
        }
      }
    } else {
      ObSortedVector<ObString>::iterator pos = properties_vec.end();
      if (OB_FAIL(properties_vec.insert_unique(defined_key, pos, cmp, unique))) {
        if (ret == OB_CONFLICT_VALUE) {
          ret = OB_SUCCESS; // confilict means found duplicated nodes, it is not an error.
          is_valid = true;
        }
      } else if (OB_FAIL(check_pattern_key_in_add_pro(defined_key, schema, is_valid))) {
        LOG_WARN("fail to check pattern key.", K(ret));
      }
    }
  }
  return ret;
}

int ObJsonSchemaValidator::check_pattern_key_in_add_pro(const ObString& key, ObIJsonBase *schema, bool& found)
{
  INIT_SUCC(ret);
  int array_size = schema->element_count();
  found = false;
  for (int i = 1; i < array_size && OB_SUCC(ret) && !found; i += 2) {
    ObIJsonBase *tmp_value = nullptr;
    if (OB_FAIL(schema->get_array_element(i, tmp_value))) {
      LOG_WARN("fail to get.", K(i), K(array_size), K(ret));
    } else if (OB_ISNULL(tmp_value) || tmp_value->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wrong number.", K(i), K(array_size), K(ret));
    } else {
      int pro_num = tmp_value->element_count();
      for (int i = 0; i < pro_num && OB_SUCC(ret) && !found; ++i) {
        ObIJsonBase *tmp_str = nullptr;
        if (OB_FAIL(tmp_value->get_array_element(i, tmp_str))) {
          LOG_WARN("fail to get.", K(i), K(pro_num), K(ret));
        } else if (OB_ISNULL(tmp_str) || tmp_str->json_type() != ObJsonNodeType::J_STRING) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("wrong number.", K(i), K(pro_num), K(ret));
        } else {
          ObString reg_str = ObString(tmp_str->get_data_length(), tmp_str->get_data());
          bool regex_ans = false;
          if (OB_FAIL(ObJsonSchemaUtils::if_regex_match(key, reg_str, str_buf_, regex_ans))) {
          } else {
            found = regex_ans;
          }
        }
      } // add all pattern properties key
    }
  } // check all pattern properties vector
  return ret;
}

int ObJsonSchemaValidator::check_unique_items(ObIJsonBase *json_doc, ObIJsonBase *schema, bool& is_valid)
{
  INIT_SUCC(ret);
  is_valid = true;
  ObSortedVector<ObIJsonBase*> dup;
  ObJsonContentCmp cmp;
  ObJsonContentUnique unique;
  int array_size = json_doc->element_count();
  for (int i = 0; i < array_size && OB_SUCC(ret); ++i) {
    ObSortedVector<ObIJsonBase*>::iterator pos = dup.end();
    ObIJsonBase *tmp_value = nullptr;
    if (OB_FAIL(json_doc->get_array_element(i, tmp_value))) {
      LOG_WARN("fail to get.", K(i), K(array_size), K(ret));
    } else if (OB_ISNULL(tmp_value)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("wrong number.", K(i), K(array_size), K(ret));
    } else if (OB_FAIL(dup.insert_unique(tmp_value, pos, cmp, unique))) {
      if (ret == OB_CONFLICT_VALUE) {
        ret = OB_SUCCESS;
        is_valid = false;
      }
    }
  }
  return ret;
}

ObIJsonBase* ObJsonSchemaCache::schema_at(size_t idx)
{
  ObIJsonBase* ptr = nullptr;
  if (idx < schema_arr_ptr_.size()) {
    ptr = schema_arr_ptr_[idx];
  }
  return ptr;
}

void ObJsonSchemaCache::set_allocator(common::ObIAllocator *allocator)
{
  if (allocator != nullptr && size() == 0) {
    allocator_ = allocator;
  }
}

bool ObJsonSchemaCache::is_match(ObString& in_str, size_t idx)
{
  bool result = false;
  if (idx < size()) {
    ObString schema_str = schema_str_[idx];
    if (!schema_str.empty()) {
      if (in_str.length() == schema_str.length()) {
        result = (in_str.compare(schema_str) == 0);
      } else {
        result = false;
      }
    }
  }
  return result;
}

int ObJsonSchemaCache::find_and_add_cache(ObIJsonBase*& out_schema, ObString& in_str, int arg_idx, const ObJsonInType& in_type)
{
  INIT_SUCC(ret);
  if (!is_match(in_str, arg_idx)) {
    ObIJsonBase* in_json = nullptr;
    // whether it is Oracle or MySQL, only lowercase true/false is considered a Boolean value
    // so, use strict mode
    uint32_t parse_flag = ObJsonParser::JSN_STRICT_FLAG;
    parse_flag |= ObJsonParser::JSN_SCHEMA_FLAG;

    if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator_, in_str, in_type, in_type,
                                                 in_json, parse_flag))) {

      LOG_WARN("fail to get json base", K(ret), K(in_type));
    } else {
      ObJsonSchemaTree json_schema(allocator_);
      if (OB_FAIL(json_schema.build_schema_tree(in_json))) {
        LOG_WARN("invalid json schema", K(ret));
      } else if (OB_ISNULL(out_schema = json_schema.get_schema_map())) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("should not be null", K(ret));
      } else {
        ret = set_schema(out_schema, in_str, arg_idx, arg_idx);
      }
    }
  } else {
    out_schema = schema_at(arg_idx);
  }
  return ret;
}

int ObJsonSchemaCache::fill_empty(size_t reserve_size)
{
  INIT_SUCC(ret);
  if (reserve_size > schema_arr_ptr_.size()) {
    // fill element in vector
    if (OB_FAIL(schema_arr_ptr_.reserve(reserve_size))) {
      LOG_WARN("fail to reserve for path arr.", K(ret), K(reserve_size));
    } else if (OB_FAIL(stat_arr_.reserve(reserve_size))) {
      LOG_WARN("fail to reserve for stat arr.", K(ret), K(reserve_size));
    } else if (OB_FAIL(schema_str_.reserve(reserve_size))) {
      LOG_WARN("fail to reserve for stat arr.", K(ret), K(reserve_size));
    } else if (schema_arr_ptr_.size() != stat_arr_.size() || schema_str_.size() != stat_arr_.size()) {
      LOG_WARN("Length is not equals.", K(ret), K(reserve_size));
    }
    int size = schema_arr_ptr_.size();
    for (size_t cur = size; OB_SUCC(ret) && cur < reserve_size; ++cur) {
      if (OB_FAIL(schema_arr_ptr_.push_back(nullptr))) {
        LOG_WARN("fail to push NUll to path arr", K(cur), K(reserve_size), K(ret));
      } else if (OB_FAIL(stat_arr_.push_back(ObSchemaCacheStat()))) {
        LOG_WARN("fail to push stat to stat arr", K(cur), K(reserve_size), K(ret));
      } else if (OB_FAIL(schema_str_.push_back(ObString::make_empty_string()))) {
        LOG_WARN("fail to push stat to stat arr", K(cur), K(reserve_size), K(ret));
      }
    }
  }
  return ret;
}

int ObJsonSchemaCache::set_schema(ObIJsonBase* j_schema, const ObString& in_str, int arg_idx, int index)
{
  INIT_SUCC(ret);
  if (OB_FAIL(fill_empty(arg_idx + 1))) {
    LOG_WARN("fail to fill empty.", K(ret), K(arg_idx));
  } else if (index >= schema_arr_ptr_.size()) {
    ret = OB_ERROR_OUT_OF_RANGE;
    LOG_WARN("index out of range.", K(ret), K(index), K(schema_arr_ptr_.size()));
  } else {
    schema_arr_ptr_[index] = j_schema;
    stat_arr_[index] = ObSchemaCacheStat(INITIALIZED, arg_idx);
    schema_str_[index] = in_str;
  }
  return ret;
}

int ObJsonSchemaUtils::is_all_children_subschema(ObJsonNode* array_of_subschema)
{
  INIT_SUCC(ret);
  if (OB_NOT_NULL(array_of_subschema) && array_of_subschema->json_type() == ObJsonNodeType::J_ARRAY) {
    ObJsonArray* array = static_cast<ObJsonArray*>(array_of_subschema);
    int size = array->element_count();
    for (int i = 0; i < size && OB_SUCC(ret); ++i) {
      ObJsonNode* tmp = (*array)[i];
      if (OB_ISNULL(tmp)){
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("shouldn't be null", K(ret), K(i));
      } else if (tmp->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_ERR_TYPE_OF_JSON_SCHEMA;
        LOG_WARN("json schema must be object", K(ret), K(i), K(tmp->json_type()));
      }
    }
  }
  return ret;
}

int ObJsonSchemaUtils::check_if_composition_legal(ObJsonObject* origin_schema, ObJsonSubSchemaKeywords& key_words)
{
  INIT_SUCC(ret);
  if (OB_NOT_NULL(origin_schema)) {
    bool is_legal = false;
    if (OB_FAIL(check_composition_by_name(ObJsonSchemaItem::ALLOF, origin_schema, is_legal))) {
      LOG_WARN("fail to check all of", K(ret));
    } else if (is_legal && OB_FALSE_IT(key_words.all_of_ = 1)) {
    } else if (OB_FAIL(check_composition_by_name(ObJsonSchemaItem::ANYOF, origin_schema, is_legal))) {
      LOG_WARN("fail to check all of", K(ret));
    } else if (is_legal && OB_FALSE_IT(key_words.any_of_ = 1)) {
    } else if (OB_FAIL(check_composition_by_name(ObJsonSchemaItem::ONEOF, origin_schema, is_legal))) {
      LOG_WARN("fail to check all of", K(ret));
    } else if (is_legal && OB_FALSE_IT(key_words.one_of_ = 1)) {
    } else if (OB_FAIL(check_composition_by_name(ObJsonSchemaItem::NOT, origin_schema, is_legal))) {
      LOG_WARN("fail to check all of", K(ret));
    } else if (is_legal && OB_FALSE_IT(key_words.not_ = 1)) {
    }
  }
  return ret;
}

int ObJsonSchemaUtils::check_composition_by_name(const ObString& key_word, ObJsonObject* origin_schema, bool& is_legal)
{
  INIT_SUCC(ret);
  is_legal = false;
  ObJsonNode* node = origin_schema->get_value(key_word);
  if (OB_ISNULL(node)) {
  // didn't define, its normal
  } else if (key_word.compare(ObJsonSchemaItem::NOT) == 0) {
    if (node->json_type() == ObJsonNodeType::J_OBJECT) {
      is_legal = true;
    }
  } else if (node->json_type() == ObJsonNodeType::J_ARRAY && node->element_count() > 0) {
    if (OB_FAIL(ObJsonSchemaUtils::is_all_children_subschema(node))) {
      // composition, each element has its own schema, check if all schema element
      // if not, raise error
      LOG_WARN("illegal subschema in item array", K(ret), K(key_word));
    } else {
      is_legal = true;
    }
  } // not array, ignore in mysql, raise error in oracle
  return ret;
}

int ObJsonSchemaUtils::json_doc_move_to_key(const ObString& key_word, ObJsonObject*& json_schema)
{
  INIT_SUCC(ret);
  ObJsonNode* tmp_node = nullptr;
  if (OB_ISNULL(json_schema)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("shouldn't be null.", K(ret));
  } else if (OB_ISNULL(tmp_node = json_schema->get_value(key_word))) {
    ret = OB_SEARCH_NOT_FOUND;
    LOG_WARN("didn't find value of key.", K(ret), K(key_word));
  } else if (tmp_node->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be object type.", K(ret), K(key_word));
  } else {
    json_schema = static_cast<ObJsonObject*>(tmp_node);
  }
  return ret;
}

int ObJsonSchemaUtils::json_doc_move_to_array(const ObString& key_word, ObJsonObject* json_schema, ObJsonArray*& array_schema)
{
  INIT_SUCC(ret);
  ObJsonNode* tmp_node = nullptr;
  if (OB_ISNULL(json_schema)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("shouldn't be null.", K(ret));
  } else if (OB_ISNULL(tmp_node = json_schema->get_value(key_word))) {
    ret = OB_SEARCH_NOT_FOUND;
    LOG_WARN("didn't find value of key.", K(ret), K(key_word));
  } else if (tmp_node->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be object type.", K(ret), K(key_word));
  } else {
    array_schema = static_cast<ObJsonArray*>(tmp_node);
  }
  return ret;
}

int ObJsonSchemaUtils::record_schema_array(ObStack<ObJsonObject*>& stk, ObIArray<ObJsonObject*>& array)
{
  INIT_SUCC(ret);
  int size = stk.size();
  // copy
  for (int i = 0; i < size && OB_SUCC(ret); ++i) {
    ObJsonObject* cur_schema = stk.at(i);
    ret = array.push_back(cur_schema);
  }
  return ret;
}

int ObJsonSchemaUtils::get_index_str(const int& idx, ObStringBuffer& buf)
{
  INIT_SUCC(ret);
  char res_ptr[OB_MAX_DECIMAL_PRECISION] = {0};
  char* ptr = nullptr;
  if (OB_ISNULL(ptr = ObCharset::lltostr(idx, res_ptr, 10, 1))) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("fail to transform the index(lltostr)", K(ret), K(index));
  } else {
    buf.append(res_ptr, static_cast<int32_t>(ptr - res_ptr));
  }
  return ret;
}

int ObJsonSchemaUtils::collect_key(ObJsonNode* obj, ObIAllocator *allocator, ObJsonArray* array, ObJsonBuffer& buf, bool pattern_key) {
  INIT_SUCC(ret);
  if (obj->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be object type.", K(obj->json_type()), K(ret));
  } else {
    ObJsonObject* object_node = static_cast<ObJsonObject*>(obj);
    int size = object_node->element_count();
    for (int i = 0; i < size && OB_SUCC(ret); ++i) {
      ObString key;
      ObJsonString* str_node = nullptr;
      bool valid_pattern = false;
      if (OB_FAIL(object_node->get_key_by_idx(i, key))) {
        LOG_WARN("get key failed.", K(i), K(size), K(ret));
      } else if (pattern_key
             && (OB_FAIL((ObJsonSchemaUtils::is_valid_pattern(key, buf, valid_pattern))) || !valid_pattern)) {
      } else if (OB_ISNULL(str_node = OB_NEWx(ObJsonString, allocator, key))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc failed.", K(i), K(size), K(ret));
      } else if (OB_FAIL(array->append(str_node))) {
        LOG_WARN("append failed.", K(i), K(size), K(ret));
      }
    }
  }
  return ret;
}

int ObJsonSchemaUtils::get_specific_type_of_child(const ObString& key, ObJsonNodeType expect_type, ObIArray<ObIJsonBase*> &src, ObIArray<ObIJsonBase*> &res)
{
  INIT_SUCC(ret);
  int src_size = src.count();
  for (int i = 0; i < src_size && OB_SUCC(ret); ++i) {
    ObIJsonBase* tmp_src = src.at(i);
    ObIJsonBase* tmp_res = nullptr;
    if (OB_ISNULL(tmp_src)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("null value", K(ret));
    } else if (OB_FAIL(tmp_src->get_object_value(key, tmp_res))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(tmp_res)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("null value", K(ret));
    } else if (tmp_res->json_type() != expect_type) {
      ret = OB_ERR_WRONG_VALUE;
      LOG_WARN("must be object type.", K(ret), K(tmp_src->json_type()), K(expect_type));
    } else if (OB_FAIL(res.push_back(tmp_res))) {
      LOG_WARN("fail to push key-value.", K(ret));
    }
  }
  if (res.count() > 0) {
  } else {
    ret = OB_ERR_JSON_KEY_NOT_FOUND;
  }
  return ret;
}

int ObJsonSchemaUtils::get_all_composition_def(ObIArray<ObIJsonBase*> &src, ObIArray<ObIJsonBase*> &res)
{
  INIT_SUCC(ret);
  int src_size = src.count();
  for (int i = 0; i < src_size && OB_SUCC(ret); ++i) {
    ObIJsonBase* tmp_src = src.at(i);
    ObIJsonBase* tmp_comp = nullptr;
    if (OB_ISNULL(tmp_src)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("null value", K(ret));
    } else if (OB_FAIL(tmp_src->get_object_value(ObJsonSchemaItem::COMPOSITION, tmp_comp))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS; // didn't found, its normal
      }
    } else if (OB_ISNULL(tmp_comp)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("null value", K(ret));
    } else if (tmp_comp->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_ERR_WRONG_VALUE;
      LOG_WARN("must be object type.", K(ret), K(tmp_src->json_type()));
    } else {
      int comp_size = tmp_comp->element_count();
      for (int i = 0; i < comp_size && OB_SUCC(ret); ++i) {
        ObIJsonBase* tmp_def = nullptr;
        if (OB_FAIL(tmp_comp->get_array_element(i, tmp_def))) {
          LOG_WARN("fail to get", K(i), K(comp_size), K(ret));
        } else if (OB_ISNULL(tmp_def)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("null value", K(i), K(comp_size), K(ret));
        } else if (tmp_def->json_type() != ObJsonNodeType::J_INT) {
          ret = OB_ERR_WRONG_VALUE;
          LOG_WARN("must be object type.", K(ret), K(tmp_src->json_type()));
        } else if (OB_FAIL(res.push_back(tmp_def))) {
          LOG_WARN("fail to push", K(i), K(comp_size), K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (res.count() > 0) {
  } else {
    ret = OB_ERR_JSON_KEY_NOT_FOUND;
  }
  return ret;
}

int ObJsonSchemaUtils::get_json_number(ObIJsonBase* json_doc, double& res)
{
  INIT_SUCC(ret);
  ObJsonNodeType json_type = json_doc->json_type();
  if (json_doc->is_json_number(json_type)) {
    ret = json_doc->to_double(res);
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObJsonSchemaUtils::get_single_key_value(ObIJsonBase *single_schema, ObString& key_words, ObIJsonBase *&value)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(single_schema)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("null value", K(ret));
  } else if (single_schema->element_count() != 1) {
    ret = OB_ERR_WRONG_VALUE;
    LOG_WARN("must be object type.", K(ret), K(single_schema->element_count()));
  } else if (OB_FAIL(single_schema->get_object_value(0, key_words, value))) {
    LOG_WARN("fail to get keywords and value.", K(key_words), K(ret));
  } else if (OB_ISNULL(value) || key_words.empty()) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("shouldn't be null", KPC(value), K(ret));
  }
  return ret;
}

// ObExprRegexContext need session_info, so use std::regex
// but illegal regex text will thrown exception "regex_error"
int ObJsonSchemaUtils::is_valid_pattern(const ObString& regex_text, ObJsonBuffer& buf, bool& ans)
{
  INIT_SUCC(ret);
  ans = false;
  int len = regex_text.length();
  buf.reuse();
  if (OB_FAIL(buf.append(regex_text)) || OB_FAIL(buf.append("\0"))) {
  } else {
    char* start = buf.ptr();
    try {
      std::regex pattern(start, len);
      ans = true;
    } catch (const std::regex_error& e) {
      ans = false;
    }
  }
  return ret;
}

int ObJsonSchemaUtils::if_regex_match(const ObString& text, const ObString& regex_text, ObJsonBuffer& buf, bool& ans)
{
  INIT_SUCC(ret);
  ans = false;
  buf.reuse();
  int r_len = regex_text.length();
  int t_len = text.length();
  if (OB_FAIL(buf.append(regex_text)) || OB_FAIL(buf.append("\n"))) {
  } else {
    if (OB_FAIL(buf.append(text)) || OB_FAIL(buf.append("\0"))) {
    } else {
      char* r_start = buf.ptr();
      char* t_start = buf.ptr() + r_len + 1;
      std::regex pattern(r_start, r_len);
      std::smatch results;
      ans = std::regex_search(t_start, pattern);
    }
  }
  return ret;
}

ObString ObJsonSchemaUtils::get_pointer_key(ObString& ref_str, bool& end_while)
{
  ObString key;
  char* start = ref_str.ptr();
  int key_len = 0;
  int total_len = ref_str.length();
  bool get_key = false;
  for (int i = 0; i < total_len && !get_key; ++i) {
    if (ref_str[i] != '/') {
      ++key_len;
    } else {
      get_key = true;
    }
  }
  if (get_key) {
    if (key_len + 1 < total_len) {
      key = ObString(key_len, start);
      ref_str = ObString(total_len - key_len - 1, start + key_len + 1);
    } else {
      end_while = true;
    }
  } else {
    key = ref_str;
    end_while = true; // last key
  }
  return key;
}

bool ObJsonSchemaUtils::is_legal_json_pointer_name(const ObString& name)
{
  bool ret_bool = true;
  if (name.length() == 1 && name.compare(ObJsonSchemaItem::ROOT) == 0) {
  } else {
    int len = name.length();
    for (int i = 0; i < len && ret_bool; ++i) {
      if (i == 0 && !is_legal_json_pointer_start(name[i])) {
        ret_bool = false;
      } else if (!is_legal_json_pointer_char(name[i])) {
        ret_bool = false;
      }
    }
  }
  return ret_bool;
}

bool ObJsonSchemaUtils::is_legal_json_pointer_start(const char& ch)
{
  bool ret_bool = true;
  if (!ObJsonPathUtil::letter_or_not(ch) && ch != '-' && ch != '_') {
    ret_bool = false;
  }
  return ret_bool;
}

bool ObJsonSchemaUtils::is_legal_json_pointer_char(const char& ch)
{
  bool ret_bool = true;
  if (!is_legal_json_pointer_start(ch) && !ObJsonPathUtil::is_digit(ch)) {
    ret_bool = false;
  }
  return ret_bool;
}

int ObJsonSchemaUtils::need_check_recursive(ObIArray<ObIJsonBase*> &schema_vec, bool& need_recursive, bool is_array_keywords)
{
  INIT_SUCC(ret);
  need_recursive = false;
  int size = schema_vec.count();
   for (int i = 0; i < size && !need_recursive && OB_SUCC(ret); ++i) {
    ObIJsonBase* tmp_schema = schema_vec.at(i);
    ObIJsonBase* tmp_value = nullptr;
    if (is_array_keywords) {
      if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::ITEMS, tmp_value)) || OB_ISNULL(tmp_value)) {
        if (ret == OB_SEARCH_NOT_FOUND || OB_ISNULL(tmp_value)) {
          ret = OB_SUCCESS;
          tmp_value = nullptr;
          // didn't found items, check tuple items
          if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::TUPLE_ITEMS, tmp_value)) || OB_ISNULL(tmp_value)) {
            if (ret == OB_SEARCH_NOT_FOUND || OB_ISNULL(tmp_value)) {
              ret = OB_SUCCESS;
              tmp_value = nullptr;
              // didn't found items and tuple items, check additional items
              if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::ADDITIONAL_ITEMS, tmp_value)) || OB_ISNULL(tmp_value)) {
                if (ret == OB_SEARCH_NOT_FOUND || OB_ISNULL(tmp_value)) {
                  ret = OB_SUCCESS;
                }
              } else {
                // found add items
                need_recursive = true;
              }
            }
          } else {
            // found tuple items
            need_recursive = true;
          }
        }
      } else { // found items
        need_recursive = true;
      }
    } else if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::PROPERTIES, tmp_value)) || OB_ISNULL(tmp_value)) {
      if (ret == OB_SEARCH_NOT_FOUND || OB_ISNULL(tmp_value)) {
        ret = OB_SUCCESS;
        tmp_value = nullptr;
        // didn't found properties, check pattern pro
        if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::PATTERN_PRO, tmp_value)) || OB_ISNULL(tmp_value)) {
          if (ret == OB_SEARCH_NOT_FOUND || OB_ISNULL(tmp_value)) {
            ret = OB_SUCCESS;
            tmp_value = nullptr;
            // didn't found properties and pattern pro, check additional pro
            if (OB_FAIL(tmp_schema->get_object_value(ObJsonSchemaItem::ADDITIONAL_PRO, tmp_value)) || OB_ISNULL(tmp_value)) {
              if (ret == OB_SEARCH_NOT_FOUND || OB_ISNULL(tmp_value)) {
                ret = OB_SUCCESS;
              }
            } else {
              // found add pro
              need_recursive = true;
            }
          }
        } else {
          // found pattern pro
          need_recursive = true;
        }
      }
    } else { // found properties
      need_recursive = true;
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase