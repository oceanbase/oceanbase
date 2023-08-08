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
 * This file contains implementation support for the json parse abstraction.
 */

#define USING_LOG_PREFIX SQL
#include "ob_json_parse.h"

namespace oceanbase {
namespace common {

#define RELAXJSON_FLAG rapidjson::kParseObjectKeyNoQuotesFlag \
                      | rapidjson::kParseIgnoreCaseForKeyword \
                      | rapidjson::kParseRelaxNumberFlag \
                      | rapidjson::kParseCommentsFlag \
                      | rapidjson::kParseTrailingCommasFlag

#define STRICTJSON_FLAG rapidjson::kParseObjectKeyNoQuotesFlag

int ObJsonParser::get_tree(ObIAllocator *allocator, const ObString &text, ObJsonNode *&j_tree,
                           uint32_t parse_flag)
{
  INIT_SUCC(ret);
  char buf[PARSE_SYNTAXERR_MESSAGE_LENGTH] = {0};
  const char *syntaxerr = buf;
  uint64_t offset = 0;

  if (OB_FAIL(parse_json_text(allocator, text.ptr(), text.length(),
                              syntaxerr, &offset, j_tree, parse_flag))) {
    LOG_WARN("fail to parse json text", K(ret), K(text), KCSTRING(syntaxerr));
  }

  return ret;
}

int ObJsonParser::get_tree(ObIAllocator *allocator, const char *text,
                           uint64_t length, ObJsonNode *&j_tree,
                           uint32_t parse_flag)
{
  INIT_SUCC(ret);
  char buf[PARSE_SYNTAXERR_MESSAGE_LENGTH] = {0};
  const char *syntaxerr = buf;
  uint64_t offset = 0;

  if (OB_FAIL(parse_json_text(allocator, text, length,
                              syntaxerr, &offset, j_tree,
                              parse_flag))) {
    LOG_WARN("fail to parse json text", K(ret), K(length), KCSTRING(syntaxerr));
  }

  return ret;
}                    

// 1. Use 3th libraries rapidjson.
// 2. Make a copy of the source string.
// 3. Use in situ resolution mode,
//    detail:http://rapidjson.org/zh-cn/md_doc_dom_8zh-cn.html#InSituParsing
int ObJsonParser::parse_json_text(ObIAllocator *allocator, 
                                  const char *text, uint64_t length,
                                  const char *&syntaxerr, uint64_t *offset,
                                  ObJsonNode *&j_tree, uint32_t parse_flag)
{
  INIT_SUCC(ret);

  char *buf = NULL;
  if (OB_ISNULL(allocator) || OB_ISNULL(text) || length == 0) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("param is null or json text length is 0", K(allocator), KP(text), K(length));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator->alloc(length + 1)))) { // for '\0'
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for json text", K(ret), KCSTRING(text), K(length));
  } else {
    MEMCPY(buf, text, length);
    buf[length] = '\0';
    bool with_unique_key = HAS_FLAG(parse_flag, JSN_UNIQUE_FLAG);
    ObRapidJsonHandler handler(allocator, with_unique_key);
    ObRapidJsonAllocator parse_allocator(allocator);
    rapidjson::InsituStringStream ss(static_cast<char *>(buf));
    ObRapidJsonReader reader(&parse_allocator);
    rapidjson::ParseResult r;
    try {
      if (HAS_FLAG(parse_flag, JSN_RELAXED_FLAG)) {
        r = reader.Parse<RELAXJSON_FLAG>(ss, handler);
      } else if (HAS_FLAG(parse_flag, JSN_STRICT_FLAG)) {
        r = reader.Parse<STRICTJSON_FLAG>(ss, handler);
      } else {
        r = reader.Parse<rapidjson::kParseInsituFlag>(ss, handler);
      }
    } catch (const std::bad_alloc &e) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for json text", K(ret), KCSTRING(text), K(length));
    }

    if (OB_FAIL(ret)) {
    } else if (!r.IsError()) {
      j_tree = handler.get_built_doc();
      if (OB_ISNULL(j_tree) && OB_NOT_NULL(syntaxerr)) {
        allocator->free(buf);
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The parsing failed for some other reason than a syntax error.");
        syntaxerr = NULL;
      }
    } else { // parse wrong, fill syntaxerr and offset.
      allocator->free(buf);
      if (handler.has_duplicate_key()) {
        ret = OB_ERR_DUPLICATE_KEY;
      } else {
        ret = OB_ERR_INVALID_JSON_TEXT;
      }
      if (offset != NULL){
        *offset = reader.GetErrorOffset();
      }
      if (syntaxerr != NULL) {
        syntaxerr = rapidjson::GetParseError_En(reader.GetParseErrorCode());
      }
      LOG_WARN("fail to parse json text", K(ret), K(r.Code()));
    }
  }

  if (OB_SUCC(ret) && OB_ISNULL(j_tree->get_allocator())) {
    j_tree->set_allocator(allocator);
  }

  return ret;
}

bool ObJsonParser::is_json_doc_over_depth(uint64_t depth)
{
  bool is_over = false;

  if (depth > JSON_DOCUMENT_MAX_DEPTH) {
    is_over = true;
  }

  return is_over;
}

int ObJsonParser::check_json_syntax(const ObString &j_doc, ObIAllocator *allocator,
                                    uint32_t parse_flag)
{
  INIT_SUCC(ret);
  char syntax_buf[PARSE_SYNTAXERR_MESSAGE_LENGTH] = {0};
  const char *syntaxerr = syntax_buf;
  uint64_t offset = 0;
  int32_t length = j_doc.length();
  char *alloc_buf = NULL;
  ObArenaAllocator tmp_allocator;
  ObIAllocator *allocator_ptr = OB_ISNULL(allocator) ? &tmp_allocator : allocator;

  if (length == 0) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("json text length is 0", K(ret), K(j_doc));
  } else if (OB_ISNULL(alloc_buf = reinterpret_cast<char *>(allocator_ptr->alloc(length + 1)))) { // for '\0'
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for json text", K(ret), K(j_doc));
  } else {
    MEMCPY(alloc_buf, j_doc.ptr(), length);
    alloc_buf[length] = '\0';
    if (!HAS_FLAG(parse_flag, JSN_UNIQUE_FLAG)) {
      ObJsonSyntaxCheckHandler handler(allocator);
      ObRapidJsonAllocator parse_allocator(allocator);
      rapidjson::InsituStringStream ss(static_cast<char *>(alloc_buf));
      ObRapidJsonReader reader(&parse_allocator);
      rapidjson::ParseResult r;
      try {
        if (HAS_FLAG(parse_flag, JSN_RELAXED_FLAG)) {
          r = reader.Parse<RELAXJSON_FLAG>(ss, handler);
        } else if (HAS_FLAG(parse_flag, JSN_STRICT_FLAG)) {
          r = reader.Parse<STRICTJSON_FLAG>(ss, handler);
        } else {
          r = reader.Parse<rapidjson::kParseInsituFlag>(ss, handler);
        }
      } catch (const std::bad_alloc &e) {
        allocator->free(alloc_buf);
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for json text", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (r.IsError()) {
        allocator->free(alloc_buf);
        if (handler.is_too_deep()) {
          ret = OB_ERR_JSON_OUT_OF_DEPTH;
        } else {
          ret = OB_ERR_INVALID_JSON_TEXT;
        }
        offset = reader.GetErrorOffset();
        syntaxerr = rapidjson::GetParseError_En(reader.GetParseErrorCode());
        LOG_DEBUG("fail to parse json text", K(ret), K(r.Code()), KCSTRING(syntaxerr), K(offset));
      }
    } else {
      ObRapidJsonHandler handler(allocator, true);
      ObRapidJsonAllocator parse_allocator(allocator);
      rapidjson::InsituStringStream ss(static_cast<char *>(alloc_buf));
      ObRapidJsonReader reader(&parse_allocator);
      rapidjson::ParseResult r;
      try {
        if (HAS_FLAG(parse_flag, JSN_RELAXED_FLAG)) {
          r = reader.Parse<RELAXJSON_FLAG>(ss, handler);
        } else if (HAS_FLAG(parse_flag, JSN_STRICT_FLAG)) {
          r = reader.Parse<STRICTJSON_FLAG>(ss, handler);
        } else {
          r = reader.Parse<rapidjson::kParseInsituFlag>(ss, handler);
        }
      } catch (const std::bad_alloc &e) {
        allocator->free(alloc_buf);
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for json text", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (!r.IsError()) {
        if (OB_ISNULL(handler.get_built_doc()) && OB_NOT_NULL(syntaxerr)) {
          allocator->free(alloc_buf);
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The parsing failed for some other reason than a syntax error.");
          syntaxerr = NULL;
        }
      } else {
        allocator->free(alloc_buf);
        if (lib::is_oracle_mode() && handler.has_duplicate_key()) {
          ret = OB_ERR_DUPLICATE_KEY;
        } else {
          ret = OB_ERR_INVALID_JSON_TEXT;
        }
        if (syntaxerr != NULL) {
          syntaxerr = rapidjson::GetParseError_En(reader.GetParseErrorCode());
        }
        LOG_WARN("fail to parse json text", K(ret), K(r.Code()));
      }
    }
  }

  return ret;
}

// Function which is called on each value found in the JSON document being parsed.
bool ObRapidJsonHandler::seeing_value(ObJsonNode *value) 
{
  bool is_continue = true;

  if (OB_ISNULL(value)) {
    is_continue = false;
  } else {
    switch (next_state_) {
      case ObJsonExpectNextState::EXPECT_ANYTHING: {
        dom_as_built_ = value;
        next_state_ = ObJsonExpectNextState::EXPECT_EOF;
        break;
      }

      case ObJsonExpectNextState::EXPECT_ARRAY_VALUE: {
        INIT_SUCC(ret);
        ObJsonArray *array = dynamic_cast<ObJsonArray *>(current_element_);
        if (OB_FAIL(array->append(value))) {
          LOG_WARN("fail to append element to json array", K(ret));
          is_continue = false;
        }
        break;
      }

      case ObJsonExpectNextState::EXPECT_OBJECT_VALUE: {
        INIT_SUCC(ret);
        next_state_ = ObJsonExpectNextState::EXPECT_OBJECT_KEY;
        ObJsonObject *object = dynamic_cast<ObJsonObject *>(current_element_);
        if (OB_FAIL(object->add(key_, value, with_unique_key_, true, false))) {
          LOG_WARN("fail to add element to json object", K(ret));
          if (ret == OB_ERR_DUPLICATE_KEY) {
            with_duplicate_key_ = true;
          }
          is_continue = false;
        }
        break;
      }

      default: {
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected json value", K(next_state_));
        is_continue = false;
        break;
      }
    }
  }

  return is_continue;
}

bool ObRapidJsonHandler::is_start_object_or_array(ObJsonNode *value, ObJsonExpectNextState next_state)
{
  bool is_continue = false;

  if (ObJsonParser::is_json_doc_over_depth(depth_)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "current json doc is over depth", K(OB_ERR_JSON_OUT_OF_DEPTH));
  } else {
    depth_++;
    is_continue = seeing_value(value);
    current_element_ = value;
    next_state_ = next_state;
  }
  
  return is_continue;
}

bool ObRapidJsonHandler::is_end_object_or_array() 
{
  bool is_continue = true;
  depth_--;

  ObJsonNode *parent = current_element_->get_parent();
  if (OB_ISNULL(parent)) { // current is root
    if (depth_ != 0) {
      is_continue = false;
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected tree depth", K(depth_));
    } else {
      next_state_ = ObJsonExpectNextState::EXPECT_EOF;
    }
  } else if (parent->json_type() == ObJsonNodeType::J_OBJECT) { // parent is object
    next_state_ = ObJsonExpectNextState::EXPECT_OBJECT_KEY;
  } else { // parent is array
    next_state_ = ObJsonExpectNextState::EXPECT_ARRAY_VALUE;
  }

  if (is_continue) {
    if (current_element_->json_type() == ObJsonNodeType::J_OBJECT) { // current is object
      // Sort the key-value pairs of the ObJsonObject at the current level.
      ObJsonObject *obj = static_cast<ObJsonObject *>(current_element_);
      obj->update_serialize_size();
      obj->stable_sort();
      int64_t origin_num = obj->element_count();
      obj->unique();
      if (with_unique_key_ && obj->element_count() < origin_num) {
        is_continue = false;
        with_duplicate_key_ = true;
      }
    } else { // current is array
      current_element_->update_serialize_size();
    }
  }
  current_element_ = parent;

  return is_continue;
}

bool ObRapidJsonHandler::Null()
{
  bool is_continue = false;

  void *buf = alloc(sizeof(ObJsonNull));
  if (OB_ISNULL(buf)) {
    LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for null json node", K(OB_ALLOCATE_MEMORY_FAILED));
  } else {
    ObJsonNull *node = new (buf) ObJsonNull();
    is_continue = seeing_value(node);
  }

  return is_continue;
}

bool ObRapidJsonHandler::Bool(bool value)
{
  bool is_continue = false;

  void *buf = alloc(sizeof(ObJsonBoolean));
  if (OB_ISNULL(buf)) {
    LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for boolean json node", K(OB_ALLOCATE_MEMORY_FAILED));
  } else {
    ObJsonBoolean *node = new (buf) ObJsonBoolean(value);
    is_continue = seeing_value(node);
  }

  return is_continue;
}

bool ObRapidJsonHandler::Int(int value)
{
  bool is_continue = false;

  void *buf = alloc(sizeof(ObJsonInt));
  if (OB_ISNULL(buf)) {
    LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for int json node", K(OB_ALLOCATE_MEMORY_FAILED));
  } else {
    ObJsonInt *node = new (buf) ObJsonInt(value);
    is_continue = seeing_value(node);
  }

  return is_continue;
}

bool ObRapidJsonHandler::Uint(unsigned value)
{
  bool is_continue = false;

  void *buf = alloc(sizeof(ObJsonInt)); // adapt mysql, use ObJsonInt
  if (OB_ISNULL(buf)) {
    LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for uint json node", K(OB_ALLOCATE_MEMORY_FAILED));
  } else {
    ObJsonInt *node = new (buf) ObJsonInt(value);
    is_continue = seeing_value(node);
  }

  return is_continue;
}

bool ObRapidJsonHandler::Int64(int64_t value)
{
  bool is_continue = false;

  void *buf = alloc(sizeof(ObJsonInt));
  if (OB_ISNULL(buf)) {
    LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for int64 json node", K(OB_ALLOCATE_MEMORY_FAILED));
  } else {
    ObJsonInt *node = new (buf) ObJsonInt(value);
    is_continue = seeing_value(node);
  }

  return is_continue;
}

bool ObRapidJsonHandler::Uint64(uint64_t value)
{
  bool is_continue = false;

  void *buf = alloc(sizeof(ObJsonUint));
  if (OB_ISNULL(buf)) {
    LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for uint64 json node", K(OB_ALLOCATE_MEMORY_FAILED));
  } else {
    ObJsonUint *node = new (buf) ObJsonUint(value);
    is_continue = seeing_value(node);
  }

  return is_continue;
}

bool ObRapidJsonHandler::Double(double value)
{
  bool is_continue = false;

  if (!std::isfinite(value)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "value is not finite", K(value));
  } else {
    void *buf = alloc(sizeof(ObJsonDouble));
    if (OB_ISNULL(buf)) {
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for double json node", K(OB_ALLOCATE_MEMORY_FAILED));
    } else {
      ObJsonDouble *node = new (buf) ObJsonDouble(value);
      is_continue = seeing_value(node);
    }
  }

  return is_continue;
}

// Never called, since we don't instantiate the parser with kParseNumbersAsStringsFlag.
bool ObRapidJsonHandler::RawNumber(const char *, rapidjson::SizeType, bool copy)
{
  UNUSED(copy);

  return false;
}

bool ObRapidJsonHandler::String(const char *str, rapidjson::SizeType length, bool copy)
{
  bool is_continue = false;

  void *buf = alloc(sizeof(ObJsonString));
  if (OB_ISNULL(buf)) {
    LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for string json node", K(OB_ALLOCATE_MEMORY_FAILED));
  } else {
    if (copy) {
      void *dst_buf = NULL;
      ObString src_str(length, str);
      if (length > 0) {
        if (OB_ISNULL(dst_buf = allocator_->alloc(length))) {
          LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "allocate memory fail", K(length));
        } else {
          MEMCPY(dst_buf, src_str.ptr(), src_str.length());
          ObJsonString *node = new (buf) ObJsonString(static_cast<char *>(dst_buf), length);
          is_continue = seeing_value(node);
        }
      } else {
        ObJsonString *node = new (buf) ObJsonString(str, length);
        is_continue = seeing_value(node);
      }
    } else {
      ObJsonString *node = new (buf) ObJsonString(str, length);
      is_continue = seeing_value(node);
    }
  }

  return is_continue;
}


bool ObRapidJsonHandler::StartObject()
{
  bool is_continue = false;

  void *buf = alloc(sizeof(ObJsonObject));
  if (OB_ISNULL(buf)) {
    LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for object json node", K(OB_ALLOCATE_MEMORY_FAILED));
  } else {
    ObJsonObject *node= new (buf) ObJsonObject(allocator_);
    is_continue = is_start_object_or_array(node, ObJsonExpectNextState::EXPECT_OBJECT_KEY);
  }

  return is_continue;
}

bool ObRapidJsonHandler::EndObject(rapidjson::SizeType length)
{
  UNUSED(length);
  bool is_continue = true;

  if (next_state_ != ObJsonExpectNextState::EXPECT_OBJECT_KEY) {
    is_continue = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected next json state", K(next_state_));
  } else {
    is_continue = is_end_object_or_array();
  }

  return is_continue;
}

bool ObRapidJsonHandler::StartArray()
{
  bool is_continue = false;

  void *buf = alloc(sizeof(ObJsonArray));
  if (OB_ISNULL(buf)) {
    LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory for array json node", K(OB_ALLOCATE_MEMORY_FAILED));
  } else {
    ObJsonArray *node = new (buf) ObJsonArray(allocator_);
    is_continue = is_start_object_or_array(node, ObJsonExpectNextState::EXPECT_ARRAY_VALUE);
  }

  return is_continue;
}

bool ObRapidJsonHandler::EndArray(rapidjson::SizeType length)
{
  UNUSED(length);
  bool is_continue = true;

  if (next_state_ != ObJsonExpectNextState::EXPECT_ARRAY_VALUE) {
    is_continue = false;
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected next json state", K(next_state_));
  } else {
    is_continue = is_end_object_or_array();
  }

  return is_continue;
}

bool ObRapidJsonHandler::Key(const char *str, rapidjson::SizeType length, bool copy)
{
  bool is_continue = false;

  if (next_state_ != ObJsonExpectNextState::EXPECT_OBJECT_KEY) {
    LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "unexpected next json state", K(next_state_));
  } else {
    // need deep-copy
    if (copy) {
      void *dst_buf = NULL;
      ObString src_str(length, str);
      if (length > 0) {
        if (OB_ISNULL(dst_buf = allocator_->alloc(length))) {
          LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "allocate memory fail", K(length));
        } else {
          MEMCPY(dst_buf, src_str.ptr(), src_str.length());
          key_.assign_ptr(static_cast<char *>(dst_buf), length);
          is_continue = true;
          next_state_ = ObJsonExpectNextState::EXPECT_OBJECT_VALUE;
        }
      } else {
        key_ = ObString();
        is_continue = true;
        next_state_ = ObJsonExpectNextState::EXPECT_OBJECT_VALUE;
      }
    } else {
      is_continue = true;
      next_state_ = ObJsonExpectNextState::EXPECT_OBJECT_VALUE;
      key_.assign_ptr(str, length);
    }
  }

  return is_continue;
}
#undef TEST_RELAXJSON_FLAG

} // namespace common
} // namespace oceanbase
