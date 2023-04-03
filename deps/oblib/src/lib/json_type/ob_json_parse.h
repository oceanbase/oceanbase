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
 * This file contains interface support for the json parse abstraction.
 */

#ifndef OCEANBASE_SQL_OB_JSON_PARSE
#define OCEANBASE_SQL_OB_JSON_PARSE

#include "ob_json_tree.h"
#include <rapidjson/error/en.h>
#include <rapidjson/error/error.h>
#include <rapidjson/memorystream.h>
#include <rapidjson/reader.h>

namespace oceanbase {
namespace common {

#define HAS_FLAG(flags, specific) (((flags) & (specific)) == (specific))
#define ADD_FLAG_IF_NEED(expr, flags, specific) \
  if (expr) {                                   \
    (flags) |= (specific);                      \
  }

class ObJsonParser final
{
public:
  static const uint32_t JSN_DEFAULT_FLAG = 0;
  static const uint32_t JSN_STRICT_FLAG = 1;
  static const uint32_t JSN_RELAXED_FLAG = 2;
  static const uint32_t JSN_UNIQUE_FLAG = 4;

  static const int PARSE_SYNTAXERR_MESSAGE_LENGTH = 256;
  static int get_tree(ObIAllocator *allocator, const ObString &text,
                      ObJsonNode *&j_tree, uint32_t parse_flag = 0);
  static int get_tree(ObIAllocator *allocator, const char *text, uint64_t length,
                      ObJsonNode *&j_tree, uint32_t parse_flag = 0);

  // Parse json text to json tree with rapidjson.
  // 
  // @param [in]  allocator   Alloc memory for json text.
  // @param [in]  text        The json documemt which need to parse.
  // @param [in]  length      The length of json documemt.
  // @param [out] syntaxerr   The syntax error message of json document.
  // @param [out] offset      The offset of syntax error message.
  // @param [out] j_tree      The json tree after successful parsing
  // @return Returns OB_SUCCESS on success, error code otherwise.
  static int parse_json_text(ObIAllocator *allocator, 
                             const char *text, uint64_t length,
                             const char *&syntaxerr, uint64_t *offset,
                             ObJsonNode *&j_tree, uint32_t parse_flag = 0);

  // The tree has a maximum depth of 100 layers
  static constexpr int JSON_DOCUMENT_MAX_DEPTH = 100;

  // Verify that the current JSON tree depth exceeds the maximum depth
  //
  // @param [in] depth Current json tree depth.
  // @return  Returns true beyond the maximum depth, false otherwise.
  static bool is_json_doc_over_depth(uint64_t depth);
  
  // Check json document syntax.(for json_valid)
  //
  // @param [in] j_doc The json documemt which need to check.
  // @param [in] allocator   Alloc memory In the parsing process.
  // @return  Returns OB_SUCCESS on success, error code otherwise.
  static int check_json_syntax(const ObString &j_doc, ObIAllocator *allocator = NULL,
                               uint32_t parse_flag = 0);
private:
  DISALLOW_COPY_AND_ASSIGN(ObJsonParser);
};

class ObRapidJsonAllocator
{
public:
  ObRapidJsonAllocator() // rapidjson needs a constructor with no arguments
      : allocator_(NULL)
  {
  }
  explicit ObRapidJsonAllocator(ObIAllocator *allocator)
      : allocator_(allocator)
  {
  }
  virtual ~ObRapidJsonAllocator() {}
  static const bool kNeedFree = false;
  void *Malloc(size_t size)
  { 
    return (size != 0) ? allocator_->alloc(size) : NULL;
  }
  void *Realloc(void *originalPtr, size_t originalSize, size_t newSize)
  {
    void *ptr = NULL;
    if (newSize == 0) {
      allocator_->free(originalPtr); // it is save to free NULL
    } else if (originalPtr == NULL) {
      ptr = allocator_->alloc(newSize);
    } else { // original ptr is not null, new size is not zero
      ptr = allocator_->realloc(originalPtr, originalSize, newSize);
    }
    if (OB_ISNULL(ptr)) {
      throw std::bad_alloc();
    }
    return ptr;
  }
  static void Free(void *ptr) { UNUSED(ptr); } // do nothing
private:
  ObIAllocator *allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObRapidJsonAllocator);
};

typedef rapidjson::GenericReader<rapidjson::UTF8<>, rapidjson::UTF8<>, ObRapidJsonAllocator> ObRapidJsonReader;
class ObRapidJsonHandler final : public rapidjson::BaseReaderHandler<>
{
public:
  enum class ObJsonExpectNextState
  {
    EXPECT_ANYTHING,
    EXPECT_ARRAY_VALUE,
    EXPECT_OBJECT_KEY,
    EXPECT_OBJECT_VALUE,
    EXPECT_EOF
  };
  explicit ObRapidJsonHandler(ObIAllocator *allocator, bool with_unique_key = false)
      : next_state_(ObJsonExpectNextState::EXPECT_ANYTHING),
        dom_as_built_(NULL),
        current_element_(NULL),
        depth_(0),
        key_(),
        allocator_(allocator),
        with_unique_key_(with_unique_key),
        with_duplicate_key_(false)
  {
  }
  virtual ~ObRapidJsonHandler() {}

  OB_INLINE void *alloc(const int64_t size)
  {
    void *buf = NULL;
    if (OB_NOT_NULL(allocator_)) {
      buf = allocator_->alloc(size);
    }
    return buf;
  }
  OB_INLINE void free(void *ptr)
  {
    if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(ptr)) {
      allocator_->free(ptr);
    }
  }
  OB_INLINE ObJsonNode *get_built_doc()
  {
    return dom_as_built_;
  }

  bool seeing_value(ObJsonNode *value);
  bool is_start_object_or_array(ObJsonNode *value, ObJsonExpectNextState next_state);
  bool is_end_object_or_array();
  bool Null();
  bool Bool(bool value);
  bool Int(int value);
  bool Uint(unsigned value);
  bool Int64(int64_t value);
  bool Uint64(uint64_t value);
  bool Double(double value);
  bool RawNumber(const char *, rapidjson::SizeType, bool copy);
  bool String(const char *str, rapidjson::SizeType length, bool copy);
  bool StartObject();
  bool EndObject(rapidjson::SizeType length);
  bool StartArray();
  bool EndArray(rapidjson::SizeType length);
  bool Key(const char *str, rapidjson::SizeType length, bool copy);
  bool has_duplicate_key() { return with_duplicate_key_; }

private:
  ObJsonExpectNextState next_state_;  // The state that is expected to be resolved next.
  ObJsonNode *dom_as_built_;          // The root node of json tree.
  ObJsonNode *current_element_;       // The node is parsing.
  uint64_t depth_;                    // The depth of the tree currently parsed.
  common::ObString key_;              // The current resolved key value
  ObIAllocator *allocator_;           // A memory allocator that allocates node memory.
  bool with_unique_key_;              // Whether check unique key for object
  bool with_duplicate_key_;           // Whether contain duplicate key for object
  DISALLOW_COPY_AND_ASSIGN(ObRapidJsonHandler);
};

// for json_valid
class ObJsonSyntaxCheckHandler final : public rapidjson::BaseReaderHandler<>
{
public:
  explicit ObJsonSyntaxCheckHandler(ObIAllocator *allocator)
      : allocator_(allocator),
        _depth(0),
        _is_too_deep(false)
  {
  }
  virtual ~ObJsonSyntaxCheckHandler() {}
  OB_INLINE bool StartObject()
  {
    _is_too_deep = ObJsonParser::is_json_doc_over_depth(++_depth);
    return !_is_too_deep;
  }
  OB_INLINE bool EndObject(rapidjson::SizeType length)
  {
    UNUSED(length);
    --_depth;
    return true;
  }
  OB_INLINE bool StartArray()
  {
    _is_too_deep = ObJsonParser::is_json_doc_over_depth(++_depth);
    return !_is_too_deep;
  }
  OB_INLINE bool EndArray(rapidjson::SizeType length)
  {
    UNUSED(length);
    --_depth;
    return true;
  }
  OB_INLINE bool is_too_deep() const { return _is_too_deep; }
private:
  ObIAllocator *allocator_;
  uint64_t _depth;
  bool _is_too_deep;
  DISALLOW_COPY_AND_ASSIGN(ObJsonSyntaxCheckHandler);
};

} // namespace common
} // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_JSON_PARSE