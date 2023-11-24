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

#ifndef OCEANBASE_SQL_OB_JSON_DIFF
#define OCEANBASE_SQL_OB_JSON_DIFF

#include "common/object/ob_object.h"
#include "lib/json_type/ob_json_common.h"

namespace oceanbase {
namespace common {

// only support one element
enum class ObJsonDiffOp : uint8_t
{
  INVALID = 0,
  // same as json_replace(json, path, value)
  REPLACE = 1,
  // same as json_array_insert(json, path, value) if json is array element
  // same as json_insert(json, path, value)
  INSERT,
  // same as json_remove(json, path)
  REMOVE,
  RENAME
};

const char* get_json_diff_op_str(ObJsonDiffOp op);


struct ObJsonDiff {
  OB_UNIS_VERSION(1);
public:

  ObJsonDiff() :
      op_(ObJsonDiffOp::INVALID),
      value_type_(0),
      flag_(0),
      path_(),
      value_()
  {}

  int print(ObStringBuffer &buffer);

public:
  ObJsonDiffOp op_;
  uint8_t value_type_;
  union {
    struct {
      uint16_t entry_var_type_ : 2;
    };
    uint16_t flag_;
  };
  ObString path_;
  ObString value_;
  TO_STRING_KV(
      K(op_),
      K(value_type_),
      K(entry_var_type_),
      K(path_),
      K(value_));
};

struct ObJsonBinaryDiff
{
  ObJsonBinaryDiff():
    dst_offset_(0),
    dst_len_(0)
  {}
  int64_t dst_offset_;
  int64_t dst_len_;

  TO_STRING_KV(
      K(dst_offset_),
      K(dst_len_))
};


typedef ObSEArray<ObJsonDiff, 10> ObJsonDiffArray;
typedef ObSEArray<ObJsonBinaryDiff, 10> ObJsonBinaryDiffArray;

class ObJsonBinUpdateCtx {
public:
  ObJsonBinUpdateCtx(ObIAllocator *allocator) :
      allocator_(allocator),
      is_rebuild_all_(false),
      cursor_(nullptr),
      tmp_buffer_(allocator)
  {}
  ~ObJsonBinUpdateCtx();

  ObStringBuffer& get_tmp_buffer() { tmp_buffer_.reuse(); return tmp_buffer_; }

  int current_data(ObString &data) const
  {
    return cursor_->get_data(data);
  }

  bool is_no_update();
  bool is_rebuild_all() { return is_rebuild_all_; }

  void set_lob_cursor(ObILobCursor *cursor) { cursor_ = cursor; }

  int record_inline_diff(ObJsonDiffOp op, uint8_t value_type, const ObString &path, uint8_t entry_var_type, const ObString &value);
  int record_remove_diff(const ObString &path);
  int record_diff(ObJsonDiffOp op, uint8_t value_type, const ObString &path, const ObString &value);
  int record_binary_diff(int64_t offset, int64_t len);

public:
  ObIAllocator *allocator_;
  bool is_rebuild_all_;
  ObILobCursor *cursor_;
  ObStringBuffer tmp_buffer_;
  ObJsonDiffArray json_diffs_;
  ObJsonBinaryDiffArray binary_diffs_;

  TO_STRING_KV(
      K(is_rebuild_all_),
      K(json_diffs_),
      K(binary_diffs_));

};

struct ObJsonDiffHeader {

  ObJsonDiffHeader() :
      version_(0),
      cnt_(0)
  {}
  uint8_t version_;
  uint8_t cnt_;
  TO_STRING_KV(
      K(version_),
      K(cnt_));
  NEED_SERIALIZE_AND_DESERIALIZE;
};


} // namespace common
} // namespace oceanbase
#endif // OCEANBASE_SQL_OB_JSON_BIN