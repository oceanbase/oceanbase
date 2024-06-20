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
#define USING_LOG_PREFIX LIB
#include "ob_json_diff.h"
#include "ob_json_bin.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase {
namespace common {


OB_DEF_SERIALIZE_SIZE(ObJsonDiff)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              op_,
              value_type_,
              flag_,
              path_,
              value_);
  return len;
}

OB_DEF_SERIALIZE(ObJsonDiff)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              op_,
              value_type_,
              flag_,
              path_,
              value_);
  return ret;
}

OB_DEF_DESERIALIZE(ObJsonDiff)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              op_,
              value_type_,
              flag_,
              path_,
              value_);
  return ret;
}

const char* get_json_diff_op_str(ObJsonDiffOp op)
{
  const char *str = nullptr;
  switch (op) {
    case ObJsonDiffOp::REPLACE:
      str = "replace";
      break;
    case ObJsonDiffOp::INSERT:
      str = "insert";
      break;
    case ObJsonDiffOp::REMOVE:
      str = "remove";
      break;
    default:
      str = "unknow";
      break;
  };
  return str;
}

static int print_json_path(const ObString &path, ObStringBuffer &buf)
{
  INIT_SUCC(ret);
  for (int i = 0; OB_SUCC(ret) && i < path.length(); ++i) {
    char c = *(path.ptr() + i);
    if ((c == '"' || c == '\\') && OB_FAIL(buf.append("\\", 1))) {
      LOG_WARN("append slash fail", K(ret), K(i), K(c));
    } else if (OB_FAIL(buf.append(&c, 1))) {
      LOG_WARN("append fail", K(ret), K(i), K(c));
    }
  }
  return ret;
}

int ObJsonDiff::print(ObStringBuffer &buffer)
{
  INIT_SUCC(ret);
  if (OB_FAIL(buffer.append("{"))) {
    LOG_WARN("buffer append fail", K(ret));
  } else if (OB_FAIL(buffer.append("\"op\": \""))) {
    LOG_WARN("buffer append fail", K(ret));
  } else if (OB_FAIL(buffer.append(get_json_diff_op_str(op_)))) {
    LOG_WARN("buffer append fail", K(ret));
  } else if (OB_FAIL(buffer.append("\", \"path\": \""))) {
    LOG_WARN("buffer append fail", K(ret));
  } else if (OB_FAIL(print_json_path(path_, buffer))) {
    LOG_WARN("buffer append fail", K(ret), K(path_));
  } else if (OB_FAIL(buffer.append("\""))) {
    LOG_WARN("buffer append fail", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (ObJsonDiffOp::REMOVE == op_) {
  } else if (OB_FAIL(buffer.append(", \"value\": "))) {
    LOG_WARN("buffer append fail", K(ret));
  } else {
    ObJsonBinCtx bin_ctx;
    ObJsonBin j_bin;
    ObIJsonBase *j_base = &j_bin;
    if (OB_FAIL(j_bin.reset(
        value_type_,
        value_,
        0,
        entry_var_type_,
        &bin_ctx))) {
      LOG_WARN("reset json bin fail", K(ret), K(value_type_), K(value_));
    } else if (OB_FAIL(j_base->print(buffer, true))) {
      LOG_WARN("json binary to string failed in mysql mode", K(ret), K(*j_base));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(buffer.append("}"))) {
    LOG_WARN("buffer append fail", K(ret));
  }
  return ret;
}

ObJsonBinUpdateCtx::~ObJsonBinUpdateCtx()
{
  if (nullptr != cursor_) {
    cursor_->~ObILobCursor();
    cursor_ = nullptr;
  }
}

bool ObJsonBinUpdateCtx::is_no_update()
{
  return 0 == binary_diffs_.count();
}

int ObJsonBinUpdateCtx::record_inline_diff(ObJsonDiffOp op, uint8_t value_type, const ObString &path, uint8_t entry_var_type, const ObString &value)
{
  INIT_SUCC(ret);
  ObJsonDiff json_diff;
  json_diff.op_ = op;
  json_diff.value_type_ = value_type;
  json_diff.path_ = path;
  json_diff.entry_var_type_ = entry_var_type;
  json_diff.value_ = value;
  if (OB_FAIL(json_diffs_.push_back(json_diff))) {
    LOG_WARN("push_back json diff fail", K(ret), K(json_diff));
  }
  return ret;
}


int ObJsonBinUpdateCtx::record_diff(ObJsonDiffOp op, uint8_t value_type, const ObString &path, const ObString &value)
{
  INIT_SUCC(ret);
  ObJsonDiff json_diff;
  json_diff.op_ = op;
  json_diff.value_type_ = value_type;
  json_diff.path_ = path;
  json_diff.value_ = value;
  if (OB_FAIL(json_diffs_.push_back(json_diff))) {
    LOG_WARN("push_back json diff fail", K(ret), K(json_diff));
  }
  return ret;
}

int ObJsonBinUpdateCtx::record_remove_diff(const ObString &path) {
  INIT_SUCC(ret);
  ObJsonDiff json_diff;
  json_diff.op_ = ObJsonDiffOp::REMOVE;
  json_diff.path_ = path;
  if (OB_FAIL(json_diffs_.push_back(json_diff))) {
    LOG_WARN("push_back json diff fail", K(ret), K(json_diff));
  }
  return ret;
}

int ObJsonBinUpdateCtx::record_binary_diff(int64_t offset, int64_t len)
{
  INIT_SUCC(ret);
  ObJsonBinaryDiff binary_diff;
  binary_diff.dst_offset_ = offset;
  binary_diff.dst_len_ = len;
  if (OB_FAIL(binary_diffs_.push_back(binary_diff))) {
    LOG_WARN("push_back json diff fail", K(ret), K(binary_diff));
  }
  return ret;
}


DEFINE_GET_SERIALIZE_SIZE(ObJsonDiffHeader)
{
  int64_t size = 0;
  size += serialization::encoded_length_i8(version_);
  size += serialization::encoded_length_i8(cnt_);
  return size;
}

DEFINE_SERIALIZE(ObJsonDiffHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (nullptr == buf || 0 >= buf_len || 0 > pos) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("serialize failed", K(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, new_pos, version_))) {
    LOG_ERROR("serialize failed", K(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, new_pos, cnt_))) {
    LOG_ERROR("serialize failed", K(ret), K(pos), K(buf_len));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObJsonDiffHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (nullptr == buf || 0 >= data_len || 0 > pos) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("serialize failed", K(ret), K(pos), K(data_len));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, new_pos, reinterpret_cast<int8_t*>(&version_)))) {
    LOG_ERROR("serialize failed", K(ret), K(pos), K(data_len));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, new_pos, reinterpret_cast<int8_t*>(&cnt_)))) {
    LOG_ERROR("serialize failed", K(ret), K(pos), K(data_len));
  } else {
    pos = new_pos;
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
