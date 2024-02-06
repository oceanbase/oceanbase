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
 * This file contains implementation for lob_access_utils.
 */

#ifndef OCEANBASE_SHARE_OB_LOB_ACCESS_UTILS_
#define OCEANBASE_SHARE_OB_LOB_ACCESS_UTILS_

#include "share/ob_errno.h"
#include "common/object/ob_object.h"
#include "common/object/ob_obj_type.h"
#include "sql/session/ob_basic_session_info.h"
#include "storage/lob/ob_lob_manager.h"

namespace oceanbase
{
namespace common
{
// 1. This function is used to control plan generation or lob output when execution.
// 2. Should not use it this to judge whether the inputs having or not having lob header!
//    Because both format may exist in one executing routing
OB_INLINE bool ob_enable_lob_locator_v2()
{
  const uint64_t ob_cluster_ver = GET_MIN_CLUSTER_VERSION();
  bool bret = ob_cluster_ver > CLUSTER_VERSION_4_0_0_0;
  return bret;
}

OB_INLINE bool ob_enable_datum_cast_debug_log()
{
  return false;
}

// Notice: cannot support obobj funcs/compare (in lib dir)
enum ObTextStringIterState
{
  TEXTSTRING_ITER_INVALID = 0,
  TEXTSTRING_ITER_INIT = 1,
  TEXTSTRING_ITER_NEXT = 2,
  TEXTSTRING_ITER_END = 3
};

// iterator context for lob type access
struct ObLobTextIterCtx
{
  static const uint32_t OB_LOB_ITER_DEFAULT_BUFFER_LEN = 2 * 1024 * 1024; // 2M bytes

  ObLobTextIterCtx(ObLobLocatorV2 &locator, const sql::ObBasicSessionInfo *session,
                   ObIAllocator *allocator = NULL, uint32_t buffer_len = OB_LOB_ITER_DEFAULT_BUFFER_LEN) :
    alloc_(allocator), session_(session), buff_(NULL), buff_byte_len_(buffer_len), start_offset_(0),
    total_access_len_(0), total_byte_len_(0), content_byte_len_(0), content_len_(0),
    reserved_byte_len_(0), reserved_len_(0), accessed_byte_len_(0), accessed_len_(0),
    last_accessed_byte_len_(0), last_accessed_len_(0), iter_count_(0), is_cloned_temporary_(false),
    is_backward_(false), locator_(locator), lob_query_iter_(NULL)
  {}

  TO_STRING_KV(KP_(alloc), KP_(session), KP_(buff), K_(buff_byte_len), K_(start_offset), K_(total_access_len),
               K_(content_byte_len), K_(content_len), K_(reserved_byte_len), K_(reserved_len),
               K_(accessed_byte_len), K_(accessed_len),
               K_(last_accessed_byte_len), K_(last_accessed_len), K_(iter_count),
               K_(is_cloned_temporary), K_(is_backward), K_(locator), KP_(lob_query_iter));

  void init(bool is_clone = false);
  void reuse(); // reuse this ctx for access the same lob again
  OB_INLINE void unset_clone() { is_cloned_temporary_ = false; }

  // member variables
  ObIAllocator *alloc_;
  const sql::ObBasicSessionInfo *session_;
  char *buff_;        // buffer for reading next block;
  uint32_t buff_byte_len_; // buffer byte length, set to default size if user input is too small
  uint64_t start_offset_; // lob start access offset only used when first calling get next block
  int64_t total_access_len_; // total char length for reading, will access full lob if it is 0
  int64_t total_byte_len_;

  // avaliable content length start from buff_;
  uint32_t content_byte_len_; // content byte length
  uint32_t content_len_; // content char length

  // reserved len from buff_, when calling get next block, tail of last content will be reserved
  // in buff to reserved_byte_len, new content will be put from buff + reserved_byte_len_ to end of buffer
  uint32_t reserved_byte_len_; // reserved byte length from header
  uint32_t reserved_len_; // reserved char length from header

  // accessed total len by get next row
  uint32_t accessed_byte_len_; // total accessed byte_len_
  uint32_t accessed_len_; // total accessed char_len_

  // accessed total len by get next row last time
  uint32_t last_accessed_byte_len_; // total accessed byte length before current get next block
  uint32_t last_accessed_len_; // total accessed char length before current get next block
  uint32_t iter_count_;

  bool is_cloned_temporary_; // locator_ is a cloned local temporary lob
  bool is_backward_;

  ObLobLocatorV2 locator_;
  ObLobQueryIter *lob_query_iter_;
};

// wrapper class to handle string/text type input
class ObTextStringIter
{
public:
  static const uint32_t DEAFULT_LOB_PREFIX_CHAR_LEN = 1000;
  static const uint32_t MAX_CHAR_MULTIPLIER = 4;
  ObTextStringIter(ObObjType type, ObCollationType cs_type, const ObString &datum_str,
                   bool has_lob_header) :
    type_(type), cs_type_(cs_type), is_init_(false), is_lob_(false), is_outrow_(false),
    has_lob_header_(has_lob_header), state_(TEXTSTRING_ITER_INVALID), datum_str_(datum_str),
    ctx_(nullptr), err_ret_(OB_SUCCESS)
  {
    if (is_lob_storage(type)) {
      validate_has_lob_header(has_lob_header_);
    }
  }

  ObTextStringIter(const ObObj &obj) :
    type_(obj.get_type()), cs_type_(obj.get_collation_type()), is_init_(false), is_lob_(false),
    is_outrow_(false), has_lob_header_(obj.has_lob_header()), state_(TEXTSTRING_ITER_INVALID),
    datum_str_(obj.get_string()), ctx_(nullptr), err_ret_(OB_SUCCESS)
  {
    if (is_lob_storage(obj.get_type())) {
      validate_has_lob_header(has_lob_header_);
    }
  }
  ~ObTextStringIter();

  TO_STRING_KV(K_(type), K_(cs_type), K_(is_init), K_(is_lob), K_(is_outrow),
    K_(state), K(datum_str_), KP_(ctx), K_(err_ret));

  int init(uint32_t buffer_len,
           const sql::ObBasicSessionInfo *session = NULL,
           ObIAllocator *allocator = NULL,
           bool clone_remote = false);

  ObTextStringIterState get_next_block(ObString &str);

  int get_current_block(ObString &str);

  int get_full_data(ObString &data_str, ObIAllocator *allocator = nullptr);

  int get_inrow_or_outrow_prefix_data(ObString &data_str,
                                      uint32_t prefix_char_len = DEAFULT_LOB_PREFIX_CHAR_LEN);

  void reset();
  void set_start_offset(uint64_t offset);

  void set_access_len(int64_t char_len); // total read len of outrow lob
  void set_reserved_len(uint32_t reserved_len);
  void set_reserved_byte_len(uint32_t reserved_byte_len);
  void reset_reserve_len();
  void set_backward();
  void set_forward();

  uint64_t get_start_offset();
  uint32_t get_last_accessed_len();
  uint32_t get_last_accessed_byte_len();
  uint32_t get_accessed_len();
  uint32_t get_accessed_byte_len();
  int get_inner_ret() { return err_ret_; }
  bool is_outrow_lob() { return is_outrow_; };
  int get_byte_len(int64_t &byte_len);
  int get_char_len(int64_t &char_length);
  uint32_t get_iter_count();
  uint32_t get_reserved_char_len();
  uint32_t get_reserved_byte_len();
  static int append_outrow_lob_fulldata(ObObj &obj,
                                        const sql::ObBasicSessionInfo *session,
                                        ObIAllocator &allocator);
  static int convert_outrow_lob_to_inrow_templob(const ObObj &in_obj,
                                                 ObObj &out_obj,
                                                 const sql::ObBasicSessionInfo *session,
                                                 ObIAllocator *allocator,
                                                 bool allow_persist_inrow = false,
                                                 bool need_deep_copy = false);

private:
  int get_outrow_lob_full_data(ObIAllocator *allocator = nullptr);
  int get_first_block(ObString &str);
  int get_next_block_inner(ObString &str);
  int get_outrow_prefix_data(uint32_t prefix_char_len);
  int reserve_data();
  int reserve_byte_data();
  OB_INLINE bool is_valid_for_config()
  {
    return (is_init_ && is_outrow_ && has_lob_header_
            && state_ == TEXTSTRING_ITER_INIT && OB_NOT_NULL(ctx_));
  }
private:
  ObObjType type_;
  ObCollationType cs_type_;
  uint32_t is_init_ : 1;
  uint32_t is_lob_ : 1;
  uint32_t is_outrow_ : 1;
  uint32_t has_lob_header_ : 1;// 4.0 lob compatibility
  uint32_t reserved : 28;
  ObTextStringIterState state_;
  const ObString datum_str_;
  ObLobTextIterCtx *ctx_;
  int err_ret_;
};

// wrapper class to handle templob output(including string types)
class ObTextStringResult
{
public:
  ObTextStringResult(const ObObjType type,  bool has_lob_header, ObIAllocator *allocator) :
    type_(type), buffer_(NULL), buff_len_(0), pos_(0), is_outrow_templob_(false),
    has_lob_header_(has_lob_header), is_init_(false), alloc_(allocator)
  {
    if (is_lob_storage(type)) {
      validate_has_lob_header(has_lob_header_);
    }
  }
  ~ObTextStringResult(){};

  TO_STRING_KV(K_(type), KP_(buffer), K_(buff_len), K_(pos), K_(is_outrow_templob),
               K_(has_lob_header), K_(is_init), KP_(alloc));

  static const uint32_t MAX_TMP_LOB_HEADER_LEN = 1 * 1024;

  // create resource by expr.datum_.type_ and has_lob_header_
  // inrow lobs: create medmory buffer with expr.get_str_res_mem, or allocator in cast params;
  // outrow lobs: create memory for locator, and tmp file for outrow data (not implemented)
  // support user assigned allocator
  // Notice:
  // 1. all lobs created by this class should be temp lobs
  // 2. if has_lob_header_ is false, the text result should be 4.0 compatible
  int init(const int64_t res_len, ObIAllocator *allocator = NULL);

  // copy existent loc to result
  int copy(const ObLobLocatorV2 *loc);

  // append (copy) result to buffer(file), change pos_
  int append(const char *buffer, int64_t len);
  OB_INLINE int append(const ObString &str)
  {
    return append(str.ptr(), str.length());
  }

  // overwrite exist result buffer(file), not change pos_
  int write(const char *buffer, int64_t pos, int64_t len);

  // overwrite exist result buffer(file), not change pos_
  int fill(int64_t pos, int c, int64_t len);

  // move pos_ to pos_ + offset
  int lseek(int64_t offset, int state);

  // expose buffer for user function, lseek should be called after write
  int get_reserved_buffer(char *&empty_start, int64_t &empty_len);

  bool is_init() { return is_init_; };

  OB_INLINE void get_result_buffer(ObString &buf_str) { buf_str.assign(buffer_, pos_); }
  OB_INLINE void set_has_lob_header(bool has_header) { has_lob_header_ = has_header; }
  OB_INLINE bool has_lob_header() { return (is_lob_storage(type_)) && has_lob_header_; }
  static int ob_convert_obj_temporay_lob(ObObj &obj, ObIAllocator &allocator);
  static int ob_convert_datum_temporay_lob(ObDatum &datum,
                                           const ObObjMeta &in_obj_meta,
                                           const ObObjMeta &out_obj_meta,
                                           ObIAllocator &allocator);

protected:
  int calc_buffer_len(const int64_t res_len);
  int fill_temp_lob_header(const int64_t res_len);

protected:
  const ObObjType type_;
  char *buffer_;
  int64_t buff_len_;
  int64_t pos_;
  bool is_outrow_templob_;
  bool has_lob_header_;
  bool is_init_;
  ObIAllocator *alloc_;
};

OB_INLINE bool ob_is_empty_lob(ObObjType type, const ObDatum &datum, bool has_lob_header)
{
  bool bret = false;
  if (common::ob_is_text_tc(type)) {
    common::ObLobLocatorV2 loc(datum.get_string(), has_lob_header);
    bret = loc.is_empty_lob();
  }
  return bret;
}

OB_INLINE bool ob_is_empty_lob(const ObObj &obj)
{
  bool bret = false;
  if (common::ob_is_text_tc(obj.get_type())) {
    common::ObLobLocatorV2 loc(obj.get_string(), obj.has_lob_header());
    bret = loc.is_empty_lob();
  }
  return bret;
}

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_LOB_ACCESS_UTILS_