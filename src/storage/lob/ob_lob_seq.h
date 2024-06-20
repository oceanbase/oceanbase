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

#ifndef OCEABASE_STORAGE_OB_LOB_SEQ_
#define OCEABASE_STORAGE_OB_LOB_SEQ_

#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_ctype.h"

namespace oceanbase
{
namespace storage
{

typedef struct ObLobSeqIdDesc {
  ObString seq_id_;
  // len :the array element size
  uint32_t len_;
} ObLobSeqIdDesc;

class ObLobSeqId {
public:
  ObLobSeqId(const ObString& seq_id, ObIAllocator* allocator);
  ObLobSeqId(ObIAllocator* allocator);
  ObLobSeqId();
  ~ObLobSeqId();
  void set_seq_id(ObString& seq_id);
  void set_allocator(ObIAllocator* allocator);
  int get_next_seq_id(ObString& seq_id);
  int get_next_seq_id(ObString& seq_id, ObLobSeqId &end);
  void get_seq_id(ObString& seq_id);
  int compare(ObLobSeqId &other);
  void reset();
  static char* store32be(char *ptr, uint32_t val);
  static uint32_t load32be(const char *ptr);
  static int get_seq_id(int64_t idx, ObString &seq_id);
  int64_t to_string(char* buf, const int64_t buf_len) const;
private:
  int init_digits();
  int init_seq_buf();
  void reset_digits();
  void reset_seq_buf();
  int parse();
  int add_digits(uint32_t val);
  int append_seq_buf(uint32_t val);
  int replace_seq_buf_last_node(uint32_t val);
  int extend_digits();
  int extend_seq_buf();
  int empty();
private:

  ObIAllocator* allocator_;
  ObString seq_id_;

  bool read_only_;
  bool parsed_;

  uint32_t last_seq_buf_pos_;
  char* buf_;
  uint32_t len_;
  uint32_t cap_;

  uint32_t *digits_;
  uint32_t dig_len_;
  uint32_t dig_cap_;

private:
  static const uint32_t SEQ_PER_UNIT_BUF_SIZE = 16;

public:
  static const uint32_t LOB_SEQ_DIGIT_DEFAULT_LEN = 1024;
  static const uint32_t LOB_SEQ_DEFAULT_SEQ_BUF_LEN = 1024 * 8;
  static const uint32_t LOB_SEQ_ID_MIN = 256;
  static const uint32_t LOB_SEQ_STEP_LEN = 256;
  static const uint32_t LOB_SEQ_STEP_MAX = UINT32_MAX;//65535;
};

} // storage
} // oceanbase

#endif