/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_DEF_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_DEF_H_

#include "lib/charset/ob_charset.h"

#include <cstdint>

namespace oceanbase
{
namespace storage
{
typedef int32_t ObFTTokenCode;
typedef int32_t ObFTWordBase;
typedef uint32_t ObFTWordStateIndex;

/**
 * @class ObFTSingleToken
 * @brief struct to store a single character of a charset;
 */
struct ObFTSingleToken
{
public:
  ObFTSingleToken() : token_(""), token_char_len_(0) { }
  ObFTSingleToken &operator=(const ObFTSingleToken &other) = default;
  int set_token(const char *token, int32_t token_len);
  ObString get_token() const { return ObString(token_char_len_, token_); }
  bool operator==(const ObFTSingleToken &other) const;

public:
  char token_[common::ObCharset::MAX_MB_LEN];
  uint8_t token_char_len_;
} __attribute__((packed));

class ObFTDictDesc
{
public:
  enum BuildMode {
    DDL_EXE = 0,
    REFRESH_ONLY,
    DML_OR_SELECT_EXE
  };
public:
  ObFTDictDesc(const ObCharsetType charset,
               const ObCollationType coll_type,
               const uint64_t table_id,
               const common::ObString &table_name,
               const bool need_casedown)
      : table_id_(table_id), table_name_(table_name), charset_(charset), coll_type_(coll_type),
        need_casedown_(need_casedown)
  {
  }
  ObFTDictDesc(const ObCharsetType charset,
               const ObCollationType coll_type,
               const uint64_t table_id,
               const common::ObString &table_name)
      : table_id_(table_id), table_name_(table_name), charset_(charset), coll_type_(coll_type),
        need_casedown_(is_ci_collation(coll_type))
  {
  }

  static bool is_ci_collation(const ObCollationType coll_type)
  {
    const ObCharsetInfo *cs = common::ObCharset::get_charset(coll_type);
    return (cs != nullptr) && (cs->state & OB_CS_CI);
  }

  TO_STRING_KV(K_(charset), K_(coll_type), K_(table_id), K_(table_name), K_(need_casedown));

public:
  uint64_t table_id_;
  common::ObString table_name_;
  ObCharsetType charset_;
  ObCollationType coll_type_;
  bool need_casedown_;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_DEF_H_
