/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_OB_STRINGS_H_
#define OCEANBASE_LIB_OB_STRINGS_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "common/ob_string_buf.h"

namespace oceanbase
{
namespace common
{
/**
 * an array of strings
 *
 */
class ObStrings
{
public:
  ObStrings();
  virtual ~ObStrings();
  int add_string(const ObString &str, int64_t *idx = NULL);
  int get_string(int64_t idx, ObString &str) const;
  int64_t count() const;
  void reuse();

  int64_t to_string(char *buf, const int64_t buf_len) const;
  NEED_SERIALIZE_AND_DESERIALIZE;

private:
  ObStringBuf buf_;
  ObArray<ObString> strs_;

  DISALLOW_COPY_AND_ASSIGN(ObStrings);
};
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_LIB_OB_STRINGS_H_
