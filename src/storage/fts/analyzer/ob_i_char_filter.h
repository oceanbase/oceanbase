/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_I_CHAR_FILTER_H_
#define OCEANBASE_STORAGE_OB_I_CHAR_FILTER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{

// ============================================================
// CharFilter configuration spec structures
// ============================================================

enum class ObCharFilterType
{
  CHAR_FILTER_TYPE_INVALID = 0,
  CHAR_FILTER_TYPE_HTML_STRIP,         // strip HTML tags
  CHAR_FILTER_TYPE_LOWERCASE_LEGACY,   // convert text to lowercase
  CHAR_FILTER_TYPE_UTF8MB4_BIN,        // convert input text to utf8mb4_bin
  CHAR_FILTER_TYPE_MAX
};

struct ObCharFilterSpec
{
  ObCharFilterType type_;
  ObCharFilterSpec() : type_(ObCharFilterType::CHAR_FILTER_TYPE_INVALID) {}
  virtual ~ObCharFilterSpec() = default;
  VIRTUAL_TO_STRING_KV(K_(type));
};

// ============================================================
// ObICharFilter interface
// ============================================================

// CharFilter performs character-level transformations on the raw text (e.g. HTML tag stripping,
// character mapping). Multiple CharFilters are chained sequentially: the output of one becomes
// the input of the next.
class ObICharFilter
{
public:
  ObICharFilter() = default;
  virtual ~ObICharFilter() = default;
  virtual int init(const ObCharFilterSpec &spec, common::ObIAllocator &alloc) = 0;
  virtual int filter(const char *input, const int64_t input_len,
                     const char *&output, int64_t &output_len) = 0;
  // Reset the object and free its memory. The object must not be used after this call.
  virtual void reset() = 0;
  VIRTUAL_TO_STRING_KV("ObICharFilter", "");
private:
  DISALLOW_COPY_AND_ASSIGN(ObICharFilter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_I_CHAR_FILTER_H_
