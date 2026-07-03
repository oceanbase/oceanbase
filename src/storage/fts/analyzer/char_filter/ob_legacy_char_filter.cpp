/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/fts/analyzer/char_filter/ob_legacy_char_filter.h"

#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{

ObLegacyLowercaseCharFilter::ObLegacyLowercaseCharFilter()
  : is_inited_(false),
    coll_type_(common::CS_TYPE_INVALID),
    tolower_arena_(lib::ObMemAttr(MTL_ID(), "LegacyLcChrFlt"), OB_MALLOC_NORMAL_BLOCK_SIZE)
{}

int ObLegacyLowercaseCharFilter::init(const ObCharFilterSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lowercase char filter already initialized", K(ret));
  } else if (OB_UNLIKELY(ObCharFilterType::CHAR_FILTER_TYPE_LOWERCASE_LEGACY != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid char filter type for lowercase filter", K(ret), K(spec.type_));
  } else {
    const ObLegacyLowercaseCharFilterSpec &lc_spec =
        static_cast<const ObLegacyLowercaseCharFilterSpec &>(spec);
    coll_type_ = lc_spec.coll_type_;
    is_inited_ = true;
  }
  return ret;
}

int ObLegacyLowercaseCharFilter::filter(
    const char *input,
    const int64_t input_len,
    const char *&output,
    int64_t &output_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lowercase char filter not initialized", K(ret));
  } else if (OB_ISNULL(input) || OB_UNLIKELY(input_len <= 0)) {
    output = input;
    output_len = input_len;
  } else if (OB_UNLIKELY(common::CS_TYPE_INVALID == coll_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collation type not set before filter", K(ret));
  } else {
    tolower_arena_.reuse();
    common::ObString src(input_len, input);
    common::ObString dst;
    const ObCharsetInfo *cs = nullptr;
    if (OB_ISNULL(cs = ObCharset::get_charset(coll_type_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, charset info is nullptr", K(ret), K(coll_type_));
    } else if (OB_FAIL(common::ObCharset::tolower(cs, src, dst, tolower_arena_))) {
      LOG_WARN("fail to tolower text", K(ret), K(coll_type_));
    } else {
      output = dst.ptr();
      output_len = dst.length();
    }
  }
  return ret;
}

void ObLegacyLowercaseCharFilter::reset()
{
  is_inited_ = false;
  coll_type_ = common::CS_TYPE_INVALID;
  tolower_arena_.reset();
}

ObUtf8mb4BinCharFilter::ObUtf8mb4BinCharFilter()
  : is_inited_(false),
    src_collation_(common::CS_TYPE_INVALID),
    convert_arena_(lib::ObMemAttr(MTL_ID(), "Utf8mb4BinChFlt"), OB_MALLOC_NORMAL_BLOCK_SIZE)
{}

int ObUtf8mb4BinCharFilter::init(const ObCharFilterSpec &spec, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("utf8mb4 bin char filter already initialized", K(ret));
  } else if (OB_UNLIKELY(ObCharFilterType::CHAR_FILTER_TYPE_UTF8MB4_BIN != spec.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid char filter type for utf8mb4 bin filter", K(ret), K(spec.type_));
  } else if (OB_UNLIKELY(common::CS_TYPE_INVALID == static_cast<const ObUtf8mb4BinCharFilterSpec &>(spec).src_collation_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid source collation for utf8mb4 bin filter", K(ret), K(spec));
  } else {
    src_collation_ = static_cast<const ObUtf8mb4BinCharFilterSpec &>(spec).src_collation_;
    is_inited_ = true;
  }
  return ret;
}

int ObUtf8mb4BinCharFilter::filter(
    const char *input,
    const int64_t input_len,
    const char *&output,
    int64_t &output_len)
{
  int ret = OB_SUCCESS;
  output = input;
  output_len = input_len;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("utf8mb4 bin char filter not initialized", K(ret));
  } else if (OB_ISNULL(input) && input_len > 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input is null but input_len is positive", K(ret), K(input_len));
  } else if (OB_ISNULL(input) || OB_UNLIKELY(input_len <= 0)) {
    // Empty or zero-length input: pass through as-is.
  } else if (CHARSET_UTF8MB4 != common::ObCharset::charset_type_by_coll(src_collation_)) {
    convert_arena_.reuse();
    common::ObString src(input_len, input);
    common::ObString converted;
    if (OB_FAIL(common::ObCharset::charset_convert(
            convert_arena_, src, src_collation_, common::CS_TYPE_UTF8MB4_BIN, converted))) {
      LOG_WARN("fail to convert text charset to utf8mb4 for analyzer", K(ret), K_(src_collation));
    } else {
      output = converted.ptr();
      output_len = converted.length();
    }
  }
  return ret;
}

void ObUtf8mb4BinCharFilter::reset()
{
  is_inited_ = false;
  src_collation_ = common::CS_TYPE_INVALID;
  convert_arena_.reset();
}

} // namespace storage
} // namespace oceanbase
