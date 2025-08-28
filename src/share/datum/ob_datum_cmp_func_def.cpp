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

#define USING_LOG_PREFIX SHARE

#include "ob_datum_cmp_func_def.h"
#include "share/ob_lob_access_utils.h"
#include "share/rc/ob_tenant_base.h"


using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::datum_cmp;


int ObDatumJsonCmpImpl::cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret, const bool is_lob)
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  ObString l_data;
  ObString r_data;
  common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTextStringIter l_instr_iter(ObJsonType, CS_TYPE_BINARY, l.get_string(), is_lob);
  ObTextStringIter r_instr_iter(ObJsonType, CS_TYPE_BINARY, r.get_string(), is_lob);
  if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
    COMMON_LOG(WARN, "Lob: init left lob str iter failed", K(ret), K(l));
  } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
    COMMON_LOG(WARN, "Lob: get left lob str iter full data failed ", K(ret), K(l_instr_iter));
  } else if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
    COMMON_LOG(WARN, "Lob: init right lob str iter failed", K(ret), K(ret), K(r));
  } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
    COMMON_LOG(WARN, "Lob: get right lob str iter full data failed ", K(ret), K(r_instr_iter));
  } else if (r_data.empty() || l_data.empty()) {
    if (l_data.empty() && r_data.empty()) {
      cmp_ret = 0;
    } else if (l_data.empty()) {
      cmp_ret = -1;
    } else {
      cmp_ret = 1;
    }
  } else {
    ObJsonBin j_bin_l(l_data.ptr(), l_data.length(), &allocator);
    ObJsonBin j_bin_r(r_data.ptr(), r_data.length(), &allocator);
    ObIJsonBase *j_base_l = &j_bin_l;
    ObIJsonBase *j_base_r = &j_bin_r;

    if (OB_FAIL(j_bin_l.reset_iter())) {
      COMMON_LOG(WARN, "fail to reset left json bin iter", K(ret), K(l.len_));
    } else if (OB_FAIL(j_bin_r.reset_iter())) {
      COMMON_LOG(WARN, "fail to reset right json bin iter", K(ret), K(r.len_));
    } else if (OB_FAIL(j_base_l->compare(*j_base_r, cmp_ret))) {
      COMMON_LOG(WARN, "fail to compare json", K(ret), K(*j_base_l), K(*j_base_r));
    }
  }
  return ret;
}

int ObDatumGeoCmpImpl::cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret, const bool is_lob)
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  ObString l_data;
  ObString r_data;
  common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTextStringIter l_instr_iter(ObGeometryType, CS_TYPE_BINARY, l.get_string(), is_lob);
  ObTextStringIter r_instr_iter(ObGeometryType, CS_TYPE_BINARY, r.get_string(), is_lob);
  if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
    COMMON_LOG(WARN, "Lob: init left lob str iter failed", K(ret), K(l));
  } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
    COMMON_LOG(WARN, "Lob: get left lob str iter full data failed ", K(ret), K(l_instr_iter));
  } else if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
    COMMON_LOG(WARN, "Lob: init right lob str iter failed", K(ret), K(ret), K(r));
  } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
    COMMON_LOG(WARN, "Lob: get right lob str iter full data failed ", K(ret), K(r_instr_iter));
  } else {
    cmp_ret = ObCharset::strcmpsp(CS_TYPE_BINARY, l_data.ptr(), l_data.length(), r_data.ptr(), r_data.length(), false);
  }
  cmp_ret = cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0);
  return ret;
}

int ObDatumCollectionCmpImpl::cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret, const bool is_lob)
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  ObString l_data;
  ObString r_data;
  common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTextStringIter l_instr_iter(ObGeometryType, CS_TYPE_BINARY, l.get_string(), is_lob);
  ObTextStringIter r_instr_iter(ObGeometryType, CS_TYPE_BINARY, r.get_string(), is_lob);
  if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
    COMMON_LOG(WARN, "Lob: init left lob str iter failed", K(ret), K(l));
  } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
    COMMON_LOG(WARN, "Lob: get left lob str iter full data failed ", K(ret), K(l_instr_iter));
  } else if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
    COMMON_LOG(WARN, "Lob: init right lob str iter failed", K(ret), K(ret), K(r));
  } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
    COMMON_LOG(WARN, "Lob: get right lob str iter full data failed ", K(ret), K(r_instr_iter));
  } else {
    // only memcmp supported now
    cmp_ret = MEMCMP(l_data.ptr(), r_data.ptr(), std::min(l_data.length(), r_data.length()));
    if (cmp_ret == 0 && l_data.length() != r_data.length()) {
      cmp_ret = l_data.length() > r_data.length() ? 1 : -1;
    }
  }
  return ret;
}

int ObDatumRoaringbitmapCmpImpl::cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret, const bool is_lob)
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  ObString l_data;
  ObString r_data;
  common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTextStringIter l_instr_iter(ObRoaringBitmapType, CS_TYPE_BINARY, l.get_string(), is_lob);
  ObTextStringIter r_instr_iter(ObRoaringBitmapType, CS_TYPE_BINARY, r.get_string(), is_lob);
  if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
    COMMON_LOG(WARN, "Lob: init left lob str iter failed", K(ret), K(l));
  } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
    COMMON_LOG(WARN, "Lob: get left lob str iter full data failed ", K(ret), K(l_instr_iter));
  } else if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
    COMMON_LOG(WARN, "Lob: init right lob str iter failed", K(ret), K(ret), K(r));
  } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
    COMMON_LOG(WARN, "Lob: get right lob str iter full data failed ", K(ret), K(r_instr_iter));
  } else {
    // only memcmp supported now
    cmp_ret = MEMCMP(l_data.ptr(), r_data.ptr(), std::min(l_data.length(), r_data.length()));
    if (cmp_ret == 0 && l_data.length() != r_data.length()) {
      cmp_ret = l_data.length() > r_data.length() ? 1 : -1;
    }
  }
  return ret;
}

int ObDatumTextCmpImpl::cmp_out_row(const ObDatum &l, const ObDatum &r, int &cmp_ret,
                                    const ObCollationType cs, const bool with_end_space)
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  ObString l_data;
  ObString r_data;
  const ObLobCommon& rlob = r.get_lob_data();
  const ObLobCommon& llob = l.get_lob_data();
  common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTextStringIter l_instr_iter(ObLongTextType, cs, l.get_string(), true); // longtext only indicates its a lob type
  ObTextStringIter r_instr_iter(ObLongTextType, cs, r.get_string(), true);
  if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
    COMMON_LOG(WARN, "Lob: init left lob str iter failed", K(ret), K(cs), K(l));
  } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
    COMMON_LOG(WARN, "Lob: get left lob str iter full data failed ", K(ret), K(cs), K(l_instr_iter));
  } else if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
    COMMON_LOG(WARN, "Lob: init right lob str iter failed", K(ret), K(ret), K(r));
  } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
    COMMON_LOG(WARN, "Lob: get right lob str iter full data failed ", K(ret), K(cs), K(r_instr_iter));
  } else {
    cmp_ret = ObCharset::strcmpsp(
        cs, l_data.ptr(), l_data.length(), r_data.ptr(), r_data.length(), with_end_space);
  }
  // if error occur when reading outrow lobs, the compare result is wrong.
  cmp_ret = cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0);
  return ret;
}


int ObDatumTextStringCmpImpl::cmp_out_row(const ObDatum &l, const ObDatum &r, int &cmp_ret,
                                          const ObCollationType cs, const bool with_end_space)
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  ObString l_data;
  common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTextStringIter l_instr_iter(ObLongTextType, cs, l.get_string(), true); // longtext only indicates its a lob type
  if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
    COMMON_LOG(WARN, "Lob: init left lob str iter failed", K(ret), K(cs), K(l));
  } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
    COMMON_LOG(WARN, "Lob: get left lob str iter full data failed ", K(ret), K(cs), K(l_instr_iter));
  } else {
    cmp_ret = ObCharset::strcmpsp(
        cs, l_data.ptr(), l_data.length(), r.ptr_, r.len_, with_end_space);
  }
  // if error occur when reading outrow lobs, the compare result is wrong.
  cmp_ret = cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0);
  return ret;
}

int ObDatumStringTextCmpImpl::cmp_out_row(const ObDatum &l, const ObDatum &r, int &cmp_ret,
                                  const ObCollationType cs, const bool with_end_space)
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  const ObLobCommon& rlob = r.get_lob_data();
  ObString r_data;
  common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTextStringIter r_instr_iter(ObLongTextType, cs, r.get_string(), true);  // longtext only indicates its a lob type
  if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
    COMMON_LOG(WARN, "Lob: init right lob str iter failed", K(ret), K(ret), K(r));
  } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
    COMMON_LOG(WARN, "Lob: get right lob str iter full data failed ", K(ret), K(cs), K(r_instr_iter));
  } else {
    cmp_ret = ObCharset::strcmpsp(
        cs, l.ptr_, l.len_, r_data.ptr(), r_data.length(), with_end_space);
  }
  // if error occur when reading outrow lobs, the compare result is wrong.
  cmp_ret = cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0);
  return ret;
}

