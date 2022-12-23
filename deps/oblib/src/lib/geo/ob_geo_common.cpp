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
 * This file contains implementation support for the geometry common abstraction.
 */

#define USING_LOG_PREFIX LIB
#include "lib/geo/ob_geo_common.h"

namespace oceanbase
{
namespace common
{

// ObGeoWkbByteOrderUtil
template<>
double ObGeoWkbByteOrderUtil::read<double>(const char* data, ObGeoWkbByteOrder bo)
{
  double res = 0.0;
  if (bo == ObGeoWkbByteOrder::LittleEndian) {
    res = *reinterpret_cast<const double*>(data);
  } else {
    for(int i = 0; i < 8; i++) {
      reinterpret_cast<char *>(&res)[i] = data[7 - i];
    }
  }
  return res;
}

template<>
uint32_t ObGeoWkbByteOrderUtil::read<uint32_t>(const char* data, ObGeoWkbByteOrder bo)
{
  uint32_t res = 0;
  if (bo == ObGeoWkbByteOrder::LittleEndian) {
    res = *reinterpret_cast<const uint32_t*>(data);
  } else {
    for(int i = 0; i < 4; i++) {
      reinterpret_cast<char *>(&res)[i] = data[3 - i];
    }
  }
  return res;
}

template<>
void ObGeoWkbByteOrderUtil::write<double>(char* data, double val, ObGeoWkbByteOrder bo)
{
  if (bo == ObGeoWkbByteOrder::LittleEndian) {
      *reinterpret_cast<double*>(data) = val;
  } else {
    for(int i = 0; i < 8; i++) {
      data[i] = reinterpret_cast<char *>(&val)[7 - i];
    }
  }
}

template<>
void ObGeoWkbByteOrderUtil::write<uint32_t>(char* data, uint32_t val, ObGeoWkbByteOrder bo)
{
  if (bo == ObGeoWkbByteOrder::LittleEndian) {
    *reinterpret_cast<uint32_t*>(data) = val;
  } else {
    for(int i = 0; i < 4; i++) {
      data[i] = reinterpret_cast<char *>(&val)[3 - i];
    }
  }
}

int ObWkbBuffer::reserve(int bytes)
{
  return buf_.reserve(bytes);
}

int ObWkbBuffer::append(double val)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reserve(sizeof(double)))) {
    LOG_WARN("fail to reserve more buffer", K(ret));
  } else {
    ObGeoWkbByteOrderUtil::write<double>(buf_.ptr() + buf_.length(), val, bo_);
    buf_.set_length(buf_.length() + sizeof(double));
  }
  return ret;
}

int ObWkbBuffer::append(uint32_t val)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reserve(sizeof(uint32_t)))) {
    LOG_WARN("fail to reserve more buffer", K(ret));
  } else {
    ObGeoWkbByteOrderUtil::write<uint32_t>(buf_.ptr() + buf_.length(), val, bo_);
    buf_.set_length(buf_.length() + sizeof(uint32_t));
  }
  return ret;
}

int ObWkbBuffer::append(char val)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reserve(sizeof(char)))) {
    LOG_WARN("fail to reserve more buffer", K(ret));
  } else {
    MEMCPY(buf_.ptr()+buf_.length(), &val, 1);
    buf_.set_length(buf_.length() + sizeof(char));
  }
  return ret;
}

int ObWkbBuffer::append(const char *str)
{
  return buf_.append(str);
}

int ObWkbBuffer::append(const char *str, const uint64_t len)
{
  return buf_.append(str, len);
}

int ObWkbBuffer::append(const ObString &str)
{
  return buf_.append(str);
}

int ObWkbBuffer::write(uint64_t pos, uint32_t val)
{
  int ret = OB_SUCCESS;
  ObGeoWkbByteOrderUtil::write<uint32_t>(buf_.ptr() + pos, val, bo_);
  return ret;
}

const ObString ObWkbBuffer::string() const
{
  return buf_.string();
}

uint64_t ObWkbBuffer::length() const
{
  return buf_.length();
}

int ObWkbBuffer::append_zero(const uint64_t len)
{
  int ret = OB_SUCCESS;
  uint64_t pos = length();
  if (OB_FAIL(reserve(len))) {
    LOG_WARN("fail to reserve space before move back", K(ret));
  } else if (OB_FAIL(buf_.set_length(buf_.length() + len))) {
    LOG_WARN("fail to move tail ptr", K(ret), K(len));
  } else {
    MEMSET(buf_.ptr() + pos, '\0', len);
  }
  return ret;
}
const char *ObWkbBuffer::ptr()
{
  return buf_.ptr();
}

int ObWkbBuffer::set_length(const uint64_t len)
{
  return buf_.set_length(len);
}

} // namespace common
} // namespace oceanbase