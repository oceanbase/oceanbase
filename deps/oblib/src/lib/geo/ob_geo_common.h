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

#ifndef OCEANBASE_LIB_GEO_OB_GEO_COMMON_
#define OCEANBASE_LIB_GEO_OB_GEO_COMMON_
#include <stdint.h>
#include <string.h>
#include "lib/string/ob_string.h"
#include "lib/string/ob_string_buffer.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/geo/ob_srs_info.h"

namespace oceanbase
{
namespace common
{
typedef uint32_t ObGeoSrid;
typedef ObStringBuffer ObGeoStringBuffer;

static const uint8_t GEO_VESION_1 = 1;
static const uint8_t GEO_VER_MASK = 0x40;
#define IS_GEO_VERSION(ver) (((ver) & GEO_VER_MASK) != 0)
#define GET_GEO_VERSION(ver) ((ver) & 0x3f)
#define ENCODE_GEO_VERSION(ver) ((ver) | GEO_VER_MASK)
// coordinate reference system
enum class ObGeoCRS
{
  Cartesian = 1,
  Geographic = 2,
};

enum class ObGeoWkbByteOrder
{
  BigEndian = 0,
  LittleEndian = 1,
  INVALID = 2,
};

struct ObGeoWkbHeader
{
  uint32_t srid_;
  uint8_t ver_;
  ObGeoWkbByteOrder bo_;
  ObGeoType type_;
  TO_STRING_KV(K_(srid),
               K_(ver),
               K_(bo),
               K_(type));
};

enum class ObGeoRelationType
{
  T_INVALID = 0,
  T_COVERS = 1,
  T_COVEREDBY = 2,
  T_INTERSECTS = 3,
  T_DWITHIN = 4,
  T_DFULLYWITHIN = 5
};

// will define in other file, later
enum class ObDomainOpType
{
  T_INVALID = 0,
  T_JSON_MEMBER_OF = 1,
  T_JSON_CONTAINS = 2,
  T_JSON_OVERLAPS = 3,
  T_GEO_COVERS,
  T_GEO_INTERSECTS,
  T_GEO_DWITHIN,
  T_GEO_DFULLYWITHIN,
  T_GEO_COVEREDBY,
  T_GEO_RELATE,
  T_DOMAIN_OP_END,
};
// sort from inside to outside, the order cannot be changed
enum class ObPointLocation {
  INTERIOR = 0,
  BOUNDARY = 1,
  EXTERIOR = 2,
  INVALID = 3,
};

class ObGeoWkbByteOrderUtil
{
public:
  // byte order interface
  // NOTE: ensure data is readble/writable before use
  template<typename T>
  static T read(const char* data, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian);
  static double read_double(const char* data, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian);

  template<typename T>
  static void write(char* data, T val, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian);
};

class ObWkbBuffer
{
public:
  explicit ObWkbBuffer(common::ObIAllocator &allocator, ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian): buf_(&allocator), bo_(bo) {}

  int reserve(int bytes = 128);
  int append(double val);
  int append(uint32_t val);
  int append(char val);
  int append(const char *str);
  int append(const char *str, const uint64_t len);
  int append(const ObString &str);
  int write(uint64_t pos, uint32_t val);
  int read(uint64_t pos, uint32_t &val);
  const ObString string() const;
  uint64_t length() const;
  int append_zero(const uint64_t len);
  const char *ptr();
  int set_length(const uint64_t len);

private:
  ObGeoStringBuffer buf_;
  ObGeoWkbByteOrder bo_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObWkbBuffer);
};

// Exceptions for boost::geometry.
class ObGeoNotImplementedException : public std::exception
{
 public:
  // without srs
  ObGeoNotImplementedException(ObGeoCRS crs, ObGeoType geo_type, const char *info) :
    crs_type_(crs), geo_type_(geo_type), info_(info) {};
private:
  ObGeoCRS crs_type_; // exception with crs info
  ObGeoType geo_type_;
  const char *info_;         // not implemented detail type
};


// for st_buffer_strategy and st_buffer
// 不能放lib/geo/ob_geo_func_common.h，由于这个func_common包含了boost库，st_buff头文件不能包含boost，会有编译问题
struct ObGeoBufferStrategy
{
  // default value
  size_t point_circle_val_ = 32;
  size_t join_round_val_ = 32;
  size_t end_round_val_ = 32;

  double distance_val_ = 0.0;
  double join_miter_val_ = 5.0;

  // only one strategy is permitted for same type
  bool has_point_s_ = false;
  bool has_join_s_ = false;
  bool has_end_s_ = false;

  uint8_t state_num_ = 0;

  common::ObString proj4_self_;
  common::ObString proj4_proj_;
  common::ObString proj4_wgs84_;

  const common::ObSrsItem *srs_proj_ = NULL;
  const common::ObSrsItem *srs_wgs84_ = NULL;
};

struct ObGeoErrLogInfo
{
  double value_out_of_range_;
  union {
    double min_long_val_;
    double min_lat_val_;
  };
  union {
    double max_long_val_;
    double max_lat_val_;
  };
};























} // namespace common
} // namespace oceanbase

#endif