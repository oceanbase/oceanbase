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

#ifndef OCEANBASE_LIB_GEO_OB_SRS_INFO_
#define OCEANBASE_LIB_GEO_OB_SRS_INFO_

#include <math.h>
#include "lib/container/ob_array.h"
#include "lib/container/ob_vector.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/ob_errno.h"
#include "common/data_buffer.h"

namespace oceanbase
{

namespace  common
{

const static uint32_t OB_GEO_DEFAULT_GEOGRAPHY_SRID = 4326;
const static double OB_GEO_BOUNDS_DELTA = 0.01;

#define WGS84_PARA_NUM 7
#define AXIS_DIRECTION_NUM 2
#define SRID_WORLD_MERCATOR_PG 999000
#define SRID_NORTH_UTM_START_PG 999001
#define SRID_NORTH_UTM_END_PG 999060
#define SRID_NORTH_LAMBERT_PG 999061
#define SRID_NORTH_STEREO_PG 999062
#define SRID_SOUTH_UTM_START_PG 999101
#define SRID_SOUTH_UTM_END_PG 999160
#define SRID_SOUTH_LAMBERT_PG 999161
#define SRID_LAEA_START_PG 999163
#define SRID_LAEA_END_PG 999283
// '+proj=stere +lat_0=90 +lat_ts=71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs'
#define NORTH_STEREO_WKT R"(PROJCS["unknown",GEOGCS["unknown", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Polar Stereopraphic",AUTHORITY["EPSG","9829"]],PARAMETER["Latitude of standard parallel",71,AUTHORITY["EPSG","8832"]],PARAMETER["Longitude of origin",0,AUTHORITY["EPSG","8833"]],PARAMETER["False easting",0,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",SOUTH],AXIS["N",SOUTH],AUTHORITY["EPSG","9122"]])"
// +proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs
#define WORLD_MERCATOR_WKT R"(PROJCS["unknown",GEOGCS["unknown", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Mercator",AUTHORITY["EPSG","9804"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",0,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",1,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",0,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","9122"]])"
  // +proj=laea +lat_0=-90 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs
#define SOUTH_LAMBERT_WKT R"(PROJCS["unknown",GEOGCS["unknown", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Lambert Azimuthal Equal Area",AUTHORITY["EPSG","9820"]],PARAMETER["Latitude of natural origin",-90,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",0,AUTHORITY["EPSG","8802"]],PARAMETER["False easting",0,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",NORTH],AXIS["N",NORTH],AUTHORITY["EPSG","9122"]])"
// +proj=laea +lat_0=90 +lon_0=-40 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs
#define NORTH_LAMBERT_WKT R"(PROJCS["unknown",GEOGCS["unknown", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Lambert Azimuthal Equal Area",AUTHORITY["EPSG","9820"]],PARAMETER["Latitude of natural origin",90,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",-40,AUTHORITY["EPSG","8802"]],PARAMETER["False easting",0,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",NORTH],AXIS["N",SOUTH],AUTHORITY["EPSG","9122"]])"
#define SOUTH_UTM_WKT R"(PROJCS["unknown",GEOGCS["unknown", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",%d,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.9996,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",500000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",10000000,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","9122"]])"
#define NORTH_UTM_WKT R"(PROJCS["unknown",GEOGCS["unknown", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",%d,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.9996,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",500000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","9122"]])"
#define LAEA_WKT R"(PROJCS["unknown",GEOGCS["unknown", DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Lambert Azimuthal Equal Area",AUTHORITY["EPSG","9820"]],PARAMETER["Latitude of natural origin",%g,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",%g,AUTHORITY["EPSG","8802"]],PARAMETER["False easting",0,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",0,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",NORTH],AXIS["N",NORTH],AUTHORITY["EPSG","9122"]])"


enum class ObSrsType
{
  PROJECTED_SRS = 0,
  GEOGRAPHIC_SRS,
  UNKNOW_SRS
};

enum class ObAxisDirection
{
  INIT = 0,
  EAST,
  SOUTH,
  WEST,
  NORTH,
  OTHER
};

enum class ObProjectionType
{
  UNKNOWN = 0,
  POPULAR_VISUAL_PSEUDO_MERCATOR = 1024,
  LAMBERT_AZIMUTHAL_EQUAL_AREA_SPHERICAL = 1027,
  EQUIDISTANT_CYLINDRICAL = 1028,
  EQUIDISTANT_CYLINDRICAL_SPHERICAL = 1029,
  KROVAK_NORTH_ORIENTATED = 1041,
  KROVAK_MODIFIED = 1042,
  KROVAK_MODIFIED_NORTH_ORIENTATED = 1043,
  LAMBERT_CONIC_CONFORMAL_2SP_MICHIGAN = 1051,
  COLOMBIA_URBAN = 1052,
  LAMBERT_CONIC_CONFORMAL_1SP = 9801,
  LAMBERT_CONIC_CONFORMAL_2SP = 9802,
  LAMBERT_CONIC_CONFORMAL_2SP_BELGIUM = 9803,
  MERCATOR_VARIANT_A = 9804,
  MERCATOR_VARIANT_B = 9805,
  CASSINI_SOLDNER = 9806,
  TRANSVERSE_MERCATOR = 9807,
  TRANSVERSE_MERCATOR_SOUTH_ORIENTATED = 9808,
  OBLIQUE_STEREOGRAPHIC = 9809,
  POLAR_STEREOGRAPHIC_VARIANT_A = 9810,
  NEW_ZEALAND_MAP_GRID = 9811,
  HOTINE_OBLIQUE_MERCATOR_VARIANT_A = 9812,
  LABORDE_OBLIQUE_MERCATOR = 9813,
  HOTINE_OBLIQUE_MERCATOR_VARIANT_B = 9815,
  TUNISIA_MINING_GRID = 9816,
  LAMBERT_CONIC_NEAR_CONFORMAL = 9817,
  AMERICAN_POLYCONIC = 9818,
  KROVAK = 9819,
  LAMBERT_AZIMUTHAL_EQUAL_AREA = 9820,
  ALBERS_EQUAL_AREA = 9822,
  TRANSVERSE_MERCATOR_ZONED_GRID_SYSTEM = 9824,
  LAMBERT_CONIC_CONFORMAL_WEST_ORIENTATED = 9826,
  BONNE_SOUTH_ORIENTATED = 9828,
  POLAR_STEREOGRAPHIC_VARIANT_B = 9829,
  POLAR_STEREOGRAPHIC_VARIANT_C = 9830,
  GUAM_PROJECTION = 9831,
  MODIFIED_AZIMUTHAL_EQUIDISTANT = 9832,
  HYPERBOLIC_CASSINI_SOLDNER = 9833,
  LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL = 9834,
  LAMBERT_CYLINDRICAL_EQUAL_AREA = 9835,
};

struct ObRsAuthority
{
  bool is_valid;
  common::ObString org_name;
  common::ObString org_code;
};

struct ObSpheroid
{
  common::ObString name;
  double semi_major_axis;
  double inverse_flattening;
  ObRsAuthority authority;

  ObSpheroid() : semi_major_axis(NAN), inverse_flattening(NAN) {}
};

struct ObTowgs84
{
  bool is_valid;
  double value[WGS84_PARA_NUM];

  ObTowgs84() { is_valid = false; for (double &v : value) { v = 0.0; }}
};

/* raw data from srs wkt datum, generate from srs parser */
struct ObRsDatum
{
  common::ObString name;
  ObSpheroid spheroid;
  ObTowgs84 towgs84;
  ObRsAuthority authority;
};

struct ObPrimem
{
  common::ObString name;
  double longtitude;
  ObRsAuthority authority;

  ObPrimem() : longtitude(NAN) {}
};

struct ObRsUnit
{
  common::ObString type;
  double conversion_factor;
  ObRsAuthority authority;

  ObRsUnit() : conversion_factor(NAN) {}
};

struct ObRsAxis
{
  common::ObString name;
  ObAxisDirection direction;
};

struct ObRsAxisPair
{
  ObRsAxis x;
  ObRsAxis y;
};

// Geographical SRS definition
struct ObGeographicRs
{
  common::ObString rs_name;
  ObRsDatum datum_info;
  ObPrimem primem;
  ObRsUnit unit;
  ObRsAxisPair axis;
  ObRsAuthority authority;
};

// projection method
struct ObProjection
{
  common::ObString name;
  ObRsAuthority authority;
};

struct ObProjectionPram
{
  common::ObString name;
  double value;
  ObRsAuthority authority;

  ObProjectionPram() : value(0.0) {}
};

struct ObProjectionPrams
{
  common::ObVector<ObProjectionPram> vals;
};

// Projected SRS definition
struct ObProjectionRs
{
  common::ObString rs_name;
  ObGeographicRs projected_rs;
  ObProjection projection;
  ObProjectionPrams proj_params;
  ObRsUnit unit;
  ObRsAxisPair axis;
  ObRsAuthority authority;
};

struct ObSrsBoundsItem {
  ObSrsBoundsItem()
    : minX_(NAN),
      maxX_(NAN),
      minY_(NAN),
      maxY_(NAN) {}

  int64_t to_string(char *buf, const int64_t buf_len) const;
  double minX_;
  double maxX_;
  double minY_;
  double maxY_;
};

class ObSpatialReferenceSystemBase
{
public:
  ObSpatialReferenceSystemBase(){}
  virtual ~ObSpatialReferenceSystemBase(){}
  virtual ObSrsType srs_type() const = 0;
  virtual double prime_meridian() const = 0;
  virtual double linear_unit() const = 0;
  virtual double angular_unit() const = 0;
  virtual double semi_major_axis() const = 0;
  virtual double inverse_flattening() const = 0;
  virtual bool is_wgs84() const = 0;
  virtual bool has_wgs84_value() const = 0;
  virtual ObAxisDirection axis_direction(uint8_t axis_index) const = 0;
  virtual int get_proj4_param(ObIAllocator *allocator, ObString &proj4_param) const = 0;
  virtual uint32_t get_srid() const = 0;
  virtual void set_bounds(double min_x, double min_y, double max_x, double max_y) = 0;
  virtual int set_proj4text(ObIAllocator &allocator, const ObString &src_proj4) = 0;
  virtual void set_proj4text(ObString &src_proj4) = 0;
  virtual const ObSrsBoundsItem* get_bounds() const = 0;
  virtual ObString get_proj4text() = 0;
  bool is_positive_east() const { return true; }
  bool is_positive_north() const { return false; }
  bool is_lat_first() const { return false; }

  static int create_project_srs(common::ObIAllocator* allocator, uint64_t srs_id, const ObProjectionRs *rs, ObSpatialReferenceSystemBase *&srs_info);
  static int create_geographic_srs(common::ObIAllocator* allocator, uint64_t srs_id, const ObGeographicRs *rs, ObSpatialReferenceSystemBase *&srs_info);
private:
  template <typename SRS_T, typename RS_T = ObProjectionRs>
  static int create_srs_internal(ObIAllocator* allocator, uint64_t srs_id,
                                 const RS_T *rs, ObSpatialReferenceSystemBase *&srs_info);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSpatialReferenceSystemBase);
};

class ObGeographicSrs : public ObSpatialReferenceSystemBase
{
public:
  ObGeographicSrs(common::ObIAllocator* alloc);
  virtual ~ObGeographicSrs() {}
  ObSrsType srs_type() const override { return ObSrsType::GEOGRAPHIC_SRS; }
  virtual int init(uint32_t srid, const ObGeographicRs *rs);
  double prime_meridian() const { return prime_meridian_; };
  double linear_unit() const { return 1.0; };
  double angular_unit() const { return angular_factor_; };
  double semi_major_axis() const { return semi_major_axis_; }
  double inverse_flattening() const { return inverse_flattening_; }
  bool is_wgs84() const { return is_wgs84_; }
  bool has_wgs84_value() const { return !(std::isnan(wgs84_[0])); }
  ObAxisDirection axis_direction(uint8_t axis_index) const { return axis_dir_[axis_index]; }
  int get_proj4_param(ObIAllocator *allocator, ObString &proj4_param) const;
  uint32_t get_srid() const override { return id_; }
  void set_bounds(double min_x, double min_y, double max_x, double max_y);
  int set_proj4text(ObIAllocator &allocator, const ObString &src_proj4) { return deep_copy_ob_string(allocator, src_proj4, proj4text_); }
  void set_proj4text(ObString &src_proj4) { proj4text_ = src_proj4; }
  const ObSrsBoundsItem* get_bounds() const { return &bounds_info_; }
  ObString get_proj4text() { return proj4text_; }

private:
  uint32_t id_;
  double semi_major_axis_;
  double inverse_flattening_;
  bool is_wgs84_;
  double prime_meridian_;
  double angular_factor_;
  ObSrsBoundsItem bounds_info_;
  ObString proj4text_;
  double wgs84_[WGS84_PARA_NUM];
  // direction of x and y axis;
  ObAxisDirection axis_dir_[AXIS_DIRECTION_NUM];
};

struct ObSimpleProjPram
{
  int epsg_code_;
  double value_;
  ObSimpleProjPram(int epsg_code): epsg_code_(epsg_code), value_(NAN) {}
};

class ObProjectedSrs : public ObSpatialReferenceSystemBase
{
public:
  ObProjectedSrs(common::ObIAllocator *allocator) : geographic_srs_(allocator),
                                                    linear_unit_(NAN),
                                                    simple_proj_prams_(static_cast<common::ObFIFOAllocator*>(allocator))
  {
    for (int i = 0; i < AXIS_DIRECTION_NUM; i++) {
      axis_dir_[i] = ObAxisDirection::INIT;
    }
  };
  virtual ~ObProjectedSrs() {}
  ObSrsType srs_type() const override {return ObSrsType::PROJECTED_SRS;}
  int init(uint64_t srs_id,  const ObProjectionRs *rs);
  double prime_meridian() const { return geographic_srs_.prime_meridian(); };
  double linear_unit() const { return linear_unit_; };
  double angular_unit() const { return geographic_srs_.angular_unit(); };
  double semi_major_axis() const { return 0.0; }
  double inverse_flattening() const { return 0.0; }
  int get_proj4_param(ObIAllocator *allocator, ObString &proj4_param) const;
  bool is_wgs84() const { return geographic_srs_.is_wgs84(); }
  bool has_wgs84_value() const { return geographic_srs_.has_wgs84_value(); }
  ObAxisDirection axis_direction(uint8_t axis_index) const { return axis_dir_[axis_index]; }

  virtual ObProjectionType get_projection_type() const = 0;
  virtual void register_proj_params() = 0;
  uint32_t get_srid() const override { return id_; }
  void set_bounds(double min_x, double min_y, double max_x, double max_y) { geographic_srs_.set_bounds(min_x, min_y, max_x, max_y); }
  int set_proj4text(ObIAllocator &allocator, const ObString &src_proj4) { return geographic_srs_.set_proj4text(allocator, src_proj4); }
  void set_proj4text(ObString &src_proj4) { geographic_srs_.set_proj4text(src_proj4); }
  const ObSrsBoundsItem* get_bounds() const { return geographic_srs_.get_bounds(); }
  ObString get_proj4text() { return geographic_srs_.get_proj4text(); }

private:
  uint32_t id_;
  ObGeographicSrs geographic_srs_;
  double linear_unit_;
  ObAxisDirection axis_dir_[AXIS_DIRECTION_NUM]; // direction of x and y axis;

protected:
  ObVector<ObSimpleProjPram, common::ObFIFOAllocator> simple_proj_prams_; // should be filled by subclass
};

#define OB_GEO_REG_PROJ_PARAMS(...) \
const static int arr[] = __VA_ARGS__; \
for (int32_t i = 0; i < ARRAYSIZEOF(arr); ++i) { \
  simple_proj_prams_.push_back(arr[i]); \
}


// Unknown Projection Method
// EPSG CODE: 0
// Projection Parameters EPSG CODE: None
// any other projection method can be represented by unknown projection method
class ObUnknownProjectedSrs : public ObProjectedSrs
{
public:
  ObUnknownProjectedSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObUnknownProjectedSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::UNKNOWN; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( {} ); } // supported projection parameters
};

// Popular Visualisation Pseudo Mercator Projection Method
// EPSG CODE: 1024
// Projection Parameters EPSG CODE: 8801, 8802, 8806, 8807
class ObPopularVisualPseudoMercatorSrs : public ObProjectedSrs
{
public:
  ObPopularVisualPseudoMercatorSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObPopularVisualPseudoMercatorSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::POPULAR_VISUAL_PSEUDO_MERCATOR; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( {8801, 8802, 8806, 8807} ); } // supported projection parameters
};

// Lambert Azimuthal Equal Area (Spherical) Projection Method
// EPSG CODE: 1027
// Projection Parameters EPSG CODE: 8801, 8802, 8806, 8807
class ObLambertAzimuthalEqualAreaSphericalSrs: public ObProjectedSrs
{
public:
  ObLambertAzimuthalEqualAreaSphericalSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObLambertAzimuthalEqualAreaSphericalSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::LAMBERT_AZIMUTHAL_EQUAL_AREA_SPHERICAL; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( {8801, 8802, 8806, 8807} ); } // supported projection parameters
};

// Equidistant Cylindrical Projection Method
// EPSG CODE: 1028
// Projection Parameters EPSG CODE: 8823, 8802, 8806, 8807
class ObEquidistantCylindricalSrs : public ObProjectedSrs
{
public:
  ObEquidistantCylindricalSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObEquidistantCylindricalSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::EQUIDISTANT_CYLINDRICAL; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( {8823, 8802, 8806, 8807} ); } // supported projection parameters
};

// Equidistant Cylindrical (Spherical) Projection Method
// EPSG CODE: 1029
// Projection Parameters EPSG CODE: 8823, 8802, 8806, 8807
class ObEquidistantCylindricalSphericalSrs : public ObProjectedSrs
{
public:
  ObEquidistantCylindricalSphericalSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObEquidistantCylindricalSphericalSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::EQUIDISTANT_CYLINDRICAL_SPHERICAL; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( {8823, 8802, 8806, 8807} ); } // supported projection parameters
};

// Krovak (North Orientated) Projection Method
// EPSG CODE: 1041
// Projection Parameters EPSG CODE: 8801, 8802, 8805, 8806, 8807
class ObKrovakNorthOrientatedSrs : public ObProjectedSrs
{
public:
  ObKrovakNorthOrientatedSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObKrovakNorthOrientatedSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::KROVAK_NORTH_ORIENTATED; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( {8811, 8833, 1036, 8818, 8819, 8806, 8807} ); } // supported projection parameters
};

// Krovak Modified Projection Method
// EPSG CODE: 1042
// Projection Parameters EPSG CODE: 8811, 8833, 1036, 8818, 8819, 8806, 8807, 8617, 8618, 1026, 1027, 1028, 1029, 1030, 1031, 1032, 1033, 1034, 1035
class ObKrovakModifiedSrs : public ObProjectedSrs
{
public:
  ObKrovakModifiedSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObKrovakModifiedSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::KROVAK_MODIFIED; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( {8811, 8833, 1036, 8818, 8819, 8806, 8807, 8617, 8618, 1026, 1027, 1028, 1029, 1030, 1031, 1032, 1033, 1034, 1035} ); } // supported projection parameters
};

// Krovak Modified (North Orientated) Projection Method
// EPSG CODE: 1043
// Projection Parameters EPSG CODE: 8811, 8833, 1036, 8818, 8819, 8806, 8807, 8617, 8618, 1026, 1027, 1028, 1029, 1030, 1031, 1032, 1033, 1034, 1035
class ObKrovakModifiedNorthOrientatedSrs : public ObProjectedSrs
{
public:
  ObKrovakModifiedNorthOrientatedSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObKrovakModifiedNorthOrientatedSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::KROVAK_MODIFIED_NORTH_ORIENTATED; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( {8811, 8833, 1036, 8818, 8819, 8806, 8807, 8617, 8618, 1026, 1027, 1028, 1029, 1030, 1031, 1032, 1033, 1034, 1035} ); } // supported projection parameters
};

// Lambert Conic Conformal (2SP Michigan) Projection Method
// EPSG CODE: 1051
// Projection Parameters EPSG CODE: 8821, 8822, 8823, 8824, 8826, 8827, 1038
class ObLambertConicConformal2SPMichiganSrs : public ObProjectedSrs
{
public:
  ObLambertConicConformal2SPMichiganSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObLambertConicConformal2SPMichiganSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::LAMBERT_CONIC_CONFORMAL_2SP_MICHIGAN; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8821, 8822, 8823, 8824, 8826, 8827, 1038} ); } // supported projection parameters
};

// Colombia Urban Projection Method
// EPSG CODE: 1052
// Projection Parameters EPSG CODE: 8801, 8802, 8806, 8807, 1039
class ObColombiaUrbanSrs : public ObProjectedSrs
{
public:
  ObColombiaUrbanSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObColombiaUrbanSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::COLOMBIA_URBAN; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8801, 8802, 8806, 8807, 1039 } ); } // supported projection parameters
};

// Lambert Conic Conformal (1SP) Projection Method
// EPSG CODE: 9801
// Projection Parameters EPSG CODE: 8801, 8802, 8805, 8806, 8807
class ObLambertConicConformal1SPSrs : public ObProjectedSrs
{
public:
  ObLambertConicConformal1SPSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObLambertConicConformal1SPSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::LAMBERT_CONIC_CONFORMAL_1SP; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8801, 8802, 8805, 8806, 8807 } ); } // supported projection parameters
};

// Lambert Conic Conformal (2SP) Projection Method
// EPSG CODE: 9802
// Projection Parameters EPSG CODE: 8821, 8822, 8823, 8824, 8826, 8827
class ObLambertConicConformal2SPSrs : public ObProjectedSrs
{
public:
  ObLambertConicConformal2SPSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObLambertConicConformal2SPSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::LAMBERT_CONIC_CONFORMAL_2SP; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8821, 8822, 8823, 8824, 8826, 8827 } ); } // supported projection parameters
};

// Lambert Conic Conformal (2SP Belgium) Projection Method
// EPSG CODE: 9803
// Projection Parameters EPSG CODE: 8821, 8822, 8823, 8824, 8826, 8827
class ObLambertConicConformal2SPBelgiumSrs : public ObProjectedSrs
{
public:
  ObLambertConicConformal2SPBelgiumSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObLambertConicConformal2SPBelgiumSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::LAMBERT_CONIC_CONFORMAL_2SP_BELGIUM; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8821, 8822, 8823, 8824, 8826, 8827 } ); } // supported projection parameters
};

// Mercator (variant A) Projection Method
// EPSG CODE: 9804
// Projection Parameters EPSG CODE: 8801, 8802, 8805, 8806, 8807
class ObMercatorvariantASrs : public ObProjectedSrs
{
public:
  ObMercatorvariantASrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObMercatorvariantASrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::MERCATOR_VARIANT_A; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8801, 8802, 8805, 8806, 8807 } ); } // supported projection parameters
};

// Mercator (variant B) Projection Method
// EPSG CODE: 9805
// Projection Parameters EPSG CODE: 8823, 8802, 8806, 8807
class ObMercatorvariantBSrs : public ObProjectedSrs
{
public:
  ObMercatorvariantBSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObMercatorvariantBSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::MERCATOR_VARIANT_B; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8823, 8802, 8806, 8807 } ); } // supported projection parameters
};

// Cassini-Soldner Projection Method
// EPSG CODE: 9806
// Projection Parameters EPSG CODE: 8801, 8802, 8806, 8807
class ObCassiniSoldnerSrs : public ObProjectedSrs
{
public:
  ObCassiniSoldnerSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObCassiniSoldnerSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::MERCATOR_VARIANT_B; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8801, 8802, 8806, 8807 } ); } // supported projection parameters
};

// Transverse Mercator Projection Method
// EPSG CODE: 9807
// Projection Parameters EPSG CODE: 8801, 8802, 8805, 8806, 8807
class ObTransverseMercatorSrs : public ObProjectedSrs
{
public:
  ObTransverseMercatorSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObTransverseMercatorSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::TRANSVERSE_MERCATOR; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( {8801, 8802, 8805, 8806, 8807} ); } // supported projection parameters
};

// Transverse Mercator (South Orientated) Projection Method
// EPSG CODE: 9808
// Projection Parameters EPSG CODE: 8801, 8802, 8805, 8806, 8807
class ObTransverseMercatorSouthOrientatedSrs : public ObProjectedSrs
{
public:
  ObTransverseMercatorSouthOrientatedSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObTransverseMercatorSouthOrientatedSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::TRANSVERSE_MERCATOR_SOUTH_ORIENTATED; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( {8801, 8802, 8805, 8806, 8807} ); } // supported projection parameters
};

// Oblique Stereographic Projection Method
// EPSG CODE: 9809
// Projection Parameters EPSG CODE: 8801, 8802, 8805, 8806, 8807
class ObObliqueStereographicSrs : public ObProjectedSrs
{
public:
  ObObliqueStereographicSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObObliqueStereographicSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::OBLIQUE_STEREOGRAPHIC; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( {8801, 8802, 8805, 8806, 8807} ); } // supported projection parameters
};

// Polar Stereographic (variant A) Projection Method
// EPSG CODE: 9810
// Projection Parameters EPSG CODE: 8801, 8802, 8805, 8806, 8807
class ObPolarStereographicVariantASrs : public ObProjectedSrs
{
public:
  ObPolarStereographicVariantASrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObPolarStereographicVariantASrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::POLAR_STEREOGRAPHIC_VARIANT_A; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( {8801, 8802, 8805, 8806, 8807} ); } // supported projection parameters
};

// New Zealand Map Grid Projection Method
// EPSG CODE: 9811
// Projection Parameters EPSG CODE: 8801, 8802, 8806, 8807
class ObNewZealandMapGridSrs : public ObProjectedSrs
{
public:
  ObNewZealandMapGridSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObNewZealandMapGridSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::NEW_ZEALAND_MAP_GRID; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8801, 8802, 8806, 8807 } ); } // supported projection parameters
};

// Hotine Oblique Mercator (variant A) Projection Method
// EPSG CODE: 9812
// Projection Parameters EPSG CODE: 8811, 8812, 8813, 8814, 8815, 8806, 8807
class ObHotineObliqueMercatorvariantASrs : public ObProjectedSrs
{
public:
  ObHotineObliqueMercatorvariantASrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObHotineObliqueMercatorvariantASrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::HOTINE_OBLIQUE_MERCATOR_VARIANT_A; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8811, 8812, 8813, 8814, 8815, 8806, 8807 } ); } // supported projection parameters
};

// Laborde Oblique Mercator Projection Method
// EPSG CODE: 9813
// Projection Parameters EPSG CODE: 8811, 8812, 8813, 8815, 8806, 8807
class ObLabordeObliqueMercatorSrs : public ObProjectedSrs
{
public:
  ObLabordeObliqueMercatorSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObLabordeObliqueMercatorSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::LABORDE_OBLIQUE_MERCATOR; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8811, 8812, 8813, 8815, 8806, 8807 } ); } // supported projection parameters
};

// Hotine Oblique Mercator (variant B) Projection Method
// EPSG CODE: 9815
// Projection Parameters EPSG CODE: 8811, 8812, 8813, 8814, 8815, 8816, 8817
class ObHotineObliqueMercatorVariantBSrs : public ObProjectedSrs
{
public:
  ObHotineObliqueMercatorVariantBSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObHotineObliqueMercatorVariantBSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::HOTINE_OBLIQUE_MERCATOR_VARIANT_B; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8811, 8812, 8813, 8814, 8815, 8816, 8817 } ); } // supported projection parameters
};

// Tunisia Mining Grid Projection Method
// EPSG CODE: 9816
// Projection Parameters EPSG CODE: 8821, 8822, 8826, 8827
class ObTunisiaMiningGridSrs : public ObProjectedSrs
{
public:
  ObTunisiaMiningGridSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObTunisiaMiningGridSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::TUNISIA_MINING_GRID; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8821, 8822, 8826, 8827 } ); } // supported projection parameters
};

// Lambert Conic Near-Conformal Projection Method
// EPSG CODE: 9817
// Projection Parameters EPSG CODE: 8801, 8802, 8805, 8806, 8807
class ObLambertConicNearConformalSrs : public ObProjectedSrs
{
public:
  ObLambertConicNearConformalSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObLambertConicNearConformalSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::LAMBERT_CONIC_NEAR_CONFORMAL; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8801, 8802, 8805, 8806, 8807 } ); } // supported projection parameters
};

// American Polyconic Projection Method
// EPSG CODE: 9818
// Projection Parameters EPSG CODE: 8801, 8802, 8806, 8807
class ObAmericanPolyconicSrs : public ObProjectedSrs
{
public:
  ObAmericanPolyconicSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObAmericanPolyconicSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::AMERICAN_POLYCONIC; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8801, 8802, 8806, 8807 } ); } // supported projection parameters
};

// Krovak Projection Method
// EPSG CODE: 9819
// Projection Parameters EPSG CODE: 8811, 8833, 1036, 8818, 8819, 8806, 8807
class ObKrovakSrs : public ObProjectedSrs
{
public:
  ObKrovakSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObKrovakSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::KROVAK; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8811, 8833, 1036, 8818, 8819, 8806, 8807 } ); } // supported projection parameters
};

// Lambert Azimuthal Equal Area Projection Method
// EPSG CODE: 9820
// Projection Parameters EPSG CODE: 8801, 8802, 8806, 8807
class ObLambertAzimuthalEqualAreaSrs : public ObProjectedSrs
{
public:
  ObLambertAzimuthalEqualAreaSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObLambertAzimuthalEqualAreaSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::LAMBERT_AZIMUTHAL_EQUAL_AREA; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8801, 8802, 8806, 8807 } ); } // supported projection parameters
};

// Albers Equal Area Projection Method
// EPSG CODE: 9822
// Projection Parameters EPSG CODE: 8821, 8822, 8823, 8824, 8826, 8827
class ObAlbersEqualAreaSrs : public ObProjectedSrs
{
public:
  ObAlbersEqualAreaSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObAlbersEqualAreaSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::ALBERS_EQUAL_AREA; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8821, 8822, 8823, 8824, 8826, 8827 } ); } // supported projection parameters
};

// Transverse Mercator Zoned Grid System Projection Method
// EPSG CODE: 9824
// Projection Parameters EPSG CODE: 8801, 8830, 8831, 8805, 8806, 8807
class ObTransverseMercatorZonedGridSystemSrs : public ObProjectedSrs
{
public:
  ObTransverseMercatorZonedGridSystemSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObTransverseMercatorZonedGridSystemSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::TRANSVERSE_MERCATOR_ZONED_GRID_SYSTEM; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8801, 8830, 8831, 8805, 8806, 8807 } ); } // supported projection parameters
};

// Lambert Conic Conformal (West Orientated) Projection Method
// EPSG CODE: 9826
// Projection Parameters EPSG CODE: 8801, 8802, 8805, 8806, 8807
class ObLambertConicConformalWestOrientatedSrs : public ObProjectedSrs
{
public:
  ObLambertConicConformalWestOrientatedSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObLambertConicConformalWestOrientatedSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::LAMBERT_CONIC_CONFORMAL_WEST_ORIENTATED; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8801, 8802, 8805, 8806, 8807 } ); } // supported projection parameters
};

// Bonne (South Orientated) Projection Method
// EPSG CODE: 9828
// Projection Parameters EPSG CODE: 8801, 8802, 8806, 8807
class ObBonneSouthOrientatedSrs : public ObProjectedSrs
{
public:
  ObBonneSouthOrientatedSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObBonneSouthOrientatedSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::BONNE_SOUTH_ORIENTATED; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8801, 8802, 8806, 8807 } ); } // supported projection parameters
};

// Polar Stereographic (variant B) Projection Method
// EPSG CODE: 9829
// Projection Parameters EPSG CODE: 8832, 8833, 8806, 8807
class ObPolarStereographicVariantBSrs : public ObProjectedSrs
{
public:
  ObPolarStereographicVariantBSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObPolarStereographicVariantBSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::POLAR_STEREOGRAPHIC_VARIANT_B; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8832, 8833, 8806, 8807 } ); } // supported projection parameters
};

// Polar Stereographic (variant C) Projection Method
// EPSG CODE: 9830
// Projection Parameters EPSG CODE: 8832, 8833, 8826, 8827
class ObPolarStereographicVariantCSrs : public ObProjectedSrs
{
public:
  ObPolarStereographicVariantCSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObPolarStereographicVariantCSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::POLAR_STEREOGRAPHIC_VARIANT_C; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8832, 8833, 8826, 8827 } ); } // supported projection parameters
};

// Guam Projection Projection Method
// EPSG CODE: 9831
// Projection Parameters EPSG CODE: 8801, 8802, 8806, 8807
class ObGuamProjectionSrs : public ObProjectedSrs
{
public:
  ObGuamProjectionSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObGuamProjectionSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::GUAM_PROJECTION; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8801, 8802, 8806, 8807 } ); } // supported projection parameters
};

// Modified Azimuthal Equidistant Projection Method
// EPSG CODE: 9832
// Projection Parameters EPSG CODE: 8801, 8802, 8806, 8807
class ObModifiedAzimuthalEquidistantSrs : public ObProjectedSrs
{
public:
  ObModifiedAzimuthalEquidistantSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObModifiedAzimuthalEquidistantSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::MODIFIED_AZIMUTHAL_EQUIDISTANT; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8801, 8802, 8806, 8807 } ); } // supported projection parameters
};

// Hyperbolic Cassini-Soldner Projection Method
// EPSG CODE: 9833
// Projection Parameters EPSG CODE: 8801, 8802, 8806, 8807
class ObHyperbolicCassiniSoldnerSrs : public ObProjectedSrs
{
public:
  ObHyperbolicCassiniSoldnerSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObHyperbolicCassiniSoldnerSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::HYPERBOLIC_CASSINI_SOLDNER; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8801, 8802, 8806, 8807 } ); } // supported projection parameters
};

// Lambert Cylindrical Equal Area (Spherical) Projection Method
// EPSG CODE: 9834
// Projection Parameters EPSG CODE: 8823, 8802, 8806, 8807
class ObLambertCylindricalEqualAreaSphericalSrs : public ObProjectedSrs
{
public:
  ObLambertCylindricalEqualAreaSphericalSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObLambertCylindricalEqualAreaSphericalSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8823, 8802, 8806, 8807 } ); } // supported projection parameters
};

// Lambert Cylindrical Equal Area Projection Method
// EPSG CODE: 9835
// Projection Parameters EPSG CODE: 8823, 8802, 8806, 8807
class ObLambertCylindricalEqualAreaSrs : public ObProjectedSrs
{
public:
  ObLambertCylindricalEqualAreaSrs(common::ObIAllocator *allocator) : ObProjectedSrs(allocator) {}
  virtual ~ObLambertCylindricalEqualAreaSrs() {}

  ObProjectionType get_projection_type() const override { return ObProjectionType::LAMBERT_CYLINDRICAL_EQUAL_AREA; };
  void register_proj_params() override { OB_GEO_REG_PROJ_PARAMS( { 8823, 8802, 8806, 8807 } ); } // supported projection parameters
};

class ObSrsItem
{
public:
  explicit ObSrsItem(ObSpatialReferenceSystemBase *srs_info) : srs_info_(srs_info) {}
  virtual ~ObSrsItem() {}
  inline double prime_meridian() const { return srs_info_->prime_meridian(); }
  inline double linear_uint() const { return srs_info_->linear_unit(); }
  inline double angular_unit() const { return srs_info_->angular_unit(); }
  inline double semi_major_axis() const { return srs_info_->semi_major_axis(); }
  inline bool is_wgs84() const { return srs_info_->is_wgs84(); }
  inline bool missing_towgs84() const {return (!srs_info_->is_wgs84() && !srs_info_->has_wgs84_value());}
  inline common::ObSrsType srs_type() const { return srs_info_->srs_type(); }
  const ObSrsBoundsItem* get_bounds() const { return srs_info_->get_bounds(); }
  ObString get_proj4text() const { return srs_info_->get_proj4text(); }
  bool is_lat_long_order() const;
  bool is_latitude_north() const;
  bool is_longtitude_east() const;
  int from_radians_to_srs_unit(double radians, double &srs_unit_val) const;
  int from_srs_unit_to_radians(double unit_value, double &radians) const;
  int latitude_convert_to_radians(double value, double &latitude) const;
  int latitude_convert_from_radians(double latitude, double &value) const;
  int longtitude_convert_to_radians(double value, double &longtitude) const;
  int longtitude_convert_from_radians(double longtitude, double &value) const;
  bool is_geographical_srs() const;
  int get_proj4_param(ObIAllocator *allocator, ObString &proj_param) const;
  uint32_t get_srid() const;

  double semi_minor_axis() const;

  friend class ObTenantSrs;

private:
  ObSpatialReferenceSystemBase *srs_info_;
  DISALLOW_COPY_AND_ASSIGN(ObSrsItem);
};

class ObSrsUtils final
{
public:
  ObSrsUtils() {}
  ~ObSrsUtils() {}

  static int check_is_wgs84(const ObGeographicRs *rs, bool &is_wgs84);
  static int get_simple_proj_params(const ObProjectionPrams &parsed_params,
                                    ObVector<ObSimpleProjPram, common::ObFIFOAllocator> &params);
  static int check_authority(const ObRsAuthority& auth, const char *target_auth_name, int target_auth_code, bool allow_invalid, bool &res);

  constexpr static double WGS_SEMI_MAJOR_AXIS = 6378137.0;
  constexpr static double WGS_INVERSE_FLATTENING = 298.257223563;
  constexpr static double WGS_CONVERSION_FACTOR = 0.017453292519943278;
  constexpr static double WGS_PRIMEM_LONG = 0.0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSrsUtils);
};

}  // namespace common
}  // namespace oceanbase

#endif /* OCEANBASE_LIB_GEO_OB_SRS_INFO_ */
