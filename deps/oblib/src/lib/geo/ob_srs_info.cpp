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
 * This file contains implementation support for the srs info.
 */


#define USING_LOG_PREFIX LIB
#include "lib/geo/ob_srs_info.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/geo/ob_geo_common.h"
#include "rpc/obmysql/ob_mysql_global.h"
#include "lib/charset/ob_dtoa.h"
#include <stdio.h>
#include <stdlib.h>

namespace oceanbase
{
namespace common
{

int ObSrsUtils::check_authority(const ObRsAuthority& auth, const char *target_auth_name, int target_auth_code, bool allow_invalid, bool &res)
{
  int ret = OB_SUCCESS;
  res = true;
  if (!auth.is_valid) {
    if (!allow_invalid) {
      res = false;
    }
  } else {
    int code = ObCharset::strntoll(auth.org_code.ptr(), auth.org_code.length(), 10, &ret);
    if (OB_FAIL(ret)) {
      res = false;
      LOG_WARN("failed to convert string to int", K(ret));
    } else if (auth.org_name.case_compare(target_auth_name) || code != target_auth_code) {
      res = false;
    }
  }
  return ret;
}

int ObSrsUtils::check_is_wgs84(const ObGeographicRs *rs, bool &is_wgs84)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_authority(rs->authority, "EPSG", 4326, false, is_wgs84))) {
    LOG_WARN("faild to check authority is wgs84 or not", K(ret));
  } else if (is_wgs84 && (rs->axis.x.direction != ObAxisDirection::NORTH ||
                          rs->axis.y.direction != ObAxisDirection::EAST)) {
    is_wgs84 = false;
  } else if (is_wgs84 && OB_FAIL(check_authority(rs->datum_info.spheroid.authority, "EPSG", 7030, true, is_wgs84))) {
    LOG_WARN("faild to check authority is wgs84 or not", K(ret));
  } else if (is_wgs84 && (rs->datum_info.spheroid.semi_major_axis != WGS_SEMI_MAJOR_AXIS ||
             rs->datum_info.spheroid.inverse_flattening != WGS_INVERSE_FLATTENING)) {
    is_wgs84 = false;
  } else if (is_wgs84 && OB_FAIL(check_authority(rs->datum_info.authority, "EPSG", 6326, true, is_wgs84))) {
    LOG_WARN("faild to check authority is wgs84 or not", K(ret), K(is_wgs84));
  } else if (is_wgs84 && OB_FAIL(check_authority(rs->primem.authority, "EPSG", 8901, true, is_wgs84))) {
    LOG_WARN("faild to check authority is wgs84 or not", K(ret), K(is_wgs84));
  } else if (is_wgs84 && rs->primem.longtitude != WGS_PRIMEM_LONG) {
    is_wgs84 = false;
  } else if (is_wgs84 && OB_FAIL(check_authority(rs->unit.authority, "EPSG", 9122, true, is_wgs84))) {
    LOG_WARN("faild to check authority is wgs84 or not", K(ret), K(is_wgs84));
  } else if (is_wgs84 && rs->unit.conversion_factor != WGS_CONVERSION_FACTOR) {
    is_wgs84 = false;
  } else if (rs->datum_info.towgs84.is_valid) {
    for (int i = 0; i < WGS84_PARA_NUM && is_wgs84; i++) {
      if (rs->datum_info.towgs84.value[i] != 0.0) {
        is_wgs84 = false;
      }
    }
  }

  return ret;
}

// todo@dazhi: compare the param_name and param_alias when epsg isnot comparable ?
int ObSrsUtils::get_simple_proj_params(const ObProjectionPrams &parsed_params,
                                       ObVector<ObSimpleProjPram, common::ObFIFOAllocator> &params)
{
  int ret = OB_SUCCESS;
  FOREACH_X(parsed_param, parsed_params.vals, OB_SUCC(ret)) {
    if (parsed_param->authority.org_name.case_compare("EPSG") == 0) {
      FOREACH(param, params) {
        ObString epsg_code_str = parsed_param->authority.org_code;
        int epsg_code = ObCharset::strntoll(epsg_code_str.ptr(), epsg_code_str.length(), 10, &ret);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to convert string to int", K(ret));
        } else if (epsg_code == param->epsg_code_) {
          param->value_ = parsed_param->value;
          break;
        }
      }
    }
  }
  FOREACH_X(param, params, OB_SUCC(ret)) {
    if (std::isnan(param->value_)) {
      int epsg_code = param->epsg_code_;
      ret = OB_ERR_UNEXPECTED; // todo@dazhi: ER_SRS_PROJ_PARAMETER_MISSING
      LOG_WARN("invalid nan projection parameter", K(ret), K(epsg_code));
    }
  }
  return ret;
}

int ObSpatialReferenceSystemBase::create_project_srs(ObIAllocator* allocator, uint64_t srs_id,
                                                     const ObProjectionRs *rs, ObSpatialReferenceSystemBase *&srs_info)
{
  int ret = OB_SUCCESS;
  int epsg_code = 0;
  if (OB_ISNULL(rs) || OB_ISNULL(allocator)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("rs or allocator is null", K(allocator), KP(rs));
  } else {
    ObString epsg_code_str = rs->projection.authority.org_code;
    epsg_code = ObCharset::strntoll(epsg_code_str.ptr(), epsg_code_str.length(), 10, &ret);
  }

  if (OB_SUCC(ret)) {
    switch (epsg_code) {
      case static_cast<int>(ObProjectionType::POPULAR_VISUAL_PSEUDO_MERCATOR) : {
        ret = create_srs_internal<ObPopularVisualPseudoMercatorSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::LAMBERT_AZIMUTHAL_EQUAL_AREA_SPHERICAL) : {
        ret = create_srs_internal<ObLambertAzimuthalEqualAreaSphericalSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::EQUIDISTANT_CYLINDRICAL) : {
        ret = create_srs_internal<ObEquidistantCylindricalSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::EQUIDISTANT_CYLINDRICAL_SPHERICAL) : {
        ret = create_srs_internal<ObEquidistantCylindricalSphericalSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::KROVAK_NORTH_ORIENTATED) : {
        ret = create_srs_internal<ObKrovakNorthOrientatedSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::KROVAK_MODIFIED) : {
        ret = create_srs_internal<ObKrovakModifiedSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::KROVAK_MODIFIED_NORTH_ORIENTATED) : {
        ret = create_srs_internal<ObKrovakModifiedNorthOrientatedSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::LAMBERT_CONIC_CONFORMAL_2SP_MICHIGAN) : {
        ret = create_srs_internal<ObLambertConicConformal2SPMichiganSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::COLOMBIA_URBAN) : {
        ret = create_srs_internal<ObColombiaUrbanSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::LAMBERT_CONIC_CONFORMAL_1SP) : {
        ret = create_srs_internal<ObLambertConicConformal1SPSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::LAMBERT_CONIC_CONFORMAL_2SP) : {
        ret = create_srs_internal<ObLambertConicConformal2SPSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::LAMBERT_CONIC_CONFORMAL_2SP_BELGIUM) : {
        ret = create_srs_internal<ObLambertConicConformal2SPBelgiumSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::MERCATOR_VARIANT_A) : {
        ret = create_srs_internal<ObMercatorvariantASrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::MERCATOR_VARIANT_B) : {
        ret = create_srs_internal<ObMercatorvariantBSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::CASSINI_SOLDNER) : {
        ret = create_srs_internal<ObCassiniSoldnerSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::TRANSVERSE_MERCATOR) : {
        ret = create_srs_internal<ObTransverseMercatorSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::TRANSVERSE_MERCATOR_SOUTH_ORIENTATED) : {
        ret = create_srs_internal<ObTransverseMercatorSouthOrientatedSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::OBLIQUE_STEREOGRAPHIC) : {
        ret = create_srs_internal<ObObliqueStereographicSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::POLAR_STEREOGRAPHIC_VARIANT_A) : {
        ret = create_srs_internal<ObPolarStereographicVariantASrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::NEW_ZEALAND_MAP_GRID) : {
        ret = create_srs_internal<ObNewZealandMapGridSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::HOTINE_OBLIQUE_MERCATOR_VARIANT_A) : {
        ret = create_srs_internal<ObHotineObliqueMercatorvariantASrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::LABORDE_OBLIQUE_MERCATOR) : {
        ret = create_srs_internal<ObLabordeObliqueMercatorSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::HOTINE_OBLIQUE_MERCATOR_VARIANT_B) : {
        ret = create_srs_internal<ObHotineObliqueMercatorVariantBSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::TUNISIA_MINING_GRID) : {
        ret = create_srs_internal<ObTunisiaMiningGridSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::LAMBERT_CONIC_NEAR_CONFORMAL) : {
        ret = create_srs_internal<ObLambertConicNearConformalSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::AMERICAN_POLYCONIC) : {
        ret = create_srs_internal<ObAmericanPolyconicSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::KROVAK) : {
        ret = create_srs_internal<ObKrovakSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::LAMBERT_AZIMUTHAL_EQUAL_AREA) : {
        ret = create_srs_internal<ObLambertAzimuthalEqualAreaSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::ALBERS_EQUAL_AREA) : {
        ret = create_srs_internal<ObAlbersEqualAreaSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::TRANSVERSE_MERCATOR_ZONED_GRID_SYSTEM) : {
        ret = create_srs_internal<ObTransverseMercatorZonedGridSystemSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::LAMBERT_CONIC_CONFORMAL_WEST_ORIENTATED) : {
        ret = create_srs_internal<ObLambertConicConformalWestOrientatedSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::BONNE_SOUTH_ORIENTATED) : {
        ret = create_srs_internal<ObBonneSouthOrientatedSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::POLAR_STEREOGRAPHIC_VARIANT_B) : {
        ret = create_srs_internal<ObPolarStereographicVariantBSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::POLAR_STEREOGRAPHIC_VARIANT_C) : {
        ret = create_srs_internal<ObPolarStereographicVariantCSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::GUAM_PROJECTION) : {
        ret = create_srs_internal<ObGuamProjectionSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::MODIFIED_AZIMUTHAL_EQUIDISTANT) : {
        ret = create_srs_internal<ObModifiedAzimuthalEquidistantSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::HYPERBOLIC_CASSINI_SOLDNER) : {
        ret = create_srs_internal<ObHyperbolicCassiniSoldnerSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL) : {
        ret = create_srs_internal<ObLambertCylindricalEqualAreaSphericalSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      case static_cast<int>(ObProjectionType::LAMBERT_CYLINDRICAL_EQUAL_AREA) : {
        ret = create_srs_internal<ObLambertCylindricalEqualAreaSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
      default: {
        ret = create_srs_internal<ObUnknownProjectedSrs>(allocator, srs_id, rs, srs_info);
        break;
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to create projected srs", K(ret), K(srs_id));
  }

  return ret;
}

template <typename SRS_T, typename RS_T>
int ObSpatialReferenceSystemBase::create_srs_internal(ObIAllocator* allocator, uint64_t srs_id,
                                                      const RS_T *rs, ObSpatialReferenceSystemBase *&srs_info)
{
  int ret = OB_SUCCESS;
  SRS_T *tmp_srs_info = NULL;
  void *buf = allocator->alloc(sizeof(SRS_T));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc projected srs failed", K(ret), K(srs_id));
  } else {
    tmp_srs_info = new(buf)SRS_T(static_cast<common::ObIAllocator*>(allocator));
    if (OB_FAIL(tmp_srs_info->init(srs_id, rs))) {
      LOG_WARN("srs info init failed", K(ret), KP(rs), K(srs_id));
    } else {
      srs_info = tmp_srs_info;
    }
  }

  if (OB_FAIL(ret) && tmp_srs_info != NULL) {
    allocator->free(tmp_srs_info);
  }
  return ret;
}

int ObSpatialReferenceSystemBase::create_geographic_srs(ObIAllocator* allocator, uint64_t srs_id,
                                                        const ObGeographicRs *rs, ObSpatialReferenceSystemBase *&srs_info)
{
  return create_srs_internal<ObGeographicSrs, ObGeographicRs>(allocator, srs_id, rs, srs_info);
}

ObGeographicSrs::ObGeographicSrs(common::ObIAllocator* alloc)
  : semi_major_axis_(NAN), inverse_flattening_(NAN),
    is_wgs84_(false), prime_meridian_(NAN), angular_factor_(NAN), bounds_info_(),
    proj4text_()
{
  for (uint8_t i = 0; i < WGS84_PARA_NUM; i++) {
    wgs84_[i] = NAN;
  }
  for (uint8_t i = 0; i < AXIS_DIRECTION_NUM; i++) {
    axis_dir_[i] = ObAxisDirection::INIT;
  }
}

int ObGeographicSrs::init(uint32_t srs_id, const ObGeographicRs *rs)
{
  int ret = OB_SUCCESS;

  id_ = srs_id;
  semi_major_axis_ = rs->datum_info.spheroid.semi_major_axis;
  inverse_flattening_ = rs->datum_info.spheroid.inverse_flattening;
  prime_meridian_ = rs->primem.longtitude;
  angular_factor_ = rs->unit.conversion_factor;

  if (std::isnan(semi_major_axis_) || std::isinf(semi_major_axis_) || id_ >= UINT32_MAX
      || std::isnan(inverse_flattening_) || std::isinf(inverse_flattening_)
      || std::isnan(prime_meridian_) || std::isinf(prime_meridian_)
      || std::isnan(angular_factor_) || std::isinf(angular_factor_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid srs value", K(id_), K(semi_major_axis_), K(inverse_flattening_), K(prime_meridian_), K(angular_factor_));
  } else {
    axis_dir_[0] = rs->axis.x.direction;
    axis_dir_[1] = rs->axis.y.direction;
    if (rs->datum_info.towgs84.is_valid) {
      for (uint8_t i = 0; i < WGS84_PARA_NUM && OB_SUCC(ret); i++) {
        wgs84_[i] = rs->datum_info.towgs84.value[i];
        if (std::isinf(wgs84_[i])) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid wgs84 value", K(wgs84_[i]));
        }
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(ObSrsUtils::check_is_wgs84(rs, is_wgs84_))) {
    LOG_WARN("failed to check srs is wgs84 based or not", K(ret));
  }

  return ret;
}

bool ObSrsItem::is_lat_long_order() const
{
  return srs_info_->srs_type() == ObSrsType::GEOGRAPHIC_SRS
    && (srs_info_->axis_direction(0) == ObAxisDirection::SOUTH || srs_info_->axis_direction(0) == ObAxisDirection::NORTH);
}

bool ObSrsItem::is_latitude_north() const
{
  return (is_lat_long_order() && srs_info_->axis_direction(0) == ObAxisDirection::NORTH)
         || srs_info_->axis_direction(1) == ObAxisDirection::NORTH;
}

bool ObSrsItem::is_longtitude_east() const
{
  return (is_lat_long_order() && srs_info_->axis_direction(1) == ObAxisDirection::EAST)
         || srs_info_->axis_direction(0) == ObAxisDirection::EAST;
}

int ObSrsItem::from_radians_to_srs_unit(double radians, double &srs_unit_val) const
{
  int ret = OB_SUCCESS;
  ObSrsType type = srs_info_->srs_type();
  double angle_uint = srs_info_->angular_unit();
  if (type == ObSrsType::GEOGRAPHIC_SRS && angle_uint > 0.0) {
    srs_unit_val = radians / angle_uint;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid srs type", K(type), K(radians), K(angle_uint));
  }
  return ret;
}

int ObSrsItem::get_proj4_param(ObIAllocator *allocator, ObString &proj_param) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(srs_info_->get_proj4_param(allocator, proj_param))) {
    LOG_WARN("failed to get proj4 param", K(ret), K(get_srid()));
  } else if (proj_param.empty()) {
    proj_param = get_proj4text();
  }
  return ret;
}

int ObGeographicSrs::get_proj4_param(ObIAllocator *allocator, ObString &proj4_param) const
{
  int ret = OB_SUCCESS;
  proj4_param.reset();
  if (is_wgs84_ || has_wgs84_value()) {
    ObGeoStringBuffer string_buf(allocator);
    char tmp_buf[FLOATING_POINT_BUFFER];
    int length = 0;
    if (OB_FAIL(string_buf.append("+proj=lonlat "))) {
      LOG_WARN("failed to append string to proj param", K(ret));
    } else {
      length = ob_fcvt(semi_major_axis_, std::numeric_limits<double>::max_digits10, FLOATING_POINT_BUFFER - 1, tmp_buf, NULL);
      if (OB_FAIL(string_buf.append("+a="))) {
        LOG_WARN("failed to append string to proj param", K(ret), K(length));
      } else if (OB_FAIL(string_buf.append(tmp_buf))) {
        LOG_WARN("failed to append string to proj param", K(ret), K(length));
      }
    }

    if (OB_SUCC(ret)) {
      if (inverse_flattening_ == 0.0) {
        length = ob_fcvt(inverse_flattening_, std::numeric_limits<double>::max_digits10, FLOATING_POINT_BUFFER - 1, tmp_buf, NULL);
        if (OB_FAIL(string_buf.append(" +b="))) {
          LOG_WARN("failed to append string to proj param", K(ret), K(length));
        } else if (OB_FAIL(string_buf.append(tmp_buf))) {
          LOG_WARN("failed to append string to proj param", K(ret), K(length));
        }
      } else {
        length = ob_fcvt(inverse_flattening_, std::numeric_limits<double>::max_digits10, FLOATING_POINT_BUFFER - 1, tmp_buf, NULL);
        if (OB_FAIL(string_buf.append(" +rf="))) {
          LOG_WARN("failed to append string to proj param", K(ret), K(length));
        } else if (OB_FAIL(string_buf.append(tmp_buf))) {
          LOG_WARN("failed to append string to proj param", K(ret), K(length));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(string_buf.append(" +towgs84="))) {
        LOG_WARN("failed to append string to proj param", K(ret));
      } else if (has_wgs84_value()) {
        for (int i = 0; i < WGS84_PARA_NUM; i++) {
          length = ob_fcvt(wgs84_[i], std::numeric_limits<double>::max_digits10, FLOATING_POINT_BUFFER - 1, tmp_buf, NULL);
          if (OB_FAIL(string_buf.append(tmp_buf))) {
            LOG_WARN("failed to append string to proj param", K(ret), K(length));
          } else if (i != WGS84_PARA_NUM - 1 && OB_FAIL(string_buf.append(","))) {
            LOG_WARN("failed to append string to proj param", K(ret), K(length));
          }
        }
      } else {
        if (OB_FAIL(string_buf.append("0,0,0,0,0,0,0"))) {
          LOG_WARN("failed to append string to proj param", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(string_buf.append(" +no_defs"))) {
        LOG_WARN("failed to append string to proj param", K(ret));
      } else if (OB_FAIL(ob_write_string(*allocator, string_buf.string(), proj4_param, true))) {
        LOG_WARN("failed to write string to proj4 param", K(ret));
      }
    }
  }
  return ret;
}

uint32_t ObSrsItem::get_srid() const
{
  uint32_t srid = 0;
  if (OB_NOT_NULL(srs_info_)) {
    srid = srs_info_->get_srid();
  }
  return srid;
}

int ObProjectedSrs::init(uint64_t srs_id,  const ObProjectionRs *rs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(geographic_srs_.init(srs_id, &(rs->projected_rs)))) {
    LOG_WARN("failed to init geographic srs", K(ret));
  } else if (std::isnan(rs->unit.conversion_factor)){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid conversion factor", K(ret));
  } else {
    id_ = srs_id;
    linear_unit_ = rs->unit.conversion_factor;
    axis_dir_[0] = rs->axis.x.direction;
    axis_dir_[1] = rs->axis.y.direction;
    if ((axis_dir_[0] == ObAxisDirection::INIT) ^
        (axis_dir_[1] == ObAxisDirection::INIT)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid axis direction, either all or none is init", K(ret));
    } else if (FALSE_IT(register_proj_params())) {
    } else if (simple_proj_prams_.size() > 0 &&
               OB_FAIL(ObSrsUtils::get_simple_proj_params(rs->proj_params, simple_proj_prams_))) {
      LOG_WARN("failed to get simple prams", K(ret), K(srs_id));
    }
  }
  return ret;
}

int ObProjectedSrs::get_proj4_param(ObIAllocator *allocator, ObString &proj4_param) const
{
  UNUSEDx(allocator, proj4_param);
  return OB_SUCCESS;
}

int ObSrsItem::from_srs_unit_to_radians(double unit_value, double &radians) const
{
  int ret = OB_SUCCESS;

  ObSrsType type = srs_info_->srs_type();
  double angle_uint = srs_info_->angular_unit();
  if (type == ObSrsType::GEOGRAPHIC_SRS && angle_uint > 0.0) {
    radians = unit_value * angle_uint;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid srs type", K(type), K(radians), K(angle_uint));
  }
  return ret;
}

int ObSrsItem::latitude_convert_to_radians(double value, double &latitude) const
{
  int ret = OB_SUCCESS;
  double radians = 0.0;
  if (OB_FAIL(from_srs_unit_to_radians(value, radians))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed convert to radians", K(ret), K(value));
  } else {
    latitude = is_latitude_north() ? radians : (radians * (-1.0));
  }
  return ret;
}

int ObSrsItem::latitude_convert_from_radians(double latitude, double &value) const
{
  int ret = OB_SUCCESS;
  double unit_value = 0.0;
  if (OB_FAIL(from_radians_to_srs_unit(latitude, unit_value))) {
    LOG_WARN("failed convert to srs unit", K(ret), K(latitude));
  } else {
    value = is_latitude_north() ? unit_value : (unit_value * (-1.0));
  }
  return ret;
}

int ObSrsItem::longtitude_convert_to_radians(double value, double &longtitude) const
{
  int ret = OB_SUCCESS;
  double angle = angular_unit();
  ObSrsType type = srs_info_->srs_type();
  if (angle > 0.0 && type == ObSrsType::GEOGRAPHIC_SRS) {
    double tmp = is_longtitude_east() ? value : (value * (-1.0));
    longtitude = (tmp + prime_meridian()) * angle;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid srs type", K(type), K(angle), K(value));
  }
  return ret;
}

int ObSrsItem::longtitude_convert_from_radians(double longtitude, double &value) const
{
  int ret = OB_SUCCESS;
  double angle = angular_unit();
  ObSrsType type = srs_info_->srs_type();
  if (angle > 0.0 && type == ObSrsType::GEOGRAPHIC_SRS) {
    double tmp = longtitude / angle;
    tmp -= prime_meridian();
    value = is_longtitude_east() ? tmp : (tmp * (-1.0));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid srs type", K(type), K(angle), K(longtitude));
  }
  return ret;
}

double ObSrsItem::semi_minor_axis() const
{
  double semi_minor = 0.0;
  if (srs_info_->srs_type() == ObSrsType::GEOGRAPHIC_SRS) {
    double flat = srs_info_->inverse_flattening();
    if (flat != 0.0) {
      semi_minor = srs_info_->semi_major_axis() * (1 - 1 / flat);
    } else {
      semi_minor = srs_info_->semi_major_axis();
    }
  }

  return semi_minor;
}

bool ObSrsItem::is_geographical_srs() const
{
  return srs_info_->srs_type() == ObSrsType::GEOGRAPHIC_SRS;
}

void ObGeographicSrs::set_bounds(double min_x, double min_y, double max_x, double max_y)
{
  bounds_info_.minX_ = min_x;
  bounds_info_.minY_ = min_y;
  bounds_info_.maxX_ = max_x;
  bounds_info_.maxY_ = max_y;
}

int64_t ObSrsBoundsItem::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K(minX_), K(minY_), K(maxX_), K(maxY_));
  return pos;
}

}  // namespace common
}  // namespace oceanbase