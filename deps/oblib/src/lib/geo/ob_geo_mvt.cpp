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

#define USING_LOG_PREFIX LIB
#include "ob_geo_mvt.h"
#include "share/ob_lob_access_utils.h"
#include "share/object/ob_obj_cast.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_mvt_encode_visitor.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_bin.h"

namespace oceanbase {
namespace common {

int mvt_agg_result::init(const ObString &lay_name, const ObString &geom_name,
                         const ObString &feat_id, const uint32_t extent)
{
  int ret = OB_SUCCESS;
  lay_name_ = lay_name;
  geom_name_ = geom_name;
  feature_id_name_ = feat_id;
  extent_ = extent;
  vector_tile__tile__layer__init(&layer_);
  layer_.version = 2;
  layer_.name = lay_name_.ptr();

  return ret;
}

int mvt_agg_result::init_layer()
{
  int ret = OB_SUCCESS;
  vector_tile__tile__layer__init(&layer_);
  layer_.version = 2;
  layer_.name = lay_name_.ptr();
  values_map_.create(DEFAULT_BUCKET_NUM, "MvtValues", "MvtValues", MTL_ID());
  layer_.extent = extent_;
  return ret;
}

int mvt_agg_result::generate_feature(ObObj *tmp_obj, uint32_t obj_cnt)
{
  int ret = OB_SUCCESS;
  if (geom_idx_ >= obj_cnt) {
    ret = OB_ERR_GIS_UNSUPPORTED_ARGUMENT;
    LOG_WARN("can't find geom column in feature", K(ret), K(column_offset_), K(geom_idx_));
  } else if (tmp_obj[geom_idx_].is_null()) {
    // geometry column is null. do nothing
  } else if (!tmp_obj[geom_idx_].is_geometry()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid object type", K(ret), K(tmp_obj[geom_idx_]));
  } else {
    feature_ = static_cast<VectorTile__Tile__Feature *>(allocator_.alloc(sizeof(VectorTile__Tile__Feature)));
    if (OB_ISNULL(feature_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      const ObObjMeta& meta = tmp_obj[geom_idx_].get_meta();
      ObString str = tmp_obj[geom_idx_].get_string();
      ObGeometry *geo = NULL;
      vector_tile__tile__feature__init(feature_);
      if (OB_FAIL(features_.push_back(feature_))) {
        LOG_WARN("failed to push back feature", K(ret));
      } else if (meta.is_null()) {
        str.reset();
      } else if (is_lob_storage(meta.get_type())) {
        ObTextStringIter str_iter(meta.get_type(), meta.get_collation_type(), str, tmp_obj[geom_idx_].has_lob_header());
        if (OB_FAIL(str_iter.init(0, NULL, temp_allocator_))) {
          LOG_WARN("Lob: init lob str iter failed ", K(ret), K(str_iter));
        } else if (OB_FAIL(str_iter.get_full_data(str))) {
          LOG_WARN("Lob: str iter get full data failed ", K(ret), K(str_iter));
        } else if (OB_FAIL(ObGeoTypeUtil::construct_geometry(*temp_allocator_, str, NULL, geo, true))) {
          LOG_WARN("failed to construct geometry", K(ret));
        } else if (ObGeoTypeUtil::is_3d_geo_type(geo->type())
                  && OB_FAIL(ObGeoTypeUtil::convert_geometry_3D_to_2D(NULL, allocator_, geo, ObGeoBuildFlag::GEO_ALL_DISABLE, geo))) {
          LOG_WARN("failed to convert 3d to 2d", K(ret));
        } else if (OB_FAIL(transform_geom(*geo))) {
          LOG_WARN("failed to transform geometry", K(ret));
        } else if (OB_FAIL(transform_other_column(tmp_obj, obj_cnt))) {
          LOG_WARN("failed to transform other column", K(ret));
        } else {
          feature_->n_tags = tags_.size();
          feature_->tags = static_cast<uint32_t *>(allocator_.alloc(feature_->n_tags * sizeof(*(feature_->tags))));
          if (OB_ISNULL(feature_->tags) && tags_.size()) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", K(ret), K(tags_.size()));
          }
          for (uint32_t i = 0; i < tags_.size() && OB_SUCC(ret); i++) {
            feature_->tags[i] = tags_.at(i);
          }
          tags_.clear();
        }
      }
    }
  }

  return ret;
}

int mvt_agg_result::transform_geom(const ObGeometry &geo)
{
  int ret = OB_SUCCESS;
  switch(geo.type()) {
    case ObGeoType::POINT :
    case ObGeoType::MULTIPOINT :
      feature_->type = VECTOR_TILE__TILE__GEOM_TYPE__POINT;
      break;
    case ObGeoType::LINESTRING :
    case ObGeoType::MULTILINESTRING :
      feature_->type = VECTOR_TILE__TILE__GEOM_TYPE__LINESTRING;
      break;
    case ObGeoType::POLYGON :
    case ObGeoType::MULTIPOLYGON :
      feature_->type = VECTOR_TILE__TILE__GEOM_TYPE__POLYGON;
      break;
    default :
      ret = OB_ERR_UNEXPECTED_GEOMETRY_TYPE;
        LOG_WARN("unexpected geometry type for st_area", K(ret));
        LOG_USER_ERROR(OB_ERR_UNEXPECTED_GEOMETRY_TYPE, ObGeoTypeUtil::get_geo_name_by_type(geo.type()),
          ObGeoTypeUtil::get_geo_name_by_type(geo.type()), "_st_asmvt");
  }
  if (OB_SUCC(ret)) {
    ObGeoMvtEncodeVisitor visitor;
    if (OB_FAIL(const_cast<ObGeometry &>(geo).do_visit(visitor))) {
      LOG_WARN("failed to do mvt encode visit", K(ret));
    } else {
      ObVector<uint32_t> &buf = visitor.get_encode_buffer();
      feature_->n_geometry = buf.size();
      feature_->geometry = static_cast<uint32_t *>(allocator_.alloc(sizeof(uint32_t) * feature_->n_geometry));
      if (OB_ISNULL(feature_->geometry)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(feature_->n_geometry));
      } else {
        for (uint32_t i = 0; i < feature_->n_geometry; i++) {
          feature_->geometry[i] = buf.at(i);
        }
      }
    }
  }
  return ret;
}

int mvt_agg_result::get_key_id(ObString col_name, uint32_t &key_id)
{
  int ret = OB_SUCCESS;
  key_id = UINT32_MAX;
  for (uint32_t i = 0; i < keys_.size() && key_id == UINT32_MAX; i++) {
    if (col_name.case_compare(keys_.at(i)) == 0) {
      key_id = i;
    }
  }
  if (key_id == UINT32_MAX) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int mvt_agg_result::transform_json_column(ObObj &json)
{
  int ret = OB_SUCCESS;
  ObString str;
  ObTextStringIter str_iter(json.get_meta().get_type(), json.get_meta().get_collation_type(), json.get_string(), json.has_lob_header());
  if (OB_FAIL(str_iter.init(0, NULL, temp_allocator_))) {
    LOG_WARN("Lob: init lob str iter failed ", K(ret), K(str_iter));
  } else if (OB_FAIL(str_iter.get_full_data(str))) {
    LOG_WARN("Lob: str iter get full data failed ", K(ret), K(str_iter));
  } else {
    ObArenaAllocator temp_allocator;
    ObJsonBin j_bin(str.ptr(), str.length(), &temp_allocator);
    ObIJsonBase *j_base = &j_bin;
    if (OB_FAIL(j_bin.reset_iter())) {
      COMMON_LOG(WARN, "fail to reset json bin iter", K(ret));
    } else if (j_base->json_type() == ObJsonNodeType::J_OBJECT) {
      uint64_t count = j_base->element_count();
      for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        uint32_t key_id;
        ObString key;
        ObString key_name;
        if (OB_FAIL(j_base->get_key(i, key))) {
          LOG_WARN("fail to get key", K(ret), K(i));
        } else if (OB_FAIL(get_key_id(key, key_id))) {
          if (OB_FAIL(ob_write_string(allocator_, key, key_name, true))) {
            LOG_WARN("write string failed", K(ret), K(key));
          } else if (OB_FAIL(keys_.push_back(key_name))) {
            LOG_WARN("failed to push back col name to keys", K(ret), K(i));
          } else {
            key_id = keys_.size() - 1;
          }
        }
        if (OB_SUCC(ret)) {
          ObIJsonBase *jb_ptr = NULL;
          ObJsonBin j_val(temp_allocator_);
          jb_ptr = &j_val;
          if (OB_FAIL(j_base->get_object_value(i, jb_ptr))) {
            LOG_WARN("fail to get object value", K(ret), K(i));
          } else { // value
            bool ignore_type = false;
            ObTileValue tile_value;
            VectorTile__Tile__Value value;
            vector_tile__tile__value__init(&value);
            ObJsonNodeType j_type = jb_ptr->json_type();
            switch(j_type) {
              case ObJsonNodeType::J_BOOLEAN: {
                bool v = jb_ptr->get_boolean();
                value.bool_value = v;
                value.test_oneof_case = VECTOR_TILE__TILE__VALUE__TEST_ONEOF_BOOL_VALUE;
                tile_value.ptr_ = &value.bool_value;
                tile_value.len_ = sizeof(value.bool_value);
                break;
              }
              case ObJsonNodeType::J_DOUBLE: {
                double v = jb_ptr->get_double();
                value.double_value = v;
                value.test_oneof_case = VECTOR_TILE__TILE__VALUE__TEST_ONEOF_DOUBLE_VALUE;
                tile_value.ptr_ = &value.double_value;
                tile_value.len_ = sizeof(value.double_value);
                break;
              }
              case ObJsonNodeType::J_INT: {
                int64_t v = jb_ptr->get_int();
                if (v >= 0) {
                  value.uint_value = v;
                  value.test_oneof_case = VECTOR_TILE__TILE__VALUE__TEST_ONEOF_UINT_VALUE;
                  tile_value.ptr_ = &value.uint_value;
                  tile_value.len_ = sizeof(value.uint_value);
                } else {
                  value.int_value = v;
                  value.test_oneof_case = VECTOR_TILE__TILE__VALUE__TEST_ONEOF_SINT_VALUE;
                  tile_value.ptr_ = &value.int_value;
                  tile_value.len_ = sizeof(value.int_value);
                }
                break;
              }
              case ObJsonNodeType::J_UINT: {
                int64_t v = jb_ptr->get_uint();
                value.uint_value = v;
                value.test_oneof_case = VECTOR_TILE__TILE__VALUE__TEST_ONEOF_UINT_VALUE;
                tile_value.ptr_ = &value.uint_value;
                tile_value.len_ = sizeof(value.uint_value);
                break;
              }
              case ObJsonNodeType::J_STRING: {
                uint64_t data_len = jb_ptr->get_data_length();
                const char *data = jb_ptr->get_data();
                ObString str_val;
                if (OB_FAIL(ob_write_string(allocator_, ObString(data_len, data), str_val, true))) {
                  LOG_WARN("failed to copy string ", K(ret), KP(data));
                } else {
                  value.string_value = str_val.ptr();
                  value.test_oneof_case = VECTOR_TILE__TILE__VALUE__TEST_ONEOF_STRING_VALUE;
                  tile_value.ptr_ = value.string_value;
                  tile_value.len_ = str_val.length();
                }
                break;
              }
              default:
                // ignore other type, do nothing
                ignore_type = true;
                break;;
            }
            if (OB_SUCC(ret) && !ignore_type) {
              tile_value.value_ = value;
              uint32_t tag_id;
              if (OB_FAIL(values_map_.get_refactored(tile_value, tag_id))) {
                LOG_WARN("failed to get key", K(ret));
                if (OB_HASH_NOT_EXIST == ret) {
                  tag_id = values_map_.size();
                  if (OB_FAIL(values_map_.set_refactored(tile_value, tag_id))) {
                    LOG_WARN("failed to set key", K(ret));
                  }
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(tags_.push_back(key_id))) {
                  LOG_WARN("failed to push back key id", K(ret), K(key_id), K(tag_id));
                } else if (OB_FAIL(tags_.push_back(tag_id))) {
                  LOG_WARN("failed to push back value id", K(ret), K(key_id), K(tag_id));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int mvt_agg_result::transform_other_column(ObObj *tmp_obj, uint32_t obj_cnt)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = column_offset_; i < obj_cnt && OB_SUCC(ret); i += 2) {
    ObObjTypeClass tc = tmp_obj[i].get_type_class();
    ObObjType type = tmp_obj[i].get_type();
    uint32_t key_id;
    if (i == geom_idx_) {
      // do nothing
    } else if (i == feat_id_idx_) {
      // feature id
      if (ob_is_null(type)) {
        // do nothing, ignore null
      } else if (!ob_is_int_tc(type) && !ob_is_uint_tc(type)) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("invalid type for feature id", K(ret), K(type));
      } else {
        int64_t v = tmp_obj[i].get_int();
        if (v >= 0) {
          feature_->has_id = true;
          feature_->id = v;
        }
      }
    } else if (ob_is_json(type)) {
      if (OB_FAIL(transform_json_column(tmp_obj[i]))) {
        LOG_WARN("failed to transform json column", K(ret));
      }
    } else if (tmp_obj[i].is_null()
              || ob_is_enum_or_set_type(type)) { // enum/set type mvt encode isn't supported
      // do nothing
    } else if (OB_FAIL(get_key_id(tmp_obj[i + 1].get_string(), key_id))) {
      LOG_WARN("failed to get column key id", K(ret));
    } else {
      ObTileValue tile_value;
      VectorTile__Tile__Value value;
      vector_tile__tile__value__init(&value);
      if (type == ObTinyIntType) {
        // bool type
        bool v = (0 == tmp_obj[i].get_int()) ? false : true;
        value.bool_value = v;
        value.test_oneof_case = VECTOR_TILE__TILE__VALUE__TEST_ONEOF_BOOL_VALUE;
        tile_value.ptr_ = &value.bool_value;
        tile_value.len_ = sizeof(value.bool_value);
      } else if (ob_is_int_tc(type)) {
        int64_t v = tmp_obj[i].get_int();
        if (v >= 0) {
          value.uint_value = v;
          value.test_oneof_case = VECTOR_TILE__TILE__VALUE__TEST_ONEOF_UINT_VALUE;
          tile_value.ptr_ = &value.uint_value;
          tile_value.len_ = sizeof(value.uint_value);
        } else {
          value.int_value = v;
          value.test_oneof_case = VECTOR_TILE__TILE__VALUE__TEST_ONEOF_SINT_VALUE;
          tile_value.ptr_ = &value.int_value;
          tile_value.len_ = sizeof(value.int_value);
        }
      } else if (ob_is_uint_tc(type)) {
        uint64_t v = tmp_obj[i].get_uint64();
        value.uint_value = v;
        value.test_oneof_case = VECTOR_TILE__TILE__VALUE__TEST_ONEOF_UINT_VALUE;
        tile_value.ptr_ = &value.uint_value;
        tile_value.len_ = sizeof(value.uint_value);
      } else if (ob_is_float_tc(type)) {
        float v = tmp_obj[i].get_float();
        value.float_value = v;
        value.test_oneof_case = VECTOR_TILE__TILE__VALUE__TEST_ONEOF_FLOAT_VALUE;
        tile_value.ptr_ = &value.float_value;
        tile_value.len_ = sizeof(value.float_value);
      } else if (ob_is_double_tc(type)) {
        double v = tmp_obj[i].get_double();
        value.double_value = v;
        value.test_oneof_case = VECTOR_TILE__TILE__VALUE__TEST_ONEOF_DOUBLE_VALUE;
        tile_value.ptr_ = &value.double_value;
        tile_value.len_ = sizeof(value.double_value);
      } else if (ob_is_enum_or_set_type(type)) {
        // do nothing, enum/set type mvt encode isn't supported
      } else {
        // other type cast to varchar
        ObObj obj;
        ObObj geo_hex;
        ObString str;
        ObCastCtx cast_ctx(temp_allocator_, NULL, CM_NONE, ObCharset::get_system_collation());
        if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx, tmp_obj[i], obj))) {
          LOG_WARN("failed to cast number to double type", K(ret));
        } else if (ob_is_geometry(type) && OB_FAIL(ObHexUtils::hex(ObString(obj.get_string().length(), obj.get_string().ptr()),
                                                                   cast_ctx, geo_hex))) {
          LOG_WARN("failed to cast geo to hex", K(ret));
        } else if (OB_FAIL(ob_write_string(allocator_, ob_is_geometry(type) ? geo_hex.get_string() : obj.get_string(), str, true))) {
          LOG_WARN("failed to copy c string", K(ret));
        } else {
          value.string_value = str.ptr();
          value.test_oneof_case = VECTOR_TILE__TILE__VALUE__TEST_ONEOF_STRING_VALUE;
          tile_value.ptr_ = value.string_value;
          tile_value.len_ = str.length();
        }
      }
      if (OB_SUCC(ret)) {
        tile_value.value_ = value;
        uint32_t tag_id;
        if (OB_FAIL(values_map_.get_refactored(tile_value, tag_id))) {
          if (OB_HASH_NOT_EXIST == ret) {
            tag_id = values_map_.size();
            if (OB_FAIL(values_map_.set_refactored(tile_value, tag_id))) {
              LOG_WARN("failed to set key", K(ret));
            }
          } else {
            LOG_WARN("failed to get key", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(tags_.push_back(key_id))) {
            LOG_WARN("failed to push back key id", K(ret), K(key_id), K(tag_id));
          } else if (OB_FAIL(tags_.push_back(tag_id))) {
            LOG_WARN("failed to push back value id", K(ret), K(key_id), K(tag_id));
          }
        }
      }
    }
  }

  return ret;
}

int mvt_agg_result::mvt_pack(ObString &blob_res)
{
  int ret = OB_SUCCESS;
  if (tile_ == NULL) {
    tile_ = static_cast<VectorTile__Tile *>(allocator_.alloc(sizeof(VectorTile__Tile)));
    if (OB_ISNULL(tile_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      vector_tile__tile__init(tile_);
      tile_->layers = static_cast<VectorTile__Tile__Layer **>(allocator_.alloc(sizeof(VectorTile__Tile__Layer *)));
      if (OB_ISNULL(tile_->layers)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        tile_->layers[0] = &layer_;
        tile_->n_layers = 1;
        layer_.features = static_cast<VectorTile__Tile__Feature **>(allocator_.alloc(sizeof(VectorTile__Tile__Feature *) * features_.size()));
        if (OB_ISNULL(layer_.features) && features_.size()) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), K(features_.size()));
        } else {
          for (uint32_t i = 0; i < features_.size(); i++) {
            layer_.features[i] = features_.at(i);
          }
          layer_.n_features = features_.size();
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // add keys to layer
    layer_.n_keys = keys_.size();
    layer_.keys = static_cast<char **>(allocator_.alloc(sizeof(char *) * keys_.size()));
    if (OB_ISNULL(layer_.keys) && keys_.size()) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    }
    for (uint32_t i = 0; i < keys_.size() && OB_SUCC(ret); i++) {
      layer_.keys[i] = keys_.at(i).ptr();
    }

    if (OB_SUCC(ret)) {
      // add values to layer
      layer_.n_values = values_map_.size();
      layer_.values = static_cast<VectorTile__Tile__Value **>(allocator_.alloc(sizeof(*layer_.values) * values_map_.size()));
      if (OB_ISNULL(layer_.values) && !values_map_.empty()) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      }
      AttributeMap::iterator lt = values_map_.begin();
      while (OB_SUCC(ret) && lt != values_map_.end()) {
        uint32_t i = lt->second;
        layer_.values[i] = &(lt->first.value_);
        ++lt;
      }
    }

    if (OB_SUCC(ret)) {
      size_t total_len =  vector_tile__tile__get_packed_size(tile_);
      uint8_t *buf = static_cast<uint8_t *>(allocator_.alloc(total_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        vector_tile__tile__pack(tile_, buf);
        blob_res.assign_ptr(reinterpret_cast<char *>(buf), total_len);
      }
    }
  }
  return ret;

}

bool mvt_agg_result::is_upper_char_exist(const ObString &str)
{
  bool res = false;
  const char *ptr = str.ptr();
  for (int32_t i = 0; i < str.length() && !res; i++) {
    if (isupper(static_cast<unsigned char>(ptr[i]))) {
      res = true;
    }
  }
  return res;
}

} // namespace common
} // namespace oceanbase