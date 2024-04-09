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

#ifndef OCEANBASE_LIB_GEO_MVT_
#define OCEANBASE_LIB_GEO_MVT_

#include "ob_geo.h"
#include "lib/string/ob_string.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_iarray.h"
#include "lib/allocator/ob_allocator.h"
#include "ob_vector_tile.pb-c.h"

namespace oceanbase
{
namespace common
{

typedef struct _ObTileValue {
  VectorTile__Tile__Value value_;
  void *ptr_;
  int32_t len_;
  inline uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = murmurhash(ptr_, len_, hash_val);
    return hash_val;
  }

  inline int hash(uint64_t &res) const
  {
    res = cal_hash(0);
    return OB_SUCCESS;
  }
  inline uint64_t cal_hash(uint64_t seed) const
  {
    uint64_t hash_val = seed;
    if (OB_LIKELY(NULL != ptr_) && OB_LIKELY(len_ > 0)) {
      hash_val = murmurhash(ptr_, len_, hash_val);
    }
    return hash_val;
  }

  inline int hash(uint64_t &hash_val, uint64_t seed) const
  {
    hash_val = cal_hash(seed);
    return OB_SUCCESS;
  }

  inline bool operator==(const _ObTileValue &other) const
  {
    bool bret = false;
    if (ptr_ == other.ptr_ && len_ == other.len_) {
      bret = true;
    } else if (len_ == other.len_) {
      bret = memcmp(ptr_, other.ptr_, len_) == 0;
    }
    return bret;
  }
} ObTileValue;

typedef common::hash::ObHashMap<ObTileValue, uint32_t> AttributeMap;
class mvt_agg_result
{
public:
  mvt_agg_result(common::ObIAllocator &alloc) : allocator_(alloc),
                     inited_(false),
                     lay_name_("default"),
                     geom_name_(),
                     geom_idx_(UINT32_MAX),
                     feature_id_name_(),
                     feat_id_idx_(UINT32_MAX),
                     feature_(nullptr),
                     layer_(),
                     tile_(nullptr),
                     column_cnt_(UINT32_MAX),
                     extent_(4096),
                     feature_capacity_(FEATURE_CAPACITY_INIT),
                     column_offset_(0) {}
  virtual ~mvt_agg_result() { values_map_.destroy(); }
  int init(const ObString &lay_name, const ObString &geom_name,
           const ObString &feat_id, const uint32_t extent);
  inline bool is_inited() { return inited_; }
  inline const common::ObString& get_geom_name() const { return geom_name_; }
  inline common::ObString& get_geom_name() { return geom_name_; }
  int init_layer();
  int generate_feature(ObObj *tmp_obj, uint32_t obj_cnt);
  int transform_geom(const ObGeometry &geo);
  int transform_other_column(ObObj *tmp_obj, uint32_t obj_cnt);
  int transform_json_column(ObObj &json);
  int mvt_pack(ObString &blob_res);
  void set_tmp_allocator(common::ObIAllocator *temp_allocator) { temp_allocator_ = temp_allocator;}
  static bool is_upper_char_exist(const ObString &str);
private:
  int get_key_id(ObString col_name, uint32_t &key_id);

public:

  static const uint32_t FEATURE_CAPACITY_INIT = 50;
  static const int64_t DEFAULT_BUCKET_NUM = 10243L;
  common::ObIAllocator &allocator_;
  // for single row iterate allocator;
  common::ObIAllocator *temp_allocator_;
  bool inited_;
  common::ObString lay_name_;
  common::ObString geom_name_;
  // feature geometry index
  uint32_t geom_idx_;
  common::ObString feature_id_name_;
  // feature id column index
  uint32_t feat_id_idx_;
  VectorTile__Tile__Feature *feature_;
  ObVector<VectorTile__Tile__Feature*> features_;
  VectorTile__Tile__Layer layer_;
  VectorTile__Tile *tile_;
  ObVector<common::ObString> keys_;
  AttributeMap values_map_;
  uint32_t column_cnt_;
  uint32_t extent_;
  uint32_t feature_capacity_;
  uint32_t column_offset_;
  ObVector<uint32_t> tags_;
};

} // namespace common
} // namespace oceanbase

#endif