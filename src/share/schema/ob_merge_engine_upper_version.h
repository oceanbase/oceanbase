/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_MERGE_ENGINE_UPPER_VERSION_H
#define _OB_MERGE_ENGINE_UPPER_VERSION_H

#include "common/ob_store_format.h"
#include "lib/container/ob_se_array.h"
#include "share/scn.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObMergeEngineUpperVersion
{
  OB_UNIS_VERSION(1);
  static constexpr uint8_t MERGE_ENGINE_UPPER_VERSION_V1 = 1;
public:
  ObMergeEngineUpperVersion();
  ~ObMergeEngineUpperVersion() = default;
  void reset();
  int assign(const ObMergeEngineUpperVersion &other);
  void disable_merge_engine_for_lob();
  int write_string(ObString &str, ObIAllocator &allocator) const;
  int init_upper_version(const ObMergeEngineType merge_engine_type);
  int update_upper_version(
    const uint64_t tenant_id,
    const share::SCN &upper_version,
    const ObMergeEngineType origin_merge_engine_type,
    const ObMergeEngineType new_merge_engine_type);
  int decide_query_merge_engine(const int64_t major_table_version, ObMergeEngineType &merge_engine_type) const;
  OB_INLINE bool is_inited() const { return upper_versions_.count() > 0; }
  OB_INLINE bool is_valid() const
  {
    return version_ == MERGE_ENGINE_UPPER_VERSION_V1 &&
      upper_versions_.count() <= static_cast<int64_t>(ObMergeEngineType::OB_MERGE_ENGINE_UNKNOWN) &&
      ObMergeEngineStoreFormat::is_merge_engine_valid(original_merge_engine_type_);
  }
  OB_INLINE ObMergeEngineType get_original_merge_engine_type() const { return original_merge_engine_type_; }
  OB_INLINE void set_original_merge_engine_type(const ObMergeEngineType merge_engine_type) { original_merge_engine_type_ = merge_engine_type; }
  inline int64_t get_convert_size() const
  {
    int64_t convert_size = sizeof(*this);
    convert_size += upper_versions_.get_data_size();
    return convert_size;
  }
  TO_STRING_KV(K_(version), K_(original_merge_engine_type), K_(upper_versions));
private:
  uint8_t version_;
  ObMergeEngineType original_merge_engine_type_;
  // the right boundary of row's trans version of each merge engine
  ObSEArray<share::SCN, 4> upper_versions_;
  DISALLOW_COPY_AND_ASSIGN(ObMergeEngineUpperVersion);
};

} // end namespace schema
} // end namespace share
} // end namespace oceanbase

#endif /* _OB_MERGE_ENGINE_UPPER_VERSION_H */
