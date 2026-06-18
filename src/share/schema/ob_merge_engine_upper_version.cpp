/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "ob_merge_engine_upper_version.h"
#include "share/ob_cluster_version.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
OB_INLINE static ObMergeEngineType convert_to_merge_engine_type(const int64_t idx)
{
  return static_cast<ObMergeEngineType>(idx);
}

OB_INLINE static int64_t convert_to_idx(const ObMergeEngineType merge_engine_type)
{
  return static_cast<int64_t>(merge_engine_type);
}

static constexpr ObMergeEngineType MERGE_ENGINE_CHOICE_ORDER[static_cast<int>(ObMergeEngineType::OB_MERGE_ENGINE_UNKNOWN)] = {
  ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE,
  ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT,
  ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY,
};

/************************************* ObMergeEngineUpperVersion **********************************/
ObMergeEngineUpperVersion::ObMergeEngineUpperVersion()
  : version_(MERGE_ENGINE_UPPER_VERSION_V1),
    original_merge_engine_type_(ObMergeEngineType::OB_MERGE_ENGINE_MAX),
    upper_versions_()
{
}

void ObMergeEngineUpperVersion::reset()
{
  upper_versions_.reset();
  original_merge_engine_type_ = ObMergeEngineType::OB_MERGE_ENGINE_MAX;
  version_ = MERGE_ENGINE_UPPER_VERSION_V1;
}

int ObMergeEngineUpperVersion::assign(const ObMergeEngineUpperVersion &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(upper_versions_.assign(other.upper_versions_))) {
      LOG_WARN("Fail to assign enable versions", K(ret));
    } else {
      original_merge_engine_type_ = other.original_merge_engine_type_;
      version_ = other.version_;
    }
  }
  return ret;
}

void ObMergeEngineUpperVersion::disable_merge_engine_for_lob()
{
  int disable_idx = convert_to_idx(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  // old server may not init the upper versions
  if (disable_idx < upper_versions_.count()) {
    upper_versions_[disable_idx] = share::SCN::min_scn();
  }
}

int ObMergeEngineUpperVersion::write_string(ObString &str, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  int64_t len = get_serialize_size();
  int64_t pos = 0;
  void *buf = nullptr;
  if (!is_inited()) {
    str = ObString::make_empty_string();
  } else if (OB_ISNULL(buf = allocator.alloc(len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc memory for merge engine upper version", K(ret), K(len));
  } else if (OB_FAIL(serialize(static_cast<char *>(buf), len, pos))) {
    allocator.free(buf);
    buf = nullptr;
    LOG_WARN("Failed to serialize merge engine upper version", K(ret), KP(buf), K(len), K(pos));
  } else {
    str.assign_ptr(static_cast<char *>(buf), len);
  }
  return ret;
}

int ObMergeEngineUpperVersion::init_upper_version(const ObMergeEngineType merge_engine_type)
{
  int ret = OB_SUCCESS;
  const int64_t merge_engine_count = convert_to_idx(ObMergeEngineType::OB_MERGE_ENGINE_UNKNOWN);
  if (OB_UNLIKELY(!ObMergeEngineStoreFormat::is_merge_engine_valid(merge_engine_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid merge engine type", K(ret), K(merge_engine_type));
  } else if (OB_UNLIKELY(is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Merge engine upper version is already initialized", K(ret), KPC(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_engine_count; ++i) {
      if (i == convert_to_idx(merge_engine_type)) {
        if (OB_FAIL(upper_versions_.push_back(share::SCN::max_scn()))) {
          LOG_WARN("Fail to push back enable version", K(ret));
        }
      } else if (OB_FAIL(upper_versions_.push_back(share::SCN::min_scn()))) {
        LOG_WARN("Fail to push back enable version", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      original_merge_engine_type_ = merge_engine_type;
    }
  }
  return ret;
}

int ObMergeEngineUpperVersion::update_upper_version(
    const uint64_t tenant_id,
    const share::SCN &upper_version,
    const ObMergeEngineType origin_merge_engine_type,
    const ObMergeEngineType new_merge_engine_type)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_UNLIKELY(!ObMergeEngineStoreFormat::is_merge_engine_valid(origin_merge_engine_type) ||
    !ObMergeEngineStoreFormat::is_merge_engine_valid(new_merge_engine_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid merge engine type", K(ret), K(origin_merge_engine_type), K(new_merge_engine_type));
  // Upgrade scenarios below
  } else if (upper_versions_.count() < convert_to_idx(ObMergeEngineType::OB_MERGE_ENGINE_UNKNOWN)) {
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
      LOG_WARN("Fail to get min data version", K(ret), K(tenant_id));
    } else {
      int64_t compat_merge_engine_count = 0;
      // if add a new merge engine in 4.6.1
      // if (compat_version >= DATA_VERSION_4_6_1_0)) {
      //   merge_engine_count = static_cast<int64_t>(ObMergeEngineType::OB_MERGE_ENGINE_NEW) + 1;
      // } else
      if (compat_version >= DATA_VERSION_4_6_1_0) {
        compat_merge_engine_count = convert_to_idx(ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY) + 1;
      }
      // If new observer adds a new merge engine, the upper versions will be incomplete when upgrading from old servers.
      // We set the upper versions to min scn to for new merge engine to disable it before update.
      // e.g. new added merge engine is 3, orginal upper versions is [11, 22, MAX], new upper versions is [11, 22, MAX, MIN].
      while (OB_SUCC(ret) && upper_versions_.count() < compat_merge_engine_count) {
        if (OB_FAIL(upper_versions_.push_back(share::SCN::min_scn()))) {
          LOG_WARN("Fail to push back enable version", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && OB_LIKELY(origin_merge_engine_type != new_merge_engine_type)) {
    if (OB_UNLIKELY(convert_to_idx(origin_merge_engine_type) >= upper_versions_.count() ||
                    convert_to_idx(new_merge_engine_type) >= upper_versions_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid merge engine type", K(ret), K(origin_merge_engine_type), K(new_merge_engine_type), K(upper_versions_));
    } else {
      upper_versions_[convert_to_idx(origin_merge_engine_type)] = upper_version;
      upper_versions_[convert_to_idx(new_merge_engine_type)] = share::SCN::max_scn();
    }
  }
  return ret;
}

int ObMergeEngineUpperVersion::decide_query_merge_engine(const int64_t major_table_version, ObMergeEngineType &merge_engine_type) const
{
  int ret = OB_SUCCESS;
  merge_engine_type = ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE;
  const int64_t merge_engine_count = convert_to_idx(ObMergeEngineType::OB_MERGE_ENGINE_UNKNOWN);
  if (OB_UNLIKELY(merge_engine_count > upper_versions_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Merge engine count is greater than upper versions count", K(ret), K(merge_engine_count), K_(upper_versions));
  } else {
    for (int64_t i = 0; i < merge_engine_count; ++i) {
      const ObMergeEngineType candi_engine_type = MERGE_ENGINE_CHOICE_ORDER[i];
      if (major_table_version < upper_versions_.at(convert_to_idx(candi_engine_type)).get_val_for_tx()) {
        merge_engine_type = candi_engine_type;
        break;
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObMergeEngineUpperVersion,
                    version_,
                    original_merge_engine_type_,
                    upper_versions_)

}
}
}
