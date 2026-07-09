/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "ob_merge_engine_upper_version.h"
#include "ob_table_schema.h"
#include "share/ob_cluster_version.h"
#include "share/ob_share_util.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx/ob_ts_mgr.h"

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

OB_INLINE static int64_t get_compat_merge_engine_count(const uint64_t data_version)
{
  int64_t compat_merge_engine_count = 0;
  if (data_version >= DATA_VERSION_5_0_1_0) {
    compat_merge_engine_count = convert_to_idx(ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY) + 1;
  }
  return compat_merge_engine_count;
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

int ObMergeEngineUpperVersion::construct(const common::ObString &upper_version_str, const ObMergeEngineType original_merge_engine_type)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (upper_version_str.empty()) {
    reset();
    // no ddl change the merge engine type, so set the original merge engine type to the current merge engine type
    set_original_merge_engine_type(original_merge_engine_type);
  } else if (OB_FAIL(deserialize(upper_version_str.ptr(), upper_version_str.length(), pos))) {
    LOG_WARN("Failed to deserialize merge engine upper version", K(ret), KPHEX(upper_version_str.ptr(), upper_version_str.length()));
  } else if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Merge engine upper version is not valid", K(ret), KPC(this));
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
    const int64_t compat_merge_engine_count,
    const share::SCN &upper_version,
    const ObMergeEngineType origin_merge_engine_type,
    const ObMergeEngineType new_merge_engine_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(compat_merge_engine_count < 0 ||
    !upper_version.is_valid() || share::SCN::max_scn() == upper_version || share::SCN::min_scn() == upper_version ||
    !ObMergeEngineStoreFormat::is_merge_engine_valid(origin_merge_engine_type) ||
    !ObMergeEngineStoreFormat::is_merge_engine_valid(new_merge_engine_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid merge engine type", K(ret), K(compat_merge_engine_count),
      K(upper_version), K(origin_merge_engine_type), K(new_merge_engine_type));
  } else {
    // If new observer adds a new merge engine, the upper versions will be incomplete when upgrading from old servers.
    // We set the upper versions to min scn to for new merge engine to disable it before update.
    // e.g. new added merge engine is 3, orginal upper versions is [11, 22, MAX], new upper versions is [11, 22, MAX, MIN].
    while (OB_SUCC(ret) && upper_versions_.count() < compat_merge_engine_count) {
      if (OB_FAIL(upper_versions_.push_back(share::SCN::min_scn()))) {
        LOG_WARN("Fail to push back enable version", K(ret));
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

int ObMergeEngineUpperVersion::merge_upper_version_for_exchange(
    const uint64_t data_version,
    const share::SCN &current_scn,
    ObMergeEngineUpperVersion &other,
    const ObMergeEngineType this_merge_engine,
    const ObMergeEngineType other_merge_engine)
{
  int ret = OB_SUCCESS;
  const int64_t this_engine_idx = convert_to_idx(this_merge_engine);
  const int64_t other_engine_idx = convert_to_idx(other_merge_engine);
  if (data_version < DATA_VERSION_5_0_1_0) {
    // version too old to support merge engine, skip
  } else {
    const int64_t compat_merge_engine_count = get_compat_merge_engine_count(data_version);
    // step 1: fill uninitialized upper_versions to compat_merge_engine_count,
    // then set current merge engine position to max_scn
    if (upper_versions_.count() < compat_merge_engine_count) {
      const bool need_init = !is_inited();
      while (OB_SUCC(ret) && upper_versions_.count() < compat_merge_engine_count) {
        if (OB_FAIL(upper_versions_.push_back(share::SCN::min_scn()))) {
          LOG_WARN("Fail to fill this upper version", K(ret));
        }
      }
      if (OB_SUCC(ret) && need_init) {
        upper_versions_[this_engine_idx] = share::SCN::max_scn();
        original_merge_engine_type_ = this_merge_engine;
      }
    }
    if (OB_SUCC(ret) && other.upper_versions_.count() < compat_merge_engine_count) {
      const bool need_init = !other.is_inited();
      while (OB_SUCC(ret) && other.upper_versions_.count() < compat_merge_engine_count) {
        if (OB_FAIL(other.upper_versions_.push_back(share::SCN::min_scn()))) {
          LOG_WARN("Fail to fill other upper version", K(ret));
        }
      }
      if (OB_SUCC(ret) && need_init) {
        other.upper_versions_[other_engine_idx] = share::SCN::max_scn();
        other.original_merge_engine_type_ = other_merge_engine;
      }
    }
    // step 2: element-wise max
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < upper_versions_.count(); ++i) {
        const share::SCN merged = (upper_versions_.at(i) > other.upper_versions_.at(i))
                                  ? upper_versions_.at(i) : other.upper_versions_.at(i);
        upper_versions_[i] = merged;
        other.upper_versions_[i] = merged;
      }
    }
    // step 3: when merge engines differ, keep own engine as max_scn,
    // set the other's engine position to current_scn
    if (OB_SUCC(ret) && this_merge_engine != other_merge_engine) {
      if (OB_UNLIKELY(!current_scn.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid current_scn for exchange merge", K(ret), K(current_scn));
      } else {
        upper_versions_[this_engine_idx] = share::SCN::max_scn();
        upper_versions_[other_engine_idx] = current_scn;
        other.upper_versions_[other_engine_idx] = share::SCN::max_scn();
        other.upper_versions_[this_engine_idx] = current_scn;
      }
    }
    // step 4: validate — both must have equal length and exactly one max_scn at own engine position
    if (OB_SUCC(ret)) {
      int64_t this_max_cnt = 0;
      int64_t other_max_cnt = 0;
      for (int64_t i = 0; i < upper_versions_.count(); ++i) {
        if (upper_versions_.at(i) == share::SCN::max_scn()) {
          this_max_cnt++;
        }
        if (other.upper_versions_.at(i) == share::SCN::max_scn()) {
          other_max_cnt++;
        }
      }
      if (OB_UNLIKELY(upper_versions_.count() != other.upper_versions_.count()
                       || 1 != this_max_cnt
                       || 1 != other_max_cnt
                       || share::SCN::max_scn() != upper_versions_.at(this_engine_idx)
                       || share::SCN::max_scn() != other.upper_versions_.at(other_engine_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("merge_engine_upper_version validation failed after exchange merge", K(ret),
            K_(upper_versions), K(other.upper_versions_),
            K(this_merge_engine), K(other_merge_engine),
            K(this_max_cnt), K(other_max_cnt));
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObMergeEngineUpperVersion,
                    version_,
                    original_merge_engine_type_,
                    upper_versions_)

/************************************* ObMergeEngineUtil **********************************/
int ObMergeEngineUtil::get_gts_scn(const uint64_t tenant_id, share::SCN &scn)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  bool is_external_consistent = false;
  if (OB_UNLIKELY(common::OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret));
  } else if (OB_FAIL(OB_TS_MGR.get_ts_sync(tenant_id,
                                           ctx.get_timeout(),
                                           scn,
                                           is_external_consistent))) {
    LOG_WARN("fail to get gts sync", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObMergeEngineUtil::init_merge_engine_upper_version(ObTableSchema &table_schema, const ObMergeEngineType merge_engine_type)
{
  return table_schema.get_merge_engine_upper_version_for_update().init_upper_version(merge_engine_type);
}

int ObMergeEngineUtil::update_merge_engine_upper_version(
    ObTableSchema &table_schema,
    const uint64_t data_version,
    const ObMergeEngineType origin_merge_engine_type)
{
  int ret = OB_SUCCESS;
  share::SCN upper_version;
  const int64_t compat_merge_engine_count = get_compat_merge_engine_count(data_version);
  if (OB_FAIL(get_gts_scn(table_schema.get_tenant_id(), upper_version))) {
    LOG_WARN("fail to get gts scn", KR(ret), K(table_schema.get_tenant_id()));
  } else if (OB_FAIL(table_schema.get_merge_engine_upper_version_for_update().update_upper_version(
      compat_merge_engine_count, upper_version, origin_merge_engine_type, table_schema.get_merge_engine_type()))) {
    LOG_WARN("failed to update merge engine upper version", KR(ret), K(compat_merge_engine_count),
        K(upper_version), K(origin_merge_engine_type), K(table_schema));
  }
  return ret;
}

int ObMergeEngineUtil::merge_exchange_merge_engine_upper_version(
    ObTableSchema &this_table_schema,
    ObTableSchema &other_table_schema,
    const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  share::SCN current_scn;
  if (this_table_schema.get_merge_engine_type() != other_table_schema.get_merge_engine_type()) {
    if (OB_FAIL(get_gts_scn(this_table_schema.get_tenant_id(), current_scn))) {
      LOG_WARN("fail to get gts scn", KR(ret), K(this_table_schema.get_tenant_id()));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(this_table_schema.get_merge_engine_upper_version_for_update().merge_upper_version_for_exchange(
      data_version,
      current_scn,
      other_table_schema.get_merge_engine_upper_version_for_update(),
      this_table_schema.get_merge_engine_type(),
      other_table_schema.get_merge_engine_type()))) {
    LOG_WARN("fail to merge merge_engine_upper_version for exchange", KR(ret),
        K(this_table_schema), K(other_table_schema));
  }
  return ret;
}

int ObMergeEngineUtil::inherit_merge_engine(
    ObTableSchema &table_schema,
    const ObMergeEngineUpperVersion &other,
    const ObMergeEngineType inherit_merge_engine_type)
{
  int ret = OB_SUCCESS;
  const share::SCN max_upper_version = other.get_max_upper_version();
  const ObMergeEngineType origin_merge_engine_type = table_schema.get_merge_engine_type();
  if (!other.is_inited() || share::SCN::min_scn() == max_upper_version ||
      origin_merge_engine_type == inherit_merge_engine_type) {
    // Skip inheriting when the main table has no real merge engine switch history
    // (upper versions uninitialized, or initialized by exchange but never altered),
    // or the merge engine type of the aux table is unchanged.
  } else if (table_schema.is_fts_index() || table_schema.is_vec_index()) {
    table_schema.set_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  } else if (OB_FAIL(table_schema.get_merge_engine_upper_version_for_update()
                      .update_upper_version(other.get_upper_version_count(),
                                            max_upper_version,
                                            origin_merge_engine_type,
                                            inherit_merge_engine_type))) {
    LOG_WARN("fail to update merge engine upper version", KR(ret), K(other), K(inherit_merge_engine_type));
  } else if (table_schema.is_aux_lob_table()) {
    table_schema.get_merge_engine_upper_version_for_update().disable_merge_engine_for_lob();
    table_schema.set_merge_engine_type(
        ObMergeEngineStoreFormat::get_lob_aux_inherit_merge_engine_type(inherit_merge_engine_type));
  } else {
    table_schema.set_merge_engine_type(inherit_merge_engine_type);
  }
  return ret;
}

}
}
}
