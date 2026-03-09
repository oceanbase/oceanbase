/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_TEST_CO_MERGE_ITER_CHECK_H_
#define OCEANBASE_STORAGE_TEST_CO_MERGE_ITER_CHECK_H_

#include <vector>

#include "storage/compaction/ob_compaction_util.h" // ObMergeLevel
#include "common/ob_version_def.h"                 // DATA_VERSION_*

namespace oceanbase
{
namespace storage
{

enum class SchemaKind : uint8_t
{
  ALL_EACH = 0, // has_all_column_group() = true
  EACH = 1,     // has_all_column_group() = false
};

enum class BaseMajorKind : uint8_t
{
  CO_ALL_CG_BASE = 0, // all cg + normal cg
  CO_ROWKEY_CG_BASE = 1, // rowkey cg + normal cg
  CO_ROW_STORE_ONLY = 2, // all cg only
  ROW_MAJOR = 3, // non-CO major
};

enum class SpecialStatusKind : uint8_t
{
  NONE = 0,
  DELAYED_TRANSFORM_MAJOR = 1,
  COL_REPLICA_MAJOR = 2,
};

// Represents the expected concrete iterator type allocated by the merge helper.
// Mirrors the three leaf classes: ObPartitionRowMergeIter, ObPartitionMacroMergeIter,
// ObPartitionMicroMergeIter. Used in CaseParam so that each test case can declare
// the expected iter types and the test body can compare via dynamic_cast.
enum class IterKind : uint8_t
{
  ROW   = 0,  // ObPartitionRowMergeIter
  MICRO = 1,  // ObPartitionMicroMergeIter
  MACRO = 2,  // ObPartitionMacroMergeIter
};

struct CaseParam
{
  SchemaKind schema_kind_;
  BaseMajorKind base_major_kind_;
  int64_t major_row_cnt_; // drives policy's merge_type decision
  uint64_t data_version_;

  // Expected iter types for all major iters obtained from the builder
  // (via get_major_sstable_merge_iters_for_check). One entry per major iter.
  std::vector<IterKind> expected_major_iter_kinds_;

  // Expected iter types for each writer's iters.
  // First dimension: writer index; second dimension: iter index within that writer.
  std::vector<std::vector<IterKind>> expected_writer_iter_kinds_;

  bool is_all_cg_base() const
  {
    return base_major_kind_ == BaseMajorKind::CO_ALL_CG_BASE
        || base_major_kind_ == BaseMajorKind::CO_ROW_STORE_ONLY;
  }
  bool is_co_table_without_cgs() const
  {
    return base_major_kind_ == BaseMajorKind::CO_ROW_STORE_ONLY;
  }
  bool schema_has_all_cg() const
  {
    return schema_kind_ == SchemaKind::ALL_EACH;
  }
};

static const char *to_str(const SchemaKind v)
{
  return v == SchemaKind::ALL_EACH ? "ALL_EACH" : "EACH";
}

static const char *to_str(const BaseMajorKind v)
{
  switch (v) {
    case BaseMajorKind::CO_ALL_CG_BASE: return "CO_ALL_CG_BASE";
    case BaseMajorKind::CO_ROWKEY_CG_BASE: return "CO_ROWKEY_CG_BASE";
    case BaseMajorKind::CO_ROW_STORE_ONLY: return "CO_ROW_STORE_ONLY";
    case BaseMajorKind::ROW_MAJOR: return "ROW_MAJOR";
    default: return "UNKNOWN";
  }
}

static const char *to_str(const IterKind v)
{
  switch (v) {
    case IterKind::ROW:   return "ROW";
    case IterKind::MICRO: return "MICRO";
    case IterKind::MACRO: return "MACRO";
    default: return "UNKNOWN";
  }
}

// Per-version expected iter kinds. data_version_==0 acts as the default/fallback entry.
// Every BaseCaseParam must contain at least one entry with data_version_==0.
struct VersionedExpectation
{
  uint64_t data_version_; // 0 = default fallback
  std::vector<IterKind> expected_major_iter_kinds_;
  std::vector<std::vector<IterKind>> expected_writer_iter_kinds_;
};

// Base case definition: shape fields + a list of versioned expectations.
// During expansion each (base_case, version) pair resolves expectations by first
// looking for an exact data_version_ match, then falling back to the entry
// with data_version_==0.  Every BaseCaseParam MUST have a default (data_version_==0) entry.
struct BaseCaseParam
{
  SchemaKind schema_kind_;
  BaseMajorKind base_major_kind_;
  int64_t major_row_cnt_;
  std::vector<VersionedExpectation> expectations_;
};

// Central place to control which data versions are covered by this parameterized test.
// Add/remove versions here, without duplicating the whole case list below.
inline const std::vector<uint64_t> &get_data_versions_for_co_merge_iter_type_check()
{
  static const std::vector<uint64_t> versions = {
      MOCK_DATA_VERSION_4_4_2_1,
      DATA_VERSION_4_5_0_0,
      DATA_VERSION_4_5_1_0,
  };
  return versions;
}

inline std::vector<CaseParam> build_all_co_merge_iter_type_configs()
{
  // Keep ONE copy of the "shape" cases here; data_version_ is filled during expansion.
  // Each expectations_ list must contain a default entry (data_version_==0).
  // To override for a specific version, add an extra VersionedExpectation with that version.
  static const std::vector<BaseCaseParam> base_cases = {
      // policy-produced shapes -> sample combinations that can yield different merge types
      // all cg + normal cg -> all cg only
      {SchemaKind::ALL_EACH, BaseMajorKind::CO_ALL_CG_BASE, 5000,
       {{0, {IterKind::MICRO}, {}},
        {MOCK_DATA_VERSION_4_4_2_1, {IterKind::ROW}, {}}
       }
      },
      // all cg + normal cg -> all cg + normal cg
      {SchemaKind::ALL_EACH, BaseMajorKind::CO_ALL_CG_BASE, 15000,
       {{0, {IterKind::MICRO}, {{}, {IterKind::MICRO}}},
        {MOCK_DATA_VERSION_4_4_2_1, {IterKind::MICRO}, {{}, {IterKind::MICRO}}}
       }
      },
      // all cg only -> all cg only
      {SchemaKind::ALL_EACH, BaseMajorKind::CO_ROW_STORE_ONLY, 5000,
       {{0, {IterKind::MICRO}, {}},
        {MOCK_DATA_VERSION_4_4_2_1, {IterKind::ROW}, {}}
       }
      },
      // all cg only -> all cg only
      {SchemaKind::ALL_EACH, BaseMajorKind::CO_ROW_STORE_ONLY, 15000,
       {{0, {IterKind::MICRO}, {}},
        {MOCK_DATA_VERSION_4_4_2_1, {IterKind::ROW}, {}}
       }
      },
      // all cg only -> all cg + normal cg
      {SchemaKind::ALL_EACH, BaseMajorKind::CO_ROW_STORE_ONLY, 25000,
       {{0, {IterKind::MICRO}, {{}, {IterKind::ROW}}}, // row writer
       {MOCK_DATA_VERSION_4_4_2_1, {IterKind::ROW}, {{}, {IterKind::ROW}}} // row writer
       }
      },

      // rowkey cg + normal cg -> all cg only
      {SchemaKind::EACH, BaseMajorKind::CO_ROWKEY_CG_BASE, 5000,
       {{0, {IterKind::ROW}, {}},
        {MOCK_DATA_VERSION_4_4_2_1, {IterKind::ROW}, {}}
       }
      },
      // rowkey cg + normal cg -> rowkey cg + normal cg
      {SchemaKind::EACH, BaseMajorKind::CO_ROWKEY_CG_BASE, 15000,
       {{0, {IterKind::MICRO}, {{}, {IterKind::MICRO}}},
        {MOCK_DATA_VERSION_4_4_2_1, {IterKind::MICRO}, {{}, {IterKind::MICRO}}}
       }
      },
      // all cg only -> all cg only
      {SchemaKind::EACH, BaseMajorKind::CO_ROW_STORE_ONLY, 5000,
       {{0, {IterKind::MICRO}, {}},
        {MOCK_DATA_VERSION_4_4_2_1, {IterKind::ROW}, {}}
       }
      },
      // all cg only -> all cg only
      {SchemaKind::EACH, BaseMajorKind::CO_ROW_STORE_ONLY, 15000,
       {{0, {IterKind::MICRO}, {}},
        {MOCK_DATA_VERSION_4_4_2_1, {IterKind::ROW}, {}}
       }
      },
      // all cg only -> rowkey cg + normal cg // single writer
      {SchemaKind::EACH, BaseMajorKind::CO_ROW_STORE_ONLY, 18000,
       {{0, {IterKind::ROW}, {{IterKind::ROW}}},
        {MOCK_DATA_VERSION_4_4_2_1, {IterKind::ROW}, {{IterKind::ROW}}}
       }
      },

      // delay transform
      // row store -> all cg + normal cg // TODO: reuse not allowed now
      {SchemaKind::ALL_EACH, BaseMajorKind::ROW_MAJOR, 5000,
       {{0, {IterKind::ROW}, {{}, {IterKind::ROW}}}, // row writer
        {MOCK_DATA_VERSION_4_4_2_1, {IterKind::ROW}, {{}, {IterKind::ROW}}} // row writer
       }
      },
  };

  const std::vector<uint64_t> &versions = get_data_versions_for_co_merge_iter_type_check();
  std::vector<CaseParam> all;
  all.reserve(base_cases.size() * versions.size());
  for (uint64_t v : versions) {
    for (const BaseCaseParam &base : base_cases) {
      // Resolve expectations: prefer exact version match, fall back to data_version_==0.
      const VersionedExpectation *resolved = nullptr;
      const VersionedExpectation *fallback = nullptr;
      for (const VersionedExpectation &exp : base.expectations_) {
        if (exp.data_version_ == v) {
          resolved = &exp;
          break;
        }
        if (exp.data_version_ == 0) {
          fallback = &exp;
        }
      }
      if (resolved == nullptr) {
        resolved = fallback;
      }
      CaseParam p;
      p.schema_kind_               = base.schema_kind_;
      p.base_major_kind_           = base.base_major_kind_;
      p.major_row_cnt_             = base.major_row_cnt_;
      p.data_version_              = v;
      p.expected_major_iter_kinds_ = resolved->expected_major_iter_kinds_;
      p.expected_writer_iter_kinds_= resolved->expected_writer_iter_kinds_;
      all.push_back(p);
    }
  }
  return all;
}

inline const std::vector<CaseParam> &get_all_co_merge_iter_type_configs()
{
  static const std::vector<CaseParam> all_configs = build_all_co_merge_iter_type_configs();
  return all_configs;
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TEST_CO_MERGE_ITER_CHECK_H_