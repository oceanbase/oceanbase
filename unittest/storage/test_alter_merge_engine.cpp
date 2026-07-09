/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/ob_cluster_version.h"
#include "share/schema/ob_merge_engine_upper_version.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
using namespace share;
using namespace schema;

namespace unittest
{

class ObAlterMergeEngineTest : public ::testing::Test
{
public:
static constexpr int64_t COMPAT_MERGE_ENGINE_COUNT = static_cast<int64_t>(ObMergeEngineType::OB_MERGE_ENGINE_UNKNOWN);

static SCN make_scn(const uint64_t ts_ns)
{
  SCN scn;
  scn.set_min();
  scn.ts_ns_ = ts_ns;
  return scn;
}

void TearDown()
{
  upper_version_.reset();
}

void check_init_upper_version(const ObMergeEngineType merge_engine_type)
{
  ASSERT_EQ(true, upper_version_.is_valid());
  switch (merge_engine_type) {
    case ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE: {
      ASSERT_EQ(upper_version_.upper_versions_[0], share::SCN::max_scn());
      ASSERT_EQ(upper_version_.upper_versions_[1], share::SCN::min_scn());
      ASSERT_EQ(upper_version_.upper_versions_[2], share::SCN::min_scn());
      break;
    }
    case ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT: {
      ASSERT_EQ(upper_version_.upper_versions_[0], share::SCN::min_scn());
      ASSERT_EQ(upper_version_.upper_versions_[1], share::SCN::max_scn());
      ASSERT_EQ(upper_version_.upper_versions_[2], share::SCN::min_scn());
      break;
    }
    case ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY: {
      ASSERT_EQ(upper_version_.upper_versions_[0], share::SCN::min_scn());
      ASSERT_EQ(upper_version_.upper_versions_[1], share::SCN::min_scn());
      ASSERT_EQ(upper_version_.upper_versions_[2], share::SCN::max_scn());
      break;
    }
    default: {
      ASSERT_EQ(1, 0);
      break;
    }
  }
}

void check_upper_versions(const ObMergeEngineUpperVersion &upper_version, const std::vector<SCN> &scn_array)
{
  const int64_t max_idx = static_cast<int64_t>(ObMergeEngineType::OB_MERGE_ENGINE_UNKNOWN);
  ASSERT_EQ(max_idx, upper_version.upper_versions_.count());
  for (int64_t i = 0; i < max_idx; ++i) {
    ASSERT_EQ(upper_version.upper_versions_.at(i), scn_array[i]);
  }
}

void check_update_upper_version(const SCN &cur_scn, const std::vector<SCN> scn_array)
{
  printf("------check cur_scn: %llu------\n", cur_scn.ts_ns_);
  ASSERT_EQ(true, upper_version_.is_valid());
  check_upper_versions(upper_version_, scn_array);
}

// the main table and the aux table should decide the same query merge engine for any major version
void check_decide_consistency(const ObMergeEngineUpperVersion &main_upper_version,
                              const ObMergeEngineUpperVersion &aux_upper_version)
{
  const int64_t major_versions[] = {0, 1, 3, 5, 7, 9, 12, 100};
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(major_versions) / sizeof(int64_t)); ++i) {
    ObMergeEngineType main_engine = ObMergeEngineType::OB_MERGE_ENGINE_MAX;
    ObMergeEngineType aux_engine = ObMergeEngineType::OB_MERGE_ENGINE_MAX;
    ASSERT_EQ(OB_SUCCESS, main_upper_version.decide_query_merge_engine(major_versions[i], main_engine));
    ASSERT_EQ(OB_SUCCESS, aux_upper_version.decide_query_merge_engine(major_versions[i], aux_engine));
    ASSERT_EQ(main_engine, aux_engine);
  }
}

public:
  ObMergeEngineUpperVersion upper_version_;
};

TEST_F(ObAlterMergeEngineTest, test_init_upper_version)
{
  upper_version_.reset();
  ASSERT_EQ(OB_SUCCESS, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  check_init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  ASSERT_EQ(OB_ERR_UNEXPECTED, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  upper_version_.reset();
  ASSERT_EQ(OB_SUCCESS, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  check_init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  upper_version_.reset();
  ASSERT_EQ(OB_SUCCESS, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  check_init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY);
  upper_version_.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_UNKNOWN));
  upper_version_.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_MAX));
  upper_version_.reset();
}

TEST_F(ObAlterMergeEngineTest, test_update_upper_version)
{
  SCN scn0, scn1, scn2, scn3;
  scn0.set_min();
  scn1.set_min();
  scn2.set_min();
  scn3.set_min();

  // update from an uninitialized upper version, the array should be padded to compat count first
  upper_version_.reset();
  upper_version_.set_original_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  scn0.ts_ns_ = 1;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, scn0, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  scn1.ts_ns_ = 1; scn2 = SCN::max_scn(); scn3 = SCN::min_scn();
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  upper_version_.reset();
  ASSERT_EQ(OB_SUCCESS, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  scn0.ts_ns_ = 3;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, scn0, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  scn1.ts_ns_ = 3; scn2 = SCN::max_scn(); scn3 = SCN::min_scn();
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 4;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, scn0, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  scn1.ts_ns_ = 3; scn2.ts_ns_ = 4; scn3 = SCN::max_scn();
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 5;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, scn0, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  scn1.ts_ns_ = 3; scn2 = SCN::max_scn(); scn3.ts_ns_ = 5;
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 6;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, scn0, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  scn1 = SCN::max_scn(); scn2.ts_ns_ = 6; scn3.ts_ns_ = 5;
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 7;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, scn0, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  scn1.ts_ns_ = 7; scn2.ts_ns_ = 6; scn3 = SCN::max_scn();
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 8;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, scn0, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  scn1 = SCN::max_scn(); scn2.ts_ns_ = 6; scn3.ts_ns_ = 8;
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  upper_version_.reset();
  ASSERT_EQ(OB_SUCCESS, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  scn0.ts_ns_ = 9;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, scn0, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  scn1 = SCN::min_scn(); scn2 = SCN::max_scn(); scn3.ts_ns_ = 9;
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 10;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, scn0, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  scn1 = SCN::min_scn(); scn2.ts_ns_ = 10; scn3 = SCN::max_scn();
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 11;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, scn0, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  scn1 = SCN::max_scn(); scn2.ts_ns_ = 10; scn3.ts_ns_ = 11;
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  upper_version_.reset();
  ASSERT_EQ(OB_SUCCESS, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  scn0.ts_ns_ = 12;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, scn0, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  scn1 = SCN::min_scn(); scn2.ts_ns_ = 12; scn3 = SCN::max_scn();
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 13;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, scn0, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  scn1 = SCN::max_scn(); scn2.ts_ns_ = 12; scn3.ts_ns_ = 13;
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});

  scn0.ts_ns_ = 14;
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, scn0, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  scn1.ts_ns_ = 14; scn2.ts_ns_ = 12; scn3 = SCN::max_scn();
  check_update_upper_version(scn0, std::vector<SCN> {scn1, scn2, scn3});
}

TEST_F(ObAlterMergeEngineTest, test_update_upper_version_invalid_args)
{
  const SCN valid_scn = make_scn(5);
  SCN invalid_scn;
  upper_version_.reset();
  // negative compat merge engine count
  ASSERT_EQ(OB_INVALID_ARGUMENT, upper_version_.update_upper_version(-1, valid_scn, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  // invalid / min / max upper version
  ASSERT_EQ(OB_INVALID_ARGUMENT, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, invalid_scn, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  ASSERT_EQ(OB_INVALID_ARGUMENT, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, SCN::min_scn(), ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  ASSERT_EQ(OB_INVALID_ARGUMENT, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, SCN::max_scn(), ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  // invalid merge engine types
  ASSERT_EQ(OB_INVALID_ARGUMENT, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, valid_scn, ObMergeEngineType::OB_MERGE_ENGINE_UNKNOWN, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  ASSERT_EQ(OB_INVALID_ARGUMENT, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, valid_scn, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_MAX));
  // the array should stay untouched after rejected calls
  ASSERT_EQ(0, upper_version_.get_upper_version_count());
  // merge engine idx exceeds the compat count from old servers
  ASSERT_EQ(OB_ERR_UNEXPECTED, upper_version_.update_upper_version(2, valid_scn, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
}

TEST_F(ObAlterMergeEngineTest, test_get_max_upper_version)
{
  // uninitialized: no history, return min_scn
  upper_version_.reset();
  ASSERT_EQ(0, upper_version_.get_upper_version_count());
  ASSERT_EQ(SCN::min_scn(), upper_version_.get_max_upper_version());

  // initialized but never altered: the max_scn sentinel should be excluded, return min_scn
  ASSERT_EQ(OB_SUCCESS, upper_version_.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  ASSERT_EQ(COMPAT_MERGE_ENGINE_COUNT, upper_version_.get_upper_version_count());
  ASSERT_EQ(SCN::min_scn(), upper_version_.get_max_upper_version());

  // altered: return the max real upper version
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, make_scn(5), ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  ASSERT_EQ(make_scn(5), upper_version_.get_max_upper_version());
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, make_scn(9), ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  ASSERT_EQ(make_scn(9), upper_version_.get_max_upper_version());
  ASSERT_EQ(OB_SUCCESS, upper_version_.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, make_scn(11), ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  ASSERT_EQ(make_scn(11), upper_version_.get_max_upper_version());
}

TEST_F(ObAlterMergeEngineTest, test_inherit_merge_engine_skip)
{
  // case 1: the main table upper version is uninitialized, the aux table should not inherit
  ObMergeEngineUpperVersion main_upper_version;
  ObTableSchema aux_schema;
  aux_schema.set_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  // mimic the bootstrap in ObTableSchema::construct with an empty upper version string
  aux_schema.get_merge_engine_upper_version_for_update().set_original_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  ASSERT_EQ(OB_SUCCESS, ObMergeEngineUtil::inherit_merge_engine(
      aux_schema, main_upper_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  ASSERT_EQ(0, aux_schema.get_merge_engine_upper_version().get_upper_version_count());
  ASSERT_EQ(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, aux_schema.get_merge_engine_type());

  // case 2: the main table is initialized (e.g. by partition exchange) but has no real merge engine
  // switch history, the max upper version is min_scn and the aux table should not inherit
  main_upper_version.reset();
  ASSERT_EQ(OB_SUCCESS, main_upper_version.init_upper_version(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  ASSERT_EQ(SCN::min_scn(), main_upper_version.get_max_upper_version());
  ASSERT_EQ(OB_SUCCESS, ObMergeEngineUtil::inherit_merge_engine(
      aux_schema, main_upper_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  ASSERT_EQ(0, aux_schema.get_merge_engine_upper_version().get_upper_version_count());
  ASSERT_EQ(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, aux_schema.get_merge_engine_type());

  // case 3: the merge engine type of the aux table is unchanged, the aux table should not inherit
  main_upper_version.reset();
  main_upper_version.set_original_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  ASSERT_EQ(OB_SUCCESS, main_upper_version.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, make_scn(5),
      ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  ObTableSchema aux_schema2;
  aux_schema2.set_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  aux_schema2.get_merge_engine_upper_version_for_update().set_original_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  ASSERT_EQ(OB_SUCCESS, ObMergeEngineUtil::inherit_merge_engine(
      aux_schema2, main_upper_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  ASSERT_EQ(0, aux_schema2.get_merge_engine_upper_version().get_upper_version_count());
  ASSERT_EQ(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, aux_schema2.get_merge_engine_type());
}

TEST_F(ObAlterMergeEngineTest, test_inherit_merge_engine_chain)
{
  // the main table alters merge engine PARTIAL_UPDATE -> DELETE_INSERT -> APPEND_ONLY,
  // the aux table inherits after each alter and should stay consistent with the main table
  ObMergeEngineUpperVersion main_upper_version;
  main_upper_version.set_original_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  ASSERT_EQ(OB_SUCCESS, main_upper_version.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, make_scn(5),
      ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));

  ObTableSchema aux_schema;
  aux_schema.set_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  aux_schema.get_merge_engine_upper_version_for_update().set_original_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  ASSERT_EQ(OB_SUCCESS, ObMergeEngineUtil::inherit_merge_engine(
      aux_schema, main_upper_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  check_upper_versions(aux_schema.get_merge_engine_upper_version(),
      std::vector<SCN> {make_scn(5), SCN::max_scn(), SCN::min_scn()});
  ASSERT_EQ(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, aux_schema.get_merge_engine_type());
  check_decide_consistency(main_upper_version, aux_schema.get_merge_engine_upper_version());

  ASSERT_EQ(OB_SUCCESS, main_upper_version.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, make_scn(9),
      ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  ASSERT_EQ(OB_SUCCESS, ObMergeEngineUtil::inherit_merge_engine(
      aux_schema, main_upper_version, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  check_upper_versions(aux_schema.get_merge_engine_upper_version(),
      std::vector<SCN> {make_scn(5), make_scn(9), SCN::max_scn()});
  ASSERT_EQ(ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY, aux_schema.get_merge_engine_type());
  check_decide_consistency(main_upper_version, aux_schema.get_merge_engine_upper_version());
}

TEST_F(ObAlterMergeEngineTest, test_inherit_merge_engine_roundtrip)
{
  // the main table alters merge engine PARTIAL_UPDATE -> DELETE_INSERT -> PARTIAL_UPDATE,
  // the aux table should stay consistent with the main table after the roundtrip
  ObMergeEngineUpperVersion main_upper_version;
  main_upper_version.set_original_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  ASSERT_EQ(OB_SUCCESS, main_upper_version.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, make_scn(5),
      ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));

  ObTableSchema aux_schema;
  aux_schema.set_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  aux_schema.get_merge_engine_upper_version_for_update().set_original_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  ASSERT_EQ(OB_SUCCESS, ObMergeEngineUtil::inherit_merge_engine(
      aux_schema, main_upper_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  ASSERT_EQ(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, aux_schema.get_merge_engine_type());

  ASSERT_EQ(OB_SUCCESS, main_upper_version.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, make_scn(9),
      ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  ASSERT_EQ(OB_SUCCESS, ObMergeEngineUtil::inherit_merge_engine(
      aux_schema, main_upper_version, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE));
  check_upper_versions(aux_schema.get_merge_engine_upper_version(),
      std::vector<SCN> {SCN::max_scn(), make_scn(9), SCN::min_scn()});
  ASSERT_EQ(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, aux_schema.get_merge_engine_type());
  check_decide_consistency(main_upper_version, aux_schema.get_merge_engine_upper_version());
}

TEST_F(ObAlterMergeEngineTest, test_inherit_merge_engine_fts_and_lob)
{
  ObMergeEngineUpperVersion main_upper_version;
  main_upper_version.set_original_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  ASSERT_EQ(OB_SUCCESS, main_upper_version.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, make_scn(5),
      ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));

  // fts index: always force PARTIAL_UPDATE and never inherit upper versions
  ObTableSchema fts_schema;
  fts_schema.set_table_type(share::schema::ObTableType::USER_INDEX);
  fts_schema.set_index_type(share::schema::ObIndexType::INDEX_TYPE_FTS_INDEX_LOCAL);
  fts_schema.set_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  fts_schema.get_merge_engine_upper_version_for_update().set_original_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  ASSERT_EQ(OB_SUCCESS, ObMergeEngineUtil::inherit_merge_engine(
      fts_schema, main_upper_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  ASSERT_EQ(0, fts_schema.get_merge_engine_upper_version().get_upper_version_count());
  ASSERT_EQ(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, fts_schema.get_merge_engine_type());

  // lob aux table: delete-insert should be disabled after inheriting
  ObTableSchema lob_schema;
  lob_schema.set_table_type(share::schema::ObTableType::AUX_LOB_META);
  lob_schema.set_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  lob_schema.get_merge_engine_upper_version_for_update().set_original_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  ASSERT_EQ(OB_SUCCESS, ObMergeEngineUtil::inherit_merge_engine(
      lob_schema, main_upper_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT));
  check_upper_versions(lob_schema.get_merge_engine_upper_version(),
      std::vector<SCN> {make_scn(5), SCN::min_scn(), SCN::min_scn()});
  ASSERT_EQ(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, lob_schema.get_merge_engine_type());

  // lob aux table: append-only can be inherited
  ObMergeEngineUpperVersion main_upper_version2;
  main_upper_version2.set_original_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  ASSERT_EQ(OB_SUCCESS, main_upper_version2.update_upper_version(COMPAT_MERGE_ENGINE_COUNT, make_scn(7),
      ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  ObTableSchema lob_schema2;
  lob_schema2.set_table_type(share::schema::ObTableType::AUX_LOB_META);
  lob_schema2.set_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  lob_schema2.get_merge_engine_upper_version_for_update().set_original_merge_engine_type(ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  ASSERT_EQ(OB_SUCCESS, ObMergeEngineUtil::inherit_merge_engine(
      lob_schema2, main_upper_version2, ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY));
  check_upper_versions(lob_schema2.get_merge_engine_upper_version(),
      std::vector<SCN> {make_scn(7), SCN::min_scn(), SCN::max_scn()});
  ASSERT_EQ(ObMergeEngineType::OB_MERGE_ENGINE_APPEND_ONLY, lob_schema2.get_merge_engine_type());
}

}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("TRACE");
  ::testing::InitGoogleTest(&argc, argv);
  printf("start running test\n");
  return RUN_ALL_TESTS();
}
