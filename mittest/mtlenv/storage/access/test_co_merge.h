/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_TEST_CO_MERGE_H_
#define OB_TEST_CO_MERGE_H_

#include "lib/utility/ob_print_utils.h"
#include "gtest/gtest.h"

namespace oceanbase
{
namespace storage
{
struct ObCOMergeTestConfig
{
  enum class ObCOMergeTestType : uint8_t
  {
    NORMAL = 0, // build merge log then replay
    USE_ROW_TMP_FILE,
    USE_COLUMN_TMP_FILE,
    NORMAL_WITH_BASE_REPLAY, // build merge log then replay
    USE_ROW_TMP_FILE_WITH_BASE_REPLAY,
    USE_COLUMN_TMP_FILE_WITH_BASE_REPLAY,
    MAX_TSET_TYPE
  };

  ObCOMergeTestConfig(const ObCOMergeTestType &type, const int64_t compaction_batch_size)
    : type_(type), compaction_batch_size_(compaction_batch_size)
  {}
  TO_STRING_KV("type", type_to_string(type_), K_(compaction_batch_size));

  bool need_replay_base() const
  {
    return ObCOMergeTestType::NORMAL_WITH_BASE_REPLAY <= type_ && ObCOMergeTestType::MAX_TSET_TYPE > type_;
  }
  bool is_normal_test_type() const
  {
    return ObCOMergeTestType::NORMAL == type_ || ObCOMergeTestType::NORMAL_WITH_BASE_REPLAY == type_;
  }
  bool is_column_tmp_file_test_type() const
  {
    return ObCOMergeTestType::USE_COLUMN_TMP_FILE == type_ || ObCOMergeTestType::USE_COLUMN_TMP_FILE_WITH_BASE_REPLAY == type_;
  }
  bool is_batch_merge_test_type() const
  {
    return compaction_batch_size_ > 1;
  }
  static const char *type_to_string(const ObCOMergeTestType &type);

  ObCOMergeTestType type_;
  int64_t compaction_batch_size_;
};

const static char * ObCOMergeTestTypeStr[] = {
    "NORMAL",
    "USE_ROW_TMP_FILE",
    "USE_COLUMN_TMP_FILE",
    "NORMAL_WITH_BASE_REPLAY",
    "USE_ROW_TMP_FILE_WITH_BASE_REPLAY",
    "USE_COLUMN_TMP_FILE_WITH_BASE_REPLAY"
};

const char *ObCOMergeTestConfig::type_to_string(const ObCOMergeTestType &type)
{
  STATIC_ASSERT(static_cast<int64_t>(ObCOMergeTestType::MAX_TSET_TYPE) == ARRAYSIZEOF(ObCOMergeTestTypeStr), "ob comerge test type str len is mismatch");
  return ObCOMergeTestTypeStr[static_cast<int64_t>(type)];
}

class ObCOMergeTestUtil
{
public:
  static std::string custom_test_name(const ::testing::TestParamInfo<ObCOMergeTestConfig> &info)
  {
    std::string name = info.param.type_to_string(info.param.type_);
    if (info.param.is_batch_merge_test_type()) {
      name += "_";
      name += std::to_string(info.param.compaction_batch_size_);
    }
    return name;
  }
  static void generate_configs(std::vector<ObCOMergeTestConfig> &configs, const ObCOMergeTestConfig &config)
  {
    configs.push_back(config);
  }
};
} // end namespace storage
} // end namespace oceanbase
#endif // OB_TEST_CO_MERGE_H_