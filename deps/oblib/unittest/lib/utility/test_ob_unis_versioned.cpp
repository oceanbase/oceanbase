/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 */

#include <gtest/gtest.h>
#include <vector>
#include "lib/utility/ob_unis_versioned.h"

using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

namespace unis_test
{
struct Original
{
  OB_UNIS_VERSION(1);

public:
  int64_t value_ = 11;
  int decode_calls_ = 0;
};
OB_DEF_SERIALIZE(Original)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(value_);
  return ret;
}
OB_DEF_DESERIALIZE(Original)
{
  int ret = OB_SUCCESS;
  ++decode_calls_;
  OB_UNIS_DECODE(value_);
  return ret;
}
OB_DEF_SERIALIZE_SIZE(Original)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(value_);
  return len;
}

struct Second
{
  OB_UNIS_VERSION(2);

public:
  int64_t value_ = 22;
};
OB_SERIALIZE_MEMBER(Second, value_);

struct Third
{
  OB_UNIS_VERSION(3);

public:
  int64_t value_ = 33;
  int32_t extra_ = 7;
};
OB_SERIALIZE_MEMBER(Third, value_, extra_);

struct History
{
  OB_UNIS_VERSION(3);

public:
  int64_t value_ = 44;
};
OB_SERIALIZE_MEMBER(History, value_);
struct Fourth
{
  OB_UNIS_VERSION(4);

public:
  int64_t value_ = 55;
};
OB_SERIALIZE_MEMBER(Fourth, value_);

template <int64_t Version>
struct Numbered
{
  OB_UNIS_VERSION(Version);

public:
  int64_t value_ = 0;
};
OB_SERIALIZE_MEMBER_TEMP(template <int64_t Version>, Numbered<Version>, value_);
} // namespace unis_test

namespace oceanbase
{
namespace common
{
namespace serialization
{
template <>
struct UnisOriginalRecord<unis_test::History>
{
  static constexpr bool accepts(const int64_t version)
  {
    return version >= 1 && version <= 3;
  }
  static int decode_body(unis_test::History &value, const int64_t version, DESERIAL_PARAMS)
  {
    UNUSED(version);
    return value.deserialize_(buf, data_len, pos);
  }
};
} // namespace serialization
} // namespace common
} // namespace oceanbase

OB_UNIS_SER_REGISTER_TYPES(unis_test::Original, unis_test::Second, 2, unis_test::Third, 3);
OB_UNIS_SER_REGISTER_TYPES(unis_test::History, unis_test::Fourth, 4);
OB_UNIS_SER_REGISTER_TYPES(unis_test::Numbered<1>, unis_test::Numbered<2>, 2, unis_test::Numbered<3>, 3,
                           unis_test::Numbered<4>, 4,
               unis_test::Numbered<5>, 5, unis_test::Numbered<6>, 6, unis_test::Numbered<7>, 7, unis_test::Numbered<8>,
               8, unis_test::Numbered<9>, 9);

#if defined(OB_UNIS_TEST_MISSING_INSTANCE)
void invalid_record(unis_test::Original &original, unis_test::Second &second)
{
  DEF_UNIS_VERSION_DECODE_REC(record, original, second);
}
#elif defined(OB_UNIS_TEST_WRONG_ORDER)
void invalid_record(unis_test::Original &original, unis_test::Second &second, unis_test::Third &third)
{
  DEF_UNIS_VERSION_DECODE_REC(record, original, third, second);
}
#elif defined(OB_UNIS_TEST_DUPLICATE_VERSION)
void invalid_record(unis_test::Original &original, unis_test::Second &second)
{
  UnisRecordRef<unis_test::Original, unis_test::Second, unis_test::Second> record(original, second, second);
}
#elif defined(OB_UNIS_TEST_TEMPORARY_DESERIALIZE)
void invalid_record(unis_test::Original &original, unis_test::Third &third)
{
  DEF_UNIS_VERSION_DECODE_REC(record, original, unis_test::Second{}, third);
}
#elif defined(OB_UNIS_TEST_ODD_ARGUMENTS)
OB_UNIS_SER_REGISTER_TYPES(unis_test::Fourth, unis_test::Second, 2, unis_test::Third);
#elif defined(OB_UNIS_TEST_TOO_MANY_ARGUMENTS)
OB_UNIS_SER_REGISTER_TYPES(unis_test::Fourth, unis_test::Numbered<1>, 1, unis_test::Numbered<2>, 2,
                           unis_test::Numbered<3>, 3,
               unis_test::Numbered<4>, 4, unis_test::Numbered<5>, 5, unis_test::Numbered<6>, 6, unis_test::Numbered<7>,
               7, unis_test::Numbered<8>, 8, unis_test::Numbered<9>, 9);
#elif defined(OB_UNIS_TEST_DECLARED_VERSION)
OB_UNIS_SER_REGISTER_TYPE(unis_test::Fourth, unis_test::Second, 7);
#elif defined(OB_UNIS_TEST_DUPLICATE_BINDING)
OB_UNIS_SER_REGISTER_TYPE(unis_test::Original, unis_test::Second, 2);
#elif defined(OB_UNIS_TEST_ORIGINAL_OVERLAP)
OB_UNIS_SER_REGISTER_TYPES(unis_test::Third, unis_test::Numbered<3>, 3);
void invalid_record(unis_test::Third &original, unis_test::Numbered<3> &third)
{
  DEF_UNIS_VERSION_DECODE_REC(record, original, third);
}
#endif

template <typename T>
static std::vector<char> encode_record(const T &value)
{
  std::vector<char> bytes(value.get_serialize_size());
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, value.serialize(bytes.data(), bytes.size(), pos));
  EXPECT_EQ(bytes.size(), pos);
  return bytes;
}

TEST(TestUnisVersioned, encoding_preserves_envelopes_and_size)
{
  const unis_test::Second second;
  const unis_test::Third third;
  const std::vector<char> second_bytes = {2, char(0x81), char(0x80), char(0x80), char(0x80), 0, 22};
  const std::vector<char> third_bytes = {3, char(0x82), char(0x80), char(0x80), char(0x80), 0, 33, 7};
  EXPECT_EQ(second_bytes, encode_record(second));
  EXPECT_EQ(third_bytes, encode_record(third));
}

TEST(TestUnisVersioned, selects_only_the_matching_instance)
{
  const unis_test::Original source;
  const unis_test::Second second_source;
  const unis_test::Third third_source;
  std::vector<std::vector<char>> inputs{encode_record(source), encode_record(second_source),
                                        encode_record(third_source)};
  for (int64_t version = 1; version <= 3; ++version) {
    unis_test::Original original;
    unis_test::Second second;
    unis_test::Third third;
    original.value_ = second.value_ = third.value_ = -1;
    DEF_UNIS_VERSION_DECODE_REC(record, original, second, third);
    EXPECT_EQ(-1, record.version());
    int64_t pos = 0;
    const auto &bytes = inputs[version - 1];
    ASSERT_EQ(OB_SUCCESS, record.deserialize(bytes.data(), bytes.size(), pos));
    EXPECT_EQ(bytes.size(), pos);
    EXPECT_TRUE(record.decoded());
    EXPECT_EQ(version, record.version());
    EXPECT_EQ(version == 1 ? 11 : -1, original.value_);
    EXPECT_EQ(version == 2 ? 22 : -1, second.value_);
    EXPECT_EQ(version == 3 ? 33 : -1, third.value_);
  }
}

TEST(TestUnisVersioned, parsed_empty_body_still_calls_decoder)
{
  unis_test::Original original;
  unis_test::Second second;
  unis_test::Third third;
  DEF_UNIS_VERSION_DECODE_REC(record, original, second, third);
  const char bytes[] = {
    1, static_cast<char>(0x80), static_cast<char>(0x80), static_cast<char>(0x80), static_cast<char>(0x80), 0};
  int64_t pos = 0;
  UnisRecordView view;
  ASSERT_EQ(OB_SUCCESS, view.decode(bytes, sizeof(bytes), pos));
  ASSERT_EQ(sizeof(bytes), pos);
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE_PARSED(record, view);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1, original.decode_calls_);
  EXPECT_EQ(11, original.value_);
  EXPECT_EQ(1, record.version());
}

TEST(TestUnisVersioned, failed_read_resets_selection_and_stays_bounded)
{
  unis_test::Original original;
  unis_test::Second second;
  unis_test::Third third;
  DEF_UNIS_VERSION_DECODE_REC(record, original, second, third);
  auto bytes = encode_record(third);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, record.deserialize(bytes.data(), bytes.size(), pos));
  bytes[0] = 9;
  pos = 0;
  EXPECT_EQ(OB_NOT_SUPPORTED, record.deserialize(bytes.data(), bytes.size(), pos));
  EXPECT_EQ(1 + OB_SERIALIZE_SIZE_NEED_BYTES, pos);
  EXPECT_FALSE(record.decoded());
  EXPECT_EQ(-1, record.version());
  bytes[0] = 3;
  for (int64_t len = 0; len < bytes.size(); ++len) {
    pos = 0;
    EXPECT_NE(OB_SUCCESS, record.deserialize(bytes.data(), len, pos));
    EXPECT_LE(pos, len);
    EXPECT_FALSE(record.decoded());
  }
}

TEST(TestUnisVersioned, expected_version_rejects_other_registered_and_original_versions)
{
  unis_test::Original original;
  unis_test::Second second;
  unis_test::Third third;
  DEF_UNIS_VERSION_DECODE_REC(record, original, second, third);
  auto bytes = encode_record(second);
  int64_t pos = 0;
  EXPECT_EQ(OB_NOT_SUPPORTED, record.deserialize_expected<3>(bytes.data(), bytes.size(), pos));
  EXPECT_FALSE(record.decoded());
  bytes = encode_record(original);
  pos = 0;
  EXPECT_EQ(OB_NOT_SUPPORTED, record.deserialize_expected<3>(bytes.data(), bytes.size(), pos));
  bytes = encode_record(third);
  pos = 0;
  const char *buf = bytes.data();
  const int64_t data_len = bytes.size();
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE_EXPECTED(record, 3);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(3, record.version());
}

TEST(TestUnisVersioned, expected_version_error_precedes_invalid_body_extent)
{
  unis_test::Original original;
  unis_test::Second second;
  unis_test::Third third;
  DEF_UNIS_VERSION_DECODE_REC(record, original, second, third);
  const char bytes[] = {2, 127};
  int64_t pos = 0;
  EXPECT_EQ(OB_NOT_SUPPORTED, record.deserialize_expected<3>(bytes, sizeof(bytes), pos));
  EXPECT_EQ(sizeof(bytes), pos);
  EXPECT_FALSE(record.decoded());
  pos = 0;
  EXPECT_EQ(OB_DESERIALIZE_ERROR, record.deserialize(bytes, sizeof(bytes), pos));
  EXPECT_EQ(sizeof(bytes), pos);
  EXPECT_FALSE(record.decoded());
}

TEST(TestUnisVersioned, original_history_reports_actual_version)
{
  unis_test::History original;
  unis_test::Fourth fourth;
  auto bytes = encode_record(original);
  for (int64_t version = 1; version <= 3; ++version) {
    bytes[0] = static_cast<char>(version);
    original.value_ = 0;
    DEF_UNIS_VERSION_DECODE_REC(record, original, fourth);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, record.deserialize_original(bytes.data(), bytes.size(), pos));
    EXPECT_EQ(version, record.version());
    EXPECT_EQ(44, original.value_);
  }
  DEF_UNIS_VERSION_DECODE_REC(record, original, fourth);
  bytes[0] = 4;
  int64_t pos = 0;
  EXPECT_EQ(OB_NOT_SUPPORTED, record.deserialize_original(bytes.data(), bytes.size(), pos));
  EXPECT_FALSE(record.decoded());
}

TEST(TestUnisVersioned, eight_registered_versions_use_the_complete_candidate_set)
{
  unis_test::Numbered<1> original;
  unis_test::Numbered<2> second;
  unis_test::Numbered<3> third;
  unis_test::Numbered<4> fourth;
  unis_test::Numbered<5> fifth;
  unis_test::Numbered<6> sixth;
  unis_test::Numbered<7> seventh;
  unis_test::Numbered<8> eighth;
  unis_test::Numbered<9> ninth;
  ninth.value_ = 91;
  auto bytes = encode_record(ninth);
  DEF_UNIS_VERSION_DECODE_REC(record, original, second, third, fourth, fifth, sixth, seventh, eighth, ninth);
  for (int64_t version = 2; version <= 9; ++version) {
    bytes[0] = static_cast<char>(version);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, record.deserialize(bytes.data(), bytes.size(), pos));
    EXPECT_EQ(version, record.version());
    EXPECT_EQ(version, record.result().index());
  }
  EXPECT_EQ(91, second.value_);
  EXPECT_EQ(91, third.value_);
  EXPECT_EQ(91, fourth.value_);
  EXPECT_EQ(91, fifth.value_);
  EXPECT_EQ(91, sixth.value_);
  EXPECT_EQ(91, seventh.value_);
  EXPECT_EQ(91, eighth.value_);
  EXPECT_EQ(91, ninth.value_);
}

TEST(TestUnisVersioned, decode_macros_skip_absent_record)
{
  unis_test::Original original;
  unis_test::Second second;
  unis_test::Third third;
  DEF_UNIS_VERSION_DECODE_REC(record, original, second, third);
  const char *buf = "";
  const int64_t data_len = 0;
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(record);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_FALSE(record.decoded());
  OB_UNIS_DECODE_REQUIRED(record);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_FALSE(record.decoded());
  EXPECT_EQ(0, pos);
  EXPECT_EQ(0, original.decode_calls_);
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
