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

#include <gtest/gtest.h>
#include <cstring>
#include <string>
#include <type_traits>
#include <vector>
#include "lib/unittest_diagnostic_info_util.h"
#include "rpc/obrpc/ob_rpc_endec.h"
#include "rpc/obrpc/ob_rpc_proxy.h"

namespace oceanbase
{
namespace obrpc
{
namespace
{
int64_t rpc_packet_size_limit_for_test = common::OB_MAX_RPC_PACKET_LENGTH;
}

// Override oblib's weak default without changing the production configuration API.
int64_t get_max_rpc_packet_size()
{
  return rpc_packet_size_limit_for_test;
}

namespace
{
using namespace common;

static_assert(!std::is_copy_constructible<ObRpcPreparedBody>::value,
              "prepared RPC body must have a single owner");
static_assert(!std::is_copy_assignable<ObRpcPreparedBody>::value,
              "prepared RPC body must not copy its owned allocation");

struct CountingRequest
{
  CountingRequest() : bytes_(32 * 1024, 'a'), serialize_count_(0), serialize_ret_(OB_SUCCESS) {}

  int serialize(char *buf, const int64_t len, int64_t &pos) const
  {
    ++serialize_count_;
    int ret = serialize_ret_;
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(buf) || pos < 0 || len - pos < get_serialize_size()) {
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        MEMCPY(buf + pos, bytes_.data(), bytes_.size());
        pos += bytes_.size();
      }
    }
    return ret;
  }

  int64_t get_serialize_size() const { return bytes_.size(); }

  std::string bytes_;
  mutable int64_t serialize_count_;
  int serialize_ret_;
};

// Counts real LZ4 calls and injects no-gain or failure results for reuse tests.
class CountingCompressor : public ObCompressor
{
public:
  enum Mode { COMPRESS, NO_GAIN, FAIL_COMPRESS, FAIL_OVERFLOW,
              ZERO_OUTPUT, NEGATIVE_OUTPUT, EXPANDED_OUTPUT };

  explicit CountingCompressor(Mode mode = COMPRESS)
      : mode_(mode), compress_count_(0), overflow_count_(0), compressor_() {}

  int compress(const char *src, const int64_t src_size, char *dst,
               const int64_t dst_capacity, int64_t &dst_size) override
  {
    ++compress_count_;
    int ret = OB_SUCCESS;
    if (FAIL_COMPRESS == mode_) {
      ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    } else if (ZERO_OUTPUT == mode_) {
      dst_size = 0;
    } else if (NEGATIVE_OUTPUT == mode_) {
      dst_size = -1;
    } else if (EXPANDED_OUTPUT == mode_) {
      dst_size = src_size + 1;
    } else if (NO_GAIN == mode_) {
      if (dst_capacity < src_size) {
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        MEMCPY(dst, src, src_size);
        dst_size = src_size;
      }
    } else {
      ret = compressor_.compress(src, src_size, dst, dst_capacity, dst_size);
    }
    return ret;
  }

  int decompress(const char *src, const int64_t src_size, char *dst,
                 const int64_t dst_capacity, int64_t &dst_size) override
  {
    return compressor_.decompress(src, src_size, dst, dst_capacity, dst_size);
  }

  int get_max_overflow_size(const int64_t src_size, int64_t &overflow) const override
  {
    int ret = OB_SUCCESS;
    ++overflow_count_;
    if (FAIL_OVERFLOW == mode_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = compressor_.get_max_overflow_size(src_size, overflow);
    }
    return ret;
  }

  const char *get_compressor_name() const override { return compressor_.get_compressor_name(); }
  ObCompressorType get_compressor_type() const override { return LZ4_COMPRESSOR; }

  Mode mode_;
  int64_t compress_count_;
  mutable int64_t overflow_count_;

private:
  ObLZ4Compressor compressor_;
};

class OverflowCompressor : public CountingCompressor
{
public:
  explicit OverflowCompressor(int64_t overflow_size) : overflow_size_(overflow_size) {}
  int get_max_overflow_size(const int64_t src_size, int64_t &overflow) const override
  {
    UNUSED(src_size);
    ++overflow_count_;
    overflow = overflow_size_;
    return OB_SUCCESS;
  }
private:
  int64_t overflow_size_;
};

class MarkerExtraPayload : public ObIRpcExtraPayload
{
public:
  int64_t get_serialize_size() const override { return sizeof(int64_t); }
  int serialize(char *buf, const int64_t len, int64_t &pos) const override
  {
    return serialization::encode_i64(buf, len, pos, 0x123456789abcdef);
  }
  int deserialize(const char *buf, const int64_t len, int64_t &pos) override
  {
    int64_t marker = 0;
    return serialization::decode_i64(buf, len, pos, &marker);
  }
};

class SizedExtraPayload : public MarkerExtraPayload
{
public:
  explicit SizedExtraPayload(int64_t estimated_size, int serialize_ret = OB_SUCCESS)
      : estimated_size_(estimated_size), serialize_ret_(serialize_ret), serialize_count_(0) {}
  int64_t get_serialize_size() const override { return estimated_size_; }
  int serialize(char *buf, const int64_t len, int64_t &pos) const override
  {
    ++serialize_count_;
    int ret = serialize_ret_;
    if (OB_SUCC(ret)) {
      ret = MarkerExtraPayload::serialize(buf, len, pos);
    }
    return ret;
  }
  int64_t estimated_size_;
  int serialize_ret_;
  mutable int64_t serialize_count_;
};

// Temporarily replaces process-wide runtime payload state and restores it on scope exit.
// Only use this guard in serial tests because the state is shared across threads.
class ScopedRuntimePayload
{
public:
  ScopedRuntimePayload(bool enabled, ObIRpcExtraPayload &payload)
      : old_enabled_(lib::g_runtime_enabled), old_payload_(ObIRpcExtraPayload::instance())
  {
    lib::g_runtime_enabled = enabled;
    ObIRpcExtraPayload::set_extra_payload(payload);
  }
  ~ScopedRuntimePayload()
  {
    ObIRpcExtraPayload::set_extra_payload(old_payload_);
    lib::g_runtime_enabled = old_enabled_;
  }
private:
  bool old_enabled_;
  ObIRpcExtraPayload &old_payload_;
};

class ScopedRpcPacketLimit
{
public:
  explicit ScopedRpcPacketLimit(int64_t limit)
      : old_limit_(rpc_packet_size_limit_for_test)
  {
    rpc_packet_size_limit_for_test = limit;
  }
  ~ScopedRpcPacketLimit() { rpc_packet_size_limit_for_test = old_limit_; }
private:
  int64_t old_limit_;
};

class TestRpcPreparedBody : public ::testing::Test
{
protected:
  static constexpr uint64_t TENANT_ID = 1002;

  int prepare(ObRpcPreparedBody &body, const CountingRequest &req, CountingCompressor &compressor)
  {
    return body.prepare(req, OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR, &compressor);
  }

  void check_plain_fallback(CountingCompressor::Mode mode)
  {
    CountingRequest req;
    CountingCompressor compressor(mode);
    ObRpcPreparedBody body;
    const int64_t estimated_size = req.get_serialize_size() + calc_extra_payload_size();
    ObLZ4Compressor reference_compressor;
    int64_t overflow_size = 0;
    ASSERT_EQ(OB_SUCCESS, reference_compressor.get_max_overflow_size(estimated_size, overflow_size));
    for (int i = 0; i < 3; ++i) {
      ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
      EXPECT_EQ(estimated_size + overflow_size, body.get_payload_size());
    }
    ASSERT_TRUE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR));
    EXPECT_FALSE(body.is_compressed());
    EXPECT_EQ(1, req.serialize_count_);
    EXPECT_EQ(1, compressor.compress_count_);
    EXPECT_EQ(1, compressor.overflow_count_);
    ASSERT_GE(body.get_size(), req.get_serialize_size());
    EXPECT_EQ(body.get_original_size(), body.get_size());
    EXPECT_EQ(0, MEMCMP(body.get_data(), req.bytes_.data(), req.bytes_.size()));

    std::vector<char> destination(body.get_size());
    ObRpcPacket packet;
    ASSERT_EQ(OB_SUCCESS, body.fill_packet(packet, destination.data(), destination.size()));
    EXPECT_EQ(INVALID_COMPRESSOR, packet.get_compressor_type());
    EXPECT_EQ(0, packet.get_original_len());
    EXPECT_EQ(body.get_size(), packet.get_clen());
    EXPECT_EQ(0, MEMCMP(packet.get_cdata(), req.bytes_.data(), req.bytes_.size()));
  }
};

constexpr uint64_t TestRpcPreparedBody::TENANT_ID;

TEST_F(TestRpcPreparedBody, three_destinations_prepare_and_compress_once)
{
  CountingRequest req;
  CountingCompressor compressor;
  ObRpcPreparedBody body;
  const int64_t estimated_size = req.get_serialize_size() + calc_extra_payload_size();
  ObLZ4Compressor reference_compressor;
  int64_t overflow_size = 0;
  ASSERT_EQ(OB_SUCCESS, reference_compressor.get_max_overflow_size(estimated_size, overflow_size));
  ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
  ASSERT_TRUE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR));
  ASSERT_TRUE(body.is_compressed());
  const char *const prepared_data = body.get_data();

  for (int i = 0; i < 3; ++i) {
    if (i > 0) {
      ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
    }
    EXPECT_EQ(prepared_data, body.get_data());
    EXPECT_EQ(estimated_size + overflow_size, body.get_payload_size());
    std::vector<char> destination(body.get_size());
    ObRpcPacket packet;
    ASSERT_EQ(OB_SUCCESS, body.fill_packet(packet, destination.data(), destination.size()));
    EXPECT_EQ(LZ4_COMPRESSOR, packet.get_compressor_type());
    EXPECT_EQ(body.get_original_size(), packet.get_original_len());
    EXPECT_EQ(body.get_size(), packet.get_clen());
    EXPECT_EQ(0, MEMCMP(prepared_data, destination.data(), destination.size()));
  }
  EXPECT_EQ(1, req.serialize_count_);
  EXPECT_EQ(1, compressor.compress_count_);
  EXPECT_EQ(1, compressor.overflow_count_);
}

TEST_F(TestRpcPreparedBody, next_fanout_uses_new_context)
{
  CountingRequest req;
  CountingCompressor compressor;
  for (int i = 0; i < 2; ++i) {
    ObRpcPreparedBody body;
    ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
    ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
  }
  EXPECT_EQ(2, req.serialize_count_);
  EXPECT_EQ(2, compressor.compress_count_);
}

TEST_F(TestRpcPreparedBody, no_gain_reuses_plain_body)
{
  check_plain_fallback(CountingCompressor::NO_GAIN);
}

TEST_F(TestRpcPreparedBody, compression_failure_reuses_plain_body)
{
  check_plain_fallback(CountingCompressor::FAIL_COMPRESS);
}

TEST_F(TestRpcPreparedBody, copied_body_outlives_context_and_destinations_are_independent)
{
  CountingRequest req;
  const std::string original_request = req.bytes_;
  CountingCompressor compressor;
  std::vector<char> destination1;
  std::vector<char> destination2;
  ObRpcPacket packet1;
  ObRpcPacket packet2;
  int64_t original_size = 0;
  {
    ObRpcPreparedBody body;
    ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
    ASSERT_TRUE(body.is_compressed());
    original_size = body.get_original_size();
    destination1.resize(body.get_size());
    destination2.resize(body.get_size());
    ASSERT_EQ(OB_SUCCESS, body.fill_packet(packet1, destination1.data(), destination1.size()));
    ASSERT_EQ(OB_SUCCESS, body.fill_packet(packet2, destination2.data(), destination2.size()));
    EXPECT_NE(body.get_data(), packet1.get_cdata());
    EXPECT_NE(body.get_data(), packet2.get_cdata());
    EXPECT_NE(packet1.get_cdata(), packet2.get_cdata());
    destination1[0] ^= 1;
    EXPECT_EQ(0, MEMCMP(body.get_data(), destination2.data(), destination2.size()));
  }
  // Decode the second request after the shared body owner has been destroyed.
  std::vector<char> decoded(original_size);
  int64_t decoded_size = 0;
  ASSERT_EQ(OB_SUCCESS, compressor.decompress(packet2.get_cdata(), packet2.get_clen(),
                                            decoded.data(), decoded.size(), decoded_size));
  ASSERT_EQ(original_size, decoded_size);
  ASSERT_GE(decoded_size, req.get_serialize_size());
  EXPECT_EQ(0, MEMCMP(decoded.data(), original_request.data(), original_request.size()));
  EXPECT_EQ(original_request, req.bytes_);
}

TEST_F(TestRpcPreparedBody, key_mismatch_is_not_reusable)
{
  CountingRequest req;
  CountingCompressor compressor;
  ObRpcPreparedBody body;
  ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
  const char *const prepared_data = body.get_data();
  EXPECT_TRUE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR));
  EXPECT_FALSE(body.matches(OB_LOG_PREPARE_REQ, TENANT_ID, LZ4_COMPRESSOR));
  EXPECT_FALSE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID + 1, LZ4_COMPRESSOR));
  EXPECT_FALSE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID, ZSTD_COMPRESSOR));
  EXPECT_EQ(OB_STATE_NOT_MATCH,
            body.prepare(req, OB_LOG_PREPARE_REQ, TENANT_ID, LZ4_COMPRESSOR, &compressor));
  EXPECT_EQ(OB_STATE_NOT_MATCH,
            body.prepare(req, OB_LOG_PUSH_REQ, TENANT_ID + 1, LZ4_COMPRESSOR, &compressor));
  EXPECT_EQ(OB_STATE_NOT_MATCH,
            body.prepare(req, OB_LOG_PUSH_REQ, TENANT_ID, ZSTD_COMPRESSOR, &compressor));
  EXPECT_TRUE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR));
  EXPECT_EQ(prepared_data, body.get_data());
  EXPECT_EQ(OB_SUCCESS, prepare(body, req, compressor));
  EXPECT_EQ(1, req.serialize_count_);
  EXPECT_EQ(1, compressor.compress_count_);
}

TEST_F(TestRpcPreparedBody, non_common_compression_modes_bypass_without_work)
{
  const ObCompressorType modes[] = {INVALID_COMPRESSOR, NONE_COMPRESSOR, STREAM_LZ4_COMPRESSOR};
  for (const ObCompressorType mode : modes) {
    CountingRequest req;
    CountingCompressor compressor;
    ObRpcPreparedBody body;
    EXPECT_EQ(OB_NOT_SUPPORTED, body.prepare(req, OB_LOG_PUSH_REQ, TENANT_ID, mode, &compressor));
    EXPECT_EQ(OB_NOT_SUPPORTED, prepare(body, req, compressor));
    EXPECT_EQ(0, req.serialize_count_);
    EXPECT_EQ(0, compressor.compress_count_);
  }
}

TEST_F(TestRpcPreparedBody, preparation_failure_bypasses_later_attempts)
{
  CountingRequest req;
  CountingCompressor compressor(CountingCompressor::FAIL_OVERFLOW);
  ObRpcPreparedBody body;
  EXPECT_NE(OB_SUCCESS, prepare(body, req, compressor));
  const int64_t serialized = req.serialize_count_;
  const int64_t overflow_calls = compressor.overflow_count_;
  compressor.mode_ = CountingCompressor::COMPRESS;
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(OB_NOT_SUPPORTED, prepare(body, req, compressor));
  }
  EXPECT_EQ(serialized, req.serialize_count_);
  EXPECT_EQ(overflow_calls, compressor.overflow_count_);
  EXPECT_EQ(0, compressor.compress_count_);
}

TEST_F(TestRpcPreparedBody, serialization_failure_does_not_publish_partial_body)
{
  CountingRequest req;
  req.serialize_ret_ = OB_SERIALIZE_ERROR;
  CountingCompressor compressor;
  ObRpcPreparedBody body;
  EXPECT_NE(OB_SUCCESS, prepare(body, req, compressor));
  EXPECT_EQ(1, req.serialize_count_);
  EXPECT_EQ(0, compressor.compress_count_);
  req.serialize_ret_ = OB_SUCCESS;
  EXPECT_EQ(OB_NOT_SUPPORTED, prepare(body, req, compressor));
  EXPECT_EQ(1, req.serialize_count_);
  EXPECT_EQ(0, compressor.compress_count_);
}

TEST_F(TestRpcPreparedBody, undersized_destination_does_not_damage_reusable_body)
{
  CountingRequest req;
  CountingCompressor compressor;
  ObRpcPreparedBody body;
  ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
  ASSERT_GT(body.get_size(), 0);
  ASSERT_TRUE(OB_NOT_NULL(body.get_data()));
  const int64_t body_size = body.get_size();
  const char *const prepared_data = body.get_data();
  const std::string prepared_bytes(prepared_data, body_size);
  const char guard_byte = 'Z';
  std::vector<char> destination(body_size + 3, guard_byte);
  const std::vector<char> untouched = destination;

  ObRpcPacket null_packet;
  EXPECT_EQ(OB_INVALID_ARGUMENT, body.fill_packet(null_packet, nullptr, body_size));
  const int64_t invalid_capacities[] = {-1, 0, body_size - 1};
  for (const int64_t capacity : invalid_capacities) {
    ObRpcPacket packet;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              body.fill_packet(packet, destination.data() + 1, capacity));
    EXPECT_EQ(untouched, destination);
  }

  const int64_t valid_capacities[] = {body_size, body_size + 1};
  for (const int64_t capacity : valid_capacities) {
    MEMSET(destination.data(), guard_byte, destination.size());
    ObRpcPacket packet;
    ASSERT_EQ(OB_SUCCESS,
              body.fill_packet(packet, destination.data() + 1, capacity));
    EXPECT_EQ(body_size, packet.get_clen());
    EXPECT_EQ(destination.data() + 1, packet.get_cdata());
    EXPECT_EQ(0, MEMCMP(prepared_bytes.data(), packet.get_cdata(), body_size));
    EXPECT_EQ(guard_byte, destination.front());
    // Preserve both the first unused byte and the guard beyond the supplied capacity.
    EXPECT_EQ(guard_byte, destination[body_size + 1]);
    EXPECT_EQ(guard_byte, destination[body_size + 2]);
  }

  ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
  EXPECT_EQ(prepared_data, body.get_data());
  EXPECT_EQ(body_size, body.get_size());
  EXPECT_EQ(0, MEMCMP(prepared_bytes.data(), body.get_data(), body_size));
  EXPECT_EQ(1, req.serialize_count_);
  EXPECT_EQ(1, compressor.compress_count_);
}

TEST_F(TestRpcPreparedBody, extra_payload_bytes_and_flags_match_existing_encoder)
{
  ASSERT_FALSE(OBTRACE->is_inited());
  MarkerExtraPayload extra;
  for (const bool runtime_enabled : {false, true}) {
    ScopedRuntimePayload guard(runtime_enabled, extra);
    for (const CountingCompressor::Mode mode :
         {CountingCompressor::COMPRESS, CountingCompressor::NO_GAIN}) {
      CountingRequest req;
      CountingCompressor compressor(mode);
      std::vector<char> expected(req.get_serialize_size() + calc_extra_payload_size());
      ObRpcPacket expected_metadata;
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, serialization::encode(expected.data(), expected.size(), pos, req));
      ASSERT_EQ(OB_SUCCESS,
                fill_extra_payload(expected_metadata, expected.data(), expected.size(), pos));
      expected.resize(pos);
      ASSERT_GT(pos, req.get_serialize_size());
      req.serialize_count_ = 0;

      ObRpcPreparedBody body;
      ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
      ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
      std::vector<char> destination(body.get_size());
      ObRpcPacket packet;
      ASSERT_EQ(OB_SUCCESS, body.fill_packet(packet, destination.data(), destination.size()));
      EXPECT_EQ(expected_metadata.has_context(), packet.has_context());
      EXPECT_EQ(expected_metadata.has_disable_debugsync(), packet.has_disable_debugsync());
      EXPECT_EQ(expected_metadata.has_trace_info(), packet.has_trace_info());
      EXPECT_EQ(runtime_enabled, packet.has_context());
      EXPECT_EQ(runtime_enabled, packet.has_disable_debugsync());

      std::vector<char> decoded(body.get_original_size());
      int64_t decoded_size = body.get_size();
      if (body.is_compressed()) {
        ASSERT_EQ(OB_SUCCESS, compressor.decompress(packet.get_cdata(), packet.get_clen(),
                                                  decoded.data(), decoded.size(), decoded_size));
      } else {
        MEMCPY(decoded.data(), packet.get_cdata(), packet.get_clen());
      }
      ASSERT_EQ(expected.size(), decoded_size);
      EXPECT_EQ(0, MEMCMP(expected.data(), decoded.data(), expected.size()));
      EXPECT_EQ(1, req.serialize_count_);
      EXPECT_EQ(1, compressor.compress_count_);
    }
  }
}

TEST_F(TestRpcPreparedBody, initialized_trace_bypasses_without_preparing_body)
{
  // This standalone test owns no pre-existing trace/span to discard.
  ASSERT_FALSE(OBTRACE->is_inited());
  CountingRequest req;
  CountingCompressor compressor;
  ObRpcPreparedBody body;
  {
    struct TraceGuard
    {
      TraceGuard()
      {
        trace::UUID trace_id;
        trace_id.low_ = 1;
        OBTRACE->init(trace_id, trace::UUID(), 0);
      }
      ~TraceGuard() { OBTRACE->reset(); }
    } guard;
    ASSERT_TRUE(OBTRACE->is_inited());
    EXPECT_EQ(OB_NOT_SUPPORTED, prepare(body, req, compressor));
    EXPECT_EQ(0, req.serialize_count_);
    EXPECT_EQ(0, compressor.compress_count_);
  }
  EXPECT_FALSE(OBTRACE->is_inited());
  EXPECT_EQ(OB_NOT_SUPPORTED, prepare(body, req, compressor));
  ObRpcPreparedBody next_body;
  EXPECT_EQ(OB_SUCCESS, prepare(next_body, req, compressor));
  EXPECT_EQ(1, compressor.compress_count_);
}

TEST_F(TestRpcPreparedBody, prepared_body_excludes_unused_length_estimate)
{
  ASSERT_FALSE(OBTRACE->is_inited());
  MarkerExtraPayload extra;
  ScopedRuntimePayload runtime_guard(false, extra);
  struct UpperBoundRequest : public CountingRequest
  {
    int64_t get_serialize_size() const { return CountingRequest::get_serialize_size() + 256; }
  } req;

  std::vector<char> expected(req.bytes_.size() + extra.get_serialize_size());
  MEMCPY(expected.data(), req.bytes_.data(), req.bytes_.size());
  int64_t expected_size = req.bytes_.size();
  ASSERT_EQ(OB_SUCCESS, extra.serialize(expected.data(), expected.size(), expected_size));

  CountingCompressor compressor;
  ObRpcPreparedBody body;
  const int64_t estimated_size = req.get_serialize_size() + extra.get_serialize_size();
  ObLZ4Compressor reference_compressor;
  int64_t overflow_size = 0;
  ASSERT_EQ(OB_SUCCESS, reference_compressor.get_max_overflow_size(estimated_size, overflow_size));
  ASSERT_EQ(OB_SUCCESS,
            body.prepare(req, OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR, &compressor));
  ASSERT_TRUE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR));
  ASSERT_TRUE(body.is_compressed());
  EXPECT_EQ(expected_size, body.get_original_size());
  EXPECT_EQ(estimated_size + overflow_size, body.get_payload_size());
  ASSERT_EQ(OB_SUCCESS,
            body.prepare(req, OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR, &compressor));
  EXPECT_EQ(estimated_size + overflow_size, body.get_payload_size());
  EXPECT_EQ(1, req.serialize_count_);
  EXPECT_EQ(1, compressor.compress_count_);
  EXPECT_EQ(1, compressor.overflow_count_);

  std::vector<char> destination(body.get_size());
  ObRpcPacket packet;
  ASSERT_EQ(OB_SUCCESS, body.fill_packet(packet, destination.data(), destination.size()));
  EXPECT_EQ(expected_size, packet.get_original_len());
  std::vector<char> decoded(expected_size);
  int64_t decoded_size = 0;
  ASSERT_EQ(OB_SUCCESS, compressor.decompress(packet.get_cdata(), packet.get_clen(), decoded.data(),
                                            decoded.size(), decoded_size));
  ASSERT_EQ(expected_size, decoded_size);
  EXPECT_EQ(0, MEMCMP(expected.data(), decoded.data(), expected_size));
}

TEST_F(TestRpcPreparedBody, prepare_preserves_outer_rpc_encode_flag)
{
  ASSERT_FALSE(OBTRACE->is_inited());
  lib::ObUnitTestEnableDiagnoseGuard diagnose_guard;
  ObDiagnosticInfo diagnostic_info;
  // Keep the stack owner's reference so the switch guard does not return it to a tenant container.
  ASSERT_EQ(OB_SUCCESS, ObLocalDiagnosticInfo::inc_ref(&diagnostic_info));
  ObDiagnosticInfoSwitchGuard diagnostic_guard(&diagnostic_info);
  ASSERT_EQ(&diagnostic_info, ObLocalDiagnosticInfo::get());
  ASSERT_FALSE(diagnostic_info.get_ash_stat().in_rpc_encode_);
  CountingRequest req;
  CountingCompressor compressor;
  ObRpcPreparedBody body;
  {
    ACTIVE_SESSION_FLAG_SETTER_GUARD(in_rpc_encode);
    ASSERT_TRUE(diagnostic_info.get_ash_stat().in_rpc_encode_);
    ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
    EXPECT_TRUE(diagnostic_info.get_ash_stat().in_rpc_encode_);
    ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
    EXPECT_TRUE(diagnostic_info.get_ash_stat().in_rpc_encode_);
    EXPECT_EQ(OB_STATE_NOT_MATCH,
              body.prepare(req, OB_LOG_PREPARE_REQ, TENANT_ID, LZ4_COMPRESSOR, &compressor));
    EXPECT_TRUE(diagnostic_info.get_ash_stat().in_rpc_encode_);

    CountingRequest failed_req;
    failed_req.serialize_ret_ = OB_SERIALIZE_ERROR;
    CountingCompressor failed_compressor;
    ObRpcPreparedBody failed_body;
    EXPECT_EQ(OB_SERIALIZE_ERROR, prepare(failed_body, failed_req, failed_compressor));
    EXPECT_TRUE(diagnostic_info.get_ash_stat().in_rpc_encode_);
    failed_req.serialize_ret_ = OB_SUCCESS;
    EXPECT_EQ(OB_NOT_SUPPORTED, prepare(failed_body, failed_req, failed_compressor));
    EXPECT_TRUE(diagnostic_info.get_ash_stat().in_rpc_encode_);
    EXPECT_EQ(1, failed_req.serialize_count_);
    EXPECT_EQ(0, failed_compressor.compress_count_);
  }
  EXPECT_FALSE(diagnostic_info.get_ash_stat().in_rpc_encode_);
  EXPECT_EQ(1, req.serialize_count_);
  EXPECT_EQ(1, compressor.compress_count_);
}

TEST_F(TestRpcPreparedBody, fresh_packet_headers_are_initialized_independently)
{
  CountingRequest req;
  CountingCompressor compressor;
  ObRpcPreparedBody body;
  ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
  rpc::frame::ObReqTransport transport(nullptr, nullptr);
  ObRpcProxy proxy;
  ASSERT_EQ(OB_SUCCESS, proxy.init(&transport, 44));
  proxy.set_tenant(TENANT_ID);
  proxy.set_group_id(17);
  proxy.set_dst_cluster(435);
  proxy.set_timeout(3000000);
  ObRpcOpts opts;
  opts.pr_ = ORPR3;
  std::vector<char> destination1(body.get_size());
  std::vector<char> destination2(body.get_size());
  ObRpcPacket packet1;
  ObRpcPacket packet2;
  ASSERT_EQ(OB_SUCCESS, body.fill_packet(packet1, destination1.data(), destination1.size()));
  ASSERT_EQ(OB_SUCCESS, body.fill_packet(packet2, destination2.data(), destination2.size()));
  ASSERT_EQ(OB_SUCCESS, proxy.init_pkt(&packet1, OB_LOG_PUSH_REQ, opts, false));
  ASSERT_EQ(OB_SUCCESS, proxy.init_pkt(&packet2, OB_LOG_PUSH_REQ, opts, false));
  for (const ObRpcPacket *packet : {&packet1, &packet2}) {
    EXPECT_EQ(OB_LOG_PUSH_REQ, packet->get_pcode());
    EXPECT_EQ(TENANT_ID, packet->get_tenant_id());
    EXPECT_EQ(17, packet->get_group_id());
    EXPECT_EQ(44, packet->get_src_cluster_id());
    EXPECT_EQ(435, packet->get_dst_cluster_id());
    EXPECT_EQ(3000000, packet->get_timeout());
    EXPECT_EQ(ORPR3, packet->get_priority());
    EXPECT_EQ(OB_SUCCESS, packet->verify_checksum());
  }
  EXPECT_NE(packet1.get_chid(), packet2.get_chid());
  EXPECT_EQ(packet1.get_checksum(), packet2.get_checksum());
  EXPECT_EQ(1, compressor.compress_count_);
}

TEST_F(TestRpcPreparedBody, invalid_estimated_lengths_do_not_serialize_or_allocate_body)
{
  struct SizedRequest : public CountingRequest
  {
    explicit SizedRequest(int64_t size) : estimated_size_(size) {}
    int64_t get_serialize_size() const { return estimated_size_; }
    int64_t estimated_size_;
  };
  struct SizeCase
  {
    int64_t args_size_;
    int64_t extra_size_;
    int64_t limit_;
    int expected_ret_;
  };
  const SizeCase cases[] = {
      {-1, 8, 1024, OB_RPC_PACKET_TOO_LONG},
      {32, -1, 1024, OB_RPC_PACKET_TOO_LONG},
      {1025, 0, 1024, OB_RPC_PACKET_TOO_LONG},
      {1024, 1, 1024, OB_RPC_PACKET_TOO_LONG},
      {INT64_MAX, 1, INT64_MAX, OB_RPC_PACKET_TOO_LONG},
      {1, INT64_MAX, INT64_MAX, OB_RPC_PACKET_TOO_LONG},
      {static_cast<int64_t>(INT32_MAX) + 1, 0, INT64_MAX, OB_INVALID_ARGUMENT},
      {0, 0, 1024, OB_INVALID_ARGUMENT}};
  for (const SizeCase &test_case : cases) {
    SCOPED_TRACE(::testing::Message() << "args=" << test_case.args_size_
                 << " extra=" << test_case.extra_size_ << " limit=" << test_case.limit_);
    SizedRequest req(test_case.args_size_);
    SizedExtraPayload extra(test_case.extra_size_);
    ScopedRuntimePayload runtime_guard(false, extra);
    ScopedRpcPacketLimit limit_guard(test_case.limit_);
    CountingCompressor compressor;
    ObRpcPreparedBody body;
    EXPECT_EQ(test_case.expected_ret_,
              body.prepare(req, OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR, &compressor));
    EXPECT_EQ(nullptr, body.get_data());
    EXPECT_EQ(0, body.get_size());
    EXPECT_EQ(0, body.get_original_size());
    EXPECT_EQ(0, body.get_payload_size());
    EXPECT_EQ(0, req.serialize_count_);
    EXPECT_EQ(0, extra.serialize_count_);
    EXPECT_EQ(0, compressor.overflow_count_);
    EXPECT_EQ(0, compressor.compress_count_);
    EXPECT_EQ(OB_NOT_SUPPORTED,
              body.prepare(req, OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR, &compressor));
  }
}

TEST_F(TestRpcPreparedBody, invalid_serialized_positions_do_not_publish_body)
{
  struct InvalidPositionRequest
  {
    explicit InvalidPositionRequest(int64_t pos) : reported_pos_(pos), serialize_count_(0) {}
    int64_t get_serialize_size() const { return 16; }
    int serialize(char *buf, const int64_t len, int64_t &pos) const
    {
      UNUSED(buf);
      UNUSED(len);
      ++serialize_count_;
      // Report a broken serializer's position without performing an out-of-bounds write.
      pos = reported_pos_;
      return OB_SUCCESS;
    }
    int64_t reported_pos_;
    mutable int64_t serialize_count_;
  };
  const int64_t positions[] = {-1, 17, INT64_MAX};
  for (const int64_t position : positions) {
    SCOPED_TRACE(position);
    InvalidPositionRequest req(position);
    SizedExtraPayload extra(8);
    ScopedRuntimePayload runtime_guard(false, extra);
    CountingCompressor compressor;
    ObRpcPreparedBody body;
    EXPECT_EQ(OB_ERR_UNEXPECTED,
              body.prepare(req, OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR, &compressor));
    EXPECT_EQ(nullptr, body.get_data());
    EXPECT_EQ(0, body.get_size());
    EXPECT_EQ(0, body.get_original_size());
    EXPECT_EQ(0, body.get_payload_size());
    EXPECT_EQ(1, req.serialize_count_);
    EXPECT_EQ(0, extra.serialize_count_);
    EXPECT_EQ(0, compressor.compress_count_);
    EXPECT_EQ(OB_NOT_SUPPORTED,
              body.prepare(req, OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR, &compressor));
    EXPECT_EQ(1, req.serialize_count_);
  }
}

TEST_F(TestRpcPreparedBody, extra_payload_failure_releases_body_and_bypasses_retry)
{
  CountingRequest req;
  SizedExtraPayload extra(8, OB_SERIALIZE_ERROR);
  ScopedRuntimePayload runtime_guard(false, extra);
  CountingCompressor compressor;
  ObRpcPreparedBody body;
  EXPECT_EQ(OB_SERIALIZE_ERROR, prepare(body, req, compressor));
  EXPECT_EQ(nullptr, body.get_data());
  EXPECT_EQ(0, body.get_size());
  EXPECT_EQ(0, body.get_original_size());
  EXPECT_EQ(0, body.get_payload_size());
  EXPECT_FALSE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR));
  EXPECT_EQ(1, req.serialize_count_);
  EXPECT_EQ(1, extra.serialize_count_);
  EXPECT_EQ(0, compressor.overflow_count_);
  EXPECT_EQ(0, compressor.compress_count_);
  extra.serialize_ret_ = OB_SUCCESS;
  EXPECT_EQ(OB_NOT_SUPPORTED, prepare(body, req, compressor));
  EXPECT_EQ(1, req.serialize_count_);
  EXPECT_EQ(1, extra.serialize_count_);
}

TEST_F(TestRpcPreparedBody, invalid_extra_payload_positions_do_not_publish_body)
{
  struct InvalidPositionExtra : public MarkerExtraPayload
  {
    explicit InvalidPositionExtra(bool past_end) : past_end_(past_end) {}
    int serialize(char *buf, const int64_t len, int64_t &pos) const override
    {
      UNUSED(buf);
      pos = past_end_ ? len + 1 : -1;
      return OB_SUCCESS;
    }
    bool past_end_;
  };
  for (const bool past_end : {false, true}) {
    SCOPED_TRACE(past_end);
    CountingRequest req;
    InvalidPositionExtra extra(past_end);
    ScopedRuntimePayload runtime_guard(false, extra);
    CountingCompressor compressor;
    ObRpcPreparedBody body;
    EXPECT_EQ(OB_ERR_UNEXPECTED, prepare(body, req, compressor));
    EXPECT_EQ(nullptr, body.get_data());
    EXPECT_EQ(0, body.get_size());
    EXPECT_EQ(0, body.get_original_size());
    EXPECT_EQ(0, body.get_payload_size());
    EXPECT_EQ(1, req.serialize_count_);
    EXPECT_EQ(0, compressor.compress_count_);
    EXPECT_EQ(OB_NOT_SUPPORTED, prepare(body, req, compressor));
  }
}

TEST_F(TestRpcPreparedBody, empty_args_can_reuse_a_nonempty_extra_payload)
{
  CountingRequest req;
  req.bytes_.clear();
  MarkerExtraPayload extra;
  ScopedRuntimePayload runtime_guard(false, extra);
  CountingCompressor compressor(CountingCompressor::NO_GAIN);
  ObRpcPreparedBody body;
  ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
  ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
  ASSERT_EQ(extra.get_serialize_size(), body.get_size());
  EXPECT_EQ(body.get_size(), body.get_original_size());
  EXPECT_FALSE(body.is_compressed());
  std::vector<char> expected(extra.get_serialize_size());
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, extra.serialize(expected.data(), expected.size(), pos));
  EXPECT_EQ(0, MEMCMP(expected.data(), body.get_data(), expected.size()));
  EXPECT_EQ(1, req.serialize_count_);
  EXPECT_EQ(1, compressor.compress_count_);
}

TEST_F(TestRpcPreparedBody, invalid_or_expanded_compression_output_reuses_plain_body)
{
  const CountingCompressor::Mode modes[] = {
      CountingCompressor::ZERO_OUTPUT,
      CountingCompressor::NEGATIVE_OUTPUT,
      CountingCompressor::EXPANDED_OUTPUT};
  for (const CountingCompressor::Mode mode : modes) {
    SCOPED_TRACE(static_cast<int>(mode));
    check_plain_fallback(mode);
  }
}

TEST_F(TestRpcPreparedBody, invalid_compressor_overflow_releases_body)
{
  const int64_t overflows[] = {-1, INT64_MAX};
  for (const int64_t overflow : overflows) {
    SCOPED_TRACE(overflow);
    CountingRequest req;
    MarkerExtraPayload extra;
    ScopedRuntimePayload runtime_guard(false, extra);
    ScopedRpcPacketLimit limit_guard(INT64_MAX);
    OverflowCompressor compressor(overflow);
    ObRpcPreparedBody body;
    EXPECT_EQ(OB_SIZE_OVERFLOW, prepare(body, req, compressor));
    EXPECT_EQ(nullptr, body.get_data());
    EXPECT_EQ(0, body.get_original_size());
    EXPECT_EQ(0, body.get_size());
    EXPECT_EQ(0, body.get_payload_size());
    EXPECT_EQ(1, req.serialize_count_);
    EXPECT_EQ(1, compressor.overflow_count_);
    EXPECT_EQ(0, compressor.compress_count_);
    EXPECT_EQ(OB_NOT_SUPPORTED, prepare(body, req, compressor));
    EXPECT_EQ(1, compressor.overflow_count_);
  }
}

TEST_F(TestRpcPreparedBody, zero_compressor_overflow_is_valid)
{
  CountingRequest req;
  MarkerExtraPayload extra;
  ScopedRuntimePayload runtime_guard(false, extra);
  OverflowCompressor compressor(0);
  compressor.mode_ = CountingCompressor::NO_GAIN;
  const int64_t original_size = req.get_serialize_size() + extra.get_serialize_size();
  ScopedRpcPacketLimit limit_guard(original_size);
  ObRpcPreparedBody body;
  ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
  EXPECT_FALSE(body.is_compressed());
  EXPECT_EQ(original_size, body.get_size());
  EXPECT_EQ(original_size, body.get_payload_size());
  EXPECT_EQ(OB_SUCCESS, prepare(body, req, compressor));
  EXPECT_EQ(1, req.serialize_count_);
  EXPECT_EQ(1, compressor.overflow_count_);
  EXPECT_EQ(1, compressor.compress_count_);
}

TEST_F(TestRpcPreparedBody, compression_capacity_respects_packet_limit_boundary)
{
  MarkerExtraPayload extra;
  ScopedRuntimePayload runtime_guard(false, extra);
  CountingRequest reference_req;
  const int64_t original_size = reference_req.get_serialize_size() + extra.get_serialize_size();
  ObLZ4Compressor reference_compressor;
  int64_t overflow = 0;
  ASSERT_EQ(OB_SUCCESS, reference_compressor.get_max_overflow_size(original_size, overflow));
  const int64_t capacity = original_size + overflow;
  const int64_t margins[] = {-1, 0, 1};
  for (const int64_t margin : margins) {
    SCOPED_TRACE(margin);
    ScopedRpcPacketLimit limit_guard(capacity + margin);
    CountingRequest req;
    CountingCompressor compressor;
    ObRpcPreparedBody body;
    const int ret = prepare(body, req, compressor);
    if (margin < 0) {
      EXPECT_EQ(OB_SIZE_OVERFLOW, ret);
      EXPECT_EQ(nullptr, body.get_data());
      EXPECT_EQ(0, body.get_payload_size());
      EXPECT_EQ(0, compressor.compress_count_);
      EXPECT_EQ(OB_NOT_SUPPORTED, prepare(body, req, compressor));
    } else {
      ASSERT_EQ(OB_SUCCESS, ret);
      EXPECT_TRUE(body.is_compressed());
      EXPECT_EQ(capacity, body.get_payload_size());
      EXPECT_EQ(original_size, body.get_original_size());
      EXPECT_EQ(OB_SUCCESS, prepare(body, req, compressor));
      EXPECT_EQ(1, compressor.compress_count_);
    }
    EXPECT_EQ(1, req.serialize_count_);
    EXPECT_EQ(1, compressor.overflow_count_);
  }
}

TEST_F(TestRpcPreparedBody, ready_body_rechecks_original_size_at_dynamic_limit_boundary)
{
  CountingRequest req;
  MarkerExtraPayload extra;
  ScopedRuntimePayload runtime_guard(false, extra);
  CountingCompressor compressor;
  ObRpcPreparedBody body;
  ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
  const int64_t original_size = body.get_original_size();
  const char *const original_data = body.get_data();
  ASSERT_LT(body.get_size(), original_size - 1);
  const int64_t margins[] = {-1, 0, 1};
  for (const int64_t margin : margins) {
    SCOPED_TRACE(margin);
    ScopedRpcPacketLimit limit_guard(original_size + margin);
    EXPECT_EQ(margin < 0 ? OB_RPC_PACKET_TOO_LONG : OB_SUCCESS, prepare(body, req, compressor));
    EXPECT_TRUE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR));
    EXPECT_EQ(original_data, body.get_data());
    EXPECT_EQ(original_size, body.get_original_size());
  }
  EXPECT_EQ(OB_SUCCESS, prepare(body, req, compressor));
  EXPECT_EQ(1, req.serialize_count_);
  EXPECT_EQ(1, compressor.overflow_count_);
  EXPECT_EQ(1, compressor.compress_count_);
}

TEST_F(TestRpcPreparedBody, empty_and_bypassed_bodies_reject_fill_without_writing)
{
  CountingRequest req;
  CountingCompressor compressor;
  ObRpcPreparedBody body;
  std::vector<char> destination(16, 'Z');
  const std::vector<char> untouched = destination;
  ObRpcPacket packet;

  EXPECT_EQ(OB_INVALID_ARGUMENT,
            body.fill_packet(packet, destination.data() + 1, destination.size() - 2));
  EXPECT_EQ(untouched, destination);
  ASSERT_EQ(OB_NOT_SUPPORTED,
            body.prepare(req, OB_LOG_PUSH_REQ, TENANT_ID, NONE_COMPRESSOR, &compressor));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            body.fill_packet(packet, destination.data() + 1, destination.size() - 2));
  EXPECT_EQ(untouched, destination);
  EXPECT_FALSE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR));
  EXPECT_EQ(0, req.serialize_count_);
  EXPECT_EQ(0, compressor.compress_count_);
}

TEST_F(TestRpcPreparedBody, initialized_trace_invalidates_ready_body_permanently)
{
  ASSERT_FALSE(OBTRACE->is_inited());
  CountingRequest req;
  CountingCompressor compressor;
  ObRpcPreparedBody body;
  ASSERT_EQ(OB_SUCCESS, prepare(body, req, compressor));
  ASSERT_TRUE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR));
  std::vector<char> destination(body.get_size() + 2, 'Z');
  const std::vector<char> untouched = destination;
  ObRpcPacket packet;

  {
    struct TraceGuard
    {
      TraceGuard()
      {
        trace::UUID trace_id;
        trace_id.low_ = 1;
        OBTRACE->init(trace_id, trace::UUID(), 0);
      }
      ~TraceGuard() { OBTRACE->reset(); }
    } guard;
    ASSERT_TRUE(OBTRACE->is_inited());
    EXPECT_EQ(OB_NOT_SUPPORTED, prepare(body, req, compressor));
    EXPECT_FALSE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR));
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              body.fill_packet(packet, destination.data() + 1, destination.size() - 2));
    EXPECT_EQ(untouched, destination);
  }

  EXPECT_FALSE(OBTRACE->is_inited());
  EXPECT_EQ(OB_NOT_SUPPORTED, prepare(body, req, compressor));
  EXPECT_FALSE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            body.fill_packet(packet, destination.data() + 1, destination.size() - 2));
  EXPECT_EQ(untouched, destination);
  EXPECT_EQ(1, req.serialize_count_);
  EXPECT_EQ(1, compressor.compress_count_);
}

struct OversizedEntryRequest
{
  explicit OversizedEntryRequest(int64_t body_limit)
      : body_limit_(body_limit), size_count_(0), serialize_count_(0) {}

  int64_t get_serialize_size() const
  {
    ++size_count_;
    return body_limit_ + 1;
  }

  int serialize(char *buf, const int64_t len, int64_t &pos) const
  {
    UNUSED(buf);
    UNUSED(len);
    UNUSED(pos);
    ++serialize_count_;
    return OB_ERR_UNEXPECTED;
  }

  int64_t body_limit_;
  mutable int64_t size_count_;
  mutable int64_t serialize_count_;
};

TEST_F(TestRpcPreparedBody, prepared_entry_rejects_invalid_body_and_clears_outputs)
{
  ASSERT_FALSE(OBTRACE->is_inited());
  ObRpcMemPool pool(TENANT_ID, "RpcPreparedTest");
  rpc::frame::ObReqTransport transport(nullptr, nullptr);
  ObRpcProxy proxy;
  ASSERT_EQ(OB_SUCCESS, proxy.init(&transport, 44));
  proxy.set_tenant(TENANT_ID);
  proxy.set_compressor_type(LZ4_COMPRESSOR);
  ObRpcOpts opts;
  CountingRequest args;
  CountingCompressor compressor;
  ObRpcPreparedBody body;

  // A real stack buffer verifies output clearing without an invalid pointer.
  char previous_output[8] = {0};
  char *req = previous_output;
  int64_t req_size = sizeof(previous_output);
  EXPECT_EQ(OB_STATE_NOT_MATCH, rpc_encode_prepared_req(
      proxy, pool, OB_LOG_PUSH_REQ, opts, body, false, req, req_size));
  EXPECT_EQ(nullptr, req);
  EXPECT_EQ(0, req_size);

  ASSERT_EQ(OB_SUCCESS, prepare(body, args, compressor));
  proxy.set_tenant(TENANT_ID + 1);
  req = previous_output;
  req_size = sizeof(previous_output);
  EXPECT_EQ(OB_STATE_NOT_MATCH, rpc_encode_prepared_req(
      proxy, pool, OB_LOG_PUSH_REQ, opts, body, false, req, req_size));
  EXPECT_EQ(nullptr, req);
  EXPECT_EQ(0, req_size);
  EXPECT_TRUE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR));
  proxy.set_tenant(TENANT_ID);
  {
    ScopedRpcPacketLimit limit_guard(body.get_size() - 1);
    req = previous_output;
    req_size = sizeof(previous_output);
    EXPECT_EQ(OB_RPC_PACKET_TOO_LONG, rpc_encode_prepared_req(
        proxy, pool, OB_LOG_PUSH_REQ, opts, body, false, req, req_size));
    EXPECT_EQ(nullptr, req);
    EXPECT_EQ(0, req_size);
  }
  EXPECT_EQ(1, args.serialize_count_);
  EXPECT_EQ(1, compressor.compress_count_);
}

TEST_F(TestRpcPreparedBody, cached_encoder_rejects_original_body_over_the_new_limit)
{
  ASSERT_FALSE(OBTRACE->is_inited());
  ObRpcMemPool pool(TENANT_ID, "RpcPreparedTest");
  CountingRequest args;
  MarkerExtraPayload extra;
  ScopedRuntimePayload runtime_guard(false, extra);
  CountingCompressor compressor;
  ObRpcPreparedBody body;
  ASSERT_EQ(OB_SUCCESS, prepare(body, args, compressor));
  ASSERT_LT(body.get_size(), body.get_original_size() - 1);
  rpc::frame::ObReqTransport transport(nullptr, nullptr);
  ObRpcProxy proxy;
  ASSERT_EQ(OB_SUCCESS, proxy.init(&transport, 44));
  proxy.set_tenant(TENANT_ID);
  proxy.set_compressor_type(LZ4_COMPRESSOR);
  proxy.set_reusable_body(&body);
  ObRpcOpts opts;
  char *req = nullptr;
  int64_t req_size = 0;
  ScopedRpcPacketLimit limit_guard(body.get_original_size() - 1);
  EXPECT_EQ(OB_RPC_PACKET_TOO_LONG, rpc_encode_req(
      proxy, pool, OB_LOG_PUSH_REQ, args, opts, req, req_size, false));
  EXPECT_EQ(nullptr, req);
  EXPECT_EQ(0, req_size);
  EXPECT_TRUE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR));
  EXPECT_EQ(1, args.serialize_count_);
  EXPECT_EQ(1, compressor.compress_count_);
}

TEST_F(TestRpcPreparedBody, oversized_preparation_uses_the_original_encoding_fallback)
{
  ASSERT_FALSE(OBTRACE->is_inited());
  ObRpcMemPool pool(TENANT_ID, "RpcPreparedTest");
  // The original 435 encoder allocates before checking the packet limit.
  ScopedRpcPacketLimit limit_guard(64 * 1024);
  MarkerExtraPayload extra;
  ScopedRuntimePayload runtime_guard(false, extra);
  rpc::frame::ObReqTransport transport(nullptr, nullptr);
  ObRpcProxy proxy;
  ASSERT_EQ(OB_SUCCESS, proxy.init(&transport, 44));
  proxy.set_tenant(TENANT_ID);
  proxy.set_compressor_type(LZ4_COMPRESSOR);
  ObRpcPreparedBody body;
  proxy.set_reusable_body(&body);
  ObRpcOpts opts;
  OversizedEntryRequest args(get_max_rpc_packet_size());
  char *req = nullptr;
  int64_t req_size = 0;

  EXPECT_EQ(OB_RPC_PACKET_TOO_LONG, rpc_encode_req(
      proxy, pool, OB_LOG_PUSH_REQ, args, opts, req, req_size, false));
  EXPECT_EQ(nullptr, req);
  EXPECT_EQ(0, req_size);
  // Preparation and the original encoder each validate the declared length.
  EXPECT_EQ(2, args.size_count_);
  EXPECT_EQ(0, args.serialize_count_);
  EXPECT_EQ(nullptr, body.get_data());

  CountingRequest valid_args;
  CountingCompressor compressor;
  EXPECT_EQ(OB_NOT_SUPPORTED, prepare(body, valid_args, compressor));
  EXPECT_EQ(0, valid_args.serialize_count_);
  EXPECT_EQ(0, compressor.compress_count_);
}

TEST_F(TestRpcPreparedBody, stream_encoding_bypasses_preparation_without_poisoning_the_cache)
{
  ASSERT_FALSE(OBTRACE->is_inited());
  // Keep oversized fallback allocations small while allowing later preparation.
  ScopedRpcPacketLimit limit_guard(64 * 1024);
  MarkerExtraPayload extra;
  ScopedRuntimePayload runtime_guard(false, extra);
  rpc::frame::ObReqTransport transport(nullptr, nullptr);
  ObRpcProxy proxy;
  ASSERT_EQ(OB_SUCCESS, proxy.init(&transport, 44));
  proxy.set_tenant(TENANT_ID);
  proxy.set_compressor_type(LZ4_COMPRESSOR);
  ObRpcOpts opts;
  struct StreamCase
  {
    bool is_next_;
    bool is_last_;
    int64_t session_id_;
  };
  const StreamCase cases[] = {{true, false, 0}, {false, true, 0}, {false, false, 17}};

  for (const StreamCase &stream : cases) {
    ObRpcMemPool pool(TENANT_ID, "RpcPreparedTest");
    ObRpcPreparedBody body;
    proxy.set_reusable_body(&body);
    OversizedEntryRequest args(get_max_rpc_packet_size());
    char *req = nullptr;
    int64_t req_size = 0;
    EXPECT_EQ(OB_RPC_PACKET_TOO_LONG, rpc_encode_req(
        proxy, pool, OB_LOG_PUSH_REQ, args, opts, req, req_size, false,
        stream.is_next_, stream.is_last_, stream.session_id_));
    EXPECT_EQ(nullptr, req);
    EXPECT_EQ(0, req_size);
    EXPECT_EQ(1, args.size_count_);
    EXPECT_EQ(0, args.serialize_count_);

    // A skipped cache remains usable for a later ordinary preparation.
    CountingRequest valid_args;
    CountingCompressor compressor;
    EXPECT_EQ(OB_SUCCESS, prepare(body, valid_args, compressor));
    EXPECT_EQ(1, valid_args.serialize_count_);
    EXPECT_EQ(1, compressor.compress_count_);
    proxy.set_reusable_body(nullptr);
  }
}

TEST_F(TestRpcPreparedBody, prepared_entry_copies_outlive_the_cached_body)
{
  ASSERT_FALSE(OBTRACE->is_inited());
  MarkerExtraPayload extra;
  ScopedRuntimePayload runtime_guard(false, extra);
  rpc::frame::ObReqTransport transport(nullptr, nullptr);
  ObRpcProxy proxy;
  ASSERT_EQ(OB_SUCCESS, proxy.init(&transport, 44));
  proxy.set_tenant(TENANT_ID);
  proxy.set_group_id(17);
  proxy.set_dst_cluster(435);
  proxy.set_timeout(3000000);
  proxy.set_compressor_type(LZ4_COMPRESSOR);
  ObRpcOpts opts;
  opts.pr_ = ORPR3;
  CountingRequest args;
  const int64_t original_size = args.get_serialize_size() + extra.get_serialize_size();
  std::vector<char> expected(original_size);
  MEMCPY(expected.data(), args.bytes_.data(), args.bytes_.size());
  int64_t pos = args.bytes_.size();
  ASSERT_EQ(OB_SUCCESS, extra.serialize(expected.data(), expected.size(), pos));
  ASSERT_EQ(original_size, pos);

  // Each request pool outlives the cache and frees its buffer even on ASSERT.
  ObRpcMemPool first_pool(TENANT_ID, "RpcPreparedTest");
  ObRpcMemPool second_pool(TENANT_ID, "RpcPreparedTest");
  char *first_data = nullptr;
  char *second_data = nullptr;
  int64_t first_size = 0;
  int64_t second_size = 0;
  int64_t body_size = 0;
  {
    ObRpcPreparedBody body;
    proxy.set_reusable_body(&body);
    ASSERT_EQ(OB_SUCCESS, rpc_encode_req(proxy, first_pool, OB_LOG_PUSH_REQ,
        args, opts, first_data, first_size, false));
    ASSERT_TRUE(OB_NOT_NULL(first_data));
    ASSERT_TRUE(body.matches(OB_LOG_PUSH_REQ, TENANT_ID, LZ4_COMPRESSOR));
    ASSERT_TRUE(body.is_compressed());
    ASSERT_EQ(1, args.serialize_count_);
    const char *cached_data = body.get_data();
    body_size = body.get_size();
    ASSERT_GT(body_size, 0);
    ASSERT_LT(body_size, original_size);

    proxy.set_dst_cluster(436);
    ASSERT_EQ(OB_SUCCESS, rpc_encode_req(proxy, second_pool, OB_LOG_PUSH_REQ,
        args, opts, second_data, second_size, false));
    ASSERT_TRUE(OB_NOT_NULL(second_data));
    EXPECT_NE(first_data, second_data);
    EXPECT_EQ(cached_data, body.get_data());
    EXPECT_EQ(1, args.serialize_count_);
    proxy.set_reusable_body(nullptr);
  }

  // Decode and read both request buffers only after the cache has been freed.
  ObRpcPacket packet1;
  ObRpcPacket packet2;
  ASSERT_EQ(OB_SUCCESS, packet1.decode(first_data, first_size));
  ASSERT_EQ(OB_SUCCESS, packet2.decode(second_data, second_size));
  EXPECT_EQ(ObRpcPacket::get_header_size() + body_size, first_size);
  EXPECT_EQ(first_size, second_size);
  EXPECT_EQ(435, packet1.get_dst_cluster_id());
  EXPECT_EQ(436, packet2.get_dst_cluster_id());
  EXPECT_EQ(packet1.get_checksum(), packet2.get_checksum());
  ObLZ4Compressor decompressor;
  for (const ObRpcPacket *packet : {&packet1, &packet2}) {
    ASSERT_EQ(OB_SUCCESS, packet->verify_checksum());
    EXPECT_EQ(OB_LOG_PUSH_REQ, packet->get_pcode());
    EXPECT_EQ(TENANT_ID, packet->get_tenant_id());
    EXPECT_EQ(17, packet->get_group_id());
    EXPECT_EQ(44, packet->get_src_cluster_id());
    EXPECT_EQ(3000000, packet->get_timeout());
    EXPECT_EQ(ORPR3, packet->get_priority());
    EXPECT_FALSE(packet->is_stream());
    EXPECT_EQ(0, packet->get_session_id());
    EXPECT_FALSE(packet->has_context());
    EXPECT_FALSE(packet->has_disable_debugsync());
    EXPECT_FALSE(packet->has_trace_info());
    ASSERT_EQ(LZ4_COMPRESSOR, packet->get_compressor_type());
    ASSERT_EQ(original_size, packet->get_original_len());
    ASSERT_EQ(body_size, packet->get_clen());
    std::vector<char> decoded(original_size);
    int64_t decoded_size = 0;
    ASSERT_EQ(OB_SUCCESS, decompressor.decompress(packet->get_cdata(), packet->get_clen(),
        decoded.data(), decoded.size(), decoded_size));
    ASSERT_EQ(original_size, decoded_size);
    EXPECT_EQ(0, MEMCMP(expected.data(), decoded.data(), original_size));
  }

  // Changing one outgoing body cannot corrupt the other destination's copy.
  first_data[ObRpcPacket::get_header_size()] ^= 1;
  EXPECT_EQ(OB_PACKET_CHECKSUM_ERROR, packet1.verify_checksum());
  EXPECT_EQ(OB_SUCCESS, packet2.verify_checksum());
}

} // namespace
} // namespace obrpc
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_rpc_prepared_body.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
