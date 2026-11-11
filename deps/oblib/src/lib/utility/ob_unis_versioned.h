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

#ifndef OCEANBASE_LIB_UTILITY_OB_UNIS_VERSIONED_H_
#define OCEANBASE_LIB_UTILITY_OB_UNIS_VERSIONED_H_

#include <functional>
#include <tuple>
#include <type_traits>
#include <variant>
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace common
{
namespace serialization
{

template <typename Original, int64_t Version>
struct UnisVersionTraits
{
  static constexpr bool registered = false;
  using WireType = void;
};

template <typename Original>
struct UnisResultTraits;

template <typename Original>
struct UnisOriginalRecord
{
  static constexpr bool accepts(const int64_t version)
  {
    return version == Original::unis_declared_version();
  }
  static int decode_body(Original &original, const int64_t version, DESERIAL_PARAMS)
  {
    UNUSED(version);
    return original.deserialize_(buf, data_len, pos);
  }
};

// A successfully decoded view retains the input and its validated body boundary.
// Callers must keep that input alive and unchanged until decoding/replay finishes.
class UnisRecordView
{
public:
  UnisRecordView() : buf_(nullptr), version_(0), body_start_(0), body_end_(0)
  {}

  int decode(DESERIAL_PARAMS)
  {
    return decode_header<false, 0>(buf, data_len, pos);
  }

  template <int64_t Version>
  int decode_expected(DESERIAL_PARAMS)
  {
    return decode_header<true, Version>(buf, data_len, pos);
  }

private:
  template <bool CheckVersion, int64_t Version>
  int decode_header(DESERIAL_PARAMS)
  {
    int ret = OB_SUCCESS;
    int64_t version = 0;
    int64_t body_len = 0;
    buf_ = nullptr;
    if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0) || OB_UNLIKELY(pos > data_len)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(decode_vi64(buf, data_len, pos, &version))) {
      RPC_WARN("decode record version failed", K(ret));
    } else if (OB_FAIL(decode_vi64(buf, data_len, pos, &body_len))) {
      RPC_WARN("decode record body length failed", K(ret));
    } else if (CheckVersion && version != Version) {
      ret = OB_NOT_SUPPORTED;
    } else if (OB_UNLIKELY(body_len < 0 || body_len > data_len - pos)) {
      ret = OB_DESERIALIZE_ERROR;
      RPC_WARN("invalid record body length", K(ret), K(body_len), K(pos), K(data_len));
    } else {
      buf_ = buf;
      version_ = version;
      body_start_ = pos;
      body_end_ = pos + body_len;
    }
    return ret;
  }

public:
  int64_t version() const
  {
    return version_;
  }
  const char *body() const
  {
    return buf_ + body_start_;
  }
  int64_t body_size() const
  {
    return body_end_ - body_start_;
  }
  int64_t body_start() const
  {
    return body_start_;
  }
  int64_t body_end() const
  {
    return body_end_;
  }

private:
  const char *buf_;
  int64_t version_;
  int64_t body_start_;
  int64_t body_end_;
};

template <typename T>
struct UnisOriginalResult
{
  std::reference_wrapper<T> value;
  int64_t wire_version;
};

template <int64_t Version, typename Wire>
struct UnisVersionResult
{
  std::reference_wrapper<Wire> value;
  static constexpr int64_t wire_version = Version;
};

template <typename Original, typename Wire>
constexpr bool unis_binding_matches()
{
  using Binding = UnisVersionTraits<Original, Wire::unis_declared_version()>;
  return Binding::registered && std::is_same_v<typename Binding::WireType, Wire>
         && !UnisOriginalRecord<Original>::accepts(Wire::unis_declared_version());
}

template <typename Original, typename... Wires>
class UnisRecordRef
{
  template <typename Wire>
  static constexpr bool unique_version()
  {
    return (0 + ... + int(Wire::unis_declared_version() == Wires::unis_declared_version())) == 1;
  }

public:
  using Result = std::variant<std::monostate, UnisOriginalResult<Original>,
                              UnisVersionResult<Wires::unis_declared_version(), Wires>...>;
  static_assert(sizeof...(Wires) > 0, "record requires registered version types");
  static_assert((unis_binding_matches<Original, Wires>() && ...), "invalid record version binding");
  static_assert((unique_version<Wires>() && ...), "duplicate record version");
  static_assert((std::is_nothrow_copy_constructible_v<UnisVersionResult<Wires::unis_declared_version(), Wires>> && ...),
                "record results must not throw");

  UnisRecordRef(Original &original, Wires &...wires) noexcept : original_(original), wires_(wires...), result_()
  {}

  int deserialize(DESERIAL_PARAMS)
  {
    result_.template emplace<0>();
    UnisRecordView view;
    int ret = view.decode(buf, data_len, pos);
    if (OB_SUCC(ret)) { ret = dispatch(view, pos); }
    return ret;
  }

  int deserialize_parsed(const UnisRecordView &view, int64_t &pos)
  {
    result_.template emplace<0>();
    return dispatch(view, pos);
  }

  int deserialize_parsed_original(const UnisRecordView &view, int64_t &pos)
  {
    result_.template emplace<0>();
    return UnisOriginalRecord<Original>::accepts(view.version()) ? decode_original(view, pos) : OB_NOT_SUPPORTED;
  }

  int deserialize_original(DESERIAL_PARAMS)
  {
    result_.template emplace<0>();
    UnisRecordView view;
    int ret = view.decode(buf, data_len, pos);
    if (OB_SUCC(ret)) {
      ret = UnisOriginalRecord<Original>::accepts(view.version()) ? decode_original(view, pos) : OB_NOT_SUPPORTED;
    }
    return ret;
  }

  template <int64_t Version>
  int deserialize_expected(DESERIAL_PARAMS)
  {
    result_.template emplace<0>();
    UnisRecordView view;
    int ret = view.decode_expected<Version>(buf, data_len, pos);
    if (OB_SUCC(ret)) { ret = decode_expected_body<Version>(view, pos); }
    return ret;
  }

  template <int64_t Version>
  int deserialize_parsed_expected(const UnisRecordView &view, int64_t &pos)
  {
    result_.template emplace<0>();
    return view.version() == Version ? decode_expected_body<Version>(view, pos) : OB_NOT_SUPPORTED;
  }

  const Result &result() const
  {
    return result_;
  }
  bool decoded() const
  {
    return result_.index() != 0;
  }
  constexpr int64_t version() const
  {
    return std::visit(
      [](const auto &selected) constexpr -> int64_t {
        if constexpr (std::is_same_v<std::decay_t<decltype(selected)>, std::monostate>) {
          return -1;
        } else {
          return selected.wire_version;
        }
      },
      result_);
  }

private:
  template <typename Wire>
  int decode_wire(const UnisRecordView &view, int64_t &pos)
  {
    Wire &wire = std::get<Wire &>(wires_);
    int64_t body_pos = 0;
    int ret = wire.deserialize_(view.body(), view.body_size(), body_pos);
    pos = view.body_end();
    if (OB_SUCC(ret)) {
      using Selected = UnisVersionResult<Wire::unis_declared_version(), Wire>;
      result_.template emplace<Selected>(Selected{std::ref(wire)});
    }
    return ret;
  }

  template <int64_t Version>
  int decode_expected_body(const UnisRecordView &view, int64_t &pos)
  {
    using Binding = UnisVersionTraits<Original, Version>;
    static_assert(Binding::registered || UnisOriginalRecord<Original>::accepts(Version),
                  "expected version is not registered or original");
    if constexpr (UnisOriginalRecord<Original>::accepts(Version)) {
      return decode_original(view, pos);
    } else {
      return decode_wire<typename Binding::WireType>(view, pos);
    }
  }

  int decode_original(const UnisRecordView &view, int64_t &pos)
  {
    int64_t body_pos = 0;
    int ret =
      UnisOriginalRecord<Original>::decode_body(original_, view.version(), view.body(), view.body_size(), body_pos);
    pos = view.body_end();
    if (OB_SUCC(ret)) {
      result_.template emplace<UnisOriginalResult<Original>>(
        UnisOriginalResult<Original>{std::ref(original_), view.version()});
    }
    return ret;
  }

  template <size_t Index = 0>
  int dispatch(const UnisRecordView &view, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    if constexpr (Index < sizeof...(Wires)) {
      using Wire = std::remove_reference_t<std::tuple_element_t<Index, std::tuple<Wires &...>>>;
      if (view.version() == Wire::unis_declared_version()) {
        ret = decode_wire<Wire>(view, pos);
      } else {
        ret = dispatch<Index + 1>(view, pos);
      }
    } else if (UnisOriginalRecord<Original>::accepts(view.version())) {
      ret = decode_original(view, pos);
    } else {
      ret = OB_NOT_SUPPORTED;
    }
    return ret;
  }

  Original &original_;
  std::tuple<Wires &...> wires_;
  Result result_;
};

template <typename Original, typename... Wires>
auto make_unis_deser_record(Original &original, Wires &...wires) -> typename UnisResultTraits<Original>::Record
{
  using Expected = typename UnisResultTraits<Original>::Record;
  static_assert(std::is_same_v<Expected, UnisRecordRef<Original, Wires...>>,
                "record instances must match OB_UNIS_SER_REGISTER_TYPES order and types");
  return Expected(original, wires...);
}

} // namespace serialization
} // namespace common
} // namespace oceanbase

// This template infrastructure is exempt from the business-code auto restriction.
#define DEF_UNIS_VERSION_DECODE_REC(record, original, ...) \
  auto record = ::oceanbase::common::serialization::make_unis_deser_record(original, __VA_ARGS__)

#define OB_UNIS_SER_REGISTER_TYPE(ORIGINAL, WIRE, VERSION)                                                                 \
  namespace oceanbase                                                                                                  \
  {                                                                                                                    \
  namespace common                                                                                                     \
  {                                                                                                                    \
  namespace serialization                                                                                              \
  {                                                                                                                    \
  template <>                                                                                                          \
  struct UnisVersionTraits<ORIGINAL, VERSION>                                                                          \
  {                                                                                                                    \
    static constexpr bool registered = true;                                                                           \
    using OriginalType = ORIGINAL;                                                                                     \
    using WireType = WIRE;                                                                                             \
    static constexpr int64_t version = VERSION;                                                                        \
    static_assert(WIRE::unis_declared_version() == VERSION, "registered version does not match type");                 \
  };                                                                                                                   \
  }                                                                                                                    \
  }                                                                                                                    \
  }

#define OB_UNIS_PP_CAT_(A, B) A##B
#define OB_UNIS_PP_CAT(A, B) OB_UNIS_PP_CAT_(A, B)
#define OB_UNIS_PP_REGISTER_2(C, W, V) OB_UNIS_SER_REGISTER_TYPE(C, W, V)
#define OB_UNIS_PP_REGISTER_4(C, W, V, ...) OB_UNIS_SER_REGISTER_TYPE(C, W, V) OB_UNIS_PP_REGISTER_2(C, __VA_ARGS__)
#define OB_UNIS_PP_REGISTER_6(C, W, V, ...) OB_UNIS_SER_REGISTER_TYPE(C, W, V) OB_UNIS_PP_REGISTER_4(C, __VA_ARGS__)
#define OB_UNIS_PP_REGISTER_8(C, W, V, ...) OB_UNIS_SER_REGISTER_TYPE(C, W, V) OB_UNIS_PP_REGISTER_6(C, __VA_ARGS__)
#define OB_UNIS_PP_REGISTER_10(C, W, V, ...) OB_UNIS_SER_REGISTER_TYPE(C, W, V) OB_UNIS_PP_REGISTER_8(C, __VA_ARGS__)
#define OB_UNIS_PP_REGISTER_12(C, W, V, ...) OB_UNIS_SER_REGISTER_TYPE(C, W, V) OB_UNIS_PP_REGISTER_10(C, __VA_ARGS__)
#define OB_UNIS_PP_REGISTER_14(C, W, V, ...) OB_UNIS_SER_REGISTER_TYPE(C, W, V) OB_UNIS_PP_REGISTER_12(C, __VA_ARGS__)
#define OB_UNIS_PP_REGISTER_16(C, W, V, ...) OB_UNIS_SER_REGISTER_TYPE(C, W, V) OB_UNIS_PP_REGISTER_14(C, __VA_ARGS__)
#define OB_UNIS_PP_TYPES_2(W, V) W
#define OB_UNIS_PP_TYPES_4(W, V, ...) W, OB_UNIS_PP_TYPES_2(__VA_ARGS__)
#define OB_UNIS_PP_TYPES_6(W, V, ...) W, OB_UNIS_PP_TYPES_4(__VA_ARGS__)
#define OB_UNIS_PP_TYPES_8(W, V, ...) W, OB_UNIS_PP_TYPES_6(__VA_ARGS__)
#define OB_UNIS_PP_TYPES_10(W, V, ...) W, OB_UNIS_PP_TYPES_8(__VA_ARGS__)
#define OB_UNIS_PP_TYPES_12(W, V, ...) W, OB_UNIS_PP_TYPES_10(__VA_ARGS__)
#define OB_UNIS_PP_TYPES_14(W, V, ...) W, OB_UNIS_PP_TYPES_12(__VA_ARGS__)
#define OB_UNIS_PP_TYPES_16(W, V, ...) W, OB_UNIS_PP_TYPES_14(__VA_ARGS__)

#define OB_UNIS_SER_REGISTER_TYPES(ORIGINAL, ...)                                                                                  \
  static_assert(ARGS_NUM(__VA_ARGS__) >= 2 && ARGS_NUM(__VA_ARGS__) <= 16 && ARGS_NUM(__VA_ARGS__) % 2 == 0,           \
                "OB_UNIS_SER_REGISTER_TYPES requires 1 to 8 type/version pairs");                                                  \
  OB_UNIS_PP_CAT(OB_UNIS_PP_REGISTER_, ARGS_NUM(__VA_ARGS__))(ORIGINAL, __VA_ARGS__) namespace oceanbase               \
  {                                                                                                                    \
    namespace common                                                                                                   \
    {                                                                                                                  \
    namespace serialization                                                                                            \
    {                                                                                                                  \
    template <>                                                                                                        \
    struct UnisResultTraits<ORIGINAL>                                                                                  \
    {                                                                                                                  \
      using Record = UnisRecordRef<ORIGINAL, OB_UNIS_PP_CAT(OB_UNIS_PP_TYPES_, ARGS_NUM(__VA_ARGS__))(__VA_ARGS__)>;   \
    };                                                                                                                 \
    }                                                                                                                  \
    }                                                                                                                  \
  }

#endif // OCEANBASE_LIB_UTILITY_OB_UNIS_VERSIONED_H_
