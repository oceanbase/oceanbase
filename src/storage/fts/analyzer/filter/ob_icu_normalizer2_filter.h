/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ICU_NORMALIZER2_FILTER_H_
#define OB_ICU_NORMALIZER2_FILTER_H_

#include "storage/fts/analyzer/ob_analyzer.h"

#include <unicode/normalizer2.h>

namespace oceanbase
{
namespace storage
{

struct ObICUNormalizer2FilterSpec : public ObTokenFilterSpec
{
  enum class Name
  {
    INVALID = 0,
    NFC,
    NFKC,
    NFKC_CF,
    MAX
  };
  Name name_;
  UNormalization2Mode mode_;

  ObICUNormalizer2FilterSpec(
      Name name = Name::NFKC_CF,
      UNormalization2Mode mode = UNormalization2Mode::UNORM2_COMPOSE)
    : ObTokenFilterSpec(ObTokenFilterType::TOKEN_FILTER_TYPE_ICU_NORMALIZATION),
      name_(name),
      mode_(mode)
  {}
  explicit ObICUNormalizer2FilterSpec(ObTokenFilterType type)
    : ObTokenFilterSpec(type),
      name_(Name::NFKC_CF),
      mode_(UNormalization2Mode::UNORM2_COMPOSE)
  {}
  INHERIT_TO_STRING_KV("ObTokenFilterSpec", ObTokenFilterSpec, K_(name), K_(mode));
};

class ObICUNormalizer2Filter : public ObITokenFilter
{
public:
  ObICUNormalizer2Filter();
  ~ObICUNormalizer2Filter();
  int init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc) override;
  int get_next_token(ObTokenAttr &token) override;
  void reset() override;
protected:
  int reserve_buffer_on_demand(const int32_t new_len);
protected:
  static constexpr const char *NORMALIZER_NAMES[] = {"nfc", "nfkc", "nfkc_cf"};
  static constexpr int32_t LOCAL_BUFFER_SIZE = 256;
  common::ObArenaAllocator allocator_;
  const icu::Normalizer2 *normalizer_;
  char local_buffer_[LOCAL_BUFFER_SIZE];
  char *buffer_;
  int32_t buffer_size_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObICUNormalizer2Filter);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_ICU_NORMALIZER2_FILTER_H_
