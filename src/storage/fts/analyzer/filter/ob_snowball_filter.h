/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_SNOWBALL_FILTER_H_
#define OB_SNOWBALL_FILTER_H_

#include "storage/fts/analyzer/ob_analyzer.h"

#include <libstemmer.h>

namespace oceanbase
{
namespace storage
{

struct ObSnowballFilterSpec : public ObTokenFilterSpec
{
  enum class Algorithm
  {
    INVALID = 0,
    ARABIC,     ARMENIAN,   BASQUE,     CATALAN,    DANISH,
    DUTCH,      ENGLISH,    ESPERANTO,  ESTONIAN,   FINNISH,
    FRENCH,     GERMAN,     GREEK,      HINDI,      HUNGARIAN,
    INDONESIAN, IRISH,      ITALIAN,    LITHUANIAN, NEPALI,
    NORWEGIAN,  PORTUGUESE, ROMANIAN,   RUSSIAN,    SERBIAN,
    SPANISH,    SWEDISH,    TAMIL,      TURKISH,    YIDDISH,
    PORTER,
    MAX
  };
  Algorithm algo_;

  ObSnowballFilterSpec(Algorithm algo = Algorithm::INVALID)
    : ObTokenFilterSpec(ObTokenFilterType::TOKEN_FILTER_TYPE_SNOWBALL), algo_(algo)
  {}

  INHERIT_TO_STRING_KV("ObTokenFilterSpec", ObTokenFilterSpec, K_(algo));
};

class ObSnowballFilter : public ObITokenFilter
{
public:
  ObSnowballFilter();
  ~ObSnowballFilter();
  int init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc) override;
  int get_next_token(ObTokenAttr &token) override;
  void reset() override;
protected:
  static constexpr const char *ALGORITHM_CODES[] = {
      "ar", "hy", "eu", "ca", "da",
      "nl", "en", "eo", "et", "fi",
      "fr", "de", "el", "hi", "hu",
      "id", "ga", "it", "lt", "ne",
      "no", "pt", "ro", "ru", "sr",
      "es", "sv", "ta", "tr", "yi",
      "porter"
  };
  struct sb_stemmer *stemmer_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSnowballFilter);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_SNOWBALL_FILTER_H_
