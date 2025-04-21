/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "storage/fts/utils/ob_ft_ngram_impl.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/utils/ob_ft_char_utils.h"

#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{
oceanbase::storage::ObFTNgramImpl::ObFTNgramImpl()
    : cs_(nullptr),
      fulltext_start_(nullptr),
      fulltext_end_(nullptr),
      window_(),
      is_inited_(false)
{
}

ObFTNgramImpl::~ObFTNgramImpl() {}

int ObFTNgramImpl::init(const ObCharsetInfo *const cs,
                        const char *const fulltext,
                        const int64_t fulltext_len,
                        const int64_t min,
                        const int64_t max)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(cs) || OB_ISNULL(fulltext) || OB_UNLIKELY(fulltext_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(cs), KP(fulltext), K(fulltext_len));
  } else {
    cs_ = cs;
    fulltext_start_ = fulltext;
    fulltext_end_ = fulltext + fulltext_len;
    cur_ = fulltext_start_;
    window_.reset();
    window_.min_ngram_size_ = min;
    window_.max_ngram_size_ = max;
    is_inited_ = true;
  }
  return ret;
}

void ObFTNgramImpl::reset()
{
  cs_ = nullptr;
  fulltext_start_ = nullptr;
  fulltext_end_ = nullptr;
  window_.reset();
  is_inited_ = false;
}


int ObFTNgramImpl::get_next_token(const char *&word,
                                  int64_t &word_len,
                                  int64_t &char_cnt,
                                  int64_t &word_freq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObFTNgramImpl is not inited", K(ret));
  } else {
    bool found = false;

    while (OB_SUCC(ret) && !found) {
      if (window_.meet_delimiter_) {
        if (window_.cnt_ < window_.min_ngram_size_) {
          if (window_.is_last_batch_) {
            ret = OB_ITER_END;
          } else {
            window_.reset();
          }
        } else {
          window_.out_ngram(word, word_len, char_cnt, word_freq);
          found = true;
        }
      } else {
        if (window_.cnt_ < window_.max_ngram_size_) {
          if (cur_ >= fulltext_end_) {
            // meet end and end it.
            window_.meet_delimiter_ = true;
            window_.is_last_batch_ = true;
          } else {
            const int64_t c_len = ob_mbcharlen_ptr(cs_, cur_, fulltext_end_);
            if (cur_ + c_len > fulltext_end_ || 0 == c_len) {
              // if invalid char, end it
              window_.meet_delimiter_ = true;
              window_.is_last_batch_ = true;
            } else {
              int type = 0;
              cs_->cset->ctype(cs_, &type, (uchar *)cur_, (uchar *)fulltext_end_);
              bool delimiter = !true_word_char(type, *cur_);
              if (delimiter) {
                window_.meet_delimiter_ = true;
              } else {
                Word word;
                word.len = c_len;
                word.ptr = cur_;
                window_.add_word(word);
              }
            }
            // anyway step next.
            cur_ += c_len;
          }
        } else {
          window_.out_ngram(word, word_len, char_cnt, word_freq);
          found = true;
        }
      }
    }
  }
  return ret;
}

void ObFTNgramImpl::Window::out_ngram(const char *&word,
                                      int64_t &word_len,
                                      int64_t &char_cnt,
                                      int64_t &word_freq)
{
  int ret = OB_SUCCESS;

  if (ngram_n_ == 0) {
    ngram_n_ = min_ngram_size_;
  }
  word = word_[start_].ptr;
  int ngram_end = (start_ + ngram_n_ - 1) % NGRAM_ARRAY_SIZE;
  word_len = word_[ngram_end].ptr - word_[start_].ptr + word_[ngram_end].len;
  char_cnt = ngram_n_;
  word_freq = 1;

  if (ngram_n_ == max_ngram_size_ || ngram_n_ == cnt_) {
    pop_start();
    ngram_n_ = 0;
  } else {
    ngram_n_++;
  }
}
void ObFTNgramImpl::Window::add_word(const Word &word)
{
  int index = (start_ + cnt_) % NGRAM_ARRAY_SIZE;
  word_[index] = word;
  cnt_++;
}

} // namespace storage
} // namespace oceanbase