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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_SEQ_GENERATOR_H
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_SEQ_GENERATOR_H

#include "storage/ddl/ob_ddl_seq_generator.h"

namespace oceanbase
{

namespace blocksstable
{

struct ObMacroSeqParam
{
public:
  enum SeqType
  {
    SEQ_TYPE_INC = 0,
    SEQ_TYPE_SKIP = 1,
    SEQ_TYPE_MAX,
  };
  ObMacroSeqParam() : seq_type_(SEQ_TYPE_MAX), start_(0), interval_(0), step_(0) {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(seq_type), K_(start), K_(interval), K_(step));

  SeqType seq_type_;
  int64_t start_;
  int64_t interval_;
  int64_t step_;
};

class ObMacroSeqGenerator
{
public:
  ObMacroSeqGenerator() {}
  virtual ~ObMacroSeqGenerator() {}
  virtual int init(const ObMacroSeqParam &seq_param) = 0;
  virtual int get_next(int64_t &seq_val) = 0;
  virtual int preview_next(const int64_t current_val, int64_t &next_val) const = 0;
  virtual int64_t get_current() = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObMacroIncSeqGenerator: public ObMacroSeqGenerator
{
public:
  ObMacroIncSeqGenerator() : is_inited_(false), start_(0), current_(-1), seq_threshold_(0) {}
  virtual ~ObMacroIncSeqGenerator() {}
  virtual int init(const ObMacroSeqParam &seq_param) override;
  virtual int get_next(int64_t &seq_val) override;
  virtual int preview_next(const int64_t current_val, int64_t &next_val) const override;
  virtual int64_t get_current() override { return current_; }
  TO_STRING_KV(K_(is_inited), K_(start), K_(current), K_(seq_threshold));
private:
  bool is_inited_;
  int64_t start_;
  int64_t current_;
  int64_t seq_threshold_;
};

class ObMacroSkipSeqGenerator: public ObMacroSeqGenerator
{
public:
  ObMacroSkipSeqGenerator() : ddl_seq_generator_() {}
  virtual ~ObMacroSkipSeqGenerator() {}
  virtual int init(const ObMacroSeqParam &seq_param) override;
  virtual int get_next(int64_t &seq_val) override;
  virtual int preview_next(const int64_t current_val, int64_t &next_val) const override;
  virtual int64_t get_current() override { return ddl_seq_generator_.get_current(); }
  TO_STRING_KV(K_(ddl_seq_generator));
private:
  storage::ObDDLSeqGenerator ddl_seq_generator_;
};

}// namespace blocksstable
}// namespace oceanbase

#endif//OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_SEQ_GENERATOR_H
