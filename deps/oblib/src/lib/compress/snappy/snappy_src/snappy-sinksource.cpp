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

#include <string.h>

#include "snappy-sinksource.h"

namespace snappy {

Source::~Source() { }

Sink::~Sink() { }

char* Sink::GetAppendBuffer(size_t, char* scratch) {
  return scratch;
}

ByteArraySource::~ByteArraySource() { }

size_t ByteArraySource::Available() const { return left_; }

const char* ByteArraySource::Peek(size_t* len) {
  *len = left_;
  return ptr_;
}

void ByteArraySource::Skip(size_t n) {
  left_ -= n;
  ptr_ += n;
}

UncheckedByteArraySink::~UncheckedByteArraySink() { }

void UncheckedByteArraySink::Append(const char* data, size_t n) {
  // Do no copying if the caller filled in the result of GetAppendBuffer()
  if (data != dest_) {
    memcpy(dest_, data, n);
  }
  dest_ += n;
}

char* UncheckedByteArraySink::GetAppendBuffer(size_t, char*) {
  return dest_;
}


}
