/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef AVRO_DATA_FILE_WRITER_H
#define AVRO_DATA_FILE_WRITER_H

#define USING_LOG_PREFIX SQL

#include "sql/engine/basic/ob_select_into_basic.h"

#include <avro/DataFile.hh>

using namespace avro;

namespace oceanbase
{

namespace sql
{

namespace iceberg
{

/**
*  An Avro datafile that can store objects of type T.
*/
template<typename T>
class DataFileWriter {
  std::unique_ptr<DataFileWriterBase> base_;

public:
  /**
  * Constructs a new data file.
  */
  DataFileWriter(const char *filename, const ValidSchema &schema,
                 size_t syncInterval = 16 * 1024, Codec codec = NULL_CODEC) : base_(new DataFileWriterBase(filename, schema, syncInterval, codec)) {}

  DataFileWriter(std::unique_ptr<OutputStream> outputStream, const ValidSchema &schema,
                 size_t syncInterval = 16 * 1024, Codec codec = NULL_CODEC) : base_(new DataFileWriterBase(std::move(outputStream), schema, syncInterval, codec)) {}

  DataFileWriter(const char *filename, const ValidSchema &schema,
                 const std::map<std::string, std::string> &metadata,
                 size_t syncInterval = 16 * 1024, Codec codec = NULL_CODEC) : base_(new DataFileWriterBase(fileOutputStream(filename), schema, syncInterval, codec, metadata)) {}

  DataFileWriter(std::unique_ptr<OutputStream> outputStream, const ValidSchema &schema,
                 const std::map<std::string, std::string> &metadata,
                 size_t syncInterval = 16 * 1024, Codec codec = NULL_CODEC) : base_(new DataFileWriterBase(std::move(outputStream), schema, syncInterval, codec, metadata)) {}
  DataFileWriter(const DataFileWriter &) = delete;
  DataFileWriter &operator=(const DataFileWriter &) = delete;

  /**
  * Writes the given piece of data into the file.
  */
  void write(const T &datum) {
    base_->syncIfNeeded();
    avro::encode(base_->encoder(), datum);
    base_->incr();
  }

  /**
  *  Returns the byte offset (within the current file) of the start of the current block being written.
  */
  uint64_t getCurrentBlockStart() { return base_->getCurrentBlockStart(); }

  /**
  * Closes the current file. Once closed this datafile object cannot be
  * used for writing any more.
  */
  void close() { base_->close(); }

  /**
  * Returns the schema for this data file.
  */
  const ValidSchema &schema() const { return base_->schema(); }

  /**
  * Flushes any unwritten data into the file.
  */
  void flush() { base_->flush(); }
};


class ObAvroOutputStream : public OutputStream {
public:
  ObAvroOutputStream(ObStorageAppender& storage_appender, size_t bufferSize = 8 * 1024)
    : storage_appender_(storage_appender),
      bufferSize_(bufferSize),
      buffer_(nullptr),
      next_(nullptr),
      available_(bufferSize_),
      byteCount_(0),
      allocator_(ObMemAttr(MTL_ID(), "AvroWrite"))
  {
    void *ptr = NULL;
    int ret = OB_SUCCESS;
    if (OB_ISNULL(ptr = allocator_.alloc(bufferSize))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate buffer", K(bufferSize));
      throw std::bad_alloc();
    } else {
      buffer_ = static_cast<uint8_t*>(ptr);
      next_ = buffer_;
    }
  }

  ~ObAvroOutputStream() override
  {
    allocator_.reset();
    buffer_ = nullptr;
    next_ = nullptr;
  }
  /**
   * Returns a buffer that can be written into.
   * On successful return, data has the pointer to the buffer
   * and len has the number of bytes available at data.
   */
  // Invariant: byteCount_ == bytesWritten + bufferSize_ - available_;
  bool next(uint8_t **data, size_t *len) final
  {
    if (available_ == 0) {
      flush();
    }
    *data = next_;
    *len = available_;
    next_ += available_;
    byteCount_ += available_;
    available_ = 0;
    return true;
  }

  /**
   * "Returns" back to the stream some of the buffer obtained
   * from in the last call to next().
   */
  void backup(size_t len) final
  {
    available_ += len;
    next_ -= len;
    byteCount_ -= len;
  }

  /**
   * Number of bytes written so far into this stream. The whole buffer
   * returned by next() is assumed to be written unless some of
   * it was returned using backup().
   */
  uint64_t byteCount() const final
  {
    return byteCount_;
  }

  /**
   * Flushes any data remaining in the buffer to the stream's underlying
   * store, if any.
   */
  void flush() final
  {
    int ret = OB_SUCCESS;
    int64_t write_size = 0;
    if (OB_FAIL(storage_appender_.append(reinterpret_cast<char*>(buffer_), bufferSize_ - available_, write_size))) {
      LOG_WARN("failed to append data", K(ret), K(bufferSize_), K(available_), KP(buffer_), K(write_size), K(lbt()));
      throw std::runtime_error("failed to append data");
    } else {
      next_ = buffer_;
      available_ = bufferSize_;
    }
  }

  std::unique_ptr<OutputStream> static create_avro_output_stream(ObStorageAppender& storage_appender,
                                                                 size_t bufferSize = 8 * 1024) {
    return std::make_unique<ObAvroOutputStream>(storage_appender, bufferSize);
  }

private:
  ObStorageAppender& storage_appender_;
  size_t bufferSize_;
  uint8_t *buffer_;
  uint8_t *next_;
  size_t available_;
  size_t byteCount_;
  common::ObArenaAllocator allocator_;
};

} // namespace iceberg
} // namespace sql
} // namespace oceanbase

#endif // AVRO_DATA_FILE_WRITER_H