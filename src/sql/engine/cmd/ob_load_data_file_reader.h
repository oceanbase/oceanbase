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

#ifndef OCEANBASE_SQL_LOAD_DATA_FILE_READER_H_
#define OCEANBASE_SQL_LOAD_DATA_FILE_READER_H_

#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/file/ob_file.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "share/backup/ob_backup_struct.h"
#include "observer/mysql/obmp_packet_sender.h"

namespace oceanbase
{
namespace sql
{

class ObSQLSessionInfo;

struct ObFileReadParam
{
public:
  ObFileReadParam();
  TO_STRING_KV(K_(file_location), K_(filename), K_(timeout_ts));
  
public:
  ObLoadFileLocation file_location_;
  ObString filename_;
  ObCSVGeneralFormat::ObCSVCompression compression_format_;
  share::ObBackupStorageInfo access_info_;
  observer::ObIMPPacketSender *packet_handle_;
  ObSQLSessionInfo *session_;
  int64_t timeout_ts_;  // A job always has a deadline and file reading may cost a long time

public:
  static int parse_compression_format(ObString compression_name,
                                      ObString filename,
                                      ObCSVGeneralFormat::ObCSVCompression &compression_format);
};

class ObFileReader
{
public:
  ObFileReader(ObIAllocator &allocator) : allocator_(allocator) {}
  virtual ~ObFileReader() {}

  /**
   * read data from file into the buffer
   *
   * @note read_size equals to 0 does not mean end of file.
   *       You should call `eof` to decide whether end of file.
   *       This is not the same with the system call `read`.
   */
  virtual int  read(char *buf, int64_t count, int64_t &read_size) = 0;

  /**
   * get the file size
   *
   * Stream files may not support this feature.
   */
  virtual int  get_file_size(int64_t &file_size) = 0;

  /**
   * seek to the specific position and the `read` subsequently fetch data from the position
   *
   * You can use `seekable` to check whether this file can read at random position.
   */
  virtual int  seek(int64_t offset) = 0;
  virtual bool seekable() const { return true; }
  virtual int64_t get_offset() const = 0;
  virtual bool eof() const = 0;

  /**
   * read data until we got `count` bytes data or exception occurs
   *
   * This routine calls `read` repeatly until we got `count` bytes
   * data.
   * As usual, the normal `read` try to read data once and return.
   */
  int readn(char *buffer, int64_t count, int64_t &read_size);

  /**
   * A file reader factory
   */
  static int open(const ObFileReadParam &param, ObIAllocator &allocator, ObFileReader *& file_reader);
  static void destroy(ObFileReader *file_reader);

private:
  static int open_decompress_reader(const ObFileReadParam &param,
                                    ObIAllocator &allocator,
                                    ObFileReader *source_reader,
                                    ObFileReader *&file_reader);

protected:
  ObIAllocator &allocator_;
};

/**
 * Stream file that can read sequential only
 */
class ObStreamFileReader : public ObFileReader
{
public:
  ObStreamFileReader(ObIAllocator &allocator): ObFileReader(allocator) {}
  virtual ~ObStreamFileReader() {}

  int  get_file_size(int64_t &file_size) override { return OB_NOT_SUPPORTED; }
  int  seek(int64_t offset) override { return OB_NOT_SUPPORTED; }
  bool seekable() const override { return false; }
};

class ObRandomFileReader : public ObFileReader
{
public:
  ObRandomFileReader(ObIAllocator &allocator);
  virtual ~ObRandomFileReader();

  int  read(char *buf, int64_t count, int64_t &read_size) override;
  int  seek(int64_t offset) override;
  int  get_file_size(int64_t &file_size) override;
  int64_t get_offset() const override { return offset_; }
  bool eof() const override { return eof_; }

  int open(const ObString &filename);
  
private:
  ObString             filename_;
  common::ObFileReader file_reader_;
  int64_t              offset_;
  bool                 eof_;
  bool                 is_inited_;
};

class ObRandomOSSReader : public ObFileReader
{
public:
  ObRandomOSSReader(ObIAllocator &allocator);
  virtual ~ObRandomOSSReader();
  int open(const share::ObBackupStorageInfo &storage_info, const ObString &filename);
  
  int read(char *buf, int64_t count, int64_t &read_size) override;
  int seek(int64_t offset) override;
  int get_file_size(int64_t &file_size) override;
  int64_t get_offset() const override { return offset_; }
  bool eof() const override { return eof_; }
  
private:
  ObIODevice *device_handle_;
  ObIOFd      fd_;
  int64_t     offset_;
  bool        eof_;
  bool        is_inited_;
};

/**
 * A strem file reader whose data source is mysql packets
 * Refer to LOAD DATA LOCAL INFILE for more detail.
 * Read data flow:
 * client send file content through mysql packets
 * (@see PacketStreamFileReader::read) and end with an
 * empty mysql packet.
 */
class ObPacketStreamFileReader : public ObStreamFileReader
{
public:
  ObPacketStreamFileReader(ObIAllocator &allocator);
  virtual ~ObPacketStreamFileReader();
  
  int open(const ObString &filename,
           observer::ObIMPPacketSender &packet_handle,
           ObSQLSessionInfo *session,
           int64_t timeout_ts);

  int read(char *buf, int64_t count, int64_t &read_size) override;
  int64_t get_offset() const override { return read_size_; }
  bool eof() const override { return eof_; }

private:
  int receive_packet();

  /// The packet read from NIO is cached, so we must release it explicitly
  /// and then we can reuse the resource
  int release_packet();
  
  bool is_timeout() const;
  bool is_killed() const;
  
private:
  observer::ObIMPPacketSender *packet_handle_; // We use this handle to read packet from client
  ObSQLSessionInfo *session_;
  int64_t timeout_ts_; // The deadline of job
  
  // As we read a packet from client, the NIO store the data into the NIO buffer
  // and allocate an ObPacket by an allocator(arena_allocator_). The ObPacket(cached_packet_)
  // is cached in the memory of allocator.
  // NOTE: arena_allocator only ccache the size is smaller than it's page_size(8K,16K, etc)
  ObArenaAllocator arena_allocator_;
  obmysql::ObMySQLRawPacket *cached_packet_;
  
  int64_t received_size_;  // All data received in bytes
  int64_t read_size_;      // All data has been read in bytes
  bool    eof_;
};

/**
 * base class for stream decompressor
 */
class ObDecompressor
{
public:
  explicit ObDecompressor(ObIAllocator &allocator);
  virtual ~ObDecompressor();

  virtual int  init() = 0;
  virtual void destroy() = 0;
  virtual int  decompress(const char *src, int64_t src_size, int64_t &consumed_size,
                          char *dest, int64_t dest_capacity, int64_t &decompressed_size) = 0;

  virtual ObCSVGeneralFormat::ObCSVCompression compression_format() const = 0;

  static int create(ObCSVGeneralFormat::ObCSVCompression format,
                    ObIAllocator &allocator,
                    ObDecompressor *&decompressor);
  static void destroy(ObDecompressor *decompressor);

protected:
  ObIAllocator &allocator_;
};

/**
 * stream decompress file reader
 */
class ObDecompressFileReader : public ObStreamFileReader
{
public:
  explicit ObDecompressFileReader(ObIAllocator &allocator);
  virtual ~ObDecompressFileReader();

  int open(const ObFileReadParam &param, ObFileReader *source_reader);

public:
  int read(char *buf, int64_t capability, int64_t &read_size) override;
  int64_t get_offset() const override { return uncompressed_size_; }
  bool eof() const override { return eof_; }

private:
  int read_compressed_data();

protected:
  ObFileReader *source_reader_ = nullptr;

  char *  compressed_data_    = nullptr; /// compressed data buffer
  int64_t compress_data_size_ = 0;       /// the valid data size in compressed data buffer
  int64_t consumed_data_size_ = 0;       /// handled buffer size in the compressed data buffer
  int64_t uncompressed_size_  = 0;       /// decompressed size from compressed data

  bool  eof_ = false;

  ObDecompressor *decompressor_ = nullptr;

  static const int64_t COMPRESSED_DATA_BUFFER_SIZE;
};

/**
 * gzip/deflate decompressor
 */
class ObZlibDecompressor : public ObDecompressor
{
public:
  explicit ObZlibDecompressor(ObIAllocator &allocator,
                              ObCSVGeneralFormat::ObCSVCompression compression_format);
  virtual ~ObZlibDecompressor();

  int  init() override;
  void destroy() override;

  int decompress(const char *src, int64_t src_size, int64_t &consumed_size,
                 char *dest, int64_t dest_capacity, int64_t &decompressed_size) override;

  ObCSVGeneralFormat::ObCSVCompression compression_format() const override { return compression_format_; }

private:
  void *zlib_stream_ptr_    = nullptr;
  bool  zstream_need_reset_ = false; // the zstreamptr should be reset if we got Z_STREAM_END

  ObCSVGeneralFormat::ObCSVCompression compression_format_;
};

/**
 * zstd decompressor
 */
class ObZstdDecompressor : public ObDecompressor
{
public:
  explicit ObZstdDecompressor(ObIAllocator &allocator);
  virtual ~ObZstdDecompressor();

  int  init() override;
  void destroy() override;

  int decompress(const char *src, int64_t src_size, int64_t &consumed_size,
                 char *dest, int64_t dest_capacity, int64_t &decompressed_size) override;

  ObCSVGeneralFormat::ObCSVCompression compression_format() const override
  {
    return ObCSVGeneralFormat::ObCSVCompression::ZSTD;
  }
private:
  void *zstd_stream_context_ = nullptr;
};

} // namespace sql
} // namespace oceanbase

#endif  // OCEANBASE_SQL_LOAD_DATA_FILE_READER_H_
