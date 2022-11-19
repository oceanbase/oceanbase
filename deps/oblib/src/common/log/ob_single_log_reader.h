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

#ifndef OCEANBASE_COMMON_OB_SINGLE_LOG_READER_
#define OCEANBASE_COMMON_OB_SINGLE_LOG_READER_

#include "lib/allocator/ob_malloc.h"
#include "lib/file/ob_file.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "common/data_buffer.h"
#include "common/log/ob_log_entry.h"

namespace oceanbase
{
namespace common
{
class ObSingleLogReader
{
public:
  static const int64_t LOG_BUFFER_MAX_LENGTH;
public:
  ObSingleLogReader();
  virtual ~ObSingleLogReader();

  /**
   * @brief ObSingleLogReader初始化
   * ObSingleLogReader必须要先调用init函数进行初始化后, 才可以进行open和read_log调用
   * 初始化时, 会分配LOG_BUFFER_MAX_LENGTH长度的读缓冲区
   * 在析构函数中释放读缓冲区
   * @param [in] log_dir 日志文件夹
   * @return OB_SUCCESS 初始化成功
   * @return OB_INIT_TWICE 已经初始化
   * @return OB_ERROR 初始化失败
   */
  int init(const char *log_dir);

  /**
   * @brief 打开一个文件
   * open函数会打开日志文件
   * 调用close函数关闭日志文件后, 可以再次调用open函数打开其他日志文件, 缓冲区复用
   * @param [in] file_id 读取的操作日志文件id
   * @param [in] last_log_seq 上一条日志序号, 用于判断日志是否连续, 默认值0表示无效
   */
  int open(const uint64_t file_id, const uint64_t last_log_seq = 0);

  /**
   * @brief 关闭日志文件
   * 关闭已经打开的日志文件, 之后可以再次调用init函数, 读取其他日志文件
   */
  int close();

  /**
   * @brief 重置内部状态, 释放缓冲区内存
   */
  int reset();

  /**
   * @brief 从操作日志中读取一个更新操作
   * @param [out] cmd 日志类型
   * @param [out] log_seq 日志序号
   * @param [out] log_data 日志内容
   * @param [out] data_len 缓冲区长度
   * @return OB_SUCCESS: 如果成功;
   *         OB_READ_NOTHING: 从文件中没有读到数据
   *         others: 发生了错误.
   */
  virtual int read_log(LogCommand &cmd, uint64_t &log_seq, char *&log_data, int64_t &data_len) = 0;
  int revise();
  bool if_file_exist(const uint64_t file_id);
  inline uint64_t get_cur_log_file_id() const
  {
    return file_id_;
  }
  inline uint64_t get_last_log_seq_id() const
  {
    return last_log_seq_;
  }
  inline uint64_t get_last_log_offset() const
  {
    return pos_;
  }

  /// @brief is log file opened
  inline bool is_opened() const { return file_.is_opened(); }

  ///When @brief is initialized, get the maximum log file number in the current directory
  int get_max_log_file_id(uint64_t &max_log_file_id);
  int64_t get_cur_offset() const;

  inline void unset_dio() { dio_ = false; }

protected:
  int read_header(ObLogEntry &entry);
  int trim_last_zero_padding(int64_t header_size);
  int open_with_lucky(const uint64_t file_id, const uint64_t last_log_seq);
protected:
  /**
   * 从日志文件中读取数据到读缓冲区
   * @return OB_SUCCESS: 如果成功;
   *         OB_READ_NOTHING: 从文件中没有读到数据
   *         others: 发生了错误.
   */
  int read_log_();
protected:
  bool is_inited_;  //Initialization tag
  uint64_t file_id_;  //Log file id
  uint64_t last_log_seq_;  //Last log (Mutator) serial number
  ObDataBuffer log_buffer_;  //Read buffer
  char file_name_[OB_MAX_FILE_NAME_LENGTH];  //Log file name
  char log_dir_[OB_MAX_FILE_NAME_LENGTH];  //Log directory
  int64_t pos_;
  int64_t pread_pos_;
  ObFileReader file_;
  bool dio_;
};
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_SINGLE_LOG_READER_
