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

#ifndef OCEANBASE_SHARE_OB_ARCHIVE_PIECE_H_
#define OCEANBASE_SHARE_OB_ARCHIVE_PIECE_H_

#include "lib/ob_define.h"                 // int64_t..
#include "lib/utility/ob_print_utils.h"    // print
#include <cstdint>
#include "share/scn.h"    //SCN

namespace oceanbase
{
namespace share
{
/*
 * 归档数据需要支持离线拷贝, 按目录管理等需求, 因此在归档时需要确定某份数据属于哪一个备份目录
 * 在2.x/3.x方案中, 由rs调度多observer共同参与, 具体到每一条LogEntry需要明确放置到某一个目录下
 * 在4.x, 由observer根据clog文件包含日志时间维度范围自动按照目录管理策略, 决定某个日志文件备份目录
 *
 * 对于跨多个目录的日志文件, 需要在对应多个目录都备份满足实际时间约束的日志, 比如
 * 某个文件A包含日志范围跨越3天, 1d: {(log_id: 1-10)}, 2d: {(log_id: 11-15)}, 3d: {(log_id: 16-30)},
 *
 * 最终呈现效果可能是[d1:A(1-10)]/[d2:A(1-15)]/[d3:A(1-30)],
 * 也可能是[d1:A(1-30)]/[d2:A(1-30)]/[d3:A(1-30)]
 *
 * 公式 piece = (scn.convert_to_ts() - genesis_scn.convert_to_ts()) / interval + base_piece_id
 * 其中scn日志提交scn, genesis_ts为基准, interval为piece时间跨度默认为一天
 *
 * 实际生产环境以天为piece切分单元, 并且不对用户展示,
 * 测试环境支持更细粒度目录切分单元
 *
 * NB: piece切分单元配置仅支持未开启归档时更改, 一旦开启则不能修改
 * */
class ObArchivePiece final
{
public:
  //us
  const int64_t ONE_SECOND = 1000 * 1000L;
  const int64_t ONE_MINUTE = 60L * ONE_SECOND;
  const int64_t ONE_HOUR = 60L * ONE_MINUTE;
  const int64_t ONE_DAY = 24L * ONE_HOUR;

public:
  ObArchivePiece();
  ObArchivePiece(const SCN &scn, const int64_t interval_us, const SCN &genesis_scn, const int64_t base_piece_id);
  ~ObArchivePiece();

public:
  int64_t get_piece_id() const { return piece_id_; }
  int get_piece_lower_limit(share::SCN &scn);
  bool is_valid() const;
  void reset();
  int set(const int64_t piece_id, const int64_t interval_us, const SCN &genesis_scn, const int64_t base_piece_id);
  void inc();
  ObArchivePiece &operator=(const ObArchivePiece &other);
  ObArchivePiece &operator++();       // 前置++
  bool operator==(const ObArchivePiece &other) const;
  bool operator!=(const ObArchivePiece &other) const;
  bool operator>(const ObArchivePiece &other) const;
  TO_STRING_KV(K_(interval_us), K_(genesis_scn), K_(piece_id), K_(base_piece_id));

private:
  int64_t        interval_us_;    // piece时间长度
  SCN      genesis_scn_;  // 归档基准SCN
  int64_t        base_piece_id_; // 基准piece id
  int64_t        piece_id_;    // piece目录
};

} // namespace share
} // namespace oceanbase
#endif /* OCEANBASE_SHARE_OB_ARCHIVE_PIECE_H_ */
