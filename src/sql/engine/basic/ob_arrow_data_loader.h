/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_DATA_LOADER_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_DATA_LOADER_H_

#include <apache-arrow/arrow/api.h>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
struct ObDatum;
class ObIVector;
}

namespace sql
{
class ObEvalCtx;
class ObExpr;
struct ObDatumMeta;

/**
 * A shared loader that writes an arrow::Array into an OceanBase expression
 * vector. Callers must initialize the destination vector before load().
 */
class ObArrowDataLoader
{
public:
  virtual ~ObArrowDataLoader() { destroy(); }

  virtual int init(const arrow::DataType &arrow_type, const ObDatumMeta &datum_type)
  {
    UNUSED(arrow_type);
    UNUSED(datum_type);
    return OB_SUCCESS;
  }
  virtual void destroy() {}
  virtual int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) = 0;

  virtual int64_t to_string(char *buf, const int64_t buf_len) const
  {
    UNUSED(buf);
    UNUSED(buf_len);
    return 0;
  }
};

class ObArrowDataLoaderFactory final
{
public:
  ObArrowDataLoaderFactory() = default;

  int select_loader(common::ObIAllocator &allocator,
                    const arrow::DataType &arrow_type,
                    const ObDatumMeta &datum_type,
                    ObArrowDataLoader *&loader);
};

template <typename ArrowType>
class ObCopyableArrowDataLoader : public ObArrowDataLoader
{
public:
  ~ObCopyableArrowDataLoader() override { destroy(); }
  int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) override;
};

template <typename ArrowType>
class ObIntToInt64ArrowDataLoader : public ObArrowDataLoader
{
public:
  ~ObIntToInt64ArrowDataLoader() override { destroy(); }
  int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) override;
};

template <typename ArrowType>
class ObStringToStringArrowDataLoader : public ObArrowDataLoader
{
public:
  ~ObStringToStringArrowDataLoader() override { destroy(); }
  int init(const arrow::DataType &arrow_type, const ObDatumMeta &ob_type) override;
  int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) override;

private:
  using DatumSetter =
      int (*)(ObExpr *expr, ObEvalCtx &eval_ctx, const common::ObString &in_str,
              common::ObDatum &datum);
  DatumSetter datum_setter_ = nullptr;
};

template <typename ArrowType>
class ObStringToTimeArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const arrow::DataType &arrow_type, const ObDatumMeta &ob_type) override;
  int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) override;
};

template <typename ArrowType>
class ObStringToDateTimeArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const arrow::DataType &arrow_type, const ObDatumMeta &ob_type) override;
  int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) override;

private:
  using DateTimeHandler = int (*)(const common::ObString &, common::ObIVector *, int64_t);
  DateTimeHandler datetime_handler_ = nullptr;
};

template <typename ArrowType>
class ObStringToMysqlDateArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const arrow::DataType &arrow_type, const ObDatumMeta &ob_type) override;
  int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) override;
};

template <typename ArrowType>
class ObStringToDateArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const arrow::DataType &arrow_type, const ObDatumMeta &ob_type) override;
  int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) override;
};

template <typename ArrowType>
class ObStringToYearArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const arrow::DataType &arrow_type, const ObDatumMeta &ob_type) override;
  int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) override;
};

template <typename ArrowType>
class ObStringToTimestampArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const arrow::DataType &arrow_type, const ObDatumMeta &ob_type) override;
  int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) override;
};

template <typename ArrowType>
class ObBinaryToGisArrowDataLoader : public ObArrowDataLoader
{
public:
  int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) override;
};

class ObBoolToIntArrowDataLoader : public ObArrowDataLoader
{
public:
  int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) override;
};

class ObDecimalArrowDataLoader : public ObArrowDataLoader
{
public:
  int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) override;
};

class ObDecimalToIntArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const arrow::DataType &arrow_type, const ObDatumMeta &ob_type) override;
  int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) override;

private:
  int (*get_int64_func_)(const std::shared_ptr<arrow::DataType> &, const uint8_t *, int64_t &) = nullptr;
};

class ObDate32ToMysqlDateArrowDataLoader : public ObArrowDataLoader
{
public:
  int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) override;
};

template <typename ArrowType>
class ObTimeArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const arrow::DataType &arrow_type, const ObDatumMeta &ob_type) override;
  int load(const arrow::Array &array, ObEvalCtx &eval_ctx, ObExpr *expr) override;

private:
  int64_t muliples_ = 0;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_DATA_LOADER_H_
