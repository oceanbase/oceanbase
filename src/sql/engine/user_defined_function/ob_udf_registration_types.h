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

#ifndef UDF_REGISTRATION_TYPES_H_
#define UDF_REGISTRATION_TYPES_H_


namespace oceanbase
{
namespace sql
{

/*
 * All these types are also defined in mysql.
 * Change all names.
 * */

/*
Type of the user defined function return slot and arguments
*/
enum UdfItemResult {
  INVALID_RESULT = -1, /* not valid for UDFs */
  STRING_RESULT = 0,   /* char * */
  REAL_RESULT,         /* double */
  INT_RESULT,          /* long long */
  ROW_RESULT,          /* not valid for UDFs */
  DECIMAL_RESULT       /* char *, to be converted to/from a decimal */
};

enum ItemUdfType { UDFTYPE_FUNCTION = 1, UDFTYPE_AGGREGATE };

/*
The user defined function args.
We fill all the data in stage of resolver except args and lengths.
Only got the row we are working on can we fill args and lengths.
*/
typedef struct UDF_ARGS {
  unsigned int arg_count;           /* Number of arguments */
  enum UdfItemResult *arg_type;     /* Pointer to item_results */
  char **args;                      /* Pointer to argument */
  unsigned long *lengths;           /* Length of string arguments */
  char *maybe_null;                 /* Set to 1 (not '1') for all maybe_null args */
  char **attributes;                /* Pointer to attribute name */
  unsigned long *attribute_lengths; /* Length of attribute arguments */
  void *extension;
} ObUdfArgs;

/*
Information about the result of a user defined function

@todo add a notion for determinism of the UDF.

@sa Item_udf_func::update_used_tables()
*/
typedef struct UDF_INIT {
  bool maybe_null;          /* 1 if function can return NULL */
  unsigned int decimals;    /* for real functions */
  unsigned long max_length; /* For string functions */
  char *ptr;                /* free pointer for function data */
  bool const_item;          /* 1 if function always returns the same value */
  void *extension;
} ObUdfInit;


/* udf helper function */
typedef void (*ObUdfFuncClear)(ObUdfInit *udf_init,
                               unsigned char *is_null,
                               unsigned char *error);

typedef void (*ObUdfFuncAdd)(ObUdfInit *udf_init,
                             ObUdfArgs *udf_args,
                             unsigned char *is_null,
                             unsigned char *error);

typedef void (*ObUdfFuncDeinit)(ObUdfInit *udf_init);

typedef bool (*ObUdfFuncInit)(ObUdfInit *udf_init,
                              ObUdfArgs *udf_args,
                              char *message);


/* udf process row function */
typedef void (*ObUdfFuncAny)(void);

typedef double (*ObUdfFuncDouble)(ObUdfInit *udf_init,
                                  ObUdfArgs *udf_args,
                                  unsigned char *is_null,
                                  unsigned char *error);

typedef long long (*ObUdfFuncLonglong)(ObUdfInit *udf_init,
                                       ObUdfArgs *udf_args,
                                       unsigned char *is_null,
                                       unsigned char *error);
 
typedef char *(*ObUdfFuncString)(ObUdfInit *initid,
                                 ObUdfArgs *args,
                                 char *result,
                                 unsigned long *length,
                                 unsigned char *is_null,
                                 unsigned char *error);

typedef void* ObUdfSoHandler;

}
}

#endif
