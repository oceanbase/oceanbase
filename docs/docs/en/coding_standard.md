---
title: Coding Standard
---

| Number | Document Version | Revised Chapter | Reason for Revision | Revision Date   |
| -------| ---------------- | --------------- | ------------------- | -------------   |
| 1      | 1.0              |                 | New                 | June 15th, 2023 |

# 1 Introduction
This coding standard is applicable to the OceanBase project of Ant Group. It provides some coding constraints and defines coding styles. In the OceanBase project, the kernel code must comply with the coding style of this document, the test code is recommended to comply with the coding constraints of this document, and other codes must also comply with the coding constraints and coding style of this document.

This coding standard is committed to writing C/C++ code that is easy to understand, reduces traps, and has a unified format. Therefore:

- The most common and understandable way is used to write the code;
- Avoid using any obscure ways, such as "foo(int x = 1)";
- Avoid very technical ways, such as "a += b; b = a-b; a -= b;" or "a ^= b; b ^= a; a ^= b;" to exchange the values of variables a and b.
  
Finally, this document summarizes the coding constraints for quick reference.
This coding standard will be continuously supplemented and improved as needed.

# 2 Directory and Files
## 2.1 Directory Structure

The subdirectories of the OceanBase system are as follows:
- src: contains source code, including header files and implementation files
- unittest: contains unit test code and small-scale integration test code written by developers
- tools: contains external tools
- docs: contains documentation
- rpm: contains RPM spec files
- script: contains operation and maintenance scripts for OceanBase.

Implementation files for C code are named ".c", header files are named ".h", implementation files for C++ code are named ".cpp", and header files are named ".h". In principle, header files and implementation files must correspond one-to-one, and directories under "src" and "unittest" must correspond one-to-one. All file names are written in lowercase English letters, with words separated by underscores ('_').

For example, under the "src/common" directory, there is a header file named "ob_schema.h" and an implementation file named "ob_schema.cpp". Correspondingly, under the "unittest/common" directory, there is a unit test file named "test_schema.cpp".

Of course, developers may also perform module-level or multi-module integration testing. These testing codes are also placed under the "unittest" directory, but subdirectories and file names are not required to correspond one-to-one with those under "src". For example, integration testing code for the Baseline Storage Engine is placed under the "unittest/storagetest" directory.

## 2.2 Copyright Information

Currently (as of May 2023), all source code files in Observer must include the following copyright information in the file header:

```cpp
Copyright (c) 2021 OceanBase
OceanBase is licensed under Mulan PubL v2.
You can use this software according to the terms and conditions of the Mulan PubL v2.
You may obtain a copy of Mulan PubL v2 at:
         http://license.coscl.org.cn/MulanPubL-2.0
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PubL v2 for more details.
```

## 2.3 Header File Code

Header files should not contain implementation code, with the exception of inline functions or C++ templates. Additionally, if the template code is too long, some or all of the template functions can be extracted to a dedicated .ipp file, as demonstrated in "common/ob_vector.h" and "common/ob_vector.ipp".

Header files should be as concise and clear as possible, making them easy for users to understand. If the implementation code of some functions is short and you wish to place them directly in the header file, the functions must be declared as inline functions and comply with the standards for inline functions.

## 2.4 Define Protection

All header files should use `#define` to prevent multiple inclusions of the same file. The naming format should be: \_. For example, the header file "common/ob_schema.h" in the common module should be protected in the following way:

```cpp
#ifndef OCEANBASE_COMMON_OB_SCHEMA_
#define OCEANBASE_COMMON_OB_SCHEMA_
…
#endif // OCEANBASE_COMMON_OB_SCHEMA_
```

## 2.5 Header File Dependencies

When using other classes in a header file, try to use forward declaration instead of `#include`.

When a header file is included, new dependencies may be introduced. If this header file is modified, the code will be recompiled. If this header file also includes other header files, any changes to those header files will cause all code that includes that header file to be recompiled. Therefore, we tend to reduce the number of included header files, especially in header files that include other header files.

Using forward declaration can significantly reduce the number of header files needed. For example, if a header file uses the class "ObFoo" but does not need to access the declaration of "ObFoo", the header file only needs to include the forward declaration `class ObFoo;`, without needing to `#include "ob_foo.h"`.

Without being able to access the class definition, what operations can we perform on a class "ObFoo" in a header file?
- We can declare the data member type as `ObFoo *` or `ObFoo &`.
- We can declare the function parameter/return value type as ObFoo (but cannot define the implementation).
- We can declare the type of a static data member as ObFoo, because the definition of the static data member is outside the class definition.

On the other hand, if your class is a subclass of ObFoo or contains a non-static data member of type `ObFoo`, you must include the "ob_foo.h" header file.
Of course, if using pointer members instead of object members reduces code readability or execution efficiency, do not do so just to avoid `#include`.

## 2.6 Inline Functions

In order to improve execution efficiency, sometimes we need to use inline functions, but it is important to understand how inline functions work. It is recommended to only use inline functions in performance-critical areas, with the executed code being less than 10 lines and not including loops or switch statements, and without using recursive mechanisms.

Inline functions are most commonly used in C++ classes to define access functions. On one hand, inlining the function can avoid function call overhead, making the target code more efficient; on the other hand, each inline function call will copy the code, leading to an increase in the total program code. If the code inside the function body is relatively long, or there are loops in the function body, the time to execute the code in the function body will be greater than the overhead of the function call, making it unsuitable for inlining.

Constructors and destructors of classes can be misleading. They may appear to be short, but be careful as they may hide some behavior, such as "secretly" executing the constructors and destructors of base classes or member objects.

## 2.7 #include Path and Order

Header files within a project should be imported according to the project directory tree structure and should not use special paths such as ".", "..", etc. It is recommended to include header files in the following order: the header file corresponding to the current file, system C header files, system C++ header files, other library header files (libeasy, tbsys), and other internal header files of OceanBase, to avoid multiple inclusions. The system C header files use angle brackets and end with ".h", system C++ header files use angle brackets and do not end with ".h", and other cases use quotes. For example:

```cpp
#include <stdio.h>
#include <algorithm>
#include "common/ob_schema.h"
```

The reason for placing the header file corresponding to the current file in the priority position is to reduce hidden dependencies. We hope that each header file can be compiled independently. The simplest way to achieve this is to include it as the first .h file in the corresponding .cpp file.

For example, the include order of "ob_schema.cpp" is as follows:

```cpp
#include "common/ob_schema.h"
 
#include <string.h>
#include <stdlib.h>
#include <assert.h>
 
#include <algorithm>
 
#include "config.h"
#include "tblog.h"
 
#include "common/utility.h"
#include "common/ob_obj_type.h"
#include "common/ob_schema_helper.h"
#include "common/ob_define.h"
#include "common/file_directory_utils.h"
```

## 2.8 Summary

The subdirectories in src and unittest correspond to each other, and "tests" is used to store test code.

Header files should not include implementation code, with the exception of inline functions and templates.

Use `#define` to protect header files from being included multiple times.

Reduce compilation dependencies through forward declaration to prevent a change in one file triggering a domino effect.

Proper use of inline functions can improve execution efficiency.

The include paths for files within a project should use relative paths, and the include order should be: the header file corresponding to the current file, system C header files, system C++ header files, other library header files (Libeasy, tbsys), and other internal header files of OceanBase.

# 3 Scope 
## 3.1 Namespace 
All variables, functions, and classes in the OceanBase source code are distinguished by namespaces, with namespaces corresponding to the directories where the code is located. For example, the namespace corresponding to "ob_schema.h" in the "src/common" directory is "oceanbase::common".

```cpp
// .h file
namespace oceanbase
{
// Please do not indent
namespace common
{
// All declarations should be placed in namespaces, and please do not indent.
class ObSchemaManager
{
public:
  int func();
};
} // namespace common
} // namespace oceanbase
 
// .cpp file
namespace oceanbase
{
namespace common
{
// All function implementations should also be placed in namespaces.
int ObSchemaManager::func()
{
  ...
}
 
} // namespace common
} // namespace oceanbase
```

**It is prohibited to use anonymous namespaces** because the compiler will assign a random name string to the anonymous namespace, which can affect GDB debugging.

Both header and implementation files may include references to classes in other namespaces. For example, declare classes in other namespaces in header files, as follows:

```cpp
namespace oceanbase
{
namespace common
{
class ObConfigManager; // Forward declaration of the class common::ObConfigManager
}
 
namespace chunkserver
{
 
class ObChunkServer
{
public:
  int func();
};
 
} // namespace chunkserver
} // namespace oceanbase
```

C++ allows the use of using, which can be divided into two categories:

1. Using directive: For example, using namespace common, which allows the compiler to automatically search for symbols in the common namespace from now on.

2. Using declaration: For example, using `common::ObSchemaManager`, which makes `ObSchemaManager` equivalent to `common::ObSchemaManager` from now on.

Because using directive is likely to pollute the scope, **it is prohibited to use it in header files**, but using declaration is allowed. In .cpp files, using directive is allowed, for example, when implementing `ObChunkServer`, it may need to use classes from the common namespace. However, it is important to note that only other namespaces can be introduced using directives in .cpp files. The code in the .cpp file itself still needs to be put in its own namespace. For example: 

```cpp
// incorrect ways of using
// The implementation code should be put in the chunkserver namespace 
// instead of using the using namespace chunkserver directive.
namespace oceanbase
{
using namespace common;
using namespace chunkserver;
 
// using symbols from the common namespace
int ObChunkServer::func()
{
  ...
}
 
} // namespace oceanbase
 
// The correct way is to put the implementation code in the chunkserver namespace.
namespace oceanbase
{
using namespace common;
 
namespace chunkserver
{
// Using symbols from the common namespace
int ObChunkServer::func()
{
  ...
}
 
} // namespace chunkserver
} // namespace oceanbase
```
## 3.2 Nested Classes 
If a class is a member of another class, it can be defined as a nested class. Nested classes are also known as member classes.

```cpp
class ObFoo
{
private:
  // ObBar is a nested class/member class inside ObFoo, 
  // and ObFoo is referred to as the host class/outer class."
  class ObBar
  {
   ...
  };
};
```

When a nested class is only used by the outer class, it is recommended to place it within the scope of the outer class to avoid polluting other scopes with the same class name. It is also recommended to forward declare the nested class in the outer class's .h file, and define the nested class's implementation in the .cpp file, to improve readability by avoiding the inclusion of the nested class's implementation in the outer class's .h file. 

Additionally, it is generally advised to avoid defining nested classes as `public`, unless they are part of the external interface.

## 3.3 Global Variables and Functions
The use of global variables or functions should be strictly limited. New global variables and functions should not be added, except for those that already exist. 

If it is necessary to violate this guideline, please discuss and obtain approval beforehand, and provide detailed comments explaining the reason.

Global variables and functions can cause a range of issues, such as naming conflicts and uncertainties in the initialization order for global objects. If it is necessary to share a variable globally, it should be placed in a server singleton, such as `ObUpdateServerMain` in `UpdateServer`.

Global constants should be defined in `ob_define.h`, and global functions should be defined in the `common/ob_define.h` and utility methods (`common/utility.h`, `common/ob_print_utils.h`).

**It is prohibited to define global const variables in header files.**

Similar to the reason for prohibiting the use of static variables in header files, global const variables (including constexpr) without explicit extern have internal linkage, and multiple copies will be generated in the binary program.

**Experimental analysis**
```cpp
// "a.h"
const int zzz = 1000;
extern const int bbb;
```

```cpp
// "a.cpp"
#include "a.h"
#include <stdio.h>
const int bbb = 2000;
void func1()
{
  printf("a.cpp &zzz=%p\n", &zzz);
  printf("a.cpp &bbb=%p\n", &bbb);
}
```

```cpp
// "b.cpp"
#include "a.h"
#include <stdio.h>
void func2()
{
  printf("b.cpp &zzz=%p\n", &zzz);
  printf("b.cpp &bbb=%p\n", &bbb);
}
```

```cpp
// "main.cpp"
void func2();
void func1();
int main(int argc, char *argv[])
{
  func1();
  func2();
  return 0;
}
```

The compiled and executed program shows that multiple instances of the variable "zzz" are created, while only one instance of the variable "bbb" exists.

```cpp
[OceanBase224004 tmp]$ ./a.out
a.cpp &zzz=0x4007e8
a.cpp &bbb=0x400798
b.cpp &zzz=0x400838
b.cpp &bbb=0x400798
```

## 3.4 Local Variables
**It is recommended to declare variables at the beginning of a statement block.**

Simple variable declarations should be initialized when declared.

OceanBase believes that declaring variables at the beginning of each statement block leads to more readable code. Additionally, OceanBase allows for code such as `for (int i = 0; i < 10; ++i)` where the variable 'i' is declared at the beginning of the loop statement block. If the declaration and use of a variable are far apart, it indicates that the statement block contains too much code, which often means that the code needs to be refactored.

Declaring variables inside a loop body can be inefficient, as the constructor and destructor of an object will be called each time the loop iterates, and the variable will need to be pushed and popped from the stack each time. Therefore, it is recommended to extract such variables from the loop body to improve efficiency. **It is prohibited to declare complex variables** (e.g. class variables) inside a loop body, but if it is necessary to do so, approval from the team leader must be obtained, and a detailed comment explaining the reason must be provided. For the sake of code readability, declaring references inside a loop body is allowed.

```cpp
// Inefficient implementation
for (int i = 0; i < 100000; ++i) {
  ObFoo f;  // The constructor and destructor are called every time the loop is entered
  f.do_something();
}
 
// Efficient implementation
ObFoo f;
for (int i = 0; i < 100000; ++i) {
  f.do_something();
}

// For readability, references can be declared inside the loop
for(int i = 0; i < N; ++i) {
   const T &t = very_long_variable_name.at(i);
   t.f1();
   t.f2();
   ...
}
```

In addition, OceanBase sets limits on the size of local variables and does not recommend defining excessively large local variables.
1. The function stack should not exceed 32K.
2. A single local variable should not exceed 8K.

## 3.5 Static Variables
**Defining static variables in header files is prohibited**
Initializing static variables (whether const or not) is not allowed in .h header files, except for the following one exception. Otherwise, such static variables will produce a static stored variable in each compilation unit (.o file) and result in multiple instances of static variables after linking. If it is a const variable, it will cause the binary program file to bloat. If it is not a const variable, it may cause severe bugs. 

Note that defining (define) is prohibited, not declaring (declare).

**[Exception] Static const/constexpr static member variables**

Static member variables such as const int (including `int32_t`, `int64_t`, `uint32_t`, `uint64_t`, etc.), `static constexpr double`, etc. are often used to define hardcode array lengths. They do not occupy storage, do not have addresses (can be regarded as `#define` macro constants), and are allowed to be initialized in header files. 

Does that mean the following form (pseudocode) is allowed.

```cpp
class Foo {
  static const/constexpr xxx = yyy;
};
```

The explanation for this exception is as follows: In C++98, it is allowed to define the value of a static const integer variable when it is declared.

```cpp
class ObBar
{
public:
  static const int CONST_V = 1;
};
```

The fact is that the C++ compiler considers the following code equivalent to the previous one.

```cpp
class ObBar
{
  enum { CONST_V = 1 };
}
```

If the address of this type of variable is taken in the program, an "Undefined reference" error will occur during linking. In such cases, the correct approach is to place the definition of the variable in the .cpp file.

```cpp
// in the header file
class ObBar
{
  static const int CONST_V;
}
// in the implementation file
const int ObBar::CONST_V = 1;
```

Before C\+\+11, the C\+\+98 standard only allowed static const variables of integral type to be initialized with definitions included in the class declaration. In C++11, constexpr is introduced, and static constexpr member variables (including types such as double) can also be initialized in the declaration. This kind of variable will not generate static area storage after compilation.

> Before C++11, the values of variables could be used in constant expressions only if the variables are declared const, have an initializer which is a constant expression, and are of integral or enumeration type. C++11 removes the restriction that the variables must be of integral or enumeration type if they  are defined with the constexpr keyword:
>
> constexpr double earth_gravitational_acceleration = 9.8;
> constexpr double moon_gravitational_acceleration = earth_gravitational_acceleration / 6.0;

> Such data variables are implicitly const, and must have an initializer which must be a constant expression.

**Case 1**

According to the current code style of OceanBase, we will define static variables (such as `ob_define.h`) in the header file, so that each cpp file will generate a declaration and definition of this variable when including this header file. In particular, some large objects (latch, wait event, etc.) generate a static definition in the header file, resulting in the generation of binary and memory expansion.

Simply move the definition of several static variables from the header file to the cpp file, and change the header file to extern definition, the effect is quite obvious:
binary size: 2.6G->2.4G, reduce 200M.
Observer initial running memory: 6.3G->5.9G, reduced by 400M.

**Case 2**
In the example below, different cpps see different copies of global variables. It was originally expected to communicate through global static, but it turned out to be different. This will also result in a "false" singleton implementation.

**Analysis of behavior of static variables**
Let's write a small program to verify the performance of static variable definitions in .h.

```cpp
// "a.h"
static unsigned char xxx[256]=
{
  1, 2, 3
};
static unsigned char yyy = 10;
static const unsigned char ccc = 100;
```

```cpp
// "a.cpp"
#include "a.h"
#include <stdio.h>
void func1()
{
  printf("a.cpp &xxx=%p\n", xxx);
  printf("a.cpp &yyy=%p\n", &yyy);
  printf("a.cpp &ccc=%p\n", &ccc);
}
```

```cpp
// "b.cpp"
#include "a.h"
#include <stdio.h>
void func2()
{
  printf("b.cpp xxx=%p\n", xxx);
  printf("b.cpp &yyy=%p\n", &yyy);
  printf("b.cpp &ccc=%p\n", &ccc);
}
```

```cpp
// "main.cpp"
void func2();
void func1();
int main(int argc, char *argv[])
{
  func1();
  func2();
  return 0;
}
```

Compile and execute, and you can see that whether it is a static integer or an array, whether there is const or not, multiple instances are generated.

```txt
[OceanBase224004 tmp]$ g++ a.cpp b.cpp main.cpp
[OceanBase224004 tmp]$ ./a.out
a.cpp &xxx=0x601060
a.cpp &yyy=0x601160
a.cpp &ccc=0x400775
b.cpp xxx=0x601180
b.cpp &yyy=0x601280
b.cpp &ccc=0x4007a2
```

## 3.6 Resource recovery and parameter recovery
Resource management follows the principle of "who applies for release" and releases resources uniformly at the end of the statement block. If it must be violated, please obtain the consent of the group leader in advance, and explain the reason in detail.
The code structure of each statement block is as follows:
1. Variable definition
2. Resource application
3. Business logic
4. Resource release

```cpp
// wrong
void *ptr = ob_malloc(sizeof(ObStruct), ObModIds::OB_COMMON_ARRAY);
if (NULL == ptr) {
  // print error log
} else {
  if (OB_SUCCESS != (ret = do_something1(ptr))) {
    // print error log
    ob_tc_free(ptr, ObModIds::OB_COMMON_ARRAY);
    ptr = NULL;
  } else if (OB_SUCCESS != (ret = do_something2(ptr))) {
    // print error log
    ob_free(ptr, ObModIds::OB_COMMON_ARRAY);
    ptr = NULL;
  } else { }
}
```

```cpp
// correct
void *ptr = ob_malloc(100, ObModIds::OB_COMMON_ARRAY);
if (NULL == ptr) {
  // print error log
} else {
  if (OB_SUCCESS != (ret = do_something1(ptr))) {
    // print error log
  } else if (OB_SUCCESS != (ret = do_something2(ptr))) {
    // print error log
  } else { }
}
// release resource
if (NULL != ptr) {
  ob_free(ptr, ObModIds::OB_COMMON_ARRAY);
  ptr = NULL;
}
```

In the above example, the outermost `if` branch only judges the failure of resource application, and the `else` branch handles the business logic. Therefore, the code for resource release can also be placed at the end of the outermost `else` branch.

```cpp
// Another correct way of writing requires the if branch 
// to simply handle resource application failures
void *ptr = ob_malloc(100, ObModIds::OB_COMMON_ARRAY);
if (NULL == ptr) {
  // print error log
} else {
  if (OB_SUCCESS != (ret = do_something1(ptr))) {
    // print error log
  } else if (OB_SUCCESS != (ret = do_something2(ptr))) {
    // print error log
  } else { }
  // release resources
  ob_free(ptr, ObModIds::OB_COMMON_ARRAY);
  ptr = NULL;
}
```

Therefore, if resources need to be released, the resources should be released uniformly before the function returns or at the end of the outermost else branch.

In some cases, it is necessary to save the input parameters at the beginning of the statement block and restore the parameters in case of an exception. Similar to resource reclamation, parameters can only be restored at the end of a statement block. The most typical example is the serialization function, such as:

```cpp
// wrong
int serialize(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t ori_pos = pos;
 
  if (OB_SUCCESS != (ret = serialize_one(buf, buf_len, pos)) {
    pos = ori_pos;
    ...
  } else if (OB_SUCCESS != (ret = serialize_two(buf, buf_len, pos)) {
    pos = ori_pos;
    ...
  } else {
    ...
  }
  return ret;
}
```

The problem with this usage is that it is likely to forget to restore the value of pos in a certain branch. The correct way to write it is as follows.

```cpp
// Correct
int serialize(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t ori_pos = pos;
 
  if (OB_SUCCESS != (ret = serialize_one(buf, buf_len, pos)) {
    ...
  } else if (OB_SUCCESS != (ret = serialize_two(buf, buf_len, pos)) {
    ...
  } else {
    ...
  }
 
  if (OB_SUCCESS != ret) {
    pos = ori_pos;
  }
  return ret;
}
```

So if you need to restore the input parameters, do so before the function returns.

## 3.7 Summary

1. Namespaces correspond to directories. **Anonymous namespaces are prohibited**. **Using directives are prohibited in .h files, and only using declarations are allowed**.
2. Nested classes are suitable for scenarios that are only used by external classes. It is recommended to pre-declare in .h files and implement them in .cpp files. Try not to use public.
3. **In addition to the existing global variables and global functions, no new global variables and global functions shall be added**. If it must be violated, please obtain the consent of the group leader in advance, and explain the reason in detail.
4. **Local variables are declared at the beginning of the statement block, and it is mandatory to initialize simple variables when they are declared.**
5. **It is forbidden to declare non-simple variables in the loop body**. If it must be violated, please obtain the consent of the group leader in advance, and explain the reason in detail.
6. **Resource management follows the principle of "who applies for release"**. If resources need to be released, release them before the function returns or at the end of the outermost else branch. So if you need to restore the input parameters, do so before the function returns. If it must be violated, please obtain the consent of the group leader in advance, and explain the reason in detail.

# 4 Class
## 4.1 Constructors and Destructors
The constructor only performs trivial initialization work, such as initializing pointers to `NULL` and variables to 0 or -1. Non-trivial initialization operations are not allowed in the constructor. If necessary, define a separate `int init()` method and add an `is_inited_` variable to identify whether the object has been initialized successfully. This is because, if object construction fails, an indeterminate state may occur.

Every class (including interface classes) is required to define a constructor, even if the class does not have any member variables, it also needs to define an empty default constructor. This is because, if no constructor is defined, the compiler will automatically generate a default constructor, which often has some side effects, such as initializing some member variables to random values.

Every class (including interface classes) is required to define a destructor, even if the class does not have any member variables, it also needs to define an empty destructor. In addition, if there is no special reason (performance is particularly critical, it will not be inherited and does not contain virtual functions), the destructor of the class should be declared as virtual.

## 4.2 explicit keyword
Use the C++ keyword `explicit` for single-argument constructors.

Usually, a constructor with only one parameter can be used for conversion. For example, if `ObFoo::ObFoo(ObString name)` is defined, when an `ObString` is passed to a function that needs to pass in an `ObFoo` object, the constructor `ObFoo::ObFoo( ObString name)` will be automatically called and the string will be converted to a temporary `ObFoo` object passed to the calling function. This implicit conversion always brings some potential bugs.

## 4.3 Copy Constructor
**In principle, the copy constructor should not be used (except for the base classes that have already been defined)**. If it must be violated, please obtain the consent of the group leader in advance, and explain the reason in detail. In addition, classes that do not need a copy constructor should use the `DISALLOW_COPY_AND_ASSIGN` macro (`ob_define.h`), except for interface classes.

```cpp
#define DISALLOW_COPY_AND_ASSIGN(type_name) \
  type_name(const type_name&)               \
  void operator=(const type_name&)
 
class ObFoo
{
public:
  ObFoo();
  ~ObFoo();
 
private:
  DISALLOW_COPY_AND_ASSIGN(ObFoo);
};
```

## 4.4 reuse & reset & clear

**`reset` is used to reset the object, `reuse` is used to reuse the object, and clear is prohibited**. described as follows.

1. Both `reset` and `reuse` are used for object reuse. This `reuse` is often introduced in order to optimize performance because the memory allocation and construction of objects of certain key classes is too time-consuming.
2. The meaning of reset is to restore the state of the object to the initial state after the execution of the constructor or init function. Refer to `ObRow::reset`;
3. Use `reuse` in other situations except `reset`. Unlike `reset`, `reuse` often does not release some resources that are time-consuming to reapply, such as memory. Refer to `PageArena::reuse`;
4. `clear` is widely used in the container class of C++ STL, and often means to clear the size of the container class to 0, but it does not clear the internal objects or release the memory. The difference between `clear` and `reset/reuse` is very subtle. In order to simplify understanding, the use of `clear` is prohibited, and the used ones are gradually removed.

## 4.5 Member Initialization
**All members must be initialized, and the order in which member variables are initialized is consistent with the order in which they are defined.**

The `constructor`, `init` method, and `reset/reuse` method of each class may perform some initialization operations on the class, and it is necessary to ensure that all members have been initialized. If the constructor only initializes some members, then the `is_inited_` variable must be set to `false`, and the `init` method will continue to complete the initialization of other members. The members of the struct type can be initialized by the `reset` method (if the initialization is only to clear the struct members to 0, they can also be initialized by using `memset`); the members of the class type can be initialized by `init/reset/reuse` and other methods.
The initialization order of member variables needs to be consistent with the definition order. The advantage of this is that it is easy to find out whether you have forgotten which members have been initialized.

## 4.6 Structures and Classes
Use `struct` only when there is only data, and use `class` for everything else.

Struct is used on passive objects that only contain data, which may include associated constants, and `reset/is_valid`, serialization/deserialization these general functions. If you need more functions, `class` is more suitable. If in doubt, use class directly.

If combined with STL, you can use `struct` instead of `class` for functor and traits.

It should be noted that the data members inside the `class` can only be defined as `private` (private, except for static members), and can be accessed through the access functions `get_xxx` and `set_xxx`.

## 4.7 Common Functions
The common functions contained in each class must adopt standard prototypes, and the serialization/deserialization functions must be implemented using macros.
The general functions contained in each class include: `init`, `destroy`, `reuse`, `reset`, `deep_copy`, `shallow_copy`, `to_string`, `is_valid`. The prototypes of these functions are as follows:

```cpp
class ObFoo
{
public:
  int  init(init_param_list);
  bool is_inited();
  void destroy();
 
  void reset();
  void reuse();
  
  int deep_copy(const ObFoo &src);
  int shallow_copy(const ObFoo &src);
 
  bool is_valid();
 
  int64_t to_string(char *buf, const int64_t buf_len) const;

  NEED_SERIALIZE_AND_DESERIALIZE;
};
```

It should be noted that `to_string` will always add '\0' at the end, and the function returns the actual printed byte length (excluding '\0'). It is implemented internally by calling `databuff_printf` related functions, please refer to `common/ob_print_utils.h` for details.

Serialization and deserialization functions need to be implemented through macros, for example:

```cpp
class ObSort
{
public:
  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  common::ObSArray<ObSortColumn> sort_columns_;
  int64_t mem_size_limit_;
  int64_t sort_row_count_;
};
```

For class `ObSort`, the three fields that need to be serialized are
`sort_columns_`, `mem_size_limit_`, `sort_row_count_`. Just write in `ob_sort.cpp`:

```cpp
DEFINE_SERIALIZE_AND_DESERIALIZE(ObSort, sort_columns_, mem_size_limit_, sort_row_count_);
```

The serialization and deserialization can be completed and the realization of the three functions that have calculated the length after serialization can be completed.
The generic functions of structures are the same as the generic functions of classes.

## 4.8 Common Macro
For the convenience of coding, some defined macros can be used in OceanBase, but it is not recommended for students to add macros by themselves. If it is really necessary to add macros, please confirm with the team leader before adding them.

Here are some commonly used macros:

1. `OB_SUCC`

   It is usually used to judge whether the return value is `OB_SUCCESS`, which is equivalent to `OB_SUCCESS == (ret = func())`. Note that ret needs to be pre-defined in the function when using `OB_SUCC`, for example, the following writing method.

   ```cpp
   ret = OB_SUCCESS;
   if (OB_SUCC(func())) {
     // do something
   }
   ```

2. `OB_FAIL`

   It is usually used to judge whether the return value is not `OB_SUCCESS`, which is equivalent to `OB_SUCCESS != (ret = func())`. Note that ret needs to be pre-defined in the function when using `OB_FAIL`, for example, the following writing method.

   ```cpp
   ret = OB_SUCCESS;
   if (OB_FAIL(func())) {
     // do something
   }
   ```

3. `OB_ISNULL`

   It is usually used to judge whether the pointer is empty, which is equivalent to nullptr ==, for example, the following writing method.
   
   ```cpp
   if (OB_ISNULL(ptr)) {
      // do something
   }
   ```
   
4. `OB_NOT_NULL`

   It is usually used to judge whether the pointer is not empty, which is equivalent to nullptr !=, for example, the following writing method
   ```cpp
   if (OB_NOT_NULL(ptr)) {
     // do something
   }
   ```
   
5. `IS_INIT`

   It is usually used to judge whether the class has been initialized, which is equivalent to `is_inited_`. Note that the member `is_inited_` needs to exist in the class, for example, the following writing method.
   ```cpp
   if (IS_INIT) {
     // do something
   }
   ```

6. `IS_NOT_INIT`

    It is usually used to judge whether the class has been initialized, which is equivalent to `!is_inited_`. Note that the member `is_inited_` needs to exist in the class, for example, the following writing method.

   ```cpp
   if (IS_NOT_INIT) {
     // do something
   }
   ```

7. `REACH_TIME_INTERVAL`

   It is used to judge whether a certain time interval has been exceeded. The parameter is `us`. Note that there will be a static variable to record the time inside the macro, so the judgment of time is global. It is usually used to control the log output frequency. For example, the following writing method will Let the system do some actions after more than 1s interval.
   ```cpp
   if (REACH_TIME_INTERVAL(1000 * 1000)) {
     // do something
   }
   ```

8. `OZ`

   It is used to simplify the log output after `OB_FAIL`. When you only need to simply output the log after an error is reported, you can use `OZ`. Note that when using `OZ`, you need to define `USING_LOG_PREFIX` at the beginning of the cpp file. For example, the following writing method.

   ```cpp
   OZ(func());
   ```

   Equivalent to

   ```cpp
   if (OB_FAIL(func())) {
     LOG_WARN("fail to exec func, ", K(ret));
   }
   ```

9. `K`

   Usually used for log output, output variable name and variable value, such as the following writing.
   ```cpp
   if (OB_FAIL(ret)) {
     LOG_WARN("fail to exec func, ", K(ret));
   }
   ```

10. `KP`

    Usually used for log output, output variable names and pointers, such as the following writing method.
    ```cpp
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to exec func, ", K(ret), KP(ptr));
    }
    ```

## 4.9 Inherit

All inheritance must be `public`, and inheritance must be used with care: use inheritance only if it "is one", use composition if it "has one".

When a subclass inherits from a parent class, the subclass contains all the data and operation definitions of the parent class. In C++ practice, inheritance is mainly used in two scenarios: implementation inheritance, where the subclass inherits the implementation code of the parent class; interface inheritance, where the subclass inherits the method name of the parent class. For implementation inheritance, because the code that implements the subclass is extended between the parent class and the subclass, it becomes more difficult to understand its implementation, and it should be used with caution.

Multiple inheritance is also used in OceanBase. The scenario is rare, and requires at most one base class to contain implementation, and other base classes are pure interface classes.

## 4.10 Operator Overloading
**Except for container classes, custom data types (`ObString`, `ObNumber`, etc.) and a few global basic classes such as `ObRowkey`, `ObObj`, `ObRange`, etc., do not overload operators (except simple structure assignment operations)**. If it must be violated, please discuss and approve it in advance, and explain the reasons in detail.

C++ STL template classes have a large number of overloaded operators, such as comparison functions, four operators, self-increment, and self-decrement. Such codes seem to be more intuitive, but in fact they often confuse the caller, such as making the caller mistaken for some time-consuming Operations are as efficient as built-in operations.

Avoid overloading the assignment operator (operator=) for anything but simple constructs. If necessary, copy functions such as `deep_copy`, `shallow_copy`, etc. can be defined. Among them, `deep_copy` indicates that all members need deep copy, and `shallow_copy` indicates other situations. If some members need a shallow copy and some need a `deep copy`, then use `shallow_copy`.

## 4.11 Declaration Order
Use a specific declaration order in the header file, public before private, and member functions before data members.

The order of definition is as follows: `public` block, `protected` block, `private` block, and the internal order of each block is as follows:
1. typedefs and enums;
2. constant;
3. constructor;
4. destructor;
5. For member functions, static member functions come first, and ordinary member functions follow;
6. For data members, static data members come first, and ordinary data members follow;
The macro `DISALLOW_COPY_AND_ASSIGN` is placed after the `private:` block as the last part of the class.

The function definitions in the .cpp file should be as consistent as possible with the declaration order in the .h.

The reason why the constant definition should be placed in front of the function definition (constructor/destructor, member function) instead of in the data member is because the constant may be referenced by the function.

## 4.12 Summary
1. The constructor only does trivial initialization. Each class needs to define at least one constructor, and the destructor with virtual function or subclass is declared as virtual.
2. In order to avoid implicit type conversion, the single-argument constructor needs to be declared as explicit.
3. **In principle, the copy constructor shall not be used (except for the base classes that have been defined and used)**. If it must be violated, please discuss and approve it in advance, and explain the reasons in detail.
4. Use `DISALLOW_COPY_AND_ASSIGN` to avoid abuse of copy constructor and assignment operation;
5. **Use reset for class reset, reuse for reuse, and clear for prohibition.**
6. **It is necessary to ensure that all members are initialized, and the initialization sequence of member variables is consistent with the definition sequence.**
7. Use struct only when there is only data, and use class in all other cases.
8. The common functions contained in each class must use standard prototypes, and the serialization/deserialization functions must be implemented using macros.
9. Prefer composition and only use inheritance for "is-a" relationships. Avoid private inheritance and multiple inheritance. When multiple inheritance is used, it is required that except for one base class with implementation, the other base classes are pure interface classes.
10. **Except for existing container classes, custom types, and a small number of global basic classes, overloading of operators is not allowed (except for simple structure assignment operations)**. If it must be violated, please obtain the consent of the group leader in advance, and explain the reason in detail.
11. Declaration order: `public`, `protected`, `private`.

# 5 Function
## 5.1 Single Entry and Single Exit
**It is mandatory for all functions to return at the end, and it is forbidden to call global jump instructions such as `return`, `goto`, and exit halfway. If it must be violated, please discuss it in advance and explain the reason in detail.**

OceanBase believes that large-scale project development should give priority to avoiding common pitfalls, and it is worthwhile to sacrifice programming complexity. The single entry and single exit can make it difficult for developers to forget to release resources or restore function input parameters. At any time, the function is required to have only one exit.

## 5.2 Function Return Value
**Except for the following exceptions, the function must return the ret error code:**
1. Simple access function `set_xxx()/get_xxx()`. If the `set/get` function is complex or error-prone, it must still return an error code.
2. The `at(i)` function that has been defined and used (to define and use a new one, please obtain the consent of the team leader in advance, and explain the reason in detail).
3. Operator overloading that has been defined and used (to define and use new ones, please obtain the consent of the team leader in advance, and explain the reasons in detail).
4. For other small amount of functions, such as general function `void reset(); void reuse();` etc., refer to section 4.7 general function.

**The function caller must check the return value (error code) of the function and handle it.**

**Only ret variables of type int can be used to represent errors, and ret can only represent errors (except for iterator functions due to historical reasons).** If you need to return other types of values, such as the compare function returning a value of type bool, you need to use other variable names, such as `bool_ret`. For example:

```cpp
// wrong
bool operator()(const RowRun &r1, const RowRun &r2) const
{
  bool ret = false;
  int err = do_something();
  return ret;
}
 
// correct
bool operator()(const RowRun &r1, const RowRun &r2) const
{
  bool bool_ret = false;
  int ret = do_something();
  return bool_ret;
}
```

If some error codes need to be temporarily saved during function execution, try to use variables with clear meanings, such as `hash_ret` and `alloc_ret`. If the meaning is not clear, then `ret1` and `ret2` can also be used in sequence to avoid confusion caused by using err to represent error codes. For example:

```cpp
int func()
{
  int ret = OB_SUCCESS;
 
  ret = do_something();
  if (OB_SUCCESS != ret) {
    int alloc_ret = clear_some_resource ();
    if (OB_SUCCESS != alloc_ret) {
      // print error log
    }
  } else {
    ...
  }
  return ret;
}
```

## 5.3 Sequential Statement
If multiple sequential statements are doing the same thing, condensed writing can be used in some cases.
This makes sequential code tedious due to the need to judge errors during function execution. For example:

```cpp
// verbose code
int ret = OB_SUCCESS;
 
ret = do_something1();
if (OB_SUCCESS != ret) {
  // print error log
}
 
if (OB_SUCCESS == ret) {
  ret = do_something2();
  if (OB_SUCCESS != ret) {
    // print error log
  }
}
// more code ...
```

It can be seen that there are only two lines of code that are really effective, but the overall code size is several times that of the effective code. This will make a screen contain too little valid code, affecting readability.
If each step in the sequence statement requires only one line of code, it is recommended to simplify the code in the following way:

```cpp
// Use shorthand when there is only one line of code per step in sequential logic
int ret = OB_SUCCESS;
 
if (OB_FAIL(do_something1())) {
  // print error log
} else if (OB_FAIL(do_something2())) {
  // print error log
} else if (OB_SUCCESS != (ret = do_something3())) {
  // print error log
} else { }
```

If some steps of the sequence statement take more than one line of code, then some changes are required:

```cpp
// When some steps in the sequential logic exceed one line of code, 
// use simplified writing and make certain changes
int ret = OB_SUCCESS;
 
if (OB_SUCCESS != (ret = do_something1())) {
  // print error log
} else if (OB_SUCCESS != (ret = do_something2())) {
  // print error log
} else {
  // Step 3 executes more than one line of code
  if (OB_SUCCESS != (ret = do_something3_1())) {
    // print error log
  } else if (OB_SUCCESS != (ret = do_something3_2()) {
    // print error log
} else { }
}
 
if (OB_SUCCESS == ret) {  // start a new logic
  if (OB_SUCCESS != (ret = do_something4())) {
    // print error log
  } else if (OB_SUCCESS != (ret = do_something5())) {
    // print error log
  } else { }
}
```

In the actual coding process, when should concise writing be used? OceanBase believes that when each step of a sequential statement has only one line of statement, and these steps are logically coupled tightly, the concise writing method should be used as much as possible. However, if it logically belongs to multiple sections, each of which does a different thing, then brevity should only be used within each section, not brevity for the sake of brevity.
It should be noted that if the sequential statement is followed by a conditional statement. If sequential statements are reduced to conditional statements, then they cannot be combined into one large conditional statement, but should be separated in code structure. For example:

```cpp
// wrong
if (OB_SUCCESS != (ret = do_something1())) {
  // print error log
} else if (OB_SUCCESS != (ret = do_something2())) {
   // print error log
} else if (cond) {
   // do something if cond
} else {
   // do something if !cond
}
 
// The first correct way
if (OB_SUCCESS != (ret = do_something1())) {
  // print error log
} else if (OB_SUCCESS != (ret = do_something2())) {
  // print error log
} else {
  if (cond) {
    // do something if cond
  } else {
    // do something if !cond
  }
}
 
// The second correct way
if (OB_SUCCESS != (ret = do_something1())) {
  // print error log
} else if (OB_SUCCESS != (ret = do_something2())) {
  // print error log
} else { }
 
if (OB_SUCCESS == ret) {
  if (cond) {
    // do something if cond
  } else {
    // do something if !cond
  }
}
```
## 5.4 Loop Statement
Judge `OB_SUCCESS == ret` in the loop condition to prevent the error code from being overwritten.
OceanBase has discovered a large number of problems where error codes are covered. These problems often lead to serious consequences, such as data inconsistency, and are very difficult to find. For example:
```cpp
// error code is overwritten
for (int i = 0; i < 100; ++i) {
  ret = do_something();
  if (OB_SUCCESS != ret) {
    // print error log
  } else {
	...
  }
}
```
In the above example, the `if` branch in the for loop is wrong, but forget to break, so that the code will enter the next loop, and the error code of the previous execution will be overwritten.
Therefore, the standard for loop statement is written as follows:
```cpp
for (int i = 0; OB_SUCCESS == ret && i < xxx; ++i) {
  ...
}
```
In addition, the standard `while` loop statement is written as follows:
```cpp
while (OB_SUCCESS == ret && other_cond) {
  ...
}
```
A `break` or `continue` may be used in a loop statement to change the execution path. OceanBase believes that it should be used as little as possible, which is the same as the principle of single entry and single exit of a function. It is equivalent to the input source of the subsequent code of the loop statement being multiple entries, which increases the complexity of the code. If it is really necessary to use `break` and `continue`, it is required to explain the reason in detail through comments, and you need to pay special attention when writing code or reviewing code. In addition, considering that the input source of the follow-up code is multiple entries, it is necessary to ensure that it is clear what conditions the input of the follow-up code satisfies.

## 5.5 Conditional Statements
Conditional statements need to follow the MECE principle.
The word MECE comes from the McKinsey analysis method, which means mutually independent and completely exhaustive (Mutually Exclusive Collectively Exhaustive). In principle, every `if/else` branch of a conditional statement needs to fully exhaust all possibilities.
Some bad programming style, such as:
```cpp
// bad programming style
if (OB_SUCCESS != ret && x > 0) {
   // do something
}
 
if (OB_SUCCESS == ret && x < 0 ) {
   // do something
}
 
if (OB_SUCCESS == ret && x == 0) {
   // do something
}
```

Such code does not conform to the MECE principle, it is difficult to analyze whether all possibilities are exhausted, and it is easy to miss some scenarios.
If there is only one condition, the correct way to write it is:
```cpp
// The correct way to write a judgment condition
if (cond) {
  // do something
} else {
  // do something
}
```
In principle, every `if/else` branch is complete, even if the last else branch does nothing. However, there is one exception. If the if condition is just some error judgment or parameter checking, and there is no other logic, then the else branch can be omitted.
```cpp
// The if statement is only to judge the error code, the else branch can be omitted
if (OB_SUCCESS != ret) {
   // handle errors
}
```
If two judgment conditions are included, compare the following two possible writing methods:
```cpp
// The first way of writing the two judgment conditions (correct)
if (cond1) {
  if (cond2) {
    // do something
  } else {
    // do something
  }
} else {
  if (cond2) {
    // do something
  } else {
    // do something
  }
}
 
// The second way of writing the two judgment conditions (wrong)
if (cond1 && cond2) {
   // do something
} else if (cond1 && !cond2) {
   // do something
} else if (!cond1 && cond2) {
   // do something
} else {
   // do something
}
```

The first writing method is divided into two layers, and the second writing method is divided into one layer. OceanBase only allows the first writing method. Of course, `cond1` and `cond2` here are from the perspective of business logic, referring to two independent business logics, rather than saying that `cond1` and `cond2` cannot contain `&&` or `||` operators. For example:

```cpp
// Whether app_name is empty, including ||
if (NULL == app_name || app_name[0] == '\0') {
   ...
}
 
// Judging whether table_name or column_name is empty, it is considered a business logic
if (NULL != table_name || NULL != column_name) {
   ...
}
```

In any case, the number of `if/else` branches in each layer should not exceed 5. Why choose 5? This number also comes from the McKinsey analysis method. Generally speaking, the branch logic of the same level is generally between 3 and 5. If it exceeds, the division is often unreasonable.

## 5.6 Const Declaration
Declare function parameters that do not change as const. In addition, if the function does not modify member variables, it should also be declared as a const function.
Declaring parameters as const can avoid some unnecessary errors, such as constant parameters being changed due to code errors. For simple data type value transfer, many people have disputes about whether to declare const, because in this case, declaring const has no effect.
Considering that most of the existing code in OceanBase has been declared as const, and it is easier to operate this way, as long as the function parameters do not change, they are uniformly declared as const.
## 5.7 Function Parameters
The number of function parameters should not exceed 7. The recommended order is: input parameters first, output parameters last. If some parameters are both input parameters and output parameters, they will be treated as input parameters and placed at the front like other input parameters. Add a new The parameters also need to follow this principle. 

**The principle of coding: don't trust anyone in the code! Every function (whether `public` or `private`, except `inline` functions) must check the legality of each input parameter, and it is strongly recommended that inline functions also perform these checks (unless there are serious performance problems)**. All functions (whether `public` or `private`) must check the legality of values obtained from class member variables or through function calls (such as get return values or output parameters), even if the return value is successful, the legality of output parameters must still be checked . Variable (parameter) check, only needs to be checked once in a function (if the value obtained by calling one or several functions multiple times, then check each time). 
These checks include but are not limited to:

1. Whether the pointer is `NULL`, and whether the string is empty
2. Whether the parameter value of the numeric type exceeds the value range. In particular, whether the subscript of the `array/string/buffer` is out of bounds
3. Whether the parameter of the object type is valid. Generally, the object can define a `bool is_valid()` method (refer to `common::TableSchema`)

If an implicit check has been made within the function, for example by a check function, it should be stated where the variable is assigned. For example:

```cpp
// Variables that have been implicitly checked should be explained 
// where the variable is assigned:
if (!param.is_valid() || !context.is_valid()) {
     ret = OB_INVALID_ARGUMENT;
     STORAGE_LOG(WARN, "Invalid argument", K(ret), K(param), K(param));
   } else {
     // block_cache_ not empty has been checked in the previous context.is_valid()
     ObMicroBlockCache *block_cache = context.cache_context_.block_cache_;
     ...
}
```

Use the `if` statement to check the validity of the input parameters (the function itself) and the output parameters (the function caller), and prohibit the use of `assert` and the previously defined `OB_ASSERT` macro at any time.
Examples are as follows:

```cpp
// function that needs to return an error
int _func(void *ptr)
{
  int ret = OB_SUCCESS;
 
  if (NULL == ptr) {
    // print error log
    ret = OB_INVALID_ARGUMENT;
  }
  else {
    // Execute business logic
  }
  return ret;
}
```

## 5.8 Function Call
When calling the function, you should try to avoid passing in some meaningless special values, such as `NULL`, `true/false`, `0/-1`, etc., and use constants instead. If you must pass in a special value, you need to use annotation instructions.

For example:

```cpp
// wrong
int ret = do_something(param1, 100, NULL);
 
// Correct
ObCallback *null_callback = NULL;
int ret = do_something(param1, NUM_TIMES, null_callback);
```

## 5.9 Pointer or Reference
Function parameters can choose pointers or references. Use more references while respecting idioms.

Pointer parameters and reference parameters can often achieve the same effect. Considering that the OceanBase coding specification has strict requirements for error judgment, more references are used to reduce some redundant error judgment codes. Provided, of course, that idioms are followed, such as:

1. The method of applying for an object often returns a pointer, and the corresponding release method also passes in a pointer.
2. If the member of the object is a pointer, the corresponding `set_xxx` input is also a pointer.

## 5.10 Function Length
It is mandatory that a single function does not exceed 120 lines. If it must be violated, please obtain the consent of the group leader in advance, and explain the reasons in detail.

Most open source projects limit the number of lines of a single function. Generally speaking, functions with more than 80 lines are often inappropriate. Considering that OceanBase has a lot of redundant error judgment codes, a single function is limited to no more than 120 lines. If the function is too long, consider splitting it into several smaller and more manageable functions, or re-examine the design and modify the structure of the class.

## 5.11 Summary
1. **Strictly abide by the single-entry single-exit function. If it must be violated, please obtain the consent of the project leader and project architect in advance, and explain the reasons in detail.**
2. **Except for the simple access functions `set_xxx()/get_xxx()` and a few exceptions (such as operator overloading, existing at(i) functions, general function `reset()/reuse()` of classes, etc.), all functions (public and private) should use `ret` to return the error code. If the set/get is complicated or may make an error, `ret` should still be used to return the error code.** Only ret variables of type int can be used to represent errors, and ret can only represent errors (except for iterator functions due to historical reasons).
3. If multiple sequential statements are doing the same thing, then, in some cases, you can use simplified writing.
4. Judge `OB_SUCCESS == ret` in the loop condition to prevent the error code from being overwritten.
5. The conditional statement needs to abide by the MECE principle: each condition is independent of each other and completely exhausted, and the number of branches of a single `if/else` should not exceed 5 as far as possible.
6. Declare functions/function parameters as const whenever possible.
7. **The principle of coding: do not trust anyone in the code! Every function (whether public or private, except inline functions) must check the legality of each input parameter, and it is strongly recommended that inline functions also perform these checks (unless there are serious performance problems). All functions (whether public or private) must check the legality of values obtained from class member variables or through function calls (such as get return values or output parameters), even if the return value is successful, the legality of output parameters must still be checked . Variable (parameter) check, only needs to be checked once in a function (if the value obtained by calling one or several functions multiple times, then check each time).** When defining functions, the recommended order is: input parameters first, output parameters last.
8. **Prohibit the use of assert and OB_ASSERT.**
9. When calling the function, you should try to avoid passing in some meaningless special values, and use constants instead.
10. On the premise of respecting idiomatic usage, use more references.
11. It is mandatory that a single function does not exceed 120 lines. If it must be violated, please obtain the consent of the group leader in advance, and explain the reasons in detail.

# 6 C&C++ Features
The advantage of C++ is flexibility, and the disadvantage is also flexibility. For many functions of C++, OceanBase is conservative, and this section describes some of them. There are two principles for choosing these features:
1. Principle of caution: This feature is relatively "safe", even for beginners, there are not too many "pitfalls"
2. Necessity: It has "sufficient" benefits to improve the coding quality of OceanBase

## 6.1 Smart Pointers and Resource Guard

Smart pointers are not allowed, allowing automatic release of resources through the Guard class.
The boost library supports smart pointers, including `scoped_ptr`, `shared_ptr`, and `auto_ptr`. Many people think that smart pointers can be used safely, especially `scoped_ptr`. However, most of OceanBase's existing code releases resources manually, and smart pointers are prone to side effects if they are not used well. Therefore, smart pointers are not allowed.
Users are allowed to write some Guard classes by hand. The methods of these classes will apply for some resources, and these resources will be released automatically when the class is destroyed, such as LockGuard and SessionGuard.

## 6.2 Memory Allocation and Release
It is required to use the memory allocator to allocate memory, and immediately set the pointer to NULL after the memory is released.

The methods OceanBase can use for memory allocation include `ob_malloc` and various memory allocators. It is required to use the memory allocator to allocate memory, and specify the module it belongs to when allocating. The advantage of this is that it is convenient for the system to manage memory. If there is a memory leak, it is easy to see which module it is. In addition, it is necessary to prevent reference to the memory space that has been released, and it is required to set the pointer to `NULL` immediately after free.

```cpp
void *ptr = ob_malloc(100, ObModIds::OB_COMMON_ARRAY);
 
// do something
 
if (NULL != ptr) {
  // Release resources
  ob_free(ptr, ObModIds::OB_COMMON_ARRAY);
  ptr = NULL; // empty the pointer immediately after free
}
```

## 6.3 String
The `std::string` class is prohibited, and `ObString` is used instead. In addition, when manipulating C strings, it is required to use length-limited string functions.

C++'s `std::string` class is very convenient to use. The problem is that it is impossible to figure out its internal behavior, such as copying and implicit conversion. OceanBase requires the use of `ObString` as much as possible, and the memory used in it needs to be manually managed by developers.

Sometimes C strings are used. Be careful not to use string manipulation functions with unlimited length, including `strcpy/strcat/strdup/sprintf/strncpy`, but use the corresponding string manipulation functions with limited length `strncat/strndup/snprintf/memcpy`. You can use strlen to get the length of a string. The reason why `strncpy` is not used is that if the incoming buffer is not enough, it will not automatically '\0', and there are performance problems, so it needs to be replaced by `memcpy/snprintf`.

## 6.4 Array/String/Buffer Access
**When a function passes an array/string/buffer as a parameter, the length of the array/string/buffer must be passed at the same time. When accessing the contents of an array/string/buffer, you must check whether the subscript is out of bounds.**

## 6.5 Friend
Friend can only be used in the same file. If it must be violated, please obtain the consent of the group leader in advance and explain the reason in detail. Declaring unit test classes as friends is an exception, but should be used with caution.
Friend is usually defined in the same file to prevent code readers from going to other files to find their use of a class's `private` members. Scenarios where friend is often used include:

1. Iterator: The iterator class is often declared as a friend, for example, `ObQueryEngine` declares `ObQueryEngineIterator` as a `friend` (friend class `ObQueryEngineIterator`).
2. Factory mode: For example, declare `ObFooBuilder` as a friend of `ObFoo` so that `ObFooBuilder` can access the internal state of `ObFoo`.

In some cases, it may be convenient to declare a unit test class as a friend of the class under test in order to improve test coverage. However, this approach needs to be approached with caution. In most cases, we should indirectly test private functions through various input combinations of public functions, otherwise, these unit test codes will be difficult to maintain.

## 6.6 Exception
C++ exceptions are prohibited.
Some programming languages encourage the use of exceptions, such as Java. Exceptions do make writing code more convenient, but only in the code writing stage, subsequent debugging and bug correction will be very inconvenient. Exceptions make the program control flow more complicated, and it is easy to forget to catch some exceptions. Therefore, it is forbidden to use ret error codes to return errors.
## 6.7 Runtime Type Identification
The use of Run-Time Type Identification (RTTI) is prohibited.
Runtime type recognition often indicates a problem with the design itself, and if it must be used, it usually indicates that the design of the class needs to be reconsidered.

## 6.8 Type Conversion
Use `static_cast<>` and other C++ type conversions, prohibiting the use of int-like C cast of `y = (int) x`.
C++-style type conversions include:
1. `static_cast`: Similar to the C style, it can do value cast, or a clear upcast from the subclass of the pointer to the parent class.
2. `const_cast`: Remove the const attribute.
3. `reinterpret_cast`: Unsafe conversion between pointer types and integers or other pointers, so be **careful** when using it.
4. `dynamic_cast`: Except for test code, it is **forbidden** to use.

`const_cast` needs to be used with **caution**. In particular, for an input parameter declared as const. In principle, it is **forbidden** to use `const_cast` to remove const.
`const_cast` will cause cognitive difficulties for code readers: for a const input parameter of a function, when analyzing the code logic, it will be considered that this parameter is generated outside the function and will not be modified inside the function; using `const_cast` will destroy this assumption, resulting in code readers cannot notice the modification of const input parameters inside the function. For example, `const_cast` in the following code fragment is prohibited.

```cpp
int foo(const char* bar, int64_t len)
{
  ...
  memcpy(const_cast<char*>(bar), src, len);
  ...
  return OB_SUCCESS;
}
```

## 6.9 Output
Try to use `to_cstring` output.

In principle, every class that supports printing needs to implement `to_string`,
An example of using `to_cstring` is as follows:

```cpp
FILL_TRACE_LOG("cur_trans_id=%s", to_cstring(my_session->get_trans_id()));
FILL_TRACE_LOG("session_trans_id=%s", to_cstring(physical_plan->get_trans_id()));
```

## 6.10 Integers
Use `int` for the returned ret error code, and use `int64_t` for function parameters and loop times as much as possible. In other cases, use a signed number with a specified length, such as `int32_t`, `int64_t`. Avoid unsigned numbers, except for a few idioms.

The reason why function parameters and loop times use `int64_t` as much as possible is to avoid a large number of data type conversions in function calls and loop statements. Of course, idioms can be exceptions, such as ports being `int32_t`. In a structure such as struct, there is often a need for 8-byte alignment or memory saving, so a signed number of a specified length can be used.

Except for idioms such as bit sets or numbers (such as `table_id`), the use of unsigned numbers should be avoided. Unsigned numbers may bring some hidden dangers, such as:
```cpp
for (unsigned int i = foo. length() – 1; i >= 0; --i)
```

The above code never terminates.
For numbering, some current codes use 0 as an illegal value, and some use `MAX_UINT64` as an illegal value. In the future, it will be uniformly stipulated that both 0 and `MAX_UINT64` are illegal values, and the inline function `is_valid_id` is provided in the utility for checking. In addition, uniformly initialize the number value to the macro `OB_INVALID_ID`, and adjust the initial value of the macro `OB_INVALID_ID` to 0.
## 6.11 sizeof
Try to use `sizeof(var_name)` instead of `sizeof(type)`.
This is because, if the type of `var_name` changes, `sizeof(var_name)` will automatically synchronize, but `sizeof(type)` will not, which may bring some hidden dangers.

```cpp
ObStruct data;
memset(&data, 0, sizeof(data)); // correct way
memset(&data, 0, sizeof(ObStruct)); // Wrong way
```

It should be noted that instead of using sizeof to calculate the length of a string, use strlen instead. For example:

```cpp
char *p = "abcdefg";
// sizeof(p) indicates the pointer size, which is equal to 8 on a 64-bit machine
int64_t nsize = sizeof(p); 
```

## 6.12 0 and nullptr
Use 0 for integers, 0.0 for real numbers, `nullptr` for pointers (replacing the previous `NULL`), and '\0' for strings.

## 6.13 Preprocessing Macros
**In addition to existing macros, no new macros shall be defined, and inline functions, enumerations, and constants shall be substituted**. If it is necessary to define a new macro, please obtain the agreement of the group leader in advance, and explain the reason in detail.
Macros can do things that other techniques cannot, such as stringifying (using #), concatenation (using ##). Macros are often used for output and serialization, such as `common/ob_print_utils.h`, rpc-related classes. However, in many cases, other methods can be used instead of macros: macro inline efficiency-critical code can be replaced by inline functions, and macro storage constants can be used const variable substitution.
The principle of judgment is: in addition to output and serialization, as long as macros can be used, try not to use macros.

## 6.14 Boost and STL
**In STL, only algorithm functions defined in the <algorithm> header file are allowed, such as std_sort, and other STL or boost functions are prohibited. If it must be violated, please obtain the consent of the project leader and project architect in advance, and explain the reasons in detail.**

OceanBase has a conservative attitude towards libraries like boost and STL, and we believe that writing code correctly is far more important than writing code conveniently. Except for the algorithm class functions defined by STL <algorithm>, other functions should not be used.

## 6.15 auto
**What is**
The specific type is omitted when declaring the variable, and the compiler automatically deduces the type according to the initialization expression.

**Example**
```cpp
auto i = 42; // i is an int
auto l = 42LL; // l is a long long
auto p = new foo(); // p is a foo*
```

**Is it allowed**
**Prohibited.**
Although it is possible to make the declaration of some template types shorter, we hope that the type declaration matches the user's intention. For example, in the above examples 1 and 2, the type should be explicitly declared.

## 6.16 Range-based for Loops
**What is**
The new for loop syntax is used to easily traverse the container that provides `begin()`, `end()`.

**Example**
```cpp
for(const auto& kvp : map) {
  std::cout << kvp. first << std::endl;
}
```
**Is it allowed**
**Prohibited.**
This feature is just a syntactic sugar. The `FOREACH` macro defined by ourselves has been widely used in the previous OceanBase code, which can achieve similar effects.

## 6.17 Override and Final
**What is**
`override` is used to indicate that a virtual function is an overload of a virtual function in the base class; `final` indicates that a virtual function cannot be overloaded by a derived class.

**Example**
```cpp
class B
{
public:
  virtual void f(short) {std::cout << "B::f" << std::endl;}
  virtual void f2(short) override final {std::cout << "B::f2" << std::endl;}
};
class D : public B
{
public:
  virtual void f(int) override {std::cout << "D::f" << std::endl;}
};
class F : public B
{
public:
  virtual void f2(int) override {std::cout << "D::f2" << std::endl;} // compiler error
};
```

**Is it allowed**
**Allow.** `override` and `final` are not only allowed, but strongly recommended, and should be added wherever they can be used.

According to previous experience, the overloading of the virtual function in the OceanBase missed the `const`, resulting in endless errors of overloading errors. It is required that in the new code, all overloads must be added with override to avoid this wrong overload situation.

In addition to being used for virtual functions, when a class is added with the final keyword, it means that it cannot be further derived, which is conducive to compiler optimization. When such a class has no parent class, the destructor does not need to add virtual.

```cpp
class ObLinearRetry final: public ObIRetryPolicy
{
   //...
};

class ObCData final
{
  ~ObCData();
}
```

## 6.18 Strongly-typed Enums
**What is**
Traditional enumerated types have too many shortcomings to be true types. For example, it is implicitly converted to an integer; the enumeration value is in the same scope as the place where its type is defined.
**Example**

```cpp
enum class Options {None, One, All};
Options o = Options::All;
```

**Is it allowed**
**Allow.** The original enumeration type is a bug in the C++ language. The new enumeration type makes the compiler's inspection more strict, and uses new keyword definitions, which do not conflict with the original enum.

## 6.19 Lambdas
**What is**
A concept borrowed from functional programming for conveniently writing anonymous functions.
**Example**
```cpp
std::function<int(int)> lfib = [&lfib](int n) {return n < 2 ? 1 : lfib(n-1) + lfib(n-2);};
```
**Is it allowed**
**prohibited**. The novel syntax of lambda makes C code look like a new language, and most people don't have enough understanding of functional programming, so the cost of learning is relatively high. Does not meet principle (1). In addition, lambda is essentially equivalent to defining a functor, which is a syntactic sugar that does not increase the abstraction ability of C. Does not meet principle (2).
## 6.20 Non-member begin() and end()
**What is**
The global functions `std::begin()` and `std::end()` are used to conveniently abstract operations on containers.

**Example**
```cpp
int arr[] = {1,2,3};
std::for_each(std::begin(arr), std::end(arr), [](int n) {std::cout << n << std::endl;});
```

**Is it allowed**
**Prohibited**. This feature is mainly to make STL easier to use, but OceanBase prohibits the use of STL containers.

## 6.21 static_assert and Type Traits
**What is**
The compile-time `assert` and compile-time constraint checking supported by the compiler.
**Example**
```cpp
template <typename T, size_t Size>
class Vector
{
  static_assert(Size < 3, "Size is too small");
  T_points[Size];
};
```
**Is it allowed**
**allow**. Although the OB code has defined `STATIC_ASSERT` by itself, it is only a simulation of the compiler check, and the error report is not friendly. And `type_traits` brings great benefits to the use of templates.
## 6.22 Move Semantics
**What is**
The move constructor and move assignment operator are one of the most important new features of C++11. Along with it, the concept of rvalue is introduced. Move semantics can make many container implementations much more efficient than before.
**Example**
```cpp
// move constructor
Buffer(Buffer&& temp):
    name(std::move(temp.name)),
    size(temp. size),
    buffer(std::move(temp.buffer))
{
  temp._buffer = nullptr;
  temp._size   = 0;
}
```
**Is it allowed**
**Prohibited**. Banning it may bring some controversy. Mainly based on the following considerations:
1. OceanBase does not use STL containers, so the optimization of the standard library using move semantics does not bring us benefits.
2. The semantics of move semantic and rvalue are more complicated, and it is easy to introduce pitfalls
3. Using it to transform some existing containers of OceanBase can indeed improve performance. However, the memory management method of OceanBase has made the use of move semantics smaller. In many cases, we have optimized it during implementation, and only store pointers in the container, not large objects.

It is recommended to consider other C++11 features after a period of familiarity, when the coding standard is revised next time.

## 6.23 constexpr
**What is**
More standardized compile-time constant expression evaluation support, no longer need to use various template tricks to achieve the effect of compile-time evaluation.
**Example**
```cpp
constexpr int getDefaultArraySize (int multiplier)
{
  return 10 * multiplier;
}

int my_array[ getDefaultArraySize( 3 ) ];
```

**Is it allowed**
**allow**. Constants are always more friendly to compiler optimization. In the above example, the use of macros is also avoided. In addition, constexpr supports floating-point calculations, which cannot be replaced by static const.
## 6.23 Uniform Initialization Syntax and Semantics
**What is**
The initialization of variables of any type in any context can use the unified {} syntax.
**Example**
```cpp
X x1 = X{1,2};
X x2 = {1,2}; // the = is optional
X x3{1,2};
X* p = new X{1,2};
```

**Is it allowed**
**Prohibited**. Syntactically more uniform, but again without any significant benefit. At the same time, it will significantly affect the style of OceanBase code and affect readability.
## 6.24 Right Angle Brackets
**What is**
Fix a common syntax problem in original C. It turns out that when the templates of C-defined templates are nested, the ending >> must be separated by spaces, which is no longer needed.
**Example**
```cpp
typedef std::vector<std::vector<bool>> Flags;
```

**Is it allowed**
**allow.**

## 6.25 Variadic Templates
**What is**
Variadic templates.

**Example**
```cpp
template<typename Arg1, typename... Args>
void func(const Arg1& arg1, const Args&... args)
{
  process( arg1 );
  func(args...); // note: arg1 does not appear here!
}
```
**Is it allowed**
**Allow**. This is a key feature for template programming. Because there is no variable-length template parameter, some basic libraries of OceanBase, such as `to_string`, `to_yson`, RPC framework, log library, etc., need to be implemented with some tricks and macros. And more type safe.

## 6.26 Unrestricted Unions
**What is**
Before, union could not contain classes with constructors as members, but now it can.
**Example**
```cpp
struct Point {
  Point() {}
  Point(int x, int y): x(x), y(y) {}
  int x, y;
};
union_{
  int z;
  double w;
  Point p; // Illegal in C03; legal in C11.
  U() {} // Due to the Point member, a constructor definition is now needed.
  U(const Point& pt) : p(pt) {} // Construct Point object using initializer list.
  U& operator=(const Point& pt) { new(&p) Point(pt); return *this; } // Assign Point object using placement 'new'.
};
```
**Is it allowed**
**Allow**. There are many places in the OceanBase code that have to define redundant domains because of this limitation, or use tricky methods to bypass (define char array placeholders). See `sql::ObPostExprItem` for a miserable example.

## 6.27 Explicitly Defaulted and Deleted Special Member Functions
**What is**
One of the most disturbing things about C++ before is that the compiler implicitly and automatically generates constructors, copy constructors, assignment operators, destructors, etc. for you. They can now be explicitly required or disallowed.
**Example**
```cpp
struct NonCopyable {
  NonCopyable() = default;
  NonCopyable(const NonCopyable&) = delete;
  NonCopyable& operator=(const NonCopyable&) = delete;
};
struct NoInt {
  void f(double i);
  void f(int) = delete;
};
```

**Is it allowed**
**Allowed**. This feature is like tailor-made for OceanBase; the function of disabling a certain function is also very useful.
## 6.28 Type Alias (Alias Declaration)
**What is**
Use the new alias declaration syntax to define an alias of a type, similar to the previous typedef; moreover, you can also define an alias template.
**Example**
```cpp
// C++11
using func = void(*)(int);
// C++03 equivalent:
// typedef void (*func)(int);
template using ptr = T*;
// the name 'ptr' is now an alias for pointer to T
ptr ptr_int;
```

**Is it allowed**
**Prohibited**. For the time being, there is no need for alias templates, and the same effect can be achieved by using typedef for non-template aliases.

## 6.29 Summary

1. **Smart pointers are not allowed**, and resources are allowed to be released automatically through the Guard class.
2. It is required to use the memory allocator to allocate memory, and immediately set the pointer to `NULL` after the memory is released.
3. **Prohibit the use of std::string class, use ObString instead**. In addition, when manipulating C strings, it is required to use length-limited string functions.
4. **When passing an array/string/buffer as a parameter, the length must be passed at the same time. When reading and writing the content of the array/string/buffer, check whether the subscript is out of bounds.**
5. `friend` can only be used in the same file. If it must be violated, please obtain the consent of the code owner in advance and explain the reason in detail. Declaring unit test classes as `friend` is an exception, but should be used with caution.
6. **C++ exceptions are prohibited.**
7. Prohibit the use of runtime type identification (RTTI).
8. Use C++ type conversions such as `static_cast<>`, and prohibit the use of int C cast of `y = (int) x`.
9. Try to use `to_cstring` output.
10. Use int for the returned ret error code, and use `int64_t` for function parameters and loop times as much as possible. In other cases, use a signed number with a specified length, such as `int32_t`, `int64_t`. Try to avoid using unsigned numbers.
11. Try to use `sizeof(var_name)` instead of `sizeof(type)`.
12. Use 0 for integers, 0.0 for real numbers, `NULL` for pointers, and '\0' for strings.
13. **In addition to the existing macros, no new macros shall be defined, and inline functions, enumerations, and constants shall be used instead**. If it must be violated, please obtain the consent of the group leader in advance, and explain the reasons in detail.
14. **Except for the algorithm class functions defined in the <algorithm> header file in STL, the use of STL and boost is prohibited. If it must be violated, please obtain the consent of the project leader and project architect in advance, and explain the reasons in detail.**

# 7 Naming Rules
## 7.1 General Rules
Function naming, variable naming, file naming should be descriptive and not overly abbreviated, types and variables should be nouns, and functions can use "imperative" verbs.
Identifier naming sometimes uses some common abbreviations, but it is not allowed to use too professional or unpopular ones. For example we can use the following ranges:
1. temp can be abbreviated as tmp;
2. statistic can be abbreviated as stat;
3. increment can be abbreviated as inc;
4. message can be abbreviated as msg;
5. count can be abbreviated as cnt;
6. buffer can be abbreviated as buf instead of buff;
7. current can be abbreviated as cur instead of curr;
When using abbreviations, consider whether each project team member can understand them. Avoid abbreviations if you're not sure.

Types and variables are generally nouns, for example, `ObFileReader`, `num_errors`.
Function names are usually imperative, eg `open_file()`, `set_num_errors()`.

## 7.2 File Naming
Self-describing well composed of all lowercase words separated by '_', for example
`ob_update_server.h` and `ob_update_server.cpp`.
The .h file and the .cpp file correspond to each other. If the template class code is long, it can be placed in the .ipp file, such as `ob_vector.h` and `ob_vector.ipp`.

## 7.3 Type Naming
Use self-describing well-formed words. In order to distinguish it from variables, it is recommended to use the first letter of the word capitalized and no separator in the middle. Nested classes do not need to add "Ob" prefix, other classes need to add "Ob" prefix. For example:
```cpp
// class and structs
class ObArenaAllocator
{ 
  ...
};
struct ObUpsInfo
{ 
  ...
};
 
// typedefs
typedef ObStringBufT<> ObStringBuf;
 
// enums
enum ObPacketCode
{
};
 
// inner class
class ObOuterClass
{
private:
   class InnerClass
   {
   };
};
```

The interface class needs to be preceded by an "I" modifier, and other classes should not be added, for example:

```cpp
class ObIAllocator
{ 
  ...
};
```

## 7.4 Variable Naming
### 7.4.1 Intra-class Variable Naming
Self-describing good all lowercase words, separated by '\_', in order to avoid confusion with other variables, it is required to add '\_' in the back end to distinguish, for example:
```cpp
class ObArenaAllocator
{
private:
   ModuleArena arena_;
};
 
struct ObUpsInfo
{
   common::ObServer addr_;
   int32_t inner_port_;
};
```

### 7.4.2 Common Variable Naming
Self-describing well-formed all-lowercase words separated by '_'.

### 7.4.3 Global Variable Naming
New global variables must not be used in addition to existing global variables. If it must be violated, please obtain the consent of the code owner in advance, and explain the reasons in detail. Global variables are composed of self-describing all-lowercase words separated by '\_'. In order to mark the global nature, it is required to add the 'g\_' modifier at the front end. For example:

```cpp
// globe thread number
int64_t g_thread_number;
```

## 7.5 Function Naming
### 7.5.1 Function Naming within a Class
Self-describing well-formed all-lowercase words separated by '_', for example:
```cpp
class ObArenaAllocator
{
public:
  int64_t used() const;
  int64_t total() const;
};
```

### 7.5.2 Access Function Naming
The name of the access function needs to correspond to the class member variable. If the class member variable is `xxx`, then the access function is `set_xxx` and `get_xxx` respectively.

### 7.5.3 Ordinary Function Naming
Self-describing well-formed all-lowercase words separated by '_'.

## 7.6 Constant Naming
All compile-time constants, whether local, global or in a class, are required to be composed of all uppercase letters, separated by '_' between words. For example:
```cpp
static const int NUM_TEST_CASES = 6;
```

# 7.7 Macro Naming
Try to avoid using macros. Macro names are all composed of uppercase letters, and words are separated by '_'. Note that parameters must be enclosed in parentheses when defining a macro. For example:
```cpp
// Correct spelling
#define MAX(a, b) (((a) > (b)) ? (a) : (b))
// wrong wording
#define MAX(a, b) ((a > b) ? a : b)
```

## 7.8 Precautions
There are a few points that are easy to forget, as follows:
1. Try not to use abbreviations unless they are clear enough and widely accepted by project team members.
2. In addition to the exception that the name of the interface class needs to be modified with I, other classes, structures, and enumeration types do not need modifiers
3. The variable names in the struct also need to be underlined

# 8 Typography Style
## 8.1 Code Indentation
Do not use the Tab key for indentation, you can use spaces instead, and different coding tools can be set, requiring the use of two spaces for indentation (4 spaces for indentation will appear a bit compact when the single-line code is relatively long).

## 8.2 Empty Lines
Minimize unnecessary blank lines and only do so when the logic of the code is clearly divided into multiple parts.

The internal code of the function body is determined by the visual code. Generally speaking, only when the code is logically divided into multiple parts, a blank line needs to be added between each part.

None of the following code should have blank lines:
```cpp
// There should be no blank lines at the beginning and end of the function
void function()
{
  int ret = OB_SUCCESS;
 
}
 
// Do not have blank lines at the beginning and end of the code block
while (cond) {
  // do_something();
 
}
if (cond) {
 
   // do_something()
}
```
Empty lines below are reasonable.

```cpp
// Function initialization and business logic are two parts, 
// with a blank line in between
void function(const char *buf, const int64_t buf_len, int64_t &pos)
{
   int ret = OB_SUCCESS;
   int64_t ori_pos = pos;
   if (NULL == buf || buf_len <= 0 || pos >= buf_len) {
     // print error log
     ret = OB_INVALID_ARGUMENT;
   } else {
     ori_pos = pos;
   }
 
   // Execute business logic
   return ret;
}
```
## 8.3 Line Length
The length of each line shall not exceed 100 characters, and one Chinese character is equivalent to two characters.
100 characters is the maximum value of a single line, and the maximum limit can be increased to 120 characters in the following situations:
1. If a line of comments contains commands or URLs exceeding 100 characters.
2. Include long paths.
## 8.4 Function Declaration
The return type and the function name are on the same line, and the parameters are also placed on the same line as much as possible.
The function looks like this:
```cpp
int ClassName::function_name(Type par_name1, Type par_name2)
{
   int ret = OB_SUCCESS;
   do_something();
   ...
   return ret;
}
```
If the same line of text is too much to accommodate all parameters, you can split the parameters into multiple lines, with one parameter per line:
```cpp
int ClassName::really_long_function_name(Type par_name1,
     Type par_name2, Type par_name3) // empty 4 spaces
{
   int ret = OB_SUCCESS;
   do_something();
   ...
   return ret;
}
```
You can also put each parameter on a separate line, and each subsequent parameter is aligned with the first parameter, as follows:
```cpp
// The following parameters are aligned with the first parameter
int ClassName::really_long_function_name(Type par_name1,
Type par_name2, // align with the first parameter
Type par_name3)
{
   int ret = OB_SUCCESS;
   do_something();
   ...
   return ret;
}
```
If you can't even fit the first parameter:
```cpp
// Start a new line for each parameter, with 4 spaces
int ClassName::really_really_long_function_name(
     Type par_name1, // empty 4 spaces
     Type par_name2,
     Type par_name3)
{
   int ret = OB_SUCCESS;
   do_something();
   ...
   return ret;
}
```
Note the following points:
- The return value is always on the same line as the function name;
- The opening parenthesis is always on the same line as the function name;
- There is no space between the function name and the opening parenthesis;
- There is no space between parentheses and parameters;
- The opening curly brace is always alone on the first line of the function (starting a new line);
- The closing curly brace is always alone on the last line of the function;
- All formal parameter names at function declaration and implementation must be consistent;
- All formal parameters should be aligned as much as possible;
- default indentation is 2 spaces;
- The parameters after the line break keep the indentation of 4 spaces;
- If the function is declared const, the keyword const should be on the same line as the last parameter
Some parameters are not used, and these parameter names are annotated when the function is defined:
```cpp
// Correct
int ObCircle::rotate(double /*radians*/)
{
}
 
// wrong
int ObCircle::rotate(double)
{
}
```
## 8.5 Function Calls
Try to put it on the same line. If you can't fit it, you can cut it into multiple lines. The splitting method is similar to the function declaration.
The form of a function call is often like this (no spaces after the opening parenthesis and before the closing parenthesis):
```cpp
int ret = function(argument1, argument2, argument3);
```
If it is divided into multiple lines, the following parameters can be split into the next line, as follows:
```cpp
int ret = really_long_function(argument1,
    argument2, argument3); // empty 4 spaces
```
You can also put each parameter on a separate line, and each subsequent line is aligned with the first parameter, as follows:
```
int ret = really_long_function(argument1,
                               argument2, // align with the first argument
                               argument3);
```
If the function name is too long, all parameters can be separated into separate lines, as follows:
```cpp
int ret = really_really_long_function(
     argument1,
     argument2, // empty 4 spaces
     argument3);
```
For placement new, a space needs to be added between new and the pointer variable, as follows:
```cpp
new (ptr) ObArray();// There is a space between new and '('
```
## 8.6 Conditional Statements
`{` and if or else on the same line, `}` start a new line. In addition, between if and "(", ")" and `{` are guaranteed to contain a space.
Conditional statements tend to look like this:

```cpp
if (cond) { // There is no space between (and cond, cond and)
   ...
} else { // } and else, there is a space between else and {
   ...
}
```

In any case, both if and else statements need to have `{` and `}`, even if the branch is only one line statement. In principle, `}` always start a new line, but there is one exception. If the else branch does nothing, `}` does not need a new line, as follows:
```cpp
if (OB_SUCCESS == (ret = do_something1())) {
   ...
} else if (OB_SUCCESS == (ret = do_somethng2())) {
   ...
} else { } // else branch does nothing, } does not require a new line
```
For the comparison statement, if it is `=`, `!=`, then the constant needs to be written in front; while `>`, `>=`, `<`, `<=`, there is no such restriction. For example:
```cpp
// Correct
if (NULL == p) {
  ...
}
 
// wrong
if (p == NULL) {
  ...
}
```
 
## 8.7 Expressions
There is a space between the expression operator and the preceding and following variables, as follows:
```cpp
a = b; // There is a space before and after =
a > b;
a & b;
```
For boolean expressions, if the maximum length of the line is exceeded, the line break format needs to be taken care of. In addition, complex expressions need to use parentheses to clarify the order of operations of the expression to avoid using the default priority.
When breaking a line, the logical operator is always at the beginning of the next line, with 4 spaces:
```cpp
if ((condition1 && condition2)
    || (condition3 && condition4) // && operator is at the beginning of the line, with 4 spaces
    || (condition5 && condition6)) {
  do_something();
  ...
} else {
  do_another_thing();
  ...
}
```
If the expression is complex, parentheses should be added to clarify the order of operations of the expression.
```cpp
// correct
word = (high << 8) | low;
if ((a && b) || (c && d)) {
  ...
} else {
  ...
}
 
// wrong
word = high << 8 | low;
if (a && b || c && d) {
  ...
} else {
  ...
}
```

The ternary operator should be written in one line as much as possible. If it exceeds one line, it needs to be written in three lines. as follows:
```cpp
// The ternary operator is written in one line
int64_t length = (0 == digit_idx_) ? digit_pos_ : (digit_pos_ + 1);
 
// The ternary operator is written in three lines
int64_t length = (0 == digit_idx_)
    ? (ObNumber::MAX_CALC_LEN - digit_pos_ - 1) // 4 spaces
    : (ObNumber::MAX_CALC_LEN - digit_pos_);
 
// Error: Breaking into two lines is not allowed
int64_t length = (0 == digit_idx_) ? (ObNumber::MAX_CALC_LEN – digit_pos_ - 1)
     : (ObNumber::MAX_CALC_LEN – digit_pos_);
```

## 8.8 Loops and Switch Selection Statements
Both the `switch` statement and the `case` block in it need to use `{}`. In addition, each case branch must add a break statement. Even if you can ensure that you will not go to the default branch, you need to write the default branch.
```cpp
switch (var) {
case OB_TYPE_ONE: { // top case
     // empty 4 spaces relative to case, empty 4 spaces relative to switch
    break;
  }
case OB_TYPE_TWO: {
    ...
    break;
  }
default: {
    perform error handling;
  }
}
```
An empty loop body needs to write an empty comment instead of a simple semicolon. For example:
```cpp
// correct way
while (cond) {
  //empty
}
 
for (int64_t i = 0; i < num; ++i) {
  //empty
}
 
// wrong way
while (cond) ;
for (int64_t i = 0; i < num; ++i) ;
```

## 8.9 Variable Declaration
Only one variable is declared per line, and the variable must be initialized when it is declared. When declaring a pointer variable or parameter, `(*, &)` next to the variable name. The same is true for pointers or references `(*, &)` when a function type is declared.
```cpp
// correct way
int64_t *ptr1 = NULL;
int64_t *ptr2 = NULL;
 
// wrong way
 
int64_t *ptr1 = NULL, ptr2 = NULL; // error, declare only one variable per line
int64_t *ptr3; // Error, variable must be initialized when declared
int64_t* ptr = NULL; // error, * is next to the variable name, not next to the data type
 
char* get_buf(); // error, * is next to the variable name, not next to the data type
char *get_buf(); // correct
 
int set_buf(char* ptr); // error, * is next to the variable name, not next to the data type
int set_buf(char *ptr); // correct
```
 
 
## 8.10 Variable References
For references and pointers, you need to pay attention: there should be no spaces before and after periods `(.)` or arrows `(->)`. There can be no spaces after the pointer `(*)` and the address operator `(&)`, and the address operator is next to the variable name.
```cpp
// correct way
p = &x;
x = *p;
x = r->y;
x = r.y;
```

## 8.11 Preprocessing Directives
Do not indent preprocessing directives, start at the beginning of the line. Even if a preprocessing directive is in an indented code block, the directive should start at the beginning of the line.
```cpp
// Correct way of writing, preprocessing directive is at the beginning of the line
#if !defined(_OB_VERSION) || _OB_VERSION<=300
    do_something1();
#elif _OB_VERSION>300
    do_something2();
#endif
```

## 8.12 Class Format
The order of declarations is `public`, `protected`, and `private`. These three keywords are in the top case and are not indented.
The basic format of a class declaration is as follows:

```cpp
class ObMyClass : public ObOtherClass // : there is a space before and after
{ // { start a new line
public: // top grid
  ObMyClass(); // Indent 2 spaces relative to public
  ~ObMyClass();
  explicit ObMyClass(int var);
 
  int some_function1(); // first class function function
  int some_function2();
 
  inline void set_some_var(int64_t var) {some_var_ = var;} // the second type of function
  inline int64_t get_some_var() const {return some_var_;}
 
  inline int some_inline_func(); // The third type of function
 
private:
  int some_internal_function(); // function defined first
 
  int64_t some_var_; // variables are defined after
  DISALLOW_COPY_AND_ASSIGN(ObMyClass);
};
 
int ObMyClass::some_inline_func()
{
  ...
}
```
For the declaration order of classes, please refer to Chapter 4 Declaration Order. It should be noted that only inline functions whose implementation code is one line can be placed in the class definition, and other inline functions can be placed outside the class definition in the .h file. In the above example, `set_some_var` and `get_some_var` have only one line of implementation code, so they are placed inside the class definition; the implementation code of `some_inline_func` exceeds one line, and need to be placed outside the class definition. This has the advantage of making class definitions more compact.
## 8.13 Initialization Lists
The constructor initialization list is placed on the same line or indented according to 4 spaces and lined up in several lines, and the following parameters are aligned with the first parameter. In addition, if the initialization list needs to wrap, it must start from the first parameter.
Two acceptable initializer list formats are:
```cpp
// initializer list on the same line
ObMyClass::ObMyClass(int var) : some_var_(var), other_var_(var+1)
{
  ...
}
 
// The initialization list is placed on multiple lines, indented by 4 spaces
ObMyClass::ObMyClass(int var)
    : some_var_(var),
      some_other_var_(var+1) // The second parameter is aligned with the first parameter
{
  ...
}
```
## 8.14 Namespaces
Namespace contents are not indented.
```cpp
namespace oceanbase
{
namespace common
{
class ObMyClass // ObMyClass do not indent
{
  ...
}
} // namespace common
} // namespace oceanbase
```
## 8.15 Constants Instead of Numbers
Avoid confusing numbers and use meaningful symbols instead. Constants that involve physical states or have physical meanings should not use numbers directly, but must be replaced by meaningful enumerations or constants.
```cpp
const int64_t OB_MAX_HOST_NAME_LENGTH = 128;
const int64_t OB_MAX_HOST_NUM = 128;
```
## 8.16 Precautions
1. The `{` of the `if&else`, `for&while` and `switch&case` statements are placed at the end of the line instead of starting a new line;
2. Define the `public`, `protected` and `private` keywords of the class with 2 spaces, and pay attention to the declaration order of the class.
3. When cutting a line into multiple lines, you need to pay attention to the format.
4. Minimize unnecessary blank lines as much as possible, and only do this when the code logic is clearly divided into multiple parts.
5. Do not indent the contents of the namespace.


# 9 Notes
Comments are written for others to understand the code, the following rules describe what should be commented and where.
## 9.1 Comment Language and Style
The comment language is required to use English, and Chinese cannot be used, and the comment style adopts `//`. The purpose of comments is to make your code easier for others to understand.
The comment style can use `//` or `/* */`, except for the comments of the header file, `//` is used in other cases.
## 9.2 Document Comments
Add a copyright notice at the beginning of each file, see Section 2.2 for the copyright notice.
For key algorithms and business logic, it should be clearly described here, and the file header should be defined.
## 9.3 Class Annotations
Each class definition must be accompanied by a comment describing the function and usage of the class. For example:
```cpp
// memtable iterator: the following four requirements all use MemTableGetIter iteration
// 1. [General get/scan] need to construct RNE cell, and construct mtime/ctime cell 
// according to create_time, if there is column filtering, it will also construct NOP cell
// 2. [dump2text of QueryEngine] There is no column filtering and transaction id 
// filtering, no NOP will be constructed, but RNE/mtime/ctime will be constructed
// 3. [Dump] Without column filtering and transaction id filtering, empty rows will be 
// skipped in QueryEngine, RNE and NOP will not be constructed, but mtime/ctime 
// will be constructed
// 4. [Single-line merge] Before merging, it is necessary to determine whether there 
// is still data after the transaction id is filtered. If not, the GetIter 
// iteration is not called to prevent the RNE from being constructed and written 
// back to the memtable; in addition, RowCompaction is required to ensure that the
// order is not adjusted to prevent the mtime representing the transaction ID from
// being adjusted to the common column.
// 5. [update and return] is similar to regular get/scan, but without transaction
// id filtering
class MemTableGetIter : public common::ObIterator
{
};
```

Please note that the things that need to be paid attention to when using the class are indicated here, especially whether it is thread-safe, how resources are released, and so on.
## 9.4 Function Annotations
### 9.4.1 Function Declaration Comments
The function declaration comment is placed before the function declaration, and mainly describes the function declaration itself rather than how the function is completed. The content to be described includes:
- The input and output of the function.
- If the function allocated space, it needs to be freed by the caller.
- Whether the parameter can be NULL.
- Whether there are performance risks in function usage.
- Is the function reentrant and what are its synchronization prerequisites
```cpp
// Returns an iterator for this table.
//Note:
// It's the client's responsibility to delete the iterator
// when it's done with it.
//
// The method is equivalent to:
// ObMyIterator *iter = table->new_iterator();
// iter->seek_to_front();
// return iter;
ObMyIterator *get_iterator() const;
```
Generally speaking, the external interface functions of each class need to be annotated. Of course, self-describing functions such as constructors, destructors, and accessor functions do not need comments.
If the comment needs to describe the input, output parameters or return value, the format is as follows:
```cpp
// Gets the value according to the specified key.
//
// @param [in] key the specified key.
// @param [in] value the result value.
// @return the error code.
int get(const ObKey &key, ObValue &value);
```

Examples of annotations for function reentrancy are as follows:
```cpp
// This function is not thread safe, but it will be called by only one xxx thread.
int thread_unsafe_func();
```
### 9.4.2 Function Implementation Comments
If the function implementation algorithm is unique or has some bright spots, you can add function implementation comments in the .cpp file. For example, the programming skills used, the general steps of implementation, or the reasons for this implementation, such as explaining why the first half needs to be locked and the second half does not. Note that the focus here is on how to implement, rather than copying the function declaration comments in the .h file.
## 9.5 Variable Annotations
Local variables do not need to write comments. Member variables and global variables generally need to write comments, unless the project team members recognize that the variable is self-describing. If some values of the variable have special meaning, such as NULL, -1, then it must be stated in the comment.

Use good and unambiguous language to indicate the purpose, point of use, and scope of variables. Comments can appear on the right side of the variable definition or on the top line of the variable definition according to the number of characters in the line, for example:

```cpp
// comments appear on the top line
private:
  // Keeps track of the total number of entries in the table.
  // -1 means that we don't yet know how many entries the table has.
  int num_total_entries_;
 
  // Comments appear to the right of the variable
  static const int NUM_TEST_CASES = 6; // the total number of test cases.
```

## 9.6 Implementation Notes
Similarly, you must make detailed comments on business-critical points, sophisticated algorithms, and poorly readable parts in the internal implementation of the function. It can also appear at the top of a code snippet or to the right of a line of code.
```cpp
// it may fail, but the caller will retry until success.
ret = try_recycle_schema();
```
Be careful not to write comments in pseudo-code, that is too cumbersome and of little value.
## 9.7 TODO Comments
For functions that have not been implemented or are not perfectly implemented, sometimes we need to add TODO comments. All TODO comments must reflect the worker and the completion time. Of course, if the completion time is undecided, you can mark it clearly. For example:
```cpp
// TODO(somebody): needs another network roundtrip, will be solved by 2014/12.
```
## 9.8 Precautions
1. The comment language can be English or Chinese, and the comment style adopts //
2. Comments are often used to describe classes, function interfaces, and key implementation points. Comments are encouraged unless it is self-describing code.
3. Be sure not to forget TODO comments.

# 10 Multithreading
## 10.1 Starting and Stopping Threads
1. Except for very special cases, it is forbidden to dynamically start and stop threads. Once the server is initialized, the number of threads is fixed. Special cases such as: a backdoor reserved for the server, when all threads of the server are occupied, a worker thread is added.
2. In order to ensure that a thread will not be busy waiting in an infinite loop when exiting, the loop generally needs to judge the stop flag.
## 10.2 pthread_key
1. There are only 1024 `pthread_key` at most, and this limit cannot be increased, so special attention should be paid when using it.
2. If you want to use a large number of thread-local variables, it is recommended to use the thread number as an array subscript to obtain a thread-private variable. An `itid()` function is encapsulated in the OceanBase to obtain continuously increasing thread numbers.

```cpp
void *get_thread_local_variable()
{
  return global_array_[itid()];
}
```
## 10.3 Timers
Time-consuming tasks cannot be completed in the timer, and time-consuming tasks need to be submitted to the thread pool for execution.
## 10.4 Locking and Unlocking
It is recommended to use the Guard method to use locks
```cpp
// scope is the whole function
int foo()
{
  SpinLockGuardguard(lock_);
  ...
}
// scope is a clause
while(...) {
  SpinLockGuardguard(lock_);
  ...
}
```
If the scope of the lock is not the entire function or a certain clause, such as locking in the middle of function execution and unlocking before the function exits, manual locking and unlocking are allowed in this case:
```cpp
int foo()
{
  int ret = OB_SUCCESS;
  bool lock_succ = false;
  if (OB_SUCCESS != (ret = lock_.lock())) {
    lock_succ = false;
  } else {
    lock_succ = true;
  }
  ... // some statements are executed
  if (lock_succ) {
    lock_.unlock();
  }
  return ret;
}
```
## 10.5 Standard Usage of Cond/Signal
1. Use cond/signal through `CThreadcond` encapsulated by tbsys
2. Prohibit the use of `cond_wait()` without a timeout
3. Use cond/signal in the following idioms

```cpp
// waiting logic
cond. lock();
while(need_wait())
{
  cond.wait(timeout);
}
cond.unlock();
// wake up logic
cond.lock();
cond.signal();
cond.unlock();
```
## 10.6 Atomic Operations
Uniformly use the macros defined in ob_define.h to do atomic operations, you need to pay attention to:
1. Atomic reads and writes also need to be done with `ATOMIC_LOAD()` and `ATOMIC_STORE()`
2. Use `ATOMIC_FAA()` and `ATOMIC_AAF()` to distinguish between `fetch_and_add` and `add_and_fetch`
3. Use `ATOMIC_VCAS()` and `ATOMIC_BCAS()` to distinguish between `CAS` operations returning value or `bool`
## 10.7 Compiler Barriers
Generally, memory barriers are also required where compiler barriers are to be used, and memory barriers contain compiler barriers, so there should be no place where compiler barriers are required.
## 10.8 Memory Barriers
1. Although there are various memory barriers, we only recommend using the full barrier. Because finer barriers are very error-prone, currently in the engineering practice of OceanBase, there is no code that must use finer barriers to meet performance requirements. How complicated are the various barriers, you can refer to this document [memory-barriers](https://www.kernel.org/doc/Documentation/memory-barriers.txt)
2. The atomic operation comes with a barrier, so it is generally not necessary to manually add a barrier.
3. If you need to manually add a barrier, use a macro:
```cpp
#define MEM_BARRIER() __sync_synchronize()
```
## 10.9 Reference Counting and shared_ptr
First of all, `shared_ptr` must not be used, because `shared_ptr` is just syntactic sugar and does not solve the problem we hope to solve with reference counting.
1. To put it simply: it is safe for multiple threads to operate different `shared_ptr` at the same time, but it is not safe for multiple threads to operate the same `shared_ptr` at the same time. When we consider reference counting, it is often necessary to operate the same shared_ptr with multiple threads.
2. For details, please refer to [shared_ptr](http://en.cppreference.com/w/cpp/memory/shared_ptr)
Secondly, reference counting seems simple, but it is actually not easy to implement correctly. It is not recommended to use reference counting unless you think about it very clearly.
To use reference counting, you must first consider the following questions: How to ensure that the object is not recycled or reused before adding 1 to the reference count?
There are currently 2 methods in OceanBase that use reference counting, you can refer to:
The following simple scenarios can use reference counting:
   a. Single-threaded object construction, the initial reference count of the object is 1
   b. After that, the single thread increases the reference count, and passes the object to the rest of the threads for use, and the rest of the threads decrement the reference count after use.
   c. Finally, the single thread decides to release the object and decrements the reference count by 1.
This is the usage of `FifoAllocator` in OceanBase.
If the above simple scenario is not satisfied, a global lock is required to ensure security:
   a. Add a read lock in step 1 in Example 1
   b. Add a write lock in step 3 in Example 1
This is what UPS does when it manages `schema_mgr`
## 10.10 Alignment
In order to avoid cache false sharing, if a variable will be frequently accessed by multiple threads, it is recommended to align it with the cache line when defining the variable.
```cpp
int64_t foo CACHE_ALIGNED;
```
But if there are a large number of objects, in order to save memory, it is allowed not to align by cache line.
If it is an object constructed by dynamically applying for memory, you need to pay attention that at least the starting address of the object is 8-byte aligned. For example. If you use `page_arena`, you can allocate 8-byte aligned memory through `alloc_aligned()`.
```cpp
struct_A *p = page_arena_.alloc_aligned(sizeof(*p));
```
## 10.11 volatile
Generally speaking, it is not recommended to use volatile variables, for reasons refer to this document [volatile-considered-harmful](https://www.kernel.org/doc/Documentation/volatile-considered-harmful.txt).
Use `ATOMIC_LOAD()/ATOMIC_STORE()` instead to ensure that reads and writes to variables are not optimized away.
```cpp
// Wrong
volatile int64_ti = 0;
x = i;
i = y;
// recommended practice
int64_t i = 0;
x = ATOMIC_LOAD(&i);
ATOMIC_STORE(&i, y);
```
It is still reasonable to use volatile in a few cases, such as to indicate the state, but this state change has no strict timing meaning: such as a flag variable indicating thread exit.
```cpp
volatile bool stop_CACHE_ALIGNED;
```
Or certain monitoring items.
```cpp
volatile int64_t counter_CACHE_ALIGNED;
```
## 10.12 How to Use CAS
Because `ATOMIC_VCAS` returns the latest value of *addr in the event of an operation failure, it is not necessary to use `ATOMIC_LOAD` to read again every retry.
For example, to achieve atomic plus 1, use the CAS operation as follows:
```cpp
int64_t tmp_val = 0;
int64_told_val = ATOMIC_LOAD(addr)
while(old_val != (tmp_val = ATOMIC_VCAS(addr, old_val, old_val + 1)))
{
  old_val = tmp_val;
}
```
## 10.13 Spin Wait and PAUSE
Add `PAUSE()` to the spin wait cycle. On some CPUs, `PAUSE()` can improve performance, and generally speaking, `PAUSE()` can reduce CPU power consumption.
```cpp
while (need_retry()) {
  PAUSE();
}
```
The role of PAUSE can be seen in this answer: [What is the purpose of the "PAUSE" instruction in x86](http://stackoverflow.com/questions/12894078/pause-instruction-in-x86/12904645#12904645)
## 10.14 Critical Sections
Do not perform time-consuming or complicated operations in the critical section, such as opening/closing files, reading and writing files, etc.
## 10.15 Avoid Program Core or Exit
The restart time of the database system is often measured in hours. A large area of core or exit will cause the interruption of database services and may be exploited by malicious attackers. Therefore, the program core or exit must be avoided, such as accessing the address pointed to by a null pointer (except for temporary modification for locating bugs), or calling abort (unless an external instruction is received), etc. If it must be violated, please obtain the consent of the project leader and project architect in advance, and explain the reasons in detail.

# 11 Log Specification
Version 1.0 of the logging module has two major improvements:
**Support multi-dimensional, fine-grained printing level settings**
Compared with the previous version, which only supported the global uniform setting of the log level, the 1.0 version supports four different scopes of printing log settings: statement, session, tenant and global (or server). The setting methods of different ranges are as follows:
- SQL statement hint
- Set the session log level variable
- Set tenant log level variable
- Set syslog level variable
In addition, version 1.0 also supports the concepts of log modules and submodules. When printing logs in the program, it is necessary to indicate the module (or module + submodule) to which the log belongs and the log printing level to which the log belongs. The system supports users to set different printing levels for each module and sub-module.
**Stricter log printing format**
In version 0.5, there is a problem that the print log format is not uniform and difficult to read. For example, when printing the variable `m` whose value is 5, there are many different printing formats: "m = 5", "m=5", "m(5)",
"m is 5", "m:5", etc. The new log module allows users to print the values of required variables in the form of key-value pairs.
## 11.1 Log Printing Level

| Log Level | User | Level Definition |
| --------- | ---- | -------- |
| ERROR     | DBA  | Any unexpected, unrecoverable error requiring human intervention. The observer cannot provide normal service exceptions, such as the disk is full and the listening port is occupied. It can also be some internal inspection errors after our productization, such as our 4377 (dml defensive check error), 4103 (data checksum error), etc., which require DBA intervention to restore |
| WARN      | DBA  | In an unexpected scenario, the observer can provide services, but the behavior may not meet expectations, such as our write current limit |
| INFO      | DBA  | (Startup default level). A small amount of flagged information about system state changes. For example, a user, a table is added, the system enters daily merge, partition migration, etc. |
| EDIAG     | RD   | Error Diagnosis, diagnostic information to assist in troubleshooting, unexpected logical errors, such as function parameters that do not meet expectations, etc., usually OceanBase program BUG |
| WDIAG     | RD   | Warning Diagnosis, diagnostic information to assist in troubleshooting, expected errors, such as function return failure |
| TRACE     | RD   | Requests granular debugging information, such as printing a TRACE log at different stages of executing a SQL statement |
| DEBUG     | RD   | General and detailed debugging information to track the internal state and data structure of the system. |

It should be noted that DEBUG logs are often used for integration testing or online system debugging, and cannot be used as a substitute for unit testing.

## 11.2 Division of Printing Modules (Example)
| Module      | Submodule Definition                                |
| ----------- | --------------------------------------------------- |
| SQL         | Parser, transformer, optimizer, executor, scheduler |
| STORAGE     | TBD                                                 |
| TRANSACTION | TBD                                                 |
| ROOTSERVER  | TBD                                                 |
| COMMON      | TBD                                                 |
| DML         | TBD                                                 |

The definition of sub-modules under each module will be further refined by each group. The definitions of modules and submodules are placed in the file ob_log_module.h.
## 11.3 Setting of Print Range
Version 1.0 supports users to set the printing level separately by statement, session and global (system) scope. The priority of reference in the system is 
1. statement
2. session
3. For system global (or server), only when the previous item is not set or the setting is invalid, the system will refer to the subsequent level settings.
### 11.3.1 Statement Scope Printing Level Setting
**Set format**
Add `/*+ ... log_level=[log_level_statement]...*/` to the statement hint
(For the format of log_level_statement, see the following chapters)
**Scope of action**
The processing and execution process of the entire statement, including statement analysis, optimization, execution, etc. After the execution of the statement ends, this setting becomes invalid automatically.
### 11.3.2 Session Scope Printing Level Setting
**Set format**
```sql
sql> set @@session.log_level = '[log_level_statement]';
```
**Scope of action**
From the setting to the end of the session.
### 11.3.3 Tenant-wide Printing Level Settings
**Set format**
```sql
sql>set @@global.log_level = '[log_level_statement]';
```
**Scope of action**
It will take effect for all user sessions from the user setting until all user sessions exit.
### 11.3.4 System (or server) Wide Printing Level Setting
**Set format**

```sql
sql>alter system set log_level = '[log_level_statement]{,server_ip=xxx.xxx.xxx.xxx}';
```
**Scope of action**
When the user specifies `server_ip`, the setting takes effect only for the server, and remains valid until the server exits or restarts. When the user does not specify `server_ip`, the setting takes effect for all servers in the entire system, and remains until the entire system reboots (newly launched servers also need to obey this setting).
### 11.3.5 log_level_statement Format
```
log_level_statement =
mod_level_statement {, mod_level_statement }
mod_level_statement=
[mod[.[submod|*]]:][ERROR|WARNING|INFO|TRACE|DEBUG]
```

The definitions of mod and submod refer to section 12.2. If no mod or submod is specified, this setting will take effect for all mods. If multiple `mod_level_statement` settings conflict, the last valid setting shall prevail.
User settings do not guarantee atomicity: for example, when there are multiple settings, if the nth item setting is unsuccessful (syntax error or module does not exist), if it is a session or system-level setting, the statement will report an error, but before The effective item will not be rolled back. If it occurs in the statement hint, no error will be reported and the previous effective item will not be rolled back.
## 11.4 Unification of Log Format
Version 1.0 uses the "key=value" format to print logs uniformly. The log module uniformly provides an interface similar to the following:
```cpp
OB_MOD_LOG(mod,submod, level, "info_string", var1_name, var1, var2, 2.3, current_range, 
    range, ...);
```
The corresponding print information is

```txt
[2014-10-09 10:23:54.639198] DEBUG ob_tbnet_callback.cpp:203 [12530][Ytrace_id] info_string(var1_name=5, var2=2.3, current_range= "table_id:50,(MIN;MAX)" )
```
Among them, `info_string` is a summary of the main information of the log, which should be concise, clear, and easy to read. Avoid "operator failed" and other non-informative character strings.
The log header of each line (including information such as file name and line number) is automatically generated by the log printing module. For ease of use, the log module header file (`ob_log_module.h`) will also provide macros defined in units of modules and submodules, making the program print statements in a certain file or folder more concise, for example:
```cpp
#define OB_SQL_PARSER_LOG(level, ...) OB_MOD_LOG(sql, parser, level, ...)
```
The choice of the name of the printed variable should consider the needs of different occasions. If you do not use the variable name itself, you should consider whether there is already a variable name with the same meaning in use in the system (for example, whether the version number is printed as "data_version" or "version", or "data version" should be as uniform as possible), so as to facilitate future debugging and monitor.
In case of return due to unsuccessful operation, the log must be printed, and the error code must be printed.
Since the new log supports module and range settings, it will be more effective in filtering the printed information. In principle, the necessary debug log information should be further enriched to facilitate future troubleshooting and debugging.

# 12 Coding Constraint Summary
## 12.1 Scope
1. Namespaces correspond to directories. **Anonymous namespaces are prohibited**. **Using directives are prohibited in .h files, and only using declarations are allowed**.
2. Nested classes are suitable for scenarios that are only used by external classes. It is recommended to pre-declare in .h files and implement them in .cpp files. Try not to use `public`.
3. **In addition to the existing global variables and global functions, no new global variables and global functions shall be added**. If it must be violated, please obtain the consent of the code owner in advance, and explain the reason in detail.
4. **Local variables are declared at the beginning of the statement block, and it is mandatory to initialize simple variables when they are declared.**
5. **It is forbidden to declare non-simple variables in the loop body**. If it must be violated, please obtain the consent of the group leader in advance, and explain the reason in detail.
6. **Resource management follows the principle of "who applies for release"**. If resources need to be released, release them before the function returns or at the end of the outermost else branch. So if you need to restore the input parameters, do so before the function returns. If it must be violated, please obtain the consent of the group leader in advance, and explain the reason in detail.
## 12.2 Class
1. The constructor only does trivial initialization. Each class needs to define at least one constructor, and the destructor with virtual functions or subclasses is declared as virtual.
2. In order to avoid implicit type conversion, the single-parameter constructor needs to be declared as explicit.
3. **In principle, the copy constructor must not be used (except for the basic classes that have been defined and used)**. If it must be violated, please obtain the consent of the group leader in advance, and explain the reason in detail.
4. Use `DISALLOW_COPY_AND_ASSIGN` to avoid abuse of copy constructor and assignment operation;
5. **Class reset uses reset, reuse uses reuse, and clear is prohibited.**
6. **It is necessary to ensure that all members are initialized, and the initialization order of member variables is consistent with the order of definition.**
7. Use struct only when there is only data, and use class in all other cases.
8. The common functions contained in each class must use standard prototypes, and the serialization/deserialization functions must be implemented using macros.
9. Prioritize composition and only use inheritance for "is-a" relationships. Avoid private inheritance and multiple inheritance. When multiple inheritance is used, it is required that except for one base class with implementation, the other base classes are pure interface classes.
10. **Except for existing container classes, custom types, and a small number of global basic classes, overloading of operators is not allowed (except for simple structure assignment operations)**. If it must be violated, please obtain the consent of the group leader in advance, and explain the reasons in detail.
11. Declaration order: `public`, `protected`, `private`.
## 12.3 Functions
1. **Strictly abide by the single entry and single exit of the function. If it must be violated, please obtain the consent of the code owner and project architect in advance, and explain the reasons in detail.**
2. **Except for simple access functions set_xxx()/get_xxx() and a few exceptions (such as operator overloading, existing at(i) functions, general function reset()/reuse() of classes, etc.), all functions (public and private) should use ret to return the error code**. If the `set/get` is complicated or may make an error, ret should still be used to return the error code. Only ret variables of type int can be used to represent errors, and ret can only represent errors (except for iterator functions due to historical reasons).
3. If multiple sequential statements are doing the same thing, then, in some cases, you can use simplified writing.
4. Judging `OB_SUCCESS == ret` in the loop condition to prevent the error code from being overwritten.
5. Conditional statements need to abide by the MECE principle: each condition is independent of each other and completely exhausted, and the number of branches of a single if/else should not exceed 5 as far as possible.
6. Declare functions/function parameters as const whenever possible.
7. **The principle of coding: do not trust anyone in the code! Every function (whether public or private, except inline functions) must check the legality of each input parameter, and it is strongly recommended that inline functions also perform these checks (unless there are serious performance problems). All functions (whether public or private) must check the legality of values obtained from class member variables or through function calls (such as get return values or output parameters), even if the return value is successful, the legality of output parameters must still be checked . Variable (parameter) check, only needs to be checked once in a function (if the value obtained by calling one or several functions multiple times, then check each time)**. When defining functions, the recommended order is: input parameters first, output parameters last.
8. **Prohibit the use of assert and OB_ASSERT**.
9. Try to avoid passing in some meaningless special values when calling the function, and use constants instead.
10. On the premise of adhering to idioms, use more references.
11. It is mandatory that a single function does not exceed 120 lines. If it must be violated, please obtain the consent of the group leader in advance, and explain the reason in detail.
## 12.4 C&C++ Features
1. **Smart pointers are not allowed**, and resources are allowed to be released automatically through the Guard class.
2. It is required to use the memory allocator to apply for memory, and immediately set the pointer to NULL after the memory is released.
3. **Prohibit the use of std::string class, use ObString instead**. In addition, when manipulating C strings, it is required to use length-limited string functions.
4. **When passing an array/string/buffer as a parameter, the length must be passed at the same time. When reading and writing the content of the array/string/buffer, check whether the subscript is out of bounds.**
5. Friends can only be used in the same file. If it must be violated, please obtain the consent of the group leader in advance and explain the reason in detail. Declaring unit test classes as friends is an exception, but should be used with caution.
6. **Prohibit the use of C++ exceptions.**
7. Prohibit the use of runtime type identification (RTTI).
8. Use C++ type conversions such as `static_cast<>`, and prohibit the use of C-type conversions like int `y = (int) x`.
9. Try to use `to_cstring` output.
10. Use int for the returned ret error code, and use `int64_t` for function parameters and loop times as much as possible. In other cases, use a signed number with a specified length, such as `int32_t`, `int64_t`. Try to avoid using unsigned numbers.
11. Try to use `sizeof(var_name)` instead of `sizeof(type)`.
12. Use 0 for integers, 0.0 for real numbers, `NULL` for pointers, and '\0' for strings.
13. **In addition to the existing macros, no new macros shall be defined, and inline functions, enumerations and constants shall be used instead**. If it must be violated, please obtain the consent of the group leader in advance, and explain the reason in detail.
14. **Except for the algorithm class functions defined in the <algorithm> header file in STL, the use of STL and boost is prohibited. If it must be violated, please obtain the consent of the code owner and project architect in advance, and explain the reasons in detail.**
## 12.5 Others
- **Do not perform time-consuming or complex operations in the critical section, such as opening/closing files, reading and writing files, etc.**
- **Do not use shared_ptr and strictly limit the use of reference counting.**
- **It is necessary to avoid program core or exit, such as accessing the address pointed to by a null pointer (except temporary modification for locating bugs), or calling abort (unless an external instruction is received). If it must be violated, please obtain the consent of the code owner and project architect in advance, and explain the reasons in detail.**
