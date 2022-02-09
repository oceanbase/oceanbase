# ob_error工具调研及方案

## 调研

### 同类产品

#### Oracle oerr

1.用法：

> $ oerr facility errorcode

2.举例：

如果得到`ORA-7300`的错误码，则“ora”是设施，而“7300”是错误。所以输入***oerr ora 7300***。

如果得到`LCD-111`的错误码，则输入***oerr lcd 111***。

1.输出：

> $ oerr ora 600
>
> ora-00600: internal error code, arguments: [%s], [%s], [%s], [%s], [%s], [%s], [%s], [%s]
>
> *Cause:  This is the generic internal error number for Oracle program
>
> ​     exceptions.  This indicates that a process has encountered an
>
> ​     exceptional condition.
>
> *Action: Report as a bug - the first argument is the internal error number

输出简要描述了特定Oracle错误代码的原因以及解决数据库错误的一个或多个操作

2.错误码文档

[官方文档](https://docs.Oracle.com/cd/E11882_01/server.112/e17766/toc.htm)

#### MySQL perror

1.用法

> $ perror [options] errorcode ...

perror支持option：

- `--help`, `--info`, `-I`,`-?`
  显示帮助消息并退出。
- `--silent`, `-s`
  静音模式。仅打印错误消息。
- `--verbose`, `-v`
  详细模式。打印错误代码和消息。这是默认行为。
- `--version`, `-V`
  显示版本信息并退出。

errorcode支持多种输入格式

2.举例：

对于[ER_WRONG_VALUE_FOR_VAR](https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_wrong_value_for_var)错误，perror支持以下任何参数: `1231`,`001231`, `MY-1231`, `MY-001231`或者 `ER_WRONG_VALUE_FOR_VAR`（这是一种宏定义）。

1.输出

> $ perror 1231
>
> MySQL error code MY-001231 (ER_WRONG_VALUE_FOR_VAR): Variable '%-.64s'
>
> can't be set to the value of '%-.200s'

输出描述了特定mysql错误代码的含义

支持输入多个错误码，如果错误码和操作系统错误码重叠，会显示两个错误信息

> $ perror 1 13
>
> OS error code  1:  Operation not permitted
>
> MySQL error code MY-000001: Can't create/write to file '%s' (OS errno %d - %s)
>
> OS error code  13:  Permission denied
>
> MySQL error code MY-000013: Can't get stat of '%s' (OS errno %d - %s)

2.错误码文档

[官方文档](https://dev.mysql.com/doc/mysql-errors/8.0/en/error-reference-introduction.html)

**结果分析：**

考虑采用oerr的方式，输入形式为`ob_error [option] errorcode`

### 错误码格式

#### 操作系统

> 错误码

值域在1-133

#### Oracle

> 设施+错误码

比如来源于数据库的错误，错误码格式为ora-<err_number>

错误码值域在大概0-64621，由于是 设施+错误码 的组合形式，并没有可循规律的值域范围。

具体值域参考[官方文档](https://docs.Oracle.com/cd/E11882_01/server.112/e17766/toc.htm)

#### MySQL

> ERROR 错误码 (SQLSTATE)

错误码值域在0-51999，被划分为不同的范围，每个范围都有自己的用途：

- 1 到 999：全局错误代码。此错误代码范围称为“全局”，因为它是服务器和客户端使用的共享范围。
  当此范围内的错误源自服务器端时，服务器将其写入错误日志，用前导零填充错误代码至六位并添加前缀 `MY-`.
  当此范围内的错误源自客户端时，客户端库将其提供给客户端程序，而没有零填充或前缀。
- 1,000 到 1,999：为发送到客户端的消息保留的服务器错误代码。
- 2,000 到 2,999：保留供客户端库使用的客户端错误代码。
- 3,000 到 4,999：为发送到客户端的消息保留的服务器错误代码。
- 5,000 到 5,999：保留供X插件用于发送到客户端的消息的错误代码。
- 10,000 到 49,999：为要写入错误日志（不发送到客户端）的消息保留的服务器错误代码。
  当发生此范围内的错误时，服务器将其写入错误日志，用前导零填充错误代码至六位并添加前缀`MY-`.
- 50,000 到 51,999：保留供第三方使用的错误代码。

服务器处理写入错误日志的错误消息与发送给客户端的错误消息不同：

- 当服务器将消息写入错误日志时，它会用前导零填充错误代码到六位并添加前缀`MY-`（例如： `MY-000022`， `MY-010048`）。
- 当服务器向客户端程序发送消息时，它不会向错误代码添加零填充或前缀（例如： `1036`, `3013`）。

其中，mysql错误信息还包括一个SQLSTATE值的元素，该值是一个由五个字符组成的字符串（例如，‘HY000’）。SQLSTATE 值取自 ANSI SQL 和 ODBC，比数字错误代码更标准化。

#### OceanBase

首先明确一点，ob内部的错误码在绝大多数情况下是不会重复的，其值域在1-65535。但是用户所得到的错误码并不是ob内部的错误码，而是根据MySQL或者Oracle模式，经过了一次映射所得到的相应模式的错误码。

并且，ob内部的每个错误码都是同时映射了MySQL以及Oracle两种模式的，表示**同种含义**下两种模式的不同错误输出形式，输出时只需要根据具体模式输出相应模式的错误码形式。

##### MySQL模式

> ERROR 错误码 (SQLSTATE)
>
> 如：ERROR 1007 (HY000)

错误码在1000-3999表示MySQL原有的错误，4000-65535表示ob的错误，具体参考[文档](https://www.oceanbase.com/docs/oceanbase-database/oceanbase-database/V3.1.2/use-error-information-1)。

ob为MySQL的原有错误类型保留1-3999的错误码值域（并不是所有的值都覆盖，因为有的MySQL错误并不会在ob上发生），也就是说ob内部的错误码是没有1-3999的。

比如，对于`ERROR 1007`这样小于4000的MySQL错误码，ob内部的错误码实际上是一个大于3999的其他值（暂时没有规律可循），这里为5158；

> ERROR 1007 (HY000) : Can't create database '%s'; database exists
>
> > OceanBase 错误码：5158

而对于`ERROR 5805`这样大于3999的MySQL错误码，ob内部的错误码与之相等，为5805。

> ERROR 5805 (HY000) : non-constant expression is not allowed for pivot|unpivot values
>
> > OceanBase 错误码：5805

MySQL模式下在[1,4000)范围内有着极少数特殊的错误码存在冲突问题，其MySQL错误码相同，但含义不同且ob内部错误码也不同，详见[文档](https://www.oceanbase.com/docs/oceanbase-database/oceanbase-database/V3.1.2/0001-3999#title-9k7-sdh-gch)。以`ERROR 1064`为例：

> ERROR 1064 (0B000) : Failed to init SQL parser
>
> > OceanBase 错误码：5000
>
> ERROR 1064 (42000) : Parse error ","%s near \'%.*s\' at line %d
>
> > OceanBase 错误码：5001
>
> ERROR 1064 (42000) : Wrong number of partitions defined, mismatch with pervious setting
>
> > OceanBase 错误码：5282

##### Oracle模式

> 前缀-错误码
>
> 如：ORA-00910

Oracle模式的错误码与Oracle的原有错误码保持一致，ob内部的对应错误码则取决于具体含义。比如以上一章节提及的ob内部错误码5805为例，其对应的错误类型在Oracle中存在，为`ORA-56901`错误，那么在Oracle模式下的错误输出则为：

> ORA-56901：non-constant expression is not allowed for pivot|unpivot values

相反，ob可能存在一些Oracle本身并没有的错误类型（称为不兼容Oracle的错误），这种情况下，Oracle模式的错误码就保持00600（00600同时也表示了部分Oracle本身有的错误类型），并在输出信息中以`arguments`指示ob的内部错误码。比如以上一章节提及的ob内部错误码5158（`ERROR 1007`)为例，Oracle本身并没有该错误类型，因此在Oracle模式下的错误输出为：

> ORA-00600：internal error code, arguments: -5158, Can't create database '%.*s'; database exists

Oracle模式下同样有部分特殊的错误码，存在设施+错误码相同的情况，但相同错误码的ob内部错误码是不相同的。

极特殊案例：ORA-12899(ob errorcode 5167) 和ORA-01861(ob errorcode 4219)，出现了不同Oracle的错误码对应相同ob内部错误码的情况，从而违背了ob内部错误码不应该重复的原则，使得映射关系不明确

**结果分析：**

错误码1-999目前没有支持任何错误类型，因此可以直接支持OS的错误码

值域冲突可能发生在不同模式之间（1）或者相同模式之间（2）。

冲突1，可以通过提供**前缀**（如`ORA`）加以区分；

冲突2，对于`ora-00600`并且是不兼容Oracle模式（带有`argument`）的错误之间的冲突，可以通过额外输入ob内部错误码（`argument`）解决；

冲突2，对于其他相同模式下的相同错误码冲突，可以将同一错误码的所有不同情况全部输出。

## 方案设计

1.工具名：ob_error

2.用法

> $ ob_error [option1]
>
> $ ob_error [facility] errorcode [option2]

ob_error支持option1：

- `--help`, `-h`
  显示帮助消息并退出。
- `--version`, `-V`
  显示版本信息并退出。

ob_error支持option2:

- `--argument=ARG`, `-a=ARG`

表示额外输入argument，用法举例：`$ob_error ora 600 -a=5542`

facility用于区分MySQL模式和Oracle模式:

- `my`,`[Mm][Yy]`

  MySQL模式

- `ora`,`[Oo][Rr][Aa]`

  Oracle模式，错误源于数据库。

- `pls`, `[Pp][Ll][Ss]`

  Oracle模式，错误源于存储过程。

errorcode可以有前缀0输入，如00600。

在不输入facility的情况下，按两种模式解析错误码并将两种模式的结果全部输出；

在输入facility的情况下，按对应模式和设施解析错误码，只输出单模式（MySQL模式或Oracle模式）的对应信息。

在ora 00600的特殊情况下，如果用户没有提前输入-a参数，那么输出Oracle自身所表示的ORA-00600错误。

输出为错误代码、错误基本消息、错误原因cause以及错误可能的解决方法solution。

1.用例1

> $ ob_error my 4000
>
>
> ERROR 4000 (HY000): Common error
>
> Cause: to be continue
>
> Solution: to be continue

2.用例2

> $ ob_error 13
>
>
> OS Error 13: Permission denied
>
> Cause: to be continue
>
> Solution: to be continue

1.用例3

> $ ob_error 600
>
>
> ORA-00600: auto increment service busy
>
> Cause: to be continue
>
> Solution: to be continue
>
>
> ORA-00600: rowid type mismatch, expect %s, got %s
>
> Cause: to be continue
>
> Solution: to be continue
>
>
> ORA-00600: rowid type mismatch, expect %ld, actual %ld
>
> Cause: to be continue
>
> Solution: to be continue

2.用例4

> $ ob_error ora 00051
>
>
> ORA-00051: timeout occurred while waiting for a resource
>
> Cause: to be continue
>
> Solution: to be continue

1.用例5

> $ ob_error ora 600 -a=5858
>
>
> ORA-00600: internal error code, arguments: -5858, Conflicting declarations: '%s' and '%s'
>
> Cause: to be continue
>
> Solution: to be continue

2.用例6

> ob_error 5858
>
>
> ERROR 5858 (42000): Conflicting declarations: '%s' and '%s'
>
> Cause: to be continue
>
> Solution: to be continue
>
>
> ORA-00600: internal error code, arguments: -5858, Conflicting declarations: '%s' and '%s'
>
> Cause: to be continue
>
> Solution: to be continue

## 方案实现

1.在oberror_errno.def内增加C2/C3字段分别表示错误原因和错误解决方法

2.具体错误原因和解决方法暂时用通用字段“to be continue”代替，后续由研发同学更新，更新方法可参考oberror_errno.def文件的注解，更新后需重新执行gen_oberror_errno.pl

1.增加Oracle模式错误码到ob错误码，以及mysql模式错误码（小于4000）到ob错误码的映射关系

2.根据最终映射得到的ob错误码，从数组中获取错误信息（含义/原因/解决方法等）

1.实现命令行解析，根据用户输入，输出错误信息

2.任何错误码的更新，只需要对应更新oberror_errno.def文件，重新解析生成cpp/h文件后重新编译，映射关系将能够自动更新
