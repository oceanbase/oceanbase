#!/usr/bin/env python3

"""Verify parser token definitions and keyword arrays stay in sync.

How to use:
  python3 src/sql/parser/check_keyword_token_mapping.py { mysql | oracle | plmysql | ploracle }

What this script checks:
  - The reserved/non-reserved keyword token types declared in one yacc file must match the
    ReservedKeyword/NonReservedKeyword arrays in the corresponding .c file.
  - Commented-out tokens in yacc or in keyword arrays are ignored.
  - Known aliases and intentional yacc/.c mismatches are handled through
    `yacc_only_ignore` and `c_only_ignore`.

Exit code:
  - 0: no mismatch was found
  - 1: mismatches were found and reported, or arguments were invalid

"""

# How to maintain ignore lists?
# Aliases are auto-derived from the keyword arrays in the .c file. The two ignore lists
# only capture intentional yacc/.c mismatches that should not fail the check.
#
# 1. `c_only_ignore`: token exists in the .c array but not in the .y section
#    > for example: "NULLX" is defined in `MYSQL_RESERVED_KEYWORDS` .c array
#    > but not defined as a reserved keyword in `sql_parser_mysql_mode.y`
#    > otherwise there could be grammar conflicts
# 2. `yacc_only_ignore`: token exists in the .y section but not in the .c array
#    > for example: "BLOCK_INDEX" is defined as a non reserved keyword in `sql_parser_mysql_mode.y`
#    > but there is no matching pattern in `MYSQL_NON_RESERVED_KEYWORDS` array of `non_reserved_keywords_mysql_mode.c`
#    > this could be a historical reason like functionality not implemented

import re
import sys
from pathlib import Path
from typing import Iterable, Tuple, Set


class KeywordSectionSpec(object):
    def __init__(self, c_array_name, yacc_only_ignore=(), c_only_ignore=()):
        self.c_array_name = c_array_name
        self.yacc_only_ignore = yacc_only_ignore
        self.c_only_ignore = c_only_ignore


class SourceSpec(object):
    def __init__(self, source_name, yacc_path, c_path, reserved, non_reserved):
        self.source_name = source_name
        self.yacc_path = yacc_path
        self.c_path = c_path
        self.reserved = reserved
        self.non_reserved = non_reserved


BASE_DIR = Path(__file__).resolve().parents[3]

RESERVED_KEYWORD_BEGIN = "//-----------------------------reserved keyword begin-----------------------------------------------"
RESERVED_KEYWORD_END = "//-----------------------------reserved keyword end-------------------------------------------------"
NON_RESERVED_KEYWORD_BEGIN = "//-----------------------------non_reserved keyword begin-------------------------------------------"
NON_RESERVED_KEYWORD_END = "//-----------------------------non_reserved keyword end---------------------------------------------"

SQL_MYSQL_SPEC = SourceSpec(
    source_name="mysql sql",
    yacc_path=BASE_DIR / "src/sql/parser/sql_parser_mysql_mode.y",
    c_path=BASE_DIR / "src/sql/parser/non_reserved_keywords_mysql_mode.c",
    reserved=KeywordSectionSpec(
        c_array_name="MYSQL_RESERVED_KEYWORDS",
        yacc_only_ignore=(),
        c_only_ignore=("BOOL_VALUE", "NULLX"),
    ),
    non_reserved=KeywordSectionSpec(
        c_array_name="MYSQL_NON_RESERVED_KEYWORDS",
        yacc_only_ignore=("BLOCK_INDEX", "BLOOM_FILTER", "CLOG", "COLUMN_STAT", "ILOG", "RANDOM"),
        c_only_ignore=(),
    )
)


SQL_ORACLE_SPEC = SourceSpec(
    source_name="oracle sql",
    yacc_path=BASE_DIR / "close_modules/oracle_parser/sql/parser/sql_parser_oracle_mode.y",
    c_path=BASE_DIR / "src/sql/parser/non_reserved_keywords_oracle_mode.c",
    reserved=KeywordSectionSpec(
        c_array_name="ORACLE_RESERVED_KEYWORDS",
        yacc_only_ignore=("FALSE", "TRUE"),
        c_only_ignore=("NULLX", "BOOL_VALUE"),
    ),
    non_reserved=KeywordSectionSpec(
        c_array_name="ORACLE_NON_RESERVED_KEYWORDS",
        yacc_only_ignore=("BLOCK_INDEX", "BLOOM_FILTER", "CLOG", "COLUMN_STAT", "ILOG", "PRIMARY_ROOTSERVICE_LIST", "RANDOM"),
        c_only_ignore=(),
    ),
)


PL_MYSQL_SPEC = SourceSpec(
    source_name="mysql pl",
    yacc_path=BASE_DIR / "src/pl/parser/pl_parser_mysql_mode.y",
    c_path=BASE_DIR / "src/pl/parser/pl_non_reserved_keywords_mysql_mode.c",
    reserved=KeywordSectionSpec(
        c_array_name="MYSQL_PL_RESERVED_KEYWORDS",
        yacc_only_ignore=("ALTER", "BINARY", "CALL", "CREATE", "DO", "DROP", "INDEX", "INTO", "LOAD", "PROCEDURE", "RENAME", "SELECT", "SET", "TABLE", "TRIGGER"),
        c_only_ignore=(),
    ),
    non_reserved=KeywordSectionSpec(
        c_array_name="MYSQL_PL_NON_RESERVED_KEYWORDS",
        yacc_only_ignore=("COUNT", "FUNCTION", "JSON"),
        c_only_ignore=(),
    ),
)


PL_ORACLE_SPEC = SourceSpec(
    source_name="oracle pl",
    yacc_path=BASE_DIR / "close_modules/oracle_pl/pl/parser/pl_parser_oracle_mode.y",
    c_path=BASE_DIR / "close_modules/oracle_pl/pl/parser/pl_non_reserved_keywords_oracle_mode.c",
    reserved=KeywordSectionSpec(
        c_array_name="ORACLE_PL_RESERVED_KEYWORDS",
        yacc_only_ignore=(),
        c_only_ignore=("SQL_KEYWORD",),
    ),
    non_reserved=KeywordSectionSpec(
        c_array_name="ORACLE_PL_NON_RESERVED_KEYWORDS",
        yacc_only_ignore=("JSON", "VARCHAR"),
        c_only_ignore=(),
    ),
)


MODE_SPECS = {
    "mysql": SQL_MYSQL_SPEC,
    "oracle": SQL_ORACLE_SPEC,
    "plmysql": PL_MYSQL_SPEC,
    "ploracle": PL_ORACLE_SPEC,
}


def usage() -> str:
    return (
        "Usage: python3 src/sql/parser/check_keyword_token_mapping.py "
        "{mysql|oracle|plmysql|ploracle}"
    )


def strip_block_comments(text: str) -> str:
    """Remove C-style block comments so commented tokens do not participate in checks."""
    return re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)


def extract_token_section(text: str, start_marker: str) -> str:
    """Extract the body of a %token declaration block.

    The block starts at the line exactly matching start_marker and stops when the
    next yacc directive line beginning with '%' is reached.
    """
    lines = text.splitlines()
    start_index = None
    for index, line in enumerate(lines):
        if line.strip() == start_marker:
            start_index = index + 1
            break
    if start_index is None:
        raise ValueError(f"cannot find start marker: {start_marker}")

    section_lines = []
    for line in lines[start_index:]:
        stripped = line.lstrip()
        if stripped.startswith("%"):
            break
        section_lines.append(line)
    return "\n".join(section_lines)


def extract_marker_annotated_token_block(text: str, begin_marker: str, end_marker: str):
    """Extract token type names from a comment-wrapped yacc %token block.

    The body is located strictly between `begin_marker` and `end_marker` lines
    (both must match exactly after `.strip()`).
    """
    lines = text.splitlines()
    begin_index = None
    end_index = None
    for i, line in enumerate(lines):
        if line.strip() == begin_marker:
            begin_index = i + 1
            break
    if begin_index is None:
        raise ValueError(f"cannot find begin marker: {begin_marker}")

    for j in range(begin_index, len(lines)):
        if lines[j].strip() == end_marker:
            end_index = j
            break
    if end_index is None:
        raise ValueError(f"cannot find end marker: {end_marker}")

    section = "\n".join(lines[begin_index:end_index])
    # Remove block comments so commented-out tokens do not participate in checks.
    section = strip_block_comments(section)
    return set(re.findall(r"\b[A-Z_][A-Z0-9_]*\b", section))


def extract_yacc_token_block(text: str, start_marker: str):
    """Extract token type names from one yacc %token block after removing comments."""
    section = strip_block_comments(extract_token_section(text, start_marker))
    return set(re.findall(r"\b[A-Z_][A-Z0-9_]*\b", section))


def extract_keyword_array_body(text: str, array_name: str) -> str:
    """Extract the initializer body of one keyword array from the .c file."""
    match = re.search(
        rf"static\s+const\s+\w+\s+{array_name}\[\]\s*=\s*\{{(.*?)\}};",
        text,
        flags=re.DOTALL,
    )
    if match is None:
        raise ValueError(f"cannot find array body: {array_name}")
    return strip_block_comments(match.group(1))


def extract_keyword_array_entries(text: str, array_name: str):
    """Extract (keyword literal, token type) entries from one keyword array."""
    body = extract_keyword_array_body(text, array_name)
    return re.findall(r'\{\s*"([^"]+)"\s*,\s*([A-Z_][A-Z0-9_]*)\s*\}', body)


def extract_keyword_array_token_types(text: str, array_name: str):
    """Extract token types from one keyword array of {\"word\", TOKEN_TYPE} entries."""
    return {token_type for _, token_type in extract_keyword_array_entries(text, array_name)}


def auto_detect_aliases(text: str, array_name: str) -> dict:
    """Infer yacc-token to c-token aliases from keyword array entries.

    For entries like {"char", CHARACTER}, lex produces CHARACTER while yacc usually
    declares CHAR. Derive that alias from the .c array directly so maintainers only
    keep truly exceptional cases in token_exceptions.
    """
    aliases = {}
    for keyword_literal, token_type in extract_keyword_array_entries(text, array_name):
        yacc_token = keyword_literal.upper()
        if yacc_token != token_type:
            aliases[yacc_token] = token_type
    return aliases


def normalize_tokens(tokens: Iterable[str], aliases: dict):
    """Apply alias mapping before comparing token sets."""
    return {aliases.get(token, token) for token in tokens}


def render_report_section(title: str, diff_tokens, fix_hint: str):
    if not diff_tokens:
        return None
    return "\n".join([title + ":", *[f"  - {token}" for token in sorted(diff_tokens)], fix_hint])


def build_fix_hint(spec: SourceSpec, section_name: str, report_type: str) -> str:
    def get_spec_var_name(spec: SourceSpec) -> str:
        for name, value in globals().items():
            if name.endswith("_SPEC") and value is spec:
                return name
        raise ValueError(f"cannot find SourceSpec variable name for: {spec.source_name}")

    section = getattr(spec, section_name)
    spec_var_name = get_spec_var_name(spec)
    c_path = spec.c_path.relative_to(BASE_DIR).as_posix()
    yacc_path = spec.yacc_path.relative_to(BASE_DIR).as_posix()
    ignore_field = "yacc_only_ignore" if report_type == "yacc-only" else "c_only_ignore"
    return (
        f"Check if token type matches between `{section.c_array_name}` in {c_path} "
        f"and the reserved/non-reserved keyword token blocks in {yacc_path}.\n"
        f"Or you can modify `{spec_var_name}.{section_name}.{ignore_field}` in {Path(__file__).name} "
        f"(SEE <How to maintain ignore lists?>)"
    )


def build_report(
    spec: SourceSpec,
    yacc_reserved,
    c_reserved,
    yacc_non_reserved,
    c_non_reserved,
) -> str:
    """Build the final human-readable mismatch report."""
    c_text = spec.c_path.read_text(encoding="utf-8")
    section_tokens = (
        ("reserved", yacc_reserved, c_reserved),
        ("non_reserved", yacc_non_reserved, c_non_reserved),
    )
    report_sections = []

    for section_name, yacc_tokens, c_tokens in section_tokens:
        section = getattr(spec, section_name)
        aliases = auto_detect_aliases(c_text, section.c_array_name)
        normalized_yacc_tokens = normalize_tokens(yacc_tokens, aliases)
        yacc_only_ignore = set(section.yacc_only_ignore)
        c_only_ignore = set(section.c_only_ignore)
        title_prefix = section_name.replace("_", "-")
        report_sections.extend(filter(None, (
            render_report_section(
                f"{title_prefix} yacc-only",
                normalized_yacc_tokens - c_tokens - yacc_only_ignore,
                build_fix_hint(spec, section_name, "yacc-only"),
            ),
            render_report_section(
                f"{title_prefix} c-only",
                c_tokens - normalized_yacc_tokens - c_only_ignore,
                build_fix_hint(spec, section_name, "c-only"),
            ),
        )))

    return "OK" if not report_sections else "\n\n".join(report_sections)


def collect_tokens(spec: SourceSpec):
    yacc_text = spec.yacc_path.read_text(encoding="utf-8")
    c_text = spec.c_path.read_text(encoding="utf-8")
    section_names = ("reserved", "non_reserved")
    token_sets = []

    for section_name in section_names:
        section = getattr(spec, section_name)
        if section_name == "reserved":
            yacc_token_block = extract_marker_annotated_token_block(
                yacc_text, RESERVED_KEYWORD_BEGIN, RESERVED_KEYWORD_END
            )
        elif section_name == "non_reserved":
            yacc_token_block = extract_marker_annotated_token_block(
                yacc_text, NON_RESERVED_KEYWORD_BEGIN, NON_RESERVED_KEYWORD_END
            )
        else:
            raise ValueError(f"unexpected section_name: {section_name}")
        token_sets.extend((
            yacc_token_block,
            extract_keyword_array_token_types(c_text, section.c_array_name),
        ))

    return tuple(token_sets)


def main():
    if len(sys.argv) < 2:
        print("Missing required mode argument.")
        print(usage())
        return 1

    mode = sys.argv[1].lower()
    spec = MODE_SPECS.get(mode)
    if spec is None:
        print(f"Unsupported mode: {mode}")
        print(usage())
        return 1

    report = build_report(spec, *collect_tokens(spec))
    if report != "OK":
        print(report)
    return 0 if report == "OK" else 1


if __name__ == "__main__":
    sys.exit(main())
