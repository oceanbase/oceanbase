#!/usr/bin/env python3
"""
Replace all Mulan PubL v2 license headers with Apache 2.0 SPDX headers.
Handles:
  1. /* */ block comment style (anywhere in file, including string literals)
  2. // line comment style
  3. #  line comment style (Perl/Shell)
  4. Bare text in markdown code blocks (docs)
  5. RPM spec/cmake metadata fields
"""

import os
import re
import sys


SKIP_PATHS = {
    '.git', '.secignore', '.secignore.opensource', 'LICENSE',
    'replace_license.py',
    os.path.join('test', 'static', 'license_scan.py'),
    os.path.join('test', 'static', 'license', 'license.py'),
}


def find_files_with_mulan(root_dir):
    mulan_files = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        rel = os.path.relpath(dirpath, root_dir)
        if rel == '.git' or rel.startswith('.git' + os.sep):
            dirnames[:] = []
            continue
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            frel = os.path.relpath(filepath, root_dir)
            if frel in SKIP_PATHS:
                continue
            try:
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                if 'MulanPubL-2.0' in content or 'Mulan PubL v2' in content \
                   or 'MulanPubL-1.0' in content or 'Mulan PubL v1' in content:
                    mulan_files.append(filepath)
            except (IOError, OSError):
                pass
    return mulan_files


def extract_year(text):
    m = re.search(r'Copyright\s*\((?:c|code)\)\s*(\d{4}(?:\s*-\s*\d{4})?)', text)
    return m.group(1).strip() if m else '2021'


def replace_block_comments(content):
    """Replace /* */ style Mulan license blocks (works anywhere in file)."""
    pattern = re.compile(r'/\*\*?(?:[^*]|\*(?!/))*\*/')

    def repl(m):
        block = m.group(0)
        if ('Mulan PubL' in block or 'MulanPubL' in block or 'Mulan PSL' in block) and 'OceanBase' in block:
            year = extract_year(block)
            return (
                '/**\n'
                ' * Copyright (c) {year} OceanBase\n'
                ' * SPDX-License-Identifier: Apache-2.0\n'
                ' */'.format(year=year)
            )
        return block

    return pattern.sub(repl, content)


def replace_doubleslash_comments(content):
    """Replace consecutive // line Mulan license blocks."""
    lines = content.split('\n')
    result = []
    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        if stripped.startswith('//') and 'Copyright' in stripped and 'OceanBase' in stripped:
            block_lines = [lines[i]]
            j = i + 1
            while j < len(lines) and lines[j].strip().startswith('//'):
                block_lines.append(lines[j])
                j += 1
            block_text = '\n'.join(block_lines)
            if 'Mulan PubL' in block_text or 'MulanPubL' in block_text or 'Mulan PSL' in block_text:
                year = extract_year(block_text)
                result.append('// Copyright (c) {year} OceanBase'.format(year=year))
                result.append('// SPDX-License-Identifier: Apache-2.0')
                i = j
                continue
        result.append(lines[i])
        i += 1
    return '\n'.join(result)


def replace_hash_comments(content):
    """Replace consecutive # line Mulan license blocks (Perl/Shell)."""
    lines = content.split('\n')
    result = []
    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        if stripped.startswith('#') and not stripped.startswith('#!') and \
           'Copyright' in stripped and 'OceanBase' in stripped:
            block_lines = [lines[i]]
            j = i + 1
            while j < len(lines):
                next_stripped = lines[j].strip()
                if next_stripped.startswith('#') and not next_stripped.startswith('#!'):
                    block_lines.append(lines[j])
                    j += 1
                else:
                    break
            block_text = '\n'.join(block_lines)
            if 'Mulan PubL' in block_text or 'MulanPubL' in block_text or 'Mulan PSL' in block_text:
                year = extract_year(block_text)
                result.append('# Copyright (c) {year} OceanBase'.format(year=year))
                result.append('# SPDX-License-Identifier: Apache-2.0')
                i = j
                continue
        result.append(lines[i])
        i += 1
    return '\n'.join(result)


def replace_bare_text(content):
    """Replace bare text Mulan license (in markdown code blocks)."""
    pattern = re.compile(
        r'(Copyright\s*\(c\)\s*(\d{4}(?:\s*-\s*\d{4})?)\s+OceanBase\s*\n)'
        r'((?:.*\n)*?)'
        r'(See the Mulan PubL v[12] for more details\.\s*\n)',
        re.MULTILINE
    )

    def repl(m):
        year = m.group(2).strip()
        return (
            'Copyright (c) {year} OceanBase\n'
            'SPDX-License-Identifier: Apache-2.0\n'.format(year=year)
        )

    return pattern.sub(repl, content)


def replace_metadata_fields(content):
    """Replace Mulan PubL v2 in metadata fields (RPM spec, CMake, etc.)."""
    content = re.sub(
        r'(License:\s*)Mulan PubL v2\.?',
        r'\g<1>Apache-2.0',
        content
    )
    content = re.sub(
        r'(CPACK_RPM_PACKAGE_LICENSE\s+"?)Mulan PubL v2\.?("?)',
        r'\g<1>Apache-2.0\g<2>',
        content
    )
    content = re.sub(
        r'\tMulan PubL v2\t',
        '\tApache-2.0\t',
        content
    )
    content = content.replace('OBP_LICENSE_MULAN_PUBL_V2', 'OBP_LICENSE_APACHE_V2')
    content = content.replace('OBP_LICENSE_MULAN_PSL_V2', 'OBP_LICENSE_APACHE_V2')
    return content


def process_file(filepath, root_dir, dry_run=False):
    try:
        with open(filepath, 'rb') as f:
            raw = f.read()
        content = raw.decode('utf-8', errors='ignore')
    except (IOError, OSError) as e:
        print("  ERROR reading {}: {}".format(filepath, e))
        return False

    original = content
    content = replace_block_comments(content)
    content = replace_doubleslash_comments(content)
    content = replace_hash_comments(content)
    content = replace_bare_text(content)
    content = replace_metadata_fields(content)

    if content != original:
        rel = os.path.relpath(filepath, root_dir)
        if dry_run:
            print("  [DRY-RUN] Would modify: {}".format(rel))
        else:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print("  Modified: {}".format(rel))
        return True
    return False


def main():
    dry_run = '--dry-run' in sys.argv
    root_dir = os.path.dirname(os.path.abspath(__file__))

    print("Scanning for files containing Mulan PubL v2 license...")
    files = find_files_with_mulan(root_dir)
    print("Found {} files with Mulan license references.\n".format(len(files)))

    modified = 0
    skipped = 0
    for filepath in sorted(files):
        if process_file(filepath, root_dir, dry_run=dry_run):
            modified += 1
        else:
            rel = os.path.relpath(filepath, root_dir)
            print("  Unchanged (no pattern matched): {}".format(rel))
            skipped += 1

    print("\nDone. Modified: {}, Unchanged: {}".format(modified, skipped))
    if dry_run:
        print("(Dry-run mode - no files were actually changed)")


if __name__ == '__main__':
    main()
