import re

from honeycomb import meta


def build_create_table_ddl(table_name, schema, col_defs,
                           col_comments, table_comment, storage_type,
                           partitioned_by, full_path,
                           tblproperties=None):
    if col_comments is not None:
        nested_col_comments = {key: value for key, value
                               in col_comments.items()
                               if '.' in key}
        col_comments = {key: value for key, value
                        in col_comments.items()
                        if '.' not in key}

        col_defs = add_comments_to_col_defs(col_defs, col_comments)
    else:
        nested_col_comments = None

    col_defs = col_defs.to_string(header=False, index=False)

    # Removing excess whitespace left by df.to_string()
    col_defs = re.sub(
        r' +',
        ' ',
        col_defs
    )

    col_defs = col_defs.replace('\n', ',\n    ')

    if nested_col_comments:
        col_defs = add_nested_col_comments(
            col_defs, nested_col_comments)

    create_table_ddl = """
CREATE EXTERNAL TABLE {schema}.{table_name} (
    {col_defs}
){table_comment}{partitioned_by}
{storage_format_ddl}
LOCATION 's3://{full_path}'{tblproperties}
    """.format(
        schema=schema,
        table_name=table_name,
        # BUG: pd.Series truncates long strings output by to_string,
        # have to cast to DataFrame first.
        col_defs=col_defs,
        table_comment=('\nCOMMENT \'{table_comment}\''.format(
            table_comment=table_comment)) if table_comment else '',
        partitioned_by=('\nPARTITIONED BY ({})'.format(', '.join(
            ['{} {}'.format(partition_name, partition_type)
             for partition_name, partition_type in partitioned_by.items()]))
            if partitioned_by else ''),
        storage_format_ddl=meta.storage_type_specs[storage_type]['ddl'],
        full_path=full_path.rsplit('/', 1)[0] + '/',
        tblproperties=('\nTBLPROPERTIES (\n  {}\n)'.format('\n  '.join([
            '\'{}\'=\'{}\''.format(prop_name, prop_val)
            for prop_name, prop_val in tblproperties.items()]))
            if tblproperties else '')
    )

    return create_table_ddl


def add_comments_to_col_defs(col_defs, comments):
    for column, comment in comments.items():
        col_defs.loc[col_defs['col_name'] == column, 'comment'] = comment

    col_defs['comment'] = (
        ' COMMENT \'' + col_defs['comment'].astype(str) + '\'')
    return col_defs


def add_nested_col_comments(col_defs, nested_col_comments):
    for col, comment in nested_col_comments.items():
        # How many layers deep we need to go to add a comment
        total_nesting_levels = col.count('.')
        current_nesting_level = 0
        block_start = 0
        block_end = -1
        col_defs = scan_ddl_level(col, comment, col_defs,
                                  block_start, block_end,
                                  current_nesting_level,
                                  total_nesting_levels)
    return col_defs


def scan_ddl_level(col, comment, col_defs,
                   level_block_start, level_block_end,
                   current_nesting_level, total_nesting_levels,):
    next_level_exists = True
    while next_level_exists:
        col_at_level = col.split('.')[current_nesting_level]
        col_at_level = re.escape(col_at_level)

        # Where the first block of the next level of nesting begins
        # AKA, the end of the current block of the current level
        next_level_block_start = col_defs.find(
            '<', level_block_start, level_block_end)

        if next_level_block_start >= 0:
            # Where the first block of the next level ends
            # AKA, the start level of the next block of the current level
            next_level_block_end = find_matching_bracket(
                col_defs, next_level_block_start)
        else:
            next_level_exists = False
            next_level_block_start = level_block_end + 1
            next_level_block_end = -1

        match = re.search(
            r'(?:^|\s*|,)({}[: ])'.format(col_at_level),
            col_defs[level_block_start:next_level_block_start])

        if match:
            col_loc = match.start(1) + level_block_start
            return handle_nested_col_match(col, comment, col_at_level, col_loc,
                                           col_defs,
                                           next_level_block_start,
                                           next_level_block_end,
                                           current_nesting_level,
                                           total_nesting_levels)

        # No match, but there is another block on the current level to search
        else:
            level_block_start = next_level_block_end + 1

    # No match, no additional blocks on current level to search
    raise ValueError(
        'Sub-field {} not found in definition for {}'.format(
            col_at_level,
            col
        ))


def handle_nested_col_match(col, comment, col_at_level, col_loc,
                            col_defs,
                            next_level_block_start, next_level_block_end,
                            current_nesting_level, total_nesting_levels):
    if current_nesting_level == total_nesting_levels:
        # Matching an array OR a struct
        array_or_struct = re.search(
            r'(?:{}[: ]\s*(?:ARRAY|STRUCT)\s*)(<)'.format(col_at_level),
            col_defs[col_loc:])
        # Array/struct columns need their comments applied
        # outside the brackets, rather than within
        if array_or_struct:
            col_start_bracket_loc = array_or_struct.start(1) + col_loc
            col_end_bracket_loc = find_matching_bracket(
                col_defs,
                col_start_bracket_loc)
            col_def_end = col_end_bracket_loc + 1
        else:
            col_end_match = re.search(
                r'(?:{}:\s*\w*\s*)([,>\n])'.format(col_at_level),
                col_defs[col_loc:])
            col_def_end = col_end_match.start(1) + col_loc

        col_defs = (col_defs[:col_def_end] +
                    ' COMMENT \'{}\''.format(comment) +
                    col_defs[col_def_end:])
        return col_defs
    else:
        current_nesting_level += 1

        # Matching an array OF a struct
        array_of_struct = re.search(
            r'(?:{}[: ]\s*ARRAY\s*<\s*STRUCT\s*)(<)'.format(col_at_level),
            col_defs[col_loc:])

        # Structs within arrays do not have names, and should not be
        # commented. So, we skip to the next level of nesting
        if array_of_struct:
            next_level_block_start = array_of_struct.start(1) + col_loc
            next_level_block_end = find_matching_bracket(
                col_defs, next_level_block_start)

        # Searching between the brackets that follow the just found col
        return scan_ddl_level(
            col, comment, col_defs,
            # Trimming the '<' and '>' characters out
            level_block_start=next_level_block_start + 1,
            level_block_end=next_level_block_end - 1,
            current_nesting_level=current_nesting_level,
            total_nesting_levels=total_nesting_levels
        )


def find_matching_bracket(col_defs, start_ind):
    bracket_count = 0
    for i, c in enumerate(col_defs[start_ind:]):
        if c == '<':
            bracket_count += 1
        elif c == '>':
            bracket_count -= 1
        if bracket_count == 0:
            return i + start_ind

    raise ValueError(
        'No matching bracket found for {} at character {}.'.format(
            col_defs[start_ind], start_ind)
        )
