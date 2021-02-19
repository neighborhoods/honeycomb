import re

from honeycomb import meta


def build_create_table_ddl(table_name, schema, col_defs,
                           col_comments, table_comment, storage_type,
                           partitioned_by, full_path,
                           tblproperties=None):
    """
    Assembles the CREATE TABLE statement for the DataFrame being uploaded.

    Args:
        table_name (str):
            The name of the table to be created
        schema (str):
            The schema the table is being created within
        col_defs (pd.DataFrame)
            A DataFrame containing the columns 'col_name' and 'dtype',
            describing the dtype of each column in the DataFrame being uploaded
        col_comments (dict<str:str>):
            A mapping from column/field names to column comments to be applied
            in the create statement
        table_comment (str):
            A table comment to be applied in the create statement
        storage_type (str):
            The underlying file type that the table will be based upon
        partitioned_by (dict<str:str>)
            A mapping from a partition key to the dtype of the
            partition values
        full_path (str):
            The full S3 path that the tables files will be stored at -
            not including the S3 prefix.
        tblproperties (str, optional):
            Any arguments or configurations to be provided in the TBLPROPERTIES
            section of the create table statement.

    Returns
        str: The assembled create table statement.
    """
    col_defs = format_col_defs(col_defs, col_comments)

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


def format_col_defs(col_defs, col_comments):
    """
    Formats col_defs as a string and inserts column comments into it

    Args:
        col_defs (pd.DataFrame)
            A DataFrame containing the columns 'col_name' and 'dtype',
            describing the dtype of each column in the DataFrame being uploaded
        col_comments (dict<str:str>):
            A mapping from column/field names to column comments to be applied
            in the create statement
    Returns:
        str: col_defs as a string with column comments inserted
    """
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

    return col_defs


def add_comments_to_col_defs(col_defs, col_comments):
    """
    Iterates through a mapping from col names to comments, and adds the
    comments to a new column on matching col names

    Args:
        col_defs (pd.DataFrame)
            A DataFrame containing the columns 'col_name' and 'dtype',
            describing the dtype of each column in the DataFrame being uploaded
        col_comments (dict<str:str>):
            A mapping from column/field names to column comments to be applied
            in the create statement

    Returns:
        pd.DataFrame: col_defs with a 'comment' column appended and populated
    """
    for column, comment in col_comments.items():
        col_defs.loc[col_defs['col_name'] == column, 'comment'] = comment

    col_defs['comment'] = (
        ' COMMENT \'' + col_defs['comment'].astype(str) + '\'')
    return col_defs


def add_nested_col_comments(col_defs, nested_col_comments):
    """
    Adds nested column comments to the fully formatted column
    definition section of the create table statement. Nested column/field
    comments must be handled quite differently, and the least intrusive method
    of doing so is to parse and iterate through the otherwise completed DDL

    Args:
        col_defs (str)
            The fully formatted, translated-from-dataframe column definition
            section of a DDL statement.
        nested_col_comments (dict<str:str>):
            A mapping from nested column/field names to column comments to
            be applied in the create statement
    Returns:
        str: The create table statement with nested column comments added
    """
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
                   current_nesting_level, total_nesting_levels):
    """
    Scans through a 'level' of DDL in the column definitions of a create
    table statement. A 'level', in this case, represents an indentation
    level in a SQL string (if it were properly formatted, which is not
    required for this function). So, a level includes all columns
    and their types UNLESS a column/field contains structs or arrays
    of structs. In that case, the nested fields constitute their own level,
    and are not included in the original level.

    This function operates on 'blocks' at a time. A 'block', in this case,
    represents a section of col_defs that does not contain any definitions
    of nested fields. If a level is being scanned and nested field definitions
    are found, the function scans until the nested definition begins (AKA
    where the first 'block' ends), and then resume after the nested definition
    ends (where the next 'block' begins).

    If a deeper level of nesting is encountered while scanning and the
    column/field we're searching for is supposed to be in a deeper layer, a
    scan of that encountered deeper level will automatically begin (if all
    prerequisite parent column/field matches have been satisfied).

    Args:
        col (str):
            The full name of the column/field definition being searched for,
            including parent column names in the prefix.
        comment (str):
            The comment to be added to the column/field being searched for
        col_defs (str):
            The fully formatted, translated-from-dataframe column definition
            section of a DDL statement.
        level_block_start (int):
            Index of col_defs that the current block starts at
        level_block_end (int):
            Index of col_defs that the current block ends at
        current_nesting_level (int):
            The level of nesting currently being scanned through. If
            current_nesting_level == total_nesting_levels, we have reached
            the desired level of nesting to find the column/field
            to add a comment to
        total_nesting_levels (int):
            How many levels of nesting are required to scan to find the
            column/field definition being serached for. Determined by
            how many '.' characters are in 'col'

    Returns:
        str: col_defs with the nested column comment for col inserted

    Raises:
        ValueError:
            If a definition for a col or one of its sub-fields was not found
    """
    next_level_exists = True
    while next_level_exists:
        # Only want to search for the subfield that would exist at this level
        col_at_level = col.split('.')[current_nesting_level]
        col_at_level = re.escape(col_at_level)

        # Where the first block of the next level of nesting begins
        # AKA, the end of the current block of the current level
        next_level_block_start = col_defs.find(
            '<', level_block_start, level_block_end)

        # If another level of nesting exists somewhere in the remainder of
        # the col_defs string
        if next_level_block_start >= 0:
            # Where the next level of nesting ends
            # AKA, the start point of the next block of the current level
            next_level_block_end = find_matching_bracket(
                col_defs, next_level_block_start)

        # No more nesting to be found - the current block will take us
        # to the end of the string. Without a match in the current block,
        # there is no matching defined column to what is specified in
        # col_comments, and an error will be raised
        else:
            next_level_exists = False
            next_level_block_start = level_block_end + 1
            next_level_block_end = -1

        # Check if there is a match for the current column level in the
        # current block of this level
        match = re.search(
            r'(?:^|\s*|,)({}[: ])'.format(col_at_level),
            col_defs[level_block_start:next_level_block_start])

        # Match found in current block
        if match:
            col_loc = match.start(1) + level_block_start
            return handle_nested_col_match(col, comment, col_at_level, col_loc,
                                           col_defs,
                                           next_level_block_start,
                                           next_level_block_end,
                                           current_nesting_level,
                                           total_nesting_levels)

        # No match, but there may be another block on the level to search
        else:
            level_block_start = next_level_block_end + 1

    # No match, no additional blocks on current level to search
    raise ValueError(
        'Sub-field {} not found in definition for {}'.format(
            col_at_level,
            col
        ))


def handle_nested_col_match(col, comment, col_at_level, col_loc, col_defs,
                            next_level_block_start, next_level_block_end,
                            current_nesting_level, total_nesting_levels):
    """
    Determines next actions when a match is found for col_at_level. If
    we have reached the desired level of nesting, insert the comment. If we
    haven't reached the desired level of nesting yet, then begin searching
    the next level - as long as it's been properly defined.

    Args:
        col (str):
            The full name of the column/field definition being searched for,
            including parent column names in the prefix.
        comment (str):
            The comment to be added to the column/field being searched for
        col_loc (int):
            Index of col_defs that the defintion of col begins at
        col_defs (str):
            The fully formatted, translated-from-dataframe column definition
            section of a DDL statement.
        next_level_block_start (int):
            Index of col_defs that the first block of the next level starts at
        next_level_block_end (int):
            Index of col_defs that the first block block of the next level
            ends at
        current_nesting_level (int):
            The level of nesting currently being scanned through. If
            current_nesting_level == total_nesting_levels, we have reached
            the desired level of nesting to find the column/field
            to add a comment to
        total_nesting_levels (int):
            How many levels of nesting are required to scan to find the
            column definition being serached for. Determined by how many '.'
            characters are in 'col'
    """
    # We've reached the level of nesting that this comment must be inserted at
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
    # We have not yet reached the appropriate level of nesting
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
    """
    Given a starting index, this finds the index of the matching
    bracket of the highest-level bracket in the string (AKA the first
    one encountered)

    Args:
        col_defs (str):
            The fully formatted, translated-from-dataframe column definition
            section of a DDL statement.
        start_ind (int): The index to start searching from in col_defs
    Returns:
        int: index of col_defs that the next matching bracket is at
    Raises:
        ValueError: If no matching bracket is found
    """
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


def add_comments_to_avro_schema(avro_schema, col_comments):
    """
    When defining a Hive external table Avro with a provided string literal
    schema, comments will be detected from the 'doc' fields of the
    schema literal, rather than from the standard column definitions (in fact,
    you don't technically even need the standard column definitions when
    passing a literal avro schema, but they don't harm anything either).

    This function takes the Avro schema (as generated by pandavro) and injects
    the column comments in col_comments (as modified by
    'restructure_comments_for_avro') into it.
    """
    for field in avro_schema['fields']:
        field_name = field['name']
        if field_name in col_comments:
            field_metadata = col_comments[field_name]
            if 'comment' in field_metadata:
                atomic_comment = field_metadata['comment']
                field['doc'] = atomic_comment.replace('"', '\'')

            non_null_field_type = get_non_null_field_type(field['type'])

            # If the field's non-null type is a dictionary, that means that the
            # field is a complex type - either an array, a struct, or an
            # array of structs.
            #
            # If it's an array, it will contain the key 'items', which will
            # not point to a dict value. No further action needed
            #
            # If it's a struct, it will not contain the key 'items',
            # and the field should be passed to a recursive call to this fn
            #
            # If it's an array of structs, it will contain the key 'items',
            # which WILL point to a dict value. In this case, the struct within
            # the array does not need a comment, but the values within it
            # might, so unnest it from 'items' and pass it to a recursive call
            if isinstance(non_null_field_type, dict):
                # Unnest any number of nested arrays - they don't need internal
                # comments, since they don't hold named fields
                if 'items' in non_null_field_type:
                    while (isinstance(non_null_field_type, dict)
                           and 'items' in non_null_field_type):
                        non_null_field_type = get_non_null_field_type(
                            non_null_field_type['items'])

            # If non_null_field_type is still a dictionary after unnesting
            # any and all arrays, then there is a nested struct type that
            # must be iterated through
            if isinstance(non_null_field_type, dict):
                add_comments_to_avro_schema(
                    non_null_field_type,
                    field_metadata['subfields'])

    return avro_schema


def get_non_null_field_type(field_type):
    """
    If the field's type is a list, that means that one item will be
    'null' (indicating that the field is nullable, which most fields
    should be), and the other item will be the field's actual type
    """
    if isinstance(field_type, list):
        for i in range(len(field_type)):
            if field_type[i] != 'null':
                final_type_idx = i
        non_null_field_type = field_type[final_type_idx]
    else:
        non_null_field_type = field_type

    return non_null_field_type


def restructure_comments_for_avro(col_comments):
    """
    Restructures column comments as used in standard table column definitions
    to a format that is more easily injected into an Avro schema

    Original form of col_comments:
    {
        '<field>': '<comment>',
        '<field>.<subfield>': '<comment>',
        '<field>.<subfield>.<sub-subfield>': 'comment'
    }

    Restructured for easier injection into Avro schema:
    {
        '<field>': {
            'comment': '<comment>',
            'subfields': {
                '<subfield>': {
                    'comment': '<comment>',
                    'subfields': {
                        '<sub-subfield>': {
                            'comment': '<comment>'
                        }
                    }
                }
            }
        }
    }
    """
    subfields_key = 'subfields'

    avro_style_comments = {}
    col_comments = {col_name: col_comments[col_name]
                    for col_name in sorted(col_comments)}
    for col_name, col_comment in col_comments.items():
        field_names = col_name.split('.')
        current_fields = avro_style_comments
        while len(field_names) > 0:
            next_level_field = field_names.pop(0)

            if next_level_field not in current_fields:
                current_fields[next_level_field] = {subfields_key: {}}

            current_fields = current_fields[next_level_field]
            if len(field_names) > 0:
                current_fields = current_fields[subfields_key]

        current_fields['comment'] = col_comment

    return avro_style_comments
