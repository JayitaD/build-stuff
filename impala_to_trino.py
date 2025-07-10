#!/usr/bin/env python3
"""
Standalone function for replacing Impala SQL functions with Trino equivalents.

This module provides a single replace_func function that handles all Impala to Trino
function conversions with comprehensive error handling and validation.
"""

import re
from typing import Dict, List, Tuple, Any, Optional


def replace_func(sql_content: str, debug: bool = False) -> Tuple[str, List[str], Dict[str, int]]:
    """
    Replace Impala SQL functions with their Trino equivalents.
    
    Args:
        sql_content: The SQL content to convert
        debug: Enable debug output for conversion tracking
        
    Returns:
        Tuple of (converted_sql, warnings, conversion_stats)
    """
    warnings = []
    conversion_stats = {
        'total_conversions': 0,
        'functions_converted': {},
        'errors': 0
    }
    
    # Function mapping registry
    function_mappings = {
        'add_months': {
            'pattern': r'add_months\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': lambda match: f"date_add('month', {match.group(2)}, CAST({match.group(1)} AS TIMESTAMP))",
            'description': 'Convert add_months to date_add with month interval'
        },
        'adddate': {
            'pattern': r'adddate\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': lambda match: f"date_add('day', {match.group(2)}, {match.group(1)})",
            'description': 'Convert adddate to date_add with day interval'
        },
        'date_add': {
            'pattern': r'date_add\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': _replace_date_add,
            'description': 'Convert date_add to Trino syntax'
        },
        'date_part': {
            'pattern': r'DATE_PART\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': _replace_date_part,
            'description': 'Convert date_part to extract function'
        },
        'date_sub': {
            'pattern': r'DATE_SUB\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': _replace_date_sub,
            'description': 'Convert date_sub to date_add with negative values'
        },
        'datediff': {
            'pattern': r'datediff\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': lambda match: f"date_diff('day', {match.group(2)}, {match.group(1)})",
            'description': 'Convert datediff to date_diff'
        },
        'dayofmonth': {
            'pattern': r'dayofmonth\s*\(([^)]+)\)',
            'replacement': lambda match: f"DAY({match.group(1)})",
            'description': 'Convert dayofmonth to DAY function'
        },
        'dayofweek': {
            'pattern': r'dayofweek\s*\(([^)]+)\)',
            'replacement': lambda match: f"IF(day_of_week({match.group(1)})+1 > 7, 1, day_of_week({match.group(1)})+1)",
            'description': 'Convert dayofweek to day_of_week with adjustment'
        },
        'from_utc_timestamp': {
            'pattern': r'from_utc_timestamp\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': lambda match: f"at_timezone({match.group(1)}, {match.group(2)})",
            'description': 'Convert from_utc_timestamp to at_timezone'
        },
        'to_utc_timestamp': {
            'pattern': r'to_utc_timestamp\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': lambda match: f"at_timezone({match.group(1)}, 'UTC')",
            'description': 'Convert to_utc_timestamp to at_timezone with UTC'
        },
        'group_concat': {
            'pattern': r'group_concat\s*\(([^)]+)\)',
            'replacement': lambda match: f"array_join(array_agg({match.group(1)}), ',')",
            'description': 'Convert group_concat to array_join with array_agg'
        },
        'ifnull': {
            'pattern': r'ifnull\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': lambda match: f"COALESCE({match.group(1)}, {match.group(2)})",
            'description': 'Convert ifnull to COALESCE'
        },
        'instr': {
            'pattern': r'instr\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': lambda match: f"strpos({match.group(1)}, {match.group(2)})",
            'description': 'Convert instr to strpos'
        },
        'int_months_between': {
            'pattern': r'int_months_between\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': lambda match: f"date_diff('month', {match.group(2)}, {match.group(1)})",
            'description': 'Convert int_months_between to date_diff with month interval'
        },
        'left': {
            'pattern': r'left\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': lambda match: f"substring({match.group(1)}, 1, {match.group(2)})",
            'description': 'Convert left to substring'
        },
        'strleft': {
            'pattern': r'strleft\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': lambda match: f"substring({match.group(1)}, 1, {match.group(2)})",
            'description': 'Convert strleft to substring'
        },
        'strright': {
            'pattern': r'strright\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': lambda match: f"substring({match.group(1)}, -{match.group(2)})",
            'description': 'Convert strright to substring with negative start'
        },
        'right': {
            'pattern': r'right\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': lambda match: f"substring({match.group(1)}, -{match.group(2)})",
            'description': 'Convert right to substring with negative start'
        },
        'to_date': {
            'pattern': r'to_date\s*\(([^)]+)\)',
            'replacement': lambda match: f"date({match.group(1)})",
            'description': 'Convert to_date to date function'
        },
        'trunc': {
            'pattern': r'trunc\s*\(([^,]+),\s*([^)]+)\)',
            'replacement': lambda match: f"date_trunc({match.group(2)}, {match.group(1)})",
            'description': 'Convert trunc to date_trunc'
        },
        'unix_timestamp': {
            'pattern': r'unix_timestamp\s*\(([^)]*)\)',
            'replacement': lambda match: f"to_unixtime({match.group(1) if match.group(1) else 'now()'})",
            'description': 'Convert unix_timestamp to to_unixtime'
        },
        'weekofyear': {
            'pattern': r'weekofyear\s*\(([^)]+)\)',
            'replacement': lambda match: f"week({match.group(1)})",
            'description': 'Convert weekofyear to week function'
        },
        'current_timestamp': {
            'pattern': r'current_timestamp\s*\(\)',
            'replacement': 'CAST(NOW() AS TIMESTAMP)',
            'description': 'Convert current_timestamp to NOW with CAST'
        }
    }
    
    try:
        if debug:
            print(f"Starting SQL conversion. Input length: {len(sql_content)} characters")
        
        # Apply function conversions
        for func_name, mapping in function_mappings.items():
            try:
                if debug:
                    print(f"Applying {func_name} conversion...")
                
                # Count matches before replacement
                matches_before = len(re.findall(mapping['pattern'], sql_content, flags=re.IGNORECASE))
                
                # Apply replacement
                sql_content = re.sub(
                    mapping['pattern'], 
                    mapping['replacement'], 
                    sql_content, 
                    flags=re.IGNORECASE
                )
                
                # Count matches after replacement
                matches_after = len(re.findall(mapping['pattern'], sql_content, flags=re.IGNORECASE))
                
                # Track conversions
                conversions_made = matches_before - matches_after
                if conversions_made > 0:
                    conversion_stats['functions_converted'][func_name] = conversions_made
                    conversion_stats['total_conversions'] += conversions_made
                
                if debug:
                    print(f"  {func_name}: {conversions_made} conversions applied")
                    
            except Exception as e:
                warning_msg = f"Warning: Failed to apply {func_name} conversion: {str(e)}"
                warnings.append(warning_msg)
                conversion_stats['errors'] += 1
                if debug:
                    print(f"  {func_name}: ERROR - {str(e)}")
        
        # Apply additional transformations
        sql_content = _apply_additional_transformations(sql_content, debug)
        
        # Validate the converted SQL
        validation_warnings = _validate_converted_sql(sql_content, debug)
        warnings.extend(validation_warnings)
        
        if debug:
            print(f"Conversion completed. Total conversions: {conversion_stats['total_conversions']}")
            print(f"Warnings: {len(warnings)}")
        
        return sql_content, warnings, conversion_stats
        
    except Exception as e:
        error_msg = f"Failed to convert SQL: {str(e)}"
        warnings.append(error_msg)
        conversion_stats['errors'] += 1
        if debug:
            print(f"FATAL ERROR: {error_msg}")
        raise


def _replace_date_add(match) -> str:
    """Handle date_add conversion with interval support."""
    date_part = match.group(1)
    value = match.group(2).split()
    
    if len(value) == 1:
        return f"date_add('day', {match.group(2)}, CAST({date_part} AS TIMESTAMP))"
    else:
        value_part = value[1]
        unit = value[2]
        
        if unit.endswith('s'):
            unit = unit[:-1]
        return f"date_add('{unit}', {value_part}, {date_part})"


def _replace_date_part(match) -> str:
    """Handle date_part conversion with proper part mapping."""
    part = match.group(1).strip().lower()
    date = match.group(2).strip()
    
    part_mapping = {
        "'year'": "year",
        "'month'": "month",
        "'day'": "day",
        "'hour'": "hour",
        "'minute'": "minute",
        "'second'": "second"
    }
    
    part = part_mapping.get(part, part).replace("'", "")
    return f"extract({part} FROM {date})"


def _replace_date_sub(match) -> str:
    """Handle date_sub conversion with interval support."""
    date_part = match.group(1)
    value = match.group(2).split()
    
    if len(value) == 1:
        return f"date_add('day', -{match.group(2)}, {date_part})"
    else:
        value_part = value[1]
        unit = value[2]
        
        if unit.endswith('s'):
            unit = unit[:-1]
        return f"date_add('{unit}', -{value_part}, {date_part})"


def _apply_additional_transformations(sql_content: str, debug: bool = False) -> str:
    """Apply additional SQL transformations beyond function conversions."""
    if debug:
        print("Applying additional transformations...")
    
    # Replace data types
    type_mappings = {
        r'\bstring\b': 'VARCHAR',
        r'\bfloat\b': 'DOUBLE',
        r'\bint\b': 'INTEGER',
        r'\bbigint\b': 'BIGINT'
    }
    
    for old_type, new_type in type_mappings.items():
        sql_content = re.sub(old_type, new_type, sql_content, flags=re.IGNORECASE)
    
    # Replace backticks with double quotes
    sql_content = sql_content.replace('`', '"')
    
    # Replace interval syntax
    sql_content = re.sub(r'\binterval (\d+) ', r"INTERVAL '\1' ", sql_content, flags=re.IGNORECASE)
    
    # Replace backslashes in regex patterns
    sql_content = re.sub(r'\\+', r'\\\\', sql_content)
    
    # Replace specific function calls
    function_replacements = {
        r"default\.gb_format_datetime": "gb_format_datetime",
        r"default\.gb_json_parser": "gb_json_parser",
        r"default\.gb_to_est": "gb_to_est",
        r"default\.gb_completed_months": "gb_completed_months",
        r"\btempdb\b": "caastle_insights",
        r"'EDT'": "'EST5EDT'"
    }
    
    for old_func, new_func in function_replacements.items():
        sql_content = re.sub(old_func, new_func, sql_content, flags=re.IGNORECASE)
    
    if debug:
        print("Additional transformations completed")
    
    return sql_content


def _validate_converted_sql(sql_content: str, debug: bool = False) -> List[str]:
    """Validate the converted SQL for common issues."""
    warnings = []
    
    if debug:
        print("Validating converted SQL...")
    
    # Check for common issues
    if 'add_months(' in sql_content.lower():
        warnings.append("Potential unconverted add_months function detected")
    
    if 'datediff(' in sql_content.lower():
        warnings.append("Potential unconverted datediff function detected")
    
    if 'current_timestamp()' in sql_content.lower():
        warnings.append("Potential unconverted current_timestamp function detected")
    
    # Check for balanced parentheses
    if sql_content.count('(') != sql_content.count(')'):
        warnings.append("Unbalanced parentheses detected")
    
    # Check for common Trino syntax issues
    if 'group_concat(' in sql_content.lower():
        warnings.append("group_concat function detected - verify conversion to array_join")
    
    if debug:
        print(f"Validation completed. Warnings: {len(warnings)}")
    
    return warnings


# Example usage and testing
if __name__ == "__main__":
    # Test the function
    test_sql = """
    SELECT 
        add_months(created_date, 3) as future_date,
        datediff(end_date, start_date) as days_diff,
        group_concat(name) as names_list,
        ifnull(revenue, 0) as safe_revenue,
        dayofweek(created_date) as day_num
    FROM transactions
    WHERE date_part('year', created_date) = 2023
      AND current_timestamp() > '2023-01-01'
    ORDER BY created_date;
    """
    
    print("Original SQL:")
    print(test_sql)
    print("\n" + "="*50)
    
    # Convert with debug output
    converted_sql, warnings, stats = replace_func(test_sql, debug=True)
    
    print("\nConverted SQL:")
    print(converted_sql)
    
    if warnings:
        print(f"\nWarnings ({len(warnings)}):")
        for warning in warnings:
            print(f"  - {warning}")
    
    print(f"\nConversion Statistics:")
    print(f"  Total conversions: {stats['total_conversions']}")
    print(f"  Functions converted: {stats['functions_converted']}")
    print(f"  Errors: {stats['errors']}") 