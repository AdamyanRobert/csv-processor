import argparse
import re
from typing import Tuple
from tabulate import tabulate

from processor import CSVProcessor


def parse_conditions(condition: str) -> Tuple[str, str, str]:
    match = re.match(r'^([^<>=]+?)([<>=])(.+)$', condition)
    if not match:
        raise ValueError(f"Invalid condition format: {condition}")

    column = match.group(1).strip()
    operator = match.group(2)
    value = match.group(3).strip()

    return column, operator, value


def main() -> None:
    parser = argparse.ArgumentParser(description='CSV file processor')
    parser.add_argument('file', help='Path to the CSV file')
    parser.add_argument('--where', help='Filter condition, e.g., price>100')
    parser.add_argument('--aggregate', help='Aggregation, e.g., price=avg')
    parser.add_argument('--order-by', help='Sorting, e.g., price=desc or name=asc')

    args = parser.parse_args()

    if not any([args.where, args.aggregate, args.order_by]):
        parser.print_help()
        return

    try:
        processor = CSVProcessor(args.file)
        result_data = processor.data

        if args.where:
            column, operator, value = parse_conditions(args.where)
            result_data = processor.filter_data(column, operator, value)
            processor.data = result_data

        if args.aggregate:
            column, _, function = parse_conditions(args.aggregate)
            aggregated_result = processor.aggregate_data(column, function)
            return print(tabulate([{f"{column}_{function}": aggregated_result}], headers="keys", tablefmt="grid"))

        if args.order_by:
            column, _, direction = parse_conditions(args.order_by)
            if direction not in ['asc', 'desc']:
                raise ValueError(f"Sort direction must be 'asc' or 'desc', got: {direction}")
            result_data = processor.sort_data(column, direction)

        if result_data:
            print(tabulate(result_data, headers="keys", tablefmt="grid"))
        else:
            print("No data to display")

    except FileNotFoundError:
        print(f"Error: File '{args.file}' not found")
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == '__main__':
    main()
