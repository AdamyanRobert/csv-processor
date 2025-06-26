import csv
from typing import List, Dict, Union, Optional, Callable, Any


class CSVProcessor:
    OPERATORS = {
        ">": lambda x, y: x > y,
        "<": lambda x, y: x < y,
        "=": lambda x, y: x == y
    }

    AGGREGATORS = {
        'avg': lambda values: sum(values) / len(values),
        'min': min,
        'max': max,
    }

    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.data = CSVProcessor._load_csv(filename)
        self.columns: List[str] = list(self.data[0].keys()) if self.data else []
        self.cache_type: Dict[str, Optional[type]] = {i: None for i in self.columns}

    @staticmethod
    def _load_csv(filename: str) -> List[Dict[str, str]]:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            return list(reader)

    def _find_column_type(self, column: str) -> type:
        if self.cache_type[column] is not None:
            return self.cache_type[column]

        has_float = False
        for row in self.data:
            value = row[column].strip()
            try:
                float(value)
                if '.' in value:
                    has_float = True
            except ValueError:
                self.cache_type[column] = str
                return str

        self.cache_type[column] = float if has_float else int
        return self.cache_type[column]

    def _validate_column(self, column: str) -> None:
        if column not in self.columns:
            raise ValueError(f"'{column}' not found in data columns")

    def filter_data(self, column: str, operator: str, value: str) -> List[Dict[str, str]]:
        self._validate_column(column)

        result = []
        column_type = self._find_column_type(column)
        typed_value = column_type(value)

        for row in self.data:
            item = column_type(row[column])
            if CSVProcessor.OPERATORS[operator](item, typed_value):
                result.append(row)

        return result

    def aggregate_data(self, column: str, function: str) -> Union[int, float]:
        self._validate_column(column)

        if function not in self.AGGREGATORS:
            raise ValueError(f"Unsupported function: {function}")

        column_type = self._find_column_type(column)

        if column_type == str:
            raise ValueError(f"Aggregation is not supported for text columns")

        values = [column_type(row[column]) for row in self.data]
        return self.AGGREGATORS[function](values)

    def sort_data(self, column: str, direction: str = 'asc') -> List[Dict[str, str]]:
        """Сортирует данные по указанной колонке"""
        self._validate_column(column)

        if direction not in ['asc', 'desc']:
            raise ValueError(f"Sort direction must be 'asc' or 'desc'")

        column_type = self._find_column_type(column)
        reverse = direction == 'desc'

        return sorted(self.data, key=lambda row: column_type(row[column]), reverse=reverse)
