import pytest
import tempfile
import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from processor import CSVProcessor
from main import parse_conditions, main


class TestCSVProcessor:

    @pytest.fixture
    def sample_csv_file(self):
        content = """name,brand,price,rating
iphone 15 pro,apple,999,4.9
galaxy s23 ultra,samsung,1199,4.8
redmi note 12,xiaomi,199,4.6
poco x5 pro,xiaomi,299,4.4"""

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write(content)
        yield f.name
        os.unlink(f.name)

    def test_load_csv_file(self, sample_csv_file):
        processor = CSVProcessor(sample_csv_file)

        assert len(processor.data) == 4
        assert processor.columns == ['name', 'brand', 'price', 'rating']
        assert processor.data[0]['name'] == 'iphone 15 pro'

    def test_load_csv_static_method(self, sample_csv_file):
        data = CSVProcessor._load_csv(sample_csv_file)

        assert len(data) == 4
        assert data[0]['name'] == 'iphone 15 pro'
        assert data[0]['brand'] == 'apple'

    def test_load_nonexistent_file(self):
        with pytest.raises(FileNotFoundError):
            CSVProcessor('nonexistent_file.csv')

    def test_load_nonexistent_file_static(self):
        with pytest.raises(FileNotFoundError):
            CSVProcessor._load_csv('nonexistent_file.csv')

    def test_column_type_detection(self, sample_csv_file):
        processor = CSVProcessor(sample_csv_file)

        assert processor._find_column_type('name') == str
        assert processor._find_column_type('brand') == str
        assert processor._find_column_type('price') == int
        assert processor._find_column_type('rating') == float

    def test_filter_data_equals(self, sample_csv_file):
        processor = CSVProcessor(sample_csv_file)

        result = processor.filter_data('brand', '=', 'xiaomi')
        assert len(result) == 2
        assert all(row['brand'] == 'xiaomi' for row in result)

    def test_filter_data_greater_than(self, sample_csv_file):
        processor = CSVProcessor(sample_csv_file)

        result = processor.filter_data('price', '>', '500')
        assert len(result) == 2  # iphone и galaxy
        assert all(int(row['price']) > 500 for row in result)

    def test_filter_data_less_than(self, sample_csv_file):
        processor = CSVProcessor(sample_csv_file)

        result = processor.filter_data('rating', '<', '4.7')
        assert len(result) == 2  # redmi и poco
        assert all(float(row['rating']) < 4.7 for row in result)

    def test_filter_invalid_column(self, sample_csv_file):
        processor = CSVProcessor(sample_csv_file)

        with pytest.raises(ValueError, match="not found in data columns"):
            processor.filter_data('invalid_column', '=', 'value')

    def test_aggregate_avg(self, sample_csv_file):
        processor = CSVProcessor(sample_csv_file)

        result = processor.aggregate_data('price', 'avg')
        expected = (999 + 1199 + 199 + 299) / 4
        assert result == expected

    def test_aggregate_min_max(self, sample_csv_file):
        processor = CSVProcessor(sample_csv_file)

        assert processor.aggregate_data('price', 'min') == 199
        assert processor.aggregate_data('price', 'max') == 1199

    def test_aggregate_text_column_error(self, sample_csv_file):
        processor = CSVProcessor(sample_csv_file)

        with pytest.raises(ValueError, match="Aggregation is not supported for text columns"):
            processor.aggregate_data('name', 'avg')

    def test_aggregate_invalid_function(self, sample_csv_file):
        processor = CSVProcessor(sample_csv_file)

        with pytest.raises(ValueError, match="Unsupported function"):
            processor.aggregate_data('price', 'median')

    def test_sort_data_asc(self, sample_csv_file):
        processor = CSVProcessor(sample_csv_file)

        result = processor.sort_data('price', 'asc')
        prices = [int(row['price']) for row in result]
        assert prices == [199, 299, 999, 1199]

    def test_sort_data_desc(self, sample_csv_file):
        processor = CSVProcessor(sample_csv_file)

        result = processor.sort_data('price', 'desc')
        prices = [int(row['price']) for row in result]
        assert prices == [1199, 999, 299, 199]


class TestMainModule:
    @pytest.mark.parametrize(
        "condition, res_tup",
        [
            ('price=100', ("price", "=", "100")),
            ('rating>4.5', ("rating", ">", "4.5")),
            ('price<500', ("price", "<", "500")),
        ]
    )
    def test_parse_conditions(self, condition, res_tup):
        assert parse_conditions(condition) == res_tup

    def test_parse_conditions_with_spaces(self):
        column, operator, value = parse_conditions(' price = 100 ')
        assert column == 'price'
        assert operator == '='
        assert value == '100'

    def test_parse_conditions_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid condition format"):
            parse_conditions('invalid_condition')

    @patch('sys.argv', ['main.py', 'test.csv'])
    def test_main_no_parameters(self):
        with patch('argparse.ArgumentParser.print_help') as mock_help:
            main()
            mock_help.assert_called_once()

    @patch('sys.argv', ['main.py', 'nonexistent.csv', '--where', 'price>100'])
    @patch('builtins.print')
    def test_main_file_not_found(self, mock_print):
        main()
        mock_print.assert_called_with("Error: File 'nonexistent.csv' not found")


class TestIntegration:

    @pytest.fixture
    def test_csv_file(self):
        content = """product,category,price,stock
laptop,electronics,1500,10
mouse,electronics,25,50
desk,furniture,300,5
chair,furniture,150,8"""

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write(content)
        yield f.name
        os.unlink(f.name)

    def test_filter_integration(self, test_csv_file):

        column, operator, value = parse_conditions('price>100')

        processor = CSVProcessor(test_csv_file)
        result = processor.filter_data(column, operator, value)

        assert len(result) == 3
        for row in result:
            assert int(row['price']) > 100

    def test_aggregate_integration(self, test_csv_file):

        column, _, function = parse_conditions('price=avg')

        processor = CSVProcessor(test_csv_file)
        result = processor.aggregate_data(column, function)

        expected = (1500 + 25 + 300 + 150) / 4
        assert result == expected

    def test_combined_filter_and_aggregate(self, test_csv_file):

        processor = CSVProcessor(test_csv_file)

        filtered_data = processor.filter_data('category', '=', 'electronics')
        assert len(filtered_data) == 2

        processor.data = filtered_data

        avg_price = processor.aggregate_data('price', 'avg')
        expected = (1500 + 25) / 2
        assert avg_price == expected
