import xlrd
import collections
import unidecode
import csv
from lib.etl.utils import get_files_name_list


class XlsxToCSV:
    XLSX_DIR = "/home/joaoh/joaoproject/challenge/raw"
    CSV_DIR = "/home/joaoh/joaoproject/challenge/csv"

    def __init__(
        self,
    ):
        pass

    def execute(
        self,
    ):
        xlsx_files_name_list = get_files_name_list(files_dir=self.XLSX_DIR)
        self._process_files(xlsx_files_name_list)

    def _process_files(self, xlsx_files_name_list):
        list(
            map(
                lambda workbook_name: self._convert_file(workbook_name),
                xlsx_files_name_list,
            )
        )

    def _convert_file(self, workbook_name):
        with xlrd.open_workbook(workbook_name) as workbook:
            sheets_in_workbook = [sheet.name for sheet in workbook.sheets()]

            for sheet_name in sheets_in_workbook:
                self._to_csv(
                    workbook_name=workbook_name.split("/")[-1].split(".")[0],
                    sheet_name=sheet_name,
                    sheet_file=workbook.sheet_by_name(sheet_name),
                )

    def _to_csv(self, workbook_name, sheet_name, sheet_file):
        with open(f"{self.CSV_DIR}/{workbook_name}_{sheet_name}.csv", "w") as csv_file:
            csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
            for index, row_num in enumerate(range(sheet_file.nrows)):
                if index == 0:
                    header, indexes = self._ajusts_csv_columns_and_indexes(
                        sheet_file.row_values(row_num)
                    )
                    csv_writer.writerow(header)
                    continue

                csv_writer.writerow(
                    self._fill_with_right_indexes(
                        row_values=sheet_file.row_values(row_num), row_indexes=indexes
                    )
                )

    def _ajusts_csv_columns_and_indexes(self, header):
        header = self._format_headers(header)
        origical_column_indexes = [index for index, value in enumerate(header) if value]
        header = [str(value).replace('"', "") for value in header if value]
        duplicity_header = [
            item
            for item, count in collections.Counter(header).items()
            if count > 1 and item != '""'
        ]
        while duplicity_header:
            header = self._change_duplicity_header(header, duplicity_header)
            duplicity_header = [
                item
                for item, count in collections.Counter(header).items()
                if count > 1 and item != '""'
            ]
        return header, origical_column_indexes

    @staticmethod
    def _change_duplicity_header(header, duplicity_header):
        for index, header_value in enumerate(header):
            for duplicity in duplicity_header:
                if header_value == duplicity:
                    header[index] = f"{duplicity}_"
                    return header

    @staticmethod
    def _format_headers(headers):
        return list(
            map(
                lambda header: unidecode.unidecode(str(header))
                .lower()
                .replace(" ", "_"),
                headers,
            )
        )

    @staticmethod
    def _fill_with_right_indexes(row_values, row_indexes):
        return [value for index, value in enumerate(row_values) if index in row_indexes]
