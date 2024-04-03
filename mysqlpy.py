# define the dataset summary class
import pyodbc 
import pandas as pd
import getpass
from datetime import datetime, timezone
from enum import IntEnum
from datetime import date, datetime, timedelta, timezone
import sys
import math
import xlsxwriter
import numpy as np

class DatasetSummary:
    def __init__(self, connection, params):
        self.params = params
        self.connection = connection
        self.system_name = params["SYSTEM_NAME"]
        self.database = params["DATABASE_NAME"]
        self.schema = params["SCHEMA_NAME"]
        self.table = params["TABLE_NAME"]
        self.provided_by = params["PROVIDED_BY"]
        self.notes = params["NOTES"]
        self.content = params["CONTENT"]
        self.meta_data_present = params["META_DATA_PRESENT"]
        self.drive_path = params["FILE_PATH"]
        self.who = params["WHO"]
        self.when = params["WHEN"]
        self.where = params["WHERE"]
        self.how = params["HOW"]
        self.fltr = params["FILTER"]

    def getTotalRecordCount(self):
        record_count_sql = f"select count(*) from {self.schema}.{self.table}"
        # print(record_count_sql)
        record_count = self.connection.cursor().execute(record_count_sql).fetchall()
        return record_count[0][0]

    def getTotalAttributeCount(self):
        attribute_count_sql = f"SELECT count(COLUMN_NAME) \
                                FROM INFORMATION_SCHEMA.COLUMNS \
                                WHERE TABLE_NAME = '{self.table}';"
        attribute_count = (
            self.connection.cursor().execute(attribute_count_sql).fetchall()
        )
        return attribute_count[0][0]

    def getCurrentDateTime(self):
        current_time_sql = "SELECT GETDATE();"
        current_time = self.connection.cursor().execute(current_time_sql).fetchall()
        return current_time[0][0]

    def getNumberOfEmptyColumn(self, thresold=0):
        blank_column_count = 0
        blank_columns = []
        column_list_sql = f"SELECT COLUMN_NAME \
                          FROM INFORMATION_SCHEMA.COLUMNS \
                          WHERE TABLE_NAME = '{self.table}'; "
        column_list = self.connection.cursor().execute(column_list_sql).fetchall()
        print(column_list)
        flat_column_list = [item for sublist in column_list for item in sublist]
        total_record_count = self.getTotalRecordCount()
        for column in flat_column_list:
            print(column)
            not_null_count_sql = f"SELECT COUNT(*) \
                                 FROM {self.schema}.{self.table} \
                                 WHERE {column} IS NOT NULL \
                                 AND LEN(LTRIM(RTRIM({column}))) > 0 ;"
            print(not_null_count_sql)
            not_null_count = (
                self.connection.cursor().execute(not_null_count_sql).fetchall()[0][0]
            )
            percentage_of_not_null_value = (not_null_count * 100) / total_record_count
            if percentage_of_not_null_value <= thresold:
                blank_column_count = blank_column_count + 1
                blank_columns.append(column)
        print("here are blank columns: ", blank_columns)
        return blank_column_count, blank_columns

    def getSummary(self):
        datasummary = []
        record_count = self.getTotalRecordCount()
        attribute_count = self.getTotalAttributeCount()
        current_time = self.getCurrentDateTime()

        number_of_empty_column, empty_columns = self.getNumberOfEmptyColumn()

        datasummary.append(
            {
                "DATA SET NAME": self.table,
                "SOURCE SYSTEM NAME (Schema)": self.system_name,
                "AS OF SOURCE DATE": current_time,
                "PROVIDED BY": self.provided_by,
                "CONTENTS": self.content,
                "META DATA PRESENT": self.meta_data_present,
                "RECORD COUNT": record_count,
                "NUMBER OF ATTRIBUTES": attribute_count,
                "NUMBER OF EMPTY COLUMN ": number_of_empty_column,
                "NOTES": self.notes,
                "FILTER": self.fltr,
                "WHO": self.who,
                "WHEN": current_time,
                "WHERE": self.where,
                "HOW": self.how,
            }
        )

        data_frame = pd.DataFrame(datasummary)
        data_frame_transpose = data_frame.T.reset_index().rename(
            columns={"index": "ATTRIBUTE", 0: "VALUE"}
        )
        final_drive_path = (
            f"{self.drive_path}\\{self.database}_{self.schema}_{self.table}_SUMMARY.csv"
        )
        data_frame_transpose.to_csv(final_drive_path, index=False)
        print(f"Report has been written to the file {final_drive_path}")


class AttributeSummary:
    def __init__(self, connection, params):
        self.connection = connection
        self.database = params["DATABASE_NAME"]
        self.schema = params["SCHEMA_NAME"]
        self.table = params["TABLE_NAME"]
        self.file_path = params["FILE_PATH"]
        self.when = params["WHEN"]
        self.fltr = params["FILTER"]

    def getDatatype(self, column):
        datatype_sql = f"SELECT DATA_TYPE FROM \
                     INFORMATION_SCHEMA.COLUMNS \
                     WHERE TABLE_NAME = '{self.table}' AND TABLE_SCHEMA = '{self.schema}' AND COLUMN_NAME = '{column}'"
        data_type_of_the_column = (
            self.connection.cursor().execute(datatype_sql).fetchall()
        )
        return data_type_of_the_column[0][0]

    def getIsNullable(self, column):
        is_nullable_sql = f"SELECT IS_NULLABLE FROM \
                           INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_NAME = '{self.table}' AND TABLE_SCHEMA = '{self.schema}' AND COLUMN_NAME = '{column}'"
        is_nullable = self.connection.cursor().execute(is_nullable_sql).fetchall()
        return is_nullable[0][0]

    def getTotalRecordCount(self):
        record_count_sql = f"SELECT COUNT(*) FROM \
                            {self.schema}.{self.table}"
        record_count = self.connection.cursor().execute(record_count_sql).fetchall()
        return record_count[0][0]

    def getColumPercentPopulated(self, column, total_count):
        if total_count == 0:
            return 0, 0  # Return 0 count and 0 percent if total_count is 0

        not_null_count_sql = f"SELECT COUNT(*) FROM \
                             {self.schema}.{self.table} \
                             WHERE {column} IS NOT NULL AND LEN(LTRIM(RTRIM({column}))) > 0 "
        not_null_count = (
            self.connection.cursor().execute(not_null_count_sql).fetchall()[0][0]
        )
        percent_not_null_count = round(((not_null_count * 100.0) / total_count), 4)
        return not_null_count, percent_not_null_count

    def getDistinctValuePercentRespectToTotalCount(self, column, total_count):
        if total_count == 0:
            return 0, 0  # Return 0 count and 0 percent if total_count is 0

        distinct_value_sql = f"SELECT COUNT(DISTINCT {column}) FROM \
                              {self.schema}.{self.table} "
        distinct_count = self.connection.cursor().execute(distinct_value_sql).fetchall()

        percentage_of_distinct_value_count = round(
            ((distinct_count[0][0] * 100.0) / total_count), 4
        )
        return distinct_count[0][0], percentage_of_distinct_value_count

    def getDistinctValuePercentRespectToPopulated(
        self, distinct_count, populated_count
    ):
        if populated_count == 0:
            return 0
        percentage_of_distinct_value_count = round(
            ((distinct_count * 100) / populated_count), 4
        )
        return percentage_of_distinct_value_count

    def getMaxMinAvgStdVarForNum(self, column, populated_record_count):
        if populated_record_count > 0:
            max_min_Avg_std_var_sql = f"SELECT MIN({column}) AS min, MAX({column}) AS max, \
                                        AVG(CAST({column} AS FLOAT)) AS average, STDEV(CAST({column} AS FLOAT)) AS stdev,\
                                        VARP(CAST({column} AS FLOAT)) AS var FROM {self.schema}.{self.table}"
            result = (
                self.connection.cursor().execute(max_min_Avg_std_var_sql).fetchall()
            )
            if round(result[0][0], 4) == round(result[0][1], 4):
                return (
                    round(result[0][0], 4),
                    round(result[0][1], 4),
                    round(result[0][2], 4),
                    0,
                    0,
                )
            else:
                return (
                    round(result[0][0], 4),
                    round(result[0][1], 4),
                    round(result[0][2], 4),
                    round(result[0][3], 4),
                    round(result[0][4], 4),
                )
        else:
            return "N/A", "N/A", "N/A", "N/A", "N/A"

    def getMaxMinAvgStdVarForNotNum(self, column, populated_record_count):
        if populated_record_count > 0:
            max_min_sql = f"SELECT MIN({column}) AS min, MAX({column}) AS max \
                            FROM {self.schema}.{self.table}"
            try:
                result = self.connection.cursor().execute(max_min_sql).fetchall()
                return result[0][0], result[0][1]
            except:
                return "error", "error"
        else:
            return "N/A", "N/A"

    def getQuereyResult(self, columns):
        attribute_summary = []
        attribute_detail = []
        numeric_data_types = [
            "NUMBER",
            "number",
            "FLOAT",
            "float",
            "INT",
            "int",
            "BIGINT",
            "bigint",
            "SMALLINT",
            "smallint",
            "TINYINT",
            "tinyint",
            "DECIMAL",
            "decimal",
            "NUMERIC",
            "numeric",
            "FLOAT",
            "float",
            "REAL",
            "real",
            "MONEY",
            "money",
            "SMALLMONEY",
            "smallmoney",
        ]

        for column in columns:
            print(f"{column} under process")
            data_type = self.getDatatype(column)
            is_nullable = self.getIsNullable(column)
            total_record_count = self.getTotalRecordCount()
            (
                populated_record_count,
                percent_populated_record_count,
            ) = self.getColumPercentPopulated(column, total_record_count)
            (
                distinct_count,
                percent_distinct_value_count_respect_to_total_record,
            ) = self.getDistinctValuePercentRespectToTotalCount(
                column, total_record_count
            )
            percent_distinct_value_count_respect_to_total_populated = (
                self.getDistinctValuePercentRespectToPopulated(
                    distinct_count, populated_record_count
                )
            )
            # Get statistical measures for numeric and non-numeric columns
            if data_type in numeric_data_types:
                min, max, avg, stdev, var = self.getMaxMinAvgStdVarForNum(
                    column, populated_record_count
                )
            else:
                min, max = self.getMaxMinAvgStdVarForNotNum(
                    column, populated_record_count
                )
                avg, stdev, var = "N/A", "N/A", "N/A"

            attribute_summary_json = {
                "COLUMN NAME": column,
                "DATA TYPE": data_type,
                "IS NULLABLE": is_nullable,
                "TOTAL RECORD COUNT": total_record_count,
                "POPULATED RECORD COUNT": populated_record_count,
                "% POPULATED RECORD COUNT": f"{percent_populated_record_count}",
                "DISTINCT RECORD COUNT": distinct_count,
                "% DISTINCT RECORD COUNT RESPECT TO TOTAL": f"{percent_distinct_value_count_respect_to_total_record}",
                "% DISTINCT RECORD COUNT RESPECT TO POPULATED": f"{percent_distinct_value_count_respect_to_total_populated}",
            }
            attribute_detail_json = {
                "COLUMN NAME": column,
                "DATA TYPE": data_type,
                "IS NULLABLE": is_nullable,
                "TOTAL RECORD COUNT": total_record_count,
                "POPULATED RECORD COUNT": populated_record_count,
                "% POPULATED RECORD COUNT": f"{percent_populated_record_count}",
                "DISTINCT RECORD COUNT": distinct_count,
                "% DISTINCT RECORD COUNT RESPECT TO TOTAL": f"{percent_distinct_value_count_respect_to_total_record}",
                "% DISTINCT RECORD COUNT RESPECT TO POPULATED": f"{percent_distinct_value_count_respect_to_total_populated}",
                "MIN": f"{min}",
                "MAX": f"{max}",
                "AVERAGE": f"{avg}",
                "STANDARD DEVIATION": f"{stdev}",
                "VARIANCE": f"{var}",
            }

            attribute_summary.append(attribute_summary_json)
            attribute_detail.append(attribute_detail_json)
        return attribute_summary, attribute_detail

    def saveResultToCSV(self):
        column_list_sql = f"SELECT COLUMN_NAME \
                          FROM INFORMATION_SCHEMA.COLUMNS \
                          WHERE TABLE_NAME = '{self.table}'; "
        column_list = self.connection.cursor().execute(column_list_sql).fetchall()
        flat_column_list = [item for sublist in column_list for item in sublist]
        atrribute_summary, attribute_detail = self.getQuereyResult(flat_column_list)
        data_frame = pd.DataFrame(atrribute_summary)
        data_frame2 = pd.DataFrame(attribute_detail)
        final_path_summary = f"{self.file_path}\\{self.database}_{self.schema}_{self.table}_ATTRIBUTE_SUMMARY.csv"
        final_path_detail = f"{self.file_path}\\{self.database}_{self.schema}_{self.table}_ATTRIBUTE_DETAIL.csv"
        data_frame.to_csv(final_path_summary, index=False)
        data_frame2.to_csv(final_path_detail, index=False)
        print(f"Attribute summary report has been written to the {final_path_summary}")
        print(f"Attribute Detail report has been written to the {final_path_detail}")


class HistogramGeneration:
    def __init__(self, connection, params):
        self.connection = connection
        self.type = params["PERIOD_TYPE"]
        self.start_date = params["START_DATE"]
        self.count_of_interval = params["NUMBER_OF_THE_PERIOD"]
        current_date = datetime.today().strftime("%Y-%m-%d")
        if int(self.type) == 100:
            if self.count_of_interval is None:
                self.count_of_interval = self.monthCount(current_date)
        elif int(self.type) == 200:
            if self.count_of_interval is None:
                self.count_of_interval = self.quarterCount(current_date)
        elif int(self.type) == 300:
            if self.count_of_interval is None:
                self.count_of_interval = self.biYearCount(current_date)
        elif int(self.type) == 400:
            if self.count_of_interval is None:
                self.count_of_interval = self.yearCount(current_date)

        self.database = params["DATABASE_NAME"]
        self.schema = params["SCHEMA_NAME"]
        self.table = params["TABLE_NAME"]
        self.column_name = params["COLUMN_NAME"]
        self.file_path = params["FILE_PATH"]
        self.when = params["WHEN"]
        self.fltr = params["FILTER"]

    def date_validation(self):
        current_date = datetime.today().strftime("%Y-%m-%d")
        current_date_in_date = datetime.strptime(current_date, "%Y-%m-%d")
        start_date_in_date = datetime.strptime(self.start_date, "%Y-%m-%d")
        year_diff = current_date_in_date.year - start_date_in_date.year
        month_diff = current_date_in_date.month - start_date_in_date.month
        if year_diff == 0:
            month_diff = current_date_in_date.month - start_date_in_date.month
            if int(self.type) == 100:
                if month_diff == 0:
                    sys.exit(
                        f"[VALUE ERROR] for {str(self.type).split('.')[1]} start date needs to be at least of previous month"
                    )
                elif month_diff < 0:
                    sys.exit(
                        f"[VALUE ERROR] for {str(self.type).split('.')[1]} start date can not be of future month"
                    )

            elif int(self.type) == 200:
                current_quarter = math.floor((current_date_in_date.month - 1) / 3 + 1)
                starting_quarter = math.floor((start_date_in_date.month - 1) / 3 + 1)
                quater_diff = current_quarter - starting_quarter
                if quater_diff == 0:
                    sys.exit(
                        f"[VALUE ERROR] for {str(self.type).split('.')[1]} start date needs to be at least of previous quater"
                    )
                elif quater_diff < 0:
                    sys.exit(
                        f"[VALUE ERROR] for {str(self.type).split('.')[1]} start date can not be of future quater"
                    )

            elif int(self.type) == 300:
                starting_quarter = math.floor((start_date_in_date.month - 1) / 3 + 1)
                if starting_quarter == 1 or starting_quarter == 2:
                    sys.exit(
                        f"[VALUE ERROR] for {str(self.type).split('.')[1]} start date needs to be of previous bi-year."
                    )

            else:
                sys.exit(
                    f"[VALUE ERROR] for {str(self.type).split('.')[1]} start date needs to be of previous year."
                )

        if year_diff < 0:
            sys.exit(f"[VALUE ERROR] start date cant not be of future year.")

    def number_Of_Days(self, y, m):
        leap = 0
        if y % 400 == 0:
            leap = 1
        elif y % 100 == 0:
            leap = 0
        elif y % 4 == 0:
            leap = 1
        if m == 2:
            return 28 + leap
        list = [1, 3, 5, 7, 8, 10, 12]
        if m in list:
            return 31
        return 30

    def monthFirstAndLastDate(self, date_of_the_month):
        date_of_the_month_in_date = datetime.strptime(date_of_the_month, "%Y-%m-%d")
        dt_lastday_of_the_month = datetime(
            date_of_the_month_in_date.year,
            date_of_the_month_in_date.month,
            self.number_Of_Days(
                date_of_the_month_in_date.year, date_of_the_month_in_date.month
            ),
        ).date()
        dt_firstDay_of_the_month = datetime(
            date_of_the_month_in_date.year, date_of_the_month_in_date.month, 1
        ).date()
        return dt_firstDay_of_the_month, dt_lastday_of_the_month

    def quarterFirstAndLastDate(self, date_of_the_quarter):
        date_of_the_quarter_in_date = datetime.strptime(date_of_the_quarter, "%Y-%m-%d")
        quarter = math.floor((date_of_the_quarter_in_date.month - 1) / 3 + 1)
        dt_firstDay_of_the_quarter = datetime(
            date_of_the_quarter_in_date.year, 3 * quarter - 2, 1
        ).date()
        if 3 * quarter == 12:
            dt_lastday_of_the_quarter = datetime(
                date_of_the_quarter_in_date.year,
                12,
                self.number_Of_Days(date_of_the_quarter_in_date.year, 12),
            ).date()
        else:
            dt_lastday_of_the_quarter = datetime(
                date_of_the_quarter_in_date.year,
                3 * quarter,
                self.number_Of_Days(date_of_the_quarter_in_date.year, 3 * quarter),
            ).date()
        return dt_firstDay_of_the_quarter, dt_lastday_of_the_quarter

    def biYearFirstAndLastDate(self, date_of_the_bi_year):
        date_of_the_bi_year_in_date = datetime.strptime(date_of_the_bi_year, "%Y-%m-%d")
        if date_of_the_bi_year_in_date.month < 7:
            dt_lastday_of_the_bi_year = datetime(
                date_of_the_bi_year_in_date.year, 6, 30
            ).date()
            dt_first_day_of_the_bi_year = datetime(
                date_of_the_bi_year_in_date.year, 1, 1
            ).date()
        else:
            dt_lastday_of_the_bi_year = datetime(
                date_of_the_bi_year_in_date.year, 12, 31
            ).date()
            dt_first_day_of_the_bi_year = datetime(
                date_of_the_bi_year_in_date.year, 7, 1
            ).date()
        return dt_first_day_of_the_bi_year, dt_lastday_of_the_bi_year

    def yearFirstAndLastDate(self, date_of_the_year):
        date_of_the_year_in_date = datetime.strptime(date_of_the_year, "%Y-%m-%d")
        dt_lastday_of_the_year = datetime(date_of_the_year_in_date.year, 12, 31).date()
        dt_first_day_of_the_year = datetime(date_of_the_year_in_date.year, 1, 1).date()
        return dt_first_day_of_the_year, dt_lastday_of_the_year

    def monthCount(self, current_date):
        (
            first_day_of_the_mentioned_month,
            last_day_of_the_mentioned_month,
        ) = self.monthFirstAndLastDate(self.start_date)
        (
            first_day_of_the_current_month,
            last_day_of_the_current_month,
        ) = self.monthFirstAndLastDate(current_date)
        month_covered_of_the_starting_year = (
            12 - last_day_of_the_mentioned_month.month + 1
        )
        month_covered_of_the_current_year = last_day_of_the_current_month.month
        month_coverd_in_between = (
            (last_day_of_the_current_month.year - last_day_of_the_mentioned_month.year)
            - 1
        ) * 12
        total_number_of_month_covered = (
            month_covered_of_the_starting_year
            + month_covered_of_the_current_year
            + month_coverd_in_between
        )
        if current_date < last_day_of_the_current_month.strftime("%Y-%m-%d"):
            total_number_of_month_covered = total_number_of_month_covered - 1
        return total_number_of_month_covered

    def quarterCount(self, current_date):
        (
            first_day_of_the_mentioned_quarter,
            last_day_of_the_mentioned_quarter,
        ) = self.quarterFirstAndLastDate(self.start_date)
        (
            first_day_of_current_quarter,
            last_day_of_current_quarter,
        ) = self.quarterFirstAndLastDate(current_date)
        current_quarter_no = math.floor((last_day_of_current_quarter.month / 4) + 1)
        mentioned_date_quarter_no = math.floor(
            (last_day_of_the_mentioned_quarter.month / 4) + 1
        )
        quarter_covered_in_the_starting_year = 5 - mentioned_date_quarter_no
        current_date_quarter_no = math.floor(
            (last_day_of_current_quarter.month / 4) + 1
        )
        quarter_covered_in_the_current_year = current_date_quarter_no
        quarter_covered_in_between_years = (
            (last_day_of_current_quarter.year - last_day_of_the_mentioned_quarter.year)
            - 1
        ) * 4
        total_number_of_quarter_covered = (
            quarter_covered_in_the_starting_year
            + quarter_covered_in_the_current_year
            + quarter_covered_in_between_years
        )
        if current_date < last_day_of_current_quarter.strftime("%Y-%m-%d"):
            total_number_of_quarter_covered = total_number_of_quarter_covered - 1
        return total_number_of_quarter_covered

    def biYearCount(self, current_date):
        (
            first_day_of_the_mentioned_month,
            last_day_of_the_mentioned_month,
        ) = self.monthFirstAndLastDate(self.start_date)
        (
            first_day_of_the_current_month,
            last_day_of_the_current_month,
        ) = self.monthFirstAndLastDate(current_date)
        if last_day_of_the_mentioned_month.month < 7:
            bi_year_covered_of_the_starting_year = 2
        else:
            bi_year_covered_of_the_starting_year = 1
        if last_day_of_the_mentioned_month.month < 7:
            bi_year_covered_in_this_year = 0
        else:
            bi_year_covered_in_this_year = 1
        bi_year_covered_in_between = (
            (last_day_of_the_current_month.year - last_day_of_the_mentioned_month.year)
            - 1
        ) * 2
        total_bi_year_covered = (
            bi_year_covered_of_the_starting_year
            + bi_year_covered_in_between
            + bi_year_covered_in_this_year
        )
        return total_bi_year_covered

    def yearCount(self, current_date):
        (
            first_day_of_the_mentioned_month,
            last_day_of_the_mentioned_month,
        ) = self.monthFirstAndLastDate(self.start_date)
        (
            first_day_of_the_current_month,
            last_day_of_the_current_month,
        ) = self.monthFirstAndLastDate(current_date)
        total_year_completed = (
            last_day_of_the_current_month.year - last_day_of_the_mentioned_month.year
        )
        return total_year_completed

    def queryBuilder(self):
        querey_string = (
            " select dateRange, count(*) as count from \n (select \n case \n  "
        )
        start_date = self.start_date
        if int(self.type) == 100:
            for i in range(self.count_of_interval):
                (
                    first_date_of_the_month,
                    last_date_of_the_month,
                ) = self.monthFirstAndLastDate(start_date)
                first_date_time_of_the_month = (
                    first_date_of_the_month.strftime("%Y-%m-%d") + "T00:00:00.000"
                )
                last_date_time_of_the_month = (
                    last_date_of_the_month.strftime("%Y-%m-%d") + "T23:59:59.000"
                )
                case_string = f"when {self.column_name} between '{first_date_time_of_the_month}' and '{last_date_time_of_the_month}' then '{first_date_of_the_month} - {last_date_of_the_month}' \n"
                querey_string = querey_string + case_string
                date_of_the_month_type_date = last_date_of_the_month + timedelta(days=1)
                start_date = date_of_the_month_type_date.strftime("%Y-%m-%d")

        elif int(self.type) == 200:
            for i in range(self.count_of_interval):
                (
                    first_date_of_the_quarter,
                    last_date_of_the_quarter,
                ) = self.quarterFirstAndLastDate(start_date)
                first_date_time_of_the_quarter = (
                    first_date_of_the_quarter.strftime("%Y-%m-%d") + "T00:00:00.000"
                )
                last_date_time_of_the_quarter = (
                    last_date_of_the_quarter.strftime("%Y-%m-%d") + "T23:59:59.000"
                )
                case_string = f"when {self.column_name} between '{first_date_time_of_the_quarter}' and '{last_date_time_of_the_quarter}' then '{first_date_of_the_quarter} - {last_date_of_the_quarter}' \n"
                querey_string = querey_string + case_string
                date_of_the_quarter_type_date = last_date_of_the_quarter + timedelta(
                    days=1
                )
                start_date = date_of_the_quarter_type_date.strftime("%Y-%m-%d")

        elif int(self.type) == 300:
            for i in range(self.count_of_interval):
                (
                    first_date_of_the_bi_year,
                    last_date_of_the_bi_year,
                ) = self.biYearFirstAndLastDate(start_date)
                first_date_time_of_the_bi_year = (
                    first_date_of_the_bi_year.strftime("%Y-%m-%d") + "T00:00:00.000"
                )
                last_date_time_of_the_bi_year = (
                    last_date_of_the_bi_year.strftime("%Y-%m-%d") + "T23:59:59.000"
                )
                case_string = f"when {self.column_name} between '{first_date_time_of_the_bi_year}' and '{last_date_time_of_the_bi_year}' then '{first_date_of_the_bi_year} - {last_date_of_the_bi_year}' \n"
                querey_string = querey_string + case_string
                date_of_the_bi_type_type_date = last_date_of_the_bi_year + timedelta(
                    days=1
                )
                start_date = date_of_the_bi_type_type_date.strftime("%Y-%m-%d")

        elif int(self.type) == 400:
            for i in range(self.count_of_interval):
                (
                    first_date_of_the_year,
                    last_date_of_the_year,
                ) = self.yearFirstAndLastDate(start_date)
                first_date_time_of_the_year = (
                    first_date_of_the_year.strftime("%Y-%m-%d") + "T00:00:00.000"
                )
                last_date_time_of_the_year = (
                    last_date_of_the_year.strftime("%Y-%m-%d") + "T23:59:59.000"
                )
                case_string = f"when {self.column_name} between '{first_date_time_of_the_year}' and '{last_date_time_of_the_year}' then '{first_date_of_the_year} - {last_date_of_the_year}' \n"
                querey_string = querey_string + case_string
                date_of_the_bi_type_type_date = last_date_of_the_year + timedelta(
                    days=1
                )
                start_date = date_of_the_bi_type_type_date.strftime("%Y-%m-%d")

        querey_string = (
            querey_string
            + f"end \n as dateRange from {self.schema}.{self.table}) as subquery \n group by dateRange \n order by dateRange   ;\n"
        )
        print(querey_string)
        return querey_string

    def saveResultToCSV(self):
        self.date_validation()
        querey_string = self.queryBuilder()
        result = self.connection.cursor().execute(querey_string).fetchall()
        value_lst = [[item for item in row] for row in result]
        data_frame = pd.DataFrame(value_lst, columns=["DATE RANGE", "COUNT"])
        print("self.type is ...", str(self.type))
        type = PeriodType(self.type).name
        final_drive_path = f"{self.file_path}\\{self.database}_{self.schema}_{self.table}_{self.column_name}_{type}_DISTRIBUTION_SUMMARY.csv"
        data_frame.to_csv(final_drive_path, index=False)
        print(f"Report has been written to the {final_drive_path}")


class PeriodType(IntEnum):
    MONTHLY = 100
    QUARTERLY = 200
    BI_YEARLY = 300
    YEARLY = 400


class DateDimensionalFrquencey:
    def __init__(self, connection, params):
        self.connection = connection
        self.database = params["DATABASE_NAME"]
        self.schema = params["SCHEMA_NAME"]
        self.table = params["TABLE_NAME"]
        self.column_lst = params["COLUMN_LIST"]
        self.file_path = params["FILE_PATH"]
        self.when = params["WHEN"]
        self.fltr = params["FILTER"]

    def queryBuilder(self, column_name):
        date_querey_string = f"SELECT \
                          ISNULL(CONVERT(VARCHAR, {column_name}, 23), 'NULL') AS DATE_VALUE, \
                          COUNT(*) AS DATE_COUNT \
                          FROM \
                         {self.schema}.{self.table} \
                          GROUP BY \
                          ISNULL(CONVERT(VARCHAR, {column_name}, 23), 'NULL') \
                          ORDER BY \
                          DATE_COUNT DESC \
                          OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY;"  # tesing purpose it has been kept as 20
        datetime_query_string = f"SELECT \
                                  ISNULL(CONVERT(VARCHAR, {column_name}, 121), 'NULL') AS DATETIME_VALUE, \
                                  COUNT(*) AS DATETIME_COUNT \
                                  FROM \
                                  {self.schema}.{self.table} \
                                  GROUP BY \
                                  ISNULL(CONVERT(VARCHAR, {column_name}, 121), 'NULL') \
                                  ORDER BY \
                                  DATETIME_COUNT DESC \
                                  OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY;"  # tesing purpose it has been kept as 20
        print(date_querey_string)
        print(datetime_query_string)
        return date_querey_string, datetime_query_string

    def loopAndCount(self):
        final_df = None
        for column in self.column_lst:
            date_group_by_querey, datetime_group_by_querey = self.queryBuilder(column)
            # data = pd.read_sql(group_by_querey,self.connection) #some standard exception
            data_type_check_query = f"SELECT DATA_TYPE FROM \
                     INFORMATION_SCHEMA.COLUMNS \
                     WHERE TABLE_NAME = '{self.table}' AND TABLE_SCHEMA = '{self.schema}' AND COLUMN_NAME = '{column}'"
            data_type_of_the_column = (
                self.connection.cursor().execute(data_type_check_query).fetchall()
            )
            if data_type_of_the_column[0][0] == "date":
                group_by_count = (
                    self.connection.cursor().execute(date_group_by_querey).fetchall()
                )
            elif data_type_of_the_column[0][0] == "datetime":
                group_by_count = (
                    self.connection.cursor()
                    .execute(datetime_group_by_querey)
                    .fetchall()
                )
            else:
                print(
                    "different date type encounterd, acceptable data type is date and datetime"
                )

            # some how list of tuple is not getting saved in pandasd data frame. lets convert it to list of list
            # [('NULL', 2), ('2022-07-01', 1), ('2022-06-01', 1), ('2022-05-01', 1), ('2022-03-01', 1)] - list of tuple is getting treated as single column.
            # [['NULL', 2], ['2022-07-01', 1], ['2022-06-01', 1], ['2022-05-01', 1], ['2022-03-01', 1]] - list of list is getting treated as two column as we expected.
            value_lst = [[item for item in row] for row in group_by_count]
            # print(group_by_count)
            # print(value_lst)
            df = pd.DataFrame(value_lst, columns=["VALUE", "COUNT"])
            print(df)
            df.insert(
                loc=0,
                column="TABLE",
                value=f"{self.schema}.{self.table}",
            )
            df.insert(loc=1, column="COLUMN", value=column)
            if final_df is None:
                final_df = df
            else:
                final_df = pd.concat([final_df, df])
        final_drive_path = f"{self.file_path}\\{self.database}_{self.schema}_{self.table}_DATE_DIMENSIONAL_ATTRIBUTE_VALUE_FREQUENCY.csv"
        final_df.to_csv(final_drive_path, index=False)
        # data.to_csv(gdrive_path, index=False)
        print(f"Report has been written to the file {final_drive_path}")


class HistogramEnhancer:
    def __init__(self, params):
        self.columns_lst = params["COLUMNS"]
        self.start_year = params["START_YEAR"]
        self.start_month = params["START_MONTH"]
        self.years_to_cover = params["YEARS_TO_COVER"]
        # self.date = params["PROFILE_DATE"]
        self.file_path = params["FILE_PATH"]
        self.database = params["DATABASE_NAME"]
        self.database = params["DATABASE_NAME"]
        self.schema = params["SCHEMA_NAME"]
        self.table = params["TABLE_NAME"]

    def caller(self):
        for column in self.columns_lst:
            self.monthlyFunction(column)
            self.quaterlyFunction(column)
            self.yearlyFunction(column)

    def number_Of_Days(self, y, m):
        leap = 0
        if y % 400 == 0:
            leap = 1
        elif y % 100 == 0:
            leap = 0
        elif y % 4 == 0:
            leap = 1
        if m == 2:
            return 28 + leap
        list = [1, 3, 5, 7, 8, 10, 12]
        if m in list:
            return 31
        return 30

    def monthFirstAndLastDate(self, date_of_the_month):
        date_of_the_month_in_date = datetime.strptime(date_of_the_month, "%Y-%m-%d")
        dt_lastday_of_the_month = datetime(
            date_of_the_month_in_date.year,
            date_of_the_month_in_date.month,
            self.number_Of_Days(
                date_of_the_month_in_date.year, date_of_the_month_in_date.month
            ),
        ).date()
        dt_firstDay_of_the_month = datetime(
            date_of_the_month_in_date.year, date_of_the_month_in_date.month, 1
        ).date()
        return dt_firstDay_of_the_month, dt_lastday_of_the_month

    def quarterFirstAndLastDate(self, date_of_the_quarter):
        date_of_the_quarter_in_date = datetime.strptime(date_of_the_quarter, "%Y-%m-%d")
        quarter = math.floor((date_of_the_quarter_in_date.month - 1) / 3 + 1)
        dt_firstDay_of_the_quarter = datetime(
            date_of_the_quarter_in_date.year, 3 * quarter - 2, 1
        ).date()
        if 3 * quarter == 12:
            dt_lastday_of_the_quarter = datetime(
                date_of_the_quarter_in_date.year,
                12,
                self.number_Of_Days(date_of_the_quarter_in_date.year, 12),
            ).date()
        else:
            dt_lastday_of_the_quarter = datetime(
                date_of_the_quarter_in_date.year,
                3 * quarter,
                self.number_Of_Days(date_of_the_quarter_in_date.year, 3 * quarter),
            ).date()
        return dt_firstDay_of_the_quarter, dt_lastday_of_the_quarter

    def yearFirstAndLastDate(self, date_of_the_year):
        date_of_the_year_in_date = datetime.strptime(date_of_the_year, "%Y-%m-%d")
        dt_lastday_of_the_year = datetime(date_of_the_year_in_date.year, 12, 31).date()
        dt_first_day_of_the_year = datetime(date_of_the_year_in_date.year, 1, 1).date()
        return dt_first_day_of_the_year, dt_lastday_of_the_year

    def monthlyFunction(self, column):
        monthly_js = []
        start_y = self.start_year
        start_m = self.start_month
        for x in range(self.years_to_cover * 12):
            first_date, last_date = self.monthFirstAndLastDate(
                f"{start_y}-{start_m}-01"
            )
            print(first_date, last_date)
            monthly_js.append({"DATE RANGE": f"{first_date} - {last_date}"})

            if start_m == 12:
                start_y = start_y + 1
                start_m = 1
            else:
                start_m = start_m + 1
            currentMonth = datetime.now().month
            currentYear = datetime.now().year
            if start_m >= currentMonth and start_y == currentYear:
                break
        df_name = pd.DataFrame(monthly_js)
        df_org = pd.read_csv(
            f"{self.file_path}\\{self.database}_{self.schema}_{self.table}_{column}_MONTHLY_DISTRIBUTION_SUMMARY.csv"
        )
        df_org = df_org[df_org["DATE RANGE"].notna()]
        merged_df = pd.merge(df_name, df_org, on="DATE RANGE", how="left").fillna(0)
        drive_path = f"{self.file_path}\\{self.database}_{self.schema}_{self.table}_{column}_MONTHLY_HIST.csv"
        merged_df.to_csv(drive_path, index=False, float_format="%.0f")

    def quaterlyFunction(self, column):
        quaterly_js = []
        start_y = self.start_year
        start_m = self.start_month
        for x in range(self.years_to_cover * 4):
            first_date, last_date = self.quarterFirstAndLastDate(
                f"{start_y}-{start_m}-01"
            )
            quaterly_js.append({"DATE RANGE": f"{first_date} - {last_date}"})

            if start_m >= 10:
                start_y = start_y + 1
                start_m = 1
            else:
                start_m = start_m + 3
            currentMonth = datetime.now().month
            currentYear = datetime.now().year
            if start_m >= currentMonth and start_y == currentYear:
                break
        df_name = pd.DataFrame(quaterly_js)
        df_org = pd.read_csv(
            f"{self.file_path}\\{self.database}_{self.schema}_{self.table}_{column}_QUARTERLY_DISTRIBUTION_SUMMARY.csv"
        )
        df_org = df_org[df_org["DATE RANGE"].notna()]
        merged_df = pd.merge(df_name, df_org, on="DATE RANGE", how="left").fillna(0)
        drive_path = f"{self.file_path}\\{self.database}_{self.schema}_{self.table}_{column}_QUARTERLY_HIST.csv"
        merged_df.to_csv(drive_path, index=False, float_format="%.0f")

    def yearlyFunction(self, column):
        yearly_js = []
        start_y = self.start_year
        start_m = self.start_month
        for x in range(self.years_to_cover * 4):
            first_date, last_date = self.yearFirstAndLastDate(f"{start_y}-{start_m}-01")
            print(first_date, last_date)
            yearly_js.append({"DATE RANGE": f"{first_date} - {last_date}"})
            start_y = start_y + 1
            currentMonth = datetime.now().month
            currentYear = datetime.now().year
            if start_y == currentYear:
                break
        df_name = pd.DataFrame(yearly_js)
        df_org = pd.read_csv(
            f"{self.file_path}\\{self.database}_{self.schema}_{self.table}_{column}_YEARLY_DISTRIBUTION_SUMMARY.csv"
        )
        df_org = df_org[df_org["DATE RANGE"].notna()]
        merged_df = pd.merge(df_name, df_org, on="DATE RANGE", how="left").fillna(0)
        drive_path = f"{self.file_path}\\{self.database}_{self.schema}_{self.table}_{column}_YEARLY_HIST.csv"
        merged_df.to_csv(drive_path, index=False, float_format="%.0f")


class DateDimensionalFrquencey:
    def __init__(self, connection, params):
        self.connection = connection
        self.database = params["DATABASE_NAME"]
        self.schema = params["SCHEMA_NAME"]
        self.table = params["TABLE_NAME"]
        self.column_lst = params["COLUMN_LIST"]
        self.file_path = params["FILE_PATH"]
        self.when = params["WHEN"]
        self.fltr = params["FILTER"]

    def queryBuilder(self, column_name):
        date_querey_string = f"SELECT \
                          ISNULL(CONVERT(VARCHAR, {column_name}, 23), 'NULL') AS DATE_VALUE, \
                          COUNT(*) AS DATE_COUNT \
                          FROM \
                         {self.schema}.{self.table} \
                          GROUP BY \
                          ISNULL(CONVERT(VARCHAR, {column_name}, 23), 'NULL') \
                          ORDER BY \
                          DATE_COUNT DESC \
                          OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY;"  # tesing purpose it has been kept as 20
        datetime_query_string = f"SELECT \
                                  ISNULL(CONVERT(VARCHAR, {column_name}, 121), 'NULL') AS DATETIME_VALUE, \
                                  COUNT(*) AS DATETIME_COUNT \
                                  FROM \
                                  {self.schema}.{self.table} \
                                  GROUP BY \
                                  ISNULL(CONVERT(VARCHAR, {column_name}, 121), 'NULL') \
                                  ORDER BY \
                                  DATETIME_COUNT DESC \
                                  OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY;"  # tesing purpose it has been kept as 20
        print(date_querey_string)
        print(datetime_query_string)
        return date_querey_string, datetime_query_string

    def loopAndCount(self):
        final_df = None
        for column in self.column_lst:
            date_group_by_querey, datetime_group_by_querey = self.queryBuilder(column)
            # data = pd.read_sql(group_by_querey,self.connection) #some standard exception
            data_type_check_query = f"SELECT DATA_TYPE FROM \
                     INFORMATION_SCHEMA.COLUMNS \
                     WHERE TABLE_NAME = '{self.table}' AND TABLE_SCHEMA = '{self.schema}' AND COLUMN_NAME = '{column}'"
            data_type_of_the_column = (
                self.connection.cursor().execute(data_type_check_query).fetchall()
            )
            if data_type_of_the_column[0][0] == "date":
                group_by_count = (
                    self.connection.cursor().execute(date_group_by_querey).fetchall()
                )
            elif data_type_of_the_column[0][0] == "datetime":
                group_by_count = (
                    self.connection.cursor()
                    .execute(datetime_group_by_querey)
                    .fetchall()
                )
            else:
                print(
                    "different date type encounterd, acceptable data type is date and datetime"
                )

            # some how list of tuple is not getting saved in pandasd data frame. lets convert it to list of list
            # [('NULL', 2), ('2022-07-01', 1), ('2022-06-01', 1), ('2022-05-01', 1), ('2022-03-01', 1)] - list of tuple is getting treated as single column.
            # [['NULL', 2], ['2022-07-01', 1], ['2022-06-01', 1], ['2022-05-01', 1], ['2022-03-01', 1]] - list of list is getting treated as two column as we expected.
            value_lst = [[item for item in row] for row in group_by_count]
            # print(group_by_count)
            # print(value_lst)
            df = pd.DataFrame(value_lst, columns=["VALUE", "COUNT"])
            print(df)
            df.insert(
                loc=0,
                column="TABLE",
                value=f"{self.schema}.{self.table}",
            )
            df.insert(loc=1, column="COLUMN", value=column)
            if final_df is None:
                final_df = df
            else:
                final_df = pd.concat([final_df, df])
        final_drive_path = f"{self.file_path}\\{self.database}_{self.schema}_{self.table}_DATE_DIMENSIONAL_ATTRIBUTE_VALUE_FREQUENCY.csv"
        final_df.to_csv(final_drive_path, index=False)
        # data.to_csv(gdrive_path, index=False)
        print(f"Report has been written to the file {final_drive_path}")


class DimensionalFrquencey:
    def __init__(self, connection, params):
        self.connection = connection
        self.database = params["DATABASE_NAME"]
        self.schema = params["SCHEMA_NAME"]
        self.table = params["TABLE_NAME"]
        self.column_lst = params["COLUMN_LIST"]
        self.file_path = params["FILE_PATH"]
        self.when = params["WHEN"]
        self.fltr = params["FILTER"]

    def queryBuilder(self, column_name):
        querey_string = f"SELECT \
                          ISNULL(CONVERT(VARCHAR(255), {column_name}), 'NULL') AS VALUE, \
                          COUNT(*) AS COUNT \
                          FROM \
                          {self.schema}.{self.table} \
                          GROUP BY \
                          ISNULL(CONVERT(VARCHAR(255), {column_name}), 'NULL') \
                          ORDER BY \
                          COUNT DESC \
                          OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY;"  # tesing purpose it has been kept as 20
        print(querey_string)
        return querey_string

    def loopAndCount(self):
        final_df = None
        for column in self.column_lst:
            group_by_querey = self.queryBuilder(column)
            group_by_count = (
                self.connection.cursor().execute(group_by_querey).fetchall()
            )
            value_lst = [[item for item in row] for row in group_by_count]
            df = pd.DataFrame(value_lst, columns=["VALUE", "COUNT"])
            df.insert(
                loc=0,
                column="TABLE",
                value=f"{self.database}.{self.schema}.{self.table}",
            )
            df.insert(loc=1, column="COLUMN", value=column)
            if final_df is None:
                final_df = df
            else:
                final_df = pd.concat([final_df, df])
        final_path = f"{self.file_path}\\{self.database}_{self.schema}_{self.table}_DIMENSIONAL_ATTRIBUTE_VALUE_FREQUENCY.csv"
        final_df.to_csv(final_path, index=False)
        print(f"Report has been written to the file {final_path}")


class CsvToExcel:
    def __init__(self, params):
        self.source_folder_path = params["SOURCE_FOLDER"]
        self.destination_folder_path = params["DESTINATION_FOLDER"]
        self.database = params["DATABASE_NAME"]
        self.database = params["DATABASE_NAME"]
        self.schema = params["SCHEMA_NAME"]
        self.table = params["TABLE_NAME"]
        self.columns = params["COLUMNS"]
        # self.date = params["DATE"]
        self.start_row_for_distinct = params["START_ROW_FOR_DISTINCT"]
        self.top_count_for_distinct = params["TOP_DISTINCT"]
        self.years_to_cover = params["YEARS_TO_BE_COVERED"]
        self.writer = pd.ExcelWriter(
            f"{self.destination_folder_path}/{self.table}.xlsx"
        )

    def forSummary(self):
        df1 = pd.read_csv(
            f"{self.source_folder_path}\\{self.database}_{self.schema}_{self.table}_SUMMARY.csv"
        )
        df2 = pd.read_csv(
            f"{self.source_folder_path}\\{self.database}_{self.schema}_{self.table}_ATTRIBUTE_SUMMARY.csv"
        )
        return df1, df2

    def attributeSummary(self):
        df = pd.read_csv(
            f"{self.source_folder_path}\\{self.database}_{self.schema}_{self.table}_ATTRIBUTE_DETAIL.csv"
        )
        for i in range(len(df)):
            attribute_det = []
            if df.loc[i, "COLUMN NAME"] in self.columns:
                print("column name: ", df.loc[i, "COLUMN NAME"])
                if (
                    df.loc[i, "DATA TYPE"] == "NUMBER"
                    or df.loc[i, "DATA TYPE"] == "FLOAT"
                ):
                    attribute_det.append(
                        {
                            "COLUMN NAME": df.loc[i, "COLUMN NAME"],
                            "DATA TYPE": df.loc[i, "DATA TYPE"],
                            "IS NULLABLE": df.loc[i, "IS NULLABLE"],
                            "TOTAL RECORD COUNT": df.loc[i, "TOTAL RECORD COUNT"],
                            "POPULATED RECORD COUNT": df.loc[
                                i, "POPULATED RECORD COUNT"
                            ],
                            "% POPULATED RECORD COUNT": df.loc[
                                i, "% POPULATED RECORD COUNT"
                            ],
                            "DISTINCT RECORD COUNT": df.loc[i, "DISTINCT RECORD COUNT"],
                            "% DISTINCT RECORD COUNT RESPECT TO TOTAL": df.loc[
                                i, "% DISTINCT RECORD COUNT RESPECT TO TOTAL"
                            ],
                            "% DISTINCT RECORD COUNT RESPECT TO POPULATED": df.loc[
                                i, "% DISTINCT RECORD COUNT RESPECT TO POPULATED"
                            ],
                            "MIN": df.loc[i, "MIN"],
                            "MAX": df.loc[i, "MAX"],
                            "AVERAGE": df.loc[i, "AVERAGE"],
                            "STANDARD DEVIATION": df.loc[i, "STANDARD DEVIATION"],
                            "VARIANCE": df.loc[i, "VARIANCE"],
                        }
                    )
                    detail_df = pd.DataFrame(attribute_det)
                    data_frame_transpose = detail_df.T.reset_index().rename(
                        columns={"index": "ATTRIBUTE", 0: "VALUE"}
                    )
                    sheet_name = f'{df.loc[i, "COLUMN NAME"]}'
                    if len(sheet_name) <= 31:
                        data_frame_transpose.to_excel(
                            self.writer,
                            sheet_name=sheet_name,
                            index=False,
                            header=False,
                            startrow=1,
                            startcol=0,
                        )
                        self.create_header(sheet_name, data_frame_transpose, 0)

                    else:
                        sheet_name = sheet_name[:31]
                        data_frame_transpose.to_excel(
                            self.writer,
                            sheet_name=sheet_name,
                            index=False,
                            header=False,
                            startrow=1,
                            startcol=0,
                        )
                        self.create_header(sheet_name, data_frame_transpose, 0)

                    till_row = self.nonDateDistinctDetail(
                        df.loc[i, "COLUMN NAME"],
                        self.start_row_for_distinct,
                        df.loc[i, "TOTAL RECORD COUNT"],
                        sheet_name,
                    )
                    if till_row == self.top_count_for_distinct:
                        row_value = [
                            f"FREQUENCY OF TOP {self.top_count_for_distinct} DISTINCT VALUES"
                        ]
                    else:
                        row_value = ["FREQUENCY OF DISTINCT VALUES"]
                    self.add_title(
                        (self.start_row_for_distinct - 2), 0, sheet_name, row_value
                    )
                    self.lineChart(
                        df.loc[i, "COLUMN NAME"],
                        self.start_row_for_distinct,
                        sheet_name,
                        till_row,
                    )

                else:
                    attribute_det.append(
                        {
                            "COLUMN NAME": df.loc[i, "COLUMN NAME"],
                            "DATA TYPE": df.loc[i, "DATA TYPE"],
                            "IS NULLABLE": df.loc[i, "IS NULLABLE"],
                            "TOTAL RECORD COUNT": df.loc[i, "TOTAL RECORD COUNT"],
                            "POPULATED RECORD COUNT": df.loc[
                                i, "POPULATED RECORD COUNT"
                            ],
                            "% POPULATED RECORD COUNT": df.loc[
                                i, "% POPULATED RECORD COUNT"
                            ],
                            "DISTINCT RECORD COUNT": df.loc[i, "DISTINCT RECORD COUNT"],
                            "% DISTINCT RECORD COUNT RESPECT TO TOTAL": df.loc[
                                i, "% DISTINCT RECORD COUNT RESPECT TO TOTAL"
                            ],
                            "% DISTINCT RECORD COUNT RESPECT TO POPULATED": df.loc[
                                i, "% DISTINCT RECORD COUNT RESPECT TO POPULATED"
                            ],
                            "MIN": df.loc[i, "MIN"],
                            "MAX": df.loc[i, "MAX"],
                            "AVERAGE": "N/A",
                            "STANDARD DEVIATION": "N/A",
                            "VARIANCE": "N/A",
                        }
                    )
                    detail_df = pd.DataFrame(attribute_det)
                    data_frame_transpose = detail_df.T.reset_index().rename(
                        columns={"index": "ATTRIBUTE", 0: "VALUE"}
                    )
                    sheet_name = f'{df.loc[i, "COLUMN NAME"]}'
                    if len(sheet_name) <= 31:
                        data_frame_transpose.to_excel(
                            self.writer,
                            sheet_name=sheet_name,
                            index=False,
                            header=False,
                            startrow=1,
                            startcol=0,
                        )
                        self.create_header(sheet_name, data_frame_transpose, 0)
                    else:
                        print("length and sheet name :", len(sheet_name), sheet_name)
                        sheet_name = sheet_name[:31]
                        data_frame_transpose.to_excel(
                            self.writer,
                            sheet_name=sheet_name,
                            index=False,
                            header=False,
                            startrow=1,
                            startcol=0,
                        )
                        self.create_header(sheet_name, data_frame_transpose, 0)
                    if (
                        df.loc[i, "DATA TYPE"] == "DATE"
                        or df.loc[i, "DATA TYPE"] == "TIMESTAMP_TZ"
                        or df.loc[i, "DATA TYPE"] == "TIMESTAMP_NTZ"
                        or df.loc[i, "DATA TYPE"] == "TIMESTAMP_LTZ"
                        or df.loc[i, "DATA TYPE"] == "date"
                        or df.loc[i, "DATA TYPE"] == "datetime"
                    ):
                        till_row = self.dateDistinctDetail(
                            df.loc[i, "COLUMN NAME"],
                            self.start_row_for_distinct,
                            df.loc[i, "TOTAL RECORD COUNT"],
                            sheet_name,
                        )
                        if till_row == self.top_count_for_distinct:
                            row_value = [
                                f"FREQUENCY OF TOP {self.top_count_for_distinct} DISTINCT VALUES"
                            ]
                        else:
                            row_value = ["FREQUENCY OF DISTINCT VALUES"]
                        self.add_title(
                            (self.start_row_for_distinct - 2), 0, sheet_name, row_value
                        )
                        self.lineChart(
                            df.loc[i, "COLUMN NAME"],
                            self.start_row_for_distinct,
                            sheet_name,
                            till_row,
                        )
                        months_start_row, till_row = self.date_month_wise_chart(
                            df.loc[i, "COLUMN NAME"],
                            sheet_name,
                            df.loc[i, "TOTAL RECORD COUNT"],
                        )
                        year_count = (till_row - months_start_row) / 12
                        month_title_value = [
                            f"MONTH WISE FREQUENCY OF LAST {self.years_to_cover} YEARS"
                        ]
                        self.add_title(
                            (months_start_row - 2), 0, sheet_name, month_title_value
                        )
                        self.lineChart(
                            df.loc[i, "COLUMN NAME"],
                            months_start_row,
                            sheet_name,
                            till_row,
                        )
                        quarterly_start_row, till_row = self.date_quarter_wise_chart(
                            df.loc[i, "COLUMN NAME"],
                            sheet_name,
                            df.loc[i, "TOTAL RECORD COUNT"],
                            months_start_row,
                        )
                        quarter_title_value = [
                            f"QUARTER WISE FREQUENCY OF LAST {self.years_to_cover} YEARS"
                        ]
                        self.add_title(
                            (quarterly_start_row - 2),
                            0,
                            sheet_name,
                            quarter_title_value,
                        )
                        self.lineChart(
                            df.loc[i, "COLUMN NAME"],
                            quarterly_start_row,
                            sheet_name,
                            till_row,
                        )
                        yearly_start_row, till_row = self.date_year_wise_chart(
                            df.loc[i, "COLUMN NAME"],
                            sheet_name,
                            df.loc[i, "TOTAL RECORD COUNT"],
                            quarterly_start_row,
                        )
                        year_title_value = [
                            f"YEAR WISE FREQUENCY OF LAST {self.years_to_cover} YEARS"
                        ]
                        self.add_title(
                            (yearly_start_row - 2), 0, sheet_name, year_title_value
                        )
                        self.lineChart(
                            df.loc[i, "COLUMN NAME"],
                            yearly_start_row,
                            sheet_name,
                            till_row,
                        )
                    else:
                        till_row = self.nonDateDistinctDetail(
                            df.loc[i, "COLUMN NAME"],
                            self.start_row_for_distinct,
                            df.loc[i, "TOTAL RECORD COUNT"],
                            sheet_name,
                        )
                        if till_row == self.top_count_for_distinct:
                            row_value = [
                                f"FREQUENCY OF TOP {self.top_count_for_distinct} DISTINCT VALUES"
                            ]
                        else:
                            row_value = ["FREQUENCY OF DISTINCT VALUES"]
                        self.add_title(
                            (self.start_row_for_distinct - 2), 0, sheet_name, row_value
                        )

                        self.lineChart(
                            df.loc[i, "COLUMN NAME"],
                            self.start_row_for_distinct,
                            sheet_name,
                            till_row,
                        )

    def nonDateDistinctDetail(self, column, start_row, total_record, sheet_name):
        ####******hardcoded date it has to be changed to self.date before prod
        #sed -i '' -e 's/^null$|^$/Blank or Null/g' f"{self.source_folder_path}\\{self.database}_{self.schema}_{self.table}_DIMENSIONAL_ATTRIBUTE_VALUE_FREQUENCY.csv"

        df_distinct = pd.read_csv(
            f"{self.source_folder_path}\\{self.database}_{self.schema}_{self.table}_DIMENSIONAL_ATTRIBUTE_VALUE_FREQUENCY.csv",keep_default_na=False
        )
        filtered_df = df_distinct[df_distinct["COLUMN"] == column]
        final_filtered_df = (
            filtered_df[["VALUE", "COUNT"]]
            .sort_values(by=["COUNT"], ascending=False)
            .head(self.top_count_for_distinct)
        )
        till_row = len(final_filtered_df)

        # here only null and balnk values needed too consider as BLANK OR NULL 
        print("replacing value...........................................") 

        final_filtered_df["VALUE"] = final_filtered_df["VALUE"].replace(['NULL', ''], 'BLANK OR NULL')  # Replace nulls and empty strings
       # print('after replacing null and blank ')
        result  = final_filtered_df.groupby('VALUE')['COUNT'].sum()
        final_filtered_df = result.to_frame(name ='COUNT').reset_index().sort_values(by='COUNT', ascending=False)
        #final_filtered_df['VALUE']=filtered_df['VALUE']
        final_filtered_df["%"] = round(
            (final_filtered_df["COUNT"] / total_record) * 100, 4
        )
        # Convert the Series to a DataFrame (optional, but useful for further manipulation)
#df_result = result.to_frame(name='Total Marks') 
        print('after grp by')
        print(final_filtered_df)
        top_total = final_filtered_df["COUNT"].sum()
        top_total_percent = final_filtered_df["%"].sum()
        total_df = pd.DataFrame([[None, top_total, top_total_percent]], columns=['VALUE', 'COUNT', '%'])
        final_filtered_df = pd.concat([final_filtered_df, total_df])
        print('2nd time:', final_filtered_df)
        final_filtered_df.to_excel(
            self.writer,
            sheet_name=sheet_name,
            index=False,
            startrow=start_row + 1,
            header=False,
            startcol=0,
        )
        self.create_header(sheet_name, final_filtered_df, start_row)
        total_df = [None, top_total, top_total_percent]
        self.set_border(start_row + till_row + 1, 0, sheet_name, total_df)
        return till_row
 
    def dateDistinctDetail(self, column, start_row, total_record, sheet_name):
        ####******hardcoded date it has to be changed to self.date before prod
        df_distinct = pd.read_csv(
            f"{self.source_folder_path}\\{self.database}_{self.schema}_{self.table}_DATE_DIMENSIONAL_ATTRIBUTE_VALUE_FREQUENCY.csv"
        )
        filtered_df = df_distinct[df_distinct["COLUMN"] == column]
        final_filtered_df = (
            filtered_df[["VALUE", "COUNT"]]
            .sort_values(by=["COUNT"], ascending=False)
            .head(self.top_count_for_distinct)
        )
        till_row = len(final_filtered_df)
        print(till_row)
        final_filtered_df["%"] = round(
            (final_filtered_df["COUNT"] / total_record) * 100, 4
        )
        final_filtered_df["VALUE"].fillna("BLANK OR NULL", inplace=True)    ## NA as well as N/A blank treat 
        top_total = final_filtered_df["COUNT"].sum()
        top_total_percent = final_filtered_df["%"].sum()
        # total_df = pd.DataFrame([[None, top_total, top_total_percent]], columns=['VALUE', 'COUNT', '%'])
        # final_filtered_df = pd.concat([final_filtered_df, total_df])
        total_df = [None, top_total, top_total_percent]
        self.set_border(start_row + till_row, 0, sheet_name, total_df)
        final_filtered_df.to_excel(
            self.writer,
            sheet_name=sheet_name,
            index=False,
            startrow=start_row + 1,
            header=False,
            startcol=0,
        )
        self.create_header(sheet_name, final_filtered_df, start_row)
        total_df = [None, top_total, top_total_percent]
        self.set_border(start_row + till_row + 1, 0, sheet_name, total_df)
        return till_row

    def lineChart(self, column, start_row, sheet_name, till_row):
        till_row = till_row + 1
        print("sheet name in linechart :", sheet_name)
        workbook = self.writer.book
        worksheet = self.writer.sheets[sheet_name]
        chart = workbook.add_chart({"type": "column"})
        chart.add_series(
            {
                "name": f"={sheet_name}!$D${start_row + 1}",
                "categories": f"={sheet_name}!$A${start_row + 2}:$A${start_row + till_row}",
                "values": f"={sheet_name}!$B${start_row + 2}:$B${start_row + till_row}",
            }
        )
        chart.set_title({"name": "Count Analysis"})
        chart.set_x_axis({"name": "Value"})
        chart.set_y_axis({"name": "Count"})
        chart.set_style(11)
        # chart.set_size({'width': 10*(till_row - start_row), 'height': 4.5*(till_row - start_row)})
        worksheet.insert_chart(f"E{start_row + 1}", chart)
        worksheet.autofit()
        print("chart has been prepared")

    def date_month_wise_chart(self, column, sheet_name, total_record):
        df_monthly = pd.read_csv(
            f"{self.source_folder_path}\\{self.database}_{self.schema}_{self.table}_{column}_MONTHLY_HIST.csv"
        )
        df_monthly.rename(columns={"DATE RANGE": "VALUE"}, inplace=True)
        df_monthly["%"] = round((df_monthly["COUNT"] / total_record) * 100, 4)
        df_monthly = df_monthly[df_monthly["VALUE"].notna()]
        till_row = len(df_monthly)
        top_total = df_monthly["COUNT"].sum()
        top_total_percent = df_monthly["%"].sum()
        # total_df = pd.DataFrame([[None, top_total, top_total_percent]], columns=['VALUE', 'COUNT', '%'])
        # df_monthly = pd.concat([df_monthly, total_df])
        start_row = self.start_row_for_distinct + self.top_count_for_distinct + 6
        df_monthly.to_excel(
            self.writer,
            sheet_name=sheet_name,
            index=False,
            startrow=start_row + 1,
            header=False,
            startcol=0,
        )
        self.create_header(sheet_name, df_monthly, start_row)
        total_df = [None, top_total, top_total_percent]
        self.set_border(start_row + till_row + 1, 0, sheet_name, total_df)
        return start_row, till_row

    def date_quarter_wise_chart(
        self, column, sheet_name, total_record, monthly_start_row
    ):
        df_quarterly = pd.read_csv(
            f"{self.source_folder_path}\\{self.database}_{self.schema}_{self.table}_{column}_QUARTERLY_HIST.csv"
        )
        df_quarterly.rename(columns={"DATE RANGE": "VALUE"}, inplace=True)
        df_quarterly["%"] = round((df_quarterly["COUNT"] / total_record) * 100, 4)
        df_quarterly = df_quarterly[df_quarterly["VALUE"].notna()]
        till_row = len(df_quarterly)
        top_total = df_quarterly["COUNT"].sum()
        top_total_percent = df_quarterly["%"].sum()
        # total_df = pd.DataFrame([[None, top_total, top_total_percent]], columns=['VALUE', 'COUNT', '%'])
        # df_quarterly = pd.concat([df_quarterly, total_df])
        start_row = monthly_start_row + round(self.years_to_cover, 0) * 12 + 6
        df_quarterly.to_excel(
            self.writer,
            sheet_name=sheet_name,
            index=False,
            startrow=start_row + 1,
            header=False,
            startcol=0,
        )
        self.create_header(sheet_name, df_quarterly, start_row)
        total_df = [None, top_total, top_total_percent]
        self.set_border(start_row + till_row + 1, 0, sheet_name, total_df)
        return start_row, till_row

    def date_year_wise_chart(
        self, column, sheet_name, total_record, quarterly_start_row
    ):
        df_yearly = pd.read_csv(
            f"{self.source_folder_path}\\{self.database}_{self.schema}_{self.table}_{column}_YEARLY_HIST.csv"
        )
        df_yearly.rename(columns={"DATE RANGE": "VALUE"}, inplace=True)
        df_yearly["%"] = round((df_yearly["COUNT"] / total_record) * 100, 4)
        df_yearly = df_yearly[df_yearly["VALUE"].notna()]
        till_row = len(df_yearly)
        top_total = df_yearly["COUNT"].sum()
        top_total_percent = df_yearly["%"].sum()
        # total_df = pd.DataFrame([[None, top_total, top_total_percent]], columns=['VALUE', 'COUNT', '%'])
        # df_yearly = pd.concat([df_yearly, total_df])
        start_row = quarterly_start_row + round(self.years_to_cover, 0) * 4 + 6
        df_yearly.to_excel(
            self.writer,
            sheet_name=sheet_name,
            index=False,
            startrow=start_row + 1,
            header=False,
            startcol=0,
        )
        self.create_header(sheet_name, df_yearly, start_row)
        total_df = [None, top_total, top_total_percent]
        self.set_border(start_row + till_row + 1, 0, sheet_name, total_df)
        return start_row, till_row

    def add_title(self, row, column, sheet_name, title):
        workbook = self.writer.book
        cell_format = workbook.add_format({"bold": True, "font_size": 12})
        cell_format.set_bg_color("orange")
        worksheet = self.writer.sheets[sheet_name]
        worksheet.write_row(row, column, tuple(title), cell_format)

    def create_header(self, sheet_name, df, rownum):
        # Add a header format.
        workbook = self.writer.book
        worksheet = self.writer.sheets[sheet_name]
        header_format = workbook.add_format(
            {
                "bold": True,
                "font_size": 12,
                "valign": "top",
                "align": "center",
                "fg_color": "#b4cdcd",
                "border": 1,
            }
        )
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(rownum, col_num, value, header_format)

    def set_border(self, row, column, sheet_name, title):
        workbook = self.writer.book
        cell_format = workbook.add_format(
            {
                "bold": True,
            }
        )
        cell_format.set_border()
        worksheet = self.writer.sheets[sheet_name]
        worksheet.write_row(row, column, tuple(title), cell_format)

    def startConverting(self):
        df1, df2 = self.forSummary()
        df1.to_excel(
            self.writer,
            sheet_name="Dataset Summary",
            index=False,
            header=False,
            startrow=1,
            startcol=0,
        )
        self.create_header("Dataset Summary", df1, 0)
        worksheet = self.writer.sheets["Dataset Summary"]
        worksheet.autofit()
        df2.to_excel(
            self.writer,
            sheet_name="Attribute Summary",
            index=False,
            header=False,
            startrow=1,
            startcol=0,
        )
        self.create_header("Attribute Summary", df2, 0)
        worksheet = self.writer.sheets["Attribute Summary"]
        worksheet.autofit()
        self.attributeSummary()
        # self.writer.save()
        self.writer.close()
        print(f"check this path {self.destination_folder_path}")
