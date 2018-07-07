from pandas import pandas
import numpy as np
import constants as const
from pydash import py_


class UtilitySceneryCreation:
    __instance = None

    @staticmethod
    def get_instance():
        """ Static access method. """
        if UtilitySceneryCreation.__instance is None:
            UtilitySceneryCreation()
        return UtilitySceneryCreation.__instance

    def __init__(self):
        self.array_columns_order = [const.TIMESTAMP_COLUMN]
        """ Virtually private constructor. """
        if UtilitySceneryCreation.__instance is not None:
            raise Exception("UtilitySceneryCreation class is a singleton!")
        else:
            UtilitySceneryCreation.__instance = self




    def put_data_set(self):
        data_frame = self.util_read_data_set()

        # transform timestamp to datetime
        date_set = pandas.to_datetime(data_frame.date, unit=const.DATE_TIME_UNIT)
        data_frame[const.DATE_COLUMN] = date_set

        # create error_columns from mq135,7 and 2
        data_frame[const.MQ2_ERROR_COLUMN] = data_frame.mq2 * const.PERCENTAGE_ERROR
        data_frame[const.MQ7_ERROR_COLUMN] = data_frame.mq7 * const.PERCENTAGE_ERROR
        data_frame[const.MQ135_ERROR_COLUMN] = data_frame.mq135 * const.PERCENTAGE_ERROR
        return data_frame

    @staticmethod
    def util_read_data_set():
        return pandas.read_csv('../data/data.csv')

    @staticmethod
    def util_drop_column(col, df):
        result_process = lambda item: df.drop(col, const.ACTIVE_FLAG)
        return result_process(col)

    def util_create_scenery(self,df, historic_count, drop_columns):
        final = df
        for x in range(const.FIRST_INDEX, df[const.DATE_COLUMN].size):
            final_data = self.util_scenario(df.iloc[x], df, x+const.ONE, historic_count, drop_columns)
            if x == const.ONE_INDEX:
                final = pandas.DataFrame(data=final_data)
                final = final.T
            else:
                final_tmp = pandas.DataFrame(data=final_data)
                final_tmp = final_tmp.T
                final = final.append(final_tmp)
        # put header in df
        final.columns = self.array_columns_order
        # initialize columns array
        self.init_columns_array()
        # drop row that contains NaN
        final = final.dropna()
        return final

    def create_header_array(self, array_len, key):
        for item in reversed(range(array_len)):
            column = key + '-' + str(item+const.ONE)
            if column not in self.array_columns_order:
                self.array_columns_order.append(column)

    def util_scenario(self,item, df, index, historic_count, drop_columns):
        scenery_columns = self.util_get_scenery_columns(drop_columns)
        historic_data_set = {}

        def predicate(param): historic_data_set.update({param:self.util_process_historical_values(df, historic_count, index, param)})

        py_.for_each(scenery_columns, predicate)
        historic_final_data = np.array([item[const.DATE_COLUMN]])

        for key,values in historic_data_set.items():
            aux_column = np.array(values)
            historic_final_data = np.concatenate((historic_final_data, aux_column))
            if key not in self.array_columns_order:
                self.create_header_array(len(values), key)
        return historic_final_data

    @staticmethod
    def util_find_key_by_value(self,value,l_dictionary):
        l_key = [key for key, value in l_dictionary.iteritems() if value == value][const.ZERO]
        return l_key

    def init_columns_array(self):
        self.array_columns_order = [const.TIMESTAMP_COLUMN]

    def util_get_scenery_columns(self,drop_columns):
        return self.get_column_headers_def() - drop_columns

    @staticmethod
    def util_process_historical_values(df, periodicity_key, start_index, col_name):
        array = df.iloc[start_index:periodicity_key + start_index][col_name].values
        return np.flipud(array)

    @staticmethod
    def previous_data_historic(key, df):
        historic_frequency = {const.MINUTE: df[const.DATE_COLUMN].dt.minute,
                              const.HOUR: df[const.DATE_COLUMN].dt.hour,
                              const.MONTH: df[const.DATE_COLUMN].dt.month,
                              const.SECOND: df[const.DATE_COLUMN].dt.second,
                              const.YEAR: df[const.DATE_COLUMN].dt.year}
        return historic_frequency.get(key)

    @staticmethod
    def get_column_headers_def():
        column_headers = {
            const.TIMESTAMP_COLUMN
            , const.NODE_COLUMN
            , const.LOCATION_COLUMN
            , const.HUMIDITY_COLUMN
            , const.MQ2_COLUMN
            , const.MQ7_COLUMN
            , const.MQ2_ERROR_COLUMN
            , const.MQ7_ERROR_COLUMN
            , const.MQ135_ERROR_COLUMN
            , const.MQ135_COLUMN
            , const.TEMPERATURE_COLUMN
        }
        return column_headers

    @staticmethod
    def get_combine_vars():
        column_headers = [
              const.HUMIDITY_COLUMN
            , const.MQ2_COLUMN
            , const.MQ7_COLUMN
            , const.MQ2_ERROR_COLUMN
            , const.MQ7_ERROR_COLUMN
            , const.MQ135_ERROR_COLUMN
            , const.MQ135_COLUMN
            , const.TEMPERATURE_COLUMN
        ]
        return column_headers

    def potency(self,c):
        """potency of c
        """
        if len(c) == const.ZERO:
            return [[]]
        r = self.potency(c[:const.MINUS_ONE])
        return r + [s + [c[const.MINUS_ONE]] for s in r]

    def comb(self,c, n):
        """list of combination of n elements
            one by one
        """
        return [s for s in self.potency(c) if len(s) == n]

