from pandas import pandas
import numpy as np
import constants as const
from pydash import py_
import re



class UtilitySceneryCreation:
    __instance = None

    @staticmethod
    def get_instance():
        """ Static access method. """
        if UtilitySceneryCreation.__instance is None:
            UtilitySceneryCreation()
        return UtilitySceneryCreation.__instance

    def __init__(self):
        self.predict_column = None
        self.array_columns_order = [const.TIMESTAMP_COLUMN]
        self.scenery_columns = None
        self.predict_variable = None
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
        #data_frame[const.MQ2_ERROR_COLUMN] = data_frame.mq2 * const.PERCENTAGE_ERROR
        #data_frame[const.MQ7_ERROR_COLUMN] = data_frame.mq7 * const.PERCENTAGE_ERROR
        #data_frame[const.MQ135_ERROR_COLUMN] = data_frame.mq135 * const.PERCENTAGE_ERROR
        return data_frame

    @staticmethod
    def util_read_data_set():
        return pandas.read_csv('../data/data.csv')

    @staticmethod
    def util_drop_column(col, df):
        result_process = lambda item: df.drop(col, const.ACTIVE_FLAG)
        return result_process(col)

    def util_create_scenery(self,df, historic_count, drop_columns,prediction):
        '''
        function to create scenery
        :param df: data frame input
        :param historic_count: count to historical data
        :param drop_columns: columns to drop
        :param prediction: prediction for scenery
        :return:
        '''
        final = None
        if self.predict_column is None:
            final = df
            for x in range(const.FIRST_INDEX, 5):#df[const.DATE_COLUMN].size):

                final_data = self.util_scenario(df.iloc[x], df, x, historic_count+const.ONE, drop_columns)
                if x == const.ONE_INDEX:
                    final = pandas.DataFrame(data=final_data)
                    final = final.T
                else:
                    final_tmp = pandas.DataFrame(data=final_data)
                    final_tmp = final_tmp.T
                    final = final.append(final_tmp)
            # find mq, prediction could be None
            drop_current_data = self.find_mq(self.array_columns_order, prediction)

            if drop_current_data is not None:
                # put header in df
                final.columns = self.array_columns_order
                # get a set of columns for current data to drop for order the scenery
                drop_current_data = list(set(drop_current_data) & set(self.array_columns_order))
                # dropping the variable to predict
                final = self.util_drop_column(set(drop_current_data), final)
                # find predict columns
                predict_column_list = self.find_prediction_columns(self.array_columns_order,self.predict_column)
                # df of predicted columns
                predicted_column_df = self.get_predict_columns_df(final,predict_column_list)
                # df of unpredicted columns
                unpredicted_columns = self.get_unpredicted_columns(final,predict_column_list)
                final = pandas.concat([unpredicted_columns, predicted_column_df], axis=1, sort=False)
                # initialize columns array
                self.init_columns_array()
                # initialize prediction element
                self.predict_column = None
                # drop row that contains NaN
                final = final.dropna()
            else:
                final = None
                self.init_columns_array()
        print("---Scenery Columns----", self.scenery_columns)
        print("---scenery prediction variable---", self.predict_variable)
        return final

    def get_predict_columns_df(self, df, index_collection):
        drop_unpredicted_columns = set(df) - set(index_collection)
        df = self.util_drop_column(drop_unpredicted_columns,df)
        return df

    def get_unpredicted_columns(self, df, index_collection):
        df = self.util_drop_column(index_collection, df)
        return df

    @staticmethod
    def find_prediction_columns(header_list,comparator_string):
        comparator_string = comparator_string.split("-")
        prediction_columns = list(filter(lambda item:re.findall(comparator_string[0],item),header_list))
        return prediction_columns

    def create_header_array(self, array_len, key):
        '''
        create header array values
        :param array_len: complete data array len
        :param key: column
        :return:
        '''
        for item in reversed(range(array_len)):
            column = key + '-' + str(item)
            if column not in self.array_columns_order:
                self.array_columns_order.append(column)

    def util_scenario(self,item, df, index, historic_count, drop_columns):
        '''
        define data to create scenery
        :param item:
        :param df: data frame
        :param index: index for iterable object
        :param historic_count:
        :param drop_columns:
        :return:
        '''
        scenery_columns = self.util_get_scenery_columns(drop_columns)
        self.scenery_columns = scenery_columns
        historic_data_set = {}

        def predicate(param): historic_data_set.update({param:self.util_process_historical_values(df, historic_count,
                                                                                                  index, param)})

        py_.for_each(scenery_columns, predicate)
        historic_final_data = np.array([item[const.DATE_COLUMN]])

        for key,values in historic_data_set.items():
            aux_column = np.array(values)
            historic_final_data = np.concatenate((historic_final_data, aux_column))
            if key not in self.array_columns_order:
                self.create_header_array(len(values), key)
        return historic_final_data

    def find_mq(self,col, prediction):
        '''
        it allows to find an mq variable to predict or
        put a prediction of a variable param instead
        :param col: scenery columns
        :param prediction: prediction variable (could be None)
        :return:
        '''
        drop_list_now_values = None

        if prediction is None:
            index = py_.find_index(col, lambda x: x in self.get_mq_vars())
        else:
            index = py_.find_index(col, lambda x: x == prediction)
        if index is not const.MINUS_ONE:
            value_index = col[index]
            self.predict_variable = value_index
            self.predict_column = value_index
            index_in_list=py_.find_index(self.get_mq_vars(), lambda x: x in value_index)

            drop_list_now_values = list(self.get_vars_now())
            drop_list_now_values.pop(index_in_list)
        return drop_list_now_values

    @staticmethod
    def util_find_key_by_value(self,value,l_dictionary):
        l_key = [key for key, value in l_dictionary.iteritems() if value == value][const.ZERO]
        return l_key

    def init_columns_array(self):
        self.array_columns_order = [const.TIMESTAMP_COLUMN]

    def util_get_scenery_columns(self,drop_columns):
        '''
        return a set of scenery columns
        :param drop_columns:
        :return:
        '''
        return self.get_column_headers_def() - drop_columns

    @staticmethod
    def util_process_historical_values(df, periodicity_key, start_index, col_name):
        '''
        it allows to get historical data
        :param df: data frame
        :param periodicity_key: historical data prameter
        :param start_index:
        :param col_name: column name to obtin historical data
        :return:
        '''
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
            , const.MQ135_COLUMN
            , const.MQ7_COLUMN
           # , const.MQ2_ERROR_COLUMN
           # , const.MQ7_ERROR_COLUMN
           # , const.MQ135_ERROR_COLUMN
            , const.MQ2_COLUMN
            , const.TEMPERATURE_COLUMN
        }
        return column_headers

    @staticmethod
    def get_combine_vars():
        column_headers = [
              const.HUMIDITY_COLUMN
            , const.MQ135_COLUMN
            , const.MQ7_COLUMN
           # , const.MQ2_ERROR_COLUMN
           # , const.MQ7_ERROR_COLUMN
          #  , const.MQ135_ERROR_COLUMN
            , const.MQ2_COLUMN
            , const.TEMPERATURE_COLUMN
        ]
        return column_headers

    @staticmethod
    def get_mq_vars():
        column_headers = [
                const.MQ135_COLUMN_NOW,
                const.MQ7_COLUMN_NOW,
                const.MQ2_COLUMN_NOW,
              #  const.TEMPERATURE_COLUMN_NOW,
               # const.HUMIDITY_COLUMN_NOW
            ]
        return column_headers

    @staticmethod
    def get_vars_now():
        column_headers = [
            const.MQ135_COLUMN_NOW,
            const.MQ7_COLUMN_NOW,
            const.MQ2_COLUMN_NOW,
            const.TEMPERATURE_COLUMN_NOW,
            const.HUMIDITY_COLUMN_NOW
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

