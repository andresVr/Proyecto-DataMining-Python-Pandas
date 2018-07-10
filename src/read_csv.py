from pandas import pandas
from pydash import py_
from sklearn.model_selection import train_test_split

import constants as const
import util_scenery_creation as sc_util


class ReadData:

    def __init__(self):
        self.utility_scenery_function = sc_util.UtilitySceneryCreation.get_instance()

    def clean_data(self,interval, drop_columns, scenery_name):
        print("*******Formatting Scenario Start*******")

        # put base data set to clean
        df = self.utility_scenery_function.put_data_set()

        # drop unnecessary columns
        df = self.utility_scenery_function.util_drop_column(drop_columns,df)

        # group data using interval for T= minute, M= month, S= second, H= hour
        grouping_data_set = df.set_index(const.DATE_COLUMN).resample(interval).mean()

        # change NaN (empty data) to 0
        if const.TEMPERATURE in df.columns:
            grouping_data_set[const.TEMPERATURE].fillna(const.ZERO)

        if const.HUMIDITY in df.columns:
            grouping_data_set[const.HUMIDITY].fillna(const.ZERO)

        # set group data to a new data frame to export
        clean_data_set = grouping_data_set

        # export to csv
        clean_data_set.to_csv('../data/dataTemp.csv')
        print("*******Formatting Scenario" + scenery_name + "Done*******")


    def add_previous_columns(self,drop_columns,periodicity_key, archive_name, test_size, historic_count,create_complete_csv):
        # read clean csv
        df = pandas.read_csv('../data/dataTemp.csv')

        # transform object to date
        df[const.DATE_COLUMN] = pandas.to_datetime(df.cnv_date, infer_datetime_format=True)

        # order by date desc
        df = df.sort_values(by=const.DATE_COLUMN, ascending=const.ZERO)

        # add time variable for historical values
        df[self.time_variables_columns(periodicity_key)] = self.utility_scenery_function.previous_data_historic(periodicity_key,df)

        # create new data
        data = self.utility_scenery_function.util_create_scenery(df,historic_count,drop_columns)

        if data is not None:
            # separate train and test data from an percentage parameter
            train, test = train_test_split(data, test_size=test_size)

            # create 2 csv files, from train and test
            test_path = '../data/results/' + archive_name + '_test' + '.csv'
            train_path = '../data/results/' + archive_name + '_train' + '.csv'

            if not create_complete_csv:
                train.to_csv(train_path, index=False)
                test.to_csv(test_path,index=False)

            # if you need complete data set
            if create_complete_csv:
                path = '../data/results/' + archive_name + '.csv'
                data.to_csv(path, index=False)

            print('*******Create Data Successfully*******')
            print('******* ' + archive_name + ' Done' + '*******')

        else:
            print('*******The scenery do not have mq variable to predict*******')
            print('******* ' + archive_name + ' Dropped' + '*******')


    def create_scenario(self,interval, drop_columns, periodicity_key,
                        test_size,historic_count, create_complete_csv):
        archive_name = py_.filter_(list(drop_columns), lambda item: py_.ends_with(item, const.SCENERY))[const.FIRST_INDEX]
        drop_columns = set(drop_columns) & set(self.utility_scenery_function.get_column_headers_def())
        self.clean_data(interval, drop_columns,archive_name)
        self.add_previous_columns(drop_columns,periodicity_key, archive_name, test_size, historic_count, create_complete_csv)


    def time_variables_columns(self,key):
        time = {const.MINUTE: const.MINUTE_TEXT,
                const.HOUR: const.HOUR_TEXT,
                const.MONTH: const.MONTH_TEXT,
                const.SECOND: const.SECOND_TEXT,
                const.YEAR: const.YEAR_TEXT}
        return time.get(key)

    def create_scenarios(self,interval, periodicity_key,historic_count,
                         test_size, create_complete_csv,scenery):


        #drop_columns_data = elements_comb_array()
        drop_columns_data = scenery

        def predicate(param): self.create_scenario(interval,param, periodicity_key, test_size,historic_count, create_complete_csv)
        py_.for_each(drop_columns_data, predicate)

    def specific_scenery(self):
        return {'scenery': {'scenery', 'date', 'node', 'location', 'humidity','temperature'}}

    def elements_comb_array(self):
        columns_data = {}
        array_elements = []
        for item in range(const.VARIABLES):
            array_elements_tmp = self.utility_scenery_function.comb(self.utility_scenery_function.get_combine_vars(),
                                                                    item+1)
            array_elements.extend(array_elements_tmp)

        for x in range(len(array_elements)):
            archive_name = str(x) + '_scenery'
            array_elements[x].insert(const.FIRST_INDEX,'location')
            array_elements[x].insert(const.FIRST_INDEX,'node')
            array_elements[x].insert(const.FIRST_INDEX,'date')
            array_elements[x].insert(const.FIRST_INDEX, archive_name)
            columns_data.update({archive_name:array_elements[x]})

        return columns_data





