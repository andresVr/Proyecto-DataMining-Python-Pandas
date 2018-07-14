from pandas import pandas
from pydash import py_
from sklearn.model_selection import train_test_split
from termcolor import colored
import constants as const
import util_scenery_creation as sc_util


class ReadData:

    def __init__(self):
        # util class object
        self.utility_scenery_function = sc_util.UtilitySceneryCreation.get_instance()

    def clean_data(self,interval, drop_columns, scenery_name):
        '''
        it allows to clean data to process
        :param interval: it is the grouping parameter for data
        :param drop_columns: it is a set of unnecessary columns
        :param scenery_name: scenery name file
        :return: nothing
        '''
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

    def add_previous_columns(self,drop_columns,periodicity_key, archive_name, test_size, historic_count,
                             create_complete_csv, prediction):
        '''
        formating data to create csv
        :param drop_columns: columns set to drop
        :param periodicity_key:
        :param archive_name:
        :param test_size:
        :param historic_count:
        :param create_complete_csv:
        :param prediction: prediction for scenery creation
        :return:
        '''
        # read clean csv
        df = pandas.read_csv('../data/dataTemp.csv')

        # transform object to date
        df[const.DATE_COLUMN] = pandas.to_datetime(df.cnv_date, infer_datetime_format=True)

        # order by date desc
        df = df.sort_values(by=const.DATE_COLUMN, ascending=const.ZERO)

        # add time variable for historical values
        df[self.time_variables_columns(periodicity_key)] = self.utility_scenery_function.previous_data_historic(periodicity_key,df)

        # create new data
        data = self.utility_scenery_function.util_create_scenery(df,historic_count,drop_columns,prediction)

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

            print(colored('*******Create Data Successfully*******','green'))
            print(colored('******* ' + archive_name + ' Done' + '*******','green'))

        else:
            print(colored('*******The scenery do not have mq variable to predict*******','red'))
            print(colored('******* ' + archive_name + ' Dropped' + '*******','red'))


    def create_scenario(self,interval, drop_columns, periodicity_key,
                        test_size,historic_count, create_complete_csv,prediction):
        '''
        it allows to separate archive name to the complete set
        :param interval: it is the grouping parameter for data
        :param drop_columns: drop columns data set
        :param periodicity_key: forward unit
        :param test_size: percentage test value (o-1)
        :param historic_count: historic data count
        :param create_complete_csv: flag that allows to create complete scenery
        or 2 separate files (test and train)
        :param prediction is a prediction var for scenery
        :return:
        '''
        # separate file name
        archive_name = py_.filter_(list(drop_columns), lambda item: py_.ends_with(item, const.SCENERY))[const.FIRST_INDEX]
        # identified drop columns set
        drop_columns = set(drop_columns) & set(self.utility_scenery_function.get_column_headers_def())
        # formatting scenery
        self.clean_data(interval, drop_columns,archive_name)
        # create scenery with temp formatting set
        self.add_previous_columns(drop_columns,periodicity_key, archive_name, test_size, historic_count,
                                  create_complete_csv, prediction)

    @staticmethod
    def time_variables_columns(key):
        '''
        time variables set
        :param key: key to get variable value
        :return:
        '''
        time = {const.MINUTE: const.MINUTE_TEXT,
                const.HOUR: const.HOUR_TEXT,
                const.MONTH: const.MONTH_TEXT,
                const.SECOND: const.SECOND_TEXT,
                const.YEAR: const.YEAR_TEXT}
        return time.get(key)

    def create_scenarios(self,interval, periodicity_key,historic_count,
                         test_size, create_complete_csv,scenery, prediction):
        '''
        function that create "N" scenarios
        :param interval:  it is the grouping parameter for data
        :param periodicity_key: forward unit
        :param historic_count: historic data count
        :param test_size: percentage test value (o-1)
        :param create_complete_csv: flag that allows to create complete scenery
        or 2 separate files (test and train)
        :param scenery: is a set of one or many scenery variables to drop
        :param prediction: is a prediction for scenery could be None
        '''

        # use a set of data for scenery creation
        drop_columns_data = scenery

        # predicate that allows to create scenarios
        def predicate(param): self.create_scenario(interval,param, periodicity_key, test_size,historic_count,
                                                   create_complete_csv,prediction)
        py_.for_each(drop_columns_data, predicate)

    def elements_comb_array(self):
        columns_data = {}
        array_elements = []
        for item in range(const.VARIABLES):
            array_elements_tmp = self.utility_scenery_function.comb(self.utility_scenery_function.get_combine_vars(),
                                                                    item+1)
            array_elements.extend(array_elements_tmp)

        for x in range(enumerate(array_elements)):
            archive_name = str(x) + '_scenery'
            array_elements[x].insert(const.FIRST_INDEX,'location')
            array_elements[x].insert(const.FIRST_INDEX,'node')
            array_elements[x].insert(const.FIRST_INDEX,'date')
            array_elements[x].insert(const.FIRST_INDEX, archive_name)
            columns_data.update({archive_name:array_elements[x]})

        return columns_data





