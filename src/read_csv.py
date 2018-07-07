from pandas import pandas
from sklearn.model_selection import train_test_split
from pydash import py_
import constants as const
import util_scenery_creation as sc_util


utility_scenery_function = sc_util.UtilitySceneryCreation.get_instance()


def clean_data(interval, drop_columns, scenery_name):
    print("*******Formatting Scenario Start*******")

    # put base data set to clean
    df = utility_scenery_function.put_data_set()

    # drop unnecessary columns
    df = utility_scenery_function.util_drop_column(drop_columns,df)

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


def add_previous_columns(drop_columns,periodicity_key, archive_name, test_size, historic_count,create_complete_csv):
    # read clean csv
    df = pandas.read_csv('../data/dataTemp.csv')

    # transform object to date
    df[const.DATE_COLUMN] = pandas.to_datetime(df.cnv_date, infer_datetime_format=True)

    # order by date desc
    df = df.sort_values(by = const.DATE_COLUMN, ascending=const.ZERO)

    # add time variable for historical values
    df[time_variables_columns(periodicity_key)] = utility_scenery_function.previous_data_historic(periodicity_key,df)

    # create new data
    data = utility_scenery_function.util_create_scenery(df,historic_count,drop_columns)

    # separate train and test data from an percentage parameter
    train, test = train_test_split(data, test_size=test_size)

    # create 2 csv files, from train and test
    test_path = '../data/results/' + archive_name + '_test' + '.csv'
    train_path = '../data/results/' + archive_name + '_train' + '.csv'

    train.to_csv(train_path, index=False)
    test.to_csv(test_path,index=False)

    # if you need complete data set
    if create_complete_csv:
        path = '../data/results/' + archive_name + '.csv'
        data.to_csv(path, index=False)

    print('*******Create Data Successfully*******')
    print('******* ' + archive_name + ' Done' + '*******')


def create_scenario(interval, drop_columns, periodicity_key,
                    test_size,historic_count, create_complete_csv):
    archive_name = py_.filter_(list(drop_columns), lambda item: py_.ends_with(item, const.SCENERY))[const.FIRST_INDEX]
    drop_columns = set(drop_columns) & set(utility_scenery_function.get_column_headers_def())
    clean_data(interval, drop_columns,archive_name)
    add_previous_columns(drop_columns,periodicity_key, archive_name, test_size, historic_count, create_complete_csv)


def time_variables_columns(key):
    time = {const.MINUTE: const.MINUTE_TEXT,
            const.HOUR: const.HOUR_TEXT,
            const.MONTH: const.MONTH_TEXT,
            const.SECOND: const.SECOND_TEXT,
            const.YEAR: const.YEAR_TEXT}
    return time.get(key)


def create_scenarios(interval, periodicity_key,historic_count,
                     test_size, create_complete_csv,scenary):


    #drop_columns_data = elements_comb_array()
    drop_columns_data = scenary

    def predicate(param): create_scenario(interval,param, periodicity_key, test_size,historic_count, create_complete_csv)
    py_.for_each(drop_columns_data, predicate)


def specific_scenery():
    return {'scenery': {'scenery', 'date', 'node', 'location', 'humidity', 'mq2_error', 'mq7_error',
                  'mq135_error','temperature'}}


def elements_comb_array():
    columns_data = {}
    array_elements = []
    for item in range(const.VARIABLES):
        array_elements_tmp = utility_scenery_function.comb(utility_scenery_function.get_combine_vars(),item+1)
        array_elements.extend(array_elements_tmp)

    for x in range(len(array_elements)):
        archive_name = str(x) + '_scenery'
        array_elements[x].insert(const.FIRST_INDEX,'location')
        array_elements[x].insert(const.FIRST_INDEX,'node')
        array_elements[x].insert(const.FIRST_INDEX,'date')
        array_elements[x].insert(const.FIRST_INDEX, archive_name)
        columns_data.update({archive_name:array_elements[x]})

    return columns_data


if __name__ == "__main__":
    # T = 1 minute interval, m = minute backward, 0.3= % test, 8 = historic data count backward, False= if you
    # do not require complete data set to csv

    try:
        create_scenarios('16T', 'm', 7, 0.3, True,specific_scenery())
    except:
        print("Not enough data to process")
    #elements_comb_array()


