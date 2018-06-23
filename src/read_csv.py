from pandas import pandas
import numpy as np
from sklearn.model_selection import train_test_split
from pydash import py_


def read_data_set():
    data_frame = pandas.read_csv('../data/data.csv')

    # transform timestamp to datetime
    date_set = pandas.to_datetime(data_frame.date, unit='s')
    data_frame['cnv_date'] = date_set

    # create error_columns from mq135,7 and 2
    data_frame['mq2_error'] = data_frame.mq2 * 0.02
    data_frame['mq7_error'] = data_frame.mq7 * 0.02
    data_frame['mq135_error'] = data_frame.mq135 * 0.02
    return data_frame


def clean_data(interval, drop_columns):
    print("*******Formatting Scenario Start*******")

    # put base data set to clean
    df = read_data_set()

    # drop unnecessary columns
    df = util_drop_column(drop_columns,df)

    # group data using interval for T= hour, M= month, S= second
    grouping_data_set = df.set_index('cnv_date').resample(interval).mean()

    # change NaN (empty data) to 0
    if 'temperature' in df.columns:
        grouping_data_set['temperature'].fillna(0)

    if 'humidity' in df.columns:
        grouping_data_set['humidity'].fillna(0)

    # set group data to a new data frame to export
    clean_data_set = grouping_data_set

    # export to csv
    clean_data_set.to_csv('../data/dataTemp.csv')
    print("*******Formatting Scenario 1 Done*******")


def util_drop_column(col, df):
    result_process = lambda item: df.drop(col, 1)
    return result_process(col)


def util_create_scenery(df, historic_count,drop_columns):
    final = df
    for x in range(0,100):  # df['cnv_date'].size):
        final_data = util_scenario(df.iloc[x],df, x, historic_count,drop_columns)
        if x == 1:
            final = pandas.DataFrame(data=final_data)
            final = final.T
        else:
            final_tmp = pandas.DataFrame(data=final_data)
            final_tmp = final_tmp.T
            final = final.append(final_tmp)
    return final


def util_scenario(item,df, index, historic_count,drop_columns):
    scenery_columns = get_scenery_columns(drop_columns)
    historic_data_set = []

    def predicate(param): historic_data_set.append(util_process_historical_values(df, historic_count, index, param))
    py_.for_each(scenery_columns, predicate)
    historic_final_data = np.array([item['cnv_date']])

    for object_array in historic_data_set:
        aux_column = np.array(object_array)
        historic_final_data = np.concatenate((historic_final_data,aux_column))

    return historic_final_data


def get_scenery_columns(drop_columns):
    return get_column_headers_def() - drop_columns


def util_process_historical_values(df, periodicity_key,start_index,col_name):
    return df.iloc[start_index:periodicity_key + start_index][col_name].values


def add_previous_columns(drop_columns,periodicity_key, archive_name, test_size, historic_count,create_complete_csv):
    # read clean csv
    df = pandas.read_csv('../data/dataTemp.csv')

    # transform object to date
    df['cnv_date'] = pandas.to_datetime(df.cnv_date, infer_datetime_format=True)

    # order by date desc
    df = df.sort_values(by = 'cnv_date', ascending=0)

    # add time variable for historical values
    df[time_variables_columns(periodicity_key)] = previous_data_historic(periodicity_key,df)

    # create new data
    data = util_create_scenery(df,historic_count,drop_columns)

    # separate train and test data from an percentage parameter
    train, test = train_test_split(data, test_size=test_size)

    # create 2 csv files, from train and test
    test_path = '../data/results/' + archive_name + '_test' + '.csv'
    train_path = '../data/results/' + archive_name + '_train' + '.csv'

    train.to_csv(train_path,header=None,index=False)
    test.to_csv(test_path,header=None,index=False)

    # if you need complete data set
    if create_complete_csv:
        path = '../data/results/' + archive_name + '.csv'
        data.to_csv(path, header=None, index=False)

    print('*******Create Data Successfully*******')
    print('******* ' + archive_name + ' Done' + '*******')


def previous_data_historic(key,df):
    historic_frequency = {'m': df['cnv_date'].dt.minute,
                          'h': df['cnv_date'].dt.hour,
                          'M': df['cnv_date'].dt.month,
                          's': df['cnv_date'].dt.second,
                          'y': df['cnv_date'].dt.year}
    return historic_frequency.get(key)


def create_scenarios(interval, drop_columns, periodicity_key, archive_name,
                     test_size,historic_count, create_complete_csv):
    clean_data(interval, drop_columns)
    add_previous_columns(drop_columns,periodicity_key, archive_name, test_size, historic_count, create_complete_csv)


def time_variables_columns(key):
    time = {'m': "minute",
            'h': "hour}",
            'M': "month",
            's': "second",
            'y': "dt.year"}
    return time.get(key)


def get_column_headers_def():
    column_headers = {
        'date'
        ,'node'
        ,'location'
        ,'humidity'
        ,'mq2','mq7'
        ,'mq2_error'
        ,'mq7_error'
        , 'mq135_error'
        , 'mq135'
        ,'temperature'
    }
    return column_headers

if __name__ == "__main__":
    create_scenarios('T',{'date','node','location','humidity','mq2','mq7','mq2_error', 'mq7_error', 'mq135_error'},'m',
                     '1_scenery', 0.3, 8, False)
    create_scenarios('T',{'date','node','location','humidity','mq135','mq7','mq2_error', 'mq7_error', 'mq135_error'},'m',
                    '2_scenery', 0.3, 8, False)
    create_scenarios('T',{'date','node','location','humidity','mq135','mq2','mq2_error', 'mq7_error', 'mq135_error'},'m',
                    '3_scenery', 0.3, 8, False)
    # end first block
    create_scenarios('T',{'date', 'node', 'location', 'temperature', 'mq2', 'mq7', 'mq2_error', 'mq7_error', 'mq135_error'},
                     'm','4_scenery', 0.3, 8, False)
    create_scenarios('T',{'date','node','location','temperature','mq135','mq7','mq2_error', 'mq7_error', 'mq135_error'},'m',
                    '5_scenery', 0.3, 8, False)
    create_scenarios('T',{'date','node','location','temperature','mq135','mq2','mq2_error', 'mq7_error', 'mq135_error'},'m',
                    '6_scenery', 0.3, 8, False)
    # end
    create_scenarios('T',
                     {'date', 'node', 'location',  'mq2', 'mq7', 'mq2_error', 'mq7_error', 'mq135_error'},
                     'm', '7_scenery', 0.3, 8, False)
    create_scenarios('T', {'date', 'node', 'location', 'mq135', 'mq7', 'mq2_error', 'mq7_error',
                           'mq135_error'}, 'm',
                     '8_scenery', 0.3, 8, False)
    create_scenarios('T', {'date', 'node', 'location', 'mq135', 'mq2', 'mq2_error', 'mq7_error',
                           'mq135_error'}, 'm',
                     '9_scenery', 0.3, 8, False)



