from pandas import pandas
import numpy as np
from sklearn.model_selection import train_test_split


def clean_data(interval, drop_columns):
    print("*******Formatting Scenario Start*******")
    df = pandas.read_csv('../data/data.csv')

    # transform timestamp to datetime
    date_set = pandas.to_datetime(df.date, unit='s')
    df['cnv_date'] = date_set

    # create error_columns from mq135,7 and 2
    df['mq2_error'] = df.mq2 * 0.02
    df['mq7_error'] = df.mq7 * 0.02
    df['mq135_error'] = df.mq135 * 0.02

    # drop unnecessary columns
    df = util_drop_column(drop_columns,df)

    # group data using interval for T= hour, M= month, S= second
    grouping_data_set = df.set_index('cnv_date').resample(interval)['mq135','temperature'].mean()

    # change NaN (empty data) to 0
    grouping_data_set['temperature'].fillna(0)

    # set group data to a new data frame to export
    clean_data_set = grouping_data_set

    # export to csv
    clean_data_set.to_csv('../data/dataTemp.csv')
    print("*******Formatting Scenario 1 Done*******")


def util_drop_column(col, df):
    result_process = lambda item: df.drop(col, 1)
    return  result_process(col)


def util_create_scenery(df, historic_count):
    final = None
    for x in range(1,100):  # df['cnv_date'].size):
        final_data = util_scenario(df.iloc[x],df, x, historic_count)
        if x == 1:
            final = pandas.DataFrame(data=final_data)
            final = final.T
        else:
            final_tmp = pandas.DataFrame(data=final_data)
            final_tmp = final_tmp.T
            final = final.append(final_tmp)
    return final


def util_scenario(item,df, index, historic_count):
    historic_data_set = util_process_historical_values(df, historic_count, index, 'mq135')
    historic_data_set_2 = util_process_historical_values(df, historic_count, index, 'temperature')
    historic_data_set = np.concatenate(([item['cnv_date']],historic_data_set))
    historic_final_data = np.concatenate((historic_data_set, historic_data_set_2))
    return historic_final_data


def util_process_historical_values(df, periodicity_key,start_index,col_name):
    return df.iloc[start_index:periodicity_key + start_index][col_name].values


def add_previous_columns(periodicity_key, archive_name, test_size, historic_count):
    # read clean csv
    df = pandas.read_csv('../data/dataTemp.csv')

    # transform object to date
    df['cnv_date'] = pandas.to_datetime(df.cnv_date, infer_datetime_format=True)

    # order by date desc
    df = df.sort_values(by = 'cnv_date', ascending=0)

    # add time variable for historical values
    df[time_variables_columns(periodicity_key)] = previous_data_historic(periodicity_key,df)

    # create new data
    data = util_create_scenery(df,historic_count)

    # separate train and test data from an percentage parameter
    train, test = train_test_split(data, test_size=test_size)

    # create 2 csv files, from train and test
    test_path = '../data/results/' + archive_name + '_test' + '.csv'
    train_path = '../data/results/' + archive_name + '_train' + '.csv'
    path = '../data/results/' + archive_name + '.csv'

    train.to_csv(train_path,header=None,index=False)
    test.to_csv(test_path,header=None,index=False)
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


def create_scenarios(interval, drop_columns, periodicity_key, archive_name, test_size,historic_count):
    clean_data('T', ['date', 'node', 'location', 'humidity', 'mq2', 'mq7'])
    add_previous_columns('m', 'first_scenery', test_size, historic_count)


def time_variables_columns(key):
    time = {'m': "minute",
            'h': "hour}",
            'M': "month",
            's': "second",
            'y': "dt.year"}
    return time.get(key)

if __name__ == "__main__":
    create_scenarios('T',['date','node','location','humidity','mq2','mq7'],'m','first_scenery', 0.3, 8)
