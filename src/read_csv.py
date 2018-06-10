from pandas import pandas
import numpy as np

def clean_data(interval, drop_columns):
    print("*******Formatting Scenario Start*******")
    df = pandas.read_csv('../data/data.csv')

    # transform timestamp to datetime
    date_set = pandas.to_datetime(df.date, unit='s')
    df['cnv_date'] = date_set

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


def util_create_scenary(df):
    final = None
    for x in range (1,3000):
        final_data = util_scenario(df.iloc[x],df, x)
        if x == 1:
            final = pandas.DataFrame(data=final_data)
            final = final.T
        else:
            final_tmp = pandas.DataFrame(data=final_data)
            final_tmp = final_tmp.T
            final = final.append(final_tmp)
    return final

def util_scenario(item,df, index):
    historic_data_set = util_process_historical_values(df, 30, index, 'mq135')
    historic_data_set_2 = util_process_historical_values(df, 30, index, 'temperature')
    historic_data_set = np.concatenate(([item['cnv_date']],historic_data_set))
    historic_final_data = np.concatenate((historic_data_set, historic_data_set_2))
    return historic_final_data

def util_process_historical_values(df, periodicity_key,start_index,col_name):
    return df.iloc[start_index:periodicity_key + start_index][col_name].values



def add_previous_columns(periodicity_key):
    # read clean csv
    df = pandas.read_csv('../data/dataTemp.csv')

    # transform object to date
    df['cnv_date'] = pandas.to_datetime(df.cnv_date, infer_datetime_format=True)

    # order by date desc
    df = df.sort_values(by = 'cnv_date', ascending=0)

    # add time variable for historical values
    df[time_variables_columns(periodicity_key)] = previous_data_historic(periodicity_key,df)

    # create scenary
    scenary = util_create_scenary(df)
    scenary.to_csv('../data/finalData.csv')
    print(scenary)



def previous_data_historic(key,df):
    historic_frequency = {'m': df['cnv_date'].dt.minute,
                          'h': df['cnv_date'].dt.hour,
                          'M': df['cnv_date'].dt.month,
                          's': df['cnv_date'].dt.second,
                          'y': df['cnv_date'].dt.year}
    return historic_frequency.get(key)


def time_variables_columns(key):
    time = {'m': "minute",
            'h': "hour}",
            'M': "month",
            's': "second",
            'y': "dt.year"}
    return time.get(key)

if __name__ == "__main__":

    clean_data('T',['date','node','location','humidity','mq2','mq7'])
    add_previous_columns('m')