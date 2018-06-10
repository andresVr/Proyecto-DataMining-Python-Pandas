from pandas import pandas


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


def add_previous_columns():
    # read clean csv
    df = pandas.read_csv('../data/dataTemp.csv')

    # transform object to date
    df['cnv_date'] = pandas.to_datetime(df.cnv_date, infer_datetime_format=True)

    # order by date desc
    df = df.sort_values(by = 'cnv_date', ascending=0)

    df['minute'] = df['cnv_date'].dt.minute
    df['hour'] = df['cnv_date'].dt.hour
    df['month'] = df['cnv_date'].dt.month
    df['second'] = df['cnv_date'].dt.second
    df['year'] = df['cnv_date'].dt.year


    print(df)
    #print(previous_date)

def previous_data_frecuency(key):
    historic_frecuency = {'m': "df['cnv_date'].dt.minute",
                          'h':"df['cnv_date'].dt.hour}",
                          'M':"df['cnv_date'].dt.month",
                          's': "df['cnv_date'].dt.second",
                          'y': "df['cnv_date'].dt.year"}

if __name__ == "__main__":

    clean_data('T',['date','node','location','humidity','mq2','mq7'])
    add_previous_columns()