from pandas import pandas


def clean_data(interval, drop_columns):
    print("*******Formatting Scenario Start*******")
    df = pandas.read_csv('../data/data.csv')

    # transform timestamp to datetime
    date_set = pandas.to_datetime(df.date, unit='s')
    df['cnv_date'] = date_set

    # drop unnecessary columns
    for column in drop_columns:
        df = df.drop(column, 1)

    # group data using interval for T= hour, M= month, S= second
    grouping_data_set = df.set_index('cnv_date').resample(interval)['mq135','temperature'].mean()

    # change NaN (empty data) to 0
    grouping_data_set['temperature'].fillna(0)

    # set group data to a new data frame to export
    clean_data_set = grouping_data_set

    # export to csv
    clean_data_set.to_csv('../data/dataTemp.csv')
    print("*******Formatting Scenario 1 Done*******")


def define_scenario():
    # read clean csv
    df = pandas.read_csv('../data/dataTemp.csv')

    # transform object to date
    df['cnv_date'] = pandas.to_datetime(df.cnv_date, infer_datetime_format=True)

    # order by date desc
    sort_date = df.sort_values(by = 'cnv_date', ascending=0)

    print(sort_date)


if __name__ == "__main__":

    clean_data('10T', ['date','node','location','humidity','mq2','mq7'])

    define_scenario()