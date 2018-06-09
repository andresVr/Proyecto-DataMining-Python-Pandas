from pandas import pandas


def clean_data():
    print("*******Formatting Scenario Start*******")
    print("mq135+temperature")
    df = pandas.read_csv('../data/data.csv')
    date_set = pandas.to_datetime(df.date, unit='s')
    df['cnv_date'] = date_set
    df = df.drop('date', 1)
    df = df.drop('node', 1)
    df = df.drop('location', 1)
    df = df.drop('humidity', 1)
    df = df.drop('mq2', 1)
    df = df.drop('mq7', 1)
    grouping_data_set = df.set_index('cnv_date').resample('T')['mq135','temperature'].mean()
    grouping_data_set['temperature'].fillna(0)
    clean_data_set = grouping_data_set
    clean_data_set.to_csv('../data/dataTemp.csv')
    print("*******Formatting Scenario 1 Done*******")


def define_scenario():
    df = pandas.read_csv('../data/dataTemp.csv')
    df['cnv_date'] = pandas.to_datetime(df.cnv_date, infer_datetime_format=True)
    sort_date = df.sort_values('cnv_date',0)
    print(sort_date)
if __name__ == "__main__":
    clean_data()
    define_scenario()