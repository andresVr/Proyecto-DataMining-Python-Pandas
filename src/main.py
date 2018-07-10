import read_csv as sc_util

if __name__ == "__main__":
    # T = 1 minute interval, m = minute backward, 0.3= % test, 8 = historic data count backward, False= if you
    # do not require complete data set to csv

    try:
        create_scenario_obj = sc_util.ReadData()
        create_scenario_obj.create_scenarios('10T', 'm', 3, 0.3, True,create_scenario_obj.elements_comb_array())
    except:
        print("****Not enough data to process, SCENERY PROCESS DONE***")
    #elements_comb_array()