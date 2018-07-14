import read_csv as sc_util
import constants as const


def specific_scenery():
    '''
    use this function to define an particular scenery construction
    :return: a collection of particular scenery
    '''
    # {'scenery'(scenery name): {'scenery'(scenery name), 'date'(do not erase), 'node' (do not erase),
    # 'location' (do not erase), (put all variable to drop in the new scenery, separate by ",")'humidity',
    # 'temperature'}}
    return {'scenery': {'scenery', 'date', 'node', 'location', 'humidity', 'temperature'}}

if __name__ == "__main__":
    # T = 1 minute interval, m = minute backward, 0.3= % test, 8 = historic data count backward, False= if you
    # do not require complete data set to csv

    try:
         create_scenario_obj = sc_util.ReadData()
        # use this code line if you need the complete scenery list and a variable to predict
        # create_scenario_obj.create_scenarios('180T', 'm', 7, 0.3, True,create_scenario_obj.elements_comb_array(),
        #                                     const.MQ2_COLUMN_NOW)

        # use this code line if you need the complete scenery list and not need a variable to predict
        # DO NOT FORGET TO PUT INDENT LINE
        # create_scenario_obj.create_scenarios('180T', 'm', 7, 0.3, True,create_scenario_obj.elements_comb_array(),
        #                                     None)

        # use this code line if you need an specific scenery and a variable to predict
         create_scenario_obj.create_scenarios('180T', 'm', 7, 0.3, True, specific_scenery(),
                                              const.MQ2_COLUMN_NOW)
    except:
        print("****Not enough data to process, SCENERY PROCESS DONE***")
    #elements_comb_array()

