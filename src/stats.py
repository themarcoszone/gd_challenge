import logging
import numpy as np
import pandas as pd
from src.storage import Storage

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                    )

BUCKET='marcos-tn'
class Stats:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.daily_stats_infected = np.array([])
        self.daily_stats_geoloc_evts = np.array([])

        self.storage = Storage(BUCKET)

    def add_daily_infecteds(self, day, tot_infected):
        self.logger.info('Total infected day {} are {}'.format(day, tot_infected))
        self.daily_stats_infected = np.append(self.daily_stats_infected,
                                              [{'day': day, 'total_infected': tot_infected}])

    def add_daily_geoloc_evts(self, day, geo_loc_evts):
        self.daily_stats_geoloc_evts = np.append(self.daily_stats_geoloc_evts,
                                                [{'day': day, 'geo_evts': geo_loc_evts}])

    def get_daily_infecteds(self):
        return self.daily_stats_infected

    def get_daily_geoloc_evts(self):
        return self.daily_stats_geoloc_evts

    def save_daily_infected_stats(self, path):

        self.logger.info(f'{self.daily_stats_infected}')
        pd_daily_infected_stats = Stats.pandas_formated_df(self.daily_stats_infected,
                                                           *self.daily_stats_infected[0].keys())

        self.storage.upload_str_to_file(pd_daily_infected_stats.to_csv(), f'{path}')

    def save_geo_loc_evts_stats(self, path):
        geo_evts = [ge['geo_evts'] for ge in self.daily_stats_geoloc_evts]
        sub_tot_evts_by_coord = [evts for sublist in geo_evts for evts in sublist]

        tot_evts_by_coord_df = pd.DataFrame(sub_tot_evts_by_coord) \
            .groupby(['latitude', 'longitude'])['tot_evts'].sum()

        self.storage.upload_str_to_file(tot_evts_by_coord_df.to_csv(), f'{path}')

    @staticmethod
    def pandas_formated_df(raw_data, *cols):
        data = {}
        for col in cols:
            print(col)
            data[col] = [d[col] for d in raw_data]

        pd_df = pd.DataFrame(data, columns=[col for col in cols])
        return pd_df
