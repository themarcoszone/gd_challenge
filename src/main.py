import logging
from src.Utils import Utils
from src.geo_processor import GEOProcessor
from src.individual import Individual
from src.infected import Infected
from src.stats import Stats
import numpy as np

BASE_PATH='gs://marcos-tn/resources/covid_test/'
PARTITIONS_PATHS=['gs://marcos-tn/resources/covid_test/iso_date=2020-06-01/'
                ,'gs://marcos-tn/resources/covid_test/iso_date=2020-06-02/'
                  ,'gs://marcos-tn/resources/covid_test/iso_date=2020-06-03/'
                  ,'gs://marcos-tn/resources/covid_test/iso_date=2020-06-04/'
                  ,'gs://marcos-tn/resources/covid_test/iso_date=2020-06-05/'
                  ,'gs://marcos-tn/resources/covid_test/iso_date=2020-06-06/'
                  ,'gs://marcos-tn/resources/covid_test/iso_date=2020-06-07/'
                  ,'gs://marcos-tn/resources/covid_test/iso_date=2020-06-08/'
                  ,'gs://marcos-tn/resources/covid_test/iso_date=2020-06-09/'
                  ,'gs://marcos-tn/resources/covid_test/iso_date=2020-06-10/'
                  ,'gs://marcos-tn/resources/covid_test/iso_date=2020-06-11/'
                  ,'gs://marcos-tn/resources/covid_test/iso_date=2020-06-12/'
                  ,'gs://marcos-tn/resources/covid_test/iso_date=2020-06-13/'
                  ,'gs://marcos-tn/resources/covid_test/iso_date=2020-06-14/'
                  ,'gs://marcos-tn/resources/covid_test/iso_date=2020-06-15/']

'''
BASE_PATH='/home/truenorth/Documents/ex/covid-test'
PARTITIONS_PATHS=['/home/truenorth/Documents/ex/covid-test/iso_date=2020-06-01/'
            ,'/home/truenorth/Documents/ex/covid-test/iso_date=2020-06-02/']
'''
partitions=['2020-06-01','2020-06-02','2020-06-03','2020-06-04','2020-06-05','2020-06-06','2020-06-07','2020-06-08'
            ,'2020-06-09','2020-06-10','2020-06-11','2020-06-12','2020-06-13','2020-06-14','2020-06-15','2020-06-16'
            '2020-06-17','2020-06-18','2020-06-19','2020-06-20','2020-06-21']

RESULT_PATH = 'gs://marcos-tn/resources/covid_test/'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                    )

ITERATIONS = 15
class Main:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.utils = utils = Utils()
        self.raw_df = utils.read_parquet(BASE_PATH, *PARTITIONS_PATHS)

        self.geo_processor = GEOProcessor(self.utils)
        self.individual = Individual(self.utils)
        self.infected = Infected()
        self.stats = Stats()

    def run(self):

        day_0_df = self.get_day(partitions[0])
        random_individuals_ids = self.geo_processor.get_random_individuals(day_0_df)
        random_individuals_ids = list(random_individuals_ids)

        for id in random_individuals_ids[0:self.geo_processor.initial_sample_inds]:
            self.infected.add_infected(id)

        #Add stats for wave 0
        w0_df = self.individual \
            .get_individuals_behaviour_by_id(day_0_df, self.infected.get_infected_ids())

        wn_dict = list(map(lambda row: row.asDict(), w0_df.select('*').collect()))
        geo_points_evts = [{'latitude': p['latitude'],
                            'longitude': p['longitude'],
                            'tot_evts': p['tot_evts']} for p in wn_dict]
        self.add_stats(0, self.infected.get_infected(), geo_points_evts)


        for day in range(1, ITERATIONS):
            self.infected.increment_days()
            day_n_df = self.get_day(partitions[day])
            #WAVE N -1
            individual_behaviour_prev_wave_df = self.individual\
                .get_individuals_behaviour_by_id(day_n_df, self.infected.get_infected_ids())

            #WAVE N
            individual_behaviour_wn_df = self.individual.behaviour(day_n_df)

            wn_dict = list(map(lambda row: row.asDict(), individual_behaviour_prev_wave_df.select('*').collect()))
            geo_points_wave_n = np.array([{'id':p['id'],
                                           'latitude': p['latitude'],
                                           'longitude': p['longitude'],
                                           'tot_evts': p['tot_evts']} for p in wn_dict])

            close_contact_individuals_df = \
                self.geo_processor.search_close_contact(individual_behaviour_prev_wave_df
                                                        ,individual_behaviour_wn_df
                                                        ,self.infected.get_infected_ids())

            #new_potential_infected_id = self.individual.get_exposure(close_contact_individuals_df)

            new_potential_infected_id = self.individual.get_non_continous_exposure(close_contact_individuals_df)

            self.logger.info('There are new potential infected with exposure time {}'.format(new_potential_infected_id))
            for x in new_potential_infected_id:
                self.infected.probably_add_potential_infected(x['id'], x['total_exposure_seconds'])

            #FIX TO GET JUST LAT LON AND COUNT
            #VER DE USAR NUMPY PARA MEJORAR EFICIENCIA
            geo_points_evts = np.array([{'latitude': gp['latitude'],
                                         'longitude': gp['longitude'],
                                         'tot_evts': gp['tot_evts']} for gp in geo_points_wave_n if gp['id'] in self.infected.get_infected_ids()])

            self.infected.calculate_dies()
            self.infected.calculate_recovery()

            self.add_stats(day, self.infected.get_infected(), geo_points_evts)

        self.logger.info('Infected info {}'.format(self.stats.get_daily_infecteds()))
        #self.logger.info('Geoloc info {}'.format(self.stats.get_daily_geoloc_evts()[:1]))

        self.stats.save_daily_infected_stats('resources/covid_test/results/daily_infected.csv')
        self.stats.save_geo_loc_evts_stats('resources/covid_test/results/daily_geoloc_evts.csv')

    def add_stats(self, day,infected_ids, geo_point_evts):
        self.stats.add_daily_infecteds(day, len(infected_ids))

        self.stats.add_daily_geoloc_evts(day, geo_point_evts)

    def get_day(self, partition):
        self.raw_df.createOrReplaceTempView('raw_data')
        query_str='''
            SELECT * 
            FROM raw_data
            WHERE iso_date='{}'
            '''.format(partition)

        day_df = self.utils.sqlContext.sql(query_str)
        self.utils.sqlContext.dropTempTable('raw_data')
        self.logger.info('Got data for day {}'.format(partition))
        return day_df

    def get_info(self):
        self.raw_df.createOrReplaceTempView('raw_data')
        query_str = '''
            SELECT iso_date, COUNT(*)
            FROM raw_data
            GROUP BY iso_date
        '''
        day_df = self.utils.sqlContext.sql(query_str)
        self.logger.info('Dataset partitioned by iso_date info')
        day_df.show(20, False)


if __name__ == '__main__':
    main = Main()
    main.run()