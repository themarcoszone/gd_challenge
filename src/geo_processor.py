import logging
import sobol
import random

import numpy as np

from pyspark.sql.types import StructType, StructField, FloatType

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                    )

EXPOSURE_DISTANCE = 10


class GEOProcessor:
    def __init__(self, utils):
        self.utils = utils
        self.logger = logging.getLogger(__name__)
        self.initial_sample_inds = 100
        self.random_normalized_coordinates_df = self.gen_random_normalized_coordinates_df()

    def get_initial_plane(self, df):
        df.createOrReplaceTempView('plane')
        query_str = '''
                SELECT MIN(latitude) min_lat, MAX(latitude) max_lat, MIN(longitude) min_lon, MAX(longitude) max_lon
                FROM plane
                WHERE  iso_date = '2020-06-01'
        '''
        self.plane_coordinates = self.utils.sqlContext.sql(query_str)
        self.utils.sqlContext.dropTempTable('plane')
        res_df = list(map(lambda row: row.asDict(), self.plane_coordinates.collect()))[0]

        self.logger.info('This is the map {}'.format(res_df))

    def get_initial_plane_v2(self, df):
        df.createOrReplaceTempView('plane')
        query_str = '''
                        SELECT latitude
                        FROM plane
                        --WHERE iso_date = '2020-06-01'
                '''
        latitudes_df = self.utils.sqlContext.sql(query_str)
        latitudes_dict = list(map(lambda row: row.asDict(), latitudes_df.collect()))
        latitudes = [l['latitude'] for l in latitudes_dict]

        latitude_interval = (np.percentile(latitudes, 35), np.percentile(latitudes, 65))
        query_str = '''
                                SELECT longitude
                                FROM plane
                                WHERE  iso_date = '2020-06-01'
                                --AND latitude >= {} AND latitude <= {}
                        '''.format(latitude_interval[0], latitude_interval[1])

        longitude_df = self.utils.sqlContext.sql(query_str)
        longitudes_dict = list(map(lambda row: row.asDict(), longitude_df.collect()))
        longitudes = [l['longitude'] for l in longitudes_dict]

        longitude_interval = (np.percentile(longitudes, 35), np.percentile(longitudes, 65))

        schema = StructType([StructField('min_lat', FloatType(), True),
                             StructField('max_lat', FloatType(), True),
                             StructField('min_lon', FloatType(), True),
                             StructField('max_lon', FloatType(), True)])
        plane = [(float(latitude_interval[0]), float(latitude_interval[1])
                  ,float(longitude_interval[0]), float(longitude_interval[1]))]

        self.plane_coordinates = self.utils.sqlContext.createDataFrame(plane, schema, verifySchema=True)
        self.logger.info('This is the map {}'.format(plane))

        self.utils.sqlContext.dropTempTable('plane')

    def gen_random_normalized_coordinates_df(self):
        normalized_coordinates = [(float(x1),float(x2)) for x1, x2 in sobol.sample(dimension=2, n_points=self.initial_sample_inds,
                                                                                   skip=int(1 + random.random() * 20000))]

        schema = StructType([StructField('x', FloatType(), True),
                            StructField('y', FloatType(), True)])

        self.logger.info('Calculating random normalized coordinates')
        return self.utils.sqlContext.createDataFrame(normalized_coordinates, schema, verifySchema=True)

    def denormalize_random_coordinates(self):
        self.random_normalized_coordinates_df.createOrReplaceTempView('random_normalized')
        self.plane_coordinates.createOrReplaceTempView('plane')

        query_str = '''
                SELECT ROUND(min_lat + (max_lat - min_lat) * (rn.y/2 + 0.5), 7) as rand_lat
                    ,ROUND(min_lon + (max_lon - min_lon) * (rn.x/2 + 0.5), 7) as rand_lon
                FROM random_normalized rn
                CROSS JOIN plane
        '''

        random_coordinates_df = self.utils.sqlContext.sql(query_str)
        random_coordinates_list = list(map(lambda row: row.asDict(),
                                           random_coordinates_df.select(random_coordinates_df['rand_lat'],
                                                                        random_coordinates_df['rand_lon']).collect()))

        #self.logger.info('random coord {}'.format(random_coordinates_list))
        self.utils.sqlContext.dropTempTable('random_normalized')
        self.utils.sqlContext.dropTempTable('plane')
        self.logger.info('Random coordinates on plane was generated')

        return random_coordinates_list

    def search_closest_indinviduals_to_rnd_coordinates(self, df, rnd_pts):
        df.createOrReplaceTempView('all_coordinates')
        uniform_distributed_ids = ['']
        for coord in rnd_pts:
            query_str = '''
                    SELECT *
                        ,2 * 6371000 * ASIN(SQRT(POWER(SIN(({0} * ACOS(-1) / 180  - ac.latitude * ACOS(-1) / 180 )/2),2)
                                            + COS({0} * ACOS(-1) / 180  )
                                            *COS(ac.latitude * ACOS(-1) / 180  )
                                            * POWER(SIN(({1} * ACOS(-1) / 180   - ac.longitude * ACOS(-1) / 180  )/2),2))
                                            ) as distance
                    FROM all_coordinates ac
            '''.format(coord['rand_lat'], coord['rand_lon'])
            self.utils.sqlContext.sql(query_str).createOrReplaceTempView('distance_to_rnd_point')

            query_str = '''
                    SELECT  id
                    FROM distance_to_rnd_point
                    WHERE id not in {}
                    ORDER BY distance ASC
                    LIMIT 1
            '''.format("('')" if len(uniform_distributed_ids) == 0 else tuple(uniform_distributed_ids) + ('',))

            nearest_individuals_to_rnd_point_df =  self.utils.sqlContext.sql(query_str)
            nearests_id = list(map(lambda row: row.asDict(), nearest_individuals_to_rnd_point_df.select('id').collect()))
            id = nearests_id[0]['id']
            self.logger.info('Calculating closest individuals to random coordinates')
            uniform_distributed_ids += [id]

        uniform_distributed_ids.remove('')
        self.utils.sqlContext.dropTempTable('all_coordinates')
        self.utils.sqlContext.dropTempTable('distance_to_rnd_point')

        return uniform_distributed_ids

    def get_random_individuals(self, raw_df):
        #self.get_initial_plane(raw_df)
        self.get_initial_plane_v2(raw_df)
        random_coordinates = self.denormalize_random_coordinates()

        uniform_distributed_ids = self.search_closest_indinviduals_to_rnd_coordinates(raw_df, random_coordinates)

        return uniform_distributed_ids

    def search_close_contact(self, df1, df2, infected_inds_ids):
        df1.createOrReplaceTempView('table1')
        df2.createOrReplaceTempView('table2')
        query_str='''
                SELECT  gz.id i1, w2.id i2
                        ,struct(gz.latitude as latitude, gz.longitude as longitude) as i1_position
                        ,gz.track_position_counter as track_position_counter_i1
                        ,w2.track_position_counter as track_position_counter_i2
                        ,struct(w2.latitude as latitude, w2.longitude as longitude) as i2_position
                        ,2 * 6371000 * ASIN(SQRT(POWER(SIN((gz.latitude * ACOS(-1) / 180   - w2.latitude * ACOS(-1) / 180  )/2),2) + COS(gz.latitude * ACOS(-1) / 180  )*COS( w2.latitude * ACOS(-1) / 180  )* POWER(SIN((gz.longitude * ACOS(-1) / 180   - w2.longitude * ACOS(-1) / 180  )/2),2))) as distance
                        ,struct(gz.start_position_time as start_position_time, gz.end_position_time as end_position_time) i1_time
                        ,struct(w2.start_position_time as start_position_time, w2.end_position_time as end_position_time) i2_time
                    ,CASE 
                        WHEN gz.end_position_time < w2.start_position_time THEN 0
                        WHEN w2.end_position_time <  gz.start_position_time THEN 0
                        ELSE (unix_timestamp(LEAST(gz.end_position_time, w2.end_position_time))
                        	- unix_timestamp(GREATEST(gz.start_position_time, w2.start_position_time)) ) / (60)
                    END as exposure_time
                FROM table1 gz, table2 w2
                WHERE 2 * 6371000 * ASIN(SQRT(POWER(SIN((gz.latitude * ACOS(-1) / 180   - w2.latitude * ACOS(-1) / 180  )/2),2) + COS(gz.latitude * ACOS(-1) / 180  )*COS( w2.latitude * ACOS(-1) / 180  )* POWER(SIN((gz.longitude * ACOS(-1) / 180   - w2.longitude * ACOS(-1) / 180  )/2),2))) <= {}
                AND gz.id != w2.id 
                AND w2.id NOT IN {}
        '''.format(EXPOSURE_DISTANCE, tuple(infected_inds_ids))

        self.logger.info('Searching close contact with infected')
        closed_contacts_df = self.utils.sqlContext.sql(query_str)
        self.utils.sqlContext.dropTempTable('table1')
        self.utils.sqlContext.dropTempTable('table2')

        return closed_contacts_df
