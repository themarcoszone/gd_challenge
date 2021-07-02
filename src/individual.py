import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                    )

EXPOSURE_TIME = 30


class Individual:
    def __init__(self, utils):
        self.logger = logging.getLogger(__name__)
        self.utils = utils

    def behaviour(self, df):
        df.createOrReplaceTempView('movements')

        #AGREGAR CANTIDAD DE EVENTOS POR POSICION
        query_str='''
            SELECT id, latitude, longitude, MIN(CAST(`timestamp` AS TIMESTAMP)) as start_position_time, COUNT(*) AS tot_evts 
                    , MAX(CAST(`timestamp` AS TIMESTAMP)) as end_position_time
            FROM ( 
                select ROW_NUMBER() over (PARTITION BY id, cast(latitude as string), cast(longitude as string) order by timestamp),
                ROW_NUMBER() over (PARTITION BY id order by `timestamp`) - ROW_NUMBER() over (PARTITION BY id, cast(latitude as string), cast(longitude as string) order by timestamp) as group_time_position
                ,*
                FROM movements 
            )
            GROUP BY id, group_time_position, latitude, longitude
        '''

        individual_behaviour = self.utils.sqlContext.sql(query_str)
        individual_behaviour = self.add_row_id_partition_by_id_ordered_by_start_position_time(individual_behaviour)

        self.logger.info('Getting individual behaviour')
        self.utils.sqlContext.dropTempTable('movements')
        return individual_behaviour

    def get_individuals_by_id(self, df, ids):
        df.createOrReplaceTempView('individual')
        query_str='''
            SELECT * 
            FROM individual
            WHERE id IN {}
        '''.format(tuple(ids))

        individuals_df = self.utils.sqlContext.sql(query_str)
        self.utils.sqlContext.dropTempTable('individual')
        self.logger.info('Getting individual by id')
        return individuals_df

    def add_row_id_partition_by_id_ordered_by_start_position_time(self, df):
        df.createOrReplaceTempView('add_row_id')
        query_str='''
            SELECT ROW_NUMBER() OVER (PARTITION BY id ORDER BY start_position_time) as track_position_counter, *
            FROM add_row_id
        '''

        df_with_row_id = self.utils.sqlContext.sql(query_str)
        self.utils.sqlContext.dropTempTable('add_row_id')
        return df_with_row_id

    def count_time_close(self,df):
        df.createOrReplaceTempView('close_time')
        query_str='''
                SELECT * 
                    ,LAG(track_position_counter_i1) OVER (PARTITION BY i1, i2 ORDER BY track_position_counter_i1, track_position_counter_i2) as prev_track_position_counter_i1
                    ,LAG(track_position_counter_i2) OVER (PARTITION BY i1, i2 ORDER BY track_position_counter_i2,track_position_counter_i1) prev_track_position_counter_i2
                    ,IF(COALESCE(ABS(LAG(track_position_counter_i1) OVER (PARTITION BY i1, i2 ORDER BY track_position_counter_i1,track_position_counter_i2) - track_position_counter_i1),0) <= 1
                        AND COALESCE(ABS(LAG(track_position_counter_i2) OVER (PARTITION BY i1, i2 ORDER BY track_position_counter_i2, track_position_counter_i1) - track_position_counter_i2),0) <= 1
                        ,1
                        ,0)  as consecutive_close
                FROM close_time 
                WHERE exposure_time > 0 
        '''

        close_time_df = self.utils.sqlContext.sql(query_str)
        self.utils.sqlContext.dropTempTable('close_time')
        return close_time_df

    def group_consecutives_time_ranges(self, df):
        df.createOrReplaceTempView('how_much_time_are_close')
        query_str='''
                SELECT *
                    ,ROW_NUMBER() OVER (PARTITION BY i1, i2 ORDER BY track_position_counter_i1,track_position_counter_i2)
                    ,ROW_NUMBER() OVER (PARTITION BY i1, i2, consecutive_close ORDER BY track_position_counter_i1, track_position_counter_i2)
                    ,ROW_NUMBER() OVER (PARTITION BY i1, i2 ORDER BY track_position_counter_i1, track_position_counter_i2)
                        - IF(consecutive_close=1, ROW_NUMBER() OVER (PARTITION BY i1, i2, consecutive_close ORDER BY track_position_counter_i1, track_position_counter_i2),0) as group_consecutive_time
                FROM how_much_time_are_close
        '''

        consecutive_time_grouped = self.utils.sqlContext.sql(query_str)
        self.utils.sqlContext.dropTempTable('how_much_time_are_close')
        self.logger.info('grouping time toghether between indivs')
        return consecutive_time_grouped

    def total_consecutive_time(self, df):
        df.createOrReplaceTempView('group_consecutives_time_ranges')
        query_str='''
                SELECT i1, i2, COUNT(*) as consecutive_times, SUM(exposure_time) as total_exposure_time
                FROM group_consecutives_time_ranges
                GROUP BY i1, i2, group_consecutive_time
        '''

        total_consecutive_time_df = self.utils.sqlContext.sql(query_str)
        self.logger.info('Calculating total consecutive time together between individuals')

        self.utils.sqlContext.dropTempTable('how_much_time_are_close')
        return total_consecutive_time_df

    def total_non_continuous_time(self, df):
        df.createOrReplaceTempView('group_consecutives_time_ranges')
        query_str = '''
                        SELECT i1, i2, SUM(exposure_time) as total_exposure_time
                        FROM group_consecutives_time_ranges
                        GROUP BY i1, i2
                '''

        total_not_continuous_time_df = self.utils.sqlContext.sql(query_str)
        self.utils.sqlContext.dropTempTable('how_much_time_are_close')
        self.logger.info('Getting non consecutive time together')

        return total_not_continuous_time_df

    def get_potential_exposure(self, df):
        df.createOrReplaceTempView('potential_exposure')
        query_str= '''
                SELECT i2 as id, total_exposure_time * 60 as total_exposure_seconds
                FROM potential_exposure
                WHERE total_exposure_time >= {}
        '''.format(EXPOSURE_TIME)

        potential_exposure_id_df = self.utils.sqlContext.sql(query_str)
        self.utils.sqlContext.dropTempTable('potential_exposure')

        self.logger.info('Getting potential exposure')
        return potential_exposure_id_df

    def get_exposure(self, df):
        close_time_df = self.count_time_close(df)
        consecutive_time_grouped_df = self.group_consecutives_time_ranges(close_time_df)

        total_consecutive_time_df = self.total_consecutive_time(consecutive_time_grouped_df)

        potential_exposure_id = self.get_potential_exposure(total_consecutive_time_df)
        exposed_individuals_id = list(map(lambda row: row.asDict(), potential_exposure_id.select('id','total_exposure_seconds').collect()))

        return exposed_individuals_id

    #This case not consider a need to be 30 mins continuous near to other person
    def get_non_continous_exposure(self, df):
        close_time_df = self.count_time_close(df)
        total_not_contibuous_time_df = self.total_non_continuous_time(close_time_df)

        potential_exposure_id = self.get_potential_exposure(total_not_contibuous_time_df)
        exposed_individuals_id = list(map(lambda row: row.asDict(), potential_exposure_id.select('id','total_exposure_seconds').collect()))

        return exposed_individuals_id

    def get_individuals_behaviour_by_id(self, df, ids):
        individuals_df = self.get_individuals_by_id(df, ids)
        return self.behaviour(individuals_df)
