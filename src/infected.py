import numpy as np
import random
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                    )


class Infected:
    DEAD_PROB = 0.05
    INFECTION_PROB = 0.00005

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.infected = np.array([])
        self.dies = np.array([])
        self.daily_stats_infected = np.array([])

        #range for dead from 4 to 30, with IC (7, 25) 95%
        self.dead_range = [i for i in range(4, 31)]
        self.dead_ic_weight = [0.025 / 3] * 3 + [0.95 / 19] * 19 + [0.25 / 5] * 5

        #range for recovery from 7 to 40, with IC (10, 35) 95%
        self.recovery_range = [i for i in range(7, 41)]
        self.recovery_ic_weight = [0.025 / 3] * 3 + [0.95 / 26] * 26 + [0.025 / 5] * 5

    def probably_add_potential_infected(self, id, total_exposure_time):
        if 1 - random.random() <= Infected.INFECTION_PROB * total_exposure_time:
            self.add_infected(id)

    def add_infected(self, id):
        self.infected = np.append(self.infected, [{'id':id, 'days':1}])

    def increment_days(self):
        self.infected = np.array([{'id':x['id'], 'days':x['days'] + 1} for x in self.infected])

    def calculate_dies(self):
        self.logger.info('Calculate dies')
        total_dies = 0
        for x in self.infected:
            days_ic = random.choices(self.dead_range, weights=self.dead_ic_weight)[0]
            if x['days'] >= days_ic and 1 - random.random() <= Infected.DEAD_PROB:
                total_dies += 1
                self.dies = np.append(self.dies, [x['id']])
                self.infected = np.delete(self.infected, np.argwhere(self.infected == x['id']))

        self.logger.info('Total dies today {}  '.format(total_dies))

    def calculate_recovery(self):
        self.logger.info('Calculate recoveries')
        total_recovered = 0
        for x in self.infected:
            days_ic = random.choices(self.recovery_range, weights=self.recovery_ic_weight)[0]
            if x['days'] >= days_ic:
                total_recovered += 1
                self.infected = np.delete(self.infected, np.argwhere(self.infected == x['id']))

        self.logger.info('Total recovered today {}  '.format(total_recovered))

    def get_infected(self):
        return self.infected

    def get_dies(self):
        return self.dies

    def get_infected_ids(self):
        return [x['id'] for x in self.infected]
