import matplotlib.pyplot as plt
import pandas as pd
import argparse


class LineChart:
    def __init__(self):
        pass

    def read_data(self, path, agg_row=None, *cols):
        data = pd.read_csv(path)
        if agg_row:
            expanded_data = data.reindex(data.index.repeat(data[agg_row]))
        else:
            expanded_data = data

        return expanded_data

    def run(self, path):
        data = self.read_data(path)
        print(data)
        Year = [1920, 1930, 1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010]
        Unemployment_Rate = [9.8, 12, 8, 7.2, 6.9, 7, 6.5, 6.2, 5.5, 6.3]

        plt.plot(data['day'], data['total_infected'])
        plt.title('Total Infecteds By Day')
        plt.xlabel('Day')
        plt.ylabel('Infected')
        plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-inp', '--input_path', default=None, action="store", type=str,
                        help='Specify file input path')
    args = parser.parse_args()

    lc = LineChart()
    lc.run(args.input_path)