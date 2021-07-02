import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import argparse

class Histogram3d:
    BINS = 10

    def __init__(self):
        fig = plt.figure()
        self.ax = fig.add_subplot(projection='3d')

    def read_data(self, path, agg_row=None, *cols):
        data = pd.read_csv(path)
        if agg_row:
            expanded_data = data.reindex(data.index.repeat(data[agg_row]))
        else:
            expanded_data = data

        return expanded_data

    def get_values_ranged(self, df, bins, col):
        max = df[col].max()
        min = df[col].min()
        scale = (max - min) / bins
        bins_vals = [x for x in np.arange(min, max, scale)]
        inds_scale = np.digitize(df[col].to_numpy(), bins_vals)

        values = np.array([])
        for n in range(inds_scale.size):
            values = np.append(values, bins_vals[inds_scale[n] - 1])

        return values

    def run(self, path):
        data = self.read_data(path, 'tot_evts')
        lat_values = self.get_values_ranged(data, Histogram3d.BINS, 'latitude')
        lon_values = self.get_values_ranged(data, Histogram3d.BINS, 'longitude')

        hist, xedges, yedges = np.histogram2d(lat_values, lon_values, bins=Histogram3d.BINS,
                                              range=[[lat_values.min(), lat_values.max()], [lon_values.min(), lon_values.max()]])

        self.graph(hist, xedges, yedges)

    def graph(self, hist, xedges, yedges):
        xpos, ypos = np.meshgrid(xedges[:-1] + 0.2, yedges[:-1] + 0.2, indexing="ij")

        xpos = xpos.ravel()
        ypos = ypos.ravel()

        zpos = 0

        dx = 0.2 * np.ones_like(zpos)
        dy = 0.2 * np.ones_like(zpos)
        dz = hist.ravel()

        self.ax.bar3d(xpos, ypos, zpos, dx, dy, dz, zsort='average')
        self.ax.set_zlabel('N events')
        plt.xlabel('Latitude')
        plt.ylabel('Longitude')
        plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-inp', '--input_path', default=None, action="store", type=str,
                        help='Specify file input path')
    args = parser.parse_args()

    h3d = Histogram3d()
    h3d.run(args.input_path)