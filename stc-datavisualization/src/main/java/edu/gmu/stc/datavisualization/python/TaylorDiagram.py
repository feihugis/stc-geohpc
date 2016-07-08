#!/usr/bin/env python
# Copyright: This document has been placed in the public domain.

"""
Taylor diagram (Taylor, 2001) test implementation.
http://www-pcmdi.llnl.gov/about/staff/Taylor/CV/Taylor_diagram_primer.htm
"""

__version__ = "Time-stamp: <2012-02-17 20:59:35 ycopin>"
__author__ = "Yannick Copin <yannick.copin@laposte.net>"

import numpy as NP
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as PLT
import json
import sys


class TaylorDiagram(object):
    """Taylor diagram: plot model standard deviation and correlation
    to reference (data) sample in a single-quadrant polar plot, with
    r=stddev and theta=arccos(correlation).
    """

    def __init__(self, refstd, fig=None, rect=111, label='_', max_std=None):
        """Set up Taylor diagram axes, i.e. single quadrant polar
        plot, using mpl_toolkits.axisartist.floating_axes. refstd is
        the reference standard deviation to be compared to.
        """

        from matplotlib.projections import PolarAxes
        import mpl_toolkits.axisartist.floating_axes as FA
        import mpl_toolkits.axisartist.grid_finder as GF

        self.refstd = refstd  # Reference standard deviation

        tr = PolarAxes.PolarTransform()

        # Correlation labels
        rlocs = NP.concatenate((NP.arange(10) / 10., [0.95, 0.99]))
        tlocs = NP.arccos(rlocs)  # Conversion to polar angles
        gl1 = GF.FixedLocator(tlocs)  # Positions
        tf1 = GF.DictFormatter(dict(zip(tlocs, map(str, rlocs))))

        # Standard deviation axis extent
        self.smin = 0
        self.smax = 1.5 * self.refstd

        if max_std is not None:
            self.smax = 1.1 * max_std

        ghelper = FA.GridHelperCurveLinear(tr,
                                           extremes=(0, NP.pi / 2,  # 1st quadrant
                                                     self.smin, self.smax),
                                           grid_locator1=gl1,
                                           tick_formatter1=tf1,
                                           )

        if fig is None:
            fig = PLT.figure()

        ax = FA.FloatingSubplot(fig, rect, grid_helper=ghelper)
        fig.add_subplot(ax)

        # Adjust axes
        ax.axis["top"].set_axis_direction("bottom")  # "Angle axis"
        ax.axis["top"].toggle(ticklabels=True, label=True)
        ax.axis["top"].major_ticklabels.set_axis_direction("top")
        ax.axis["top"].label.set_axis_direction("top")
        ax.axis["top"].label.set_text("Correlation")

        ax.axis["left"].set_axis_direction("bottom")  # "X axis"
        ax.axis["left"].label.set_text("Standard deviation")

        ax.axis["right"].set_axis_direction("top")  # "Y axis"
        ax.axis["right"].toggle(ticklabels=True)
        ax.axis["right"].major_ticklabels.set_axis_direction("left")

        ax.axis["bottom"].set_visible(False)  # Useless

        # Contours along standard deviations
        ax.grid(False)

        self._ax = ax  # Graphical axes
        self.ax = ax.get_aux_axes(tr)  # Polar coordinates

        # Add reference point and stddev contour
        print "Reference std:", self.refstd
        l, = self.ax.plot([0], self.refstd, 'k*',
                          ls='', ms=10, label=label)
        t = NP.linspace(0, NP.pi / 2)
        r = NP.zeros_like(t) + self.refstd
        self.ax.plot(t, r, 'k--', label='_')

        # Collect sample points for latter use (e.g. legend)
        self.samplePoints = [l]

    def add_sample(self, stddev, corrcoef, *args, **kwargs):
        """Add sample (stddev,corrcoeff) to the Taylor diagram. args
        and kwargs are directly propagated to the Figure.plot
        command."""

        l, = self.ax.plot(NP.arccos(corrcoef), stddev,
                          *args, **kwargs)  # (theta,radius)
        self.samplePoints.append(l)

        return l

    def add_contours(self, levels=5, **kwargs):
        """Add constant centered RMS difference contours."""

        rs, ts = NP.meshgrid(NP.linspace(self.smin, self.smax),
                             NP.linspace(0, NP.pi / 2))
        # Compute centered RMS difference
        rms = NP.sqrt(self.refstd ** 2 + rs ** 2 - 2 * self.refstd * rs * NP.cos(ts))

        contours = self.ax.contour(ts, rs, rms, levels, **kwargs)

        return contours


def main(inputpath, x_axis, y_axis, output):
    input = open(inputpath)
    json_data = json.load(input)

    sample_legend = []
    sample_values = []
    sample_std_cor = []

    ref_values = NP.array(json_data[0]['values'])
    ref_std = NP.array(json_data[0]['std'])
    ref_legend = json_data[0]['legendName']

    x = range(1, len(ref_values) + 1)

    max_std = 0.0

    for i in range(1, len(json_data)):
        sample_legend.append(json_data[i]['legendName'])
        sample_values.append(NP.array(json_data[i]['values']))
        sample_std_cor.append([json_data[i]['std'], json_data[i]['correlation']])
        if json_data[i]['std'] > max_std:
            max_std = json_data[i]['std']

    fig = PLT.figure(figsize=(20, 6))

    ax1 = fig.add_subplot(1, 2, 1, xlabel=x_axis, ylabel=y_axis)
    # Taylor diagram
    dia = TaylorDiagram(ref_std, fig=fig, rect=122, label=ref_legend, max_std=max_std)

    colors = PLT.matplotlib.cm.jet(NP.linspace(0, 1, len(json_data)))

    ax1.plot(x, ref_values, 'ko', label=ref_legend)
    for i, m in enumerate(sample_values):
        ax1.plot(x, m, c=colors[i], label=sample_legend[i])
    ax1.legend(numpoints=1, prop=dict(size='small'), loc='lower right', bbox_to_anchor=(-0.1, 0.825))


    # Add samples to Taylor diagram
    for i, (stddev, corrcoef) in enumerate(sample_std_cor):
        dia.add_sample(stddev, corrcoef, marker='s', ls='', c=colors[i],
                       label=sample_legend[i])

    # Add RMS contours, and label them
    contours = dia.add_contours(colors='0.5')
    PLT.clabel(contours, inline=1, fontsize=10)

    # Add a figure legend
    fig.legend(dia.samplePoints,
               [p.get_label() for p in dia.samplePoints],
               numpoints=1, prop=dict(size='small'),bbox_to_anchor=(0.92,1))

    PLT.savefig(output)
    # PLT.show()


if __name__ == '__main__':
    print sys.argv[1] + '***************'
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    #main('/Users/feihu/Desktop/taylordiagram.json', 'precipitation', 'values(mm/d)','/Users/feihu/Desktop/taylordiagram-reanalysis.png')
