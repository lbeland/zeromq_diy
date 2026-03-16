import math
from matplotlib import colors
from matplotlib.gridspec import GridSpec
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.axes_grid1 import make_axes_locatable
import os
# import pyqtgraph as pg
# from PyQt5.QtWidgets import QApplication


results_folder = "rt_results"
send_files = [f for f in os.listdir(results_folder) if "send_times" in f]
recv_files = [f for f in os.listdir(results_folder) if "recv_times" in f]
latency_files = [f for f in os.listdir(results_folder) if "process_times" in f]

fig, axes = plt.subplots(3, 1, sharey=True,sharex=True)
fig.suptitle("Zeromq DIY with Python RT - Performance Over Time", fontsize=14, fontweight='bold')
axes = np.atleast_1d(axes).ravel()

for idx, x in enumerate(zip([latency_files, send_files, recv_files], ["Latency", "Send Period", "Receive Period"])):
    files, label = x
    ax = axes[idx]
    ax.set_title(label)
    ax.set_ylabel("Time (μs)")
    ax.set_xlabel("Message Index")
    ax.set_yscale('log')
    ax.set_ylim(0.1, 5000)

    # create histogram axis to the right
    divider = make_axes_locatable(ax)
    ax_hist = divider.append_axes("right", size="20%", pad=0.1, sharey=ax)

    for file in sorted(files):
        
        n_channels, msg_size, node, _ = file.split('_')
        if int(n_channels) == 20 and int(msg_size) >= 1024:
            print(f"Processing file: {file} | Channels: {n_channels} | Msg Size: {msg_size} ")

            data = np.loadtxt(os.path.join(results_folder, file), skiprows=0, delimiter=',')
            ax.plot(data[:]*1e6, linewidth=0.5,label=f"Ch {n_channels} | Msg {msg_size}")

            # Use logarithmic bins for better distribution when data spans multiple orders of magnitude
            data_us = data[:]*1e6
            bins = np.logspace(np.log10(max(data_us.min(), 0.01)), np.log10(data_us.max()), 20)
            n,bins,_ = ax_hist.hist(data_us, bins=bins, histtype='step',orientation='horizontal')
            ax_hist.set_xlabel("Frequency")
            ax_hist.set_yscale('log')
            # ax_hist.set_xscale('log')
            # ax_hist.grid(True, which="both", alpha=0.3)


axes[0].legend(loc="upper right")
plt.show()

