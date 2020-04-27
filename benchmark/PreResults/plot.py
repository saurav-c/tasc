# import matplotlib.pyplot as plt
# import numpy as np


# fig = plt.figure()
# ax = fig.add_axes([0,0,1,1])
# writes = [3.097, 0.752]
# wErr = [(0, 0), [39.680 - writes[0], 4.076-writes[1]]]
# store = ['DynanoDB', 'AFTSI']

# ind = np.arange(2)
# colors = ['red','blue']

# barList = ax.bar(ind, writes, yerr=wErr, color=colors)
# ax.set_yscale('log')

# rects = ax.patches

# # Make some labels.
# labels = [str(x) for x in writes]

# topLabel = [str(x) for x in wErr]

# def autolabel(bars,peakval):
#     for ii,bar in enumerate(bars):
#         height = bars[ii]
#         plt.text(ind[ii], height-5, '%s'% (peakval[ii]), ha='center', va='bottom')
# autolabel(writes,labels) 

# # for rect, label in zip(rects, labels):
# #     height = rect.get_height()
# #     ax.text(rect.get_x() + rect.get_width() / 2, height - 3, label,
# #             ha='center', va='bottom')

# # for rect, label in zip(rects, labels):
# #     height = rect.get_height()
# #     ax.text(rect.get_x() + rect.get_width() / 2, height - 3, label,
# #             ha='center', va='bottom')
            
# plt.show()