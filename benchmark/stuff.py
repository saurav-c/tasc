import matplotlib.pyplot as plt
import numpy as np

def plotWrites():
	# fig = plt.figure()
	# ax = fig.add_axes([0,0,1,1])
	writes = [3.097, 0.752]
	wErr = [(0, 0), [39.680 - writes[0], 4.076-writes[1]]]
	store = ('DynanoDB', 'AFTSI')
	ind = np.arange(2)
	colors = ['red','blue']
	barList = plt.bar(ind, writes, yerr=wErr, color=colors)
	#plt.yscale('log')
	plt.title('Write Latency')
	plt.xticks(ind, store)
	# plt.yticks(np.arange(0, 100, 10))
	for i in range(len(writes)):
		plt.annotate(str(writes[i]), xy=(ind[i],writes[i]))

	plt.ylabel('Latency (ms)')

	plt.savefig('write1.png')


def plotReads():
	# fig = plt.figure()
	# ax = fig.add_axes([0,0,1,1])
	writes = [3.755, 16.550]
	wErr = [(0, 0), [40.812 - writes[0], 54.802-writes[1]]]
	store = ('DynanoDB', 'AFTSI')
	ind = np.arange(2)
	colors = ['red','blue']
	barList = plt.bar(ind, writes, yerr=wErr, color=colors)
	#plt.yscale('log')
	plt.title('Read Latency')
	plt.xticks(ind, store)
	# plt.yticks(np.arange(0, 100, 10))
	for i in range(len(writes)):
		plt.annotate(str(writes[i]), xy=(ind[i],writes[i]))

	plt.ylabel('Latency (ms)')

	plt.savefig('read1.png')

def commit():
	# fig = plt.figure()
	# ax = fig.add_axes([0,0,1,1])
	writes = [6.467, 37.541]
	wErr = [(0, 0), [45.326 - writes[0], 123.335-writes[1]]]
	store = ('DynanoDB', 'AFTSI')
	ind = np.arange(2)
	colors = ['red','blue']
	barList = plt.bar(ind, writes, yerr=wErr, color=colors)
	#plt.yscale('log')
	plt.title('Committed Transaction Latency')
	plt.xticks(ind, store)
	# plt.yticks(np.arange(0, 100, 10))
	for i in range(len(writes)):
		plt.annotate(str(writes[i]), xy=(ind[i],writes[i]))

	plt.ylabel('Latency (ms)')

	plt.savefig('commit1.png')



plotWrites()
plotReads()
commit()