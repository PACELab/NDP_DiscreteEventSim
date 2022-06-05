import random
import numpy as np
import pandas as pd
import simpy
import sys
import time
import math
import json
import gc

# Simulation Params
RANDOM_SEED = 1568
LOADING_TIME = 50000000
SETTLING_TIME = lambda : 10*LOADING_TIME
SIM_TIME = lambda : LOADING_TIME + SETTLING_TIME()

# Global
COMPUTE_NUM_MACHNIES = 3500
STORAGE_NUM_MACHNIES = 400
NUM_BLOCKS = 600 #not used


# Machine specs
COMPUTE_NUM_CORES  = 2
COMPUTE_TOT_MEMORY = 192*1024 # in MB
COMPUTE_CLOCK_FREQ = 2.7e9 # Hz
# c5.metal	$4.08	96	192 GiB	EBS Only	25 Gigabit


EFFICIENCY_FACTOR = 1
STORAGE_NUM_CORES  = 4
STORAGE_TOT_MEMORY = 512*1024 # in MB
STORAGE_CLOCK_FREQ = 2.666e9 # Hz
# i3.metal	$4.992	64	512 GiB	8 x 1900 NVMe SSD	25 Gigabit


# Tasks inter arrival time
INIT_TASKS = 1
T_INTER = 100000 # Seconds
NUM_OPERATIONS = 3
PARTITION_SIZE = 256 # MB
CYC_PER_BYTE = 5
# PARTITION_CYCLES = lambda: (PARTITION_SIZE/1024)*CYC_PER_BYTE*1e9
NUM_PARTITIONS = 600

DECOMP_RF = 60 #0.2
RF1 = 1
RF2 = 10000
# INFLATION_FACTOR = 10


# Network Specs
CONCURRENT_CONNECTIONS = 3
WAN_BANDWIDTH = 400*1000/8 # MBps
# WAN_LATENCY = 0.010 # Seconds
# LAN_BANDWIDTH = 100*1000/8 # MBps
# LAN_LATENCY = 0.001 # Seconds

# MTU = 1500/1024/1024 #1500 Bytes
MTU = 5 #MB



columns_part = ["Job", "Cut", "Part", "sto_arrive", "sto_start", "sto_end", "sto_machine", "sto_cyc", 
			"sto_inp", "net_start", "net_end", "net_bytes", "com_arrive", "com_start", "com_end", 
			"com_machine", "com_cyc", "com_inp", "net2_start", "net2_end", "net2_bytes"]
timings_part = pd.DataFrame(columns=columns_part)

columns_job = ["Job","com_cluster_queue","sto_cluster_queue","network_queue","Cut","Arrival_Time",
				"Net_1_join","Net_1_start","Net_end","End_Time"]
timings_job = pd.DataFrame(columns=columns_job)


def multiplyList(myList) :
	 
	# Multiply elements one by one
	result = 1
	for x in myList:
		 result = result * x
	return result

class Machine():
	def __init__(self, env, id, mtype):
		self.env = env
		self.name = id
		self.type = mtype
		self.queuedrainrate = math.inf

		if mtype=="com":
			self.cores = simpy.Resource(env, COMPUTE_NUM_CORES)
			self.clockfreq = COMPUTE_CLOCK_FREQ
			self.memory = simpy.Container(env, COMPUTE_TOT_MEMORY ,COMPUTE_TOT_MEMORY)

		if mtype=="sto":
			self.cores = simpy.Resource(env, STORAGE_NUM_CORES)
			self.clockfreq = STORAGE_CLOCK_FREQ * EFFICIENCY_FACTOR
			self.memory = simpy.Container(env, STORAGE_TOT_MEMORY ,STORAGE_TOT_MEMORY)

	def compute(self, op):
		yield self.env.timeout(op.cycles/self.clockfreq)

		self.set_queuedrainrate(op.cycles/self.clockfreq)

	def set_queuedrainrate(self, new_val):
		if self.queuedrainrate == math.inf:
			self.queuedrainrate = 1/new_val
		self.queuedrainrate = 0.4*(1/new_val) + 0.6*self.queuedrainrate

def transfer(env,transfer_size,job_net_queue,parent_job):
	transfer_size_remaining = transfer_size
	# mess_id = 1
	# start = env.now
	with job_net_queue.request() as req:
		yield req
		yield env.process(parent_job.network_connection_start(env))
		while(transfer_size_remaining>0):
			with network_queue.request() as request:
				yield request #blocking
				
				data_size = min(transfer_size_remaining,MTU)
				yield env.timeout(data_size/WAN_BANDWIDTH)
				
				transfer_size_remaining -= data_size #blocking
	

class Operation:
	def __init__(self,id,blk_id,inp_size,dior,inflation_factor,cycles=None):
		self.name = id
		self.blk_id = blk_id
		self.inp_size = inp_size
		self.dior = dior
		
		if cycles is not None:
			self.cycles = cycles
		else:
			self.cycles = (self.inp_size/1024)*CYC_PER_BYTE*1e9*inflation_factor
		
		self.out_size = self.inp_size/self.dior
		
	def next_op_inp_size(self):
		return self.out_size



class Partition:
	def __init__(self,id,parent_job,inp_size,blk_id,num_ops):
		self.name = id
		self.parent_job = parent_job
		self.init_operations = []
		
		for x in range(int(num_ops)):
			if(x==0):
				new_op = Operation(x,blk_id,inp_size,DECOMP_RF,5)
			if(x==1):
				new_op = Operation(x,blk_id,self.init_operations[x-1].next_op_inp_size(),RF1,1)
			if(x==2):
				new_op = Operation(x,blk_id,self.init_operations[x-1].next_op_inp_size(),RF2,20)

			self.init_operations.append(new_op)

		

	def run(self, env, part_entry, job_net_queue, barrier, part_network_barrier):
		# part_entry["Part"] = self.name

		# execute sto op
		if self.op_sto != None:
			# part_entry["sto_arrive"] = env.now
			with central_queues["sto"].request() as req:
				yield req

				machine = best_machine("sto")
				# part_entry["sto_machine"] = machine.name
				with machine.cores.request() as request:
					yield request			

					# part_entry["sto_start"] = env.now	
					yield env.process(machine.compute(self.op_sto))
					# part_entry["sto_end"] = env.now
					# part_entry["sto_cyc"] = self.op_sto.cycles
					# part_entry["sto_inp"] = self.op_sto.inp_size

					# TODO: Memory usage
					# yield machine.memory.get(part.memory)
					# yield machine.memory.put(part.memory)
				

		# network delay
		if self.op_com != None:
			# part_entry["net_start"] = env.now
			# part_entry["net_bytes"] = self.op_com.inp_size
			
			yield env.process(transfer(env,self.op_com.inp_size,job_net_queue,self.parent_job))
			part_network_barrier.put(1)

			# part_entry["net_end"] = env.now

		# execute com op
		if self.op_com != None:
			# part_entry["com_arrive"] = env.now
			with central_queues["com"].request() as req:
				yield req

				machine = best_machine("com")
				# part_entry["com_machine"] = machine.name
				with machine.cores.request() as request:
					yield request			

					# part_entry["com_start"] = env.now
					yield env.process(machine.compute(self.op_com))
					# part_entry["com_end"] = env.now
					# part_entry["com_cyc"] = self.op_com.cycles
					# part_entry["com_inp"] = self.op_com.inp_size
					
					# TODO: Memory usage
					# yield machine.memory.get(part.memory)
					# yield machine.memory.put(part.memory)

		# Small results
		if self.op_com == None:
			# part_entry["net2_start"] = env.now
			# part_entry["net2_bytes"] = self.op_sto.out_size
			
			# with network_connection_queue.request() as request:
			# 	yield request
				
			yield env.process(transfer(env,self.op_sto.out_size,job_net_queue,self.parent_job))
			part_network_barrier.put(1)
			
			# part_entry["net2_end"] = env.now

		# timings_part.loc[len(timings_part)] = part_entry

		barrier.put(1)

def fill_cluster_status(job_entry):
	job_entry["com_cluster_queue"] = len(central_queues["com"].queue)/COMPUTE_NUM_MACHNIES
	job_entry["sto_cluster_queue"] = len(central_queues["sto"].queue)/STORAGE_NUM_MACHNIES
	job_entry["network_queue"] = len(network_connection_queue.queue)


class Job():
	def __init__(self,id,cut=None):
		self.name = id
		self.job_entry = {}
		fill_cluster_status(self.job_entry)

		# each partition
		inp_size = PARTITION_SIZE
		num_ops = NUM_OPERATIONS
		num_parts = NUM_PARTITIONS

		blk_ids = random.sample(range(NUM_BLOCKS),num_parts)
		self.partitions = [Partition(x,self,inp_size,blk_ids[x],num_ops) for x in range(num_parts)]

		if cut!=None:
			self.cut = cut
		else:
			self.cut = best_cut(self)
		
		print("====",self.cut)

		self.aggregate_operations()
		
		self.network_connection_queue_request = None

	def aggregate_operations(self, cut=None):	
		if cut==None:
			cut = self.cut
		
		for part in self.partitions:
			ops_sto = part.init_operations[:cut]
			ops_com = part.init_operations[cut:]

			if cut == 0:
				part.op_sto = None				
			else:
				# part.op_sto = Operation(0  , 0, sum([x.cycles for x in ops_sto]),
				# 						ops_sto[0].inp_size,
				# 						ops_sto[0].blk_id,
				# 						dior=multiplyList([x.dior for x in ops_sto]))

				part.op_sto = Operation(0, 
										ops_sto[0].blk_id, 
										ops_sto[0].inp_size,
										multiplyList([x.dior for x in ops_sto]),
										0,
										cycles=sum([x.cycles for x in ops_sto])
										)

			if cut == NUM_OPERATIONS:
				part.op_com = None
			else:
				# part.op_com = Operation(cut, 0, sum([x.cycles for x in ops_com]),
				# 						ops_com[0].inp_size,
				# 						ops_com[0].blk_id,
				# 						dior=multiplyList([x.dior for x in ops_com]))
				part.op_com = Operation(cut,
										ops_com[0].blk_id,
										ops_com[0].inp_size,
										multiplyList([x.dior for x in ops_com]),
										0,
										cycles=sum([x.cycles for x in ops_com])
										)
			# del part.init_operations

		if part.op_com == None:
			self.transfer_part_size = part.op_sto.out_size
		else:
			self.transfer_part_size = part.op_com.inp_size

	def network_connection_start(self,env):
		if self.network_connection_queue_request == None:			
			self.job_entry["Net_1_join"] = env.now
			
			self.network_connection_queue_request =  network_connection_queue.request()
			self.network_connection_queue_request.payload = self
			yield self.network_connection_queue_request
			
			self.job_entry["Net_1_start"] = env.now
			
			self.first_part_transfer_done = 1


	def run(self,env):
		job_entry = self.job_entry
		job_entry["Job"] = self.name
		job_entry["Cut"] = self.cut
		job_entry["Arrival_Time"] = env.now
		
		job_net_queue = simpy.Resource(env, 1)

		barrier = simpy.Container(env, len(self.partitions) ,0)
		part_network_barrier = simpy.Container(env, len(self.partitions) ,0)
		self.part_network_barrier = part_network_barrier

		for part in self.partitions:
			part_entry = None
			# part_entry = dict.fromkeys(columns_part)
			# part_entry["Job"] = self.name
			# part_entry["Cut"] = self.cut
			env.process(part.run(env,part_entry,job_net_queue,barrier,part_network_barrier))

		yield part_network_barrier.get(len(self.partitions))
		yield network_connection_queue.release(self.network_connection_queue_request)

		job_entry["Net_end"] = env.now

		yield barrier.get(len(self.partitions))
		del barrier
		job_entry["End_Time"] = env.now
		timings_job.loc[len(timings_job)] = job_entry

		# TODO: add final network delay


def network_delay(data_size,bandwidth):
	# input in MB
	return data_size/bandwidth

def data_remaining_on_network():
	request_list = network_connection_queue.users + network_connection_queue.queue
	job_list = [x.payload for x in request_list]
	cuts = [x.cut for x in job_list]
	transfer_sizes = np.array([x.transfer_part_size for x in job_list])
	partitions_remaining = np.array([x.part_network_barrier.capacity for x in job_list]) \
							- np.array([x.part_network_barrier.level for x in job_list])
	return sum(transfer_sizes * partitions_remaining)

def setup(env):
	# init global machines
	global machines, central_queues, network_queue, network_connection_queue
	machines = {
					"com":[Machine(env, x, "com") for x in range(COMPUTE_NUM_MACHNIES)],
					"sto":[Machine(env, x, "sto") for x in range(STORAGE_NUM_MACHNIES)]
				}

	central_queues = {
						"com":simpy.Resource(env, COMPUTE_NUM_CORES*COMPUTE_NUM_MACHNIES),
						"sto":simpy.Resource(env, STORAGE_NUM_CORES*STORAGE_NUM_MACHNIES)
					}

	network_queue = simpy.Resource(env, 1)
	network_connection_queue = simpy.Resource(env, CONCURRENT_CONNECTIONS)
	

	# starting tasks
	for i in range(INIT_TASKS):
		job = Job(i)
		env.process(job.run(env))
		# job.run(env)

	last_time_saved = 0
	while env.now < LOADING_TIME+1:
		# machines_status(machines)
		print(env.now)
		yield env.timeout(T_INTER) 

		#TODO: randomize inter arrival... poission distribution (exp inter arrival)? 
		i += 1
		job = Job(i)
		env.process(job.run(env))
		# job.run(env)

		# input()

		if env.now > last_time_saved+100*T_INTER:
			last_time_saved = env.now
			global senario
			print("------------",
					"\npolicy:", policy,
					"\nminutes:", (time.time()-start_time)//60,
					"\nsim time:", env.now,
					"\nsec/simsec:", "%.4f"%((time.time()-start_time)/env.now),
					"\nJobs completed:", len(timings_job),
					"\nsenario:", senario,
					"\n------------")
			print("------------",
					"\npolicy:", policy,
					"\nminutes:", (time.time()-start_time)//60,
					"\nsim time:", env.now,
					"\nsec/simsec:", "%.4f"%((time.time()-start_time)/env.now),
					"\nJobs completed:", len(timings_job),
					"\nsenario:", senario,
					"\n------------",file=sys.stderr)

			timings_job.to_csv(output_folder+"/timings_job_async_"+code_name+".csv")
			# timings_part.to_csv(output_folder+"/timings_part_async_"+code_name+".csv")
			gc.collect()

	# return

	# print("settling...")
	# for x in range(1000):
	# 	print(env.now)
	# 	yield env.timeout(SETTLING_TIME()/1000) 
	# 	print("------------",
	# 			"\npolicy:", policy,
	# 			"\nminutes:", (time.time()-start_time)//60,
	# 			"\nsim time:", env.now,
	# 			"\nsec/simsec:", "%.4f"%((time.time()-start_time)/env.now),
	# 			"\nJobs completed:", len(timings_job),
	# 			"\nsenario:", senario,
	# 			"\n------------")
	# 	print("------------",
	# 			"\npolicy:", policy,
	# 			"\nminutes:", (time.time()-start_time)//60,
	# 			"\nsim time:", env.now,
	# 			"\nsec/simsec:", "%.4f"%((time.time()-start_time)/env.now),
	# 			"\nJobs completed:", len(timings_job),
	# 			"\nsenario:", senario,
	# 			"\n------------",file=sys.stderr)
		
	# 	timings_job.to_csv(output_folder+"/timings_job_async_"+code_name+".csv")
	# 	# timings_part.to_csv(output_folder+"/timings_part_async_"+code_name+".csv")
	# 	gc.collect()

	# if env.now > SIM_TIME():
	# 	return

def machines_status(machines):
	print("==Machines Status=")
	cores_busy = np.array([x.cores.count+len(x.cores.queue) for x in machines])
	# mem_used = np.array([x.memory.capacity-x.memory.level for x in machines])
	# utilization = 500*cores_busy+mem_used

	# print("[%.2f"%env.now,"]\t",cores_busy,mem_used,utilization)
	print("[%.2f"%env.now,"]\t",cores_busy)


def best_machine(mtype):
	machines_np_type = np.array(machines[mtype])
	cores_busy = np.array([x.cores.count for x in machines_np_type])
	good_machines = machines_np_type[cores_busy==np.min(cores_busy)]
	
	return random.choice(good_machines)

def best_cut(job):
	# return 1
	if policy=="fil":
		return 1

	if policy=="fp":
		return 2


	for op_i in range(len(job.partitions[0].init_operations)):
		inp_size_op = sum([part.init_operations[op_i].inp_size for part in job.partitions])
		decision = best_cluster(inp_size_op,len(job.partitions))
		
		print("best_cut",op_i,decision)

		if decision == "com":
			return op_i

	return op_i+1


def best_cluster(inp_size_op, num_partitions):

	if policy=="random":
		return random.choice(["sto","com"])
	
	if policy=="sto":
		return "sto"
	
	if policy=="com":
		return "com"

	if policy=="busy":
		
		# compute
		network_delay = 0
		avg_queue_size = len(central_queues["com"].queue)/COMPUTE_NUM_MACHNIES
		avg_queue_dr = sum([machine.queuedrainrate for machine in machines["com"]])/COMPUTE_NUM_MACHNIES

		T_total_compute = network_delay + (avg_queue_size/(avg_queue_dr*COMPUTE_NUM_CORES)) \
							+ num_partitions/(avg_queue_dr*COMPUTE_NUM_MACHNIES*COMPUTE_NUM_CORES)

		# print("best_cluster:",inp_size_op,WAN_BANDWIDTH,WAN_LATENCY,network_delay)
		print("best_cluster_com:",T_total_compute,avg_queue_size,avg_queue_dr,network_delay)

		# storage
		avg_queue_size = len(central_queues["sto"].queue)/STORAGE_NUM_MACHNIES
		avg_queue_dr = sum([machine.queuedrainrate for machine in machines["sto"]])/STORAGE_NUM_MACHNIES

		T_total_storage = (avg_queue_size/(avg_queue_dr*STORAGE_NUM_CORES)) \
							+ num_partitions/(avg_queue_dr*STORAGE_NUM_MACHNIES*STORAGE_NUM_CORES)

		print("best_cluster_sto:",T_total_storage,avg_queue_size,avg_queue_dr)

		if T_total_storage < T_total_compute:
			ret = "sto"
		else:
			ret = "com"

		# print("com: %.2f\tnet: %.2f\tsto: %.2f\t%s"%(T_total_compute,network_delay,T_total_storage,ret))

		return ret

	if policy=="net":
		
		# compute
		# network_delay = inp_size_op/WAN_BANDWIDTH + len(network_queue.queue)*256/WAN_BANDWIDTH
		if len(network_connection_queue.users)==CONCURRENT_CONNECTIONS:
			print(">=3 active connections")
			network_delay = inp_size_op*(CONCURRENT_CONNECTIONS)/WAN_BANDWIDTH		\
							+ data_remaining_on_network()/WAN_BANDWIDTH
							# + len(network_connection_queue.queue)*PARTITION_SIZE*NUM_PARTITIONS/WAN_BANDWIDTH	\
							# + 3*PARTITION_SIZE*(NUM_PARTITIONS/2)/WAN_BANDWIDTH

		else:
			print("< 3 active connections")
			network_delay = inp_size_op*(len(network_connection_queue.users)+1)/WAN_BANDWIDTH


		avg_queue_size = len(central_queues["com"].queue)/COMPUTE_NUM_MACHNIES
		avg_queue_dr = sum([machine.queuedrainrate for machine in machines["com"]])/COMPUTE_NUM_MACHNIES

		T_total_compute = network_delay + (avg_queue_size/(avg_queue_dr*COMPUTE_NUM_CORES)) \
							+ num_partitions/(avg_queue_dr*COMPUTE_NUM_MACHNIES*COMPUTE_NUM_CORES)

		# print("best_cluster:",inp_size_op,WAN_BANDWIDTH,WAN_LATENCY,network_delay)
		print("com_cluster_score:",T_total_compute,avg_queue_size,avg_queue_dr,network_delay)

		# storage
		avg_queue_size = len(central_queues["sto"].queue)/STORAGE_NUM_MACHNIES
		avg_queue_dr = sum([machine.queuedrainrate for machine in machines["sto"]])/STORAGE_NUM_MACHNIES

		T_total_storage = (avg_queue_size/(avg_queue_dr*STORAGE_NUM_CORES)) \
							+ num_partitions/(avg_queue_dr*STORAGE_NUM_MACHNIES*STORAGE_NUM_CORES)

		print("sto_cluster_score:",T_total_storage,avg_queue_size,avg_queue_dr)

		if T_total_storage < T_total_compute:
			ret = "sto"
		else:
			ret = "com"

		# print("com: %.2f\tnet: %.2f\tsto: %.2f\t%s"%(T_total_compute,network_delay,T_total_storage,ret))

		return ret


if len(sys.argv)>1:
	policy = sys.argv[1]
	code_name = policy
	senario = 0
	print(policy)
	output_folder = "no_wrapper"

else:
	inp_json = " ".join(sys.stdin.readlines())
	inp_json = json.loads(inp_json)
	
	print(inp_json)
	print(inp_json,file=sys.stderr)

	globals().update(inp_json)
	output_folder = "wrapper"


random.seed(RANDOM_SEED)
env = simpy.Environment()
env.process(setup(env))

start_time = time.time()
env.run(until=SIM_TIME()+1)

print((time.time()-start_time)/60,time.time()-start_time,(time.time()-start_time)/SIM_TIME())
timings_job.to_csv(output_folder+"/timings_job_async_"+code_name+".csv")
# timings_part.to_csv(output_folder+"/timings_part_async_"+code_name+".csv")

print("------------",
					"\npolicy:", policy,
					"\nminutes:", (time.time()-start_time)//60,
					"\nsim time:", env.now,
					"\nsec/simsec:", "%.4f"%((time.time()-start_time)/env.now),
					"\nJobs completed:", len(timings_job),
					"\nsenario:", senario,
					"\n------------",file=sys.stderr)