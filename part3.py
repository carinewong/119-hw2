"""
Part 3: Measuring Performance

Now that you have drawn the dataflow graph in part 2,
this part will explore how performance of real pipelines
can differ from the theoretical model of dataflow graphs.

We will measure the performance of your pipeline
using your ThroughputHelper and LatencyHelper from HW1.

=== Coding part 1: making the input size and partitioning configurable ===

We would like to measure the throughput and latency of your PART1_PIPELINE,
but first, we need to make it configurable in:
(i) the input size
(ii) the level of parallelism.

Currently, your pipeline should have two inputs, load_input() and load_input_bigger().
You will need to change part1 by making the following additions:

- Make load_input and load_input_bigger take arguments that can be None, like this:

    def load_input(N=None, P=None)

    def load_input_bigger(N=None, P=None)

You will also need to do the same thing to q8_a and q8_b:

    def q8_a(N=None, P=None)

    def q8_b(N=None, P=None)

Here, the argument N = None is an optional parameter that, if specified, gives the size of the input
to be considered, and P = None is an optional parameter that, if specifed, gives the level of parallelism
(number of partitions) in the RDD.

You will need to make both functions work with the new signatures.
Be careful to check that the above changes should preserve the existing functionality of part1
(so python3 part1.py should still give the same output as before!)

Don't make any other changes to the function sigatures.

Once this is done, define a *new* version of the PART_1_PIPELINE, below,
that takes as input the parameters N and P.
(This time, you don't have to consider the None case.)
You should not modify the existing PART_1_PIPELINE.

You may either delete the parts of the code that save the output file, or change these to a different output file like part1-answers-temp.txt.
"""
import part1
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.utils import IllegalArgumentException
import pyspark

try:
    # This should run on the driver (python3 part1.py, pytest, python3 part3.py)
    spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
    sc = spark.sparkContext
except PySparkRuntimeError:
    # This path happens on workers when they import part1.
    # We don’t want to create a SparkContext here.
    spark = None
    sc = None

ANSWER_FILE = "output/part1-answers-tmp.txt"
UNFINISHED = 0

def log_answer(name, func, *args):
    try:
        answer = func(*args)
        print(f"{name} answer: {answer}")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},{answer}\n')
            print(f"Answer saved to {ANSWER_FILE}")
    except NotImplementedError:
        print(f"Warning: {name} not implemented.")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},Not Implemented\n')
        global UNFINISHED
        UNFINISHED += 1

def PART_1_PIPELINE_PARAMETRIC(N, P):
    """
    Follow the same logic as PART_1_PIPELINE
    N = number of inputs
    P = parallelism (number of partitions)
    (You can copy the code here), but make the following changes:
    - load_input should use an input of size N.
    - load_input_bigger (including q8_a and q8_b) should use an input of size N.
    - both of these should return an RDD with level of parallelism P (number of partitions = P).
    """
    open(ANSWER_FILE, 'w').close()

    try:
        dfs = part1.load_input()
    except NotImplementedError:
        print("Welcome to Part 1! Implement load_input() to get started.")
        dfs = sc.parallelize([])

    # Questions 1-3
    log_answer("q1", part1.q1)
    log_answer("q2", part1.q2)
    # 3: commentary

    # Questions 4-10
    log_answer("q4", part1.q4, dfs)
    log_answer("q5", part1.q5, dfs)
    log_answer("q6", part1.q6, dfs)
    log_answer("q7", part1.q7, dfs)
    log_answer("q8a", part1.q8_a, N, P)
    log_answer("q8b", part1.q8_b, N, P)
    # 9: commentary
    # 10: commentary

    # Questions 11-18
    log_answer("q11", part1.q11, dfs)
    # 12: commentary
    # 13: commentary
    log_answer("q14", part1.q14, dfs)
    # 15: commentary
    log_answer("q16a", part1.q16_a)
    log_answer("q16b", part1.q16_b)
    log_answer("q16c", part1.q16_c)
    # 17: commentary
    # 18: commentary

    # Questions 19-20
    # 19: commentary
    log_answer("q20", part1.q20)

    # Answer: return the number of questions that are not implemented
    if UNFINISHED > 0:
        print("Warning: there are unfinished questions.")

    return f"{UNFINISHED} unfinished questions"

"""
=== Coding part 2: measuring the throughput and latency ===

Now we are ready to measure the throughput and latency.

To start, copy the code for ThroughputHelper and LatencyHelper from HW1 into this file.

Then, please measure the performance of PART1_PIPELINE as a whole
using five levels of parallelism:
- parallelism 1
- parallelism 2
- parallelism 4
- parallelism 8
- parallelism 16

For each level of parallelism, you should measure the throughput and latency as the number of input
items increases, using the following input sizes:
- N = 1, 10, 100, 1000, 10_000, 100_000, 1_000_000.

- Note that the larger sizes may take a while to run (for example, up to 30 minutes). You can try with smaller sizes to test your code first.

You can generate any plots you like (for example, a bar chart or an x-y plot on a log scale,)
but store them in the following 10 files,
where the file name corresponds to the level of parallelism:

output/part3-throughput-1.png
output/part3-throughput-2.png
output/part3-throughput-4.png
output/part3-throughput-8.png
output/part3-throughput-16.png
output/part3-latency-1.png
output/part3-latency-2.png
output/part3-latency-4.png
output/part3-latency-8.png
output/part3-latency-16.png

Clarifying notes:

- To control the level of parallelism, use the N, P parameters in your PART_1_PIPELINE_PARAMETRIC above.

- Make sure you sanity check the output to see if it matches what you would expect! The pipeline should run slower
  for larger input sizes N (in general) and for fewer number of partitions P (in general).

- For throughput, the "number of input items" should be 2 * N -- that is, N input items for load_input, and N for load_input_bigger.

- For latency, please measure the performance of the code on the entire input dataset
(rather than a single input row as we did on HW1).
MapReduce is batch processing, so all input rows are processed as a batch
and the latency of any individual input row is the same as the latency of the entire dataset.
That is why we are assuming the latency will just be the running time of the entire dataset.

- Please set `NUM_RUNS` to `1` if you haven't already. Note that this will make the values for low numbers (like `N=1`, `N=10`, and `N=100`) vary quite unpredictably.
"""
import time
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Copy in ThroughputHelper and LatencyHelper
class ThroughputHelper:
    def __init__(self):
        # Initialize the object.
        # Pipelines: a list of functions, where each function
        # can be run on no arguments.
        # (like: def f(): ... )
        self.pipelines = []

        # Pipeline names
        # A list of names for each pipeline
        self.names = []

        # Pipeline input sizes
        self.sizes = []

        # Pipeline throughputs
        # This is set to None, but will be set to a list after throughputs
        # are calculated.
        self.throughputs = None

    def add_pipeline(self, name, size, func):
        self.names.append(name)
        self.sizes.append(size)
        self.pipelines.append(func)

    def compare_throughput(self):
        # Measure the throughput of all pipelines
        # and store it in a list in self.throughputs.
        # Make sure to use the NUM_RUNS variable.
        # Also, return the resulting list of throughputs,
        # in **number of items per second.**
        
        #formula: N / (T / NUM_RUNS) where N = number of input items, T is total time
        NUM_RUNS = 1
        all_throughputs = []
        for name, size, func in zip(self.names, self.sizes, self.pipelines):
            start_time = time.perf_counter()
            for i in range(NUM_RUNS):
                func()
            end_time = time.perf_counter()
            T = end_time - start_time
            denom = T / NUM_RUNS
            if denom > 0:
                this_throughput = size / denom
            elif denom == 0:
                this_throughput = 0
            all_throughputs.append(this_throughput)
        self.throughputs = all_throughputs
        return all_throughputs

    def generate_plot(self, filename):
        # Generate a plot for throughput using matplotlib.
        # You can use any plot you like, but a bar chart probably makes
        # the most sense.
        # Make sure you include a legend.
        # Save the result in the filename provided.
        plt.figure()
        plt.bar(self.names, self.throughputs)
        plt.xticks(rotation=45)
        plt.title("Comparing Throughput for our Pipelines")
        plt.xlabel("Pipeline Type")
        plt.ylabel("Throughput (items / second)")
        plt.xticks(rotation = 45, fontsize=6)
        plt.tight_layout()
        plt.savefig(filename)

class LatencyHelper:
    def __init__(self):
        # Initialize the object.
        # Pipelines: a list of functions, where each function
        # can be run on no arguments.
        # (like: def f(): ... )
        self.pipelines = []

        # Pipeline names
        # A list of names for each pipeline
        self.names = []

        # Pipeline latencies
        # This is set to None, but will be set to a list after latencies
        # are calculated.
        self.latencies = None

    def add_pipeline(self, name, func):
        self.names.append(name)
        self.pipelines.append(func)

    def compare_latency(self):
        # Measure the latency of all pipelines
        # and store it in a list in self.latencies.
        # Also, return the resulting list of latencies,
        # in **milliseconds.**
        all_latency = []
        NUM_RUNS = 1
        for name, pipeline in zip(self.names, self.pipelines):
            start_time = time.perf_counter()
            for run in range(NUM_RUNS):
                pipeline()
            end_time = time.perf_counter()

            this_latency_ms = ((end_time - start_time) * 1000 ) / NUM_RUNS 
            this_latency_ms = f"{this_latency_ms:.6f}"
            all_latency.append(this_latency_ms)
        self.latencies = all_latency
        return all_latency


    def generate_plot(self, filename):
        # Generate a plot for latency using matplotlib.
        # You can use any plot you like, but a bar chart probably makes
        # the most sense.
        # Make sure you include a legend.
        # Save the result in the filename provided.
        latencies = [float(x) for x in self.latencies]
        plt.figure()
        plt.bar(self.names, latencies)
        plt.xticks(rotation = 45)
        plt.title("Comparison of Latency for Pipelines")
        plt.xlabel("Pipeline Type")
        plt.ylabel("Latency (in milliseconds)")
        plt.yticks(fontsize=6)
        plt.tight_layout()
        plt.savefig(filename)
        plt.close()

"""
=== Reflection part ===

Once this is done, write a reflection and save it in
a text file, output/part3-reflection.txt.

I would like you to think about and answer the following questions:

1. What would we expect from the throughput and latency
of the pipeline, given only the dataflow graph?

Use the information we have seen in class. In particular,
how should throughput and latency change when we double the amount of parallelism?

Please ignore pipeline and task parallelism for this question.
The parallelism we care about here is data parallelism.

2. In practice, does the expectation from question 1
match the performance you see on the actual measurements? Why or why not?

State specific numbers! What was the expected throughput and what was the observed?
What was the expected latency and what was the observed?

3. Finally, use your answers to Q1-Q2 to form a conjecture
as to what differences there are (large or small) between
the theoretical model and the actual runtime.
Name some overheads that might be present in the pipeline
that are not accounted for by our theoretical model of
dataflow graphs that could affect performance.

You should list an explicit conjecture in your reflection, like this:

    Conjecture: I conjecture that ....

You may have different conjectures for different parallelism cases.
For example, for the parallelism=4 case vs the parallelism=16 case,
if you believe that different overheads are relevant for these different scenarios.

=== Grading notes ===

- Don't forget to fill out the entrypoint below before submitting your code!
Running python3 part3.py should work and should re-generate all of your plots in output/.

- You should modify the code for `part1.py` directly. Make sure that your `python3 part1.py` still runs and gets the same output as before!

- Your larger cases may take a while to run, but they should not take any
  longer than 30 minutes (half an hour).
  You should be including only up to N=1_000_000 in the list above,
  make sure you aren't running the N=10_000_000 case.

- In the reflection, please write at least a paragraph for each question. (5 sentences each)

- Please include specific numbers in your reflection (particularly for Q2).

=== Entrypoint ===
"""

if __name__ == '__main__':
    spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
    sc = spark.sparkContext
    print("Complete part 3. Please use the main function below to generate your plots so that they are regenerated whenever the code is run:")

    P_vals = [1, 2, 4, 8, 16]
    N_vals = [1, 10, 100, 1000, 10000, 100000, 1000000]

    for P in P_vals:
        Throughput = ThroughputHelper()
        Latency = LatencyHelper() 
        
        def pipeline_helper(N, P):
            def pipeline_function():
                PART_1_PIPELINE_PARAMETRIC(N, P)
            return pipeline_function
            
        for N in N_vals:
            Throughput.add_pipeline(f"P={P}, N = {N}", 2*N, pipeline_helper(N, P))
            Latency.add_pipeline(f"P={P}, N={N}", pipeline_helper(N, P))
        
        #measure and save plots
        throughput_results = Throughput.compare_throughput()
        latency_results = Latency.compare_latency()
        Throughput.generate_plot(f"output/part3-throughput-{P}.png")
        Latency.generate_plot(f"output/part3-latency-{P}.png")
        