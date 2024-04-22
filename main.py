import math

from Pyro4 import expose
import random


class Solver:
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.workers = workers
        print("Inited")

    def solve(self):
        print("Job Started")
        print("Workers %d" % len(self.workers))

        n = self.read_input()

        a = []
        for i in xrange(n):
            a.append(random.randint(1, 1000000000))

        chunks = self.chunk(a, len(self.workers))

        # map
        mapped = []
        for i in xrange(0, len(self.workers)):
            print("map %d" % i)
            mapped.append(self.workers[i].mymap(chunks[i]))

        # reduce
        primes = self.myreduce(mapped)

        # output
        self.write_output(primes)

        print("Job Finished")

    def chunk(self, a, n):
        avg = len(a) / float(n)
        out = []
        last = 0.0
        while last < len(a):
            out.append(a[int(last):int(last + avg)])
            last += avg
        return out

    @staticmethod
    @expose
    def mymap(test_array):
        multipliers = []

        for num in test_array:
            multipliers.append(Solver.get_multipliers(num))

        return multipliers

    @staticmethod
    def get_multipliers(n):
        if n % 2 == 0:
            return [2, math.floor(n / 2)]

        a = math.ceil(math.sqrt(n))
        b2 = a * a - n
        sqrt_tmp = math.sqrt(b2)
        while not 1e-6 < sqrt_tmp - int(sqrt_tmp) < 1e6:
            a += 1
            b2 = a * a - n
            sqrt_tmp = math.sqrt(b2)

        b = int(math.sqrt(b2))
        return [a - b, a + b]

    @staticmethod
    @expose
    def myreduce(mapped):
        print("reduce")
        output = []

        for array in mapped:
            print("reduce loop")
            output += array.value
        print("reduce done")
        return output

    def read_input(self):
        f = open(self.input_file_name, 'r')
        n = int(f.readline())
        f.close()
        return n

    def write_output(self, output):
        f = open(self.output_file_name, 'w')
        f.write(',\n'.join([str(i) for i in output]))
        f.write('\n')
        f.close()
        print("output done")

