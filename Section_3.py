#!/home/spl/ml/data_incubator/bin/python
# Python version 3.7.6
from functools import reduce
import concurrent.futures
import itertools
import random

def permutate(sequence):
    '''
    Description:
        Find all permutations of an list
    Arg(s):
        sequence: a list
    Return:
        permutations: a list of all permuations of the input
    '''
    random.shuffle(sequence)
    N = len(sequence)

    if N == 0:
        return 
    if N == 1:
        yield sequence
    
    permutations = []

    for i in range(N):
        temp = sequence[i]
        rest = sequence[:i] + sequence[i+1:]
        for j in permutate(rest):
            yield ([temp]+j)

def drawn_all(perm):
    '''
    Arg: a list of permutations
    Return: list of all the possible payments
    '''
    results = []
    
    for i in perm:
        value = i[0]
        for j in range(1,len(i)):
            value += abs(i[j] - i[j-1])
        results.append(value)
    return results

def drawn_one(seq):
    '''
    Return: payments
    '''
    value = seq[0]

    for i in range(1, len(seq)):
        value += abs(seq[i] - seq[i-1])

    return value


def drawn_parallel(sequence, condition = None, workers = 6):
    '''
    Args:  
        sequence: iterable, integers
        condition: function or logics, a logical operations, 
                    for example: lambda x: x>1
    Return: list of all the possible payments
    '''
    
    N = len(sequence)
    perm = permutate(list(sequence))
    chunk_size = fractorial(11)

    count = 0
    sum = 0
    condition_count = 0
    var = 0
    results = []

    def chunked_iterable(iterable, size):
        it = iter(iterable)
        while True:
            chunk = tuple(itertools.islice(it, size))
            if not chunk:
                break
            yield chunk

    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(drawn_all, perm_chunk) \
                for perm_chunk in chunked_iterable(perm, chunk_size)
        }

        while futures:
            done, futures = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )

            for fut in done:
                for val in fut.result():
                    count += 1
                    sum += val

                    if count % 100 == 0 :
                        results.append(val)

                    if condition is not None:
                        if condition(val):
                            condition_count += 1
                    else:
                        pass

            for perm_chunk in chunked_iterable(perm, chunk_size):
                futures.add(
                    executor.submit(drawn_all, perm_chunk)
                )

    return [sum, count, results, condition_count]

def fractorial(value):
    '''
    Return: the fractorial of an integer
    '''
    if value == 0:
        return 1
    else:
        return value * fractorial(value - 1)

def mean(seq):
    '''
    Return: the mean of a list of numbers
    '''
    N = len(seq)
    sum = reduce(lambda x,y: x+y, seq)
    mean = sum / N

    return mean

def std(seq):
    '''
    Return: standard deviation of the a list of numbers
    '''
    count = 0
    var = 0
    u = mean(seq)

    for i in seq:
        count += 1
        var += (i - u) ** 2

    stdv = (var / count) ** 0.5

    return stdv

if __name__ == "__main__":
    #Q1 mean of total payment for N = 10
    sum_10, count_10, results_10, condition_count_10 = drawn_parallel(list(range(1,11)), condition=lambda x: x>=45)
    print('The mean of total payment for N = 10 is: \n {:>.5f}'.format(sum_10 / count_10))

    #Q2 standard deviation of the total payment for N = 10
    print('The standard deviation of total payment for N = 10 is: \n {:>.5f}'.format(std(results_10)))

    #Q3 mean of total payment for N = 20   
    i = 0
    condition_count_20 = 0
    a = list(range(1,21))
    results_20 = []
    random.seed(42)
    seed_sample = random.sample(range(fractorial(10)),fractorial(10))
    while i < fractorial(10):
        random.seed(seed_sample[i])
        random.shuffle(a)
        val = drawn_one(a)
        results_20.append(val)
        i += 1
        if val >= 160:
            condition_count_20 += 1
    print('The mean of total payment for N = 20 is: \n {:>.5f}'.format(mean(results_20)))        

    #Q4 standard deviation of total payment for N=20
    print('The standard deviation of total payment for N = 20 is: \n {:>.5f}'.format(std(results_20)))
    
    #Q5 the probability that total payment >= 45 for N=10
    print('The probability that total payment >= 45 for N=10 is \n {:>.5f}'.format(condition_count_10 / count_10))

    #Q6 the probability that total payment >= 160 for N=20
    print('The probability that total payment >= 160 for N=20 is \n {:>.5f}'.format(condition_count_20 / i))