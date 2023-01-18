import heapq
from mrjob.job import MRJob
from mrjob.step import MRStep

class ExpensiveBrands(MRJob):

    def mapper(self, _, line):
        # split the line by the comma delimiter
        fields = line.split(',')
        # get the brand and price fields
        brand = fields[0]
        price = float(fields[1])
        # yield the brand and price as key-value pairs
        yield brand, price

    def reducer(self, brand, prices):
        # get the maximum price for the brand
        max_price = max(prices)
        # add the brand and maximum price to the priority queue
        # if the queue is already at size 20, only add the brand if it has a higher price
        # than the current minimum price in the queue
        if len(self.q) < 20:
            heapq.heappush(self.q, (max_price, brand))
        elif max_price > self.q[0][0]:
            heapq.heapreplace(self.q, (max_price, brand))

if __name__ == '__main__':
    # create an empty priority queue
    ExpensiveBrands.q = []
    ExpensiveBrands.run()
    # output the top 20 brands and their prices
    for price, brand in heapq.nlargest(20, ExpensiveBrands.q):
        print(brand, price)
