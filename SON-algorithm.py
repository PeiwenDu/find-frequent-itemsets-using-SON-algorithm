from pyspark import SparkContext
import sys,time
from itertools import combinations

filter_threshold = int(sys.argv[1])
support = int(sys.argv[2])
inputfile = sys.argv[3]
outputfile = sys.argv[4]

# filter_threshold = 70
# support = 50
# inputfile = "/Users/peiwendu/Downloads/public_data/AZ_yelp.csv"
# outputfile = "peiwen_du_task2.txt"

sc = SparkContext(appName="inf553_hw2.2")
sc.setLogLevel("WARN")

def findcandidate(partition,support, max_length):
    p = list(partition)
    # store the candidate pair and its count
    # construct pair
    canId = dict()
    candidate_item_for_next_round = []
    candidate_pair_form_last_round =[]
    for i in range(max_length):
        if i == 0:
            for map in p:
                for item in map:
                    item = (item,)
                    canId[item] = canId.get(item, 0) + 1
        else:
            # find the candidate item from the last round
            # items_last_round = []
            # notHave = True
            # for item in canId:
            #     if len(item) == i:
            #         notHave = False
            #         items_last_round.extend(item)
            # if notHave | len(set(items_last_round)) <= i:
            #     break
            if len(set(candidate_item_for_next_round)) < i+1:
                break
            items = combinations(set(candidate_item_for_next_round), i + 1)
            # pair whose children are all in candidate pair
            items_fromkeys = []
            isFre = True
            for item in items:
                if i!=1:
                    for sonitem in combinations(item, i):
                        if tuple(sorted(sonitem)) in candidate_pair_form_last_round:
                            isFre = True
                        else:
                            isFre = False
                            break
                if isFre:
                    items_fromkeys.append(tuple(sorted(item)))
            if len(items_fromkeys) == 0:
                break
            for map in p:
                for item in items_fromkeys:
                    if set(item) <= set(map):
                        canId[item] = canId.get(item, 0) + 1
        # store this round's pair whose count<support
        keys = []
        for item in canId:
            if len(item) == i + 1:
                if canId[item] < support:
                    keys.append(item)
        for key in keys:
            canId.pop(key)

        candidate_item_for_next_round = []
        candidate_pair_form_last_round = []
        for item in canId:
            candidate_item_for_next_round.extend(item)
            candidate_pair_form_last_round.append(item)
            # print(item)
            yield (item, 1)
        # print(len(canId))
        canId.clear()


def findfrequent(partition, candidate_pair):
    p = list(partition)
    freId = dict()
    for item in candidate_pair:
        for map in p:
            if set(item) <= set(map):
                freId[item] = freId.get(item, 0) + 1
    for item in freId:
        yield (item, freId[item])

def changetoindex(bid,business_index):
    return (bid[0],business_index[bid[1]])

def findoriginalname(map,index_business):
    namemap = []
    for item in map:
        namemap.append(index_business[item])
    return namemap


start = time.time()
test_data = sc.textFile(inputfile).filter(lambda x: "user_id,business_id" not in x).map(lambda x: x.split(",")).map(
    lambda x: (x[0], x[1]))

# user_baskets = test_data.groupByKey().map(lambda x: set(x[1])).filter(lambda x:len(x)>filter_threshold)
business_amount= test_data.map(lambda x:x[1]).distinct().collect()
business_index = dict()
index_business = dict()
b_index=0
for bid in business_amount:
    business_index[bid] = b_index
    index_business[b_index]=bid
    b_index+=1
user_baskets = test_data.map(lambda x:changetoindex(x,business_index)).groupByKey().map(lambda x: set(x[1])).filter(lambda x:len(x)>filter_threshold)

num_partition = user_baskets.getNumPartitions()
# print(num_partition)
support_in_partition = support / num_partition
max_length = user_baskets.map(lambda x: len(x)).max()
candidate_pair = user_baskets.mapPartitions(lambda x: findcandidate(x, support_in_partition, max_length)).map(
    lambda x: tuple(sorted(x[0]))).distinct()
max_length_of_candidate = candidate_pair.map(lambda x: len(x)).max()
candidate_pairs = candidate_pair.collect()

frequent_pair = user_baskets.mapPartitions(lambda x: findfrequent(x, candidate_pairs)).reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] >= support).map(lambda x: tuple(sorted(x[0]))).distinct()

max_length_of_frequent = frequent_pair.map(lambda x:len(x)).max()
with open(outputfile, "w") as f:
    f.write("Candidates:\n")
    for i in range(max_length_of_candidate):
        candidateItem = candidate_pair.filter(lambda x: len(x) == i + 1).map(lambda x:findoriginalname(x,index_business)).map(lambda x: (tuple(sorted(x)), 1)).sortByKey().map(
            lambda x: x[0]).collect()
        for item in candidateItem:
            f.write("(")
            for bid in item:
                f.write("'" + bid + "'")
                if item.index(bid) != len(item) - 1:
                    f.write(",")
                    f.write(" ")
            f.write(")")
            if candidateItem.index(item) != len(candidateItem) - 1:
                f.write(",")
        f.write("\n\n")
    f.write("Frequent Itemsets:\n")
    for i in range(max_length_of_frequent):
        freItem = frequent_pair.filter(lambda x: len(x) == i + 1).map(lambda x:findoriginalname(x,index_business)).map(lambda x: (tuple(sorted(x)), 1)).sortByKey().map(
            lambda x: x[0]).collect()
        for item in freItem:
            f.write("(")
            for bid in item:
                f.write("'" + bid + "'")
                if item.index(bid) != len(item) - 1:
                    f.write(",")
                    f.write(" ")
            f.write(")")
            if freItem.index(item) != len(freItem) - 1:
                f.write(",")
        f.write("\n\n")

#
# iscorrect = user_baskets.filter(lambda x: len(
#     set(['64dfRmMmUsOdLnkBOtzp4w', 'k1QpHAkzKTrFYfk6u--VgQ', 'z6-reuC5BYf_Rth9gMBfgQ']).intersection(x)) == 2).count()
# print(iscorrect)

end = time.time()
print("Duration:"+str(end-start))

