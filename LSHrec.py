from pyspark import SparkContext
import sys



#define func

#minhash func
def minHash(user):
    minhash_val = []
    for i in range(20):
        temp = []
        for mov in user[1]:
            h_val = (3*mov+13*i)%100
            temp.append(h_val)
            minhash = min(temp)
        minhash_val.append(minhash)
    return minhash_val


#LSH func
def lsh(hash_matrix):
    candidate = []
    for i in range(5):
        for u1 in range(len(hash_matrix)):
            for u2 in range(u1+1, len(hash_matrix)):
                if hash_matrix[u1][i*4:(i+1)*4] == hash_matrix[u2][i*4:(i+1)*4]:
                    candidate.append([u1,u2])
    return list(set(tuple(i) for i in candidate))

# calculate jaccard similarity
def jaccard (u1,u2):
    intersection = set(matrix_val[u1][1]).intersection(matrix_val[u2][1])
    union = set(matrix_val[u1][1]).union(matrix_val[u2][1])
    jaccard = len(intersection)/len(union)
    return jaccard

# generate the jaccard sets
def jaccardSets(candidates):
    jaccard_set = []
    for pair in candidates:
        temp = [(pair[0],pair[1]),jaccard(pair[0],pair[1])]
        temp1 = [(pair[1],pair[0]),jaccard(pair[0],pair[1])]
        jaccard_set.append(temp)
        jaccard_set.append(temp1)
    return jaccard_set


#find top5
def findTop5 (jaccardSets):
    kvset = []
    for kv in jaccardSets:
        temp = [kv[0][0], (kv[0][1], kv[1])]
        kvset.append(temp)

    temp = sc.parallelize(kvset).groupByKey().map(lambda x: (x[0], list(x[1])))
    sorted_top = temp.map(lambda x: [x[0], sorted(x[1], key=lambda x: x[1], reverse=True)]).mapValues(lambda x: [a[0] for a in x][:5])
    return sorted_top

#find top3 movies
def topMov (similarUser):
    kv = []
    similarMov = []
    top3Mov = []
    for k, v in similarUser:
        if len(v) > 1:
            temp = matrix_val[v[0]][1] + matrix_val[v[1]][1]
        elif len(v) == 1:
            temp = matrix_val[v[0]][1]
        else:
            temp = []
        kv.append([k, temp])

    for i in kv:
        new_dict = {}
        for j in i[1]:     # in there, I didn't remove the movie which users already watched. IF want to remove those movies, just add an if-else
            new_dict.setdefault(j, 0)
            new_dict[j] += 1
        similarMov.append([i[0], new_dict])

    for mov in similarMov:
        temp_mov = sorted(mov[1].items(), key=lambda x: (-x[1], x[0]))[:3]
        top3Mov.append([mov[0], [x[0] for x in temp_mov]])

    return sorted(top3Mov)

#main func
if __name__ == "__main__":
    ## read input file
    sc = SparkContext(appName="lsh")
    text = sc.textFile(sys.argv[1], 4)   

    # generate eigen_matrix
    matrix = text.map(lambda x: x.split(',')).map(lambda x: (x[0], [int(i) for i in x[1:]]))
    matrix_val = matrix.collect()

    # get for each user
    hash_matrix = matrix.map(minHash).collect()

    # transform the matrix, no need
    # hash_matrix_tran = np.array(matrix.map(minHash).collect()).transpose()

    candidates = lsh(hash_matrix)
    jaccardSets = jaccardSets(candidates)
    top5 = findTop5(jaccardSets).collect()
    recommendMov = topMov(top5)

    with open(sys.argv[2], 'w') as file:
        for x in recommendMov:
            file.write('U' + str(x[0] + 1) + ',' + str(x[1])[1:-1].replace(' ', '') + '\n')











