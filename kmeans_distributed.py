import numpy as np
import pandas as pd


max_iters = 25
k = 4


points = pd.read_csv("points_0.csv", header=None)
# centroids = pd.read_csv("initial_10_kmeans_centroids.csv", header=None)
centroids = pd.read_csv("initial_10_kmeans_centroids.csv", header=None).sample(n=k)


def euclidean(point1, point2):
    return np.sqrt(np.sum((np.array(point1) - np.array(point2))**2))


def single_kmeans():

    for _ in range(max_iters):
        clusters = {i: [] for i in range(k)}
        for _, d_row in points.iterrows():
            point = d_row.tolist()
            costs = [euclidean(point, c) for c in centroids.values]
            closest_centroid_id = np.argmin(costs)
            clusters[closest_centroid_id].append(point)
        for key in clusters.keys():
            if clusters[key]:
                centroids.iloc[key] = np.mean(np.array(clusters[key]), axis=0).tolist()
    print(centroids)

    return centroids

def distributes_kmeans():
    num_points = points.shape[0]
    points_split = [
        points.iloc[:num_points//4,],
        points.iloc[num_points//4:num_points//4*2,],
        points.iloc[num_points//4*2:num_points//4*3,],
        points.iloc[num_points//4*3:,]
    ]

    for i in range(max_iters):
        cluster_sums_split = np.array([worker_stage(points_chunk, centroids) for points_chunk in points_split])
        for c in range(k):
            centroids.iloc[c] = (np.sum(np.array(cluster_sums_split[:, c, :]), axis=0) / 2000).tolist()
    print(centroids)


def worker_stage(my_points, my_centroids):
    clusters = {i: [] for i in range(k)}
    cluster_sums = pd.DataFrame(np.zeros((k, my_points.shape[1])))
    for _, d_row in my_points.iterrows():
        point = d_row.tolist()
        costs = [euclidean(point, c) for c in my_centroids.values]
        closest_centroid_id = np.argmin(costs)
        clusters[closest_centroid_id].append(point)
    for key in range(k):
        cluster_sums.iloc[key] = np.sum(np.array(clusters[key]), axis=0).tolist()
    return cluster_sums



# single_kmeans()
distributes_kmeans()



