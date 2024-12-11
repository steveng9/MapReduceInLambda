import sys
import pandas as pd
import numpy as np
import random

# K = int(sys.argv[1])
# num_points = int(sys.argv[2])
# dimensions = int(sys.argv[3])
# num_files = int(sys.argv[4])

K = 15
num_points = 10_000
dimensions = 20
num_files = 2000

for i, file in enumerate(range(num_files)):
    a = np.random.rand(num_points, dimensions)
    for j in range(a.shape[1]):
        x = random.randint(3, 30) # scale some columns (i.e. dimensions) arbitrarily for more variation
        a[:, j] *= x

    pd.DataFrame(a).to_csv(f"points_{i}.csv", header=False, index=False)
    if i == 0:
        b = a[np.random.choice(a.shape[0], K, replace=False)]
        pd.DataFrame(b).to_csv(f"initial_{K}_kmeans_centroids.csv", header=False, index=False)
