from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import pandas as pd

from functools import reduce

import matplotlib.pyplot as plt


def preparing_kmeans (activity_profile, 
                        performance_profile, 
                        grade_profile,
                        vle_profile):

    data = [activity_profile, performance_profile, grade_profile, vle_profile]

    data = reduce(lambda  left, right: pd.merge(left, right, on=['case:concept:name'],
                                            how='inner'), data).fillna(0)

    X = data.drop("case:concept:name",axis=1)

    X = StandardScaler().fit_transform(X)

    distortions = []
    for i in range(1, 20):
        km = KMeans(
            n_clusters=i, init='random',
            n_init=20,
            random_state=0
        )
        km.fit(X)
        distortions.append(km.inertia_)

    # plot
    plt.plot(range(1, 20), distortions, marker='o')
    plt.xlabel('Number of clusters')
    plt.ylabel('Distortion')
    plt.show()

    return data, X

def kmeans_apply (X, data, clusters = int):
    km = KMeans(
        n_clusters=clusters, 
        init='random',
        n_init=20, 
        random_state=0)

    y_km = km.fit_predict(X)

    data["cluster"] = y_km

    return data