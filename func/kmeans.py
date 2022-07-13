import logging
from functools import reduce

import matplotlib.pyplot as plt
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler


def preparing_kmeans(activity_profile, performance_profile, grade_profile, vle_profile):
    """Function for preparing the data for the kmeans
    clustering alogrithm.

    The profiles get merged in the same dataframe (merged on
    the case_id) and afterwards scaled to the same unit. The
    graph for the elbow method gets prepared, to manually decide
    on the number of clusters

    Returns:
        data:
            a dataframe with all the profiles appended
        X:
            data that the kmeans will be applied to
            (case_id dropped and scaler applied)
    """
    logger = logging.getLogger("preparing_kmeans")
    logger.setLevel(logging.INFO)

    data = [activity_profile, performance_profile, grade_profile, vle_profile]
    logger.debug("The profiles are merged")
    data = reduce(
        lambda left, right: pd.merge(
            left, right, on=["case:concept:name"], how="inner"
        ),
        data,
    ).fillna(0)

    X = data.drop("case:concept:name", axis=1)

    X = StandardScaler().fit_transform(X)

    logger.info("Elbow graph gets prepared")
    distortions = []
    for i in range(1, 20):
        km = KMeans(n_clusters=i, init="random", n_init=20, random_state=0)
        km.fit(X)
        distortions.append(km.inertia_)

    # plot
    plt.plot(range(1, 20), distortions, marker="o")
    plt.xlabel("Number of clusters")
    plt.ylabel("Distortion")
    plt.show()

    logger.info("Data is ready for kmeans")

    return data, X


def kmeans_apply(X, data, clusters=int):
    """Function that applies the kmeans clustering
    and gets the results by fitting them over the X
    variable. It adds a new column "cluster" to the
    data df for the results

    Args:
        X:
            data that the kmeans is applied to
        data:
            initial dataframe where the results get
            appended
        clusters:
            number of clusters to be created - must
            be an int

    Returns:
        A pandas dataframe with the cluster label
        attached (if 2 clusters - labels are 0 and 1)
    """
    logger = logging.getLogger("apply_kmeans")
    logger.setLevel(logging.INFO)

    logger.debug("Begining clustering with %s clusters", clusters)
    km = KMeans(n_clusters=clusters, init="random", n_init=20, random_state=0)

    logger.info("Clustering done")
    y_km = km.fit_predict(X)

    data["cluster"] = y_km

    return data
