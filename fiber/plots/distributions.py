import matplotlib.pyplot as plt
import seaborn as sns


def hist(series, rotate_labels_by=None, **kwargs):
    fig, ax = plt.subplots()
    sns.distplot(series, ax=ax, **kwargs)
    if rotate_labels_by:
        plt.setp(ax.get_xticklabels(), rotation=rotate_labels_by)
    return fig


def bars(series, rotate_labels_by=None, **kwargs):
    fig, ax = plt.subplots()
    sns.countplot(series, ax=ax, **kwargs)
    if rotate_labels_by:
        plt.setp(ax.get_xticklabels(), rotation=rotate_labels_by)
    return fig
