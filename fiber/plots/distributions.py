import seaborn as sns
import matplotlib.pyplot as plt


def hist(series):
    fig, ax = plt.subplots()
    sns.distplot(series, ax=ax)
    return fig


def bars(series):
    fig, ax = plt.subplots()
    sns.countplot(series, palette='RdBu', ax=ax)
    return fig
