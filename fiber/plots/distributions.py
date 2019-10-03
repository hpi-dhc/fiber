import matplotlib.pyplot as plt
import seaborn as sns


def hist(series):
    fig, ax = plt.subplots()
    sns.distplot(series, ax=ax)
    return fig


def bars(series):
    fig, ax = plt.subplots()
    sns.countplot(series, palette='RdBu', ax=ax)
    return fig
