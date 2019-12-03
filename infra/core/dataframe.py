import os
from typing import List

import numpy as np
import pandas as pd


def calc_statistics(series: pd.Series, round_factor: int = 2, n_bins: int = 50,
                    quantiles: List[float] = [0.05, 0.25, 0.75, 0.95]):
    """
    Calculates statistics for pandas series

    Args:
        series: pandas series.
        round_factor: integer represents the requierd
        n_bins : number of bins to biuld variable histogram
        quantiles: quantiles to describe the variable distribution

    Returns:
        stats: dictionary of statistics with their calculated
               values for the given series.
    """
    stats = {}
    formating = "{:." + str(round_factor) + "}"

    if (np.issubdtype(series.dtype, np.number)) and \
       (np.count_nonzero(np.array(series)) == len(series)):

        stats.update({"variable_name": series.name()})
        stats.update({"mean": series.mean().round(round_factor)})
        stats.update({"mean": series.mean().round(round_factor)})
        stats.update({"std": series.std().round(round_factor)})
        stats.update({"min": series.min().round(round_factor)})
        stats.update({"max": series.max().round(round_factor)})
        stats.update({"kurtosis": series.kurtosis().round(round_factor)})
        stats.update({"skewness": series.skew().round(round_factor)})
        stats.update({"sum": int(series.sum())})
        stats.update({"mad": series.mad().round(round_factor)})
        stats.update({"n_zeros": (len(series) - np.count_nonzero(series))})
        stats.update({"n_nan": series.isna().sum()})
        stats.update({"{:.0%}".format(percentile): formating.format(value)
                      for percentile, value in series.quantile(quantiles).
                      to_dict().items()})

        if ("75%" in stats.keys()) and ("25%" in stats.keys()):
            stats.update({"iqr": formating.format(
                float(stats["75%"]) - float(stats["25%"]))})

        if ("mean%" in stats.keys()):
            stats.update({"coefficient_of_variation": formating.format(
                stats["std"] / stats["mean"])})
        else:
            stats.update({"coefficient_of_variation": np.NaN})

        stats.update({"p_zeros": formating.format(
            float(stats["n_zeros"])/len(series))})
        stats.update({"p_nan": formating.format(
            float(stats["n_nan"]) / len(series))})

        bins = np.histogram(np.array(series), bins=n_bins, density=True)[1]
        stats.update({"bins": bins.round(decimals=round_factor, out=None)})

    return stats


def get_statistics_df(df):
    """
    Gets statistics for each dataframe columns

    Args:
        df: pandas dataframe

    Returns:
        stats_df: Dataframe contians statistics for each columns 
                  of the input dataframe
    """

    stats_names = ['variable_name', 'mean', 'std', 'min', 'max', 'kurtosis', 
                   'skewness', 'sum', 'mad', 'n_zeros', 'n_nan', '5%', '25%', 
                   '75%', '95%', 'iqr', 'coefficient_of_variation', 'p_zeros', 'p_nan', 'bins']
    stats_df = pd.DataFrame(columns=stats_names)

    for column in df.columns:
        stats = calc_statistics(df[column])
        if stats:
            stats_df = stats_df.append(stats, ignore_index=True)

    return stats_df


def save_dataframes(names_list, df_list, base_path, func_name):
    """
    Save dataframes as csv in specific path

    Args:
        names_list: list of csv names
        df_list: list of data frame to save as csv file
        base_path: path to save function output
        func_name: name of decorated function

    """
    csv_path = os.path.join(base_path, func_name)

    if not os.path.exists(csv_path):
        os.mkdir(csv_path)

    for idx, df in enumerate(df_list):
        df.to_csv(os.path.join(csv_path, names_list[idx] + '.csv'))
