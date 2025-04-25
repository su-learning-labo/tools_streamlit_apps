# calculations.py

import pandas as pd


def df_output_summary(df, group_by_columns, sum_columns):
    """
    指定された列に基づいてデータフレームを集計します。

    :param df: 集計するデータフレーム
    :param group_by_columns: グループ化する列のリスト
    :param sum_columns: 合計する列のリスト
    :return: 集計されたデータフレーム
    """
    summary_df = df[group_by_columns + sum_columns]
    output_all = summary_df.groupby(group_by_columns).sum()

    return output_all


def journal_post(df, group_by_columns, sum_columns):
    """
        支払仕訳用データの集計を行います。

        :param df: 集計するデータフレーム
        :param group_by_columns: グループ化する列のリスト
        :param sum_columns: 合計する列のリスト
        :return: 集計されたデータフレーム
        """
    journal_df = df[group_by_columns + sum_columns]
    output_journal_payment = journal_df.groupby(group_by_columns).sum()

    return output_journal_payment


def post_eom(df, group_by_columns, melt_columns, exclude_columns=None):
    """
        月末計上仕訳用データの集計を行います。

        :param df: 集計するデータフレーム
        :param group_by_columns: グループ化する列のリスト
        :param melt_columns: メルト処理を行う列のリスト
        :param exclude_columns: 縦変換から除外するカラム名
        :return: 集計されたデータフレーム
        """
    eom_df = df[group_by_columns + melt_columns]
    df_melt = eom_df.melt(id_vars=group_by_columns, var_name='区分', value_name='金額').query('区分 not in @exclude_columns')
    df_post_eom = df_melt.groupby(group_by_columns + ['区分']).sum().sort_values(['区分', '雇用形態'])

    return df_post_eom


def get_payee_count(df):
    """
    データセット内の総受給者数を返します。

    :param df: データフレーム
    :return: 総受給者数
    """
    total_payee = df.shape[0]
    return total_payee

def get_total_payment(df, total_columns):
    """
    データセット内の総支払額を返します。

    :param df: データフレーム
    :param total_columns: 合計計算用カラム
    :return: 総支払額
    """
    total_payment = df[total_columns].sum()
    return total_payment

def get_total_transfer_amount(df, pay_column):
    """
    データセット内の総振込額を返します。


    :param df: データフレーム
    :param pay_column: list
    :return: 総振込額
    """
    total_amount = df[pay_column].sum()
    return total_amount
