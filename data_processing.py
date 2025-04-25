# data_processing.py

import pandas as pd


def load_df(file, encoding='cp932'):
    """
        指定されたファイルからDataFrameを読み込む。

        :param file: 読み込むファイルのパス
        :param encoding: ファイルのエンコーディング（デフォルトは 'cp932'）
        :return: 読み込まれたpandas DataFrame
        """
    return pd.read_csv(file, encoding=encoding)


def add_total_column(df, column_list, new_column_name):
    """
        指定された複数の列の合計を新しい列として追加する。

        :param df: データフレーム
        :param column_list: 合計する列のリスト
        :param new_column_name: 新しい列の名前
        :return: 更新されたデータフレーム
        """
    df[new_column_name] = df[column_list].sum(axis=1)
    return df


def rearrange_columns(df, new_order):
    """
    データフレームの列を指定された順序で並べ替える。

    :param df: データフレーム
    :param new_order: 新しい列順のリスト
    :return: 列が並べ替えられたデータフレーム
    """
    return df[new_order]


def rename_columns(df, rename_dict):
    """
    データフレームの列名を指定された新しい名前に変更する。

    :param df: データフレーム
    :param rename_dict: 旧列名と新列名のマッピングを含む辞書
    :return: 列名が変更されたデータフレーム
    """
    return df.rename(columns=rename_dict)


def replace_values(df, column, replace_dict):
    """
    指定した列の値を辞書に基づいて置換する。

    :param df: データフレーム。
    :param column: 値を置換する列名。
    :param replace_dict: 置換ルールの辞書。
    :return: 値が置換されたデータフレーム。
    """
    df[column] = df[column].replace(replace_dict)
    return df


def conditional_replace(df, condition, target_column, new_value):
    """
    条件に基づいて特定の列の値を変更する。

    :param df: データフレーム。
    :param condition: 変更を適用する条件（ブール式）。
    :param target_column: 値を変更する列名。
    :param new_value: 新しい値。
    :return: 値が変更されたデータフレーム。
    """
    # 条件に基づいて指定された列の値を変更
    if callable(new_value):
        # 新しい値がラムダ式の場合、データフレームに適用
        df[target_column] = df[target_column].mask(condition, new_value(df))
    else:
        # 新しい値が固定値の場合
        df[target_column] = df[target_column].mask(condition, new_value)
    return df


def convert_df_to_csv(df, index=False):
    """
    DataFrameをCSV形式に変換する。

    :param df: 変換するpandas DataFrame
    :param index: CSV出力にインデックスを含めるかどうか
    :return: CSV形式の文字列
    """
    return df.to_csv(index=index).encode('cp932')
