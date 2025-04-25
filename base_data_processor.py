# base_data_processor.py

from data_processing import load_df, rearrange_columns, rename_columns, add_total_column, replace_values, conditional_replace
import pandas as pd
from typing import Union
import streamlit as st
import numpy as np
from pathlib import Path


class BaseDataProcessor:
    """データ処理の基底クラス"""

    columns_order = []  # 列の並び替え順序
    columns_rename = {}  # 列名の変更マッピング
    total_columns = {}  # 合計列の追加ルール
    replace_rules = {}  # 値の置き換えルール
    conditional_rules = []  # 条件付き置き換えルール

    def __init__(self, file):
        """
        基底クラスの初期化
        Args:
            file: アップロードされたファイルまたはファイルパス
        """
        try:
            if file is None:
                self.df = pd.DataFrame()
            else:
                # ファイルパスの場合の処理
                if isinstance(file, (str, Path)):
                    file_path = Path(file)
                    if not file_path.exists():
                        raise FileNotFoundError(f"ファイルが見つかりません: {file_path}")
                    file = open(file_path, 'rb')

                # ファイルの読み込みを試行（複数のエンコーディングを試す）
                encodings = ['utf-8', 'utf-8-sig', 'cp932', 'shift-jis']
                last_error = None

                for encoding in encodings:
                    try:
                        # ファイルポインタを先頭に戻す
                        file.seek(0)
                        self.df = pd.read_csv(file, encoding=encoding, dtype=str)
                        st.success(f"ファイルを {encoding} で正常に読み込みました")
                        break
                    except UnicodeDecodeError as e:
                        last_error = e
                        continue
                    except Exception as e:
                        st.error(f"ファイル読み込みエラー ({encoding}): {str(e)}")
                        last_error = e
                        continue
                else:
                    error_msg = f"ファイルの読み込みに失敗しました: {str(last_error)}"
                    st.error(error_msg)
                    raise ValueError(error_msg)

                # ファイルパスの場合はファイルを閉じる
                if isinstance(file, (str, Path)):
                    file.close()
                
                # 空白文字の処理
                self.df = self.df.replace('', np.nan)
                self.df = self.df.fillna('')

                # デバッグ情報
                st.info(f"読み込んだカラム: {', '.join(self.df.columns)}")
                st.info(f"データ件数: {len(self.df)}件")
                
            # 計算サマリーの初期化
            self.calculation_summary = {
                'calculated_items': [],
                'missing_columns': [],
                'excluded_items': [],
                'calculation_warnings': []
            }

        except Exception as e:
            st.error(f"初期化エラー: {str(e)}")
            self.df = pd.DataFrame()
            raise

    def validate_dataframe(self) -> bool:
        """
        データフレームの基本的な検証
        Returns:
            bool: データフレームが有効な場合はTrue
        """
        return self.df is not None and not self.df.empty

    def clear_calculation_summary(self) -> None:
        """計算サマリーをクリア"""
        self.calculation_summary = {
            'calculated_items': [],
            'missing_columns': [],
            'excluded_items': [],
            'calculation_warnings': []
        }

    def add_calculation_info(self, category: str, message: str) -> None:
        """
        計算情報を追加
        Args:
            category: 情報カテゴリ
            message: メッセージ
        """
        if category in self.calculation_summary:
            self.calculation_summary[category].append(message)

    def display_calculation_summary(self) -> None:
        """計算サマリーを表示"""
        if not any(self.calculation_summary.values()):
            return

        st.write("### 処理サマリー")
        
        # 計算項目
        if self.calculation_summary['calculated_items']:
            st.write("#### 計算された項目")
            for item in self.calculation_summary['calculated_items']:
                st.write(f"- {item}")

        # 除外項目
        if self.calculation_summary['excluded_items']:
            st.write("#### 除外された項目")
            for item in self.calculation_summary['excluded_items']:
                st.write(f"- {item}")

        # 不足カラム
        if self.calculation_summary['missing_columns']:
            st.warning("#### 不足しているカラム")
            for item in self.calculation_summary['missing_columns']:
                st.write(f"- {item}")

        # 警告
        if self.calculation_summary['calculation_warnings']:
            st.warning("#### 処理の警告")
            for item in self.calculation_summary['calculation_warnings']:
                st.write(f"- {item}")

    def to_csv(self, index: bool = False) -> bytes:
        """
        DataFrameをCSV形式に変換
        Args:
            index: インデックスを含めるかどうか
        Returns:
            bytes: CSV形式のバイトデータ
        """
        try:
            if not self.validate_dataframe():
                return b""
            return self.df.to_csv(index=index).encode('cp932')
        except Exception as e:
            st.error(f"CSV変換エラー: {str(e)}")
            return b""

    def get_column_names(self) -> list:
        """
        現在のカラム名一覧を取得
        Returns:
            list: カラム名のリスト
        """
        return list(self.df.columns) if self.validate_dataframe() else []

    def process_data(self):
        """
        データ処理の基本メソッド。
        サブクラスでオーバーライドして使用します。
        """
        if not self.validate_dataframe():
            raise ValueError("有効なデータフレームが存在しません")
        return self.df

    def add_total_columns(self):
        for new_column_name, column_list in self.total_columns.items():
            self.df = add_total_column(self.df, column_list, new_column_name)

    def rename_df_columns(self):
        if self.columns_rename:
            self.df = rename_columns(self.df, self.columns_rename)

    def rearrange_df_columns(self):
        if self.columns_order:
            self.df = rearrange_columns(self.df, self.columns_order)

    def apply_replace_values(self):
        for column, replacements in self.replace_rules.items():
            self.df = replace_values(self.df, column, replacements)

    def apply_conditional_replace(self):
        for condition, column, new_value in self.conditional_rules:
            self.df = conditional_replace(self.df, condition(self.df), column, new_value)

