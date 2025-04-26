import pandas as pd
import numpy as np
from utils.config_loader import ConfigLoader
from typing import Dict, Any, List, Optional, Tuple
import streamlit as st

class SalaryDataProcessor:
    """給与データ処理クラス"""

    def __init__(self, file):
        """
        給与データ処理クラスの初期化
        Args:
            file: アップロードされたCSVファイル
        """
        try:
            self.df = pd.read_csv(file, encoding='cp932')
            self.config = ConfigLoader()
            self.summary = None
            self.processed = False
        except Exception as e:
            st.error(f"初期化エラー: {str(e)}")
            raise

    def _validate_columns(self) -> bool:
        """
        カラムの検証
        Returns:
            bool: 検証結果
        """
        required_columns = self.config.get_required_columns('salary')
        if not required_columns:
            st.error("必須カラムの設定が見つかりません")
            return False

        missing_columns = [col for col in required_columns if col not in self.df.columns]
        if missing_columns:
            st.error(f"必須カラムが不足しています: {', '.join(missing_columns)}")
            return False

        return True

    def _convert_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        数値カラムの変換
        Args:
            df: 入力データフレーム
        Returns:
            pd.DataFrame: 変換後のデータフレーム
        """
        numeric_columns = self.config.get_numeric_columns('salary')
        if not numeric_columns:
            return df

        for col in numeric_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(
                        df[col].astype(str).replace(r'[^\d.-]', '', regex=True),
                        errors='coerce'
                    ).fillna(0)
                except Exception as e:
                    st.warning(f"カラム '{col}' の数値変換でエラーが発生しました")
                    df[col] = 0

        return df

    def _calculate_totals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        合計列の計算
        Args:
            df: 入力データフレーム
        Returns:
            pd.DataFrame: 計算後のデータフレーム
        """
        try:
            # 手当グループごとの合計を計算
            total_columns = self.config.get_settings('salary', 'calculations.total_columns')
            if total_columns:
                for total_name, group_name in total_columns.items():
                    group_columns = self.config.get_settings('salary', f'input_columns.salary_items.{group_name}')
                    if group_columns:
                        existing_columns = [col for col in group_columns if col in df.columns]
                        if existing_columns:
                            df[total_name] = df[existing_columns].fillna(0).sum(axis=1)
                            # st.success(f"{total_name}の計算が完了しました")

            # 支給総額の計算
            payment_columns = ['基本給'] + [col for col in ['資格手当合計', '時間外勤務手当合計', 'その他手当合計', '通勤手当合計'] if col in df.columns]
            if payment_columns:
                df['支給総額'] = df[payment_columns].fillna(0).sum(axis=1)
                # st.success("支給総額の計算が完了しました")

            # 差引支給額と振込金額の計算
            if '支給総額' in df.columns and '控除合計' in df.columns:
                df['差引支給額'] = df['支給総額'] - df['控除合計']
                df['振込金額1'] = df['差引支給額']
                # st.success("差引支給額と振込金額の計算が完了しました")

            return df

        except Exception as e:
            st.error(f"合計計算でエラーが発生しました: {str(e)}")
            return df

    def _transform_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        列の変換処理
        Args:
            df: 入力データフレーム
        Returns:
            pd.DataFrame: 変換後のデータフレーム
        """
        try:
            # 列名の変換（設定がなければスキップ）
            try:
                rename_rules = self.config.get_settings('salary', 'transformations.columns_rename')
            except Exception:
                rename_rules = None
            if rename_rules:
                df = df.rename(columns=rename_rules)
                # st.success("列名の変換が完了しました")

            # 条件付き変換とコード変換（メソッドがなければスキップ）
            if hasattr(self, '_apply_conditional_rules'):
                try:
                    self._apply_conditional_rules(df)
                except Exception:
                    pass
            if hasattr(self, '_apply_code_mappings'):
                try:
                    self._apply_code_mappings(df)
                except Exception:
                    pass

            return df

        except Exception as e:
            st.error(f"列変換でエラーが発生しました: {str(e)}")
            return df

    def _calculate_summary(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        サマリーデータの計算
        Args:
            df: 入力データフレーム
        Returns:
            Optional[pd.DataFrame]: 集計データ
        """
        try:
            if df.empty:
                return None

            # 集計グループの設定
            group_cols = ['部門', '部署', '雇用区分']
            existing_cols = [col for col in group_cols if col in df.columns]
            
            if not existing_cols:
                st.warning("集計に必要なカラムが見つかりません")
                return None

            # st.info(f"{', '.join(existing_cols)}で集計を実行します")

            # 集計の実行
            agg_dict = {
                'コード': 'count',
                '支給総額': 'sum' if '支給総額' in df.columns else None,
                '振込金額': 'sum' if '振込金額' in df.columns else None
            }
            agg_dict = {k: v for k, v in agg_dict.items() if v is not None}

            summary = df.groupby(existing_cols, as_index=False).agg(agg_dict)
            summary = summary.rename(columns={'コード': '支給人数'})

            # 一人当たり支給額の計算
            if '支給総額' in summary.columns:
                summary['一人当たり支給額'] = (summary['支給総額'] / summary['支給人数']).fillna(0)

            # 合計行の追加
            total_row_dict = {
                '雇用区分': '合計',
                '支給人数': summary['支給人数'].sum()
            }
            for col in ['支給総額', '振込金額']:
                if col in summary.columns:
                    total_row_dict[col] = summary[col].sum()
                else:
                    total_row_dict[col] = 0  # 存在しない場合は0

            if '一人当たり支給額' in summary.columns:
                if total_row_dict['支給人数'] > 0:
                    total_row_dict['一人当たり支給額'] = total_row_dict.get('支給総額', 0) / total_row_dict['支給人数']
                else:
                    total_row_dict['一人当たり支給額'] = 0

            total_row = pd.DataFrame([total_row_dict])
            summary = pd.concat([summary, total_row], ignore_index=True)

            # 数値の整形
            numeric_cols = ['支給人数', '支給総額', '振込金額', '一人当たり支給額']
            for col in numeric_cols:
                if col in summary.columns:
                    summary[col] = summary[col].fillna(0).round(0).astype(int)

            # st.success("集計処理が完了しました")
            return summary

        except Exception as e:
            st.error(f"集計処理でエラーが発生しました: {str(e)}")
            return None

    def process_data(self) -> Optional[pd.DataFrame]:
        """
        データ処理の実行
        Returns:
            Optional[pd.DataFrame]: 処理済みデータフレーム
        """
        try:
            if not self._validate_columns():
                return None

            # st.info("データ処理を開始します")
            processed_df = self.df.copy()
            
            # 1. 数値変換
            processed_df = self._convert_numeric_columns(processed_df)
            
            # 2. 合計計算
            processed_df = self._calculate_totals(processed_df)
            
            # 3. 列変換
            processed_df = self._transform_columns(processed_df)
            
            # 4. カラム順序の変更
            output_columns = self.config.get_settings('salary', 'output_settings.columns_order')
            # if output_columns:
            #     existing_columns = [col for col in output_columns if col in processed_df.columns]
            #     remaining_columns = [col for col in processed_df.columns if col not in existing_columns]
            processed_df = processed_df[output_columns]
                # st.success("カラム順序の変更が完了しました")

            # 5. サマリーの計算
            self.summary = self._calculate_summary(processed_df)
            
            # 処理完了フラグを設定
            self.processed = True
            self.df = processed_df

            # st.success("全ての処理が完了しました")
            return processed_df

        except Exception as e:
            st.error(f"データ処理エラー: {str(e)}")
            return None

    def process_uploaded_data(self) -> bool:
        """
        アップロードされたデータの処理を実行
        Returns:
            bool: 処理が成功した場合はTrue
        """
        try:
            processed_df = self.process_data()
            if processed_df is None:
                return False
                
            return True

        except Exception as e:
            st.error(f"データ処理エラー: {str(e)}")
            return False

    def _apply_conditional_rules(self, df: pd.DataFrame) -> None:
        """
        設定ファイルのconditional_rulesに従い、条件付き変換を行う
        """
        try:
            rules_dict = self.config.get_settings('salary', 'transformations.conditional_rules')
            if not rules_dict:
                return
            for group, rules in rules_dict.items():
                for rule in rules:
                    conditions = rule.get('conditions', {})
                    target = rule.get('target')
                    value = rule.get('value')
                    source = rule.get('source')
                    if not target or not conditions:
                        continue
                    # 条件に一致する行を抽出
                    mask = pd.Series([True] * len(df))
                    for col, cond_val in conditions.items():
                        if isinstance(cond_val, str) and cond_val.startswith('>'):
                            try:
                                num = float(cond_val[1:].strip())
                                mask &= df[col].astype(float) > num
                            except Exception:
                                mask &= False
                        elif isinstance(cond_val, str) and cond_val.startswith('<'):
                            try:
                                num = float(cond_val[1:].strip())
                                mask &= df[col].astype(float) < num
                            except Exception:
                                mask &= False
                        else:
                            mask &= df[col] == cond_val
                    if value is not None:
                        df.loc[mask, target] = value
                    elif source is not None and source in df.columns:
                        df.loc[mask, target] = df.loc[mask, source]
        except Exception as e:
            st.warning(f"条件付き変換でエラー: {str(e)}")

    def _apply_code_mappings(self, df: pd.DataFrame) -> None:
        """
        設定ファイルのcode_mappingsに従い、コード値を名称等に変換する
        """
        try:
            mappings = self.config.get_settings('salary', 'transformations.code_mappings')
            if not mappings:
                return
            for col, mapping in mappings.items():
                if col in df.columns:
                    df[col] = df[col].map(mapping)
        except Exception as e:
            st.warning(f"コード変換でエラー: {str(e)}")