import pandas as pd
import numpy as np
from utils.config_loader import ConfigLoader
from typing import Dict, Any, List, Optional, Tuple
import streamlit as st
import unicodedata



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

    def _normalize(self, col):
        col = unicodedata.normalize('NFKC', str(col))
        col = col.strip().replace(' ', '').replace('\u3000', '')
        return col

    def _validate_columns(self) -> bool:
        """
        カラムの検証
        Returns:
            bool: 検証結果
        """
        required_columns = self.config.get_settings('salary', 'input.required_columns')
        if not required_columns:
            st.error("必須カラムの設定が見つかりません")
            return False
        
        csv_columns = [self._normalize(col) for col in self.df.columns]
        required_columns_normalized = [self._normalize(col) for col in required_columns]

        # デバッグ用出力
        # st.write('csv columns:', csv_columns)
        # st.write('required columns:', required_columns_normalized)

        missing_columns = [col for col, norm_col in zip(required_columns, required_columns_normalized) if norm_col not in csv_columns]
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
        numeric_columns = self.config.get_settings('salary', 'input.numeric_columns')
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
                    group_columns = self.config.get_settings('salary', f'input_columns.groups.{group_name}')
                    if group_columns:
                        existing_columns = [col for col in group_columns if col in df.columns]
                        if not existing_columns:
                            st.warning(f"{total_name}の計算対象カラムが見つかりません: {group_columns}")
                        else:
                            if total_name in df.columns:
                                df.drop(columns=[total_name], inplace=True)
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
            if '振込金額' in df.columns:
                df.drop(columns=['振込金額'], inplace=True)
            df['振込金額'] = df['差引支給額'] - df['差引支給＿負']
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
            rename_rules = self.config.get_settings('salary', 'transformations.columns_rename')
            if rename_rules:
                # 変換前の列名が存在するか確認
                existing_columns = df.columns.tolist()
                valid_rules = {
                    old_col: new_col
                    for old_col, new_col in rename_rules.items()
                    if old_col in existing_columns
                }

                if valid_rules:
                    df = df.rename(columns=valid_rules)

                    for old_col, new_col in valid_rules.items():
                        if new_col not in df.columns:
                            st.warning(f'列名の変換に失敗: {old_col} -> {new_col}')
                else:
                    st.warning('有効な列名変換ルールが見つかりません')
                
            # 条件付き変換とコード変換（メソッドがなければスキップ）
            if hasattr(self, '_apply_code_mappings'):
                try:
                    self._apply_code_mappings(df)
                except Exception:
                    st.waring(f'コードマッピングでエラーが発生: str{e}')

            if hasattr(self, '_apply_conditional_rules'):
                try:
                    self._apply_conditional_rules(df)
                except Exception:
                    st.waring(f'コード変換でエラーが発生: str{e}')
                    pass


            required_columns = self.config.get_settings('salary', 'output_settings.detail.required_columns')
            if required_columns:
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    st.error(f'返還後の必須カラムが見つかりません: {", ".join(missing_columns)}')
            else:
                st.warning('必須カラムの設定が見つかりません')

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
                '差引支給＿負': 'sum' if '差引支給＿負' in df.columns else None,
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

    def process_data(self) -> tuple:
        """
        データ処理の実行
        Returns:
            tuple: (全項目用データフレーム, サマリー用データフレーム)
        """
        try:
            if not self._validate_columns():
                return None, None

            # st.info("データ処理を開始します")
            processed_df = self.df.copy()
            
            # 1. 数値変換
            processed_df = self._convert_numeric_columns(processed_df)
            # processed_df[processed_df.columns != self.config.get_settings('salary', 'input.numeric_columns')] = processed_df[processed_df.columns != self.config.get_settings('salary', 'input.numeric_columns')].astype(str)


            # 2. 合計計算
            processed_df = self._calculate_totals(processed_df)
            #  デバッグ：合計計算後
            if any(processed_df.columns.duplicated()):
                st.error(f"[合計計算後] 重複カラム: {processed_df.columns[processed_df.columns.duplicated()].tolist()}")

            # 3. 列変換
            processed_df = self._transform_columns(processed_df)
            # デバッグ：列変換後
            if any(processed_df.columns.duplicated()):
                st.error(f"[列変換後] 重複カラム: {processed_df.columns[processed_df.columns.duplicated()].tolist()}")

            # 4. カラム順序の変更
            output_columns_detail = self.config.get_settings('salary', 'output_settings.detail.columns_order')
            processed_df_detail = processed_df[output_columns_detail]
            # デバッグ：カラム順序変更後
            if any(processed_df_detail.columns.duplicated()):
                st.error(f"[カラム順序変更後] 重複カラム: {processed_df_detail.columns[processed_df_detail.columns.duplicated()].tolist()}")
            output_columns_summary = self.config.get_settings('salary', 'output_settings.summary.columns_order')
            processed_df_summary = processed_df[output_columns_summary]

            # 5. サマリーの計算
            self.summary = self._calculate_summary(processed_df_summary)
            
            # 処理完了フラグを設定
            self.processed = True

            # st.success("全ての処理が完了しました")
            return processed_df_detail, processed_df_summary

        except Exception as e:
            st.error(f"データ処理エラー: {str(e)}")
            return None, None

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
        Args:
            df: 変換対象のデータフレーム
        """
        try:
            mappings = self.config.get_settings('salary', 'transformations.code_mappings')
            if not mappings:
                st.warning("コード変換マッピングが設定されていません")
                return


            # 部門コードの変換
            if 'department_code' in mappings and  '部門コード' in df.columns:
                dept_mapping = mappings['department_code']
                df['部門コード'] = df['部門コード'].astype(int).map(
                    lambda x: dept_mapping.get(str(x), str(x))
                )

            # 部署コードの変換
            if 'section_code' in mappings and '部署コード' in df.columns:
                section_mapping = mappings['section_code']
                df['部署コード'] = df['部署コード'].astype(int).map(
                    lambda x: section_mapping.get(str(x), str(x))
                )

            # 結果の確認
            for mapping_type, code_map in mappings.items():
                if mapping_type == 'department_code':
                    target_col = '部門コード'
                elif mapping_type == 'section_code':
                    target_col = '部署コード'
                else:
                    continue

                if target_col in df.columns:
                    # 現在の値取得（NaN以外）
                    current_codes = set(df[target_col].dropna().astype(str).unique())
                    unmapped = current_codes - {str(k) for k in code_map.keys()}
                    if unmapped:
                        st.warning(f"{mapping_type}の変換に失敗したコード: {unmapped}")
                    str_code_map = {str(k): v for k, v in code_map.items()}
                    df[target_col] = df[target_col].astype(str).map(
                        lambda x: str_code_map.get(x, x) if pd.notna(x) else x
                    )
        except Exception as e:
            st.warning(f"コード変換でエラー: {str(e)}")