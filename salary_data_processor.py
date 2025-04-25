import pandas as pd
import numpy as np
from utils.config_loader import ConfigLoader
from typing import Dict, Any, List, Optional
from base_data_processor import BaseDataProcessor
from calculations import (df_output_summary, journal_post, post_eom,
                          get_payee_count, get_total_payment,
                          get_total_transfer_amount)
import streamlit as st


class SalaryDataProcessor(BaseDataProcessor):
    """給与データ処理クラス"""

    def __init__(self, file):
        """
        給与データ処理クラスの初期化
        Args:
            file: アップロードされたCSVファイル
        """
        try:
            super().__init__(file)
            self.config = ConfigLoader()
            self._validate_required_columns()
            self._initialize_numeric_columns()
            self.total_payee = 0
            self.total_payment = 0
            self.total_transfer_amount = 0
            self.per_person_payment = 0
            self.summary = None
            self.post_eom_data = None
            self.journal = None
        except Exception as e:
            st.error(f"初期化エラー: {str(e)}")
            raise

    def _validate_required_columns(self) -> None:
        """必須カラムの存在チェック"""
        try:
            required_columns = self.config.get_required_columns('salary')
            if required_columns is None:
                st.warning("必須カラムの設定が見つかりません")
                return

            # カラム名の正規化（大文字小文字、全角半角、スペースを統一）
            def normalize_column_name(col):
                import unicodedata
                # 全角を半角に変換
                col = unicodedata.normalize('NFKC', str(col))
                # 小文字に変換
                col = col.lower()
                # スペースを削除
                col = col.replace(' ', '')
                return col

            # 現在のカラム名を正規化して辞書を作成
            df_columns = {normalize_column_name(col): col for col in self.df.columns}
            
            # 必須カラムを正規化して確認
            missing_columns = []
            rename_mapping = {}
            
            for col in required_columns:
                norm_col = normalize_column_name(col)
                if norm_col not in df_columns:
                    missing_columns.append(col)
                else:
                    # 元のカラム名と異なる場合、リネーム用マッピングに追加
                    current_name = df_columns[norm_col]
                    if current_name != col:
                        rename_mapping[current_name] = col

            if missing_columns:
                raise ValueError(f"必須カラムが不足しています: {', '.join(missing_columns)}")

            # カラム名の標準化
            if rename_mapping:
                self.df = self.df.rename(columns=rename_mapping)
                st.info(f"以下のカラム名を標準化しました：\n{', '.join(f'{k} → {v}' for k, v in rename_mapping.items())}")

        except Exception as e:
            st.error(f"カラム検証エラー: {str(e)}")
            raise

    def _initialize_numeric_columns(self) -> None:
        """数値カラムの初期化"""
        try:
            numeric_columns = self.config.get_numeric_columns('salary')
            if not numeric_columns:
                st.warning("数値カラムの設定が見つかりません")
                return

            # 基本の数値カラムのみを初期化（合計列は除外）
            total_columns = self.config.get_settings('salary', 'aggregations.total_columns')
            final_columns = self.config.get_settings('salary', 'aggregations.final_columns')
            
            # 合計列を除外した数値カラムリストを作成
            exclude_columns = []
            if total_columns:
                exclude_columns.extend(total_columns.keys())
            if final_columns:
                exclude_columns.extend(final_columns)
            
            base_numeric_columns = [col for col in numeric_columns if col not in exclude_columns]

            # 基本の数値カラムの初期化
            columns_to_initialize = []
            for col in base_numeric_columns:
                if col not in self.df.columns:
                    columns_to_initialize.append(col)
                else:
                    try:
                        # 数値以外の文字を除去して数値変換
                        self.df[col] = self.df[col].astype(str).replace(r'[^\d.-]', '', regex=True)
                        self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
                    except Exception as e:
                        st.warning(f"カラム '{col}' の数値変換でエラーが発生しました：{str(e)}")
                        self.df[col] = 0

            # 初期化されたカラムの警告表示
            if columns_to_initialize:
                st.warning(f"以下の基本数値カラムが見つからないため、0で初期化しました：\n{', '.join(columns_to_initialize)}")
                for col in columns_to_initialize:
                    self.df[col] = 0

        except Exception as e:
            st.warning(f"数値カラムの初期化で警告: {str(e)}")

    def _calculate_total_columns(self, df: pd.DataFrame) -> None:
        """合計列の計算"""
        try:
            # 集計定義を取得
            total_columns = self.config.get_settings('salary', 'aggregations.total_columns')
            if not total_columns:
                st.warning("合計列の設定が見つかりません")
                return

            # 各手当グループの合計を計算
            total_results = {}
            for total_name, group_name in total_columns.items():
                if total_name != '支給総額':  # 支給総額は後で計算
                    # input_columnsから該当するグループのカラムを取得
                    group_columns = self.config.get_settings('salary', f'input_columns.salary_items.{group_name}')
                    if group_columns:
                        # 存在するカラムのみを使用
                        existing_columns = [col for col in group_columns if col in df.columns]
                        if existing_columns:
                            # 合計を計算
                            df[total_name] = df[existing_columns].fillna(0).sum(axis=1)
                            total_results[total_name] = existing_columns
                            st.info(f"{total_name}の計算に使用したカラム: {', '.join(existing_columns)}")
                        else:
                            st.warning(f"{total_name}の計算に必要なカラムが見つかりません")
                            df[total_name] = 0
                    else:
                        st.warning(f"{group_name}グループの設定が見つかりません")
                        df[total_name] = 0

            # 支給総額の計算（基本給 + 各種手当合計）
            payment_columns = ['基本給']
            for total_name in ['資格手当合計', '時間外勤務手当合計', 'その他手当合計', '通勤手当合計']:
                if total_name in df.columns:
                    payment_columns.append(total_name)
            
            if payment_columns:
                df['支給総額'] = df[payment_columns].fillna(0).sum(axis=1)
                st.info(f"支給総額の計算に使用したカラム: {', '.join(payment_columns)}")

            # 差引支給額の計算
            if '支給総額' in df.columns and '控除合計' in df.columns:
                df['差引支給額'] = df['支給総額'] - df['控除合計']
                st.info("差引支給額 = 支給総額 - 控除合計")

            # 振込金額の設定（差引支給額と同じ）
            if '差引支給額' in df.columns:
                df['振込金額1'] = df['差引支給額']  # 変換前の列名を使用
                st.info("振込金額1 = 差引支給額")

            # 合計列の数値変換を確認
            for col in df.columns:
                if col in total_columns or col in ['支給総額', '差引支給額', '振込金額1']:
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    except Exception as e:
                        st.warning(f"合計列 '{col}' の数値変換でエラーが発生しました：{str(e)}")

        except Exception as e:
            st.error(f"合計列の計算エラー: {str(e)}")
            raise

    def _rename_columns(self, df: pd.DataFrame) -> None:
        """列名の変換"""
        try:
            # 列名の変換ルールを取得
            rename_rules = self.config.get_columns_rename('salary')
            if not rename_rules:
                st.warning("列名変換ルールが見つかりません")
                return

            # 変換前の列名を記録
            original_columns = set(df.columns)

            # 列名の変換を実行
            df.rename(columns=rename_rules, inplace=True)

            # 変換された列名を記録
            renamed_columns = {old: new for old, new in rename_rules.items() if old in original_columns}
            if renamed_columns:
                st.info(f"以下の列名を変換しました：\n{', '.join(f'{k} → {v}' for k, v in renamed_columns.items())}")

        except Exception as e:
            st.error(f"列名変換エラー: {str(e)}")
            raise

    def _process_segments(self, df: pd.DataFrame) -> None:
        """セグメント情報の処理"""
        try:
            segments = self.config.get_settings('salary', 'segments')
            if not segments:
                st.warning("セグメント設定が見つかりません")
                return

            # セグメントカラムの初期化
            df['セグメント'] = '0'
            df['セグメント名'] = '未分類'

            # デフォルトのセグメントマッピング
            if 'default' in segments:
                df['セグメント'] = df['セグメント'].map(lambda x: segments['default'].get(str(x), x))

            # 部門別ルールの適用
            if 'department_rules' in segments:
                for rule in segments['department_rules']:
                    conditions = rule.get('conditions', {})
                    if 'department' in conditions:
                        dept_code = conditions['department']
                        df.loc[df['部門コード'] == dept_code, 'セグメント'] = rule['segment']
                    elif 'departments' in conditions:
                        dept_codes = conditions['departments']
                        df.loc[df['部門コード'].isin(dept_codes), 'セグメント'] = rule['segment']

            # セグメント名称の設定
            if 'names' in segments:
                df['セグメント名'] = df['セグメント'].map(lambda x: segments['names'].get(str(x), f"不明なセグメント: {x}"))

        except Exception as e:
            st.error(f"セグメント処理エラー: {str(e)}")
            raise

    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        設定に基づいてカラムの順序を変更
        Args:
            df (pd.DataFrame): 入力データフレーム
        Returns:
            pd.DataFrame: カラム順序を変更したデータフレーム
        """
        try:
            # 基本情報カラム
            basic_info = self.config.get_settings('salary', 'input_columns.basic_info') or []
            
            # 給与項目カラム（変換後の名称）
            salary_items = []
            for group in ['base', 'qualification', 'overtime', 'allowance', 'commute']:
                items = self.config.get_settings('salary', f'input_columns.salary_items.{group}') or []
                salary_items.extend(items)

            # 合計カラム
            total_columns = list(self.config.get_settings('salary', 'aggregations.total_columns').keys()) if self.config.get_settings('salary', 'aggregations.total_columns') else []

            # 最終カラム
            final_columns = ['支給総額', '控除合計', '差引支給額', '振込金額1']

            # 全出力カラムの順序を結合
            output_columns = basic_info + salary_items + total_columns + final_columns

            # 列名の変換ルールを適用
            rename_rules = self.config.get_columns_rename('salary')
            output_columns = [rename_rules.get(col, col) for col in output_columns]

            # 存在するカラムのみを抽出
            existing_columns = [col for col in output_columns if col in df.columns]
            missing_columns = [col for col in output_columns if col not in df.columns]
            extra_columns = [col for col in df.columns if col not in output_columns]

            # 警告メッセージの表示
            if missing_columns:
                st.warning(f"以下の出力カラムが見つかりません: {', '.join(missing_columns)}")
            if extra_columns:
                st.info(f"以下のカラムは出力カラム定義に含まれていないため、出力から除外されます: {', '.join(extra_columns)}")

            # カラム順序の変更
            return df[existing_columns].copy()

        except Exception as e:
            st.error(f"カラム順序変更エラー: {str(e)}")
            return df

    def _calculate_summary_data(self) -> pd.DataFrame:
        """
        部門別の集計データを計算
        Returns:
            pd.DataFrame: 集計データ
        """
        try:
            if self.df is None or self.df.empty:
                st.warning("集計可能なデータがありません")
                return pd.DataFrame()

            # 集計用のデータフレームを準備（重複カラムを削除）
            df_summary = self.df.loc[:, ~self.df.columns.duplicated()].copy()

            # 集計グループを決定
            group_cols = None
            if all(col in df_summary.columns for col in ['部署コード', '部署コード名']):
                group_cols = ['部署コード', '部署コード名']
            elif all(col in df_summary.columns for col in ['部門コード', '部門名']):
                group_cols = ['部門コード', '部門名']
            elif all(col in df_summary.columns for col in ['ｾｸﾞﾒﾝﾄ', 'ｾｸﾞﾒﾝﾄ名']):
                group_cols = ['ｾｸﾞﾒﾝﾄ', 'ｾｸﾞﾒﾝﾄ名']

            if not group_cols:
                st.warning("集計に必要な部門コード、部署コード、またはセグメントのカラムが見つかりません")
                return pd.DataFrame()

            # 集計対象の列を定義
            numeric_cols = []
            
            # 基本給と支給総額を追加（存在する場合のみ）
            if '基本給' in df_summary.columns:
                numeric_cols.append('基本給')
            if '支給総額' in df_summary.columns:
                numeric_cols.append('支給総額')

            # 集計の実行
            try:
                # 基本の集計を実行
                agg_dict = {col: 'sum' for col in numeric_cols}
                agg_dict['コード'] = 'count'  # 支給人数のカウント

                # グループごとの集計を実行
                summary = df_summary.groupby(group_cols, as_index=False).agg(agg_dict)

                # カラム名の設定
                summary = summary.rename(columns={'コード': '支給人数'})

                # 一人当たりの支給額を計算
                if '支給総額' in summary.columns:
                    summary['一人当たり支給額'] = (summary['支給総額'] / summary['支給人数']).fillna(0)

                # 合計行の作成
                total_row = pd.DataFrame([{
                    group_cols[0]: '合計',
                    group_cols[1]: '全体',
                    '支給人数': summary['支給人数'].sum()
                }])

                # 数値列の合計を計算
                for col in numeric_cols:
                    if col in summary.columns:
                        total_row[col] = summary[col].sum()

                # 一人当たり支給額の計算（合計行）
                if '支給総額' in total_row.columns:
                    total_row['一人当たり支給額'] = (
                        total_row['支給総額'] / total_row['支給人数']
                        if total_row['支給人数'].iloc[0] > 0 else 0
                    )

                # 合計行を追加
                summary = pd.concat([summary, total_row], ignore_index=True)

                # 数値列を整形
                numeric_format_cols = ['支給人数', '一人当たり支給額'] + numeric_cols
                for col in numeric_format_cols:
                    if col in summary.columns:
                        summary[col] = summary[col].round(0).astype(int)

                # クラス変数に保存
                self.department_summary = summary.copy()
                last_row = summary.iloc[-1]
                self.total_payee = int(last_row['支給人数'])
                self.total_payment = int(last_row['支給総額']) if '支給総額' in last_row else 0
                self.per_person_payment = int(last_row['一人当たり支給額']) if '一人当たり支給額' in last_row else 0

                return summary

            except Exception as e:
                st.error(f"集計処理でエラーが発生しました: {str(e)}")
                return pd.DataFrame()

        except Exception as e:
            st.error(f"集計データの計算エラー: {str(e)}")
            return pd.DataFrame()

    def display_summary(self) -> None:
        """サマリー情報を表示"""
        try:
            if not self.validate_dataframe():
                st.warning("集計可能なデータがありません")
                return

            # サマリーデータの計算
            self._calculate_summary_data()
            
            if hasattr(self, 'department_summary') and not self.department_summary.empty:
                # 全体サマリーの表示
                st.write("### 給与支給サマリー")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("総支給人数", f"{self.total_payee:,}人")
                with col2:
                    st.metric("支給総額", f"{self.total_payment:,.0f}円")
                with col3:
                    st.metric("一人当たり支給額", f"{self.per_person_payment:,.0f}円")

                # 部門別集計の表示
                st.write("### 部門別集計")
                st.dataframe(
                    self.department_summary.style.format({
                        '支給人数': '{:,}人',
                        '支給総額': '{:,.0f}円',
                        '一人当たり支給額': '{:,.0f}円'
                    })
                )

                # 課別集計の表示
                if hasattr(self, 'section_summary') and not self.section_summary.empty:
                    st.write("### 課別集計")
                    st.dataframe(
                        self.section_summary.style.format({
                            '支給人数': '{:,}人',
                            '支給総額': '{:,.0f}円',
                            '一人当たり支給額': '{:,.0f}円'
                        })
                    )

                # 処理済みデータの表示（開発用）
                if st.checkbox("処理済みデータを表示"):
                    st.write("### 処理済みデータ（確認用）")
                    st.dataframe(
                        self.df.style.format({
                            col: '{:,.0f}' for col in [
                                '基本給', '基本給合計', '手当合計', '支給総額'
                            ] if col in self.df.columns
                        })
                    )
            else:
                st.warning("集計可能なデータがありません")

        except Exception as e:
            st.error(f"サマリー表示でエラーが発生しました: {str(e)}")

    def convert_to_csv(self) -> bytes:
        """
        処理済みデータをCSV形式に変換
        Returns:
            bytes: CSV形式のバイトデータ
        """
        try:
            if not self.validate_dataframe():
                raise ValueError("有効なデータフレームが存在しません")

            # 出力用のDataFrameを作成
            output_df = self.df.copy()

            # 数値カラムのフォーマット
            numeric_columns = [
                '基本給', '基本給合計', '手当合計', '支給総額'
            ]
            for col in numeric_columns:
                if col in output_df.columns:
                    output_df[col] = output_df[col].round(0).astype(int)

            # CSVに変換
            return output_df.to_csv(index=False, encoding='utf-8-sig')

        except Exception as e:
            st.error(f"CSV変換エラー: {str(e)}")
            raise

    def validate_dataframe(self) -> bool:
        """
        データフレームの有効性を検証
        Returns:
            bool: データフレームが有効な場合はTrue
        """
        try:
            # 基本チェック
            if self.df is None or self.df.empty:
                st.warning("データが存在しません")
                return False

            # 必須カラムのチェック
            required_columns = self.config.get_required_columns('salary')
            if not required_columns:
                st.warning("必須カラムの設定が見つかりません")
                return False

            missing_columns = [col for col in required_columns if col not in self.df.columns]
            if missing_columns:
                st.error(f"必須カラムが不足しています: {', '.join(missing_columns)}")
                return False

            # 数値カラムのチェック
            numeric_columns = self.config.get_numeric_columns('salary')
            if numeric_columns:
                for col in numeric_columns:
                    if col in self.df.columns:
                        try:
                            # 数値変換を試行
                            pd.to_numeric(self.df[col], errors='raise')
                        except Exception as e:
                            st.error(f"カラム '{col}' に数値以外のデータが含まれています")
                            return False

            # データ内容の基本チェック
            if len(self.df) == 0:
                st.warning("データが0件です")
                return False

            # 重複チェック
            key_columns = ['会社NO', '対象年月', 'コード']
            duplicates = self.df[self.df.duplicated(subset=key_columns, keep=False)]
            if not duplicates.empty:
                st.warning(f"以下のレコードが重複しています：\n{duplicates[key_columns].to_string()}")

            return True

        except Exception as e:
            st.error(f"データフレーム検証エラー: {str(e)}")
            return False

    def __str__(self) -> str:
        """
        クラスの文字列表現を返す
        Returns:
            str: クラスの状態を表す文字列
        """
        status = []
        if self.df is not None:
            status.extend([
                f"レコード数: {len(self.df)}",
                f"カラム数: {len(self.df.columns)}",
                f"総支給人数: {self.total_payee:,}人",
                f"支給総額: {self.total_payment:,.0f}円",
                f"一人当たり支給額: {self.per_person_payment:,.0f}円"
            ])
        else:
            status.append("データなし")

        return "\n".join(status)

    def _apply_code_mappings(self, df: pd.DataFrame) -> None:
        """コード変換ルールの適用"""
        try:
            code_mappings = self.config.get_code_mappings('salary')
            if not code_mappings:
                st.warning("コード変換ルールが見つかりません")
                return

            # 部署コード変換
            if '部署コード' in code_mappings:
                if '部門コード' in df.columns:  # 元の列名
                    df['部署コード'] = df['部門コード'].astype(str).map(
                        lambda x: str(code_mappings['部署コード'].get(x, x))
                    )
                    df['部署コード名'] = df['部署コード'].map(
                        lambda x: f"部署{x}" if x not in code_mappings['部署コード'].values() 
                        else next(k for k, v in code_mappings['部署コード'].items() if str(v) == str(x))
                    )

            # 所属コード変換
            if '所属' in code_mappings:
                if '事業所' in df.columns:  # 元の列名
                    df['所属'] = df['事業所'].astype(str).map(
                        lambda x: str(code_mappings['所属'].get(x, x))
                    )
                    df['所属名'] = df['事業所名'].map(
                        lambda x: f"所属{x}" if x not in code_mappings['所属'].values() 
                        else next(k for k, v in code_mappings['所属'].items() if str(v) == str(x))
                    )

            # セグメントコード変換
            if 'ｾｸﾞﾒﾝﾄ' in code_mappings:
                df['ｾｸﾞﾒﾝﾄ'] = df['部門'].astype(str).map(
                    lambda x: str(code_mappings['ｾｸﾞﾒﾝﾄ'].get(x, x))
                )

                # セグメント名称の設定
                segment_names = self.config.get_segment_names('salary')
                if segment_names:
                    df['ｾｸﾞﾒﾝﾄ名'] = df['ｾｸﾞﾒﾝﾄ'].map(
                        lambda x: segment_names.get(x, f"不明なセグメント: {x}")
                    )

        except Exception as e:
            st.error(f"コード変換エラー: {str(e)}")
            raise

    def _apply_conditional_rules(self, df: pd.DataFrame) -> None:
        """条件付き変換ルールの適用"""
        try:
            conditional_rules = self.config.get_conditional_rules('salary')
            if not conditional_rules:
                st.warning("条件付き変換ルールが見つかりません")
                return

            # 雇用形態に基づくルール
            if 'employment_type' in conditional_rules:
                for rule in conditional_rules['employment_type']:
                    conditions = rule['conditions']
                    mask = pd.Series(True, index=df.index)
                    for col, val in conditions.items():
                        if col in df.columns:
                            mask &= (df[col] == val)
                    if mask.any():
                        df.loc[mask, rule['target']] = rule['value']

            # 部署コードの設定
            if 'department' in conditional_rules:
                for rule in conditional_rules['department']:
                    conditions = rule['conditions']
                    mask = pd.Series(True, index=df.index)
                    for col, val in conditions.items():
                        if col in df.columns:
                            if isinstance(val, list):
                                mask &= df[col].isin(val)
                            else:
                                mask &= (df[col] == val)
                    if mask.any():
                        df.loc[mask, rule['target']] = df.loc[mask, rule['value']]

            # セグメントの設定
            if 'segment' in conditional_rules:
                for rule in conditional_rules['segment']:
                    conditions = rule['conditions']
                    mask = pd.Series(True, index=df.index)
                    for col, val in conditions.items():
                        if col in df.columns:
                            if isinstance(val, str) and val.startswith('>'):
                                # 数値比較の場合
                                threshold = float(val.replace('>', '').strip())
                                mask &= (pd.to_numeric(df[col], errors='coerce') > threshold)
                            elif isinstance(val, list):
                                mask &= df[col].isin(val)
                            else:
                                mask &= (df[col] == val)
                    if mask.any():
                        df.loc[mask, rule['target']] = rule['value']

        except Exception as e:
            st.error(f"条件付き変換ルールの適用エラー: {str(e)}")
            raise

    def process_data(self) -> Optional[pd.DataFrame]:
        """
        データの処理を実行
        Returns:
            Optional[pd.DataFrame]: 処理済みのデータフレーム
        """
        try:
            if not self.validate_dataframe():
                return None

            # 1. 必須カラムの検証（元のカラム名で）
            self._validate_required_columns()

            # 2. 数値列の初期化（変換前のカラム名で）
            self._initialize_numeric_columns()

            # 3. 合計列の計算（変換前のカラム名で）
            self._calculate_total_columns(self.df)

            # 4. 列名の変換を実行
            self._rename_columns(self.df)

            # 5. カラム順序の変更（最終的な並び順）
            self.df = self._reorder_columns(self.df)

            # 6. 集計データの計算
            self.summary = self._calculate_summary_data()

            if self.summary is not None and not self.summary.empty:
                # 支給総額の計算
                self.total_payment = self.summary.loc[self.summary.index[-1], '支給総額']
                self.total_payee = self.summary.loc[self.summary.index[-1], '支給人数']
                self.total_transfer_amount = self.total_payment
                self.per_person_payment = self.total_payment / self.total_payee if self.total_payee > 0 else 0

            return self.df

        except Exception as e:
            st.error(f"データ処理エラー: {str(e)}")
            return None