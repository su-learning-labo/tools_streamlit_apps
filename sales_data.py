# models/sales_data.py
from typing import Optional
import pandas as pd
import altair as alt
from config.sales_payment_config import PaymentConfig

class SalesData:
    """売上データの処理を担当するクラス"""
    
    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.config = PaymentConfig()

    def load_data(self, sms_file, shokki_file) -> bool:
        """SMSと織機給与天引きデータを読み込み、結合する"""
        try:
            if sms_file is not None and shokki_file is not None:
                sms_df = self._read_csv_file(sms_file)
                shokki_df = self._read_csv_file(shokki_file)
                shokki_df = self._overwrite_shokki_payment(shokki_df)
                self.df = self._concat_dataframes(sms_df, shokki_df)
                return True
            return False
        except Exception as e:
            print(f"データ読み込みエラー: {e}")
            return False

    @staticmethod
    def _read_csv_file(file) -> pd.DataFrame:
        """CSVファイルを読み込む"""
        return pd.read_csv(file, encoding='cp932', dtype={'HEAD_CD': str, 'SUB_CD': str})

    @staticmethod
    def _overwrite_shokki_payment(df: pd.DataFrame) -> pd.DataFrame:
        """織機給与天引きの支払方法を上書き"""
        df['MEI_NAME_V'] = '織機給与天引き'
        return df

    def _concat_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        """データフレームを結合し、科目コードを追加"""
        df = pd.concat([df1, df2])
        df['ACCOUNT_CD'] = df['HEAD_CD'] + '-' + df['SUB_CD']
        df.loc[df['HEAD_CD'] == '5330', 'ACCOUNT_CD'] = '5330'
        return df

    def filter_data(self, payment_methods: list, include_advance: bool, include_non_sales: bool) -> pd.DataFrame:
        """条件に基づいてデータをフィルタリング"""
        df_filtered = self.df.query('MEI_NAME_V in @payment_methods')
        
        if not include_advance:
            df_filtered = df_filtered.query('KAI_CYCLE <= 1')
        if not include_non_sales:
            df_filtered = df_filtered.query('HEAD_CD != "9999"')
            
        return df_filtered

    def calculate_summary(self, df: pd.DataFrame) -> dict:
        """サマリー情報を計算"""
        return {
            'total_amount': df['SEIKYU_TOTAL'].sum(),
            'customer_count': df['INPUT_NO'].nunique(),
            'average_price': df['SEIKYU_TOTAL'].sum() / df['INPUT_NO'].nunique()
        }

    def create_payment_chart_data(self, df: pd.DataFrame) -> 'pd.DataFrame':
        """支払方法別の棒グラフを作成"""
        grouped_data = pd.pivot_table(
            df, 
            index=['MEI_NAME_V'],
            values='SEIKYU_TOTAL',
            aggfunc='sum'
        ).reset_index()

        return grouped_data
        # return alt.Chart(grouped_data).mark_bar().encode(
        #     y=alt.Y('SEIKYU_TOTAL', title='金額'),
        #     x=alt.X('MEI_NAME_V', title='支払方法'),
        #     tooltip=['SEIKYU_TOTAL']
        # ).interactive().properties(height=600, title='支払手段別請求額')

    def prepare_export_data(self, df: pd.DataFrame) -> dict:
        """エクスポート用のデータを準備"""
        return {
            'preview': self._prepare_preview_data(df),
            'sales': self._prepare_sales_data(df),
            'journal': self._prepare_journal_data(df)
        }

    def _prepare_preview_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """プレビューデータの準備"""
        return self.calc_aggregation(df)

    def _prepare_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """売上データの準備"""
        advance_df = df.query('HEAD_CD != "9999" and KAI_CYCLE > 1')
        return self.calc_aggregation(advance_df)

    def _prepare_journal_data(self, df: pd.DataFrame) -> dict:
        """仕訳データの準備"""
        return {
            'sms': self._prepare_journal_by_type(df, '織機給与天引き", "CTC', False),
            'shokki': self._prepare_journal_by_type(df, '織機給与天引き', True),
            'ctc': self._prepare_journal_by_type(df, 'CTC', True)
        }

    @staticmethod
    def calc_aggregation(df: pd.DataFrame) -> pd.DataFrame:
        """データの集計処理"""
        return pd.pivot_table(
            df,
            index=['MEI_NAME_V', 'HEAD_CD', 'SUB_CD', 'ACCOUNT_CD'],
            values='SEIKYU_TOTAL',
            aggfunc='sum'
        ).reset_index()

    def _prepare_journal_by_type(self, df: pd.DataFrame, payment_type: str, include: bool) -> pd.DataFrame:
        """支払タイプ別の仕訳データ準備"""
        query = f'MEI_NAME_V {"in" if include else "not in"} ["{payment_type}"]'
        filtered_df = df.query(query).filter(['HEAD_CD', 'SUB_CD', 'ACCOUNT_CD', 'ACCHEAD_NAME', 'SEIKYU_TOTAL'])
        return self.calc_aggregation_add_acchead(filtered_df)

    @staticmethod
    def calc_aggregation_add_acchead(df: pd.DataFrame) -> pd.DataFrame:
        """勘定科目を含めた集計"""
        return pd.pivot_table(
            df,
            index=['HEAD_CD', 'SUB_CD', 'ACCOUNT_CD', 'ACCHEAD_NAME'],
            values='SEIKYU_TOTAL',
            aggfunc='sum'
        ).reset_index()