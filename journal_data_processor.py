class JournalDataProcessor(BaseDataProcessor):
    """会計システム連携データ処理クラス"""

    def __init__(self, df: pd.DataFrame = None):
        """
        会計システム連携データ処理クラスの初期化
        Args:
            df: 処理対象のDataFrame
        """
        try:
            # 親クラスの__init__をスキップし、直接DataFrameを設定
            self.df = df.copy() if df is not None else pd.DataFrame()
            self.calculation_summary = {
                'calculated_items': [],
                'missing_columns': [],
                'excluded_items': [],
                'calculation_warnings': []
            }
            self.encoding = 'cp932'
        except Exception as e:
            st.error(f"初期化エラー: {str(e)}")
            raise

    def process_data(self) -> pd.DataFrame:
        """
        データ処理のメインメソッド
        Returns:
            pd.DataFrame: 処理済みのデータフレーム
        """
        try:
            if not self.validate_dataframe():
                st.warning("処理対象のデータが存在しません")
                return pd.DataFrame()  # 空のデータフレームを返す

            # 処理前に計算サマリーをクリア
            self.clear_calculation_summary()

            # データ処理ロジックをここに実装
            # 例: self.df = some_processing(self.df)

            return self.df

        except Exception as e:
            st.error(f"データ処理エラー: {str(e)}")
            return pd.DataFrame()  # エラー時は空のデータフレームを返す

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
                st.warning("出力可能なデータが存在しません")
                return b""  # 空のバイトデータを返す

            return self.df.to_csv(index=index).encode(self.encoding)

        except Exception as e:
            st.error(f"CSV変換エラー: {str(e)}")
            return b""  # エラー時は空のバイトデータを返す

    def get_all_data(self) -> pd.DataFrame:
        """
        全データを取得
        Returns:
            pd.DataFrame: 処理済みのデータフレーム
        """
        return self.df if self.validate_dataframe() else pd.DataFrame()

    def get_monthly_settlement(self) -> pd.DataFrame:
        """
        月末計上データを取得
        Returns:
            pd.DataFrame: 月末計上データ
        """
        try:
            if not self.validate_dataframe():
                return pd.DataFrame()
            # 月末計上データの抽出ロジックを実装
            return self.df  # 仮の実装

        except Exception as e:
            st.error(f"月末計上データ取得エラー: {str(e)}")
            return pd.DataFrame()

    def get_payment_reversal(self) -> pd.DataFrame:
        """
        支払切戻データを取得
        Returns:
            pd.DataFrame: 支払切戻データ
        """
        try:
            if not self.validate_dataframe():
                return pd.DataFrame()
            # 支払切戻データの抽出ロジックを実装
            return self.df  # 仮の実装

        except Exception as e:
            st.error(f"支払切戻データ取得エラー: {str(e)}")
            return pd.DataFrame()

    def display_summary(self) -> None:
        """サマリー情報を表示"""
        try:
            if not self.validate_dataframe():
                st.warning("表示可能なデータがありません")
                return

            # 計算サマリーの表示
            self.display_calculation_summary()

            # データの基本情報を表示
            st.write("### データ概要")
            st.write(f"総レコード数: {len(self.df):,}件")

            # タブで表示を切り替え
            tab1, tab2, tab3 = st.tabs(["全体", "月末計上", "支払切戻"])
            
            with tab1:
                if not self.df.empty:
                    st.dataframe(self.df)
                else:
                    st.info("データがありません")
            
            with tab2:
                monthly_data = self.get_monthly_settlement()
                if not monthly_data.empty:
                    st.dataframe(monthly_data)
                else:
                    st.info("月末計上データがありません")
            
            with tab3:
                reversal_data = self.get_payment_reversal()
                if not reversal_data.empty:
                    st.dataframe(reversal_data)
                else:
                    st.info("支払切戻データがありません")

        except Exception as e:
            st.error(f"サマリー表示でエラーが発生しました: {str(e)}") 