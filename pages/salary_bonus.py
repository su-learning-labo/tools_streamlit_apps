import streamlit as st
from salary_data_processor import SalaryDataProcessor
from bonus_data_processor import BonusDataProcessor
from data_processing import convert_df_to_csv


def display_file_upload() -> 'pd.DataFrame':
    """
    ファイルアップロードUIを表示し、アップロードされたDataFrameを返す。
    """
    uploaded_file = st.sidebar.file_uploader(
        '経理報告用CSVデータをアップロードしてください', type=['csv'])
    return uploaded_file


def process_uploaded_data(uploaded_file, data_type: str) -> tuple:
    """
    アップロードされたデータを処理し、結果のDataFrameとprocessorを返す。

    Args:
        uploaded_file: アップロードされたファイル
        data_type: データの種類（'給与' or '賞与'）

    Returns:
        tuple: (processed_df, processor) もしくは (None, None)
    """
    if uploaded_file is None:
        return None, None

    try:
        file_name = uploaded_file.name
        if data_type == '給与' and '勤怠' in file_name:
            processor = SalaryDataProcessor(uploaded_file)
        elif data_type == '賞与' and '賞与' in file_name:
            processor = BonusDataProcessor(uploaded_file)
        else:
            st.error("アップロードファイルと選択区分が合っていません")
            return None, None

        if not processor.process_uploaded_data():
            st.error("データの処理に失敗しました")
            return None, None, None

        processed_df_detail, processed_df_summary = processor.process_data()
        return processed_df_detail, processed_df_summary, processor

    except Exception as e:
        st.error(f"データ処理中にエラーが発生しました: {str(e)}")
        return None, None


def display_summary(processor) -> None:
    """
    集計結果のサマリーを表示する。
    Args:
        processor: データ処理クラスのインスタンス
    """
    try:
        # サマリーセクション用のスタイル
        summary_style = """
            <style>
                div[data-testid="stExpander"] {
                    background-color: #f8f9fa;
                    border: 1px solid #e0e0e0;
                    border-radius: 10px;
                    padding: 10px;
                    margin-bottom: 30px;
                }
            </style>
        """
        st.markdown(summary_style, unsafe_allow_html=True)

        with st.expander("サマリー", expanded=True):
            if processor.summary is not None:
                total_row = processor.summary.iloc[-1]
                
                # メトリクスの表示
                col1, col2, col3 = st.columns([1,1,2])
                
                with col1:
                    st.metric("総支給人数", f"{int(total_row['支給人数']):,}人")
                with col2:
                    if '一人当たり支給額' in total_row:
                        st.metric("一人当たり支給額", f"{int(total_row['一人当たり支給額']):,}円")
                with col1:
                    if '支給総額' in total_row:
                        st.metric("支給総額", f"{int(total_row['支給総額']):,}円")
                with col2:
                    if '振込金額' in total_row:
                        st.metric("振込金額", f"{int(total_row['振込金額']):,}円")

                # サマリーテーブルの表示
                st.write("### 部門別集計")
                display_df = processor.summary.copy()
                numeric_cols = ['支給人数', '支給総額', '振込金額', '一人当たり支給額']
                for col in numeric_cols:
                    if col in display_df.columns:
                        display_df[col] = display_df[col].fillna(0).apply(
                            lambda x: f"{int(x):,}{'人' if col == '支給人数' else '円'}"
                        )
                
                st.dataframe(display_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"サマリー表示でエラーが発生しました: {str(e)}")
        st.warning("一部の集計情報が表示できない可能性があります。")


def display_processed_data(processed_df: 'pd.DataFrame') -> None:
    """
    処理後のデータを表示する。
    """
    st.dataframe(processed_df, use_container_width=True, hide_index=True)


def display_processed_data_detail(processed_df: 'pd.DataFrame') -> None:
    """
    変換後の全データをグルーピングせずにexpanderで非表示状態で表示する
    """
    with st.expander("変換後の全データ", expanded=False):
        st.dataframe(processed_df, use_container_width=True, hide_index=True)


def display_accounting_data(processed_df: 'pd.DataFrame', processor) -> None:
    """
    会計システム連携用のデータを表示し、ダウンロード機能を提供する。
    """
    if processed_df is None:
        return processed_df

    df = processed_df.copy()
    group_cols = ['原価区分', '部門コード', '部門', '部署コード', '部署', '雇用区分', 'セグメントコード', 'セグメント']
    # --- グルーピング集計---
    agg_cols = [col for col in df.columns if col not in group_cols]
    agg_dict = {col: 'sum' for col in agg_cols if df[col].dtype != 'object'}
    grouped_all = df.groupby(group_cols, as_index=False).agg(agg_dict).query('振込金額 != 0')

    # --- 縦持ちデータへの変換 ---
    # カラム範囲を取得
    melt_idx_cols = ['原価区分', '雇用区分コード', '雇用区分', '部門コード', '部門', '部署コード', '部署', 'セグメントコード', 'セグメント']
    melt_cols = ['基本給', '資格手当合計', '時間外勤務手当合計', 'その他手当合計', '通勤手当合計']

    grouped_for_melt = df.groupby(melt_idx_cols, as_index=False).agg(agg_dict)
    melted = grouped_for_melt.melt(id_vars=melt_idx_cols, value_vars=melt_cols, var_name='項目', value_name='金額')

    # --- 支払仕訳用データ ---
    deducation_cols = ['P健保介護','厚生年金個人', '雇用保険', '所得税', '住民税', '年調過不足', '加入者拠出', 'ランチ弁当代', 'ﾜｰｸﾘｨ知多',	'自動車保険', 'ＣＭ会費',
                        'ＴＧ特別医療', '会社立替精算', 'その他控除', '健保給付金等', '差引支給＿負']

    agg_cols_deducation = [col for col in df.columns if col not in group_cols]
    agg_dict_deducation = {col: 'sum' for col in agg_cols_deducation if df[col].dtype != 'object'}
    grouped_deduction = df.groupby(group_cols, as_index=False).agg(agg_dict_deducation)[group_cols + deducation_cols].query()

    tab1, tab2, tab3 = st.tabs(["全体", "月末計上", "支払切返"])

    with tab1:
        st.write('### - 全体 -')
        st.dataframe(grouped_all, hide_index=True)
        st.write('ダウンロード')
        csv = convert_df_to_csv(grouped_all, index=False)
        st.download_button(
            label='変換データ', data=csv, file_name='result_details.csv', mime='text/csv')

    with tab2:
        st.write('### - 月末計上仕訳用 -')
        df_post_eom = melted.query('金額 != 0')
        post_eom_csv = convert_df_to_csv(df_post_eom, index=False)
        st.dataframe(df_post_eom, hide_index=True)
        st.write('ダウンロード')
        st.download_button(
            label='月末計上仕訳', data=post_eom_csv, file_name='result_journal_eom.csv', mime='text/csv')

    with tab3:
        st.write('### - 支払仕訳 -')
        df_journal = grouped_deduction
        st.dataframe(df_journal, hide_index=True)
        st.write('ダウンロード')
        csv_journal = convert_df_to_csv(df_journal, index=False)
        st.download_button(
            label='支払仕訳', data=csv_journal, file_name='result_journal_payment.csv', mime='text/csv')


def app():
    st.header(':clipboard: OBIC給与・賞与出力データ変換', divider='gray')

    with st.sidebar.expander(label='Manual', expanded=False):
        st.write("""
        対応データ：
        　OBIC7 経理報告用CSV（給与・賞与）
        """)

    # 処理データの選択ボックス
    data_type = st.sidebar.selectbox('データの種類を選択してください', ['給与', '賞与'])
    
    # アップロードファイルメニュー表示
    uploaded_file = display_file_upload()
    
    if uploaded_file is not None:
        try:
            # アップロードファイルの変換処理
            processed_df_detail, processed_df_summary, processor = process_uploaded_data(uploaded_file, data_type)
            
            if processed_df_detail is not None and  processed_df_summary is not None and processor is not None:
                # サマリー
                display_summary(processor)

                st.subheader(':chart_with_upwards_trend: 変換後データ（チェック用）')
                # 変換後データフレーム表示
                # display_processed_data(processed_df)
                display_processed_data_detail(processed_df_detail)
                
                st.subheader('会計システム連携加工用データ', divider='blue')
                # 会計システム連携加工用データ
                display_accounting_data(processed_df_detail, processor)
        
        except Exception as e:
            st.error(f"予期せぬエラーが発生しました: {str(e)}")
            st.error("アプリケーションの管理者に連絡してください。")


if __name__ == '__main__':
    app()