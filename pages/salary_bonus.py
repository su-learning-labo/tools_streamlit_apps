import streamlit as st
from salary_data_processor import SalaryDataProcessor
from bonus_data_processor import BonusDataProcessor
from data_processing import convert_df_to_csv

def app():
    st.header(':clipboard: OBIC給与・賞与出力データ変換',divider='gray')

    with st.sidebar.expander(label='Manual', expanded=False):
        st.write("""
        対応データ：
        　OBIC7 経理報告用CSV（給与・賞与）
        """)

    # side = st.sidebar()
    data_type = st.sidebar.selectbox('データの種類を選択してください', ['給与', '賞与'])
    uploaded_file = st.sidebar.file_uploader('経理報告用CSVデータをアップロードしてください', type=['csv'])

    if uploaded_file is not None:
        file_name = uploaded_file.name
        if data_type == '給与' and '勤怠' in file_name:
            processor = SalaryDataProcessor(uploaded_file)
        elif data_type == '賞与' and '賞与' in file_name:
            processor = BonusDataProcessor(uploaded_file)
        else:
            st.error("アップロードファイルと選択区分が合っていません")
            return

        processed_df = processor.process_data()

        # サマリーセクション
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
            st.write(f'　**総受給者数: {processor.total_payee} 人**')
            
            col1, col2, col3 = st.columns([1.2, 1.2, 2])
            col1.write(f'　**支給額合計: {processor.total_payment:,} 円**')
            col2.write(f'　**総振込額: {processor.total_transfer_amount:,} 円**')
            col3.write(f'　**一人あたり支払額: {processor.total_payment / processor.total_payee:,.1f} 円**')

        st.subheader(':chart_with_upwards_trend: 変換後データ')
        st.dataframe(processed_df, use_container_width=True)
        st.write('')
        st.subheader('会計システム連携加工用データ', divider='blue')
        tab1, tab2, tab3 = st.tabs(["全体", "月末計上", "支払切返"])
        with tab1:
            st.write('### - 全体 -')
            st.dataframe(processor.summary)

            st.write('ダウンロード')
            csv = convert_df_to_csv(processed_df)
            st.download_button(
                label='変換データ',
                data=csv,
                file_name='result_data.csv',
                mime='text/csv',
            )

        with tab2:
            # 月末計上仕訳用集計結果
            st.write('### - 月末計上仕訳用 -')
            df_post_eom = processor.post_eom_data
            post_eom_csv = convert_df_to_csv(df_post_eom, index=True)
            st.dataframe(df_post_eom)

            st.write('ダウンロード')
            st.download_button(
                label='月末計上仕訳',
                data=post_eom_csv,
                file_name='result_journal_eom.csv',
                mime='text/csv',
            )

        with tab3:
            # 支払仕訳用集計結果
            st.write('### - 支払仕訳 -')
            df_journal = processor.journal
            st.dataframe(df_journal)

            st.write('ダウンロード')
            csv_journal = convert_df_to_csv(df_journal, index=True)
            st.download_button(
                label='支払仕訳',
                data=csv_journal,
                file_name='result_journal_payment.csv',
                mime='text/csv',
            )


if __name__ == '__main__':
    app()