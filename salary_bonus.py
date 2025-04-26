import streamlit as st
from salary_data_processor import SalaryDataProcessor
import pandas as pd

def process_uploaded_data(uploaded_file) -> bool:
    """
    アップロードされたデータの処理
    Args:
        uploaded_file: アップロードされたファイル
    Returns:
        bool: 処理成功の場合True
    """
    try:
        processor = SalaryDataProcessor(uploaded_file)
        if not processor.process_uploaded_data():
            return False

        display_summary(processor)
        display_processed_data(processor)
        display_accounting_data(processor)
        return True

    except Exception as e:
        st.error(f"処理エラー: {str(e)}")
        return False

def display_summary(processor: SalaryDataProcessor) -> None:
    """
    サマリー情報の表示
    Args:
        processor: データ処理クラスのインスタンス
    """
    if processor.summary is None or processor.summary.empty:
        st.warning("集計可能なデータがありません")
        return

    st.write("### サマリー情報")
    
    # メトリクスの表示
    total_row = processor.summary.iloc[-1]
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("総支給人数", f"{int(total_row['支給人数']):,}人")
    with col2:
        if '支給総額' in total_row:
            st.metric("支給総額", f"{int(total_row['支給総額']):,}円")
    with col3:
        if '一人当たり支給額' in total_row:
            st.metric("一人当たり支給額", f"{int(total_row['一人当たり支給額']):,}円")

    # サマリーテーブルの表示
    display_df = processor.summary.copy()
    numeric_cols = ['支給人数', '支給総額', '振込金額1', '一人当たり支給額']
    for col in numeric_cols:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(
                lambda x: f"{int(x):,}{'人' if col == '支給人数' else '円'}"
            )

    st.dataframe(display_df, use_container_width=True, hide_index=True)

def display_processed_data(processor: SalaryDataProcessor) -> None:
    """
    処理済みデータの表示
    Args:
        processor: データ処理クラスのインスタンス
    """
    if processor.df is None or processor.df.empty:
        st.warning("表示可能なデータがありません")
        return

    st.write("### データ確認")
    display_df = processor.df.head().copy()
    
    # 数値カラムのフォーマット
    numeric_columns = processor.config.get_numeric_columns('salary')
    if numeric_columns:
        for col in numeric_columns:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{int(x):,}円" if pd.notnull(x) else ""
                )

    st.dataframe(display_df, use_container_width=True, hide_index=True)

def display_accounting_data(processor: SalaryDataProcessor) -> None:
    """
    会計データの表示とダウンロード
    Args:
        processor: データ処理クラスのインスタンス
    """
    if not processor.processed or processor.df is None or processor.df.empty:
        return

    st.write("### データダウンロード")
    
    # CSVダウンロードボタン
    csv_data = processor.df.to_csv(index=False, encoding='cp932')
    st.download_button(
        label="CSVダウンロード",
        data=csv_data,
        file_name="processed_salary_data.csv",
        mime="text/csv"
    ) 