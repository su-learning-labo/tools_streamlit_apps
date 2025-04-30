import streamlit as st
from plotly import graph_objects as go
import plotly.express as px
from sales_data import SalesData
from config.sales_payment_config import PaymentConfig

def app():
    """売上分析アプリケーションのメインページ"""
    
    st.header(':material/dataset: 課金システム売上集計処理(Symphonizer抽出データ）', divider='gray')
    
    # データ処理クラスのインスタンス化
    sales_data = SalesData()
    config = PaymentConfig()

    # サイドバーのファイルアップロード部分
    with st.sidebar:
        st.write('集計用のファイルをアップロードしてください')
        col1, col2 = st.columns(2)
        sms_file = col1.file_uploader('SMS請求金額', type='csv')
        shokki_file = col2.file_uploader('織機給与天引請求額', type='csv')

        # データ読み込み
        if sales_data.load_data(sms_file, shokki_file):
            st.subheader(':material/filter_list: データ選択')
            # 支払方法の選択
            with st.expander('⚠ 支払手段の選択について'):
                st.markdown("""
                SMSとは別で請求しているものや、請求対象外のものは除外
                
                例：振込/アプリデモ/貸倒処理済み/口座封鎖/その他
                """)

            payment_methods = st.multiselect(
                '対象の支払手段を選択してください',
                sales_data.df['MEI_NAME_V'].unique().tolist(),
                default=[x for x in sales_data.df['MEI_NAME_V'].unique() 
                        if x not in config.TARGET_EXCLUDING_PAYMENT]
            )

            # フィルター条件
            col1, col2 = st.columns(2)
            include_advance = col1.checkbox('年払請求を含む')
            include_non_sales = col2.checkbox('売上対象外を含める')

    
    #  メインコンテンツ 
    if sales_data.df is not None and payment_methods:
        # データのフィルタリング
        filtered_data = sales_data.filter_data(
            payment_methods, 
            include_advance, 
            include_non_sales
        )

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

        # サマリー情報の表示
        with st.expander('サマリー', expanded=True):
            summary = sales_data.calculate_summary(filtered_data)
            
            col1, col2, col3 = st.columns([1, 1, 2])
            with col1:
                st.metric('対象金額計', f'{summary["total_amount"]:,}円')
                st.metric('請求顧客数', f'{summary["customer_count"]:,}件')
            with col2:
                st.metric('平均単価', f'{summary["average_price"]:,.1f}円')

            # グラフの表示
            chart = sales_data.create_payment_chart_data(filtered_data)
            fig = px.bar(chart, x='MEI_NAME_V', y='SEIKYU_TOTAL', color='SEIKYU_TOTAL', color_continuous_scale='tealrose')
            fig.update_layout(
                title='支払手段別売上',
                xaxis_title='支払手段',
                yaxis_title='売上金額',
                legend_title='支払手段',
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='white',

            )
            st.plotly_chart(fig)


        # データプレビュー
        st.subheader(':material/grid_view: データプレビュー', divider=True)
        st.dataframe(filtered_data, hide_index=True)

        # エクスポートデータの準備と出力
        export_data = sales_data.prepare_export_data(filtered_data)
        
        st.sidebar.subheader(':material/download: データエクスポート')
        
        # ダウンロードボタンの配置
        col1, col2 = st.sidebar.columns(2)
        col1.download_button(
            '詳細データ(preview)',
            data=export_data['preview'].to_csv(encoding='cp932'),
            file_name='詳細データ.csv',
            mime='text/csv'
        )
        
        col1.download_button(
            '売上票作成用データ',
            data=export_data['sales'].to_csv(encoding='cp932'),
            file_name='売上作成用データ.csv',
            mime='text/csv'
        )
        
        journal_data = export_data['journal']
        col2.download_button(
            '仕訳用データ(SMS)',
            data=journal_data['sms'].to_csv(encoding='cp932'),
            file_name='shiwake_sms.csv',
            mime='text/csv'
        )
        
        col2.download_button(
            '仕訳用データ(織機)',
            data=journal_data['shokki'].to_csv(encoding='cp932'),
            file_name='shiwake_shokki.csv',
            mime='text/csv'
        )
        
        col2.download_button(
            '仕訳用データ(CTC)',
            data=journal_data['ctc'].to_csv(encoding='cp932'),
            file_name='shiwake_ctc.csv',
            mime='text/csv'
        )

if __name__ == "__main__":
    app()