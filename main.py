import streamlit as st

def main():
    st.set_page_config(layout='wide', page_icon=":material/home_repair_service:")
    st.title('経理データ処理ツール')

    '''
     経理処理に必要な集計や変換処理を行うツールです。
     左側のメニューから選択してください。
    '''

    top_page = st.Page(page='pages/top_page.py', title='Top', icon=':material/owl:')
    calc_salary = st.Page(page='pages/salary_bonus.py', title='1.給与賞与データ変換', icon=':material/paid:')
    journal_tm = st.Page(page='pages/journal_transform.py', title='2.仕訳日記帳データ変換', icon=':material/change_circle:')
    sales_agg = st.Page(page='pages/sales_analysis.py', title='3.SMS売上集計', icon=':material/dataset:')

    # サイドバーでページ選択
    pg = st.navigation([top_page, calc_salary, journal_tm, sales_agg])
    pg.run()

    # if page == '給与賞与計算':
    #     import pages.salary_bonus as salary_bonus
    #     salary_bonus.app()  # 給与賞与計算アプリの実行関数
    # elif page == '仕訳データ変換':
    #     import pages.journal_transform as journal_transform
    #     journal_transform.app()  # 仕訳データ変換アプリの実行関数
    # elif page == '売上データ分析':
    #     import pages.sales_analysis as sales_analysis
    #     sales_analysis.app()  # 売上データ分析アプリの実行関数

if __name__ == "__main__":
    main()
