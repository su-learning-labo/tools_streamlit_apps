import streamlit as st
import pandas as pd
import calendar
import datetime

# 必要な関数をインポート
from data_processing import load_df, convert_df_to_csv


def get_year_month_from_file(file):
    """
    ファイル名から年月文字列を取得
    """
    file_name = file.name.split('.')[0].split('_')[-1]
    return file_name


def get_end_of_month_date(str_yyyymm):
    """
    ファイル名から月末日付を取得
    """
    year = int(str_yyyymm[:4])
    month = int(str_yyyymm[-2:])
    last_day = calendar.monthrange(year, month)[1]

    eom = datetime.date(year, month, last_day).strftime('%Y/%m/%d')
    return eom


def get_df_info(df):
    """
    データフレームからファイル容量、サイズ、欠損値の有無を取得
    """
    data_shape = df.shape
    data_size = df.memory_usage().sum()
    count_null = df.isnull().any().sum()

    return data_shape, data_size, count_null


# --- 仕訳データの変換処理 ----
def filtered_df(df):
    """
    並び替えとカラムの整理、リネーム
    """

    # カラム名変更用辞書（一次処理）
    dict_account_conversion = {
        '借方科目コード': 'dr_cd',
        '借方科目名称': 'dr_name',
        '借方科目別補助コード': 'dr_sub_cd',
        '借方科目別補助名称': 'dr_sub_name',
        '借方部門コード': 'dr_section_cd',
        '借方部門名称': 'dr_section_name',
        '借方セグメント2': 'dr_segment_cd',
        '借方セグメント２名称': 'dr_segment_name',
        '貸方科目コード': 'cr_cd',
        '貸方科目名称': 'cr_name',
        '貸方科目別補助コード': 'cr_sub_cd',
        '貸方科目別補助名称': 'cr_sub_name',
        '貸方部門コード': 'cr_section_cd',
        '貸方部門名称': 'cr_section_name',
        '貸プセグメント2コード': 'cr_segment_cd',
        '貸方セグメント２名称': 'cr_segment_name',
        '金額': 'price',
        '消費税': 'tax',
        '摘要': 'outline'
    }

    df = df.filter(
        ['借方科目コード', '借方科目名称', '借方科目別補助コード', '借方科目別補助名称', '借方部門コード', '借方部門名称',
         '借方セグメント2', '借方セグメント２名称', '貸方科目コード', '貸方科目名称', '貸方科目別補助コード', '貸方科目別補助名称',
         '貸方部門コード', '貸方部門名称', '貸プセグメント2コード', '貸方セグメント２名称', '金額', '消費税', '摘要']
    ).rename(dict_account_conversion, axis=1)

    return df


def convert_df(file):
    """
    データの読み込みと変換処理
    """
    df = load_df(file)
    filtered = filtered_df(df)
    return filtered


# -- 借方データの整形処理 --
def convert_dr(df):
    """
    借方データの整形
    """
    df = df.copy().assign(price=lambda x: df['price'] - df['tax']).drop('tax', axis=1)

    _df = df.drop(
        ['cr_cd', 'cr_name', 'cr_sub_cd', 'cr_sub_name', 'cr_section_cd',
         'cr_section_name', 'cr_segment_cd', 'cr_segment_name'], axis=1
    ).dropna(subset='dr_cd').fillna(0)

    _df.columns = ['ac_cd', 'ac_name', 'sub_cd', 'sub_name', 'section_cd', 'section_name', 'segment_cd',
                   'segment_name', 'price', 'outline']

    return _df


def calc_dr(df):
    """
    借方データの計算処理
    """
    df_dr = convert_dr(df)

    df_sales = df_dr.query('5000 <= ac_cd < 6000').assign(price=df_dr['price'] * -1)
    df_cost = df_dr.query('6000 <= ac_cd <= 7999')
    df_extra_income = df_dr.query('8000 <= ac_cd < 8200').assign(price=df_dr['price'] * -1)
    df_extra_outcome = df_dr.query('8200 <= ac_cd < 8300')
    df_dr_result = pd.concat([df_sales, df_cost, df_extra_income, df_extra_outcome]).query('price != 0')

    return df_dr_result


# -- 貸方データ --
def convert_cr(df):
    """
    貸方データの整形
    """
    df = df.copy().assign(price=lambda x: df['price'] - df['tax']).drop('tax', axis=1)

    _df = df.drop(
        ['dr_cd', 'dr_name', 'dr_sub_cd', 'dr_sub_name', 'dr_section_cd',
         'dr_section_name', 'dr_segment_cd', 'dr_segment_name'], axis=1
    ).dropna(subset='cr_cd').fillna(0)

    _df.columns = ['ac_cd', 'ac_name', 'sub_cd', 'sub_name', 'section_cd', 'section_name', 'segment_cd',
                   'segment_name', 'price', 'outline']

    return _df


def calc_cr(df):
    """
    貸方データの計算処理
    """
    df_cr = convert_cr(df)

    df_sales = df_cr.query('5000 <= ac_cd < 6000')
    df_cost = df_cr.query('6000 <= ac_cd <= 7999').assign(price=df_cr['price'] * -1)
    df_extra_income = df_cr.query('8000 <= ac_cd < 8200')
    df_extra_outcome = df_cr.query('8200 <= ac_cd < 8300').assign(price=df_cr['price'] * -1)
    df_cr_result = pd.concat([df_sales, df_cost, df_extra_income, df_extra_outcome]).query('price != 0')

    return df_cr_result


# -- データ統合 --
def concat_df(dr, cr):
    """
    借方データと貸方データを結合し、型変換を行う
    """
    df = pd.concat([dr, cr]).reset_index(drop=True)
    df.dropna(subset='ac_cd', inplace=True)

    df['ac_cd'] = df['ac_cd'].apply(lambda x: str(int(x)))
    df['sub_cd'] = df['sub_cd'].apply(lambda x: str(int(x)))
    df['section_cd'] = df['section_cd'].apply(lambda x: str(int(x)))
    df['segment_cd'] = df['segment_cd'].apply(lambda x: str(int(x)))
    df['price'] = df['price'].apply(lambda x: int(x))

    return df


# --  Wide to Long 変換 --
# 集計用区分の辞書定義
large_class = {'CATV': 'コンシューマ事業',
               'ｺﾐｭﾆﾃｨﾁｬﾝﾈﾙ': 'コンシューマ事業',
               'NET': 'コンシューマ事業',
               'TEL': 'コンシューマ事業',
               'ｺﾐｭﾆﾃｨFM': 'まちづくり事業',
               'ｱﾌﾟﾘ(外販)': 'コンシューマ事業',
               'ｲﾍﾞﾝﾄ': 'まちづくり事業',
               '音響・照明': 'まちづくり事業',
               'ｿﾘｭｰｼｮﾝ': 'まちづくり事業',
               'ｽﾀｲﾙ': 'まちづくり事業',
               'ｼｮｯﾋﾟﾝｸﾞ': 'まちづくり事業',
               'ﾅﾋﾞ': 'まちづくり事業',
               'KURUTOｶﾌｪ': 'まちづくり事業',
               '指定管理': 'まちづくり事業',
               '子会社取引': 'グループ管理'}

mid_class = {'CATV': '放送',
             'ｺﾐｭﾆﾃｨﾁｬﾝﾈﾙ': '放送',
             'NET': '通信',
             'TEL': '通信',
             'ｺﾐｭﾆﾃｨFM': 'コミュニティFM',
             'ｱﾌﾟﾘ(外販)': 'アプリ',
             'ｲﾍﾞﾝﾄ': 'イベント',
             '音響・照明': 'イベント',
             'ｿﾘｭｰｼｮﾝ': 'ソリューション',
             'ｽﾀｲﾙ': 'ちたまる',
             'ｼｮｯﾋﾟﾝｸﾞ': 'ちたまる',
             'ﾅﾋﾞ': 'ちたまる',
             'KURUTOｶﾌｪ': 'KURUTO',
             '指定管理': 'KURUTO',
             '子会社取引': 'グループ取引'}


# 縦変換
def melt_df(df):
    """
    Wide型データをLong型データに変換
    """
    df_melted = df.filter(['科目CD', '科目名', '補助科目CD', '補助科目名', '部門CD', '部門名', '集計区分', 'CATV', 'ｺﾐｭﾆﾃｨﾁｬﾝﾈﾙ', 'NET', 'TEL',
                    'ｺﾐｭﾆﾃｨFM', 'ｱﾌﾟﾘ(外販)', 'ｲﾍﾞﾝﾄ', '音響・照明', 'ｿﾘｭｰｼｮﾝ', 'ｽﾀｲﾙ', 'ｼｮｯﾋﾟﾝｸﾞ', 'ﾅﾋﾞ', 'KURUTOｶﾌｪ', '指定管理',
                    '子会社取引']) \
        .melt(id_vars=['科目CD', '科目名', '補助科目CD', '補助科目名', '部門CD', '部門名', '集計区分'],
              var_name='s_class',
              value_vars=['CATV', 'ｺﾐｭﾆﾃｨﾁｬﾝﾈﾙ', 'NET', 'TEL',
                          'ｺﾐｭﾆﾃｨFM', 'ｱﾌﾟﾘ(外販)', 'ｲﾍﾞﾝﾄ', '音響・照明', 'ｿﾘｭｰｼｮﾝ', 'ｽﾀｲﾙ', 'ｼｮｯﾋﾟﾝｸﾞ', 'ﾅﾋﾞ', 'KURUTOｶﾌｪ',
                          '指定管理', '子会社取引'],
              value_name='金額')
    return df_melted


# 区分追加
def add_mapping(df):
    """
    変換後のデータに集計区分を追加
    """
    df_mapped = df \
        .assign(large_class=df['s_class'].map(large_class)) \
        .assign(mid_class=df['s_class'].map(mid_class))
    return df_mapped


# 一連の変換処理
def load_long_data(file):
    """
    配賦データを読み込み、Long型に変換する一連の処理
    """
    df = load_df(file)
    df = melt_df(df)
    df = add_mapping(df)
    df = df.fillna(0)
    df.dropna(subset='金額', inplace=True)
    df['科目CD'] = df['科目CD'].apply(lambda x: str(int(x)))
    df['補助科目CD'] = df['補助科目CD'].apply(lambda x: str(int(x)))
    df['部門CD'] = df['部門CD'].apply(lambda x: str(int(x)))

    return df


def app():
    st.header('仕訳データ変換')
    st.caption('振替伝票仕訳データを使った、データ分析用コード変換処理')

    # ファイルアップローダー
    uploaded_file = st.file_uploader('1. 振替伝票CSVファイルをアップロードしてください', type='csv')

    # Wide to Long 変換用ファイルアップローダー
    uploaded_wide_file = st.file_uploader('2. 配賦データCSVファイルをアップロードしてください', type='csv')

    # メイン処理
    if uploaded_file is not None:
        # データの読み込み
        df = convert_df(uploaded_file)

        # 借方データの整形処理
        df_dr = calc_dr(df)

        # 貸方データの整形処理
        df_cr = calc_cr(df)

        # 貸借データの縦連結
        df_concat = concat_df(df_dr, df_cr)
        concat_data_shape, concat_data_size, concat_count_null = get_df_info(df_concat)

        # 人件費に関するコードリスト
        labor_cost_cd_list = [
            '6110', '6120', '6130', '6140', '6150', '6160', '6170', '6180', '6190', '6200',
            '7110', '7120', '7130', '7140', '7150', '7160', '7170', '7180', '7190', '7200'
        ]

        # ac_cdからlabor_cost_cd_listにないものだけをリスト化
        value_list = list(set(df_concat['ac_cd'].values))
        target_list = sorted([item for item in value_list if item not in labor_cost_cd_list])

        # 人件費項目を除外したデータフレームを作成
        df_exclude_labor_cost = df_concat.query('ac_cd in @target_list')
        exc_data_shape, exc_data_size, exc_count_null = get_df_info(df_exclude_labor_cost)

        st.subheader('1-1. Result - Details')

        show_detail = st.checkbox('Check & Preview - Details!')

        if show_detail:
            st.dataframe(df_concat, use_container_width=True)

        output_result_detail = convert_df_to_csv(df_concat, index=False)

        st.download_button(
            label='DL: 詳細データ',
            data=output_result_detail,
            file_name=f'result_detail_{get_year_month_from_file(uploaded_file)}.csv',
            mime='text/csv'
        )
        st.caption(
            f'参考）サイズ: {concat_data_shape},　容量: {concat_data_size / 1024:.1f} MB, データ欠損値: {concat_count_null}')

        st.write('---')
        st.subheader('1-2. Result - Exclude Labor Cost')

        show_detail_exclude = st.checkbox('Check & Preview - Exclude Labor Cost')

        if show_detail_exclude:
            st.dataframe(df_exclude_labor_cost, use_container_width=True)

        output_result_exclude_labor_cost = convert_df_to_csv(df_exclude_labor_cost, index=False)

        st.download_button(
            label='DL: 詳細(人件費除き)',
            data=output_result_exclude_labor_cost,
            file_name=f'result_exclude_labor_cost_{get_year_month_from_file(uploaded_file)}.csv',
            mime='text/csv'
        )
        st.caption(
            f'参考）サイズ: {exc_data_shape},　容量: {exc_data_size / 1024:.1f} MB, データ欠損値: {exc_count_null}')

        st.write('---')
        st.subheader('1-3. Result - Grouped')

        show_grouped = st.checkbox('Check & Preview - Grouped!')

        pivot_data = pd.pivot_table(df_concat,
                                    index=['ac_cd', 'ac_name', 'sub_cd', 'sub_name', 'section_cd',
                                           'section_name', 'segment_cd', 'segment_name'], values='price',
                                    aggfunc=sum).reset_index()

        data_size = pivot_data.memory_usage().sum()

        output_result_grouped = convert_df_to_csv(pivot_data)

        if show_grouped:
            st.dataframe(pivot_data, use_container_width=True)

        st.download_button(
            label='DL: 集計データ',
            data=output_result_grouped,
            file_name=f'result_{get_year_month_from_file(uploaded_file)}.csv',
            mime='text/csv'
        )

        st.caption(
            f'データサイズ: {pivot_data.shape},　容量: {data_size / 1024:.1f} MB, データ欠損値: {pivot_data.isnull().any().sum()}')
        st.write('---')

        st.write('NEXT ... 【集計データをダウンロードして、配賦結果を作成】')

    else:
        st.info('1. 振替伝票CSVファイルをアップロードしてください。')

    # 縦変換用処理
    if uploaded_wide_file is not None:

        df = load_long_data(uploaded_wide_file)

        flg_box = st.radio('予算/実績の区分を選択', ('実績', '予算'))

        if flg_box == '実績':
            df['予算/実績'] = '実績'
            df['期間'] = get_end_of_month_date(get_year_month_from_file(uploaded_wide_file))

        elif flg_box == '予算':
            df['予算/実績'] = '予算'
            df['期間'] = get_end_of_month_date(get_year_month_from_file(uploaded_wide_file))

        else:
            df['予算/実績'] = ''
            df['期間'] = get_end_of_month_date(get_year_month_from_file(uploaded_wide_file))

        df_result_long = df.filter(
            ['予算/実績', '期間', '科目CD', '科目名', '補助科目CD', '補助科目名', '部門CD', '部門名', '集計区分', 's_class', 'mid_class',
             'large_class', '金額'])

        df_sales_long = \
            df_result_long \
                .query('集計区分 in ["利用料収入", "その他収入"]')

        df_cost_long = \
            df_result_long \
                .query('集計区分 not in ["利用料収入", "その他収入"]')

        st.subheader('2-1. Result - Sales_long')
        data_size = df_sales_long.memory_usage().sum()

        show_sales_long = st.checkbox('Check & Preview - sales_long')

        if show_sales_long:
            st.dataframe(df_sales_long, use_container_width=True)

        output_result_sales_long = convert_df_to_csv(df_sales_long)

        st.download_button(
            label='DL: 売上データ（long型）',
            data=output_result_sales_long,
            file_name=f'result_sales_long_{get_year_month_from_file(uploaded_wide_file)}.csv',
            mime='text/csv'
        )
        st.caption(
            f'参考）サイズ: {df_sales_long.shape},　容量: {data_size / 1024:.1f} MB, データ欠損値: {df_sales_long.isnull().any().sum()}')

        st.write('---')

        st.subheader('2-2. Result - Cost_long')
        data_size = df_cost_long.memory_usage().sum()

        show_cost_long = st.checkbox('Check & Preview - Cost_long')

        if show_cost_long:
            st.dataframe(df_cost_long, use_container_width=True)

        output_result_cost_long = convert_df_to_csv(df_cost_long)

        st.download_button(
            label='DL: 経費データ（long型）',
            data=output_result_cost_long,
            file_name=f'result_cost_long_{get_year_month_from_file(uploaded_wide_file)}.csv',
            mime='text/csv'
        )
        st.caption(
            f'参考）サイズ: {df_cost_long.shape},　容量: {data_size / 1024:.1f} MB, データ欠損値: {df_cost_long.isnull().any().sum()}')

    else:
        st.info('2. 配賦データCSVファイルをアップロードしてください。')


if __name__ == '__main__':
    app()