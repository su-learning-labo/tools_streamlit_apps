# bonus_data_processor.py

from base_data_processor import BaseDataProcessor
from calculations import (df_output_summary, journal_post, post_eom, get_payee_count, get_total_payment,
                          get_total_transfer_amount)


class BonusDataProcessor(BaseDataProcessor):
    columns_order = [
        '会社NO', '対象年月', 'コード', '氏名', '原価区分', '所属', '所属名', '所属コード1',
        '所属コード1名', '事業所', '事業所名', '部門', '部門名', '賞与額計', '健康保険',
        '介護保険', '厚生年金', '雇用保険', '社会保険計', '賞与所得税', '賞与控除合計', '差引支給額',
        '賞健保会社分', '賞介護会社分', '賞厚年会社分', '賞雇保会社分', '賞労災会社分', '賞児童手当分', '賞会社負担計'
    ]

    columns_rename = {
        '所属': '雇用形態',
        '所属名': '雇用形態名',
        '所属コード1': '部署コード1',
        '所属コード1名': '部署コード1名',
        '事業所': '所属',
        '事業所名': '所属名',
        '部門': 'ｾｸﾞﾒﾝﾄ',
        '部門名': 'ｾｸﾞﾒﾝﾄ名',

        # その他必要に応じてカラム名を追加...
    }

    total_columns = {

    }

    replace_rules = {
        '部署コード1': {
            0: 90,  # 共通
            10: 41,  # 経営企画部
            20: 31,  # コンシューマ事業部
            40: 81,  # IT推進部
            50: 21,  # 技術サービス部
            60: 11,  # まちづくり事業部
            70: 61,  # 地域情報部
        },  # 部署コードの変換ルール

        '所属': {
            13: 46,  # 総務人事課
            14: 42,  # 経営戦略課
            23: 37,  # 地域営業課
            24: 58,  # 暮らしサポート課 旧マーケティング課のコードを流用
            25: 33,  # リテンション課
            26: 55,  # カスタマーセンター
            27: 57,  # お客様管理課
            15: 82,  # IT推進課
            53: 22,  # 技術課
            54: 23,  # 通信課
            63: 18,  # にぎわい創生課
            64: 71,  # 地域サポート課
            65: 38,  # ソリューション課
            73: 63,  # 編成課
            74: 64,  # 制作課
        },

        # '雇用形態名': {'': '役員', '契約社員': '正社員', 'パート': 'アルバイト'}  # 雇用形態の変換ルール
    }

    conditional_rules = [
        (lambda df: df['所属名'] == '代表取締役社長', '雇用形態名', lambda df: df['所属名']),
        (lambda df: df['所属名'] == '取締役', '雇用形態名', lambda df: df['所属名']),
        (lambda df: df['所属名'] == '監査役', '雇用形態名', lambda df: df['所属名']),

        (lambda df: df['所属名'] == '部長', '所属', lambda df: df['部署コード1']),
        (lambda df: df['所属名'] == '部長', '所属名', lambda df: df['部署コード1名']),
        (lambda df: df['所属名'] == 'ＩＴマイスター', '所属', lambda df: df['部署コード1']),
        (lambda df: df['所属名'] == 'ＩＴマイスター', '所属名', lambda df: df['部署コード1名']),
        (lambda df: df['所属'] == 90, '所属名', lambda df: df['部署コード1名']),

        (lambda df: df['ｾｸﾞﾒﾝﾄ'] == 1, 'ｾｸﾞﾒﾝﾄ', 9001),  # 一般
        (lambda df: df['ｾｸﾞﾒﾝﾄ'] == 2, 'ｾｸﾞﾒﾝﾄ', 2200),  # FM
        (lambda df: df['ｾｸﾞﾒﾝﾄ'] == 3, 'ｾｸﾞﾒﾝﾄ', 3201),  # KURUTO（物産）
        (lambda df: df['ｾｸﾞﾒﾝﾄ'] == 4, 'ｾｸﾞﾒﾝﾄ', 3202),  # KURUTO（ｶﾌｪ）
        (lambda df: df['ｾｸﾞﾒﾝﾄ'] == 5, 'ｾｸﾞﾒﾝﾄ', 1900),  # ワクチンCC ー＞ 3サービス共通へ

        (lambda df: (df['雇用形態'] > 1) & (df['部署コード1'] == 31) & (df['ｾｸﾞﾒﾝﾄ'] == 9001), 'ｾｸﾞﾒﾝﾄ', 1900),
        # コンシューマ
        (lambda df: (df['雇用形態'] > 1) & (df['部署コード1'] == 21) & (df['ｾｸﾞﾒﾝﾄ'] == 9001), 'ｾｸﾞﾒﾝﾄ', 1900),  # 技術
        (lambda df: (df['雇用形態'] > 1) & (df['部署コード1'] == 71) & (df['ｾｸﾞﾒﾝﾄ'] == 9001), 'ｾｸﾞﾒﾝﾄ', 2300),
        # 地域サポート
        (lambda df: (df['雇用形態'] > 1) & (df['部署コード1'] == 18) & (df['ｾｸﾞﾒﾝﾄ'] == 9001), 'ｾｸﾞﾒﾝﾄ', 3101),
        # にぎわい創生課
        (lambda df: (df['雇用形態'] > 1) & (df['部署コード1'] == 37) & (df['ｾｸﾞﾒﾝﾄ'] == 9001), 'ｾｸﾞﾒﾝﾄ', 1409),
        # ソリューション課
        (lambda df: (df['雇用形態'] > 1) & (df['部署コード1'] == 63) & (df['ｾｸﾞﾒﾝﾄ'] == 9001), 'ｾｸﾞﾒﾝﾄ', 2100),  # 編成課
        (lambda df: (df['雇用形態'] > 1) & (df['部署コード1'] == 64) & (df['ｾｸﾞﾒﾝﾄ'] == 9001), 'ｾｸﾞﾒﾝﾄ', 2100),  # 制作課

        (lambda df: df['ｾｸﾞﾒﾝﾄ'] == 9001, 'ｾｸﾞﾒﾝﾄ名', '共通経費'),
        (lambda df: df['ｾｸﾞﾒﾝﾄ'] == 1900, 'ｾｸﾞﾒﾝﾄ名', '3ｻｰﾋﾞｽ共通'),
        (lambda df: df['ｾｸﾞﾒﾝﾄ'] == 2100, 'ｾｸﾞﾒﾝﾄ名', 'ｺﾐｭﾆﾃｨCH'),
        (lambda df: df['ｾｸﾞﾒﾝﾄ'] == 2200, 'ｾｸﾞﾒﾝﾄ名', 'ｺﾐｭﾆﾃｨFM'),
        (lambda df: df['ｾｸﾞﾒﾝﾄ'] == 2300, 'ｾｸﾞﾒﾝﾄ名', 'ﾌﾘｰﾍﾟｰﾊﾟｰ'),
        (lambda df: df['ｾｸﾞﾒﾝﾄ'] == 1409, 'ｾｸﾞﾒﾝﾄ名', 'ｿﾘｭｰｼｮﾝ'),
        (lambda df: df['ｾｸﾞﾒﾝﾄ'] == 3101, 'ｾｸﾞﾒﾝﾄ名', 'ｲﾍﾞﾝﾄ'),
    ]

    def __init__(self, file_path, encoding='cp932'):
        super().__init__(file_path, encoding)

    def process_data(self):
        super().process_data()

        # 集計関数を呼び出し、結果を属性に保存
        group_by_columns = ['原価区分', '雇用形態', '雇用形態名', '部署コード1', '部署コード1名', '所属', '所属名',
                            'ｾｸﾞﾒﾝﾄ', 'ｾｸﾞﾒﾝﾄ名']
        sum_columns = ['賞与額計', '健康保険', '介護保険', '厚生年金', '雇用保険', '社会保険計', '賞与所得税',
                       '賞与控除合計', '差引支給額', '賞健保会社分', '賞介護会社分', '賞厚年会社分', '賞雇保会社分',
                       '賞労災会社分', '賞児童手当分', '賞会社負担計']
        journal_columns = ['賞与額計']

        melt_columns = ['賞与額計', '健康保険', '介護保険', '厚生年金', '雇用保険','賞与所得税',
                        '差引支給額', '賞健保会社分', '賞介護会社分', '賞厚年会社分', '賞雇保会社分',
                        '賞労災会社分', '賞児童手当分',
                        ]


        self.summary = df_output_summary(self.df, group_by_columns, sum_columns)
        self.journal = journal_post(self.df, group_by_columns, sum_columns)
        self.post_eom_data = post_eom(self.df, group_by_columns, melt_columns, exclude_columns='差引支給額')
        self.total_payee = get_payee_count(self.df)
        self.total_payment = get_total_payment(self.df, total_columns="賞与額計")
        self.total_transfer_amount = get_total_transfer_amount(self.df, pay_column='差引支給額')

        return self.df

    def convert_df_to_csv(self, df, index=False):
        """
                DataFrameをCSV形式に変換する。

                :param df: 変換するpandas DataFrame
                :param index: CSV出力にインデックスを含めるかどうか
                :return: CSV形式の文字列
                """
        return df.to_csv(index=index).encode('cp932')
