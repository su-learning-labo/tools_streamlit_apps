# base_data_processor.py

from data_processing import load_df, rearrange_columns, rename_columns, add_total_column, replace_values, conditional_replace


class BaseDataProcessor:
    columns_order = []  # 列の並び替え順序
    columns_rename = {}  # 列名の変更マッピング
    total_columns = {}  # 合計列の追加ルール
    replace_rules = {}  # 値の置き換えルール
    conditional_rules = []  # 条件付き置き換えルール

    def __init__(self, file_path, encoding='cp932'):
        self.file_path = file_path
        self.encoding = encoding
        self.df = None

    def load_data(self):
        self.df = load_df(self.file_path, self.encoding)

    def process_data(self):
        self.load_data()
        self.add_total_columns()
        self.rearrange_df_columns()
        self.rename_df_columns()
        self.apply_replace_values()
        self.apply_conditional_replace()

    def add_total_columns(self):
        for new_column_name, column_list in self.total_columns.items():
            self.df = add_total_column(self.df, column_list, new_column_name)

    def rename_df_columns(self):
        if self.columns_rename:
            self.df = rename_columns(self.df, self.columns_rename)

    def rearrange_df_columns(self):
        if self.columns_order:
            self.df = rearrange_columns(self.df, self.columns_order)

    def apply_replace_values(self):
        for column, replacements in self.replace_rules.items():
            self.df = replace_values(self.df, column, replacements)

    def apply_conditional_replace(self):
        for condition, column, new_value in self.conditional_rules:
            self.df = conditional_replace(self.df, condition(self.df), column, new_value)

