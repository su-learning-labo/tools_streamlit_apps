import yaml
from pathlib import Path
from typing import Dict, Any, List, Union, Optional
import os
import streamlit as st


class ConfigLoader:
    """設定ファイルローダークラス"""

    def __init__(self, config_path: str = "config/data_processing_rules.yaml"):
        """
        設定ファイルローダーの初期化
        Args:
            config_path: 設定ファイルのパス
        """
        self.config_path = Path(config_path)
        self._load_config()

    def _load_config(self) -> None:
        """設定ファイルを読み込む"""
        try:
            if not os.path.exists(self.config_path):
                raise FileNotFoundError(f"設定ファイルが見つかりません: {self.config_path}")

            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)

        except Exception as e:
            st.error(f"設定ファイルの読み込みエラー: {str(e)}")
            raise

    def get_settings(self, section: str, key: str) -> Any:
        """
        指定されたセクションの設定値を取得
        Args:
            section: セクション名
            key: 設定キー（ドット区切りで階層指定可能）
        Returns:
            Any: 設定値
        """
        try:
            # セクションの取得
            if section not in self.config:
                st.warning(f"セクションが見つかりません: {section}")
                return None

            current = self.config[section]

            # キーの階層を分割
            keys = key.split('.')
            
            # 階層を順に探索
            for k in keys:
                if k not in current:
                    st.warning(f"設定が見つかりません: {section}.{key}")
                    return None
                current = current[k]

            return current

        except Exception as e:
            st.error(f"設定の取得エラー: {str(e)}")
            return None

    def get_required_columns(self, data_type: str) -> List[str]:
        """必須カラムのリストを取得します。"""
        return self.get_settings(data_type, 'input_validation.required_columns') or []

    def get_numeric_columns(self, data_type: str) -> List[str]:
        """数値カラムのリストを取得します。"""
        return self.get_settings(data_type, 'input_validation.numeric_columns') or []

    def get_input_columns(self, data_type: str) -> Dict[str, Any]:
        """入力データのグループ定義を取得します。"""
        return self.get_settings(data_type, 'input_columns') or {}

    def get_columns_rename(self, data_type: str) -> Dict[str, str]:
        """
        カラム名変換ルールを取得
        Args:
            data_type: データ種類
        Returns:
            Dict[str, str]: カラム名変換ルール
        """
        return self.get_settings(data_type, 'transformations.columns_rename') or {}

    def get_total_columns(self, data_type: str) -> Dict[str, List[str]]:
        """
        合計カラム定義を取得
        Args:
            data_type: データ種類
        Returns:
            Dict[str, List[str]]: 合計カラム定義
        """
        return self.get_settings(data_type, 'aggregations.total_columns') or {}

    def get_output_columns(self, data_type: str) -> List[str]:
        """
        出力カラム順序を取得
        Args:
            data_type: データ種類
        Returns:
            List[str]: 出力カラム順序
        """
        return self.get_settings(data_type, 'output_settings.columns_order') or []

    def get_rules(self, data_type: str) -> Dict[str, Any]:
        """
        指定されたデータ種類の処理ルールを取得します。

        Args:
            data_type (str): データの種類 ('salary' または 'bonus')

        Returns:
            Dict[str, Any]: 処理ルール
        """
        if data_type.lower() not in self.config:
            raise ValueError(f"Unknown data type: {data_type}")
        return self.config[data_type.lower()]

    def get_code_mappings(self, data_type: str) -> Dict[str, Dict[Union[int, str], Union[int, str]]]:
        """
        コード変換ルールを取得
        Args:
            data_type: データ種類
        Returns:
            Dict[str, Dict[Union[int, str], Union[int, str]]]: コード変換ルール
        """
        return self.get_settings(data_type, 'transformations.code_mappings') or {}

    def get_segment_names(self, data_type: str) -> Dict[str, str]:
        """
        セグメント名称マッピングを取得
        Args:
            data_type: データ種類
        Returns:
            Dict[str, str]: セグメント名称マッピング
        """
        return self.get_settings(data_type, 'transformations.segment_names') or {}

    def get_total_columns(self, data_type: str) -> Dict[str, List[str]]:
        """
        合計カラム定義を取得
        Args:
            data_type: データ種類
        Returns:
            Dict[str, List[str]]: 合計カラム定義
        """
        return self.get_settings(data_type, 'aggregations.total_columns') or {}

    def get_conditional_rules(self, data_type: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        条件付き変換ルールを取得
        Args:
            data_type: データ種類
        Returns:
            Dict[str, List[Dict[str, Any]]]: 条件付き変換ルール
        """
        return self.get_settings(data_type, 'conditional_rules') or {}

    def get_department_codes(self, data_type: str) -> Dict[str, str]:
        """
        部署コード変換ルールを取得
        Args:
            data_type: データ種類
        Returns:
            Dict[str, str]: 部署コード変換ルール
        """
        return self.get_settings(data_type, 'transformations.department_codes') or {}

    def get_section_codes(self, data_type: str) -> Dict[str, str]:
        """
        所属コード変換ルールを取得
        Args:
            data_type: データ種類
        Returns:
            Dict[str, str]: 所属コード変換ルール
        """
        return self.get_settings(data_type, 'transformations.section_codes') or {}

    def get_segments(self, data_type: str) -> Dict[str, Any]:
        """
        セグメント設定を取得
        Args:
            data_type: データ種類
        Returns:
            Dict[str, Any]: セグメント設定
        """
        return self.get_settings(data_type, 'segments') or {}

    def get_segment_rules(self, data_type: str) -> List[Dict[str, Any]]:
        """
        セグメントコード変換ルールを取得
        Args:
            data_type: データ種類
        Returns:
            List[Dict[str, Any]]: セグメントコード変換ルール
        """
        return self.get_settings(data_type, 'transformations.segment_rules') or []

    def get_department_segment_rules(self, data_type: str) -> List[Dict[str, Any]]:
        """
        部門別セグメント変換ルールを取得
        Args:
            data_type: データ種類
        Returns:
            List[Dict[str, Any]]: 部門別セグメント変換ルール
        """
        return self.get_settings(data_type, 'transformations.department_segment_rules') or []

    def get_columns_order(self, data_type: str) -> list:
        """列の順序を取得します。"""
        return self.get_rules(data_type)['columns_order']

    def get_replace_rules(self, data_type: str) -> Dict[str, Dict[int, int]]:
        """コード変換ルールを取得します。"""
        return self.get_rules(data_type)['replace_rules'] 