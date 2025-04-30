from dataclasses import dataclass, field
from typing import List

@dataclass
class PaymentConfig:
    """支払方法に関する定数を管理するクラス"""
    
    # 全支払方法
    TARGET_PAYMENT_ALL: List[str] = field(default_factory=lambda: [
        '引落',
        '現金',
        'クレジット',
        '滞納（コンビニ）',
        '払先未定（コンビニ）',
        'CTC',
        '債権回収',
        '織機給与天引き',
        '貸倒処理待ち'
    ])

    # デフォルトで除外する項目
    TARGET_EXCLUDING_PAYMENT: List[str] = field(default_factory=lambda: [
        '振込',
        'その他',
        'アプリデモ',
        '口座閉鎖'
    ])

    # SMS支払方法
    TARGET_PAYMENT_SMS: List[str] = field(default_factory=lambda: [
        '引落',
        '現金',
        'クレジット',
        '滞納（コンビニ）',
        '払先未定（コンビニ）',
        '債権回収'
    ])

    # 織機給与天引き
    TARGET_PAYMENT_SHOKKI: List[str] = field(default_factory=lambda: ['織機給与天引き'])

    # CTC
    TARGET_PAYMENT_CTC: List[str] = field(default_factory=lambda: ['CTC'])