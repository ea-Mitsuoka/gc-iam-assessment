from typing import Iterator, Set
# 変更点: identity_clientのインポートを削除

def expand_member(
    identity_client, member_type: str, member_id: str, visited_groups: Set[str]
) -> Iterator[str]:
    """メンバーがグループの場合、再帰的に展開する"""
    
    if member_type == "GROUP":
        # 修正点: 循環参照チェックをグループIDに対してのみ行う
        if member_id in visited_groups:
            return
        # 修正点: visited_groups への追加をブロックの内側に移動
        visited_groups.add(member_id)

        try:
            # 1. グループID (メールアドレス) からグループの `name` (例: groups/123xyz) を取得
            group_name = identity_client.lookup_group_name(group_key={'id': member_id}).name
            
            # 修正点: `get_membership` が不要になるよう `view=1` (FULL) を指定
            # 修正点: `parent` の指定方法を `parent=group_name` に修正
            memberships = identity_client.list_memberships(parent=group_name, view=1)
            
            for membership in memberships:
                # `view=1` のおかげで、`membership` に全情報が含まれる
                member_email = membership.preferred_member_key.id
                
                # 修正点: `membership.type_` を使って次のメンバータイプを判別
                if membership.type_ == 2: # 2 = GROUP
                    next_member_type = "GROUP"
                elif membership.type_ == 1: # 1 = USER
                    if ".gserviceaccount.com" in member_email:
                        next_member_type = "SERVICE_ACCOUNT"
                    else:
                        next_member_type = "USER"
                else:
                    continue # 不明なタイプはスキップ

                # 変更点: identity_clientを再帰呼び出しに渡す
                yield from expand_member(
                    identity_client, next_member_type, member_email, visited_groups.copy()
                )
        except Exception:
            # グループ展開に失敗した場合 (例: 権限不足)
            yield f"{member_type} (UNEXPANDED):{member_id}"
    else:
        # メンバータイプが GROUP 以外 (USER, SERVICE_ACCOUNT, SPECIAL_GROUP) の場合
        yield f"{member_type}:{member_id}"