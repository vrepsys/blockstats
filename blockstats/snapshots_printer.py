from texttable import Texttable

def print_snapshots(snapshots):
    table = Texttable()
    table.set_deco(Texttable.HEADER)
    table.set_cols_align(["l", "r", "r"])
    rows = [[snapshot['_id'], snapshot.get('start'), snapshot.get('finish')] for snapshot in snapshots]
    table.add_rows([["id", "start", "finish"]] + rows)
    print(table.draw())
    print()
