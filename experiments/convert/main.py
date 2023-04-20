from typing import List

if __name__ == '__main__':
    with open('../convertedData/brand1.tbl', 'w') as brand1:
        with open('../convertedData/mfgr.tbl', 'w') as mfgr:
            with open('../convertedData/category.tbl', 'w') as category:
                with open('../convertedData/part.tbl', 'w') as part_convert:
                    with open('../data/part.tbl', 'r') as part:
                        part_items: List[List[str]] = []
                        for line in part:
                            items: List[str] = line.split(',')
                            part_items.append(list(",".join([*items[:2], *items[5:]])))


