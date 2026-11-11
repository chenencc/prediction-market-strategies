import importlib.util
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/"src"))
sys.path.insert(0, str(ROOT.parent/"market-data-collector/services"))
spec=importlib.util.spec_from_file_location("history",ROOT/"tools/pair_history_sync.py")
history=importlib.util.module_from_spec(spec)
spec.loader.exec_module(history)
from prediction_market_strategies.backtest.pair_research import parse_pairs


def entry(first,last):
    return {"path":f"segments/{first:020d}-{last:020d}.json"}


class HistoryTests(unittest.TestCase):
    def test_entire_contiguous_snapshot(self):
        self.assertEqual(history.validate_entries([entry(1,10),entry(11,20)],1),20)

    def test_gap_overlap_empty_and_reorder_rejected(self):
        for entries in ([],[entry(2,10)],[entry(1,10),entry(12,20)],
                        [entry(1,10),entry(10,20)],[entry(11,20),entry(1,10)]):
            with self.subTest(entries=entries),self.assertRaises(ValueError):
                history.validate_entries(entries,1)

    def test_pair_filter_and_shared_mirror(self):
        h=dict(collection="test",markets=[[1,"polymarket","p"],[2,"predictfun","f"],[3,"polymarket","s"]],
               instruments=[[1,1,"py",0,"native"],[2,1,"pn",1,"native"],
                            [3,2,"fy",1,"native"],[4,2,"fn",2,"derived_complement"],
                            [5,3,"sy",0,"native"],[6,3,"sn",1,"native"]],
               pairs=[[1,[1,2],[["yes",[1,3]],["no",[2,4]]],10,None],
                      [2,[3],[["yes",[5]],["no",[6]]],10,None]])
        ps,inv=parse_pairs(h,20)
        self.assertEqual(len(inv),1)
        self.assertEqual(len(ps),2)
        self.assertEqual(ps[0].start_ns,20)
        self.assertTrue(ps[0].legs[1].mirror)
        self.assertFalse(ps[1].legs[1].mirror)
        self.assertEqual(ps[0].legs[1].instrument,ps[1].legs[1].instrument)
        self.assertEqual(inv[0]["rule_equivalence"],"unverified_mapping_only")


if __name__=="__main__":
    unittest.main()
