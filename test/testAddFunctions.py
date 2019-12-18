import os
import sys
import pdb
sys.path.append('..')
sys.path.append('./../add-profiles')
import unittest
from datetime import datetime, timedelta
import warnings
from numpy import warnings as npwarnings
import glob
import addFunctions as af
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

class testAddFunctions(unittest.TestCase):

    def setUp(self):
        self.OUTPUTDIR = os.path.join(os.getcwd(), 'test-files')

    def test_get_df_to_add(self):
        df = self.df
        files = df.file.tolist()
        self.assertIsInstance(files, list)
        self.assertGreater(len(files), 0)
        
        columns = ['file', 'filename', 'profile', 'prefix', 'platform', 'dac', 'catagory']
        self.assertEqual(len(columns), df.shape[1])
        for col in df.columns.tolist():
            self.assertIn(col, columns)
        includeDacs = ['csio', 'kordi']
        df = af.get_df_to_add(self.OUTPUTDIR, includeDacs)
        df['dac'] = df['file'].apply(lambda x: x.split('/')[-4])
        dacs = df.dac.unique().tolist()
        self.assertEqual(len(dacs), len(includeDacs))
        for dac in dacs:
            self.assertIn(dac, includeDacs)

    def test_remove_duplicate_if_mixed_or_synthetic(self):

        # Synthetic: check if core and mixed have been removed
        files = glob.glob(os.path.join(self.OUTPUTDIR, '**', '**', 'profiles', '*1900722*.nc'))
        df = af.remove_duplicate_if_mixed_or_synthetic(files)

        for row in df.itertuples(index=False):
            self.assertTrue('S' in row.prefix, 'mixed and core need to be removed')

        # Mixed: check if core has been removed (no synthetic)
        files = glob.glob(os.path.join(self.OUTPUTDIR, '**', '**', 'profiles', '*5903593*.nc'))
        df = af.remove_duplicate_if_mixed_or_synthetic(files)

        for row in df.itertuples(index=False):
            self.assertTrue('M' in row.prefix, 'core need to be removed')

        # Core: check if core has not been removed(no synthetic or mixed)
        files = glob.glob(os.path.join(self.OUTPUTDIR, '**', '**', 'profiles', '*4902325*.nc'))
        df = af.remove_duplicate_if_mixed_or_synthetic(files)

        self.assertTrue(len(files) == len(df.file.tolist()), 'core profiles should not have been removed.')

if __name__ == '__main__':
    unittest.main()