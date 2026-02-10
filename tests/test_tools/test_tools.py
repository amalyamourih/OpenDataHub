from dbfread import DBF
import pandas as pd

dbf = DBF('tests/test_tools/shp-sim-france.dbf', encoding='utf-8')
frame = pd.DataFrame(iter(dbf))
print(frame.head())