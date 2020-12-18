import pandas as pd
from pprint import pprint
import json
test =  pd.read_pickle('computed_1.pkl')

print(json.loads(test['diff'][4])['sys-libs%2Fglibc%2Fglibc-2.10.1-r2.ebuild'].split('<NL>'))