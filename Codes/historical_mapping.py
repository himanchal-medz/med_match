import random
import subprocess
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import re
import requests
import time
import numpy as np
import os
import pandas as pd
import datetime
import calendar
import time
import warnings
import datetime as dt
import time
import gc
from collections import OrderedDict
import math
from collections import Counter


historical_map_1 = pd.read_csv('historical_mapping/hist_map_latest.csv')
# historical_map = historical_map[['item_code','brand_x', 'brand_y', 'id']]
print(historical_map)