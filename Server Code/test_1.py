path='/home/ubuntu/filestore'

def wrapper(dist_name):
        
    from datetime import datetime
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
   
    warnings.filterwarnings("ignore")
    
    #####################################################################################################
    ################ Directory where input file is stored and output files will be saved ################
    #####################################################################################################
    
    #os.chdir('../Output/')
    #os.chdir('/Arun/_SelfLearn/Mapping/')
    print('dist_name:',dist_name)
    inputdata=dist_name+"_data.csv"
    outfile1=dist_name+";;"+"Combined_results.csv"
    outfile2=dist_name+";;"+"exact_mapped.csv"
    outfile3=dist_name+";;"+"partial_mapped.csv"
    outfile4=dist_name+";;"+"historical_mapped.csv"
    
    #####################################################################################################
    #df_master = pd.read_csv('drug_master2.csv') 
    df_master = pd.read_csv('drug_master_07March.csv', encoding='ISO-8859-1') # Updated Master Data with sku for all
    #df_distributor = pd.read_csv('distributor_data.csv') # New Distributor data to be mapped
    df_distributor = pd.read_csv(inputdata, encoding='ISO-8859-1') # New Distributor data to be mapped
    #df_distributor = df_distributor[['item_code', 'brand', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']]
    
    # Required Schema of Distributor Data - ['item_code', 'brand', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']
    # Appending columns if not already present, order should be maintained by distributor
    i = 1
    while(len(df_distributor.columns)<7):
        new_col_name = "new_col" + str(i)
        i = i+1
        df_distributor[new_col_name] = ""
    
    df_distributor.columns = ['item_code', 'brand', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']
    df_distributor['Master Catalogue name'] = ""
    
    #df_distributor['brand'] = df_distributor['brand'] + ' ' + df_distributor['pack']
    
    
    # Handling cases where sku_brand is not present in master data
    df_master['sku_brand'] = np.where(df_master['sku_brand'].isnull(), df_master['brand'].str.split(' ').str[0], df_master['sku_brand'])
    
    synonyms_dict = {'':['dt', 'sr', 'cr', 'mr', 'er', 'dr', 'pr', 'xr', 'xl', 'xt', 'pr', 'dpi', 'mdi', 'md', 'ap', 'md'],
         '1s':['1x1','1[*]1','1tab','1x1'],
         '2s':['2x1','1[*]2','2tab','1x2'],
         '3s':['3x1','1[*]3','3tab','1x3'],
         '4s':['4x1','1[*]4','4tab','1x4'],
         '5s':['5x1','1[*]5','5tab','1x5'],
         '6s':['6x1','1[*]6','6tab','1x6'],
         '7s':['7x1','1[*]7','7tab','1x7'],
         '8s':['8x1','1[*]8','8tab','1x8'],
         '9s':['9x1','1[*]9','9tab','1x9'],
         '10s':['10x1','1[*]10','10tab','1x10'],
         '120s':['120x1', '1[*]120','120tab','1x120'],
         '150s':['150x1', '1[*]150','150tab','1x150'],
         '200s':['200x1', '1[*]200','200tab','1x200'],
         '15s':['15x1','1[*]15','15tab','1x15'],
         '20s':['20x1','1[*]20','20tab','1x20'],
         '25s':['25x1','1[*]25','25tab','1x25'],
         '30s':['30x1','1[*]30','30tab','1x30'],
         '35s':['35x1','1[*]35','35tab','1x35'],
         '40s':['40x1','1[*]40','40tab','1x40'],
         '50s':['50x1','1[*]50','50tab','1x50'],
         '60s':['60x1','1[*]60','60tab','1x60'],
         '70s':['70x1','1[*]70','70tab','1x70'],
         '80s':['80x1','1[*]80','80tab','1x80'],
         '90s':['90x1','1[*]90','90tab','1x90'],
         'adult':['adults'],
         'aloe vera':['aloe','aloe vera','aloevera','elovera','alovera'],
         'vera':['vera vera'],
         'amla':['aamla'],
         'amritarishta':['amritarist','amritarisht','amritarishta','amritarista'],
         'amrutanjan':['amritanjan'],
         'anti':['anit'],
         'arjunarishta':['arjunarishta','arjunarista','arjunaristha'],
         'ashokarishta':['ashokarishta','ashokarista','ashokarisht','ashokarist'],
         'baby':['infant'],
         'soap':['bar'],
         'brush':['toothbrush','tooth brush'],
         'chocolate':['choclate','choco'],
         'cleansing':['cleaning'],
         'cream':['creme','creem','crema'],
         'culture':['cullture'],
         'dental gel':['oral gel'],
         'diapers':['diaper'],
         'diskette':['diskettes'],
         'drops':['drop', 'drps'],
         'expectorant':['expt'],
         'eye drops':['opthalmic solution','eye drop', 'e/d'],
         'eye':['eyes','opthalmic'],
         'eye/ear':['e/e'],
         'injection':['inj', 'inj.', 'inj.s', 'injs', 'injections', 'injtion', 'inje'],
         'janma':['janam'],
         'gel':['jelly'],
         'liquid':['liq','fluid'],
         'lotion':['moist'],
         'lozenges':['lozenge'],
         'md':['mdi'],
         'mixtard':['mixact','mistard'],
         'moisturiser':['moisturizer'],
         'moisturising':['moistarizing','moistursing'],
         'moov':['move'],
         'mother':['mom'],
         'ointment':['oint'],
         'paediatric':['paed', 'ped', 'junior', 'kid', 'kids', 'child', 'kidzo', 'pediatric', 'peadiatric', 'peditric'], 
         'paper':['strip'],
         'penfill':['pen','penfill','augpen','flexpen','flexpens'],
         'pessaries':['pessary'],
         'plus':["[+]"],
         'protein':['protien','portien'],
         'readymix':['readimix', 'ready mix'],
         'refill':['refil','rifill','rifil'],
         'wash':['rinse'],
         'capsule':['rotacap', 'rotacaps', 'transcap', 'transcaps', 'cap', 'caps', 'cap.', 'capsules', 'capsule.', 'rosycap', 'instacap', 'instacaps'],
         'skin':['derma'],
         'sg':['soft gels','soft gelatin'],
         'solution':['soln', 'soln.'],
         'spray':['spry'],
         'sf':['sugar free'],
         'suppository':['suppositories', 'supositories', 'supository'],
         'suspension':['susp', 'susp.', 'suspp', 'susppension', 'suspention'],
         'syrup':['syp', 'syp.', 'syrp', 'syr', 'syp.s', 'syps', 'syrups'],
         'tablet':['tab', 'tabs', 'tablets', 'tab.', 'tablet.', 'tab.s', 'tablt', 'chewtab'],
         'tel':['tail'],
         'thermometer':['thermometre','therometer'],
         'toothpaste':['paste','tooth paste', 'tooth toothpaste'],
         'vaccine':['vax', 'vac'],
         'vanilla':['vanila'],
         '1kg':['1000gm', '1000g'],
         '1000mg':['1gm', '1g'],
         '2gm':['2g'],
         '3gm':['3g'],
         '4gm':['4g'],
         '5gm':['5g'],
         '10gm':['10g'],
         '100gm':['100g'],
         '1ml':['1x1ml'],
         '2ml':['1x2ml'],
         '3ml':['1x3ml'],
         '4ml':['1x4ml'],
         '5ml':['1x5ml'],
         '6ml':['1x6ml'],
         '7ml':['1x7ml'],
         '8ml':['1x8ml'],
         '9ml':['1x9ml'],
         '10ml':['1x10ml'],
         '15ml':['1x15ml'],
         '20ml':['1x20ml'],
         '25ml':['1x25ml'],
         '30ml':['1x30ml'],
         '35ml':['1x35ml'],
         '40ml':['1x40ml'],
         '45ml':['1x45ml'],
         '50ml':['1x50ml'],
         '60ml':['1x60ml'],
         '70ml':['1x70ml'],
         '80ml':['1x80ml'],
         '90ml':['1x90ml'],
         '100ml':['1x100ml'],
         '200ml':['1x200ml'],
         '300ml':['1x300ml'],
         '400ml':['1x400ml'],
         '500ml':['1x500ml'],
         '1mg':['1x1mg'],
         '2mg':['1x2mg'],
         '3mg':['1x3mg'],
         '4mg':['1x4mg'],
         '5mg':['1x5mg'],
         '6mg':['1x6mg'],
         '7mg':['1x7mg'],
         '8mg':['1x8mg'],
         '9mg':['1x9mg'],
         '10mg':['1x10mg'],
         '15mg':['1x15mg'],
         '20mg':['1x20mg'],
         '25mg':['1x25mg'],
         '30mg':['1x30mg'],
         '35mg':['1x35mg'],
         '40mg':['1x40mg'],
         '45mg':['1x45mg'],
         '50mg':['1x50mg'],
         '60mg':['1x60mg'],
         '70mg':['1x70mg'],
         '80mg':['1x80mg'],
         '90mg':['1x90mg'],
         '100mg':['1x100mg'],
         '200mg':['1x200mg'],
         '300mg':['1x300mg'],
         '400mg':['1x400mg'],
         '500mg':['1x500mg'],
         '1gm':['1x1gm'],
         '2gm':['1x2gm'],
         '3gm':['1x3gm'],
         '4gm':['1x4gm'],
         '5gm':['1x5gm'],
         '6gm':['1x6gm'],
         '7gm':['1x7gm'],
         '8gm':['1x8gm'],
         '9gm':['1x9gm'],
         '10gm':['1x10gm'],
         '15gm':['1x15gm'],
         '20gm':['1x20gm'],
         '25gm':['1x25gm'],
         '30gm':['1x30gm'],
         '35gm':['1x35gm'],
         '40gm':['1x40gm'],
         '45gm':['1x45gm'],
         '50gm':['1x50gm'],
         '60gm':['1x60gm'],
         '70gm':['1x70gm'],
         '80gm':['1x80gm'],
         '90gm':['1x90gm'],
         '100gm':['1x100gm'],
         '200gm':['1x200gm'],
         '300gm':['1x300gm'],
         '400gm':['1x400gm'],
         '500gm':['1x500gm'],
         '1g':['1x1g'],
         '2g':['1x2g'],
         '3g':['1x3g'],
         '4g':['1x4g'],
         '5g':['1x5g'],
         '6g':['1x6g'],
         '7g':['1x7g'],
         '8g':['1x8g'],
         '9g':['1x9g'],
         '10g':['1x10g'],
         '15g':['1x15g'],
         '20g':['1x20g'],
         '25g':['1x25g'],
         '30g':['1x30g'],
         '35g':['1x35g'],
         '40g':['1x40g'],
         '45g':['1x45g'],
         '50g':['1x50g'],
         '60g':['1x60g'],
         '70g':['1x70g'],
         '80g':['1x80g'],
         '90g':['1x90g'],
         '100g':['1x100g'],
         '200g':['1x200g'],
         '300g':['1x300g'],
         '400g':['1x400g'],
         '500g':['1x500g']
    }

    #### Historical Mapping
    historical_map_1 = pd.read_csv('../historical_mapping/hist_map_latest.csv')
    historical_map_1 = historical_map_1[['item_code','brand_x', 'brand_y', 'drug_master_id']]

    historical_map_2 = pd.read_csv('../historical_mapping/hist_map_2.csv')
    historical_map_2 = historical_map_2[['item_code','brand_x', 'brand_y', 'drug_master_id']]

    historical_map = pd.concat([historical_map_1, historical_map_2])

    historical_map['brand_x'] = historical_map['brand_x'].str.lower()
    df_distributor['brand'] = df_distributor['brand'].str.lower()

    final = pd.merge(df_distributor, historical_map, how='left', left_on=['brand'],right_on=['brand_x'])
    final_mapped = final[final['brand_y'].notnull()].reset_index(drop = True)
    print("Historical mappings: ",final_mapped.shape[0])
    final_mapped = final_mapped[['item_code_x','brand','brand_y','drug_master_id']]
    final_mapped.to_csv(outfile4, index = False)

    final = final[final['brand_x'].isnull()].reset_index(drop = True)
    final = final[['item_code_x', 'brand', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp','Master Catalogue name']]
    final = final.rename(columns = {'item_code_x':'item_code'})
    df_distributor = final.copy()