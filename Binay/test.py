import numpy as np
import pandas as pd
from collections import OrderedDict

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
    
    #os.chdir('/Arun/_SelfLearn/Mapping/')
    #os.chdir('/Arun/_SelfLearn/Mapping/')
    print('dist_name:',dist_name)
    inputdata=dist_name+"_data.csv"
    outfile1=dist_name+"_"+"Combined_results.csv"
    outfile2=dist_name+"_"+"exact_mapped.csv"
    outfile3=dist_name+"_"+"partial_mapped.csv"
    
    #####################################################################################################
    #df_master = pd.read_csv('drug_master2.csv') 
    df_master = pd.read_csv('latest_master.csv', encoding='ISO-8859-1') # Updated Master Data with sku for all
    #df_distributor = pd.read_csv('distributor_data.csv') # New Distributor data to be mapped
    df_distributor = pd.read_csv('BinayRequest.csv', encoding='ISO-8859-1') # New Distributor data to be mapped
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
    
    
    
    
    ################# Applying synonyms to Distributor Data ################
    
    df_distributor['brand'] = df_distributor['brand'].str.lower()
    
    df_distributor['mod_brand'] = df_distributor['brand'].str.lower()
    #df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace("  ", " ")
    #df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace("   ", " ")
    df_distributor['mod_brand'] = df_distributor['mod_brand'] + ' '
    df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace('-', ' ')
    #df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace('.', ' ')
    df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace(',', ' ')
    df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace('^', ' ')
    df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace('#', ' ')
    df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace('[(]', ' ')
    df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace('[)]', ' ')
    df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace('`', '')
    df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace("'", "")
    df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace("%", "% ")
    df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace("  ", " ")
    df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace("  ", " ")
    
        
    for key,values in synonyms_dict.items():
        key = ' ' + key + ' '
        for val in values:
            val = ' ' + val + ' '
            df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace(val, key)
    
    
    df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace('*', ' ')
    df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace("  ", " ")
    df_distributor['mod_brand'] = df_distributor['mod_brand'].str.replace("  ", " ")
    df_distributor['mod_brand'] = df_distributor['mod_brand'].str.lower().str.strip()
    
    # Identify type of medicine - classifiy into tablet, capsule, injection, syrup, others
    
    df_distributor['medicine_type'] = ""
    
    df_distributor['medicine_type'] = np.where(df_distributor['mod_brand'].str.contains('tablet', regex = False), 'tablet', df_distributor['medicine_type'])
    df_distributor['medicine_type'] = np.where(df_distributor['mod_brand'].str.contains('capsule', regex = False), 'capsule', df_distributor['medicine_type'])
    df_distributor['medicine_type'] = np.where(df_distributor['mod_brand'].str.contains('injection', regex = False), 'injection', df_distributor['medicine_type'])
    df_distributor['medicine_type'] = np.where(df_distributor['mod_brand'].str.contains('syrup', regex = False), 'syrup', df_distributor['medicine_type'])
    
    
    df_distributor['medicine_type'] = np.where(df_distributor['medicine_type']=="", 'others', df_distributor['medicine_type'])
    
    
    ################# Applying synonyms to Master Data ################
    
    df_master['brand'] = df_master['brand'].str.lower()
    
    df_master['new_brand'] = df_master['brand'].str.lower()
    df_master['new_brand'] = df_master['new_brand'].str.replace("  ", " ")
    df_master['new_brand'] = df_master['new_brand'].str.replace("   ", " ")
    df_master['new_brand'] = df_master['brand'] + ' '
    df_master['new_brand'] = df_master['new_brand'].str.replace('-', ' ')
    df_master['new_brand'] = df_master['new_brand'].str.replace('^', ' ')
    df_master['new_brand'] = df_master['new_brand'].str.replace('*', ' ')
    df_master['new_brand'] = df_master['new_brand'].str.replace('#', ' ')
    df_master['new_brand'] = df_master['new_brand'].str.replace('[(]', ' ')
    df_master['new_brand'] = df_master['new_brand'].str.replace('[)]', ' ')
    df_master['new_brand'] = df_master['new_brand'].str.replace('`', '')
    df_master['new_brand'] = df_master['new_brand'].str.replace("'", "")
    df_master['new_brand'] = df_master['new_brand'].str.replace("%", "% ")
    df_master['new_brand'] = df_master['new_brand'].str.replace("  ", " ")
    df_master['new_brand'] = df_master['new_brand'].str.replace("  ", " ")
    
    for key,values in synonyms_dict.items():
        key = ' ' + key + ' '
        for val in values:
            val = ' ' + val + ' '
            df_master['new_brand'] = df_master['new_brand'].str.replace(val, key)
    
    df_master['new_brand'] = df_master['new_brand'].str.replace("  ", " ")
    df_master['new_brand'] = df_master['new_brand'].str.replace("  ", " ")
    
    df_master['sku_brand'] = df_master['sku_brand'].str.lower().str.strip()
    df_master['sku_brand'] = df_master['sku_brand'].str.replace("  ", " ")
    df_master['sku_brand'] = df_master['sku_brand'].str.replace("  ", " ")
    
    # Identify type of medicine - classifiy into tablet, capsule, injection, syrup, others
    
    df_master['medicine_type'] = ""
    
    df_master['medicine_type'] = np.where(df_master['new_brand'].str.contains('tablet', regex = False), 'tablet', df_master['medicine_type'])
    df_master['medicine_type'] = np.where(df_master['new_brand'].str.contains('capsule', regex = False), 'capsule', df_master['medicine_type'])
    df_master['medicine_type'] = np.where(df_master['new_brand'].str.contains('injection', regex = False), 'injection', df_master['medicine_type'])
    df_master['medicine_type'] = np.where(df_master['new_brand'].str.contains('syrup', regex = False), 'syrup', df_master['medicine_type'])
    
    df_master['medicine_type'] = np.where(df_master['medicine_type']=="", 'others', df_master['medicine_type'])
    
    
    
    
    ###################################################################
    ######################## COSINE SIMILARITY ########################
    ###################################################################
    
    WORD = re.compile(r"\w+")
    
    def get_cosine(vec1, vec2):
        intersection = set(vec1.keys()) & set(vec2.keys())
        numerator = sum([vec1[x] * vec2[x] for x in intersection])
    
        sum1 = sum([vec1[x] ** 2 for x in list(vec1.keys())])
        sum2 = sum([vec2[x] ** 2 for x in list(vec2.keys())])
        denominator = math.sqrt(sum1) * math.sqrt(sum2)
    
        if not denominator:
            return 0.0
        else:
            return float(numerator) / denominator
    
    
    def text_to_vector(text):
        words = WORD.findall(text)
        return Counter(words)
    
    ###################################################################
    
    
    
    
    ###################################################################################
    ########################## Preprocess and match capsules ##########################
    ###################################################################################
    
    
    df_master_cap = df_master[df_master['medicine_type']=='capsule'].reset_index(drop = True)
    df_cap = df_distributor[df_distributor['medicine_type']=='capsule'].reset_index(drop = True)
    
    def preprocess_capsules():
        
        df_cap['mod_brand'] = df_cap['mod_brand'].str.lower()
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.lower()
        
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.replace('-', ' ')
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.replace('^', ' ')
        
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.replace('1g ', '1000mg ')
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.replace(' mg', 'mg')
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.replace(' ml', 'ml')
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.replace(' mcg', 'mcg')
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.replace(' gm', 'gm')
        
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.replace('1gm', '1000mg')
        
        df_master_cap['new_brand2'] = df_master_cap['new_brand']
        df_master_cap['new_brand2'] = df_master_cap['new_brand2'].str.replace('  ', ' ')
        df_master_cap['new_brand2'] = df_master_cap['new_brand2'].str.strip()
        
        df_master_cap['new_brand'] = df_master_cap['new_brand'] + ' '
        
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.replace('mg', '')
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.replace('ml', '')
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.replace('mcg', '')
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.replace('gm', '')
        
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.replace('capsules', 'capsule')
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.replace(' capsule ', ' ')
        
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.replace('  ', ' ')
        df_master_cap['new_brand'] = df_master_cap['new_brand'].str.strip()
        
        df_cap['exp_sku_brand'] = ""
        #df_cap['mod_brand'] = ""
        df_cap['count_cap'] = 0
    
        for n in range(df_cap.shape[0]):
    
            text1 = str(df_cap['mod_brand'][n])
            #print(text1)
            
            text1 = text1.replace('-', ' ')
            text1 = text1.replace('^', '')
            text1 = text1.replace('`', '')
            text1 = text1.replace("'", "")
            text1 = text1.replace("*", "")
            
            text1 = text1.replace("x1", "s")
    
            text1 = text1.replace(' mg', 'mg')
            text1 = text1.replace(' ml', 'ml')
            text1 = text1.replace(' gm', 'gm')
            text1 = text1.replace(' mcg', 'mcg')
            
            text1 = text1.replace('mg.', 'mg')
            text1 = text1.replace('ml.', 'ml')
            text1 = text1.replace('gm.', 'gm')
            text1 = text1.replace('mcg.', 'mcg')
            
            text1 = text1.replace("1gm", "1000mg")
            
            text1 = text1.replace('cap', ' cap')
            text1 = text1.replace('  ', ' ')
    
            text1 = text1.replace(' cap.', ' cap')
            text1 = text1.replace(' capsules', ' cap')
            text1 = text1.replace(' capsule.', ' cap')
            text1 = text1.replace(' cap', ' capsule')
            text1 = text1.replace(' capsules', ' capsule') # Handling ' caps' cases - ' cap' already replaced by ' capsule'
            
            text1 = text1.replace(' capsuleule', ' capsule')
            text1 = text1.replace(' capsulesules', ' capsule')
            text1 = text1.replace(' capsulesule', ' capsule')
            
            text1 = text1.replace('  ', ' ')
            
    
            df_cap['count_cap'][n] = text1.count('capsule')
    
            if(df_cap['count_cap'][n]==2):
                text1 = 's'.join(text1.rsplit(' capsule', 1))
    
            #print(text1)
    
            df_cap['mod_brand'][n] = text1.strip()
    
            tl1 = text1.split(' ')
            #print(tl1)
            text11 = ""
    
            for i in range(len(tl1)):
    
                #print(i,'/',len(tl1), end = ', ')
    
                if( ( ('mg' in tl1[i]) & (i>0) ) | ('capsule' in tl1[i]) ):
                    break
    
                if( (i+1) < len(tl1) ):
    
                    if( (tl1[i].isnumeric()) & ('capsule' in tl1[i+1]) ):
                        break
    
                text11 = text11 + " " + tl1[i]
    
            #print(text11)
            df_cap['exp_sku_brand'][n] = text11.strip()
        
        df_cap['mod_brand2'] = df_cap['mod_brand']
        df_cap['mod_brand2'] = df_cap['mod_brand2'].str.strip()
        
        df_cap['mod_brand'] = df_cap['mod_brand'].str.replace('mg', '')
        df_cap['mod_brand'] = df_cap['mod_brand'].str.replace('ml', '')
        df_cap['mod_brand'] = df_cap['mod_brand'].str.replace('gm', '')
        df_cap['mod_brand'] = df_cap['mod_brand'].str.replace('mcg', '')
        df_cap['mod_brand'] = df_cap['mod_brand'].str.replace('mcg', '')
        df_cap['mod_brand'] = df_cap['mod_brand'].str.replace(' capsule ', ' ')
        df_cap['mod_brand'] = df_cap['mod_brand'].str.strip()
        
        df_cap['exp_sku_brand'] = df_cap['exp_sku_brand'].str.lower().str.strip()
        
        df_cap['exp_sku_brand2'] = df_cap['exp_sku_brand'].str.rsplit(' ', 1).str[0]
        df_cap['exp_sku_brand3'] = df_cap['exp_sku_brand2'].str.rsplit(' ', 1).str[0]
        df_cap['exp_sku_brand4'] = df_cap['exp_sku_brand3'].str.rsplit(' ', 1).str[0]
        
        df_master_cap['sku_brand'] = df_master_cap['sku_brand'].str.lower()
        df_master_cap['sku_brand'] = df_master_cap['sku_brand'].str.replace('-', ' ')
        df_master_cap['sku_brand'] = df_master_cap['sku_brand'].str.replace("'", ' ')
        df_master_cap['sku_brand'] = df_master_cap['sku_brand'].str.replace("  ", ' ')
        df_master_cap['sku_brand'] = df_master_cap['sku_brand'].str.replace('`', '')
        
        df_master_cap['sku_brand'] = df_master_cap['sku_brand'].str.split(' ').str[0]
        df_master_cap['sku_brand'] = df_master_cap['sku_brand'].str.lower().str.strip()
        
        df_cap['mod_brand'] = (df_cap['mod_brand'].str.split()
                                  .apply(lambda x: OrderedDict.fromkeys(x).keys())
                                  .str.join(' '))
        df_master_cap['new_brand'] = (df_master_cap['new_brand'].str.split()
                                  .apply(lambda x: OrderedDict.fromkeys(x).keys())
                                  .str.join(' '))
        
    
    def find_capsule():
        
        print('Total Capsules in Distributer Data -', df_cap.shape[0])
        
        #=================== Exact Match on full brand names ===================#
        
        df_final1 = pd.merge(df_cap, df_master_cap, how='left', left_on='mod_brand', right_on = 'new_brand')
        df_final1 = df_final1.drop_duplicates(subset=['item_code', 'brand_x'], keep='last', ignore_index = True)
        m = df_final1[df_final1['brand_y'].notnull()].shape[0]
        varn = 1;
        if df_cap.shape[0] != 0:
            varn =df_cap.shape[0]
        print('Exact Match on full brand names - ', m, ' - ', (round)(m*100/varn),'%', sep = '')
        
        df_cap2 = df_final1[df_final1['brand_y'].isnull()].reset_index(drop = True)
        df_cap2 = df_cap2[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_cap', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4']]
        df_cap2 = df_cap2.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Capsules -', df_cap2.shape[0])
        
        df_final1 = df_final1[df_final1['brand_y'].notnull()].reset_index(drop = True)
        
        
        
        #=================== Match on sku_brand and exp_sku_brand ===================#
        
        df_cap2['exp_sku_brand'] = df_cap2['exp_sku_brand'].str.lower().str.strip()
        df_master_cap['sku_brand'] = df_master_cap['sku_brand'].str.lower().str.strip()
        df_final2 = pd.merge(df_cap2, df_master_cap, how='left', left_on='exp_sku_brand', right_on = 'sku_brand')
        
        m = int(df_final2[df_final2['brand_y'].notnull()].item_code.nunique())
        varn = 1;
        if df_cap.shape[0] != 0:
            varn =df_cap.shape[0]
        print('Exact Match on exp_sku_brand - ', m, ' - ', (round)(m*100/varn),'%', sep = '')
        
        df_cap3 = df_final2[df_final2['brand_y'].isnull()].reset_index(drop = True)
        df_cap3 = df_cap3[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_cap', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4']]
        df_cap3 = df_cap3.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Capsules -', df_cap3.shape[0])
        
        df_final2 = df_final2[df_final2['brand_y'].notnull()].reset_index(drop = True)
        df_final2['cosine'] = -1.0
    
        for i in range(len(df_final2)):
    
            text1 = str(df_final2['mod_brand'][i])
            text2 = str(df_final2['new_brand'][i])
    
            vector1 = text_to_vector(text1)
            vector2 = text_to_vector(text2)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final2['cosine'][i] = cosine
        
        df_final2.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        df_final2_grp = df_final2.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand'], as_index = False).agg({'cosine':'max'}).rename(columns = {'cosine':'max_cosine'})
        df_final2 = pd.merge(df_final2, df_final2_grp, on=['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand'], how='left')
        
        df_final2 = df_final2[(df_final2['cosine']>=0) & ( df_final2['cosine']>=(0.9*df_final2['max_cosine']) )].reset_index(drop = True)
        #df_final2 = df_final2.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand', 'sku_brand'], as_index = False).agg({'brand_y':list, 'new_brand':list, 'cosine':list})
        #df_final2['count'] = df_final2['brand_y'].str.len()
        df_final2.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        #=================== Match on sku_brand and exp_sku_brand2 ===================#
        
        df_cap3['exp_sku_brand2'] = df_cap3['exp_sku_brand2'].str.lower().str.strip()
        df_master_cap['sku_brand'] = df_master_cap['sku_brand'].str.lower().str.strip()
        df_final3 = pd.merge(df_cap3, df_master_cap, how='left', left_on='exp_sku_brand2', right_on = 'sku_brand')
        
        m = int(df_final3[df_final3['brand_y'].notnull()].item_code.nunique())
        varn = 1;
        if df_cap.shape[0] != 0:
            varn =df_cap.shape[0]
        print('Exact Match on exp_sku_brand2 - ', m, ' - ', (round)(m*100/varn),'%', sep = '')
        
        df_cap4 = df_final3[df_final3['brand_y'].isnull()].reset_index(drop = True)
        df_cap4 = df_cap4[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_cap', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4']]
        df_cap4 = df_cap4.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Capsules -', df_cap4.shape[0])
        
        df_final3 = df_final3[df_final3['brand_y'].notnull()].reset_index(drop = True)
        df_final3['cosine'] = -1.0
    
        for i in range(len(df_final3)):
    
            text1 = str(df_final3['mod_brand'][i])
            text2 = str(df_final3['new_brand'][i])
    
            vector1 = text_to_vector(text1)
            vector2 = text_to_vector(text2)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final3['cosine'][i] = cosine
        
        df_final3.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        df_final3_grp = df_final3.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand2'], as_index = False).agg({'cosine':'max'}).rename(columns = {'cosine':'max_cosine'})
        df_final3 = pd.merge(df_final3, df_final3_grp, on=['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand2'], how='left')
        
        df_final3 = df_final3[(df_final3['cosine']>=0) & ( df_final3['cosine']>=(0.9*df_final3['max_cosine']) )].reset_index(drop = True)
        #df_final3 = df_final3.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand2', 'sku_brand'], as_index = False).agg({'brand_y':list, 'new_brand':list, 'cosine':list})
        #df_final3['count'] = df_final3['brand_y'].str.len()
        df_final3.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        
        #=================== Match on sku_brand and exp_sku_brand3 ===================#
        
        df_cap4['exp_sku_brand3'] = df_cap4['exp_sku_brand3'].str.lower().str.strip()
        df_master_cap['sku_brand'] = df_master_cap['sku_brand'].str.lower().str.strip()
        df_final4 = pd.merge(df_cap4, df_master_cap, how='left', left_on='exp_sku_brand3', right_on = 'sku_brand')
        
        m = int(df_final4[df_final4['brand_y'].notnull()].item_code.nunique())
        varn = 1;
        if df_cap.shape[0] != 0:
            varn =df_cap.shape[0]
        print('Exact Match on exp_sku_brand3 - ', m, ' - ', (round)(m*100/varn),'%', sep = '')
        
        df_cap5 = df_final4[df_final4['brand_y'].isnull()].reset_index(drop = True)
        df_cap5 = df_cap5[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_cap', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4']]
        df_cap5 = df_cap5.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Capsules -', df_cap5.shape[0])
        
        df_final4 = df_final4[df_final4['brand_y'].notnull()].reset_index(drop = True)
        df_final4['cosine'] = -1.0
    
        for i in range(len(df_final4)):
    
            text1 = str(df_final4['mod_brand'][i])
            text2 = str(df_final4['new_brand'][i])
    
            vector1 = text_to_vector(text1)
            vector2 = text_to_vector(text2)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final4['cosine'][i] = cosine
        
        df_final4.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        df_final4_grp = df_final4.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand3'], as_index = False).agg({'cosine':'max'}).rename(columns = {'cosine':'max_cosine'})
        df_final4 = pd.merge(df_final4, df_final4_grp, on=['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand3'], how='left')
        
        df_final4 = df_final4[(df_final4['cosine']>=0) & ( df_final4['cosine']>=(0.9*df_final4['max_cosine']) )].reset_index(drop = True)
        #df_final4 = df_final4.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand3', 'sku_brand'], as_index = False).agg({'brand_y':list, 'new_brand':list, 'cosine':list})
        #df_final4['count'] = df_final4['brand_y'].str.len()
        df_final4.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        #=================== Match on sku_brand and exp_sku_brand4 ===================#
        
        df_cap5['exp_sku_brand4'] = df_cap5['exp_sku_brand4'].str.lower().str.strip()
        df_master_cap['sku_brand'] = df_master_cap['sku_brand'].str.lower().str.strip()
        df_final5 = pd.merge(df_cap5, df_master_cap, how='left', left_on='exp_sku_brand4', right_on = 'sku_brand')
        
        m = int(df_final5[df_final5['brand_y'].notnull()].item_code.nunique())
        varn = 1;
        if df_cap.shape[0] != 0:
            varn =df_cap.shape[0]
        print('Exact Match on exp_sku_brand4 - ', m, ' - ', (round)(m*100/varn),'%', sep = '')
        
        df_cap6 = df_final5[df_final5['brand_y'].isnull()].reset_index(drop = True)
        df_cap6 = df_cap6[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_cap', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4']]
        df_cap6 = df_cap6.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Capsules -', df_cap6.shape[0])
        
        df_final5 = df_final5[df_final5['brand_y'].notnull()].reset_index(drop = True)
        df_final5['cosine'] = -1.0
    
        for i in range(len(df_final5)):
    
            text1 = str(df_final5['mod_brand'][i])
            text2 = str(df_final5['new_brand'][i])
    
            vector1 = text_to_vector(text1)
            vector2 = text_to_vector(text2)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final5['cosine'][i] = cosine
        
        df_final5.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        df_final5_grp = df_final5.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand4'], as_index = False).agg({'cosine':'max'}).rename(columns = {'cosine':'max_cosine'})
        df_final5 = pd.merge(df_final5, df_final5_grp, on=['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand4'], how='left')
        
        df_final5 = df_final5[(df_final5['cosine']>=0) & ( df_final5['cosine']>=(0.9*df_final5['max_cosine']) )].reset_index(drop = True)
        #df_final5 = df_final5.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand4', 'sku_brand'], as_index = False).agg({'brand_y':list, 'new_brand':list, 'cosine':list})
        
        """if(df_final5.shape[0]>0):
            df_final5['count'] = df_final5['brand_y'].str.len()"""
        
        df_final5.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        
        #=================== Match on full brand names using cosine similarity for sku_brand=='tbd' ===================#
        
        start_time = time.time()
        df_final6 = pd.merge(df_cap6, df_master_cap[df_master_cap['sku_brand']=='tbd'], on = 'medicine_type', how='left')
        print('\nFinal6 -', df_final6.shape[0])
    
        print("--------------------------------- Total Time Taken is %s seconds ---------------------------------" % (time.time() - start_time),end='\n\n')
        
        start_time = time.time()
        df_final6['cosine'] = -1.0
    
        for i in range(len(df_final6)):
    
                if(i%100000==0):
                    print(i)
    
                text1 = str(df_final6['mod_brand'][i])
                text2 = str(df_final6['new_brand'][i])
    
                vector1 = text_to_vector(text1)
                vector2 = text_to_vector(text2)
    
                cosine = get_cosine(vector1, vector2)
    
                df_final6['cosine'][i] = cosine
    
        print("--------------------------------- Total Time Taken is %s seconds ---------------------------------" % (time.time() - start_time),end='\n\n')
        
        df_final6.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        df_final6_grp = df_final6.groupby(['item_code', 'brand_x', 'mod_brand'], as_index = False).agg({'cosine':'max'}).rename(columns = {'cosine':'max_cosine'})
        df_final6 = pd.merge(df_final6, df_final6_grp, on=['item_code', 'brand_x', 'mod_brand'], how='left')
        
        df_final61 = df_final6.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand', 'sku_brand'], as_index = False).agg({'brand_y':list, 'new_brand':list, 'cosine':list})
        if(len(df_final61)>0):
            df_final61['count'] = df_final61['brand_y'].str.len()
        else:
            df_final61['count'] = -1
        #df_final61['count'] = df_final61['brand_y'].str.len()
        #df_final61.to_csv('final6_cap_all_cosines.csv', index = False)
        
        df_final6 = df_final6[(df_final6['cosine']>0.7) & ( df_final6['cosine']>=(0.9*df_final6['max_cosine']) )].reset_index(drop = True)
        df_final6 = df_final6.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand', 'sku_brand'], as_index = False).agg({'brand_y':list, 'new_brand':list, 'cosine':list})
        
        if(len(df_final6)>0):
            df_final6['count'] = df_final6['brand_y'].str.len()
        else:
            df_final6['count'] = -1
            
        #df_final6['count'] = df_final6['brand_y'].str.len()
        
        m = int(df_final6[df_final6['count']==1].item_code.nunique())
        varn = 1;
        if df_cap.shape[0] != 0:
            varn =df_cap.shape[0]
        print('Only 1 Match with cosine>0.7 - ', m, ' - ', (round)(m*100/varn),'%', sep = '')
        
        df_cap7 = pd.merge(df_cap6, df_final6[df_final6['count']==1], on='item_code', how='left')
        df_cap7 = df_cap7[df_cap7['brand_y'].isnull()].reset_index(drop = True)
        df_cap7 = df_cap7[['item_code', 'brand', 'Master Catalogue name', 'medicine_type',
                           'exp_sku_brand_x', 'mod_brand_x', 'count_cap', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4']]
        df_cap7 = df_cap7.rename(columns = {'exp_sku_brand_x':'exp_sku_brand', 'mod_brand_x':'mod_brand'})
        
        
        return df_final1, df_final2, df_final3, df_final4, df_final5, df_final6, df_cap6, df_cap7
        
    
    
    
    preprocess_capsules()
    df_final1, df_final2, df_final3, df_final4, df_final5, df_final6, df_cap6, df_cap7 = find_capsule()
    
    
    
    ########################## Flag for Quantity ##########################
    
    # df_final1['quantity_x']
    
    df_final2['quantity_x'] = df_final2['mod_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    df_final2['quantity_y'] = df_final2['new_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    
    df_final3['quantity_x'] = df_final3['mod_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    df_final3['quantity_y'] = df_final3['new_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    
    df_final4['quantity_x'] = df_final4['mod_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    df_final4['quantity_y'] = df_final4['new_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    
    df_final5['quantity_x'] = df_final5['mod_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    df_final5['quantity_y'] = df_final5['new_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    
    #df_final6['quantity_x'] = df_final6['mod_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    #df_final6['quantity_y'] = df_final6['new_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    
    df_final2['quantity_flag'] = np.where(df_final2['quantity_x']==df_final2['quantity_y'], 0, 1)
    df_final3['quantity_flag'] = np.where(df_final3['quantity_x']==df_final3['quantity_y'], 0, 1)
    df_final4['quantity_flag'] = np.where(df_final4['quantity_x']==df_final4['quantity_y'], 0, 1)
    df_final5['quantity_flag'] = np.where(df_final5['quantity_x']==df_final5['quantity_y'], 0, 1)
    
    #df_final2[['brand_x', 'mod_brand', 'quantity_x', 'brand_y', 'new_brand', 'quantity_y']].to_csv('check.csv', index = False)
    
    
    
    ########################## Salt Flag ##########################
    
    df_final1['salt_flag'] = 0
    df_final2['salt_flag'] = 0
    df_final3['salt_flag'] = 0
    df_final4['salt_flag'] = 0
    df_final5['salt_flag'] = 0
    #df_final6['salt_flag'] = 0
    
    #df_final2['salt_flag'] = np.where((df_final2['brand_x'].str.contains(' forte ')) & ~(df_final2['brand_y'].str.contains(' forte ')), 1, df_final2['salt_flag'])
    #df_final2['salt_flag'] = np.where((df_final2['brand_x'].str.contains(' plus ')) & ~(df_final2['brand_y'].str.contains(' plus ')), 1, df_final2['salt_flag'])
    
    check_list = ['plus', 'forte', 'a', 'af', 'am', 'as', 'bp',
                  'cd3', 'ch', 'cp', 'ct', 'cv', 'd', 'd', 'd3', 
                  'dp', 'ds', 'dsr', 'ez', 'f', 'f', 'g', 'h', 'h', 
                  'l', 'lc', 'lm', 'm', 'm', 'm', 'me', 'mf', 'oz', 
                  'p', 'pd', 'pz', 'r', 'sp', 't', 'v', 'z', 'mex', 
                  'lb', 'vg', 'dp', 'o', 'mv', 'sp', 'p']
    
    for s in check_list:
        
        s = ' ' + s + ' '
        
        df_final1['salt_flag'] = np.where((df_final1['brand_x'].str.contains(s)) & ~(df_final1['brand_y'].str.contains(s)), 1, df_final1['salt_flag'])
        #df_final1['salt_flag'] = np.where(~(df_final1['brand_x'].str.contains(s)) & (df_final1['brand_y'].str.contains(s)), 1, df_final1['salt_flag'])
    
        df_final2['salt_flag'] = np.where((df_final2['brand_x'].str.contains(s)) & ~(df_final2['brand_y'].str.contains(s)), 1, df_final2['salt_flag'])
        #df_final2['salt_flag'] = np.where(~(df_final2['brand_x'].str.contains(s)) & (df_final2['brand_y'].str.contains(s)), 1, df_final2['salt_flag'])
        
        df_final3['salt_flag'] = np.where((df_final3['brand_x'].str.contains(s)) & ~(df_final3['brand_y'].str.contains(s)), 1, df_final3['salt_flag'])
        #df_final3['salt_flag'] = np.where(~(df_final3['brand_x'].str.contains(s)) & (df_final3['brand_y'].str.contains(s)), 1, df_final3['salt_flag'])
        
        df_final4['salt_flag'] = np.where((df_final4['brand_x'].str.contains(s)) & ~(df_final4['brand_y'].str.contains(s)), 1, df_final4['salt_flag'])
        #df_final4['salt_flag'] = np.where(~(df_final4['brand_x'].str.contains(s)) & (df_final4['brand_y'].str.contains(s)), 1, df_final4['salt_flag'])
        
        df_final5['salt_flag'] = np.where((df_final5['brand_x'].str.contains(s)) & ~(df_final5['brand_y'].str.contains(s)), 1, df_final5['salt_flag'])
        #df_final5['salt_flag'] = np.where(~(df_final5['brand_x'].str.contains(s)) & (df_final5['brand_y'].str.contains(s)), 1, df_final5['salt_flag'])
        
        #df_final6['salt_flag'] = np.where((df_final6['brand_x'].str.contains(s)) & ~(df_final6['brand_y'].str.contains(s)), 1, df_final6['salt_flag'])
        #df_final6['salt_flag'] = np.where(~(df_final6['brand_x'].str.contains(s)) & (df_final6['brand_y'].str.contains(s)), 1, df_final6['salt_flag'])
    
    
    
    
    ########################## Unit of measurement flag ##########################
    
    df_final2 = pd.merge(df_final2, df_cap[['item_code', 'mod_brand2']], on='item_code', how='left')
    df_final3 = pd.merge(df_final3, df_cap[['item_code', 'mod_brand2']], on='item_code', how='left')
    df_final4 = pd.merge(df_final4, df_cap[['item_code', 'mod_brand2']], on='item_code', how='left')
    df_final5 = pd.merge(df_final5, df_cap[['item_code', 'mod_brand2']], on='item_code', how='left')
    
    df_final2['uom_flag'] = 0
    df_final3['uom_flag'] = 0
    df_final4['uom_flag'] = 0
    df_final5['uom_flag'] = 0
    #df_final6['uom_flag'] = 0
    
    df_final2['uom_x'] = ''
    df_final3['uom_x'] = ''
    df_final4['uom_x'] = ''
    df_final5['uom_x'] = ''
    
    df_final2['uom_y'] = ''
    df_final3['uom_y'] = ''
    df_final4['uom_y'] = ''
    df_final5['uom_y'] = ''
    
    uom_list = ['au', 'ccid50', 'cells', 'cfu', 'cst', 'cu', 'd-antigenunits',
                'ffu', 'gm', 'iu', 'lf', 'mcg', 'mg', 'ml', 'pfu', 'ppm', 
                'spores', 'gm/10ml', 'gm/100ml', 'gm/200ml', 'gm/2ml', 'gm/5ml', 
                'gm/ml', 'iu/gm', 'iu/ml', 'iu/5ml', 'mcg/2ml', 'mcg/3ml', 
                'mcg/gm', 'mcg/ml', 'mcg/puff', 'mg/100ml', 'mg/10ml', 'mg/15ml', 
                'mg/2.5ml', 'mg/2ml', 'mg/3ml', 'mg/4ml', 'mg/5ml', 'mg/ml', 
                'mg/gm', 'mgi/ml', 'ml/ml', 'ml/5ml', 'v/v', 'v/w', 'w/v', 'w/w']
    
    for uom in uom_list:
        
        uom = uom + ' '
        
        df_final2['uom_x'] = np.where(df_final2['mod_brand2'].str.find(uom)!=-1, uom.strip(), df_final2['uom_x'])
        df_final2['uom_y'] = np.where(df_final2['new_brand2'].str.find(uom)!=-1, uom.strip(), df_final2['uom_y'])
        
        df_final3['uom_x'] = np.where(df_final3['mod_brand2'].str.find(uom)!=-1, uom.strip(), df_final3['uom_x'])
        df_final3['uom_y'] = np.where(df_final3['new_brand2'].str.find(uom)!=-1, uom.strip(), df_final3['uom_y'])
        
        df_final4['uom_x'] = np.where(df_final4['mod_brand2'].str.find(uom)!=-1, uom.strip(), df_final4['uom_x'])
        df_final4['uom_y'] = np.where(df_final4['new_brand2'].str.find(uom)!=-1, uom.strip(), df_final4['uom_y'])
        
        df_final5['uom_x'] = np.where(df_final5['mod_brand2'].str.find(uom)!=-1, uom.strip(), df_final5['uom_x'])
        df_final5['uom_y'] = np.where(df_final5['new_brand2'].str.find(uom)!=-1, uom.strip(), df_final5['uom_y'])
        
    
    df_final2['uom_flag'] = np.where((df_final2['uom_x']==df_final2['uom_y']) & (df_final2['uom_x']!='') & (df_final2['uom_y']!=''), 0, df_final2['uom_flag'])
    df_final2['uom_flag'] = np.where((df_final2['uom_x']!=df_final2['uom_y']) & (df_final2['uom_x']!='') & (df_final2['uom_y']!=''), 1, df_final2['uom_flag'])
    df_final2['uom_flag'] = np.where((df_final2['uom_x']=='') | (df_final2['uom_y']==''), 2, df_final2['uom_flag'])
    
    df_final3['uom_flag'] = np.where((df_final3['uom_x']==df_final3['uom_y']) & (df_final3['uom_x']!='') & (df_final3['uom_y']!=''), 0, df_final3['uom_flag'])
    df_final3['uom_flag'] = np.where((df_final3['uom_x']!=df_final3['uom_y']) & (df_final3['uom_x']!='') & (df_final3['uom_y']!=''), 1, df_final3['uom_flag'])
    df_final3['uom_flag'] = np.where((df_final3['uom_x']=='') | (df_final3['uom_y']==''), 2, df_final3['uom_flag'])
    
    df_final4['uom_flag'] = np.where((df_final4['uom_x']==df_final4['uom_y']) & (df_final4['uom_x']!='') & (df_final4['uom_y']!=''), 0, df_final4['uom_flag'])
    df_final4['uom_flag'] = np.where((df_final4['uom_x']!=df_final4['uom_y']) & (df_final4['uom_x']!='') & (df_final4['uom_y']!=''), 1, df_final4['uom_flag'])
    df_final4['uom_flag'] = np.where((df_final4['uom_x']=='') | (df_final4['uom_y']==''), 2, df_final4['uom_flag'])
    
    df_final5['uom_flag'] = np.where((df_final5['uom_x']==df_final5['uom_y']) & (df_final5['uom_x']!='') & (df_final5['uom_y']!=''), 0, df_final5['uom_flag'])
    df_final5['uom_flag'] = np.where((df_final5['uom_x']!=df_final5['uom_y']) & (df_final5['uom_x']!='') & (df_final5['uom_y']!=''), 1, df_final5['uom_flag'])
    df_final5['uom_flag'] = np.where((df_final5['uom_x']=='') | (df_final5['uom_y']==''), 2, df_final5['uom_flag'])
    
    
    ########################## Flag on strengths - mg/mcg/gm ##########################
    
    df_final2['strength_flag'] = 0
    df_final3['strength_flag'] = 0
    df_final4['strength_flag'] = 0
    df_final5['strength_flag'] = 0
    
    df_final2['strength_x'] = ''
    df_final3['strength_x'] = ''
    df_final4['strength_x'] = ''
    df_final5['strength_x'] = ''
    
    df_final2['strength_y'] = ''
    df_final3['strength_y'] = ''
    df_final4['strength_y'] = ''
    df_final5['strength_y'] = ''
    
    # Final 2
    
    df_final2['strength_x'] = np.where(df_final2['mod_brand2'].str.contains('mg'), df_final2['mod_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final2['strength_x'])
    df_final2['strength_y'] = np.where(df_final2['new_brand2'].str.contains('mg'), df_final2['new_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final2['strength_y'])
    
    df_final2['strength_x'] = np.where(df_final2['mod_brand2'].str.contains('mcg'), df_final2['mod_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final2['strength_x'])
    df_final2['strength_y'] = np.where(df_final2['new_brand2'].str.contains('mcg'), df_final2['new_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final2['strength_y'])
    
    df_final2['strength_x'] = np.where(df_final2['mod_brand2'].str.contains('gm'), df_final2['mod_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final2['strength_x'])
    df_final2['strength_y'] = np.where(df_final2['new_brand2'].str.contains('gm'), df_final2['new_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final2['strength_y'])
    
    # Final 3
    
    df_final3['strength_x'] = np.where(df_final3['mod_brand2'].str.contains('mg'), df_final3['mod_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final3['strength_x'])
    df_final3['strength_y'] = np.where(df_final3['new_brand2'].str.contains('mg'), df_final3['new_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final3['strength_y'])
    
    df_final3['strength_x'] = np.where(df_final3['mod_brand2'].str.contains('mcg'), df_final3['mod_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final3['strength_x'])
    df_final3['strength_y'] = np.where(df_final3['new_brand2'].str.contains('mcg'), df_final3['new_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final3['strength_y'])
    
    df_final3['strength_x'] = np.where(df_final3['mod_brand2'].str.contains('gm'), df_final3['mod_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final3['strength_x'])
    df_final3['strength_y'] = np.where(df_final3['new_brand2'].str.contains('gm'), df_final3['new_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final3['strength_y'])
    
    # Final 4
    
    df_final4['strength_x'] = np.where(df_final4['mod_brand2'].str.contains('mg'), df_final4['mod_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final4['strength_x'])
    df_final4['strength_y'] = np.where(df_final4['new_brand2'].str.contains('mg'), df_final4['new_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final4['strength_y'])
    
    df_final4['strength_x'] = np.where(df_final4['mod_brand2'].str.contains('mcg'), df_final4['mod_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final4['strength_x'])
    df_final4['strength_y'] = np.where(df_final4['new_brand2'].str.contains('mcg'), df_final4['new_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final4['strength_y'])
    
    df_final4['strength_x'] = np.where(df_final4['mod_brand2'].str.contains('gm'), df_final4['mod_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final4['strength_x'])
    df_final4['strength_y'] = np.where(df_final4['new_brand2'].str.contains('gm'), df_final4['new_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final4['strength_y'])
    
    # Final 5
    
    df_final5['strength_x'] = np.where(df_final5['mod_brand2'].str.contains('mg'), df_final5['mod_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final5['strength_x'])
    df_final5['strength_y'] = np.where(df_final5['new_brand2'].str.contains('mg'), df_final5['new_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final5['strength_y'])
    
    df_final5['strength_x'] = np.where(df_final5['mod_brand2'].str.contains('mcg'), df_final5['mod_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final5['strength_x'])
    df_final5['strength_y'] = np.where(df_final5['new_brand2'].str.contains('mcg'), df_final5['new_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final5['strength_y'])
    
    df_final5['strength_x'] = np.where(df_final5['mod_brand2'].str.contains('gm'), df_final5['mod_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final5['strength_x'])
    df_final5['strength_y'] = np.where(df_final5['new_brand2'].str.contains('gm'), df_final5['new_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final5['strength_y'])
    
    # Flag
    
    df_final2['strength_x'] = df_final2['strength_x'].str.strip()
    df_final2['strength_y'] = df_final2['strength_y'].str.strip()
    df_final2['strength_flag'] = np.where( (df_final2['strength_x']==df_final2['strength_y']) & (df_final2['strength_x']!='') & (df_final2['strength_y']!=''), 0, df_final2['strength_flag'])
    df_final2['strength_flag'] = np.where( (df_final2['strength_x']!=df_final2['strength_y']) & (df_final2['strength_x']!='') & (df_final2['strength_y']!=''), 1, df_final2['strength_flag'])
    df_final2['strength_flag'] = np.where( (df_final2['strength_x']=='') | (df_final2['strength_y']==''), 2, df_final2['strength_flag'])
    
    df_final3['strength_x'] = df_final3['strength_x'].str.strip()
    df_final3['strength_y'] = df_final3['strength_y'].str.strip()
    df_final3['strength_flag'] = np.where( (df_final3['strength_x']==df_final3['strength_y']) & (df_final3['strength_x']!='') & (df_final3['strength_y']!=''), 0, df_final3['strength_flag'])
    df_final3['strength_flag'] = np.where( (df_final3['strength_x']!=df_final3['strength_y']) & (df_final3['strength_x']!='') & (df_final3['strength_y']!=''), 1, df_final3['strength_flag'])
    df_final3['strength_flag'] = np.where( (df_final3['strength_x']=='') | (df_final3['strength_y']==''), 2, df_final3['strength_flag'])
    
    df_final4['strength_x'] = df_final4['strength_x'].str.strip()
    df_final4['strength_y'] = df_final4['strength_y'].str.strip()
    df_final4['strength_flag'] = np.where( (df_final4['strength_x']==df_final4['strength_y']) & (df_final4['strength_x']!='') & (df_final4['strength_y']!=''), 0, df_final4['strength_flag'])
    df_final4['strength_flag'] = np.where( (df_final4['strength_x']!=df_final4['strength_y']) & (df_final4['strength_x']!='') & (df_final4['strength_y']!=''), 1, df_final4['strength_flag'])
    df_final4['strength_flag'] = np.where( (df_final4['strength_x']=='') | (df_final4['strength_y']==''), 2, df_final4['strength_flag'])
    
    df_final5['strength_x'] = df_final5['strength_x'].str.strip()
    df_final5['strength_y'] = df_final5['strength_y'].str.strip()
    df_final5['strength_flag'] = np.where( (df_final5['strength_x']==df_final5['strength_y']) & (df_final5['strength_x']!='') & (df_final5['strength_y']!=''), 0, df_final5['strength_flag'])
    df_final5['strength_flag'] = np.where( (df_final5['strength_x']!=df_final5['strength_y']) & (df_final5['strength_x']!='') & (df_final5['strength_y']!=''), 1, df_final5['strength_flag'])
    df_final5['strength_flag'] = np.where( (df_final5['strength_x']=='') | (df_final5['strength_y']==''), 2, df_final5['strength_flag'])
    
    
    
    df_final2 = pd.merge(df_final2, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_final3 = pd.merge(df_final3, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_final4 = pd.merge(df_final4, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_final5 = pd.merge(df_final5, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_final6 = pd.merge(df_final6, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_cap6 = pd.merge(df_cap6, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_cap7 = pd.merge(df_cap7, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    
    
    
    ##################### Appending all output files and merging into one #####################
    
    df_final_n = df_final2.append([df_final3, df_final4, df_final5, df_final6], ignore_index = True)
    df_final = df_final_n.append([df_final1], ignore_index = True)
    del df_final['count']
    
    df_final_grp = df_final.groupby(['item_code'], as_index = False).agg({'brand_y':'count'}).rename(columns = {'brand_y':'count'})
    df_final_grp['count'].value_counts()
    
    df_final = pd.merge(df_final, df_final_grp, on='item_code', how='left')
    df_final['mod_cosine'] = df_final['cosine'] 
    df_final.to_csv('final_cap.csv', index = False)
    
    df_cap6.to_csv('remaining_cap.csv', index = False)
    
    
    
    # Single Match 
    
    print('Total -', df_final.shape[0])
    
    print('Single Match -', df_final[df_final['count']==1].shape[0])
    df_final[df_final['count']==1].to_csv('single_match_cap.csv', index = False)
    
    # Multiple Matches
    
    print('Multiple Match -', df_final[df_final['count']>1].shape[0])
    df_final[df_final['count']>1].to_csv('multiple_match_cap.csv', index = False)
    
    
    
    ##################################################################################
    ########################## Preprocess and match tablets ##########################
    ##################################################################################
    
    
    df_master_tab = df_master[df_master['medicine_type']=='tablet'].reset_index(drop = True)
    df_tab = df_distributor[df_distributor['medicine_type']=='tablet'].reset_index(drop = True)
    
    def preprocess_tablets():
        
        df_tab['mod_brand'] = df_tab['mod_brand'].str.lower()
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.lower()
        
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace('-', ' ')
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace('^', ' ')
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace('1g ', '1000mg ')
        
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace(' mg', 'mg')
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace(' ml', 'ml')
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace(' mcg', 'mcg')
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace(' gm', 'gm')
        
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace('1gm', '1000mg')
        
        df_master_tab['new_brand2'] = df_master_tab['new_brand']
        df_master_tab['new_brand2'] = df_master_tab['new_brand2'].str.replace('  ', ' ')
        df_master_tab['new_brand2'] = df_master_tab['new_brand2'].str.strip()
        
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace('mg', '')
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace('ml', '')
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace('mcg', '')
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace('gm', '')
        
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace('tabs', ' tablet')
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace('tablets', 'tablet')
        
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace(' tablet ', ' ')
        
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.replace('  ', ' ')
        df_master_tab['new_brand'] = df_master_tab['new_brand'].str.strip()
        
        df_tab['exp_sku_brand'] = ""
        #df_tab['mod_brand'] = ""
        df_tab['count_tab'] = 0
    
        for n in range(df_tab.shape[0]):
    
            text1 = str(df_tab['mod_brand'][n])
            #print(text1)
            
            text1 = text1.replace('-', ' ')
            text1 = text1.replace('^', '')
            text1 = text1.replace('`', '')
            text1 = text1.replace("'", "") #*
            text1 = text1.replace("*", "")
            text1 = text1.replace(' mg', 'mg')
            text1 = text1.replace(' mcg', 'mcg')
            text1 = text1.replace(' gm', 'gm')
            
            text1 = text1.replace('mg.', 'mg')
            text1 = text1.replace('ml.', 'ml')
            text1 = text1.replace('gm.', 'gm')
            text1 = text1.replace('mcg.', 'mcg')
            
            text1 = text1.replace('1gm', '1000mg')
            
            text1 = text1.replace("x1", "s")
            
            #text1 = text1.replace('tab', ' tab')
            text1 = text1.replace('tab', ' tab')
            text1 = text1.replace('  ', ' ')
    
            text1 = text1.replace(' tab', ' tablet')
            text1 = text1.replace(' tab.', ' tablet')
            text1 = text1.replace(' tablets', ' tablet')
            text1 = text1.replace(' tabs', ' tablet')
            text1 = text1.replace(' tabletlets', ' tablet')
            text1 = text1.replace(' tabletlet', ' tablet')
            text1 = text1.replace(' tablet.', ' tablet')
            
            text1 = text1.replace('  ', ' ')
            
            # Handle cases like sulpitac od 200 10 tablet, cifran oz tablet 10 tablet
    
            df_tab['count_tab'][n] = text1.count('tablet')
    
            if(df_tab['count_tab'][n]==2):
                text1 = 's'.join(text1.rsplit(' tablet', 1))
    
            #print(text1)
    
            df_tab['mod_brand'][n] = text1.strip()
    
            tl1 = text1.split(' ')
            #print(tl1)
            text11 = ""
    
            for i in range(len(tl1)):
    
                #print(i,'/',len(tl1), end = ', ')
    
                if( ( ('mg' in tl1[i]) & (i>0) ) | ('tablet' in tl1[i]) ):
                    break
    
                if( (i+1) < len(tl1) ):
    
                    if( (tl1[i].isnumeric()) & ('tablet' in tl1[i+1]) ):
                        break
    
                text11 = text11 + " " + tl1[i]
    
            #print(text11)
            df_tab['exp_sku_brand'][n] = text11.strip()
        
        df_tab['mod_brand2'] = df_tab['mod_brand']
        df_tab['mod_brand2'] = df_tab['mod_brand2'].str.strip()
        
        df_tab['mod_brand'] = df_tab['mod_brand'] + ' '
        
        df_tab['mod_brand'] = df_tab['mod_brand'].str.replace('mg', '')
        df_tab['mod_brand'] = df_tab['mod_brand'].str.replace('ml', '')
        df_tab['mod_brand'] = df_tab['mod_brand'].str.replace('mcg', '')
        df_tab['mod_brand'] = df_tab['mod_brand'].str.replace('gm', '')
        df_tab['mod_brand'] = df_tab['mod_brand'].str.replace(' tablet ', ' ')
        df_tab['mod_brand'] = df_tab['mod_brand'].str.replace('  ', ' ')
        df_tab['mod_brand'] = df_tab['mod_brand'].str.strip()
        
        df_tab['exp_sku_brand'] = df_tab['exp_sku_brand'].str.lower().str.strip()
        
        df_tab['exp_sku_brand2'] = df_tab['exp_sku_brand'].str.rsplit(' ', 1).str[0]
        df_tab['exp_sku_brand3'] = df_tab['exp_sku_brand2'].str.rsplit(' ', 1).str[0]
        df_tab['exp_sku_brand4'] = df_tab['exp_sku_brand3'].str.rsplit(' ', 1).str[0]
        
        df_master_tab['sku_brand'] = df_master_tab['sku_brand'].str.lower()
        df_master_tab['sku_brand'] = df_master_tab['sku_brand'].str.replace('-', ' ')
        df_master_tab['sku_brand'] = df_master_tab['sku_brand'].str.replace("'", ' ')
        df_master_tab['sku_brand'] = df_master_tab['sku_brand'].str.replace("  ", ' ')
        df_master_tab['sku_brand'] = df_master_tab['sku_brand'].str.replace('`', '')
        
        df_master_tab['sku_brand'] = df_master_tab['sku_brand'].str.split(' ').str[0]
        df_master_tab['sku_brand'] = df_master_tab['sku_brand'].str.lower().str.strip()
        
        df_tab['mod_brand'] = (df_tab['mod_brand'].str.split()
                                  .apply(lambda x: OrderedDict.fromkeys(x).keys())
                                  .str.join(' '))
        df_master_tab['new_brand'] = (df_master_tab['new_brand'].str.split()
                                  .apply(lambda x: OrderedDict.fromkeys(x).keys())
                                  .str.join(' '))
        
        
    
    def find_tablet():
        
        print('Total Tablets in Distributer Data -', df_tab.shape[0])
        
        #=================== Exact Match on full brand names ===================#
        
        df_final1 = pd.merge(df_tab, df_master_tab, how='left', left_on='mod_brand', right_on = 'new_brand')
        df_final1 = df_final1.drop_duplicates(subset=['item_code', 'brand_x'], keep='last', ignore_index = True)
        m = df_final1[df_final1['brand_y'].notnull()].shape[0]
        varn = 1
        if df_tab.shape[0] != 0:
            varn =df_tab.shape[0]
        print('Exact Match on full brand names - ', m, ' - ', (round)(m*100/varn),'%', sep = '')
        
        df_tab2 = df_final1[df_final1['brand_y'].isnull()].reset_index(drop = True)
        df_tab2 = df_tab2[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_tab', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4']]
        df_tab2 = df_tab2.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Tablets -', df_tab2.shape[0])
        
        df_final1 = df_final1[df_final1['brand_y'].notnull()].reset_index(drop = True)
        
        #=================== Match on sku_brand and exp_sku_brand ===================#
        
        df_tab2['exp_sku_brand'] = df_tab2['exp_sku_brand'].str.lower().str.strip()
        df_master_tab['sku_brand'] = df_master_tab['sku_brand'].str.lower().str.strip()
        df_final2 = pd.merge(df_tab2, df_master_tab, how='left', left_on='exp_sku_brand', right_on = 'sku_brand')
        
        m = int(df_final2[df_final2['brand_y'].notnull()].item_code.nunique())
        varn = 1
        if df_tab.shape[0] != 0:
            varn =df_tab.shape[0]
        print('Exact Match on exp_sku_brand - ', m, ' - ', (round)(m*100/varn),'%', sep = '')
        
        df_tab3 = df_final2[df_final2['brand_y'].isnull()].reset_index(drop = True)
        df_tab3 = df_tab3[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_tab', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4']]
        df_tab3 = df_tab3.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Tablets -', df_tab3.shape[0])
        
        df_final2 = df_final2[df_final2['brand_y'].notnull()].reset_index(drop = True)
        df_final2['cosine'] = -1.0
    
        for i in range(len(df_final2)):
    
            text1 = str(df_final2['mod_brand'][i])
            text2 = str(df_final2['new_brand'][i])
    
            vector1 = text_to_vector(text1)
            vector2 = text_to_vector(text2)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final2['cosine'][i] = cosine
        
        df_final2.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        df_final2_grp = df_final2.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand'], as_index = False).agg({'cosine':'max'}).rename(columns = {'cosine':'max_cosine'})
        df_final2 = pd.merge(df_final2, df_final2_grp, on=['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand'], how='left')
        
        df_final2 = df_final2[(df_final2['cosine']>0) & ( df_final2['cosine']>=(0.9*df_final2['max_cosine']) )].reset_index(drop = True)
        #df_final2 = df_final2.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand', 'sku_brand'], as_index = False).agg({'brand_y':list, 'new_brand':list, 'cosine':list})
        #df_final2['count'] = df_final2['brand_y'].str.len()
        df_final2.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        #=================== Match on sku_brand and exp_sku_brand2 ===================#
        
        df_tab3['exp_sku_brand2'] = df_tab3['exp_sku_brand2'].str.lower().str.strip()
        df_master_tab['sku_brand'] = df_master_tab['sku_brand'].str.lower().str.strip()
        df_final3 = pd.merge(df_tab3, df_master_tab, how='left', left_on='exp_sku_brand2', right_on = 'sku_brand')
        
        m = int(df_final3[df_final3['brand_y'].notnull()].item_code.nunique())
        varn = 1
        if df_tab.shape[0] != 0:
            varn =df_tab.shape[0]
        print('Exact Match on exp_sku_brand2 - ', m, ' - ', (round)(m*100/varn),'%', sep = '')
        
        df_tab4 = df_final3[df_final3['brand_y'].isnull()].reset_index(drop = True)
        df_tab4 = df_tab4[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_tab', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4']]
        df_tab4 = df_tab4.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Tablets -', df_tab4.shape[0])
        
        df_final3 = df_final3[df_final3['brand_y'].notnull()].reset_index(drop = True)
        df_final3['cosine'] = -1.0
    
        for i in range(len(df_final3)):
    
            text1 = str(df_final3['mod_brand'][i])
            text2 = str(df_final3['new_brand'][i])
    
            vector1 = text_to_vector(text1)
            vector2 = text_to_vector(text2)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final3['cosine'][i] = cosine
        
        df_final3.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        df_final3_grp = df_final3.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand2'], as_index = False).agg({'cosine':'max'}).rename(columns = {'cosine':'max_cosine'})
        df_final3 = pd.merge(df_final3, df_final3_grp, on=['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand2'], how='left')
        
        df_final3 = df_final3[(df_final3['cosine']>0) & ( df_final3['cosine']>=(0.9*df_final3['max_cosine']) )].reset_index(drop = True)
        #df_final3 = df_final3.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand2', 'sku_brand'], as_index = False).agg({'brand_y':list, 'new_brand':list, 'cosine':list})
        #df_final3['count'] = df_final3['brand_y'].str.len()
        df_final3.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        #=================== Match on sku_brand and exp_sku_brand3 ===================#
        
        df_tab4['exp_sku_brand3'] = df_tab4['exp_sku_brand3'].str.lower().str.strip()
        df_master_tab['sku_brand'] = df_master_tab['sku_brand'].str.lower().str.strip()
        df_final4 = pd.merge(df_tab4, df_master_tab, how='left', left_on='exp_sku_brand3', right_on = 'sku_brand')
        
        m = int(df_final4[df_final4['brand_y'].notnull()].item_code.nunique())
        varn = 1
        if df_tab.shape[0] != 0:
            varn =df_tab.shape[0]
        print('Exact Match on exp_sku_brand3 - ', m, ' - ', (round)(m*100/varn),'%', sep = '')
        
        df_tab5 = df_final4[df_final4['brand_y'].isnull()].reset_index(drop = True)
        df_tab5 = df_tab5[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_tab', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4']]
        df_tab5 = df_tab5.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Tablets -', df_tab5.shape[0])
        
        df_final4 = df_final4[df_final4['brand_y'].notnull()].reset_index(drop = True)
        df_final4['cosine'] = -1.0
    
        for i in range(len(df_final4)):
    
            text1 = str(df_final4['mod_brand'][i])
            text2 = str(df_final4['new_brand'][i])
    
            vector1 = text_to_vector(text1)
            vector2 = text_to_vector(text2)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final4['cosine'][i] = cosine
        
        df_final4.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        df_final4_grp = df_final4.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand3'], as_index = False).agg({'cosine':'max'}).rename(columns = {'cosine':'max_cosine'})
        df_final4 = pd.merge(df_final4, df_final4_grp, on=['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand3'], how='left')
        
        df_final4 = df_final4[(df_final4['cosine']>0) & ( df_final4['cosine']>=(0.9*df_final4['max_cosine']) )].reset_index(drop = True)
        #df_final4 = df_final4.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand3', 'sku_brand'], as_index = False).agg({'brand_y':list, 'new_brand':list, 'cosine':list})
        #df_final4['count'] = df_final4['brand_y'].str.len()
        df_final4.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        #=================== Match on sku_brand and exp_sku_brand4 ===================#
        
        df_tab5['exp_sku_brand4'] = df_tab5['exp_sku_brand4'].str.lower().str.strip()
        df_master_tab['sku_brand'] = df_master_tab['sku_brand'].str.lower().str.strip()
        df_final5 = pd.merge(df_tab5, df_master_tab, how='left', left_on='exp_sku_brand4', right_on = 'sku_brand')
        
        m = int(df_final5[df_final5['brand_y'].notnull()].item_code.nunique())
        varn = 1
        if df_tab.shape[0] != 0:
            varn =df_tab.shape[0]
        print('Exact Match on exp_sku_brand4 - ', m, ' - ', (round)(m*100/varn),'%', sep = '')
        
        df_tab6 = df_final5[df_final5['brand_y'].isnull()].reset_index(drop = True)
        df_tab6 = df_tab6[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_tab', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4']]
        df_tab6 = df_tab6.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Tablets -', df_tab6.shape[0])
        
        df_final5 = df_final5[df_final5['brand_y'].notnull()].reset_index(drop = True)
        df_final5['cosine'] = -1.0
    
        for i in range(len(df_final5)):
    
            text1 = str(df_final5['mod_brand'][i])
            text2 = str(df_final5['new_brand'][i])
    
            vector1 = text_to_vector(text1)
            vector2 = text_to_vector(text2)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final5['cosine'][i] = cosine
        
        df_final5.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        df_final5_grp = df_final5.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand4'], as_index = False).agg({'cosine':'max'}).rename(columns = {'cosine':'max_cosine'})
        df_final5 = pd.merge(df_final5, df_final5_grp, on=['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand4'], how='left')
        
        df_final5 = df_final5[(df_final5['cosine']>0) & ( df_final5['cosine']>=(0.9*df_final5['max_cosine']) )].reset_index(drop = True)
        #df_final5 = df_final5.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand4', 'sku_brand'], as_index = False).agg({'brand_y':list, 'new_brand':list, 'cosine':list})
        #df_final5['count'] = df_final5['brand_y'].str.len()
        df_final5.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        #=================== Match on full brand names using cosine similarity for sku_brand=='tbd' ===================#
        
        start_time = time.time()
        df_final6 = pd.merge(df_tab6, df_master_tab[df_master_tab['sku_brand']=='tbd'], on = 'medicine_type', how='left')
        print('\nFinal6 -', df_final6.shape[0])
    
        print("--------------------------------- Total Time Taken is %s seconds ---------------------------------" % (time.time() - start_time),end='\n\n')
        
        start_time = time.time()
        df_final6['cosine'] = -1.0
    
        for i in range(len(df_final6)):
    
                if(i%100000==0):
                    print(i)
    
                text1 = str(df_final6['mod_brand'][i])
                text2 = str(df_final6['new_brand'][i])
    
                vector1 = text_to_vector(text1)
                vector2 = text_to_vector(text2)
    
                cosine = get_cosine(vector1, vector2)
    
                df_final6['cosine'][i] = cosine
    
        print("--------------------------------- Total Time Taken is %s seconds ---------------------------------" % (time.time() - start_time),end='\n\n')
        
        df_final6.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        df_final6_grp = df_final6.groupby(['item_code', 'brand_x', 'mod_brand'], as_index = False).agg({'cosine':'max'}).rename(columns = {'cosine':'max_cosine'})
        df_final6 = pd.merge(df_final6, df_final6_grp, on=['item_code', 'brand_x', 'mod_brand'], how='left')
        
        df_final61 = df_final6.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand', 'sku_brand'], as_index = False).agg({'brand_y':list, 'new_brand':list, 'cosine':list})
        if(len(df_final61)>0):
            df_final61['count'] = df_final61['brand_y'].str.len()
        else:
            df_final61['count'] = -1
        #df_final61['count'] = df_final61['brand_y'].str.len()
        #df_final61.to_csv('final6_tab_all_cosines.csv', index = False)
        
        df_final6 = df_final6[(df_final6['cosine']>0.7) & ( df_final6['cosine']>=(0.9*df_final6['max_cosine']) )].reset_index(drop = True)
        df_final6 = df_final6.groupby(['item_code', 'brand_x', 'mod_brand', 'exp_sku_brand', 'sku_brand'], as_index = False).agg({'brand_y':list, 'new_brand':list, 'cosine':list})
        
        if(len(df_final6)>0):
            df_final6['count'] = df_final6['brand_y'].str.len()
        else:
            df_final6['count'] = -1
        
        #df_final6['count'] = df_final6['brand_y'].str.len()
        
        m = int(df_final6[df_final6['count']==1].item_code.nunique())
        varn = 1
        if df_tab.shape[0] != 0:
            varn =df_tab.shape[0]
        print('Sku_brand = tbd - One Match - ', m, ' - ', (round)(m*100/varn),'%', sep = '')
        
        #df_tab7 = df_final6[df_final6['count']>1].reset_index(drop = True)
        
        df_tab7 = pd.merge(df_tab6, df_final6[df_final6['count']==1], on='item_code', how='left')
        df_tab7 = df_tab7[df_tab7['brand_y'].isnull()].reset_index(drop = True)
        df_tab7 = df_tab7[['item_code', 'brand', 'Master Catalogue name', 'medicine_type',
                           'exp_sku_brand_x', 'mod_brand_x', 'count_tab', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4']]
        df_tab7 = df_tab7.rename(columns = {'exp_sku_brand_x':'exp_sku_brand', 'mod_brand_x':'mod_brand'})
        
        
        return df_final1, df_final2, df_final3, df_final4, df_final5, df_final6, df_tab6, df_tab7
    
    
    
    preprocess_tablets()
    df_final1, df_final2, df_final3, df_final4, df_final5, df_final6, df_tab6, df_tab7 = find_tablet()
    
    
    
    ########################## Flag for Quantity ##########################
    
    #df_final1['quantity_x']
    
    df_final2['quantity_x'] = df_final2['mod_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    df_final2['quantity_y'] = df_final2['new_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    
    df_final3['quantity_x'] = df_final3['mod_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    df_final3['quantity_y'] = df_final3['new_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    
    df_final4['quantity_x'] = df_final4['mod_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    df_final4['quantity_y'] = df_final4['new_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    
    df_final5['quantity_x'] = df_final5['mod_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    df_final5['quantity_y'] = df_final5['new_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    
    #df_final6['quantity_x'] = df_final6['mod_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    #df_final6['quantity_y'] = df_final6['new_brand'].str.rsplit('s').str[-2].str.split(' ').str[-1]
    
    df_final2['quantity_flag'] = np.where(df_final2['quantity_x']==df_final2['quantity_y'], 0, 1)
    df_final3['quantity_flag'] = np.where(df_final3['quantity_x']==df_final3['quantity_y'], 0, 1)
    df_final4['quantity_flag'] = np.where(df_final4['quantity_x']==df_final4['quantity_y'], 0, 1)
    df_final5['quantity_flag'] = np.where(df_final5['quantity_x']==df_final5['quantity_y'], 0, 1)
    
    #df_final2[['brand_x', 'mod_brand', 'quantity_x', 'brand_y', 'new_brand', 'quantity_y']].to_csv('check.csv', index = False)
    
    
    
    
    ########################## Unit of measurement flag ##########################
    
    df_final2 = pd.merge(df_final2, df_tab[['item_code', 'mod_brand2']], on='item_code', how='left')
    df_final3 = pd.merge(df_final3, df_tab[['item_code', 'mod_brand2']], on='item_code', how='left')
    df_final4 = pd.merge(df_final4, df_tab[['item_code', 'mod_brand2']], on='item_code', how='left')
    df_final5 = pd.merge(df_final5, df_tab[['item_code', 'mod_brand2']], on='item_code', how='left')
    
    df_final2['uom_flag'] = 0
    df_final3['uom_flag'] = 0
    df_final4['uom_flag'] = 0
    df_final5['uom_flag'] = 0
    #df_final6['uom_flag'] = 0
    
    df_final2['uom_x'] = ''
    df_final3['uom_x'] = ''
    df_final4['uom_x'] = ''
    df_final5['uom_x'] = ''
    
    df_final2['uom_y'] = ''
    df_final3['uom_y'] = ''
    df_final4['uom_y'] = ''
    df_final5['uom_y'] = ''
    
    uom_list = ['au', 'ccid50', 'cells', 'cfu', 'cst', 'cu', 'd-antigenunits',
                'ffu', 'gm', 'iu', 'lf', 'mcg', 'mg', 'ml', 'pfu', 'ppm', 
                'spores', 'gm/10ml', 'gm/100ml', 'gm/200ml', 'gm/2ml', 'gm/5ml', 
                'gm/ml', 'iu/gm', 'iu/ml', 'iu/5ml', 'mcg/2ml', 'mcg/3ml', 
                'mcg/gm', 'mcg/ml', 'mcg/puff', 'mg/100ml', 'mg/10ml', 'mg/15ml', 
                'mg/2.5ml', 'mg/2ml', 'mg/3ml', 'mg/4ml', 'mg/5ml', 'mg/ml', 
                'mg/gm', 'mgi/ml', 'ml/ml', 'ml/5ml', 'v/v', 'v/w', 'w/v', 'w/w']
    
    for uom in uom_list:
        
        uom = uom + ' '
        
        df_final2['uom_x'] = np.where(df_final2['mod_brand2'].str.find(uom)!=-1, uom, df_final2['uom_x'])
        df_final2['uom_y'] = np.where(df_final2['new_brand2'].str.find(uom)!=-1, uom, df_final2['uom_y'])
        
        df_final3['uom_x'] = np.where(df_final3['mod_brand2'].str.find(uom)!=-1, uom, df_final3['uom_x'])
        df_final3['uom_y'] = np.where(df_final3['new_brand2'].str.find(uom)!=-1, uom, df_final3['uom_y'])
        
        df_final4['uom_x'] = np.where(df_final4['mod_brand2'].str.find(uom)!=-1, uom, df_final4['uom_x'])
        df_final4['uom_y'] = np.where(df_final4['new_brand2'].str.find(uom)!=-1, uom, df_final4['uom_y'])
        
        df_final5['uom_x'] = np.where(df_final5['mod_brand2'].str.find(uom)!=-1, uom, df_final5['uom_x'])
        df_final5['uom_y'] = np.where(df_final5['new_brand2'].str.find(uom)!=-1, uom, df_final5['uom_y'])
    
    df_final2['uom_flag'] = np.where((df_final2['uom_x']==df_final2['uom_y']) & (df_final2['uom_x']!='') & (df_final2['uom_y']!=''), 0, df_final2['uom_flag'])
    df_final2['uom_flag'] = np.where((df_final2['uom_x']!=df_final2['uom_y']) & (df_final2['uom_x']!='') & (df_final2['uom_y']!=''), 1, df_final2['uom_flag'])
    df_final2['uom_flag'] = np.where((df_final2['uom_x']=='') | (df_final2['uom_y']==''), 2, df_final2['uom_flag'])
    
    df_final3['uom_flag'] = np.where((df_final3['uom_x']==df_final3['uom_y']) & (df_final3['uom_x']!='') & (df_final3['uom_y']!=''), 0, df_final3['uom_flag'])
    df_final3['uom_flag'] = np.where((df_final3['uom_x']!=df_final3['uom_y']) & (df_final3['uom_x']!='') & (df_final3['uom_y']!=''), 1, df_final3['uom_flag'])
    df_final3['uom_flag'] = np.where((df_final3['uom_x']=='') | (df_final3['uom_y']==''), 2, df_final3['uom_flag'])
    
    df_final4['uom_flag'] = np.where((df_final4['uom_x']==df_final4['uom_y']) & (df_final4['uom_x']!='') & (df_final4['uom_y']!=''), 0, df_final4['uom_flag'])
    df_final4['uom_flag'] = np.where((df_final4['uom_x']!=df_final4['uom_y']) & (df_final4['uom_x']!='') & (df_final4['uom_y']!=''), 1, df_final4['uom_flag'])
    df_final4['uom_flag'] = np.where((df_final4['uom_x']=='') | (df_final4['uom_y']==''), 2, df_final4['uom_flag'])
    
    df_final5['uom_flag'] = np.where((df_final5['uom_x']==df_final5['uom_y']) & (df_final5['uom_x']!='') & (df_final5['uom_y']!=''), 0, df_final5['uom_flag'])
    df_final5['uom_flag'] = np.where((df_final5['uom_x']!=df_final5['uom_y']) & (df_final5['uom_x']!='') & (df_final5['uom_y']!=''), 1, df_final5['uom_flag'])
    df_final5['uom_flag'] = np.where((df_final5['uom_x']=='') | (df_final5['uom_y']==''), 2, df_final5['uom_flag'])
    
    
    
    ########################## Flag on strengths - mg/mcg/gm ##########################
    
    df_final2['strength_flag'] = 0
    df_final3['strength_flag'] = 0
    df_final4['strength_flag'] = 0
    df_final5['strength_flag'] = 0
    
    df_final2['strength_x'] = ''
    df_final3['strength_x'] = ''
    df_final4['strength_x'] = ''
    df_final5['strength_x'] = ''
    
    df_final2['strength_y'] = ''
    df_final3['strength_y'] = ''
    df_final4['strength_y'] = ''
    df_final5['strength_y'] = ''
    
    # Final 2
    
    df_final2['strength_x'] = np.where(df_final2['mod_brand2'].str.contains('mg'), df_final2['mod_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final2['strength_x'])
    df_final2['strength_y'] = np.where(df_final2['new_brand2'].str.contains('mg'), df_final2['new_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final2['strength_y'])
    
    df_final2['strength_x'] = np.where(df_final2['mod_brand2'].str.contains('mcg'), df_final2['mod_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final2['strength_x'])
    df_final2['strength_y'] = np.where(df_final2['new_brand2'].str.contains('mcg'), df_final2['new_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final2['strength_y'])
    
    df_final2['strength_x'] = np.where(df_final2['mod_brand2'].str.contains('gm'), df_final2['mod_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final2['strength_x'])
    df_final2['strength_y'] = np.where(df_final2['new_brand2'].str.contains('gm'), df_final2['new_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final2['strength_y'])
    
    # Final 3
    
    df_final3['strength_x'] = np.where(df_final3['mod_brand2'].str.contains('mg'), df_final3['mod_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final3['strength_x'])
    df_final3['strength_y'] = np.where(df_final3['new_brand2'].str.contains('mg'), df_final3['new_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final3['strength_y'])
    
    df_final3['strength_x'] = np.where(df_final3['mod_brand2'].str.contains('mcg'), df_final3['mod_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final3['strength_x'])
    df_final3['strength_y'] = np.where(df_final3['new_brand2'].str.contains('mcg'), df_final3['new_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final3['strength_y'])
    
    df_final3['strength_x'] = np.where(df_final3['mod_brand2'].str.contains('gm'), df_final3['mod_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final3['strength_x'])
    df_final3['strength_y'] = np.where(df_final3['new_brand2'].str.contains('gm'), df_final3['new_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final3['strength_y'])
    
    # Final 4
    
    df_final4['strength_x'] = np.where(df_final4['mod_brand2'].str.contains('mg'), df_final4['mod_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final4['strength_x'])
    df_final4['strength_y'] = np.where(df_final4['new_brand2'].str.contains('mg'), df_final4['new_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final4['strength_y'])
    
    df_final4['strength_x'] = np.where(df_final4['mod_brand2'].str.contains('mcg'), df_final4['mod_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final4['strength_x'])
    df_final4['strength_y'] = np.where(df_final4['new_brand2'].str.contains('mcg'), df_final4['new_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final4['strength_y'])
    
    df_final4['strength_x'] = np.where(df_final4['mod_brand2'].str.contains('gm'), df_final4['mod_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final4['strength_x'])
    df_final4['strength_y'] = np.where(df_final4['new_brand2'].str.contains('gm'), df_final4['new_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final4['strength_y'])
    
    # Final 5
    
    df_final5['strength_x'] = np.where(df_final5['mod_brand2'].str.contains('mg'), df_final5['mod_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final5['strength_x'])
    df_final5['strength_y'] = np.where(df_final5['new_brand2'].str.contains('mg'), df_final5['new_brand2'].str.split('mg').str[0].str.split(' ').str[-1], df_final5['strength_y'])
    
    df_final5['strength_x'] = np.where(df_final5['mod_brand2'].str.contains('mcg'), df_final5['mod_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final5['strength_x'])
    df_final5['strength_y'] = np.where(df_final5['new_brand2'].str.contains('mcg'), df_final5['new_brand2'].str.split('mcg').str[0].str.split(' ').str[-1], df_final5['strength_y'])
    
    df_final5['strength_x'] = np.where(df_final5['mod_brand2'].str.contains('gm'), df_final5['mod_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final5['strength_x'])
    df_final5['strength_y'] = np.where(df_final5['new_brand2'].str.contains('gm'), df_final5['new_brand2'].str.split('gm').str[0].str.split(' ').str[-1], df_final5['strength_y'])
    
    # Flag
    
    df_final2['strength_x'] = df_final2['strength_x'].str.strip()
    df_final2['strength_y'] = df_final2['strength_y'].str.strip()
    df_final2['strength_flag'] = np.where( (df_final2['strength_x']==df_final2['strength_y']) & (df_final2['strength_x']!='') & (df_final2['strength_y']!=''), 0, df_final2['strength_flag'])
    df_final2['strength_flag'] = np.where( (df_final2['strength_x']!=df_final2['strength_y']) & (df_final2['strength_x']!='') & (df_final2['strength_y']!=''), 1, df_final2['strength_flag'])
    df_final2['strength_flag'] = np.where( (df_final2['strength_x']=='') | (df_final2['strength_y']==''), 2, df_final2['strength_flag'])
    
    df_final3['strength_x'] = df_final3['strength_x'].str.strip()
    df_final3['strength_y'] = df_final3['strength_y'].str.strip()
    df_final3['strength_flag'] = np.where( (df_final3['strength_x']==df_final3['strength_y']) & (df_final3['strength_x']!='') & (df_final3['strength_y']!=''), 0, df_final3['strength_flag'])
    df_final3['strength_flag'] = np.where( (df_final3['strength_x']!=df_final3['strength_y']) & (df_final3['strength_x']!='') & (df_final3['strength_y']!=''), 1, df_final3['strength_flag'])
    df_final3['strength_flag'] = np.where( (df_final3['strength_x']=='') | (df_final3['strength_y']==''), 2, df_final3['strength_flag'])
    
    df_final4['strength_x'] = df_final4['strength_x'].str.strip()
    df_final4['strength_y'] = df_final4['strength_y'].str.strip()
    df_final4['strength_flag'] = np.where( (df_final4['strength_x']==df_final4['strength_y']) & (df_final4['strength_x']!='') & (df_final4['strength_y']!=''), 0, df_final4['strength_flag'])
    df_final4['strength_flag'] = np.where( (df_final4['strength_x']!=df_final4['strength_y']) & (df_final4['strength_x']!='') & (df_final4['strength_y']!=''), 1, df_final4['strength_flag'])
    df_final4['strength_flag'] = np.where( (df_final4['strength_x']=='') | (df_final4['strength_y']==''), 2, df_final4['strength_flag'])
    
    df_final5['strength_x'] = df_final5['strength_x'].str.strip()
    df_final5['strength_y'] = df_final5['strength_y'].str.strip()
    df_final5['strength_flag'] = np.where( (df_final5['strength_x']==df_final5['strength_y']) & (df_final5['strength_x']!='') & (df_final5['strength_y']!=''), 0, df_final5['strength_flag'])
    df_final5['strength_flag'] = np.where( (df_final5['strength_x']!=df_final5['strength_y']) & (df_final5['strength_x']!='') & (df_final5['strength_y']!=''), 1, df_final5['strength_flag'])
    df_final5['strength_flag'] = np.where( (df_final5['strength_x']=='') | (df_final5['strength_y']==''), 2, df_final5['strength_flag'])
    
    
    
    ########################## Salt Flag ##########################
    
    df_final1['salt_flag'] = 0
    df_final2['salt_flag'] = 0
    df_final3['salt_flag'] = 0
    df_final4['salt_flag'] = 0
    df_final5['salt_flag'] = 0
    df_final6['salt_flag'] = 0
    
    #df_final2['salt_flag'] = np.where((df_final2['brand_x'].str.contains(' forte ')) & ~(df_final2['brand_y'].str.contains(' forte ')), 1, df_final2['salt_flag'])
    #df_final2['salt_flag'] = np.where((df_final2['brand_x'].str.contains(' plus ')) & ~(df_final2['brand_y'].str.contains(' plus ')), 1, df_final2['salt_flag'])
    
    check_list = ['plus', 'forte', 'a', 'af', 'am', 'as', 'bp',
                  'cd3', 'ch', 'cp', 'ct', 'cv', 'd', 'd', 'd3', 
                  'dp', 'ds', 'dsr', 'ez', 'f', 'f', 'g', 'h', 'h', 
                  'l', 'lc', 'lm', 'm', 'm', 'm', 'me', 'mf', 'oz', 
                  'p', 'pd', 'pz', 'r', 'sp', 't', 'v', 'z', 'mex', 
                  'lb', 'vg', 'dp', 'o', 'mv', 'sp', 'p']
    
    for s in check_list:
        
        s = ' ' + s + ' '
        
        df_final1['salt_flag'] = np.where((df_final1['brand_x'].str.contains(s)) & ~(df_final1['brand_y'].str.contains(s)), 1, df_final1['salt_flag'])
        #df_final1['salt_flag'] = np.where(~(df_final1['brand_x'].str.contains(s)) & (df_final1['brand_y'].str.contains(s)), 1, df_final1['salt_flag'])
    
        df_final2['salt_flag'] = np.where((df_final2['brand_x'].str.contains(s)) & ~(df_final2['brand_y'].str.contains(s)), 1, df_final2['salt_flag'])
        #df_final2['salt_flag'] = np.where(~(df_final2['brand_x'].str.contains(s)) & (df_final2['brand_y'].str.contains(s)), 1, df_final2['salt_flag'])
        
        df_final3['salt_flag'] = np.where((df_final3['brand_x'].str.contains(s)) & ~(df_final3['brand_y'].str.contains(s)), 1, df_final3['salt_flag'])
        #df_final3['salt_flag'] = np.where(~(df_final3['brand_x'].str.contains(s)) & (df_final3['brand_y'].str.contains(s)), 1, df_final3['salt_flag'])
        
        df_final4['salt_flag'] = np.where((df_final4['brand_x'].str.contains(s)) & ~(df_final4['brand_y'].str.contains(s)), 1, df_final4['salt_flag'])
        #df_final4['salt_flag'] = np.where(~(df_final4['brand_x'].str.contains(s)) & (df_final4['brand_y'].str.contains(s)), 1, df_final4['salt_flag'])
        
        df_final5['salt_flag'] = np.where((df_final5['brand_x'].str.contains(s)) & ~(df_final5['brand_y'].str.contains(s)), 1, df_final5['salt_flag'])
        #df_final5['salt_flag'] = np.where(~(df_final5['brand_x'].str.contains(s)) & (df_final5['brand_y'].str.contains(s)), 1, df_final5['salt_flag'])
        
        #df_final6['salt_flag'] = np.where((df_final6['brand_x'].str.contains(s)) & ~(df_final6['brand_y'].str.contains(s)), 1, df_final6['salt_flag'])
        #df_final6['salt_flag'] = np.where(~(df_final6['brand_x'].str.contains(s)) & (df_final6['brand_y'].str.contains(s)), 1, df_final6['salt_flag'])
    
    
    
    
    
    df_final2 = pd.merge(df_final2, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_final3 = pd.merge(df_final3, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_final4 = pd.merge(df_final4, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_final5 = pd.merge(df_final5, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_final6 = pd.merge(df_final6, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_tab6 = pd.merge(df_tab6, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_tab7 = pd.merge(df_tab7, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    
    
    ######################### Appending all output files and merging into one #########################
    
    df_final_n = df_final2.append([df_final3, df_final4, df_final5, df_final6], ignore_index = True)
    df_final = df_final_n.append([df_final1], ignore_index = True)
    del df_final['count']
    
    df_final_grp = df_final.groupby(['item_code'], as_index = False).agg({'brand_y':'count'}).rename(columns = {'brand_y':'count'})
    df_final_grp['count'].value_counts()
    
    df_final = pd.merge(df_final, df_final_grp, on='item_code', how='left')
    df_final['mod_cosine'] = df_final['cosine'] 
    df_final.to_csv('final_tab.csv', index = False)
    
    df_tab6.to_csv('remaining_tab.csv', index = False)
    
    
    
    # Single Match 
    
    print('Total -', df_final.shape[0])
    print('Single Match -', df_final[df_final['count']==1].shape[0])
    df_final[df_final['count']==1].to_csv('single_match_tab.csv', index = False)
    
    # Multiple Matches
    
    print('Multiple Match -', df_final[df_final['count']>1].shape[0])
    df_final[df_final['count']>1].to_csv('multiple_match_tab.csv', index = False)
    
    
    #################################################################################
    ########################## Preprocess and match Others ##########################
    #################################################################################
    
    # Ignore syp, this code is for all medicines except tablets and capsules
    
    
    df_master_syp = df_master.reset_index(drop = True) #[df_master['medicine_type']=='others'].reset_index(drop = True)
    df_syp = df_distributor[(df_distributor['medicine_type']!='tablet') & (df_distributor['medicine_type']!='capsule')].reset_index(drop = True)
    
    def preprocess_syrups():
        
        df_syp['mod_brand'] = df_syp['mod_brand'].str.lower()
        df_master_syp['new_brand'] = df_master_syp['new_brand'].str.lower()
        
        #df_master_syp['new_brand'] = df_master_syp['brand'].str.replace('.', ' ')
        df_master_syp['new_brand'] = df_master_syp['new_brand'].str.replace('-', ' ')
        df_master_syp['new_brand'] = df_master_syp['new_brand'].str.replace('^', ' ')
        df_master_syp['new_brand'] = df_master_syp['new_brand'].str.replace(' k ', 'k ')
        
        df_master_syp['new_brand2'] = df_master_syp['new_brand']
        df_master_syp['new_brand2'] = df_master_syp['new_brand2'].str.strip()
        df_master_syp['new_brand2'] = df_master_syp['new_brand2'] + ' '
        
        df_master_syp['new_brand'] = df_master_syp['new_brand'].str.replace('mg[/]ml', '')
        df_master_syp['new_brand'] = df_master_syp['new_brand'].str.replace('iu[/]ml', '')
        df_master_syp['new_brand'] = df_master_syp['new_brand'].str.replace('mcg[/]ml', '')
        df_master_syp['new_brand'] = df_master_syp['new_brand'].str.replace('kg', '')
        df_master_syp['new_brand'] = df_master_syp['new_brand'].str.replace('gm', '')
        df_master_syp['new_brand'] = df_master_syp['new_brand'].str.replace('mg', '')
        df_master_syp['new_brand'] = df_master_syp['new_brand'].str.replace('mcg', '')
        df_master_syp['new_brand'] = df_master_syp['new_brand'].str.replace('ml', '')
        #df_master_syp['new_brand'] = df_master_syp['new_brand'].str.replace('iu', '')
        
        #df_master_syp['new_brand'] = df_master_syp['new_brand'].str.replace('[/]', ' / ')
        df_master_syp['new_brand'] = df_master_syp['new_brand'].str.replace('capsules', 'capsule')
        
        df_master_syp['new_brand'] = df_master_syp['new_brand'].str.replace('  ', ' ')
        df_master_syp['new_brand'] = df_master_syp['new_brand'].str.strip()
        
        df_syp['exp_sku_brand'] = ""
        #df_syp['mod_brand'] = ""
        df_syp['count_cap'] = 0
        df_syp['count_tab'] = 0
        df_syp['count_syp'] = 0
    
        for n in range(df_syp.shape[0]):
    
            text1 = str(df_syp['mod_brand'][n])
            #print(text1)
            
            text1 = text1.replace('-', ' ')
            
            #text1 = text1.replace('.', ' ') ########- Check #############
            
            text1 = text1.replace('^', '')
            text1 = text1.replace('*', '')
            text1 = text1.replace('x1', 's')
            text1 = text1.replace('`', '')
            text1 = text1.replace("'", "")
            
            text1 = text1.replace(' mg', 'mg')
            text1 = text1.replace(' ml', 'ml')
            text1 = text1.replace(' gm', 'gm')
            text1 = text1.replace(' kg', 'kg')
            text1 = text1.replace(' mcg', 'mcg')
            text1 = text1.replace(' iu', 'iu')
            text1 = text1.replace('iu/ml', 'ml')
            text1 = text1.replace('mg/ml', 'ml')
            text1 = text1.replace('mcg/ml', 'ml')
            
            
            text1 = text1.replace('mg.', 'mg')
            text1 = text1.replace('ml.', 'ml')
            text1 = text1.replace('gm.', 'gm')
            text1 = text1.replace('mcg.', 'mcg')
            
            text1 = text1.replace('kg.', 'kg')
            text1 = text1.replace('iu.', 'iu')
            
            text1 = text1.replace(' k ', 'k ') # ovidac 5 k inj --> ovidac 5k inj
            
            # Capsule
            
            text1 = text1.replace(' cap.', ' cap')
            text1 = text1.replace(' capsules', ' cap')
            text1 = text1.replace(' capsule.', ' cap')
            text1 = text1.replace(' cap', ' capsule')
            text1 = text1.replace(' capsules', ' capsule') # Handling ' caps' cases - ' cap' already replaced by ' capsule'
            
            text1 = text1.replace(' capsuleule', ' capsule')
            text1 = text1.replace(' capsulesules', ' capsule')
            text1 = text1.replace(' capsulesule', ' capsule')
            
            text1 = text1.replace('  ', ' ')
            
            # Tablet
            
            text1 = text1.replace(' tab', ' tablet')
            text1 = text1.replace(' tab.', ' tablet')
            text1 = text1.replace(' tablets', ' tablet')
            text1 = text1.replace(' tabs', ' tablet')
            text1 = text1.replace(' tabletlets', ' tablet')
            text1 = text1.replace(' tabletlet', ' tablet')
            text1 = text1.replace(' tablet.', ' tablet')
            
            text1 = text1.replace('  ', ' ')
            
            # Injection
            
            text1 = text1.replace('inj', ' inj')
            text1 = text1.replace('  ', ' ')
            
            text1 = text1.replace(' inj.s', ' inj')
            text1 = text1.replace(' inj.', ' inj')
            text1 = text1.replace(' inj)', ' inj')
            text1 = text1.replace(' injs', ' inj')
            text1 = text1.replace(' injections', ' inj')
            text1 = text1.replace(' injection', ' inj')
            text1 = text1.replace(' injecti', ' inj')
            text1 = text1.replace(' injec', ' inj')
            text1 = text1.replace(' inje', ' inj')
            text1 = text1.replace(' injtion', ' inj')
            
            text1 = text1.replace(' inj', ' injection')
            text1 = text1.replace(' injection', ' injection ')
            text1 = text1.replace('  ', ' ')
            
            # # # syp, syp., syps, syr, syrup, syrups, syrp
            
            text1 = text1.replace('syp', ' syp')
            text1 = text1.replace('syr', ' syr')
            text1 = text1.replace('  ', ' ')
            
            text1 = text1.replace(' syp.s', ' syp')
            text1 = text1.replace(' syp.', ' syp')
            text1 = text1.replace(' syps', ' syp')
            
            text1 = text1.replace(' syrups', ' syp')
            text1 = text1.replace(' syrup', ' syp')
            text1 = text1.replace(' syrp', ' syp')
            text1 = text1.replace(' syr', ' syp')
            
            text1 = text1.replace(' syp', ' syrup')
            text1 = text1.replace(' syrup', ' syrup ')
            text1 = text1.replace('  ', ' ')
            
            df_syp['count_cap'][n] = text1.count('capsule')
            df_syp['count_tab'][n] = text1.count('tablet')
            df_syp['count_syp'][n] = text1.count('syrup')
    
            if(df_syp['count_cap'][n]==2):
                text1 = 's'.join(text1.rsplit(' capsule', 1))
    
            if(df_syp['count_tab'][n]==2):
                text1 = 's'.join(text1.rsplit(' tablet', 1))
    
    
            #if(df_syp['count_syp'][n]==2):
                #text1 = 'syp '.join(text1.split(' syrup ', 1))
                
    
            #print(text1)
    
            df_syp['mod_brand'][n] = text1.strip()
    
            tl1 = text1.split(' ')
            #print(tl1)
            text11 = ""
            
            flag = 0
    
            for i in range(len(tl1)):
    
                #print(i,'/',len(tl1), end = ', ')
    
                if( ( ('mg' in tl1[i]) & (i>0) ) | ( ('iu' in tl1[i]) & (i>0) ) | ( ('ml' in tl1[i]) & (i>0) ) | ( ('gm' in tl1[i]) & (i>0) ) | ( ('kg' in tl1[i]) & (i>0) ) | ( ('mcg' in tl1[i]) & (i>0) ) ):
                    break
                    
                cat = ['lotion', 'cream', 'gel', 'drop', 'susp', 'suspension', 'spray', 'sachet', 'powder', 'solution', 
                       'paster', 'soap', 'glove', 'mask', 'device', 'inf', 'infusion', 'bar', 'wipe', 'diaper', 'bandage',
                       'shp', 'shampoo', 'spy', 'spay', 'spry', 'granul', 'pwd', 'pwr', 'powder', 'tea', 'oil', 'strip'
                       'moisturiser', 'book', 'pen', 'tab', 'cap', 'inj', 'syp', 'syr']
                
                for s in cat:
                    if( str(s.lower()) in tl1[i] ):
                        flag = 1
                        break
                
                if(flag == 1):
                    break
    
                if( (i+1) < len(tl1) ):
    
                    if( (tl1[i].isnumeric()) & (('syrup' in tl1[i+1]) | ('tablet' in tl1[i+1]) | ('capsule' in tl1[i+1]) | ('injection' in tl1[i+1])) ):
                        break
    
                text11 = text11 + " " + tl1[i]
    
            #print(text11)
            df_syp['exp_sku_brand'][n] = text11.strip()
        
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('m[.]g', 'mg')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('m[.]l[.]', 'mg')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('g[.]m[.]', 'mg')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('i[.]u[.]', 'mg')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('mg[.]', 'mg')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('ml[.]', 'ml')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('mcg[.]', 'mcg')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('gm[.]', 'gm')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('iu[.]', 'iu')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('kg[.]', 'kg')
        
        df_syp['mod_brand2'] = df_syp['mod_brand']
        df_syp['mod_brand2'] = df_syp['mod_brand2'].str.replace('  ', ' ')
        df_syp['mod_brand2'] = df_syp['mod_brand2'].str.strip()
        df_syp['mod_brand2'] = df_syp['mod_brand2'] + ' '
        
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('mg', '')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('mcg', '')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('ml', '')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('gm', '')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('kg', '')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('iu', '')
        #df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('[/]', ' / ')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('  ', ' ')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('syrup', ' syrup ')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('injection', ' injection ')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('tablet', ' tablet ')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('capsule', ' capsule ')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.replace('  ', ' ')
        df_syp['mod_brand'] = df_syp['mod_brand'].str.strip()
        
        df_syp['exp_sku_brand'] = df_syp['exp_sku_brand'].str.lower().str.strip()
        
        df_syp['exp_sku_brand2'] = df_syp['exp_sku_brand'].str.rsplit(' ', 1).str[0]
        df_syp['exp_sku_brand3'] = df_syp['exp_sku_brand2'].str.rsplit(' ', 1).str[0]
        df_syp['exp_sku_brand4'] = df_syp['exp_sku_brand3'].str.rsplit(' ', 1).str[0]
        df_syp['exp_sku_brand5'] = df_syp['exp_sku_brand4'].str.rsplit(' ', 1).str[0]
        
        df_master_syp['sku_brand'] = df_master_syp['sku_brand'].str.lower()
        df_master_syp['sku_brand'] = df_master_syp['sku_brand'].str.replace('-', ' ')
        df_master_syp['sku_brand'] = df_master_syp['sku_brand'].str.replace('^', ' ')
        df_master_syp['sku_brand'] = df_master_syp['sku_brand'].str.replace("'", ' ')
        df_master_syp['sku_brand'] = df_master_syp['sku_brand'].str.replace("  ", ' ')
        df_master_syp['sku_brand'] = df_master_syp['sku_brand'].str.replace('`', '')
        
        df_master_syp['sku_brand'] = df_master_syp['sku_brand'].str.split(' ').str[0]
        df_master_syp['sku_brand'] = df_master_syp['sku_brand'].str.lower().str.strip()
        
        df_syp['mod_brand'] = (df_syp['mod_brand'].str.split()
                                  .apply(lambda x: OrderedDict.fromkeys(x).keys())
                                  .str.join(' '))
        df_master_syp['new_brand'] = (df_master_syp['new_brand'].str.split()
                                  .apply(lambda x: OrderedDict.fromkeys(x).keys())
                                  .str.join(' '))
        
    
    
    def find_syrup():
        
        print('Total Syrups in Distributer Data -', df_syp.shape[0])
        
        #=================== Exact Match on full brand names ===================#
        
        df_final1 = pd.merge(df_syp, df_master_syp, how='left', left_on='mod_brand', right_on = 'new_brand')
        df_final1 = df_final1.drop_duplicates(subset=['item_code', 'brand_x'], keep='last', ignore_index = True)
        m = df_final1[df_final1['brand_y'].notnull()].shape[0]
        print('Exact Match on full brand names - ', m, ' - ', (round)(m*100/df_syp.shape[0]),'%', sep = '')
        
        df_syp2 = df_final1[df_final1['brand_y'].isnull()].reset_index(drop = True)
        df_syp2 = df_syp2[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_syp', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4', 'exp_sku_brand5']]
        df_syp2 = df_syp2.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Syrups -', df_syp2.shape[0])
        
        df_final1 = df_final1[df_final1['brand_y'].notnull()].reset_index(drop = True)
        
        
        #=================== Match on sku_brand and exp_sku_brand ===================#
        
        df_syp2['exp_sku_brand'] = df_syp2['exp_sku_brand'].str.lower().str.strip()
        df_master_syp['sku_brand'] = df_master_syp['sku_brand'].str.lower().str.strip()
        df_final2 = pd.merge(df_syp2, df_master_syp, how='left', left_on='exp_sku_brand', right_on = 'sku_brand')
        
        
        m = int(df_final2[df_final2['brand_y'].notnull()].item_code.nunique())
        print('Exact Match on exp_sku_brand - ', m, ' - ', (round)(m*100/df_syp.shape[0]),'%', sep = '')
        
        df_syp3 = df_final2[df_final2['brand_y'].isnull()].reset_index(drop = True)
        df_syp3 = df_syp3[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_syp', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4', 'exp_sku_brand5']]
        df_syp3 = df_syp3.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Syrups -', df_syp3.shape[0])
        
        df_final2 = df_final2[df_final2['brand_y'].notnull()].reset_index(drop = True)
        df_final2['cosine'] = -1.0
    
        for i in range(len(df_final2)):
    
            text1 = str(df_final2['mod_brand'][i])
            text2 = str(df_final2['new_brand'][i])
    
            vector1 = text_to_vector(text1)
            vector2 = text_to_vector(text2)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final2['cosine'][i] = cosine
        
        
        df_final2.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        df_final2_grp = df_final2.groupby(['item_code', 'brand_x', 'medicine_type_x', 'mod_brand', 'exp_sku_brand'], as_index = False).agg({'cosine':'max'}).rename(columns = {'cosine':'max_cosine'})
        df_final2 = pd.merge(df_final2, df_final2_grp, on=['item_code', 'brand_x', 'medicine_type_x', 'mod_brand', 'exp_sku_brand'], how='left')
        
        df_final2 = df_final2[(df_final2['cosine']>=0) & ( df_final2['cosine']>=(0.9*df_final2['max_cosine']) )].reset_index(drop = True)
        #df_final2 = df_final2.groupby(['item_code', 'brand_x', 'medicine_type_x', 'mod_brand', 'exp_sku_brand', 'sku_brand'], as_index = False).agg({'brand_y':list, 'medicine_type_y':list, 'new_brand':list, 'cosine':list})
        #df_final2['count'] = df_final2['brand_y'].str.len()
        df_final2.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        #=================== Match on sku_brand and exp_sku_brand2 ===================#
        
        df_syp3['exp_sku_brand2'] = df_syp3['exp_sku_brand2'].str.lower().str.strip()
        df_master_syp['sku_brand'] = df_master_syp['sku_brand'].str.lower().str.strip()
        df_final3 = pd.merge(df_syp3, df_master_syp, how='left', left_on='exp_sku_brand2', right_on = 'sku_brand')
        
        m = int(df_final3[df_final3['brand_y'].notnull()].item_code.nunique())
        print('Exact Match on exp_sku_brand2 - ', m, ' - ', (round)(m*100/df_syp.shape[0]),'%', sep = '')
        
        df_syp4 = df_final3[df_final3['brand_y'].isnull()].reset_index(drop = True)
        df_syp4 = df_syp4[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_syp', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4', 'exp_sku_brand5']]
        df_syp4 = df_syp4.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Syrups -', df_syp4.shape[0])
        
        df_final3 = df_final3[df_final3['brand_y'].notnull()].reset_index(drop = True)
        df_final3['cosine'] = -1.0
    
        for i in range(len(df_final3)):
    
            text1 = str(df_final3['mod_brand'][i])
            text2 = str(df_final3['new_brand'][i])
    
            vector1 = text_to_vector(text1)
            vector2 = text_to_vector(text2)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final3['cosine'][i] = cosine
        
        df_final3.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        df_final3_grp = df_final3.groupby(['item_code', 'brand_x', 'medicine_type_x', 'mod_brand', 'exp_sku_brand2'], as_index = False).agg({'cosine':'max'}).rename(columns = {'cosine':'max_cosine'})
        df_final3 = pd.merge(df_final3, df_final3_grp, on=['item_code', 'brand_x', 'medicine_type_x', 'mod_brand', 'exp_sku_brand2'], how='left')
        
        df_final3 = df_final3[(df_final3['cosine']>=0) & ( df_final3['cosine']>=(0.9*df_final3['max_cosine']) )].reset_index(drop = True)
        #df_final3 = df_final3.groupby(['item_code', 'brand_x', 'medicine_type_x', 'mod_brand', 'exp_sku_brand2', 'sku_brand'], as_index = False).agg({'brand_y':list, 'medicine_type_y':list, 'new_brand':list, 'cosine':list})
        #df_final3['count'] = df_final3['brand_y'].str.len()
        df_final3.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        #=================== Match on sku_brand and exp_sku_brand3 ===================#
        
        df_syp4['exp_sku_brand3'] = df_syp4['exp_sku_brand3'].str.lower().str.strip()
        df_master_syp['sku_brand'] = df_master_syp['sku_brand'].str.lower().str.strip()
        df_final4 = pd.merge(df_syp4, df_master_syp, how='left', left_on='exp_sku_brand3', right_on = 'sku_brand')
        
        m = int(df_final4[df_final4['brand_y'].notnull()].item_code.nunique())
        print('Exact Match on exp_sku_brand3 - ', m, ' - ', (round)(m*100/df_syp.shape[0]),'%', sep = '')
        
        df_syp5 = df_final4[df_final4['brand_y'].isnull()].reset_index(drop = True)
        df_syp5 = df_syp5[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_syp', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4', 'exp_sku_brand5']]
        df_syp5 = df_syp5.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Syrups -', df_syp5.shape[0])
        
        df_final4 = df_final4[df_final4['brand_y'].notnull()].reset_index(drop = True)
        df_final4['cosine'] = -1.0
    
        for i in range(len(df_final4)):
    
            text1 = str(df_final4['mod_brand'][i])
            text2 = str(df_final4['new_brand'][i])
    
            vector1 = text_to_vector(text1)
            vector2 = text_to_vector(text2)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final4['cosine'][i] = cosine
        
        df_final4.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        df_final4_grp = df_final4.groupby(['item_code', 'brand_x', 'medicine_type_x', 'mod_brand', 'exp_sku_brand3'], as_index = False).agg({'cosine':'max'}).rename(columns = {'cosine':'max_cosine'})
        df_final4 = pd.merge(df_final4, df_final4_grp, on=['item_code', 'brand_x', 'medicine_type_x', 'mod_brand', 'exp_sku_brand3'], how='left')
        
        df_final4 = df_final4[(df_final4['cosine']>=0) & ( df_final4['cosine']>=(0.9*df_final4['max_cosine']) )].reset_index(drop = True)
        #df_final4 = df_final4.groupby(['item_code', 'brand_x', 'medicine_type_x', 'mod_brand', 'exp_sku_brand3', 'sku_brand'], as_index = False).agg({'brand_y':list, 'medicine_type_y':list, 'new_brand':list, 'cosine':list})
        #df_final4['count'] = df_final4['brand_y'].str.len()
        df_final4.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        #=================== Match on sku_brand and exp_sku_brand4 ===================#
        
        df_syp5['exp_sku_brand4'] = df_syp5['exp_sku_brand4'].str.lower().str.strip()
        df_master_syp['sku_brand'] = df_master_syp['sku_brand'].str.lower().str.strip()
        df_final5 = pd.merge(df_syp5, df_master_syp, how='left', left_on='exp_sku_brand4', right_on = 'sku_brand')
        
        m = int(df_final5[df_final5['brand_y'].notnull()].item_code.nunique())
        print('Exact Match on exp_sku_brand4 - ', m, ' - ', (round)(m*100/df_syp.shape[0]),'%', sep = '')
        
        df_syp6 = df_final5[df_final5['brand_y'].isnull()].reset_index(drop = True)
        df_syp6 = df_syp6[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_syp', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4', 'exp_sku_brand5']]
        df_syp6 = df_syp6.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Syrups -', df_syp6.shape[0])
        
        df_final5 = df_final5[df_final5['brand_y'].notnull()].reset_index(drop = True)
        df_final5['cosine'] = -1.0
    
        for i in range(len(df_final5)):
    
            text1 = str(df_final5['mod_brand'][i])
            text2 = str(df_final5['new_brand'][i])
    
            vector1 = text_to_vector(text1)
            vector2 = text_to_vector(text2)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final5['cosine'][i] = cosine
        
        df_final5.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        df_final5_grp = df_final5.groupby(['item_code', 'brand_x', 'medicine_type_x', 'mod_brand', 'exp_sku_brand4'], as_index = False).agg({'cosine':'max'}).rename(columns = {'cosine':'max_cosine'})
        df_final5 = pd.merge(df_final5, df_final5_grp, on=['item_code', 'brand_x', 'medicine_type_x', 'mod_brand', 'exp_sku_brand4'], how='left')
        
        df_final5 = df_final5[(df_final5['cosine']>=0) & ( df_final5['cosine']>=(0.9*df_final5['max_cosine']) )].reset_index(drop = True)
        #df_final5 = df_final5.groupby(['item_code', 'brand_x', 'medicine_type_x', 'mod_brand', 'exp_sku_brand4', 'sku_brand'], as_index = False).agg({'brand_y':list, 'medicine_type_y':list, 'new_brand':list, 'cosine':list})
        
        """if(df_final5.shape[0]>0):
            df_final5['count'] = df_final5['brand_y'].str.len()"""
        
        df_final5.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
            
        
        #=================== Match on sku_brand and exp_sku_brand5 ===================#
        
        df_syp6['exp_sku_brand5'] = df_syp6['exp_sku_brand5'].str.lower().str.strip()
        df_master_syp['sku_brand'] = df_master_syp['sku_brand'].str.lower().str.strip()
        df_final6 = pd.merge(df_syp6, df_master_syp, how='left', left_on='exp_sku_brand5', right_on = 'sku_brand')
        
        m = int(df_final6[df_final6['brand_y'].notnull()].item_code.nunique())
        print('Exact Match on exp_sku_brand5 - ', m, ' - ', (round)(m*100/df_syp.shape[0]),'%', sep = '')
        
        df_syp7 = df_final6[df_final6['brand_y'].isnull()].reset_index(drop = True)
        df_syp7 = df_syp7[['item_code', 'brand_x', 'Master Catalogue name', 'medicine_type_x',
                           'exp_sku_brand', 'mod_brand', 'count_syp', 'exp_sku_brand2',
                           'exp_sku_brand3', 'exp_sku_brand4', 'exp_sku_brand5']]
        df_syp7 = df_syp7.rename(columns = {'brand_x':'brand', 'medicine_type_x':'medicine_type'})
        
        print('Remaining Syrups -', df_syp7.shape[0])
        
        df_final6 = df_final6[df_final6['brand_y'].notnull()].reset_index(drop = True)
        df_final6['cosine'] = -1.0
    
        for i in range(len(df_final6)):
    
            text1 = str(df_final6['mod_brand'][i])
            text2 = str(df_final6['new_brand'][i])
    
            vector1 = text_to_vector(text1)
            vector2 = text_to_vector(text2)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final6['cosine'][i] = cosine
        
        df_final6.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        df_final6_grp = df_final6.groupby(['item_code', 'brand_x', 'medicine_type_x', 'mod_brand', 'exp_sku_brand5'], as_index = False).agg({'cosine':'max'}).rename(columns = {'cosine':'max_cosine'})
        df_final6 = pd.merge(df_final6, df_final6_grp, on=['item_code', 'brand_x', 'medicine_type_x', 'mod_brand', 'exp_sku_brand5'], how='left')
        
        df_final6 = df_final6[(df_final6['cosine']>=0) & ( df_final6['cosine']>=(0.9*df_final6['max_cosine']) )].reset_index(drop = True)
        #df_final6 = df_final6.groupby(['item_code', 'brand_x', 'medicine_type_x', 'mod_brand', 'exp_sku_brand5', 'sku_brand'], as_index = False).agg({'brand_y':list, 'medicine_type_y':list, 'new_brand':list, 'cosine':list})
        
        """if(df_final6.shape[0]>0):
            df_final6['count'] = df_final6['brand_y'].str.len()"""
        df_final6.sort_values(by=['item_code', 'brand_x', 'cosine'], ascending = [True, True, False], ignore_index = True, inplace = True)
        
        return df_final1, df_final2, df_final3, df_final4, df_final5, df_final6, df_syp7#, df_final7, df_syp8
        
    
    
    preprocess_syrups()
    #df_final1, df_final2, df_final3, df_final4, df_final5, df_final6, df_final7, df_syp7, df_syp8 = find_syrup()
    df_final1, df_final2, df_final3, df_final4, df_final5, df_final6, df_syp7 = find_syrup()
    
    
    
    ########################## Salt Flag ##########################
    
    df_final1['salt_flag'] = 0
    df_final2['salt_flag'] = 0
    df_final3['salt_flag'] = 0
    df_final4['salt_flag'] = 0
    df_final5['salt_flag'] = 0
    df_final6['salt_flag'] = 0
    
    #df_final2['salt_flag'] = np.where((df_final2['brand_x'].str.contains(' forte ')) & ~(df_final2['brand_y'].str.contains(' forte ')), 1, df_final2['salt_flag'])
    #df_final2['salt_flag'] = np.where((df_final2['brand_x'].str.contains(' plus ')) & ~(df_final2['brand_y'].str.contains(' plus ')), 1, df_final2['salt_flag'])
    
    check_list = ['plus', 'forte', 'a', 'af', 'am', 'as', 'bp',
                  'cd3', 'ch', 'cp', 'ct', 'cv', 'd', 'd', 'd3', 
                  'dp', 'ds', 'dsr', 'ez', 'f', 'f', 'g', 'h', 'h', 
                  'l', 'lc', 'lm', 'm', 'm', 'm', 'me', 'mf', 'oz', 
                  'p', 'pd', 'pz', 'r', 'sp', 't', 'v', 'z', 'mex', 
                  'lb', 'vg', 'dp', 'o', 'mv', 'sp', 'p']
    
    for s in check_list:
        
        s = ' ' + s + ' '
        
        df_final1['salt_flag'] = np.where((df_final1['brand_x'].str.contains(s)) & ~(df_final1['brand_y'].str.contains(s)), 1, df_final1['salt_flag'])
        #df_final1['salt_flag'] = np.where(~(df_final1['brand_x'].str.contains(s)) & (df_final1['brand_y'].str.contains(s)), 1, df_final1['salt_flag'])
    
        df_final2['salt_flag'] = np.where((df_final2['brand_x'].str.contains(s)) & ~(df_final2['brand_y'].str.contains(s)), 1, df_final2['salt_flag'])
        #df_final2['salt_flag'] = np.where(~(df_final2['brand_x'].str.contains(s)) & (df_final2['brand_y'].str.contains(s)), 1, df_final2['salt_flag'])
        
        df_final3['salt_flag'] = np.where((df_final3['brand_x'].str.contains(s)) & ~(df_final3['brand_y'].str.contains(s)), 1, df_final3['salt_flag'])
        #df_final3['salt_flag'] = np.where(~(df_final3['brand_x'].str.contains(s)) & (df_final3['brand_y'].str.contains(s)), 1, df_final3['salt_flag'])
        
        df_final4['salt_flag'] = np.where((df_final4['brand_x'].str.contains(s)) & ~(df_final4['brand_y'].str.contains(s)), 1, df_final4['salt_flag'])
        #df_final4['salt_flag'] = np.where(~(df_final4['brand_x'].str.contains(s)) & (df_final4['brand_y'].str.contains(s)), 1, df_final4['salt_flag'])
        
        df_final5['salt_flag'] = np.where((df_final5['brand_x'].str.contains(s)) & ~(df_final5['brand_y'].str.contains(s)), 1, df_final5['salt_flag'])
        #df_final5['salt_flag'] = np.where(~(df_final5['brand_x'].str.contains(s)) & (df_final5['brand_y'].str.contains(s)), 1, df_final5['salt_flag'])
        
        df_final6['salt_flag'] = np.where((df_final6['brand_x'].str.contains(s)) & ~(df_final6['brand_y'].str.contains(s)), 1, df_final6['salt_flag'])
        #df_final6['salt_flag'] = np.where(~(df_final6['brand_x'].str.contains(s)) & (df_final6['brand_y'].str.contains(s)), 1, df_final6['salt_flag'])
    
    
    
    
    ########################### Unit of measurement flag ##########################
    
    df_final2 = pd.merge(df_final2, df_syp[['item_code', 'mod_brand2']], on='item_code', how='left')
    df_final3 = pd.merge(df_final3, df_syp[['item_code', 'mod_brand2']], on='item_code', how='left')
    df_final4 = pd.merge(df_final4, df_syp[['item_code', 'mod_brand2']], on='item_code', how='left')
    df_final5 = pd.merge(df_final5, df_syp[['item_code', 'mod_brand2']], on='item_code', how='left')
    df_final6 = pd.merge(df_final6, df_syp[['item_code', 'mod_brand2']], on='item_code', how='left')
    
    df_final2['uom_flag'] = 0
    df_final3['uom_flag'] = 0
    df_final4['uom_flag'] = 0
    df_final5['uom_flag'] = 0
    df_final6['uom_flag'] = 0
    
    df_final2['uom_x'] = ''
    df_final3['uom_x'] = ''
    df_final4['uom_x'] = ''
    df_final5['uom_x'] = ''
    df_final6['uom_x'] = ''
    
    df_final2['uom_y'] = ''
    df_final3['uom_y'] = ''
    df_final4['uom_y'] = ''
    df_final5['uom_y'] = ''
    df_final6['uom_y'] = ''
    
    uom_list = ['au', 'ccid50', 'cells', 'cfu', 'cst', 'cu', 'd-antigenunits',
                'ffu', 'gm', 'iu', 'lf', 'mcg', 'mg', 'ml', 'pfu', 'ppm', 
                'spores', 'gm/10ml', 'gm/100ml', 'gm/200ml', 'gm/2ml', 'gm/5ml', 
                'gm/ml', 'iu/gm', 'iu/ml', 'iu/5ml', 'mcg/2ml', 'mcg/3ml', 
                'mcg/gm', 'mcg/ml', 'mcg/puff', 'mg/100ml', 'mg/10ml', 'mg/15ml', 
                'mg/2.5ml', 'mg/2ml', 'mg/3ml', 'mg/4ml', 'mg/5ml', 'mg/ml', 
                'mg/gm', 'mgi/ml', 'ml/ml', 'ml/5ml', 'v/v', 'v/w', 'w/v', 'w/w']
    
    for uom in uom_list:
        
        uom = uom + ' '
        
        df_final2['uom_x'] = np.where(df_final2['mod_brand2'].str.find(uom)!=-1, uom, df_final2['uom_x'])
        df_final2['uom_y'] = np.where(df_final2['new_brand2'].str.find(uom)!=-1, uom, df_final2['uom_y'])
        
        df_final3['uom_x'] = np.where(df_final3['mod_brand2'].str.find(uom)!=-1, uom, df_final3['uom_x'])
        df_final3['uom_y'] = np.where(df_final3['new_brand2'].str.find(uom)!=-1, uom, df_final3['uom_y'])
        
        df_final4['uom_x'] = np.where(df_final4['mod_brand2'].str.find(uom)!=-1, uom, df_final4['uom_x'])
        df_final4['uom_y'] = np.where(df_final4['new_brand2'].str.find(uom)!=-1, uom, df_final4['uom_y'])
        
        df_final5['uom_x'] = np.where(df_final5['mod_brand2'].str.find(uom)!=-1, uom, df_final5['uom_x'])
        df_final5['uom_y'] = np.where(df_final5['new_brand2'].str.find(uom)!=-1, uom, df_final5['uom_y'])
        
        df_final6['uom_x'] = np.where(df_final6['mod_brand2'].str.find(uom)!=-1, uom, df_final6['uom_x'])
        df_final6['uom_y'] = np.where(df_final6['new_brand2'].str.find(uom)!=-1, uom, df_final6['uom_y'])
    
    df_final2['uom_flag'] = np.where((df_final2['uom_x']==df_final2['uom_y']) & (df_final2['uom_x']!='') & (df_final2['uom_y']!=''), 0, df_final2['uom_flag'])
    df_final2['uom_flag'] = np.where((df_final2['uom_x']!=df_final2['uom_y']) & (df_final2['uom_x']!='') & (df_final2['uom_y']!=''), 1, df_final2['uom_flag'])
    df_final2['uom_flag'] = np.where((df_final2['uom_x']=='') | (df_final2['uom_y']==''), 2, df_final2['uom_flag'])
    
    df_final3['uom_flag'] = np.where((df_final3['uom_x']==df_final3['uom_y']) & (df_final3['uom_x']!='') & (df_final3['uom_y']!=''), 0, df_final3['uom_flag'])
    df_final3['uom_flag'] = np.where((df_final3['uom_x']!=df_final3['uom_y']) & (df_final3['uom_x']!='') & (df_final3['uom_y']!=''), 1, df_final3['uom_flag'])
    df_final3['uom_flag'] = np.where((df_final3['uom_x']=='') | (df_final3['uom_y']==''), 2, df_final3['uom_flag'])
    
    df_final4['uom_flag'] = np.where((df_final4['uom_x']==df_final4['uom_y']) & (df_final4['uom_x']!='') & (df_final4['uom_y']!=''), 0, df_final4['uom_flag'])
    df_final4['uom_flag'] = np.where((df_final4['uom_x']!=df_final4['uom_y']) & (df_final4['uom_x']!='') & (df_final4['uom_y']!=''), 1, df_final4['uom_flag'])
    df_final4['uom_flag'] = np.where((df_final4['uom_x']=='') | (df_final4['uom_y']==''), 2, df_final4['uom_flag'])
    
    df_final5['uom_flag'] = np.where((df_final5['uom_x']==df_final5['uom_y']) & (df_final5['uom_x']!='') & (df_final5['uom_y']!=''), 0, df_final5['uom_flag'])
    df_final5['uom_flag'] = np.where((df_final5['uom_x']!=df_final5['uom_y']) & (df_final5['uom_x']!='') & (df_final5['uom_y']!=''), 1, df_final5['uom_flag'])
    df_final5['uom_flag'] = np.where((df_final5['uom_x']=='') | (df_final5['uom_y']==''), 2, df_final5['uom_flag'])
    
    df_final6['uom_flag'] = np.where((df_final6['uom_x']==df_final6['uom_y']) & (df_final6['uom_x']!='') & (df_final6['uom_y']!=''), 0, df_final6['uom_flag'])
    df_final6['uom_flag'] = np.where((df_final6['uom_x']!=df_final6['uom_y']) & (df_final6['uom_x']!='') & (df_final6['uom_y']!=''), 1, df_final6['uom_flag'])
    df_final6['uom_flag'] = np.where((df_final6['uom_x']=='') | (df_final6['uom_y']==''), 2, df_final6['uom_flag'])
    
    
    
    
    
    df_final2 = pd.merge(df_final2, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_final3 = pd.merge(df_final3, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_final4 = pd.merge(df_final4, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_final5 = pd.merge(df_final5, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_final6 = pd.merge(df_final6, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    df_syp7 = pd.merge(df_syp7, df_distributor[['item_code', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']], on='item_code', how='left')
    
    
    ########################## Flag for Medicine_type ##########################
    
    df_final1['medicine_type_flag'] = np.where(df_final1['medicine_type_x']!=df_final1['medicine_type_y'], 1, 0)
    df_final2['medicine_type_flag'] = np.where(df_final2['medicine_type_x']!=df_final2['medicine_type_y'], 1, 0)
    df_final3['medicine_type_flag'] = np.where(df_final3['medicine_type_x']!=df_final3['medicine_type_y'], 1, 0)
    df_final4['medicine_type_flag'] = np.where(df_final4['medicine_type_x']!=df_final4['medicine_type_y'], 1, 0)
    df_final5['medicine_type_flag'] = np.where(df_final5['medicine_type_x']!=df_final5['medicine_type_y'], 1, 0)
    df_final6['medicine_type_flag'] = np.where(df_final6['medicine_type_x']!=df_final6['medicine_type_y'], 1, 0)
    
    
    
    
    ########################### Appending all output files and merging into one ##########################
    
    df_final_n = df_final2.append([df_final3, df_final4, df_final5, df_final6], ignore_index = True)
    df_final = df_final_n.append([df_final1], ignore_index = True)
    #del df_final['count']
    
    df_final_grp = df_final.groupby(['item_code'], as_index = False).agg({'brand_y':'count'}).rename(columns = {'brand_y':'count'})
    df_final_grp['count'].value_counts()
    
    df_final = pd.merge(df_final, df_final_grp, on='item_code', how='left')
    df_final.to_csv('final_oth.csv', index = False)
    
    df_syp7.to_csv('remaining_oth.csv', index = False)
    
    
    
    
    ################# Modified cosine for special cases #################
    
    df_final = pd.read_csv('final_oth.csv')
    
    df_final['mod_cosine'] = df_final['cosine'] 
    
    #df_final['mod_cosine'] = np.where((df_final['mod_brand'].str.contains('syrup')) & (df_final['new_brand'].str.contains('suspension')), )
    
    for i in range(df_final.shape[0]):
        
        if(i%1000==0):
            print(i)
        
        brand_x = df_final['mod_brand'][i]
        brand_y = df_final['new_brand'][i]
        
        # Syrup & Suspension
        
        if( ((' syrup ' in brand_x) & (' suspension ' in brand_y)) | ((' suspension ' in brand_x) & (' syrup ' in brand_y)) ):
            
            brand_x = brand_x.replace(' syrup ', ' ')
            brand_x = brand_x.replace(' suspension ', ' ')
            
            brand_y = brand_y.replace(' syrup ', ' ')
            brand_y = brand_y.replace(' suspension ', ' ')
    
            vector1 = text_to_vector(brand_x)
            vector2 = text_to_vector(brand_y)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final['mod_cosine'][i] = cosine
        
        # Suspension and oral suspension - ignore oral
        
        if( (' suspension ' in brand_x) & (' suspension ' in brand_y) & ( (' oral ' in brand_x) | (' oral ' in brand_y) ) ):
            
            brand_x = brand_x.replace(' oral ', ' ')
            brand_y = brand_y.replace(' oral ', ' ')
    
            vector1 = text_to_vector(brand_x)
            vector2 = text_to_vector(brand_y)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final['mod_cosine'][i] = cosine
            
        # Powder and dusting powder - ignore dusting
        
        if( (' powder ' in brand_x) & (' powder ' in brand_y) & ( (' dusting ' in brand_x) | (' dusting ' in brand_y) ) ):
            
            brand_x = brand_x.replace(' dusting ', ' ')
            brand_y = brand_y.replace(' dusting ', ' ')
    
            vector1 = text_to_vector(brand_x)
            vector2 = text_to_vector(brand_y)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final['mod_cosine'][i] = cosine
            
        # Tablet and Capsule - ignore
        
        if( (' tablet ' in brand_y) | (' capsule ' in brand_y) ):
            
            brand_x = brand_x.replace(' tablet ', ' ')
            brand_x = brand_x.replace(' capsule ', ' ')
            
            brand_y = brand_y.replace(' tablet ', ' ')
            brand_y = brand_y.replace(' capsule ', ' ')
    
            vector1 = text_to_vector(brand_x)
            vector2 = text_to_vector(brand_y)
    
            cosine = get_cosine(vector1, vector2)
    
            df_final['mod_cosine'][i] = cosine
            
        df_final['mod_brand'][i] = brand_x
        df_final['new_brand'][i] = brand_y
    
    df_final['mod_cosine'] = df_final['mod_cosine'].astype(float)
    df_final.to_csv('final_oth.csv', index = False)
    #df_final.to_csv('check.csv', index = False)      
    
    
    
    # Single Match 
    
    print('Total -',df_final.shape[0])
    print('Single Match -',df_final[df_final['count']==1].shape[0])
    df_final[df_final['count']==1].to_csv('single_match_oth.csv', index = False)
    
    # Multiple Matches
    
    print('Multiple Match -',df_final[df_final['count']>1].shape[0])
    df_final[df_final['count']>1].to_csv('multiple_match_oth.csv', index = False)
    
    
    #############################################################################################
    ##################### Combined results for Tablets, Capsules and Others #####################
    #############################################################################################
    
    df1 = pd.read_csv('single_match_tab.csv')
    df2 = pd.read_csv('single_match_cap.csv')
    df3 = pd.read_csv('single_match_oth.csv')
    df4 = pd.read_csv('multiple_match_tab.csv')
    df5 = pd.read_csv('multiple_match_cap.csv')
    df6 = pd.read_csv('multiple_match_oth.csv')
    
    df = df3.append([df2, df1, df4, df5, df6], ignore_index = True)
    df.to_csv(outfile1, index = False) # All Columns
    
    df['mod_cosine'] = df['mod_cosine'].astype(float)
    
    df_exact_match = df[(df['mod_cosine']>0.99) | (df['mod_cosine'].isnull())].reset_index(drop = True)
    df_others = df[(df['mod_cosine']>0) & (df['mod_cosine']<=0.99)].reset_index(drop = True)
    
    print('\n\nExact Match or Cosine = 1 -->', df_exact_match.shape[0])
    print('Total SKUs -->', df_distributor.shape[0])
    
    print('\n% of Exact Match --> ', df_exact_match.shape[0]*100/df_distributor.shape[0], '%', sep='')
    
    df_exact_match[['item_code', 'brand_x', 'brand_y', 'manufacturer', 'id', 'mrp']].to_csv(outfile2, index = False)
    df_others[['item_code', 'brand_x', 'brand_y', 'manufacturer', 'id', 'mrp', 'count']].to_csv(outfile3, index = False)


wrapper('Binay')