#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function
import pandas as pd
from lib_util import pyetl
from lib_util import lib_log
from collections import OrderedDict
from lib_util import get_filename
from socket import gethostname
from lib_util import utilities
from lib_util import lib_log
# import re
# import codecs
from csv import QUOTE_ALL,QUOTE_MINIMAL,QUOTE_NONE,QUOTE_NONNUMERIC
import os,re
import codecs
# import numpy as np
from datetime import date, timedelta, datetime
import gc
# import math
# from decimal import Decimal, ROUND_HALF_UP, ROUND_FLOOR, ROUND_CEILING


logger = lib_log.Logger()


class LoadFile(pyetl.FilePyEtl):
    def __init__(self, base_directory, input_dir, output_dir):
        super(LoadFile, self).__init__()
        self.empty_df_flag = True
        self.base_directory = base_directory
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.files_data = OrderedDict([
            # ('company', {
            #     "path_or_buf": os.path.join(self.input_dir,
            #                                 "company.txt"),
            #     "sep": "|",
            #     "usecols": ['company_code','company_name','job_status','iconum','jurisdiction_incorporated'],
            #     "encoding": 'latin-1',
            #     "quoting": QUOTE_ALL,
            #     #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            # }),


            # ('fight', {

            #     "path_or_buf": os.path.join(self.input_dir,
            #                                 "fight.txt"),
            #     "sep": "|",
            #     "usecols": ['company_code','id','announce_date','campaign_type_cd','proxy_fight_flg','title','marketcap','fight_synopsis'],
            #     "encoding": 'latin-1',
            #     "quoting": QUOTE_ALL,
            #     #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            # }),

            # ('company_identifier', {
            #     "path_or_buf": os.path.join(self.input_dir,
            #                                 "company_identifier.txt"),
            #     "sep": "|",
            #     "usecols": ['company_code','code','identifier'],
            #     "encoding": 'latin-1',
            #     "quoting": QUOTE_ALL,
            #     #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            # }),

            # ('company_sic', {
            #     "path_or_buf": os.path.join(self.input_dir,
            #                                 "company_sic.txt"),
            #     "sep": "|",
            #     "usecols": ['company_code','sic_code','seq'],
            #     "encoding": 'latin-1',
            #     "quoting": QUOTE_ALL,
            #     #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            # }),

            # ('SR_LookupFightCampaignType', {
            #     "path_or_buf": os.path.join(self.input_dir,
            #                                 "SR_LookupFightCampaignType.txt"),
            #     "sep": "|",
            #     "usecols": ['Code','Web_description'],
            #     "encoding": 'latin-1',
            #     "quoting": QUOTE_ALL,
            #     #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            # }),

            ('SR_LookupFilingType', {
                "path_or_buf": os.path.join(self.input_dir,
                                            "SR_LookupFilingType.txt"),
                "sep": "|",
                "usecols": ['Code','Web_description'],
                "encoding": 'latin-1',
                "quoting": QUOTE_ALL,
                #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            }),

            # ('company_ticker', {
            #     "path_or_buf": os.path.join(self.input_dir,
            #                                 "company_ticker.txt"),
            #     "sep": "|",
            #     "usecols": ['company_code','primary_listing','stock_exchange','ticker'],
            #     "encoding": 'latin-1',
            #     "quoting": QUOTE_ALL,
            #     #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            # }),

            # ('filermst', {
            #     "path_or_buf": os.path.join(self.input_dir,
            #                                 "filermst.txt"),
            #     "sep": "|",
            #     "usecols": ['iconum','sect_code','ind_code'],
            #     "encoding": 'latin-1',
            #     "quoting": QUOTE_ALL,
            #     #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            # }),

            # ('sector', {
            #     "path_or_buf": os.path.join(self.input_dir,
            #                                 "sector.txt"),
            #     "sep": "|",
            #     "usecols": ['sect_code','sector'],
            #     "encoding": 'latin-1',
            #     "quoting": QUOTE_ALL,
            #     #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            # }),

            # ('industries', {
            #     "path_or_buf": os.path.join(self.input_dir,
            #                                 "industries.txt"),
            #     "sep": "|",
            #     "usecols": ['ind_code','industry'],
            #     "encoding": 'latin-1',
            #     "quoting": QUOTE_ALL,
            #     #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            # }),

            # ('fight_source', {
            #     "path_or_buf": os.path.join(self.input_dir,
            #                                 "fight_source.txt"),
            #     "sep": "|",
            #     "usecols": ['seat_granted_to_dissident_flg', 'contentious_13d_item_flg','publish_flg', 'unsolicited_hostile_flg', 'description','special_exhibit_cd', 'source_type_cd','dissident_filing_flg','fight_id','filing_date','proxy_fight_formal_notice_flg'],
            #     "encoding": 'latin-1',
            #     "quoting": QUOTE_ALL,
            #     #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            # }),

            # ('fight_participant', {
            #     "path_or_buf": os.path.join(self.input_dir,
            #                                 "fight_participant.txt"),
            #     "sep": "|",
            #     "usecols": ['fight_id','participant_cd'],
            #     "encoding": 'latin-1',
            #     "quoting": QUOTE_ALL,
            #     #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            # }),

            # ('proponent_lookup', {
            #     "path_or_buf": os.path.join(self.input_dir,
            #                                 "proponent_lookup.txt"),
            #     "sep": "|",
            #     "usecols": ['core_activist_flg','iconum','id'],
            #     "encoding": 'latin-1',
            #     "quoting": QUOTE_ALL,
            #     #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            # }),
             ])

        # self.results = OrderedDict([

        #     ('temp_shark_news', {
        #         "path_or_buf":os.path.join(self.output_dir,
        #                                    "temp_shark_news.txt"),
        #         "sep":"|",
        #         "encoding": 'latin-1',
        #         "quoting":QUOTE_ALL,
        #         "index":False,
        #     }),
                                                                                 
        #  ])

         
    def transform(self):
        # ### merge all the left joines for one dataframe
        # company = self.company.copy()
        # fight = self.fight.copy()
        # fight_source = self.fight_source.copy()
        # SR_LookupFightCampaignType = self.SR_LookupFightCampaignType.copy()
        # SR_LookupFilingType = self.SR_LookupFilingType.copy()
        # company_identifier = self.company_identifier.copy()
        # filermst = self.filermst.copy()
        # sector = self.sector.copy()
        # industries = self.industries.copy()
        # company_sic = self.company_sic.copy()
        # company_ticker = self.company_ticker.copy()
        # fight_participant = self.fight_participant.copy()
        # proponent_lookup = self.proponent_lookup.copy()

        # # Strip all the LTRIM,RTRIM
        # company.job_status = company.job_status.str.strip()
        # fight.campaign_type_cd = fight.campaign_type_cd.str.strip()
        # fight_source.source_type_cd = fight_source.source_type_cd.str.strip()
        # fight_source.special_exhibit_cd = fight_source.special_exhibit_cd.str.strip()
        # SR_LookupFightCampaignType.Code = SR_LookupFightCampaignType.Code.str.strip()
        # SR_LookupFilingType.Code = SR_LookupFilingType.Code.str.strip()
        # company_identifier.identifier = company_identifier.identifier.str.strip()
        # sector.sector = sector.sector.str.strip()
        # industries.industry = industries.industry.str.strip()
        # company.rename(columns={'company_code':'company_code_c'},inplace=True)
        # fight.rename(columns={'company_code':'company_code_f'},inplace=True)
        # company.rename(columns={'company_code':'company_code_c'},inplace=True)
        # fight.rename(columns={'company_code':'company_code_f'},inplace=True)
        # fight.rename(columns={'id':'id_f'},inplace=True)

        # # #res2 will create 4th df(three) = 13D/A
        # res2 = company.merge(fight, how ='left',left_on=["company_code_c"],right_on=["company_code_f"]) 
        # print('company,fight',res2.shape)

        # res2 = res2.merge(fight_source, how ='left',left_on=["id_f"], right_on=["fight_id"]) 
        # print('fight_src',res2.shape)
        # res2= res2.merge(SR_LookupFightCampaignType, how ='left', left_on=["campaign_type_cd"], right_on=["Code"])
        # print('SR_LookupFightCampaignType',res2.shape)
        # res2= res2.merge(SR_LookupFilingType, how ='left', left_on=["source_type_cd"], right_on=["Code"])
        # res2.rename(columns={'Web_description_x':'Web_description_main'},inplace=True)
        # res2.rename(columns={'Web_description_y':'func_description'},inplace=True)
        # print('SR_LookupFilingType',res2.shape)
        # res2= pd.merge(res2,company_identifier[company_identifier.identifier=='CUSIP'],left_on='company_code_f',right_on='company_code',how='left')
        # print('company_identifier',res2.shape)
        # # res2 = res2[pd.to_datetime(res2.filing_date) >= (datetime.datetime.now() - timedelta(days=3))]
        # # print(len(res2))
        
        
        # res2= res2.merge(SR_LookupFightCampaignType, how ='left', left_on=["campaign_type_cd"], right_on=["Code"])
        # print('SR_LookupFightCampaignType',res2.shape)
        # res2= res2.merge(SR_LookupFilingType, how ='left', left_on=["source_type_cd"], right_on=["Code"])
        # print('SR_LookupFilingType',res2.shape)

        # print('until_lkpfilling_typ',res2.shape)
        # # company_identifier2 = company_identifier[company_identifier.identifier == 'CUSIP']
        # res2= res2.merge(company_identifier[company_identifier.identifier == 'CUSIP'],how ='left', on=["company_code"])
        # print('company_identifier',res2.shape)
        
        # res2= res2.merge(filermst, how ='left', on=["iconum"])
        # print('filermst',len(res2))
        # res2= res2.merge(sector, how ='left', on=["sect_code"])
        # print('sector',len(res2))
        # res2= res2.merge(industries, how ='left', on=["ind_code"])
        # print('industry',len(res2))

        
        

        # res2= res2.merge(company_sic[company_sic.seq==1],how ='left', left_on=["company_code_f"],right_on=["company_code"])
        # print('company_sic',len(res2))
        # # company_ticker1 = company_ticker[company_ticker.primary_listing==-1]
        # res2= res2.merge(company_ticker[company_ticker.primary_listing==-1], how ='left', left_on=["company_code_f"],right_on=["company_code"])
        # print('company_ticker',len(res2))
        # res2= res2.merge(fight_participant, how ='left', left_on=["id_f"], right_on=["fight_id"])
        # print('fight_participants',fight_participant.shape)
        # res2= res2.merge(proponent_lookup, how ='left', left_on=["participant_cd"], right_on=["id"])
        # print('proponent_lookup',res2.shape)  

       
        # # res2=pd.read_csv(r'D:\User\learladine\Sharknews\Final________.txt',sep='|')
        # #where a.company_code = b.company_code
        # res2=res2.merge(pd.DataFrame(fight['company_code_f'].drop_duplicates()),left_on=['company_code_c'],right_on=['company_code_f'],how='inner')
        # print("where a.company_code = b.company_code",res2.shape)

        # res2=res2.merge(pd.DataFrame(fight_source['fight_id'].drop_duplicates()),left_on='id_f',right_on='fight_id',how='inner')

        # print("AND b.id = c.fight_id",res2.shape)

        

        # print('shapeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',res2.shape)
        # res2.job_status = res2.job_status.fillna('WIP')
        
        # res2 = res2[res2['job_status'].isin(["APR" ,"CHIP"])] 

        # print('job_sts',len(res2))

        # res2=res2[res2.campaign_type_cd.fillna('XXX').str.strip()!='13DFILER']
        # print('13dFiler',len(res2))

        # def check_announc_filing(anno,filing):


        #     if anno != filing:
        #             return 1
        #     else:
        #             return 0


        # res2['Annonc_filing_rst']=res2[['announce_date','filing_date']].apply(lambda x: check_announc_filing(x['announce_date'],x['filing_date']),axis=1)

        # res2=res2[res2['Annonc_filing_rst']==1]

        # del res2['Annonc_filing_rst']

        # print("b.announce_date <> c.filing_date",res2.shape)

        # res2.to_csv(r'D:\User\learladine\Sharknews\res2_join..txt',sep='|',index=False)
        # print(res2.shape)
        # print(SR_LookupFightCampaignType.dtypes)
        # print(res2.campaign_type_cd.dtypes )
        # res2= res2.merge(SR_LookupFightCampaignType,left_on='campaign_type_cd',right_on='Code',how='inner')

        # print('SR_LookupFightCampaignType',res2.shape)

        # res2=res2[pd.to_datetime(res2['announce_date'])>pd.to_datetime('2006-01-01')]

        # print('announce',len(res2))
        import datetime
        # res2=res2[pd.to_datetime(res2.filing_date).dt.date >= (datetime.date.today() - datetime.timedelta(days=300))]

        # print('res2 count after alll joinings and filters',res2.shape)
        # res2.to_csv(r'D:\User\learladine\Sharknews\res2_join.txt',sep='|',index=False)

        res2=pd.read_csv(r'D:\User\learladine\Sharknews\res2_join.txt',sep='|')

        print(res2.func_description.head(15))


        print('final__________________',res2.shape)

        
        print(res2.columns)

        res2['fight_id'] = res2.id_f.astype('int')
        res2.drop(['fight_id_x','id_f','fight_id_y'], axis=1, inplace=True)
        date=pd.to_datetime(res2['announce_date']).dt.strftime('%Y%m%d').astype('str')
        fid=res2['fight_id'].astype('str')
        res2['story_id'] = date+fid
        res2['type'] = 'new'
        # f = lambda x: 1 if x==True else 0
        # na = lambda x: 0 if pd.x==null else x
        # na_x = lambda x: 'XXX' if pd.x==null else x
        # res2.unsolicited_hostile_flg = res2.unsolicited_hostile_flg.apply(f)
        # res2.proxy_fight_formal_notice_flg = res2.proxy_fight_formal_notice_flg.apply(f)
        # res2.proxy_fight_flg = res2.proxy_fight_flg.apply(f)
        # res2.publish_flg = res2.publish_flg.apply(f)
        # res2.seat_granted_to_dissident_flg = res2.seat_granted_to_dissident_flg.apply(f)
        # res2.dissident_filing_flg = res2.dissident_filing_flg.apply(f)
        # res2.contentious_13d_item_flg = res2.contentious_13d_item_flg.apply(f)

        #
        def headline2(unsolicited_hostile_flg,announce_date,filing_date,title,func_description,proxy_fight_formal_notice_flg,\
                        special_exhibit_cd,proxy_fight_flg,publish_flg, dissident_filing_flg, seat_granted_to_dissident_flg,\
                       contentious_13d_item_flg,source_type_cd ):
            
                

            unsolicited_hostile_flg = 0 if pd.isnull(unsolicited_hostile_flg) else unsolicited_hostile_flg
            publish_flg = 0 if pd.isnull(publish_flg) else publish_flg
            dissident_filing_flg = 0 if pd.isnull(dissident_filing_flg) else dissident_filing_flg
            proxy_fight_flg = 0 if pd.isnull(proxy_fight_flg) else proxy_fight_flg
            seat_granted_to_dissident_flg = 0 if pd.isnull(seat_granted_to_dissident_flg) else seat_granted_to_dissident_flg
            contentious_13d_item_flg = 0 if pd.isnull(contentious_13d_item_flg) else contentious_13d_item_flg
            source_type_cd = 'XXX' if pd.isnull(source_type_cd) else source_type_cd
            
            if unsolicited_hostile_flg in (1,-1) and pd.to_datetime(announce_date)>pd.to_datetime('2006-01-01 00:00:00.000') and pd.to_datetime(filing_date) >= (datetime.datetime.now() - datetime.timedelta(days=10)):
                return 'Update to '+title+' Activist Campaign - '+func_description+' Discloses Unsolicited Offer'      #--Priority #2
            proxy_fight_formal_notice_flg = 0 if pd.isnull(proxy_fight_formal_notice_flg) else proxy_fight_formal_notice_flg
            if proxy_fight_formal_notice_flg in (1,-1):
                return title+' Activist Campaign Escalated to Formal Proxy Fight'                                 #--Priority #3
            special_exhibit_cd = 'XXX' if pd.isnull(special_exhibit_cd) else special_exhibit_cd
            if str(special_exhibit_cd).strip() =='SETTLE':
                concate_1= 'Settlement Agreement Disclosed Ending '+title                                          #--Priority #4a
                concate_2= ' Proxy Fight' if (0 if pd.isnull(proxy_fight_flg) else proxy_fight_flg) in (1,-1) else ' Activist Campaign'
                return concate_1+concate_2
            if str(special_exhibit_cd).strip() =='STAND':
                concate_1= 'Standstill Agreement Disclosed Ending '+title                                          #--Priority #4b
                concate_2= ' Proxy Fight' if (0 if pd.isnull(proxy_fight_flg) else proxy_fight_flg) in (1,-1) else ' Activist Campaign'
                return concate_1+concate_2
            
            #special_exhibit_cd = special_exhibit_cd.fillna('XXX')

            if publish_flg in (1, -1) and dissident_filing_flg == 0:
                concate_1= title+' - Company Makes Material Announcement Regarding '                                            #--Priority #5
                concate_2= ' Proxy Fight' if proxy_fight_flg in (1,-1) else ' Activist Campaign'
                return concate_1+concate_2
            if seat_granted_to_dissident_flg in (1, -1):
                concate_1= 'Update to '+ title                                                                      #--Priority #6                
                concate_2= ' Proxy Fight - Board Representation Granted to Activist' if proxy_fight_flg in (1,-1) else ' Activist Campaign - Board Representation Granted to Activist'
                return concate_1+concate_2
            if dissident_filing_flg in (1, -1) and str(source_type_cd).strip() in ('DEFC14A', 'DEFC14C', 'DEFN14A') and str(special_exhibit_cd).strip()=='XXX':
                return 'Dissident Definitive Contested Proxy Filed in ' + title + ' Proxy Fight'  #--Priority #7
            if dissident_filing_flg in (1, -1) and str(source_type_cd).strip() in ('PREC14A', 'PREC14C', 'PREN14A') and str(special_exhibit_cd).strip()=='XXX': 
                return 'Dissident Preliminary Contested Proxy Filed in ' + title + ' Proxy Fight'  #--Priority #8
            if dissident_filing_flg in (1, -1) and str(special_exhibit_cd).strip()== 'BOARD_LETTER': 
                return 'Update to ' + title + ' Activist Campaign - ' + func_description + ' Discloses Letter to Board' #--Priority #9
            if dissident_filing_flg in (1, -1) and str(source_type_cd).strip() == 'PR' and str(special_exhibit_cd).strip()=='XXX': 
                return 'Update to ' + title + ' Campaign - New Activist Press Release Disclosed' #--Priority #10
            if dissident_filing_flg in (1, -1) and str(source_type_cd).strip() == '13D/A' and contentious_13d_item_flg in (1, -1) and str(special_exhibit_cd).strip()=='XXX': 
                return 'Update to ' + title + ' Activist Campaign - Hostile 13D/A Filed' #--Priority #11
            
        # res2['headline']=res2[['unsolicited_hostile_flg','announce_date','filing_date','title','description','proxy_fight_formal_notice_flg',\
        #                 'special_exhibit_cd','proxy_fight_flg','publish_flg','dissident_filing_flg', 'seat_granted_to_dissident_flg',\
        #                'contentious_13d_item_flg','source_type_cd']].apply(lambda x:headline2(x['unsolicited_hostile_flg'] ,\
        #                 x['announce_date'] ,x['filing_date'] , x['title'] ,x['description'] ,x['proxy_fight_formal_notice_flg'] ,\
        #                 x['special_exhibit_cd'] ,x['proxy_fight_flg'],x['publish_flg'],x['dissident_filing_flg'],x['seat_granted_to_dissident_flg'],\
        #                 x['contentious_13d_item_flg'],x['source_type_cd']),axis=1)


        res2['headline']=res2[['unsolicited_hostile_flg','announce_date','filing_date','title','func_description','proxy_fight_formal_notice_flg',\
                    'special_exhibit_cd','proxy_fight_flg','publish_flg','dissident_filing_flg', 'seat_granted_to_dissident_flg',\
                   'contentious_13d_item_flg','source_type_cd']].apply(lambda x:headline2(x['unsolicited_hostile_flg'] ,\
                    x['announce_date'] ,x['filing_date'] , x['title'] ,x['func_description'] ,x['proxy_fight_formal_notice_flg'] ,\
                    x['special_exhibit_cd'] ,x['proxy_fight_flg'],x['publish_flg'],x['dissident_filing_flg'],x['seat_granted_to_dissident_flg'],\
                        x['contentious_13d_item_flg'],x['source_type_cd']),axis=1)
                                                                                                                                                                                                          
        
        def headline_rank2(unsolicited_hostile_flg, announce_date, filing_date,proxy_fight_formal_notice_flg,special_exhibit_cd,publish_flg,dissident_filing_flg,\
            seat_granted_to_dissident_flg,source_type_cd,contentious_13d_item_flg):
                       
            unsolicited_hostile_flg = 0 if pd.isnull(unsolicited_hostile_flg) else unsolicited_hostile_flg   
            seat_granted_to_dissident_flg = 0 if pd.isnull(seat_granted_to_dissident_flg) else seat_granted_to_dissident_flg
            publish_flg = 0 if pd.isnull(publish_flg) else publish_flg
            dissident_filing_flg = 0 if pd.isnull(dissident_filing_flg) else dissident_filing_flg
            source_type_cd = 'XXX' if pd.isnull(source_type_cd) else source_type_cd
            contentious_13d_item_flg = 0 if pd.isnull(contentious_13d_item_flg) else contentious_13d_item_flg
            proxy_fight_formal_notice_flg = 0 if pd.isnull(proxy_fight_formal_notice_flg) else proxy_fight_formal_notice_flg
            if unsolicited_hostile_flg in (1,-1) and pd.to_datetime(announce_date)>pd.to_datetime('2006-01-01 00:00:00.000') and pd.to_datetime(filing_date) >= (datetime.datetime.now() - datetime.timedelta(days=10)):
                return 2
            if proxy_fight_formal_notice_flg in (1,-1):
                return 3 #--Priority #3
            if str('XXX' if pd.isnull(special_exhibit_cd) else special_exhibit_cd).strip() =='SETTLE':
                return 4 #--Priority 4a
            if str('XXX' if pd.isnull(special_exhibit_cd) else special_exhibit_cd).strip() =='STAND':
                return 5 #--Priority #4b 
            if publish_flg in (1, -1) and dissident_filing_flg == 0:
                return 6 #--Priority #5
            if seat_granted_to_dissident_flg in (1, -1):
                return 7 #--Priority #6
            if dissident_filing_flg in (1, -1) and str(source_type_cd).strip() in ('DEFC14A', 'DEFC14C', 'DEFN14A') and pd.isnull(special_exhibit_cd):
                return 8 #--Priority #7
            if dissident_filing_flg in (1, -1) and str(source_type_cd).strip() in ('PREC14A', 'PREC14C', 'PREN14A') and pd.isnull(special_exhibit_cd):    #--Priority #8
                return 9
            if dissident_filing_flg in (1, -1) and str(special_exhibit_cd).strip() == 'BOARD_LETTER':    #--Priority #9
                return 10
            if dissident_filing_flg in (1, -1) and str(source_type_cd).strip() == 'PR' and pd.isnull(special_exhibit_cd):
                return 11       #--Priority #10
            if dissident_filing_flg in (1, -1) and str(source_type_cd).strip() == '13D/A' and contentious_13d_item_flg in (1, -1) and pd.isnull(special_exhibit_cd):
                return 12      #--Priority #11
                
                
                
        
        res2['headline_rank'] = res2[['unsolicited_hostile_flg', 'announce_date', 'filing_date','proxy_fight_formal_notice_flg','special_exhibit_cd',\
                                            'publish_flg','dissident_filing_flg','seat_granted_to_dissident_flg','source_type_cd','contentious_13d_item_flg']]\
                                            .apply(lambda x:headline_rank2(x['unsolicited_hostile_flg'] ,x['announce_date'] ,x['filing_date'] ,\
                                            x['proxy_fight_formal_notice_flg'] ,x['special_exhibit_cd'],x['publish_flg'],x['dissident_filing_flg'],\
                                            x['seat_granted_to_dissident_flg'],x['source_type_cd'],x['contentious_13d_item_flg']),axis=1)

                # res2['headline_rank'] = res2[['unsolicited_hostile_flg', 'announce_date', 'filing_date','proxy_fight_formal_notice_flg','special_exhibit_cd',\
        #                 'publish_flg','dissident_filing_flg','seat_granted_to_dissident_flg','source_type_cd','contentious_13d_item_flg']]\
        #                 .apply(lambda x:headline_rank2(x['unsolicited_hostile_flg'] ,x['announce_date'] ,x['filing_date'] ,\
        #                  x['proxy_fight_formal_notice_flg'] ,x['special_exhibit_cd'],x['publish_flg'],x['dissident_filing_flg'],\
        #                  x['seat_granted_to_dissident_flg'],x['source_type_cd'],x['contentious_13d_item_flg']),axis=1)
                        
        # res2['publication_date']= pd.to_datetime(res2.filing_date)
        # x= datetime.now().strftime('%H%M%S')
        # res2.loc[res2['filing_date']==pd.datetime.now(),'publication_time']=x
        # res2.loc[res2['filing_date']!=pd.datetime.now(),'publication_time']='180000'  
        res2['publication_date']= pd.to_datetime(res2['filing_date'],errors='coerce').dt.strftime('%Y-%m-%d')

        import datetime
        current_timestamp=datetime.datetime.now().strftime('%H%M%S')
        res2['publication_time']=pd.to_datetime(res2['filing_date'],errors='coerce').dt.strftime('%H%M%S').apply(lambda x:x if x==current_timestamp else '180000')



        res2['language'] = 'en'
        res2['cusip'] = res2.code_x
        res2['factset_id'] = res2['iconum_x']#.astype('int')
        res2['event_type'] = 'Significant Developments'
        res2['fds_sector'] = res2['sect_code']#.astype('int')
        res2['fds_sector_name'] = res2['sector']
        res2['fds_industry'] = res2['ind_code']#.astype('int')
        res2['fds_industry_name'] = res2['industry']
        res2['jurisdiction_incorporated'] = res2['jurisdiction_incorporated'].fillna('United States')
        res2['market_cap'] = res2.marketcap
        res2['sic_code'] = res2['sic_code']#.astype('int')
        res2['campaign_type'] = res2['Web_description_main']
        res2['campaign_summary'] = res2['fight_synopsis']
        res2['delivery_timestamp'] = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        res2['company_name'] = res2['company_name']
        res2['company_ticker'] = res2['ticker']
        res2['company_stock_exchange'] = res2['stock_exchange']
        res2['company_parent_name'] = res2['company_name']
        res2['company_parent_ticker'] = res2['ticker']
        res2['company_parent_stock_exchange'] = res2['stock_exchange']
        res2['company_parent_cusip'] = res2['code_x']
        res2['company_parent_factset_id'] = res2['iconum_x']#.astype('int')
        res2['sharkwatch_50_member'] = res2['core_activist_flg']
        res2['proponent_iconum'] = res2['iconum_y']#.astype('int')

        print('Res2_ sahpeeeeeeeeeeeeeeeeeeeeeeeeeeee',res2.shape)

        # print('11111111111111111111111111',res2[res2['fight_id']==1073552591])

        res2.sort_values(by=['fight_id'], inplace=True)
        res2 = res2.loc[res2['headline'].notnull()]
        # print('22222222222222222222222222222222',res2[res2['fight_id']==1073552591])

        print('After not null Res2_ sahpeeeeeeeeeeeeeeeeeeeeeeeeeeee',res2.shape)

        res2['headline'] = res2['headline'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res2['fds_sector_name'] = res2['fds_sector_name'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res2['fds_industry_name'] = res2['fds_industry_name'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res2['jurisdiction_incorporated'] = res2['jurisdiction_incorporated'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res2['market_cap'] = res2['market_cap'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res2['campaign_type'] = res2['campaign_type'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res2['latest_development'] = res2['description'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res2['campaign_summary'] = res2['campaign_summary'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res2['company_name'] = res2['company_name'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res2['company_parent_name'] = res2['company_parent_name'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')

        res2 = res2[['fight_id','story_id','type','headline' ,'publication_date','publication_time','language','cusip','factset_id','event_type'
            ,'fds_sector','fds_sector_name','fds_industry','fds_industry_name','jurisdiction_incorporated','market_cap','sic_code','campaign_type'
            ,'latest_development','campaign_summary','delivery_timestamp','company_name','company_ticker','company_stock_exchange'
            ,'company_parent_name','company_parent_ticker','company_parent_stock_exchange','company_parent_cusip','company_parent_factset_id'
            ,'sharkwatch_50_member','proponent_iconum','headline_rank']].drop_duplicates()


        
        print('33333333333333333333333333333',res2.shape)
        res2.to_csv(r"D:\User\learladine\Sharknews\res2_join_fnl.txt",sep='|',index=False,encoding='latin-1')
            


        #self.temp_shark_news = res2.copy()
        

