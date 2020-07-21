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
from decimal import Decimal, ROUND_HALF_UP


logger = lib_log.Logger()


class LoadFile(pyetl.FilePyEtl):
    def __init__(self, base_directory, input_dir, output_dir):
        super(LoadFile, self).__init__()
        self.empty_df_flag = True
        self.base_directory = base_directory
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.files_data = OrderedDict([
            ('company', {
                "path_or_buf": os.path.join(self.input_dir,
                                            "company.txt"),
                "sep": "|",
                "usecols": ['company_code','company_name','job_status','iconum','jurisdiction_incorporated'],
                "encoding": 'latin-1',
                "quoting": QUOTE_ALL,
                #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            }),


            ('fight', {

                "path_or_buf": os.path.join(self.input_dir,
                                            "fight.txt"),
                "sep": "|",
                "usecols": ['company_code','id','announce_date','campaign_type_cd','proxy_fight_flg','title','marketcap','fight_synopsis'],
                "encoding": 'latin-1',
                "quoting": QUOTE_ALL,
                #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            }),

            ('company_identifier', {
                "path_or_buf": os.path.join(self.input_dir,
                                            "company_identifier.txt"),
                "sep": "|",
                "usecols": ['company_code','code','identifier'],
                "encoding": 'latin-1',
                "quoting": QUOTE_ALL,
                #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            }),

            ('company_sic', {
                "path_or_buf": os.path.join(self.input_dir,
                                            "company_sic.txt"),
                "sep": "|",
                "usecols": ['company_code','sic_code','seq'],
                "encoding": 'latin-1',
                "quoting": QUOTE_ALL,
                #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            }),

            ('SR_LookupFightCampaignType', {
                "path_or_buf": os.path.join(self.input_dir,
                                            "SR_LookupFightCampaignType.txt"),
                "sep": "|",
                "usecols": ['Code','Web_description'],
                "encoding": 'latin-1',
                "quoting": QUOTE_ALL,
                #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            }),

            ('SR_LookupFilingType', {
                "path_or_buf": os.path.join(self.input_dir,
                                            "SR_LookupFilingType.txt"),
                "sep": "|",
                "usecols": ['Code','Web_description'],
                "encoding": 'latin-1',
                "quoting": QUOTE_ALL,
                #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            }),

            ('company_ticker', {
                "path_or_buf": os.path.join(self.input_dir,
                                            "company_ticker.txt"),
                "sep": "|",
                "usecols": ['company_code','primary_listing','stock_exchange','ticker'],
                "encoding": 'latin-1',
                "quoting": QUOTE_ALL,
                #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            }),

            ('filermst', {
                "path_or_buf": os.path.join(self.input_dir,
                                            "filermst.txt"),
                "sep": "|",
                "usecols": ['iconum','sect_code','ind_code'],
                "encoding": 'latin-1',
                "quoting": QUOTE_ALL,
                #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            }),

            ('sector', {
                "path_or_buf": os.path.join(self.input_dir,
                                            "sector.txt"),
                "sep": "|",
                "usecols": ['sect_code','sector'],
                "encoding": 'latin-1',
                "quoting": QUOTE_ALL,
                #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            }),

            ('industries', {
                "path_or_buf": os.path.join(self.input_dir,
                                            "industries.txt"),
                "sep": "|",
                "usecols": ['ind_code','industry'],
                "encoding": 'latin-1',
                "quoting": QUOTE_ALL,
                #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            }),

            ('fight_source', {
                "path_or_buf": os.path.join(self.input_dir,
                                            "fight_source.txt"),
                "sep": "|",
                "usecols": ['seat_granted_to_dissident_flg', 'publish_flg', 'unsolicited_hostile_flg', 'description','special_exhibit_cd', 'source_type_cd','dissident_filing_flg','fight_id','filing_date','proxy_fight_formal_notice_flg'],
                "encoding": 'latin-1',
                "quoting": QUOTE_ALL,
                #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            }),

            ('fight_participant', {
                "path_or_buf": os.path.join(self.input_dir,
                                            "fight_participant.txt"),
                "sep": "|",
                "usecols": ['fight_id','participant_cd'],
                "encoding": 'latin-1',
                "quoting": QUOTE_ALL,
                #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            }),

            ('proponent_lookup', {
                "path_or_buf": os.path.join(self.input_dir,
                                            "proponent_lookup.txt"),
                "sep": "|",
                "usecols": ['core_activist_flg','iconum','id'],
                "encoding": 'latin-1',
                "quoting": QUOTE_ALL,
                #"na_values": ["NaN", "", "N/A", "NULL", "n/a", "nan", "null"]
            }),
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
        
        #Rounf Off float Precision values for marketcap 
        def Round6(Val):
            return Decimal(str(Val)).quantize(Decimal('.000000'),rounding=ROUND_HALF_UP)

        def headlines_fun(campaign_type_cd,announce_date,filing_date,proxy_fight_formal_notice_flg,proxy_fight_flg,title):       
            if (campaign_type_cd =='BDCONTROL') and (announce_date==filing_date) and \
                        proxy_fight_formal_notice_flg in (1,-1):
                inner_case= title+' - Proxy Fight For Board Control Value Announced' if proxy_fight_flg in (1,-1) else \
                                title+' - Activist Campaign For Board Control Value Announced'
                return inner_case
            if (campaign_type_cd =='BDREP') and (announce_date==filing_date) and \
                        proxy_fight_formal_notice_flg in (1,-1):
                inner_case= title+' - Proxy Fight For Board Representation Announced' if proxy_fight_flg in (1,-1) else \
                                title+' - Activist Campaign For Board Representation Announced'

                return inner_case
            if (campaign_type_cd  not in ('BDREP', 'BDCONTROL')) and (announce_date==filing_date) and \
                        proxy_fight_formal_notice_flg in (1,-1):

                inner_case= title+' - Proxy Fight Announced' if proxy_fight_flg in (1,-1) else \
                                title+'- Activist Campaign Announced'
                return inner_case

            if proxy_fight_flg==0 and campaign_type_cd =='MAXVALUE':
                return title+' - Activist Campaign to Maximize Shareholder Value Announced'
            if proxy_fight_flg==0 and campaign_type_cd =='WITHHOLD':
                return title+' - Activist Campaign to Withhold Vote for Directors Announced'
            if proxy_fight_flg==0 and campaign_type_cd =='VOTEFORPRO':
                return title+' - Activist Campaign to Vote For a Stockholder Proposal Announced'
            if proxy_fight_flg==0 and campaign_type_cd =='VOTENOMERGER':
                return title+' - Activisim Against a Merger Campaign Announced'
            if proxy_fight_flg==0 and campaign_type_cd =='REMOVEOFFICER':
                return title+' - Activist Campaign to Remove Officer Announced'
            if proxy_fight_flg==0 and campaign_type_cd =='VOTEFORMPRO':
                return title+' - Campaign to Vote For a Management Proposal/Support Announced'
            if proxy_fight_flg==0 and campaign_type_cd =='VOTENOMPRO':
                return title+' - Activist Campaign to Vote Against a Management Proposal Announced'
            if proxy_fight_flg==0 and campaign_type_cd =='BDREP':
                return title+' - Activist Campaign For Board Representation Announced'
            if proxy_fight_flg==0 and campaign_type_cd =='BDCONTROL':
                return title+' - Activist Campaign For Board Control Value Announced'
            if proxy_fight_flg==0 and campaign_type_cd =='REMOVEDIRECTOR':
                return title+' - Activist Campaign to Remove Directors Announced'
            if proxy_fight_flg==0 and campaign_type_cd =='ENHANCECG':
                return title+' - Activist Campaign to Enhance Corporate Governance Announced'
            if proxy_fight_flg==0 and campaign_type_cd =='VOTEFORDIS':
                return title+' - Campaign to Support Dissident Group in Proxy Fight Announced'
            if proxy_fight_flg==0 and campaign_type_cd =='ACQUISITION':
                return title+' - Unsolicited Acquisition Proposal Disclosed'
            if proxy_fight_flg==0 and campaign_type_cd =='PUBLICSHORT':
                return title+' - Public Short Campaign Announced'

        # merge all the left joines for one dataframe
        company = self.company.copy()
        fight = self.fight.copy()
        fight_source = self.fight_source.copy()
        SR_LookupFightCampaignType = self.SR_LookupFightCampaignType.copy()
        SR_LookupFilingType = self.SR_LookupFilingType.copy()
        company_identifier = self.company_identifier.copy()
        filermst = self.filermst.copy()
        sector = self.sector.copy()
        industries = self.industries.copy()
        company_sic = self.company_sic.copy()
        company_ticker = self.company_ticker.copy()
        fight_participant = self.fight_participant.copy()
        proponent_lookup = self.proponent_lookup.copy()

        # Strip all the LTRIM,RTRIM
        company.job_status = company.job_status.str.strip()
        fight.campaign_type_cd = fight.campaign_type_cd.str.strip()
        fight_source.source_type_cd = fight_source.source_type_cd.str.strip()
        fight_source.special_exhibit_cd = fight_source.special_exhibit_cd.str.strip()
        SR_LookupFightCampaignType.Code = SR_LookupFightCampaignType.Code.str.strip()
        SR_LookupFilingType.Code = SR_LookupFilingType.Code.str.strip()
        company_identifier.identifier = company_identifier.identifier.str.strip()
        sector.sector = sector.sector.str.strip()
        industries.industry = industries.industry.str.strip()
        company.rename(columns={'company_code':'company_code_c'},inplace=True)
        fight.rename(columns={'company_code':'company_code_f'},inplace=True)
        company.rename(columns={'company_code':'company_code_c'},inplace=True)
        fight.rename(columns={'company_code':'company_code_f'},inplace=True)
        fight.rename(columns={'id':'id_f'},inplace=True)

        # #res1 will create 4th df(three) = 13D/A
        res1 = company.merge(fight, how ='left',left_on=["company_code_c"],right_on=["company_code_f"]) 
        print('company,fight',res1.shape)

        res1 = res1.merge(fight_source, how ='left',left_on=["id_f"], right_on=["fight_id"]) 
        print('fight_src',res1.shape)
        res1= res1.merge(SR_LookupFightCampaignType, how ='left', left_on=["campaign_type_cd"], right_on=["Code"])
        print('SR_LookupFightCampaignType',res1.shape)
        # res1= res1.merge(SR_LookupFilingType, how ='left', left_on=["source_type_cd"], right_on=["Code"])
        # print('SR_LookupFilingType',res1.shape)\
        ###===>>>[company_identifier.identifier=='CUSIP']
        ##===>>res1= pd.merge(res1,company_identifier,left_on='company_code_f',right_on='company_code',how='left')
        ##===>print('company_identifier',res1.shape)
          
        # company_identifier2 = 
        res1= pd.merge(res1,company_identifier[company_identifier.identifier == 'CUSIP'],left_on='company_code_f',right_on='company_code',how='left')
        print('company_identifier',res1.shape)
        
        res1= res1.merge(filermst, how ='left', on=["iconum"])
        print('filermst',len(res1))
        res1= res1.merge(sector, how ='left', on=["sect_code"])
        print('sector',len(res1))
        res1= res1.merge(industries, how ='left', on=["ind_code"])
        print('industry',len(res1))

        
        

        res1= res1.merge(company_sic[company_sic.seq==1],how ='left', left_on=["company_code_f"],right_on=["company_code"])
        print('company_sic',len(res1))
        # company_ticker1 = company_ticker[company_ticker.primary_listing==-1]
        res1= res1.merge(company_ticker[company_ticker.primary_listing==-1], how ='left', left_on=["company_code_f"],right_on=["company_code"])
        print('company_ticker',len(res1))
        res1= res1.merge(fight_participant, how ='left', left_on=["id_f"], right_on=["fight_id"])
        res1= res1.merge(proponent_lookup, how ='left', left_on=["participant_cd"], right_on=["id"])
        print('proponent_lookup',res1.shape)  
        #where a.company_code = b.company_code
        res1=res1.merge(pd.DataFrame(fight['company_code_f'].drop_duplicates()),left_on=['company_code_c'],right_on=['company_code_f'],how='inner')
        print("where a.company_code = b.company_code",res1.shape)

        print('Afterr joining shapeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',res1.shape)
        res1.job_status = res1.job_status.fillna('WIP')
        
        res1 = res1[res1['job_status'].isin(["APR" ,"CHIP"])] 

        print('job_sts',len(res1))

        res1=res1[res1.campaign_type_cd.fillna('XXX').str.strip()!='13DFILER']
        print('13dFiler',len(res1))

        res1= res1.merge(SR_LookupFightCampaignType,left_on='campaign_type_cd',right_on='Code',how='inner')

        print('SR_LookupFightCampaignType',res1.shape)

        import datetime
        res1=res1[pd.to_datetime(res1.announce_date).dt.date >= (datetime.date.today() - datetime.timedelta(days=300))]

        print('Annouance_date',res1.shape)

        
        
        res1['fight_id'] = res1.id_f.astype('int')
        res1.drop(['fight_id_x','id_f','fight_id_y'], axis=1, inplace=True)
        date=pd.to_datetime(res1['announce_date']).dt.strftime('%Y%m%d').astype('str')
        fid=res1['fight_id'].astype('str')
        res1['story_id'] = date+fid
        res1['type'] = 'new'
        print('shapeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',res1.shape)

        # f = lambda x: 1 if x==True else 0
        # # res1.unsolicited_hostile_flg = res1.unsolicited_hostile_flg.apply(f)
        # # res1.proxy_fight_formal_notice_flg = res1.proxy_fight_formal_notice_flg.apply(f)
        # # res1.proxy_fight_flg = res1.proxy_fight_flg.apply(f)
        # # res1.publish_flg = res1.publish_flg.apply(f)
        # # res1.seat_granted_to_dissident_flg = res1.seat_granted_to_dissident_flg.apply(f)
        # res1.dissident_filing_flg = res1.dissident_filing_flg.apply(f)
        # #res1.contentious_13d_item_flg = res1.contentious_13d_item_flg.apply(f)

        # # headline = case
		# # 			when (IsNull(c.dissident_filing_flg,0) in (1, -1) and IsNull(LTRIM(RTRIM(c.source_type_cd)),'XXX') = '13D/A'  
		# # 			and LTRIM(RTRIM(c.special_exhibit_cd)) is null) 
		# # 				then concat('Update to ' , b.title , ' Activist Campaign - 13D/A Filed')
		# # 			end
        # def headline_4(dissident_filing_flg,source_type_cd,special_exhibit_cd,title):
        #     if (0 if pd.isnull(dissident_filing_flg) else dissident_filing_flg in (1,-1))\
        #             and ('XXX' if pd.isnull(source_type_cd) else source_type_cd == '13D/A') and pd.isnull(special_exhibit_cd):
        #         return 'Update to '+title+' Activist Campaign - 13D/A Filed'  

        # res1['headline']=res1[['dissident_filing_flg','source_type_cd','special_exhibit_cd','title']].apply(lambda x:headline_4(x['dissident_filing_flg'] ,x['source_type_cd'],x['special_exhibit_cd'],x['title']),axis=1 )   


        # def headlines_fun(campaign_type_cd,announce_date,filing_date,proxy_fight_formal_notice_flg,proxy_fight_flg,title):       
        #     if (campaign_type_cd =='BDCONTROL') and (announce_date==filing_date) and \
        #                 proxy_fight_formal_notice_flg in (1,-1):
        #         inner_case= title+' - Proxy Fight For Board Control Value Announced' if proxy_fight_flg in (1,-1) else \
        #                         title+' - Activist Campaign For Board Control Value Announced'
        #         return inner_case
        #     if (campaign_type_cd =='BDREP') and (announce_date==filing_date) and \
        #                 proxy_fight_formal_notice_flg in (1,-1):
        #         inner_case= title+' - Proxy Fight For Board Representation Announced' if proxy_fight_flg in (1,-1) else \
        #                         title+' - Activist Campaign For Board Representation Announced'

        #         return inner_case
        #     if (campaign_type_cd  not in ('BDREP', 'BDCONTROL')) and (announce_date==filing_date) and \
        #                 proxy_fight_formal_notice_flg in (1,-1):

        #         inner_case= title+' - Proxy Fight Announced' if proxy_fight_flg in (1,-1) else \
        #                         title+'- Activist Campaign Announced'
        #         return inner_case

        #     if proxy_fight_flg==0 and campaign_type_cd =='MAXVALUE':
        #         return title+' - Activist Campaign to Maximize Shareholder Value Announced'
        #     if proxy_fight_flg==0 and campaign_type_cd =='WITHHOLD':
        #         return title+' - Activist Campaign to Withhold Vote for Directors Announced'
        #     if proxy_fight_flg==0 and campaign_type_cd =='VOTEFORPRO':
        #         return title+' - Activist Campaign to Vote For a Stockholder Proposal Announced'
        #     if proxy_fight_flg==0 and campaign_type_cd =='VOTENOMERGER':
        #         return title+' - Activisim Against a Merger Campaign Announced'
        #     if proxy_fight_flg==0 and campaign_type_cd =='REMOVEOFFICER':
        #         return title+' - Activist Campaign to Remove Officer Announced'
        #     if proxy_fight_flg==0 and campaign_type_cd =='VOTEFORMPRO':
        #         return title+' - Campaign to Vote For a Management Proposal/Support Announced'
        #     if proxy_fight_flg==0 and campaign_type_cd =='VOTENOMPRO':
        #         return title+' - Activist Campaign to Vote Against a Management Proposal Announced'
        #     if proxy_fight_flg==0 and campaign_type_cd =='BDREP':
        #         return title+' - Activist Campaign For Board Representation Announced'
        #     if proxy_fight_flg==0 and campaign_type_cd =='BDCONTROL':
        #         return title+' - Activist Campaign For Board Control Value Announced'
        #     if proxy_fight_flg==0 and campaign_type_cd =='REMOVEDIRECTOR':
        #         return title+' - Activist Campaign to Remove Directors Announced'
        #     if proxy_fight_flg==0 and campaign_type_cd =='ENHANCECG':
        #         return title+' - Activist Campaign to Enhance Corporate Governance Announced'
        #     if proxy_fight_flg==0 and campaign_type_cd =='VOTEFORDIS':
        #         return title+' - Campaign to Support Dissident Group in Proxy Fight Announced'
        #     if proxy_fight_flg==0 and campaign_type_cd =='ACQUISITION':
        #         return title+' - Unsolicited Acquisition Proposal Disclosed'
        #     if proxy_fight_flg==0 and campaign_type_cd =='PUBLICSHORT':
        #         return title+' - Public Short Campaign Announced'


        res1['headline']=res1[['campaign_type_cd','announce_date','filing_date','proxy_fight_formal_notice_flg','proxy_fight_flg','title']].\
                            apply(lambda x:headlines_fun(x['campaign_type_cd'], x['announce_date'],x['filing_date'],\
                                                        x['proxy_fight_formal_notice_flg'],x['proxy_fight_flg'], x['title']),axis=1)
                                                                                                                                                                                                                                                           
        res1['headline_rank'] = 1                  
        res1['publication_date']= pd.to_datetime(res1['announce_date'],errors='coerce').dt.strftime('%Y-%m-%d')

        import datetime
        current_timestamp=datetime.datetime.now().strftime('%H%M%S')
        res1['publication_time']=pd.to_datetime(res1['announce_date'],errors='coerce').dt.strftime('%H%M%S').apply(lambda x:x if x==current_timestamp else '180000')

        # res1=pd.read_csv(r'D:\User\learladine\Sharknews\Final_pre_1.txt',sep='|')

        res1.to_csv(r'D:\User\learladine\Sharknews\Final_pre_1.txt',sep='|',index=False)
 
        print(res1.columns)

        res1['language'] = 'en'
        res1['cusip'] = res1.code
        res1['factset_id'] = res1['iconum_x']#.astype('int')
        res1['event_type'] = 'New Activist Campaigns'
        res1['fds_sector'] = res1['sect_code']#.astype('int')
        res1['fds_sector_name'] = res1['sector']
        res1['fds_industry'] = res1['ind_code']#.astype('int')
        res1['fds_industry_name'] = res1['industry']
        res1['jurisdiction_incorporated'] = res1['jurisdiction_incorporated'].fillna('United States')
        
        res1['sic_code'] = res1['sic_code']#.astype('int')

        print(res1.columns)
        
        import datetime
        res1['campaign_type'] = res1['Web_description_x']
        res1['campaign_summary'] = res1['fight_synopsis']
        res1['latest_development'] = res1['headline']
        res1['delivery_timestamp'] = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        res1['company_name'] = res1['company_name']
        res1['company_ticker'] = res1['ticker']
        res1['marketcap'] = res1.marketcap.apply(lambda x:Round6(x))
        res1['company_stock_exchange'] = res1['stock_exchange']
        res1['company_parent_name'] = res1['company_name']
        res1['company_parent_ticker'] = res1['ticker']
        res1['company_parent_stock_exchange'] = res1['stock_exchange']
        res1['company_parent_cusip'] = res1['code']
        res1['company_parent_factset_id'] = res1['iconum_x']#.astype('int')
        res1['sharkwatch_50_member'] = res1['core_activist_flg']
        res1['proponent_iconum'] = res1['iconum_y']#.astype('int')

        print('counttttttt before headline_is not null',res1.drop_duplicates().shape)
        
        res1.sort_values(by=['fight_id'], inplace=True)
        res1 = res1.loc[res1['headline'].notnull()]
        
        res1['headline'] = res1['headline'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res1['fds_sector_name'] = res1['fds_sector_name'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res1['fds_industry_name'] = res1['fds_industry_name'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res1['jurisdiction_incorporated'] = res1['jurisdiction_incorporated'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res1['market_cap']=res1['marketcap'].apply(lambda x: '<![CDATA[' + str(x) + ']]>' if not str(x) in ['NaN','nan'] else x)
        res1['campaign_type'] = res1['campaign_type'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        
        res1['latest_development'] = res1['latest_development'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res1['campaign_summary'] = res1['campaign_summary'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res1['company_name'] = res1['company_name'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')
        res1['company_parent_name'] = res1['company_parent_name'].apply(lambda x: '<![CDATA[' + str(x) + ']]>')

        # res1.to_csv(r'D:\User\learladine\Sharknews\pre_last.txt',sep='|',index=False)
        print(res1.columns)
        res1 = res1[['fight_id','story_id','type','headline' ,'publication_date','publication_time','language','cusip','factset_id','event_type'
            ,'fds_sector','fds_sector_name','fds_industry','fds_industry_name','jurisdiction_incorporated','market_cap','sic_code','campaign_type'
            ,'latest_development','campaign_summary','delivery_timestamp','company_name','company_ticker','company_stock_exchange'
            ,'company_parent_name','company_parent_ticker','company_parent_stock_exchange','company_parent_cusip','company_parent_factset_id'
            ,'sharkwatch_50_member','proponent_iconum','headline_rank']].drop_duplicates()
        
        print('finaaaaaaaaaaaaaaaaal counttttttttttt',res1.drop_duplicates().shape)
        res1.to_csv(r'D:\User\learladine\Sharknews\last_1.txt',sep='|',index=False)
       
        # # """   


        # # #self.temp_shark_news = res1.copy()
     

     
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        