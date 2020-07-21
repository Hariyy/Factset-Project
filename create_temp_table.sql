IF OBJECT_ID('Truecourse_Lion.dbo.temp_shark_news') IS NOT NULL
BEGIN
	DROP TABLE Truecourse_Lion.dbo.temp_shark_news
END

;WITH cte as
(
select distinct fight_id
,story_id
,type
,headline
,publication_date
,publication_time
,language
,cusip
,factset_id
,event_type
,fds_sector
,fds_sector_name
,fds_industry
,fds_industry_name
,jurisdiction_incorporated
,market_cap
,sic_code
,campaign_type
,latest_development = headline
,campaign_summary
,delivery_timestamp
,company_name
,company_ticker
,company_stock_exchange
,company_parent_name
,company_parent_ticker
,company_parent_stock_exchange
,company_parent_cusip
,company_parent_factset_id
,sharkwatch_50_member
,proponent_iconum
,headline_rank
,row_number() over (Partition by fight_id, publication_date order by headline_rank ASC) as fight_rank
from
(
-----------New Activist Campaigns query--------------
select distinct fight_id
,story_id
,type
,'<![CDATA[' + headline + ']]>' headline
,publication_date
,publication_time
,language
,cusip
,factset_id
,event_type
,fds_sector
,'<![CDATA[' + fds_sector_name + ']]>' fds_sector_name
,fds_industry
,'<![CDATA[' + fds_industry_name + ']]>' fds_industry_name
,'<![CDATA[' + jurisdiction_incorporated + ']]>' jurisdiction_incorporated
,'<![CDATA[' + market_cap + ']]>' market_cap
,sic_code
,'<![CDATA[' + campaign_type + ']]>' campaign_type
,latest_development = '<![CDATA[' + headline + ']]>'
,'<![CDATA[' + campaign_summary + ']]>' campaign_summary
,delivery_timestamp
,'<![CDATA[' + company_name + ']]>' company_name
,company_ticker
,company_stock_exchange
,'<![CDATA[' + company_parent_name + ']]>' company_parent_name
,company_parent_ticker
,company_parent_stock_exchange
,company_parent_cusip
,company_parent_factset_id
,sharkwatch_50_member
,proponent_iconum
,headline_rank
	from (
			select distinct fight_id = b.id
				,story_id = CONCAT(Convert(Varchar(25), b.announce_date, 112) , b.id)
				,type = 'new'
				,headline =
					case
					when (LTRIM(RTRIM(b.campaign_type_cd)) = 'BDCONTROL' and b.announce_date = j.filing_date and j.proxy_fight_formal_notice_flg in (1,-1) )
						then ( case when b.proxy_fight_flg in (1,-1)
								then CONCAT(b.title, ' - Proxy Fight For Board Control Value Announced') else CONCAT(b.title, ' - Activist Campaign For Board Control Value Announced') end
							 )
					when (LTRIM(RTRIM(b.campaign_type_cd)) = 'BDREP' and b.announce_date = j.filing_date and j.proxy_fight_formal_notice_flg in (1,-1) )
						then ( case when b.proxy_fight_flg in (1,-1)
								then CONCAT(b.title, ' - Proxy Fight For Board Representation Announced') else CONCAT(b.title, ' - Activist Campaign For Board Representation Announced') end
							 )
					when (LTRIM(RTRIM(b.campaign_type_cd)) not in ('BDREP', 'BDCONTROL') and b.announce_date = j.filing_date and j.proxy_fight_formal_notice_flg in (1,-1) )
						then ( case when b.proxy_fight_flg in (1,-1) 
								then CONCAT(b.title, ' - Proxy Fight Announced') else CONCAT(b.title, ' - Activist Campaign Announced') end
							 )
					when b.proxy_fight_flg = 0 and LTRIM(RTRIM(b.campaign_type_cd)) = 'MAXVALUE' then CONCAT(b.title, ' - Activist Campaign to Maximize Shareholder Value Announced')
					when b.proxy_fight_flg = 0 and LTRIM(RTRIM(b.campaign_type_cd)) = 'WITHHOLD' then CONCAT(b.title, ' - Activist Campaign to Withhold Vote for Directors Announced')
					when b.proxy_fight_flg = 0 and LTRIM(RTRIM(b.campaign_type_cd)) = 'VOTEFORPRO' then CONCAT(b.title, ' - Activist Campaign to Vote For a Stockholder Proposal Announced')
					when b.proxy_fight_flg = 0 and LTRIM(RTRIM(b.campaign_type_cd)) = 'VOTENOMERGER' then CONCAT(b.title, ' - Activisim Against a Merger Campaign Announced')
					when b.proxy_fight_flg = 0 and LTRIM(RTRIM(b.campaign_type_cd)) = 'REMOVEOFFICER' then CONCAT(b.title, ' - Activist Campaign to Remove Officer Announced')
					when b.proxy_fight_flg = 0 and LTRIM(RTRIM(b.campaign_type_cd)) = 'VOTEFORMPRO' then CONCAT(b.title, ' - Campaign to Vote For a Management Proposal/Support Announced')
					when b.proxy_fight_flg = 0 and LTRIM(RTRIM(b.campaign_type_cd)) = 'VOTENOMPRO' then CONCAT(b.title, ' - Activist Campaign to Vote Against a Management Proposal Announced')
					when b.proxy_fight_flg = 0 and LTRIM(RTRIM(b.campaign_type_cd)) = 'BDREP' then CONCAT(b.title, ' - Activist Campaign For Board Representation Announced')
					when b.proxy_fight_flg = 0 and LTRIM(RTRIM(b.campaign_type_cd)) = 'BDCONTROL' then CONCAT(b.title, ' - Activist Campaign For Board Control Value Announced')
					when b.proxy_fight_flg = 0 and LTRIM(RTRIM(b.campaign_type_cd)) = 'REMOVEDIRECTOR' then CONCAT(b.title, ' - Activist Campaign to Remove Directors Announced')
					when b.proxy_fight_flg = 0 and LTRIM(RTRIM(b.campaign_type_cd)) = 'ENHANCECG' then CONCAT(b.title, ' - Activist Campaign to Enhance Corporate Governance Announced')
					when b.proxy_fight_flg = 0 and LTRIM(RTRIM(b.campaign_type_cd)) = 'VOTEFORDIS' then CONCAT(b.title, ' - Campaign to Support Dissident Group in Proxy Fight Announced')
					when b.proxy_fight_flg = 0 and LTRIM(RTRIM(b.campaign_type_cd)) = 'ACQUISITION' then CONCAT(b.title, ' - Unsolicited Acquisition Proposal Disclosed')
					when b.proxy_fight_flg = 0 and LTRIM(RTRIM(b.campaign_type_cd)) = 'PUBLICSHORT' then CONCAT(b.title, ' - Public Short Campaign Announced')
					end
				,headline_rank = 1
				,publication_date = Convert(DATE, b.announce_date, 112)
				,publication_time = CASE
									WHEN Convert(VARCHAR(8), getdate(), 112) = Convert(VARCHAR(8), b.announce_date, 112)
										THEN REPLACE(CONVERT(VARCHAR, GETDATE(), 108), ':','')
									ELSE '180000'
									END
				,language = 'en'
				,cusip = c.code
				,factset_id = a.iconum
				,event_type = 'New Activist Campaigns'
				,fds_sector = g.sect_code
				,fds_sector_name = LTRIM(RTRIM(h.sector))
				,fds_industry = g.ind_code
				,fds_industry_name = LTRIM(RTRIM(i.industry))
				,jurisdiction_incorporated = CASE
											 WHEN a.jurisdiction_incorporated is NULL
												 THEN 'United States'
											 ELSE a.jurisdiction_incorporated
											 END
				,market_cap = cast(CAST(b.marketcap as decimal(16,6)) as varchar(25))
				,sic_code = d.sic_code
				,campaign_type = e.Description
				,campaign_summary = b.fight_synopsis
				,delivery_timestamp = CONVERT(VARCHAR(8), GETDATE(), 112) + REPLACE(CONVERT(varchar, GETDATE(), 108), ':','')
				,company_name = a.company_name
				,company_ticker = f.ticker
				,company_stock_exchange = f.stock_exchange
				,company_parent_name = a.company_name
				,company_parent_ticker = f.ticker
				,company_parent_stock_exchange = f.stock_exchange
				,company_parent_cusip = c.code
				,company_parent_factset_id = a.iconum
				,sharkwatch_50_member = l.core_activist_flg
				,proponent_iconum = l.iconum
				FROM company AS a (nolock)
				left join fight AS b (nolock) ON a.company_code = b.company_code
				left join company_identifier c (NOLOCK) ON c.company_code = b.company_code and LTRIM(RTRIM(identifier)) = 'CUSIP'
				left JOIN company_sic d (NOLOCK) ON d.company_code = b.company_code and d.seq = 1
				left join SR_LookupFightCampaignType e (nolock) on LTRIM(RTRIM(e.Code)) = LTRIM(RTRIM(b.campaign_type_cd))
				left join company_ticker f (NOLOCK) ON f.company_code = b.company_code and f.primary_listing = -1
				left join truecourse_lion.dbo.filermst g (nolock) on g.iconum = a.iconum
				left join truecourse_lion.dbo.sector h (nolock) on h.sect_code = g.sect_code
				left join truecourse_lion.dbo.industries i (nolock) on i.ind_code = g.ind_code
				left join fight_source AS j (nolock) on j.fight_id = b.id
				left join fight_participant k (nolock) on k.fight_id = b.id
				left join proponent_lookup l (nolock) on l.id = k.participant_cd
				where a.company_code = b.company_code
				and IsNull(LTRIM(RTRIM(a.job_status)),'WIP') in ('APR','CHIP')
				and IsNull(LTRIM(RTRIM(b.campaign_type_cd)), 'XXX') <> '13DFILER'
				and LTRIM(RTRIM(b.campaign_type_cd)) = LTRIM(RTRIM(e.code))
				and  b.announce_date >= DATEADD(day, -3, CONVERT (date, GETDATE()))
		) one
		where headline is NOT NULL

UNION

-----------Significant Developments--------------

select distinct fight_id
,story_id
,type
,'<![CDATA[' + headline + ']]>' headline
,publication_date
,publication_time
,language
,cusip
,factset_id
,event_type
,fds_sector
,'<![CDATA[' + fds_sector_name + ']]>' fds_sector_name
,fds_industry
,'<![CDATA[' + fds_industry_name + ']]>' fds_industry_name
,'<![CDATA[' + jurisdiction_incorporated + ']]>' jurisdiction_incorporated
,'<![CDATA[' + market_cap + ']]>' market_cap
,sic_code
,'<![CDATA[' + campaign_type + ']]>' campaign_type
,latest_development = '<![CDATA[' + latest_development + ']]>'
,'<![CDATA[' + campaign_summary + ']]>' campaign_summary
,delivery_timestamp
,'<![CDATA[' + company_name + ']]>' company_name
,company_ticker
,company_stock_exchange
,'<![CDATA[' + company_parent_name + ']]>' company_parent_name
,company_parent_ticker
,company_parent_stock_exchange
,company_parent_cusip
,company_parent_factset_id
,sharkwatch_50_member
,proponent_iconum
,headline_rank
	from (
			select distinct fight_id = b.id
				,story_id = CONCAT(Convert(Varchar(25), b.announce_date, 112) , b.id)
				,type = 'new'
				,headline =
					case
					when (IsNull(c.unsolicited_hostile_flg,0) in (1, -1) and b.announce_date > '2006-01-01 00:00:00.000' and c.filing_date >= DATEADD(day, -3, CONVERT (date, GETDATE()))) --Priority #2
						then CONCAT('Update to ' , b.title, ' Activist Campaign - ' , e.description , ' Discloses Unsolicited Offer')
					when (IsNull(c.proxy_fight_formal_notice_flg,0) in (1, -1)) --Priority #3
						then concat(b.title , ' Activist Campaign Escalated to Formal Proxy Fight')
					when (IsNull(LTRIM(RTRIM(c.special_exhibit_cd)),'XXX') = 'SETTLE') --Priority #4a
						then concat('Settlement Agreement Disclosed Ending ' , b.title ,
								CASE WHEN IsNull(b.proxy_fight_flg,0) in (1, -1) THEN ' Proxy Fight'
												ELSE ' Activist Campaign'
										END)
					when (IsNull(LTRIM(RTRIM(c.special_exhibit_cd)),'XXX') = 'STAND') --Priority #4b
						then concat('Standstill Agreement Disclosed Ending ' , b.title ,
								CASE WHEN IsNull(b.proxy_fight_flg,0) in (1, -1) THEN ' Proxy Fight'
												ELSE ' Activist Campaign'
										END)
					when (IsNull(c.publish_flg,0) in (1, -1) and IsNull(c.dissident_filing_flg,0) = 0) --Priority #5
						then concat(b.title , ' - Company Makes Material Announcement Regarding ' ,
								CASE WHEN IsNull(b.proxy_fight_flg,0) in (1, -1) THEN ' Proxy Fight'
										ELSE ' Activist Campaign'
								END)
					when  (IsNull(c.seat_granted_to_dissident_flg,0) in (1, -1)) --Priority #6
						then concat('Update to ' , b.title ,
								CASE WHEN IsNull(b.proxy_fight_flg,0) in (1, -1) THEN ' Proxy Fight - Board Representation Granted to Activist'
									ELSE ' Activist Campaign - Board Representation Granted to Activist'
								END)
					when (IsNull(c.dissident_filing_flg,0) in (1, -1) and IsNull(LTRIM(RTRIM(c.source_type_cd)),'XXX') in ('DEFC14A', 'DEFC14C', 'DEFN14A') and LTRIM(RTRIM(c.special_exhibit_cd)) is null) --Priority #7
						then concat('Dissident Definitive Contested Proxy Filed in ' , b.title , ' Proxy Fight')
					when (IsNull(c.dissident_filing_flg,0) in (1, -1) and IsNull(LTRIM(RTRIM(c.source_type_cd)),'XXX') in ('PREC14A', 'PREC14C', 'PREN14A') and LTRIM(RTRIM(c.special_exhibit_cd)) is null)  --Priority #8
						then concat('Dissident Preliminary Contested Proxy Filed in ' , b.title , ' Proxy Fight')
					when (IsNull(c.dissident_filing_flg,0) in (1, -1) and IsNull(LTRIM(RTRIM(c.special_exhibit_cd)),'XXX') = 'BOARD_LETTER')  --Priority #9
						then concat('Update to ' , b.title , ' Activist Campaign - ' , e.description , ' Discloses Letter to Board' )
					when (IsNull(c.dissident_filing_flg,0) in (1, -1) and IsNull(LTRIM(RTRIM(c.source_type_cd)),'XXX') = 'PR' and LTRIM(RTRIM(c.special_exhibit_cd)) is null) --Priority #10
						then concat('Update to ' , b.title , ' Campaign - New Activist Press Release Disclosed')
					when (IsNull(c.dissident_filing_flg,0) in (1, -1) and IsNull(LTRIM(RTRIM(c.source_type_cd)),'XXX') = '13D/A' and IsNull(c.contentious_13d_item_flg,0) in (1, -1) and LTRIM(RTRIM(c.special_exhibit_cd)) is null) --Priority #11
						then concat('Update to ' , b.title , ' Activist Campaign - Hostile 13D/A Filed')
					end
				,headline_rank =
					case
					when (IsNull(c.unsolicited_hostile_flg,0) in (1, -1) and b.announce_date > '2006-01-01 00:00:00.000' and c.filing_date >= DATEADD(day, -3, CONVERT (date, GETDATE()))) --Priority #2
						then 2
					when (IsNull(c.proxy_fight_formal_notice_flg,0) in (1, -1)) --Priority #3
						then 3
					when (IsNull(LTRIM(RTRIM(c.special_exhibit_cd)),'XXX') = 'SETTLE') --Priority #4a
						then 4
					when (IsNull(LTRIM(RTRIM(c.special_exhibit_cd)),'XXX') = 'STAND') --Priority #4b
						then 5
					when (IsNull(c.publish_flg,0) in (1, -1) and IsNull(c.dissident_filing_flg,0) = 0) --Priority #5
						then 6
					when  (IsNull(c.seat_granted_to_dissident_flg,0) in (1, -1)) --Priority #6
						then 7
					when (IsNull(c.dissident_filing_flg,0) in (1, -1) and IsNull(LTRIM(RTRIM(c.source_type_cd)),'XXX') in ('DEFC14A', 'DEFC14C', 'DEFN14A') and LTRIM(RTRIM(c.special_exhibit_cd)) is null) --Priority #7
						then 8
					when (IsNull(c.dissident_filing_flg,0) in (1, -1) and IsNull(LTRIM(RTRIM(c.source_type_cd)),'XXX') in ('PREC14A', 'PREC14C', 'PREN14A') and LTRIM(RTRIM(c.special_exhibit_cd)) is null)  --Priority #8
						then 9
					when (IsNull(c.dissident_filing_flg,0) in (1, -1) and IsNull(LTRIM(RTRIM(c.special_exhibit_cd)),'XXX') = 'BOARD_LETTER')  --Priority #9
						then 10
					when (IsNull(c.dissident_filing_flg,0) in (1, -1) and IsNull(LTRIM(RTRIM(c.source_type_cd)),'XXX') = 'PR' and LTRIM(RTRIM(c.special_exhibit_cd)) is null) --Priority #10
						then 11
					when (IsNull(c.dissident_filing_flg,0) in (1, -1) and IsNull(LTRIM(RTRIM(c.source_type_cd)),'XXX') = '13D/A' and IsNull(c.contentious_13d_item_flg,0) in (1, -1) and LTRIM(RTRIM(c.special_exhibit_cd)) is null) --Priority #11
						then 12
					end
				,publication_date = Convert(DATE, c.filing_date, 112)
				,publication_time = CASE
									WHEN Convert(VARCHAR(8), getdate(), 112) = Convert(VARCHAR(8), c.filing_date, 112)
										THEN REPLACE(CONVERT(VARCHAR, GETDATE(), 108), ':','')
									ELSE '180000'
									END
				,language = 'en'
				,cusip = f.code
				,factset_id = a.iconum
				,event_type = 'Significant Developments'
				,fds_sector = g.sect_code
				,fds_sector_name = ltrim(rtrim(h.sector))
				,fds_industry = g.ind_code
				,fds_industry_name = ltrim(rtrim(i.industry))
				,jurisdiction_incorporated = CASE
											 WHEN a.jurisdiction_incorporated is NULL
												 THEN 'United States'
											 ELSE a.jurisdiction_incorporated
											 END
				,market_cap = cast(CAST(b.marketcap as decimal(16,6)) as varchar(25))
				,sic_code = j.sic_code
				,campaign_type = d.description
				,latest_development = c.Description
				,campaign_summary = b.fight_synopsis
				,delivery_timestamp = CONVERT(VARCHAR(8), GETDATE(), 112) + REPLACE(CONVERT(varchar, GETDATE(), 108), ':','')
				,company_name = a.company_name
				,company_ticker = k.ticker
				,company_stock_exchange = k.stock_exchange
				,company_parent_name = a.company_name
				,company_parent_ticker = k.ticker
				,company_parent_stock_exchange = k.stock_exchange
				,company_parent_cusip = f.code
				,company_parent_factset_id = a.iconum
				,sharkwatch_50_member = m.core_activist_flg
				,proponent_iconum = m.iconum
				FROM company AS a (nolock)
				left join fight AS b (nolock) ON a.company_code = b.company_code
				left join fight_source AS c (nolock) on c.fight_id = b.id
				left join SR_LookupFightCampaignType AS d (nolock) on LTRIM(RTRIM(d.Code)) = LTRIM(RTRIM(b.campaign_type_cd))
				left join SR_LookupFilingType e (nolock) on LTRIM(RTRIM(e.code)) = LTRIM(RTRIM(c.source_type_cd))
				left join company_identifier f (NOLOCK) ON f.company_code = b.company_code and LTRIM(RTRIM(identifier)) = 'CUSIP'
				left join truecourse_lion.dbo.filermst g (nolock) on g.iconum = a.iconum
				left join truecourse_lion.dbo.sector h (nolock) on h.sect_code = g.sect_code
				left join truecourse_lion.dbo.industries i (nolock) on i.ind_code = g.ind_code
				left JOIN company_sic j (NOLOCK) ON j.company_code = b.company_code and j.seq = 1
				left join company_ticker k (NOLOCK) ON k.company_code = b.company_code and k.primary_listing = -1
				left join fight_participant l (nolock) on l.fight_id = b.id
				left join proponent_lookup m (nolock) on m.id = l.participant_cd
				where a.company_code = b.company_code
				AND b.id = c.fight_id
				AND IsNull(LTRIM(RTRIM(a.job_status)), 'WIP') IN (
					'APR'
					,'CHIP'
					)
				AND IsNull(LTRIM(RTRIM(b.campaign_type_cd)), 'XXX') <> '13DFILER'
				AND b.announce_date <> c.filing_date
				AND LTRIM(RTRIM(b.campaign_type_cd)) = LTRIM(RTRIM(d.code))
				AND b.announce_date > '2006-01-01 00:00:00.000'
				AND c.filing_date >= DATEADD(day, -3, CONVERT(DATE, GETDATE()))
		) two
		where headline is NOT NULL

UNION

-----------13D--------------
select distinct fight_id
,story_id
,type
,'<![CDATA[' + headline + ']]>' headline
,publication_date
,publication_time
,language
,cusip
,factset_id
,event_type
,fds_sector
,'<![CDATA[' + fds_sector_name + ']]>' fds_sector_name
,fds_industry
,'<![CDATA[' + fds_industry_name + ']]>' fds_industry_name
,'<![CDATA[' + jurisdiction_incorporated + ']]>' jurisdiction_incorporated
,'<![CDATA[' + market_cap + ']]>' market_cap
,sic_code
,'<![CDATA[' + campaign_type + ']]>' campaign_type
,latest_development = '<![CDATA[' + latest_development + ']]>'
,'<![CDATA[' + campaign_summary + ']]>' campaign_summary
,delivery_timestamp
,'<![CDATA[' + company_name + ']]>' company_name
,company_ticker
,company_stock_exchange
,'<![CDATA[' + company_parent_name + ']]>' company_parent_name
,company_parent_ticker
,company_parent_stock_exchange
,company_parent_cusip
,company_parent_factset_id
,sharkwatch_50_member
,proponent_iconum
,headline_rank
	from (
			select distinct fight_id = b.id
				,story_id = CONCAT(Convert(Varchar(25), b.announce_date, 112) , b.id)
				,type = 'new'
				,headline = case
						when (IsNull(c.dissident_filing_flg,0) in (1, -1) and IsNull(LTRIM(RTRIM(c.source_type_cd)),'XXX') = '13D' and IsNull(c.contentious_13d_item_flg,0) in (1, -1) and LTRIM(RTRIM(c.special_exhibit_cd)) is null) --Priority #12
						then concat('Update to ' , b.title , ' Activist Campaign - 13D Filed')						
					end
				,headline_rank = 1
				,publication_date = Convert(DATE, c.filing_date, 112)
				,publication_time = CASE
									WHEN Convert(VARCHAR(8), getdate(), 112) = Convert(VARCHAR(8), c.filing_date, 112)
										THEN REPLACE(CONVERT(VARCHAR, GETDATE(), 108), ':','')
									ELSE '180000'
									END
				,language = 'en'
				,cusip = f.code
				,factset_id = a.iconum
				,event_type = '13D'
				,fds_sector = g.sect_code
				,fds_sector_name = ltrim(rtrim(h.sector))
				,fds_industry = g.ind_code
				,fds_industry_name = ltrim(rtrim(i.industry))
				,jurisdiction_incorporated = CASE
											 WHEN a.jurisdiction_incorporated is NULL
												 THEN 'United States'
											 ELSE a.jurisdiction_incorporated
											 END
				,market_cap = cast(CAST(b.marketcap as decimal(16,6)) as varchar(25))
				,sic_code = j.sic_code
				,campaign_type = d.description
				,latest_development = c.Description
				,campaign_summary = b.fight_synopsis
				,delivery_timestamp = CONVERT(VARCHAR(8), GETDATE(), 112) + REPLACE(CONVERT(varchar, GETDATE(), 108), ':','')
				,company_name = a.company_name
				,company_ticker = k.ticker
				,company_stock_exchange = k.stock_exchange
				,company_parent_name = a.company_name
				,company_parent_ticker = k.ticker
				,company_parent_stock_exchange = k.stock_exchange
				,company_parent_cusip = f.code
				,company_parent_factset_id = a.iconum
				,sharkwatch_50_member = m.core_activist_flg
				,proponent_iconum = m.iconum
				FROM company AS a (nolock)
				left join fight AS b (nolock) ON a.company_code = b.company_code
				left join fight_source AS c (nolock) on c.fight_id = b.id
				left join SR_LookupFightCampaignType AS d (nolock) on LTRIM(RTRIM(d.Code)) = LTRIM(RTRIM(b.campaign_type_cd))
				left join SR_LookupFilingType e (nolock) on LTRIM(RTRIM(e.code)) = LTRIM(RTRIM(c.source_type_cd))
				left join company_identifier f (NOLOCK) ON f.company_code = b.company_code and LTRIM(RTRIM(identifier)) = 'CUSIP'
				left join truecourse_lion.dbo.filermst g (nolock) on g.iconum = a.iconum
				left join truecourse_lion.dbo.sector h (nolock) on h.sect_code = g.sect_code
				left join truecourse_lion.dbo.industries i (nolock) on i.ind_code = g.ind_code
				left JOIN company_sic j (NOLOCK) ON j.company_code = b.company_code and j.seq = 1
				left join company_ticker k (NOLOCK) ON k.company_code = b.company_code and k.primary_listing = -1
				left join fight_participant l (nolock) on l.fight_id = b.id
				left join proponent_lookup m (nolock) on m.id = l.participant_cd
				where a.company_code = b.company_code
				AND b.id = c.fight_id
				AND IsNull(LTRIM(RTRIM(a.job_status)), 'WIP') IN (
					'APR'
					,'CHIP'
					)
				AND IsNull(LTRIM(RTRIM(b.campaign_type_cd)), 'XXX') <> '13DFILER'
				AND b.announce_date <> c.filing_date
				AND LTRIM(RTRIM(b.campaign_type_cd)) = LTRIM(RTRIM(d.code))
				AND b.announce_date > '2006-01-01 00:00:00.000'
				AND c.filing_date >= DATEADD(day, -3, CONVERT(DATE, GETDATE()))
		) three
		where headline is NOT NULL

UNION

-----------13D/A--------------
		select distinct fight_id
,story_id
,type
,'<![CDATA[' + headline + ']]>' headline
,publication_date
,publication_time
,language
,cusip
,factset_id
,event_type
,fds_sector
,'<![CDATA[' + fds_sector_name + ']]>' fds_sector_name
,fds_industry
,'<![CDATA[' + fds_industry_name + ']]>' fds_industry_name
,'<![CDATA[' + jurisdiction_incorporated + ']]>' jurisdiction_incorporated
,'<![CDATA[' + market_cap + ']]>' market_cap
,sic_code
,'<![CDATA[' + campaign_type + ']]>' campaign_type
,latest_development = '<![CDATA[' + latest_development + ']]>'
,'<![CDATA[' + campaign_summary + ']]>' campaign_summary
,delivery_timestamp
,'<![CDATA[' + company_name + ']]>' company_name
,company_ticker
,company_stock_exchange
,'<![CDATA[' + company_parent_name + ']]>' company_parent_name
,company_parent_ticker
,company_parent_stock_exchange
,company_parent_cusip
,company_parent_factset_id
,sharkwatch_50_member
,proponent_iconum
,headline_rank
	from (
			select distinct fight_id = b.id
				,story_id = CONCAT(Convert(Varchar(25), b.announce_date, 112) , b.id)
				,type = 'new'
				,headline = case
					when (IsNull(c.dissident_filing_flg,0) in (1, -1) and IsNull(LTRIM(RTRIM(c.source_type_cd)),'XXX') = '13D/A'  
					and LTRIM(RTRIM(c.special_exhibit_cd)) is null) 
						then concat('Update to ' , b.title , ' Activist Campaign - 13D/A Filed')
					end
				,headline_rank = 1
				,publication_date = Convert(DATE, c.filing_date, 112)
				,publication_time = CASE
									WHEN Convert(VARCHAR(8), getdate(), 112) = Convert(VARCHAR(8), c.filing_date, 112)
										THEN REPLACE(CONVERT(VARCHAR, GETDATE(), 108), ':','')
									ELSE '180000'
									END
				,language = 'en'
				,cusip = f.code
				,factset_id = a.iconum
				,event_type = '13D/A'
				,fds_sector = g.sect_code
				,fds_sector_name = ltrim(rtrim(h.sector))
				,fds_industry = g.ind_code
				,fds_industry_name = ltrim(rtrim(i.industry))
				,jurisdiction_incorporated = CASE
											 WHEN a.jurisdiction_incorporated is NULL
												 THEN 'United States'
											 ELSE a.jurisdiction_incorporated
											 END
				,market_cap = cast(CAST(b.marketcap as decimal(16,6)) as varchar(25))
				,sic_code = j.sic_code
				,campaign_type = d.description
				,latest_development = c.Description
				,campaign_summary = b.fight_synopsis
				,delivery_timestamp = CONVERT(VARCHAR(8), GETDATE(), 112) + REPLACE(CONVERT(varchar, GETDATE(), 108), ':','')
				,company_name = a.company_name
				,company_ticker = k.ticker
				,company_stock_exchange = k.stock_exchange
				,company_parent_name = a.company_name
				,company_parent_ticker = k.ticker
				,company_parent_stock_exchange = k.stock_exchange
				,company_parent_cusip = f.code
				,company_parent_factset_id = a.iconum
				,sharkwatch_50_member = m.core_activist_flg
				,proponent_iconum = m.iconum
				FROM company AS a (nolock)
				left join fight AS b (nolock) ON a.company_code = b.company_code
				left join fight_source AS c (nolock) on c.fight_id = b.id
				left join SR_LookupFightCampaignType AS d (nolock) on LTRIM(RTRIM(d.Code)) = LTRIM(RTRIM(b.campaign_type_cd))
				left join SR_LookupFilingType e (nolock) on LTRIM(RTRIM(e.code)) = LTRIM(RTRIM(c.source_type_cd))
				left join company_identifier f (NOLOCK) ON f.company_code = b.company_code and LTRIM(RTRIM(identifier)) = 'CUSIP'
				left join truecourse_lion.dbo.filermst g (nolock) on g.iconum = a.iconum
				left join truecourse_lion.dbo.sector h (nolock) on h.sect_code = g.sect_code
				left join truecourse_lion.dbo.industries i (nolock) on i.ind_code = g.ind_code
				left JOIN company_sic j (NOLOCK) ON j.company_code = b.company_code and j.seq = 1
				left join company_ticker k (NOLOCK) ON k.company_code = b.company_code and k.primary_listing = -1
				left join fight_participant l (nolock) on l.fight_id = b.id
				left join proponent_lookup m (nolock) on m.id = l.participant_cd
				where a.company_code = b.company_code
				AND b.id = c.fight_id
				AND IsNull(LTRIM(RTRIM(a.job_status)), 'WIP') IN (
					'APR'
					,'CHIP'
					)
				AND IsNull(LTRIM(RTRIM(b.campaign_type_cd)), 'XXX') <> '13DFILER'
				AND b.announce_date <> c.filing_date
				AND LTRIM(RTRIM(b.campaign_type_cd)) = LTRIM(RTRIM(d.code))
				AND b.announce_date > '2006-01-01 00:00:00.000'
				AND c.filing_date >= DATEADD(day, -3, CONVERT(DATE, GETDATE()))
		) four
		where headline is NOT NULL
) temp_shark_news
)
select fight_id
,story_id
,type
,headline
,publication_date
,publication_time
,language
,cusip
,factset_id
,event_type
,fds_sector
,fds_sector_name
,fds_industry
,fds_industry_name
,jurisdiction_incorporated
,market_cap
,sic_code
,campaign_type
,latest_development = headline
,campaign_summary
,delivery_timestamp
,company_name
,company_ticker
,company_stock_exchange
,company_parent_name
,company_parent_ticker
,company_parent_stock_exchange
,company_parent_cusip
,company_parent_factset_id
,sharkwatch_50_member
,proponent_iconum
,headline_rank
,fight_rank
into Truecourse_Lion.dbo.temp_shark_news
FROM cte
where fight_rank = 1
ORDER BY fight_id ASC