from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import findspark
import pandas as pd
from datetime import datetime
import getpass
import seaborn as sns
import matplotlib.pyplot as plt
from io import BytesIO
from pyhive import hive
import getpass
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
import pyspark.sql.functions as f 
%matplotlib inline

default_args = {
    'owner': 'parth',
    'depends_on_past': False,
    'start_date': days_ago(2),
    #'email': ['airflow@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}

schema = 'ascend_lite'
lanid = "c51836a" #getpass.getuser()
unit = "rd"
project_name = "credittrends"
tag = lanid + "_" + unit + "_" + project_name + "_python"

user_master_table = f's99grp.{lanid}_{unit}_credit_trends_master'
neethu_master_table='s99grp.c51836a_ca_credit_trends_master'
partitioned_master_table='s99grp.ca_credit_trends_master_pr'
now = datetime.now()

aggregationquery=f"""
        WITH trades AS
          ( SELECT experian_consumer_key,
                   experian_trade_key AS experian_tip_key,
                   'T' AS record_ident,
                   base_ts,
                   special_comments,
                   date_open,
                   status_date,
                   maximum_delinquency_date,
                   type_code,
                   terms,
                   ecoa,
                   amount_1,
                   amount_1_qualifier,
                   amount_2,
                   amount_2_qualifier,
                   balance_date,
                   balance_amount,
                   status,
                   reserved,
                   amount_past_due,
                   rev_install,
                   30_day_counter,
                   60_day_counter,
                   90_day_counter,
                   derog_counter,
                   collateral,
                   b4_payment_profile,
                   monthly_payment_amount,
                   monthly_payment_type,
                   last_payment_date,
                   subcode,
                   kob,
                   consumer_dispute_flag,
                   maximum_payment_code,
                   first_delinquency_date,
                   second_delinquency_date,
                   initial_payment_level_date,
                   account_condition_code,
                   enhanced_payment_status,
                   enhanced_type_code,
                   enhanced_special_comment,
                   special_payment_code,
                   special_payment_date,
                   special_payment_amount,
                   actual_payment_amount,
                   terms_duration,
                   terms_frequency,
                   original_creditor_classification_code,
                   credit_limit_amount,
                   high_balance_amount,
                   original_loan_amount,
                   charge_off_amount,
                   secondary_agency_code,
                   compliance_condition_code,
                   cii_code,
                   j5_payment_profile,
                   y2k_date_open,
                   y2k_status_date,
                   y2k_maximum_delinquency_date,
                   y2k_balance_date,
                   y2k_last_payment_date,
                   y2k_first_delinquency_date,
                   y2k_second_delinquency_date,
                   y2k_initial_payment_level_date,
                   '' AS inquiry_date,
                   '' AS iq_amount,
                   '' AS iq_type,
                   '' AS iq_terms,
                   '' AS iq_subcode,
                   '' AS iq_kob,
                   '' AS y2k_inquiry_date,
                   '' AS pr_status,
                   '' AS evaluation,
                   '' AS pr_status_date,
                   '' AS pr_amount,
                   '' AS court_region_code,
                   '' AS legal_designator,
                   '' AS court_code,
                   '' AS pr_ecoa_code,
                   '' AS asset_amount,
                   '' AS liabilities_amount,
                   '' AS pr_y2k_status_date,
                   '' AS y2k_original_filing_date,
                   '' AS original_filing_date,
                   '' AS filler1,
                   '' AS filler2,
                   '' AS filler3
           FROM {schema}.trade
           WHERE base_ts ='{datadate}' ),
             java_input AS
          ( SELECT experian_consumer_key,
                   collect_list( concat_ws('@', cast(experian_consumer_key AS char(12)), cast(experian_tip_key AS char(12)), record_ident, cast(base_ts AS char(10)), nvl(special_comments, "null"), nvl(date_open, "null"), nvl(status_date, "null"), nvl(maximum_delinquency_date, "null"), nvl(type_code, "null"), nvl(terms, "null"), nvl(ecoa, "null"), nvl(amount_1, "null"), nvl(amount_1_qualifier, "null"), nvl(amount_2, "null"), nvl(amount_2_qualifier, "null"), nvl(balance_date, "null"), nvl(balance_amount, "null"), nvl(status, "null"), nvl(reserved, "null"), nvl(amount_past_due, "null"), nvl(rev_install, "null"), nvl(30_day_counter, "null"), nvl(60_day_counter, "null"), nvl(90_day_counter, "null"), nvl(derog_counter, "null"), nvl(collateral, "null"), nvl(b4_payment_profile, "null"), nvl(monthly_payment_amount, "null"), nvl(monthly_payment_type, "null"), nvl(last_payment_date, "null"), nvl(subcode, "null"), nvl(kob, "null"), nvl(consumer_dispute_flag, "null"), nvl(maximum_payment_code, "null"), nvl(first_delinquency_date, "null"), nvl(second_delinquency_date, "null"), nvl(initial_payment_level_date, "null"), nvl(account_condition_code, "null"), nvl(enhanced_payment_status, "null"), nvl(enhanced_type_code, "null"), nvl(enhanced_special_comment, "null"), nvl(special_payment_code, "null"), nvl(special_payment_date, "null"), nvl(special_payment_amount, "null"), nvl(actual_payment_amount, "null"), nvl(terms_duration, "null"), nvl(terms_frequency, "null"), nvl(original_creditor_classification_code, "null"), nvl(credit_limit_amount, "null"), nvl(high_balance_amount, "null"), nvl(original_loan_amount, "null"), nvl(charge_off_amount, "null"), nvl(secondary_agency_code, "null"), nvl(compliance_condition_code, "null"), nvl(cii_code, "null"), nvl(j5_payment_profile, "null"), nvl(y2k_date_open, "null"), nvl(y2k_status_date, "null"), nvl(y2k_maximum_delinquency_date, "null"), nvl(y2k_balance_date, "null"), nvl(y2k_last_payment_date, "null"), nvl(y2k_first_delinquency_date, "null"), nvl(y2k_second_delinquency_date, "null"), nvl(y2k_initial_payment_level_date, "null"), nvl(inquiry_date, "null"), nvl(iq_amount, "null"), nvl(iq_type, "null"), nvl(iq_terms, "null"), nvl(iq_subcode, "null"), nvl(iq_kob, "null"), nvl(y2k_inquiry_date, "null"), nvl(pr_status, "null"), nvl(evaluation, "null"), nvl(pr_status_date, "null"), nvl(pr_amount, "null"), nvl(court_region_code, "null"), nvl(legal_designator, "null"), nvl(court_code, "null"), nvl(pr_ecoa_code, "null"), nvl(asset_amount, "null"), nvl(liabilities_amount, "null"), nvl(pr_y2k_status_date, "null"), nvl(y2k_original_filing_date, "null"), nvl(original_filing_date, "null"), nvl(filler1, "null"), nvl(filler2, "null"), nvl(filler3, "null") ) ) AS concatdata
           FROM trades
           GROUP BY experian_consumer_key ),
             tip_view AS
          (SELECT P13_JAVAFilters(concatdata)
           FROM java_input),
             atg AS
          (SELECT experian_consumer_key AS exp_pin,
                  experian_trade_key AS tr_tin,
                  company_id
           FROM atg.tin_company
           WHERE base_ts ='{datadate}' ),
             vs4 AS
          (SELECT experian_consumer_key AS exp_pin,
                  vantage_v4_score,
                  CASE
                      WHEN VANTAGE_V4_SCORE >=781
                           AND VANTAGE_V4_SCORE <= 850 THEN '[781-850]Super prime'
                      WHEN VANTAGE_V4_SCORE >=661
                           AND VANTAGE_V4_SCORE <= 780 THEN '[661-780]Prime'
                      WHEN VANTAGE_V4_SCORE >=601
                           AND VANTAGE_V4_SCORE <= 660 THEN '[601-660]Near prime'
                      WHEN VANTAGE_V4_SCORE >=500
                           AND VANTAGE_V4_SCORE <= 600 THEN '[500-600]Sub-prime'
                      WHEN VANTAGE_V4_SCORE >=300
                           AND VANTAGE_V4_SCORE <= 499 THEN '[300-499]DeepSubprime'
                      WHEN VANTAGE_V4_SCORE =1 THEN '[1]Deceased'
                      WHEN VANTAGE_V4_SCORE =4 THEN '[4]Lack of info-INQ'
                      WHEN VANTAGE_V4_SCORE =9003 THEN '[9003]Too many rec'
                      WHEN VANTAGE_V4_SCORE IS NULL THEN 'MISSING'
                      ELSE 'NA'
                  END AS vs4band
           FROM {schema}.vantage_4
           WHERE base_ts ='{datadate}' ),
             random_consumers AS
          (SELECT experian_consumer_key AS exp_pin,
                  STATE,
                  gen_id
           FROM {schema}.consumer
           WHERE base_ts ='{datadate}'
             AND rand()<=0.1 ),
             merged_data AS
          ( SELECT tip_view.*,
                   atg.tr_tin,
                   company_id,
                   vantage_v4_score,
                   vs4band,
                   STATE,
                   gen_id
           FROM tip_view
           LEFT JOIN atg ON (tip_view.experian_tip_key = atg.tr_tin
                             AND atg.exp_pin = tip_view.experian_consumer_key)
           LEFT JOIN vs4 ON (tip_view.experian_consumer_key = vs4.exp_pin)
           LEFT JOIN random_consumers ON (tip_view.experian_consumer_key = random_consumers.exp_pin)),
             derived_trades AS
          (SELECT *,
                  CASE
                      WHEN UF21_JOINT = 1 THEN UF25_AMOUNT * 0.5
                      ELSE UF25_AMOUNT
                  END AS UF25_AMT_NEW,
                  CASE
                      WHEN uf20_months_open<=3 THEN "New[0-3]"
                      WHEN uf20_months_open<=6 THEN "New[4-6]"
                      ELSE "Existing"
                  END AS account_type,
                  CASE
                      WHEN record_ident="CO" THEN "Collection"
                      WHEN uftr_indcode IN ('FC',
                                            'NU') THEN "Credit Union"
                      WHEN company_id IN (550003,
                                          912471,
                                          852058,
                                          215,
                                          911353,
                                          997960,
                                          997421,
                                          551539,
                                          7507,
                                          789081,
                                          79011,
                                          997821,
                                          362527,
                                          91008,
                                          451126,
                                          361839) THEN "FinTech"
                      WHEN UF20_KOB1='B' THEN "Bank"
                      WHEN UFTR_INDCODE IN ('FA',
                                            'FF',
                                            'FP',
                                            'FU',
                                            'FZ',
                                            'NF') THEN "Finance"
                      WHEN UFTR_INDCODE IN ('AR',
                                            'AP',
                                            'AS',
                                            'AT',
                                            'MP',
                                            'ND',
                                            'QZ',
                                            'ZR',
                                            'PF')
                           OR UF20_KOB1 IN ('C',
                                            'D',
                                            'G',
                                            'H',
                                            'J',
                                            'L',
                                            'S',
                                            'T',
                                            'W') THEN "Retail"
                      WHEN UFTR_INDCODE IN ('FS',
                                            'NS',
                                            'FL') THEN "Savings and Loan"
                      ELSE "Other"
                  END AS lender_type,
                  CASE
                      WHEN uf25_aut=1 THEN "AUT"
                      WHEN uf25_aul=1 THEN "AUL"
                      WHEN uf25_mtf=1 THEN "MTF"
                      WHEN uf25_mts=1 THEN "MTS"
                      WHEN uf25_hlc=1 THEN "HLC"
                      WHEN uf25_bca=1 THEN "BCA"
                      WHEN uf25_rta=1 THEN "RTA"
                      WHEN uf25_upl=1 THEN "UPL"
                      WHEN uftr_enhtype IN ('02',
                                            '22',
                                            '23',
                                            '11',
                                            '17',
                                            '0F',
                                            '66',
                                            '68') THEN "SPL"
                      WHEN uf25_stu=1
                           AND uf25_defer=0 THEN "STU"
                      WHEN uf25_stu=1
                           AND uf25_defer=1 THEN "STU Deferred"
                  END AS portfolio_type,
                  CASE
                      WHEN uf25_deceased=1 THEN "Deceased"
                      WHEN uf25_defer=1 THEN "Deferred"
                      WHEN uf23_forbear=1 THEN "Forbearance"
                      WHEN uf25_neutral=1 THEN "Neutral"
                      WHEN uf25_indeterminate=1 THEN "Indeterminate"
                      WHEN uf25_open=1 THEN "Open"
                      WHEN uf25_open2=1 THEN "Open2"
                      WHEN uf25_closed=1
                           AND uf24_col=0 THEN "Closed"
                      WHEN uf25_closed=1
                           AND uf24_col=1 THEN "Collection"
                  END AS account_status
           FROM merged_data
           WHERE uftr_ecoa NOT IN ('C',
                                   '3')
             OR ufco_ecoa NOT IN ('C',
                                  '3')),

        aggregated_data as (
            SELECT to_date(from_unixtime(unix_timestamp(uf20_profile_date, 'MMddyyyy'))) AS uf20_profile_date,
                   from_unixtime(unix_timestamp(uf20_profile_date,'MMddyyyy'), 'MMMyyyy') AS archive_ts,
                   company_id,
                   STATE,
                   gen_id,
                   Lender_type,
                   Portfolio_type,
                   Account_type,
                   account_status,
                   Vs4band,
                   count(tr_tin) AS raw_cnt,
                   sum(CASE
                           WHEN vantage_v4_score>=300
                                AND vantage_v4_score<=850 THEN 1
                           ELSE 0
                       END) AS vscr4_cnt,
                   sum(CASE
                           WHEN UF21_JOINT = 1 THEN 0.5
                           ELSE 1
                       END) AS CNT_Wgt_Joint,
                   sum(CASE
                           WHEN UF25_present_status = 30 THEN 1
                           ELSE 0
                       END) AS CNT_Raw_300DPD,
                   sum(CASE
                           WHEN UF25_present_status = 60 THEN 1
                           ELSE 0
                       END) AS CNT_Raw_60DPD,
                   sum(CASE
                           WHEN UF25_present_status IN (90, 120) THEN 1
                           ELSE 0
                       END) AS CNT_Raw_90_180DPD,
                   sum(CASE
                           WHEN UF25_present_status = 30
                                AND UF21_JOINT = 1 THEN 0.5
                           WHEN UF25_present_status = 30
                                AND UF21_JOINT = 0 THEN 1
                           ELSE 0
                       END) AS CNT_Wgt_30DPD,
                   sum(CASE
                           WHEN UF25_present_status = 60
                                AND UF21_JOINT = 1 THEN 0.5
                           WHEN UF25_present_status = 60
                                AND UF21_JOINT = 0 THEN 1
                           ELSE 0
                       END) AS CNT_Wgt_60DPD,
                   sum(CASE
                           WHEN uf25_present_status IN (90, 120)
                                AND UF21_JOINT = 1 THEN 0.5
                           WHEN uf25_present_status IN (90, 120)
                                AND UF21_JOINT = 0 THEN 1
                           ELSE 0
                       END) AS CNT_Wgt_90_180DPD,
                   sum(uf25_balance_jnt) AS f25bal_sum,
                   Sum(UF25_AMT_NEW) AS f25amt_sum,
                   sum(CASE
                           WHEN uf25_present_status = 30 THEN UF25_BALANCE
                           ELSE 0
                       END) AS BAL_30DPD,
                   sum(CASE
                           WHEN uf25_present_status = 60 THEN UF25_BALANCE
                           ELSE 0
                       END) AS BAL_60DPD,
                   sum(CASE
                           WHEN uf25_present_status IN (90, 120) THEN UF25_BALANCE
                           ELSE 0
                       END) AS BAL_90_180DPD,
                   sum(CASE
                           WHEN vantage_v4_score>=300
                                AND vantage_v4_score<=850 THEN vantage_v4_score
                           ELSE NULL
                       END) AS vscr4_sum,
                   Sum(UF25_BTL) AS btl_sum,
                   Sum(UF25_BTL2) AS btl2_sum
            FROM derived_trades
            GROUP BY uf20_profile_date,
                     company_id,
                     STATE,
                     gen_id,
                     Lender_type,
                     Portfolio_type,
                     Account_type,
                     account_status,
                     Vs4band)
 
        select uf20_profile_date,
        company_id, state, gen_id,
        Lender_type,
        Portfolio_type,
        Account_type, account_status,
        Vs4band,raw_cnt,
         vscr4_cnt,
         CNT_Wgt_Joint,
         CNT_Raw_300DPD ,
         CNT_Raw_60DPD ,
         CNT_Raw_90_180DPD ,
         CNT_Wgt_30DPD ,
         CNT_Wgt_60DPD  ,
         CNT_Wgt_90_180DPD  ,
         f25bal_sum ,
         f25amt_sum ,
         BAL_30DPD ,
         BAL_60DPD ,
         BAL_90_180DPD ,
         vscr4_sum ,
         btl_sum ,
         btl2_sum,
         archive_ts
          from aggregated_data

        """
        
DelinquencySQL = f"""
                        with bandtotals as
                        (
                        select UF20_Profile_date, portfolio_type, vs4band, sum(f25bal_sum) as bandbalance
                        from {master_table}
                        where account_type = 'Existing'
                        and account_status in ('Open', 'Forbearance', 'Deferred')
                        and vs4band not in ('[1]Deceased', '[4]Lack of info-INQ')
                        group by UF20_Profile_date, portfolio_type, vs4band
                        having UF20_Profile_date = '{datadate}'
                        and portfolio_type in ('MTF', 'AUT', 'UPL', 'BCA')
                        ),
                        producttotals as
                        (
                        select UF20_Profile_date, portfolio_type, sum(bandbalance) as productbalance
                        from bandtotals
                        group by UF20_Profile_date, portfolio_type
                        )
                        select bandtotals.portfolio_type, bandtotals.vs4band, bandbalance / productbalance as bandpercent
                        from bandtotals, producttotals
                        where bandtotals.UF20_Profile_date = producttotals.UF20_Profile_date
                        and bandtotals.portfolio_type = producttotals.portfolio_type
                        order by portfolio_type, vs4band"""

def VantageDistroExcelChart(writer, sheet_name, tableshape):
    # Create an Excel chart object.
    chart = writer.book.add_chart({'type': 'column', 'subtype': 'percent_stacked'})

    # Set the chart series in Excel
    num_products=tableshape[1]+1
    num_scorebands = tableshape[0] + 1
    pastels = ['#1d4f91', '#426da9', '#af1685', '#6d2077']
    for band in range(1, num_scorebands): 
        chart.add_series({
            'name':       [sheet_name, band, 0],
            'categories': [sheet_name, 0, 1, 0, num_products],
            'values':     [sheet_name, band, 1, band, num_products-1],
            'data_labels': {'value': True,  'font': {'name':'Arial', 'color': 'white', 'size': 10, 'bold':True}},
            'fill':       {'color': pastels[band - 1]},
            'gap':        150,
            'overlap': 100
        })
    chart.set_plotarea({'fill': {'color': '#f2f2f2'}})
    
    chart.set_title({'name':f'Distribution of balances by VantageScore (by product, latest quarter)\n',
                    'name_font': {'name':'Arial', 'color': '#63666a', 'size': 16, 'bold':True}})
    chart.set_legend({'position':'bottom',
                     'font': {'name':'Arial', 'color': '#63666a', 'size': 14, 'bold':True}})
    chart.set_size({'width':  12.78 * 96, 'height': 5.07 * 96})
    chart.set_x_axis({
            'line': {'none': True},
            'num_font': {'name':'Arial', 'color': '#63666a', 'size': 12, 'bold':True},
    })
    chart.set_y_axis({
        'line': {'none': True},
        'num_font': {'name':'Arial', 'color': '#63666a', 'size': 12, 'bold':True},
        'major_gridlines': {'visible': True, 'line': {'width': 0.75, 'color': '#EEECE1'}},
        'major_unit':0.2,
        'minor_unit': 0.04
    })
    return chart

def VantageDistroWorksheet (pdBalanceByVantage, writer, sheet):
    # Pivot the data so it is in the shape required for an Excel chart
    spreadsheet=pdBalanceByVantage.pivot( 'vs4band', 'portfolio_type','bandpercent').sort_index(ascending=False)
    spreadsheet.index = ['Super Prime', 'Prime', 'Near Prime', 'Subprime', 'Deep Subprime']
    spreadsheet.columns=['Auto', 'Bank Card', 'Mortgage', 'Unsecured PL']
    spreadsheet.drop ('Deep Subprime', inplace=True) # do not plot deep subprimes
    spreadsheet=spreadsheet.reindex(columns=['Mortgage', 'Auto', 'Unsecured PL', 'Bank Card'])
    with pd.option_context('display.float_format', '{:.2%}'.format):
        print(spreadsheet)
    
    # First write the data to Excel
    spreadsheet.to_excel(writer, sheet_name=sheet) 
    # Set the column width and format.
    worksheet=writer.sheets[sheet]
    worksheet.set_column('A:E', 12, percentformat) # format the numbers and set the column width
    worksheet.insert_chart('A7', VantageDistroExcelChart(writer, sheet, spreadsheet.shape)) # add the Excel chart
    
def OrigLimitsExcelChart(writer, sheet_name, tableshape, band, pdtype, line_color, min_val, y_units):
    # Create an Excel chart object.
    chart = writer.book.add_chart({'type': 'line'})
    # Set the chart series in Excel
    num_products=tableshape[1]+1
    num_scorebands = tableshape[0] + 1
    # Configure the first series.
    if len(line_color)>1:
        i=0
        for val in band: 
            chart.add_series({
                'name':       [sheet_name, 0, val],
                'categories': [sheet_name, 1, 5, num_scorebands-1, 5],
                'values':     [sheet_name, 1, val, num_scorebands-1, val],
                'line':       {'width': 3, 'color': line_color[i]},
            })
            i+=1
    else:
        chart.add_series({
                'name':       [sheet_name, 0, band],
                'categories': [sheet_name, 1, 5, num_scorebands-1, 5],
                'values':     [sheet_name, 1, band, num_scorebands-1, band],
                'line':       {'width': 3, 'color': line_color[0]},
            })
    
    chart.set_chartarea({'fill': {'color': '#dfe0e2'}})
    chart.set_plotarea({'fill': {'color': '#f2f2f2'}})
    
    chart.set_title({'name':f'Origination limits (in $BNs)',
                    'name_font': {'name':'Arial', 'color': '#1d4f91', 'size': 12, 'bold':True}})
    chart.set_legend({
        'position' : 'overlay_left',
        'layout':{
            'x': 0.05,
            'y': 0.25,
            'width':  0.2,
            'height': 0.1,
        }
    })
    chart.set_size({'width':  9.33 * 96, 'height': 2.21 * 96})
    chart.set_x_axis({
            'num_font':  {'color': '#63666a'}
        })

    chart.set_y_axis({
        'display_units': 'billions',
        'display_units_visible': False,
        'major_unit': y_units,
        'min':min_val,
        'num_format': '$#,##0 "B"',
        'num_font':  {'color': '#63666a'},
        'major_gridlines': {'visible': True, 'line': {'width': 0.75, 'color': '#EEECE1'}}
    })
    
    return chart

def OrigLimitsWorksheet (pdByOriglimits, writer, sheet):
    # Pivot the data so it is in the shape required for an Excel chart
    spreadsheet = pdByOriglimits
    spreadsheet = spreadsheet.set_index('Month of Uf20 Profile Date')
    del spreadsheet.index.name
    for col in spreadsheet.columns:
        spreadsheet[col] = spreadsheet[col].astype(float)
        spreadsheet[col] = spreadsheet[col]
    spreadsheet['AUT'] = spreadsheet['AUT'] + spreadsheet['AUL']
    spreadsheet = spreadsheet.drop(columns=['AUL'])
    spreadsheet.columns=['Auto', 'Bank Card', 'Mortgage', 'Unsecured PL']
    spreadsheet['Label'] = ""
    for i in range(len(spreadsheet.index)):
        if 'Jan' in spreadsheet.index[i]:
            spreadsheet.at[spreadsheet.index[i],'Label'] = spreadsheet.index[i].split('-')[1]
    with pd.option_context('display.float_format', '{:.2f}'.format):
        print(spreadsheet.tail())

    # First write the data to Excel
    spreadsheet.to_excel(writer, sheet_name=sheet) 
    # Set the column width and format.
    worksheet=writer.sheets[sheet]
    worksheet.set_column('B:E', 16, dollarformat) # format the numbers and set the column width
    worksheet.set_column('V:V', 20)
    worksheet.write('V2', str(spreadsheet.index[-1]), origlim_header)
    worksheet.write('W2', 'YoY', origlim_header)
    worksheet.write('X2', 'MoM', origlim_header)
    
    worksheet.write('V3', 'Mortgage', origlim3_txt)
    worksheet.write('W3', (spreadsheet['Mortgage'].iloc[-1]-spreadsheet['Mortgage'].iloc[-13])/spreadsheet['Mortgage'].iloc[-13], origlim3_num)
    worksheet.write('X3', (spreadsheet['Mortgage'].iloc[-1]-spreadsheet['Mortgage'].iloc[-2])/spreadsheet['Mortgage'].iloc[-2], origlim3_num)
    worksheet.write('V4', 'Auto', origlim1_txt)
    worksheet.write('W4', (spreadsheet['Auto'].iloc[-1]-spreadsheet['Auto'].iloc[-13])/spreadsheet['Auto'].iloc[-13], origlim1_num)
    worksheet.write('X4', (spreadsheet['Auto'].iloc[-1]-spreadsheet['Auto'].iloc[-2])/spreadsheet['Auto'].iloc[-2], origlim1_num)
    worksheet.write('V5', 'Bank Card', origlim2_txt)
    worksheet.write('W5', (spreadsheet['Bank Card'].iloc[-1]-spreadsheet['Bank Card'].iloc[-13])/spreadsheet['Bank Card'].iloc[-13], origlim2_num)
    worksheet.write('X5', (spreadsheet['Bank Card'].iloc[-1]-spreadsheet['Bank Card'].iloc[-2])/spreadsheet['Bank Card'].iloc[-2], origlim2_num)
    worksheet.write('V6', 'Unsecured PL', origlim4_txt)
    worksheet.write('W6', (spreadsheet['Unsecured PL'].iloc[-1]-spreadsheet['Unsecured PL'].iloc[-13])/spreadsheet['Unsecured PL'].iloc[-13], origlim4_num)
    worksheet.write('X6', (spreadsheet['Unsecured PL'].iloc[-1]-spreadsheet['Unsecured PL'].iloc[-2])/spreadsheet['Unsecured PL'].iloc[-2], origlim4_num)
    
    worksheet.insert_chart('H2', OrigLimitsExcelChart(writer, sheet, spreadsheet.shape, 3, 'Mortgage', ['#0081A6'], 50*(10**9), 100*(10**9))) # add the Excel chart
    worksheet.insert_chart('H13', OrigLimitsExcelChart(writer, sheet, spreadsheet.shape, [1,2], 'Auto & Bank Card', ['#1d4f91','#6d2077'], 10*(10**9), 10*(10**9))) # add the Excel chart
    worksheet.insert_chart('H24', OrigLimitsExcelChart(writer, sheet, spreadsheet.shape, 4, 'Unsecured PL', ['#e63888'], 0, 2*(10**9))) # add the Excel chart    
    
def DelinquencyExcelChart(writer, sheet_name, tableshape, offset, prod, y_units):
    # Create an Excel chart object.
    chart = writer.book.add_chart({'type': 'line'})
    # Set the chart series in Excel
    num_products=tableshape[1]+1
    num_scorebands = tableshape[0] + 2
    # Configure the first series.
    line_col = ['#1d4f91', '#6d2077', '#e63888']
    i=0
    for band in range(1+offset, 4+offset): 
        chart.add_series({
            'name':       [sheet_name, 1, band],
            'categories': [sheet_name, 2, 0, num_scorebands-1, 0],
            'values':     [sheet_name, 2, band, num_scorebands-1, band],
            'line':       {'width': 4.50, 'color': line_col[i]},
        }) 
        i+=1
    chart.set_plotarea({'fill': {'color': '#f2f2f2'}})
    chart.set_title({'name': prod,
                     'name_font': {'name':'Arial', 'color': '#1d4f91', 'size': 12, 'bold':True},
                     'layout': {
                         'x': 0.05,
                         'y': 0.02,
                     }
                    })
    chart.set_legend({'none':True})
    chart.set_size({'width':  12.78 * 72, 'height': 5.07 * 72})
    chart.set_y_axis({'num_font':  {'color': '#63666a'},
                      'major_unit': y_units,
                      'major_gridlines': {'visible': True, 'line': {'width': 0.75, 'color': '#9ea0a1'}}})
    chart.set_x_axis({'num_font':  {'rotation': -45, 'color': '#63666a'}})
    
    return chart

def DelinquencyWorksheet (pdDqtrends, writer, sheet):
    # Pivot the data so it is in the shape required for an Excel chart
    spreadsheet_AUT = pdDqtrends[pdDqtrends['portfolio_type']=='AUT'].pivot('uf20_profile_date', 'Measure Names','Measure Values')
    spreadsheet_BCA = pdDqtrends[pdDqtrends['portfolio_type']=='BCA'].pivot('uf20_profile_date', 'Measure Names','Measure Values')
    spreadsheet_MTF = pdDqtrends[pdDqtrends['portfolio_type']=='MTF'].pivot('uf20_profile_date', 'Measure Names','Measure Values')
    spreadsheet_UPL = pdDqtrends[pdDqtrends['portfolio_type']=='UPL'].pivot('uf20_profile_date', 'Measure Names','Measure Values')
    spreadsheet_AUT.index = pd.to_datetime(spreadsheet_AUT.index)
    spreadsheet_AUT.index= spreadsheet_AUT.index.strftime('%b-%y')
    spreadsheet_BCA.index = pd.to_datetime(spreadsheet_BCA.index)
    spreadsheet_BCA.index= spreadsheet_BCA.index.strftime('%b-%y')
    spreadsheet_MTF.index = pd.to_datetime(spreadsheet_MTF.index)
    spreadsheet_MTF.index= spreadsheet_MTF.index.strftime('%b-%y')
    spreadsheet_UPL.index = pd.to_datetime(spreadsheet_UPL.index)
    spreadsheet_UPL.index= spreadsheet_UPL.index.strftime('%b-%y')
    
    spreadsheet = pd.concat([spreadsheet_AUT, spreadsheet_BCA, spreadsheet_MTF, spreadsheet_UPL], axis=1)
    with pd.option_context('display.float_format', '{:.2%}'.format):
        print(spreadsheet)

    # First write the data to Excel
    spreadsheet.to_excel(writer, sheet_name=sheet, startrow=1) 
    # Set the column width and format.
    worksheet=writer.sheets[sheet]
    worksheet.set_column('B:M', 14, percentformat) # format the numbers and set the column width
    worksheet.write('B1', 'Auto', bold)
    worksheet.write('E1', 'Bank Card', bold)
    worksheet.write('H1', 'Mortgage', bold)
    worksheet.write('K1', 'Unsecured PL', bold)
    worksheet.insert_chart('O2', DelinquencyExcelChart(writer, sheet, spreadsheet.shape, 0, 'Automotive', 0.01)) # add the Excel chart
    worksheet.insert_image('O2', 'images/automotive.png', {'x_offset': 5, 'y_offset': 5, 'x_scale': 0.8, 'y_scale': 0.8})
    worksheet.write('AD2', 'YoY', bold)
    worksheet.write('AD3', 'MoM', bold)
    worksheet.write('AE2', (spreadsheet.iloc[-1,[0]]-spreadsheet.iloc[-13,[0]])/spreadsheet.iloc[-13,[0]], cols30_f)
    worksheet.write('AF2', (spreadsheet.iloc[-1,[1]]-spreadsheet.iloc[-13,[1]])/spreadsheet.iloc[-13,[1]], cols60_f)
    worksheet.write('AG2', (spreadsheet.iloc[-1,[2]]-spreadsheet.iloc[-13,[2]])/spreadsheet.iloc[-13,[2]], cols90_f)
    worksheet.write('AE3', (spreadsheet.iloc[-1,[0]]-spreadsheet.iloc[-2,[0]])/spreadsheet.iloc[-2,[0]], cols30_f)
    worksheet.write('AF3', (spreadsheet.iloc[-1,[1]]-spreadsheet.iloc[-2,[1]])/spreadsheet.iloc[-2,[1]], cols60_f)
    worksheet.write('AG3', (spreadsheet.iloc[-1,[2]]-spreadsheet.iloc[-2,[2]])/spreadsheet.iloc[-2,[2]], cols90_f)
    
    worksheet.insert_chart('O21', DelinquencyExcelChart(writer, sheet, spreadsheet.shape, 3, 'Bank Card', 0.002))
    worksheet.insert_image('O21', 'images/bankcard.png', {'x_offset': 5, 'y_offset': 5, 'x_scale': 0.8, 'y_scale': 0.8})
    worksheet.write('AD21', 'YoY', bold)
    worksheet.write('AD22', 'MoM', bold)
    worksheet.write('AE21', (spreadsheet.iloc[-1,[3]]-spreadsheet.iloc[-13,[3]])/spreadsheet.iloc[-13,[3]], cols30_f)
    worksheet.write('AF21', (spreadsheet.iloc[-1,[4]]-spreadsheet.iloc[-13,[4]])/spreadsheet.iloc[-13,[4]], cols60_f)
    worksheet.write('AG21', (spreadsheet.iloc[-1,[5]]-spreadsheet.iloc[-13,[5]])/spreadsheet.iloc[-13,[5]], cols90_f)
    worksheet.write('AE22', (spreadsheet.iloc[-1,[3]]-spreadsheet.iloc[-2,[3]])/spreadsheet.iloc[-2,[3]], cols30_f)
    worksheet.write('AF22', (spreadsheet.iloc[-1,[4]]-spreadsheet.iloc[-2,[4]])/spreadsheet.iloc[-2,[4]], cols60_f)
    worksheet.write('AG22', (spreadsheet.iloc[-1,[5]]-spreadsheet.iloc[-2,[5]])/spreadsheet.iloc[-2,[5]], cols90_f)
    
    worksheet.insert_chart('O40', DelinquencyExcelChart(writer, sheet, spreadsheet.shape, 6, 'Mortgage', 0.005))
    worksheet.insert_image('O40', 'images/mortgage.png', {'x_offset': 5, 'y_offset': 5, 'x_scale': 0.8, 'y_scale': 0.8})
    worksheet.write('AD40', 'YoY', bold)
    worksheet.write('AD41', 'MoM', bold)
    worksheet.write('AE40', (spreadsheet.iloc[-1,[6]]-spreadsheet.iloc[-13,[6]])/spreadsheet.iloc[-13,[6]], cols30_f)
    worksheet.write('AF40', (spreadsheet.iloc[-1,[7]]-spreadsheet.iloc[-13,[7]])/spreadsheet.iloc[-13,[7]], cols60_f)
    worksheet.write('AG40', (spreadsheet.iloc[-1,[8]]-spreadsheet.iloc[-13,[8]])/spreadsheet.iloc[-13,[8]], cols90_f)
    worksheet.write('AE41', (spreadsheet.iloc[-1,[6]]-spreadsheet.iloc[-2,[6]])/spreadsheet.iloc[-2,[6]], cols30_f)
    worksheet.write('AF41', (spreadsheet.iloc[-1,[7]]-spreadsheet.iloc[-2,[7]])/spreadsheet.iloc[-2,[7]], cols60_f)
    worksheet.write('AG41', (spreadsheet.iloc[-1,[8]]-spreadsheet.iloc[-2,[8]])/spreadsheet.iloc[-2,[8]], cols90_f)
    
    worksheet.insert_chart('O59', DelinquencyExcelChart(writer, sheet, spreadsheet.shape, 9, 'Unsecured Personal Loan', 0.005))
    worksheet.insert_image('O59', 'images/unsecuredPL.png', {'x_offset': 5, 'y_offset': 5, 'x_scale': 0.8, 'y_scale': 0.8})
    worksheet.write('AD59', 'YoY', bold)
    worksheet.write('AD60', 'MoM', bold)
    worksheet.write('AE59', (spreadsheet.iloc[-1,[9]]-spreadsheet.iloc[-13,[9]])/spreadsheet.iloc[-13,[9]], cols30_f)
    worksheet.write('AF59', (spreadsheet.iloc[-1,[10]]-spreadsheet.iloc[-13,[10]])/spreadsheet.iloc[-13,[10]], cols60_f)
    worksheet.write('AG59', (spreadsheet.iloc[-1,[11]]-spreadsheet.iloc[-13,[11]])/spreadsheet.iloc[-13,[11]], cols90_f)
    worksheet.write('AE60', (spreadsheet.iloc[-1,[9]]-spreadsheet.iloc[-2,[9]])/spreadsheet.iloc[-2,[9]], cols30_f)
    worksheet.write('AF60', (spreadsheet.iloc[-1,[10]]-spreadsheet.iloc[-2,[10]])/spreadsheet.iloc[-2,[10]], cols60_f)
    worksheet.write('AG60', (spreadsheet.iloc[-1,[11]]-spreadsheet.iloc[-2,[11]])/spreadsheet.iloc[-2,[11]], cols90_f)
    
def OrigTrendsExcelChart(writer, sheet_name, tableshape, band, pdtype,y_units):
    # Create an Excel chart object.
    chart = writer.book.add_chart({'type': 'column'})
    # Set the chart series in Excel
    num_products=tableshape[1]+1
    num_scorebands = tableshape[0] + 1
    # Configure the first series.
    chart.add_series({
            'name':       [sheet_name, 0, band],
            'categories': [sheet_name, 1, 0, num_scorebands-1, 0],
            'values':     [sheet_name, 1, band, num_scorebands-1, band],
            'points': [{'fill': {'color': '#e63888'}}]*(num_scorebands-14) + [{'fill': {'color': '#1D4F91'}}] + [{'fill': {'color': '#e63888'}}]*11 + [{'fill': {'color': '#1D4F91'}}],
            'gap': 70,
        })    
    chart.set_plotarea({'fill': {'color': '#f2f2f2'}})
    # Combine the charts.
    chart.set_title({'name':pdtype,
                    'name_font': {'name':'Arial', 'color': '#1d4f91', 'size': 12, 'bold':True},
                    'layout': {
                                    'x': 0.05,
                                    'y': 0.02,
                                }
                    })
    chart.set_legend({'none': True})
    chart.set_size({'width':  12.78 * 72, 'height': 5.07 * 72})
    chart.set_x_axis({'num_font':  {'rotation': -45, 'color': '#63666a'}})
    chart.set_y_axis({
            'name': 'Dollars (in $BNs)', 
            'name_font':  {'color': '#63666a'},
            'major_unit': y_units,
            'display_units': 'billions',
            'display_units_visible': False,
            'num_font':  {'color': '#63666a'},
            'major_gridlines': {'visible': True, 'line': {'width': 0.75, 'color': '#EEECE1'}}        
        })
    
    return chart

def OrigTrendsWorksheet (pdByOrigtrends, writer, sheet):
    # Pivot the data so it is in the shape required for an Excel chart
    spreadsheet = pdByOrigtrends
    spreadsheet = spreadsheet.set_index('Month of Uf20 Profile Date')
    del spreadsheet.index.name
    for col in spreadsheet.columns:
        spreadsheet[col] = spreadsheet[col].astype(float)
        spreadsheet[col] = spreadsheet[col]
    
    spreadsheet['AUT'] = spreadsheet['AUT'] + spreadsheet['AUL']
    spreadsheet = spreadsheet.drop(columns=['AUL'])
    spreadsheet.columns=['Auto', 'Bank Card', 'Mortgage', 'Unsecured PL']
    with pd.option_context('display.float_format', '{:.2f}'.format):
        print(spreadsheet.tail())

    # First write the data to Excel
    spreadsheet.to_excel(writer, sheet_name=sheet) 
    # Set the column width and format.
    worksheet=writer.sheets[sheet]
    worksheet.set_column('B:E', 16, dollarformat) # format the numbers and set the column width
    worksheet.set_column('V:V', 25)
    worksheet.insert_chart('G2', OrigTrendsExcelChart(writer, sheet, spreadsheet.shape, 1, 'Automotive', 10**10)) # add the Excel chart
    worksheet.insert_image('G2', 'images/automotive.png', {'x_offset': 5, 'y_offset': 5, 'x_scale': 0.8, 'y_scale': 0.8})
    worksheet.write('V2', str(spreadsheet.index[-1])+' $ limits (billion): ', bold)
    worksheet.write('W2', spreadsheet['Auto'].iloc[-1]/1000000000, origlimit_format)
    worksheet.write('V3', 'YoY', bold)
    worksheet.write('W3', (spreadsheet['Auto'].iloc[-1]-spreadsheet['Auto'].iloc[-13])/spreadsheet['Auto'].iloc[-13], percentformat2)
    worksheet.write('V4', 'MoM', bold)
    worksheet.write('W4', (spreadsheet['Auto'].iloc[-1]-spreadsheet['Auto'].iloc[-2])/spreadsheet['Auto'].iloc[-2], percentformat2)
    
    worksheet.insert_chart('G21', OrigTrendsExcelChart(writer, sheet, spreadsheet.shape, 2, 'Bank Card', 10**10)) # add the Excel chart
    worksheet.insert_image('G21', 'images/bankcard.png', {'x_offset': 5, 'y_offset': 5, 'x_scale': 0.8, 'y_scale': 0.8})
    worksheet.write('V21', str(spreadsheet.index[-1])+' $ limits (billion): ', bold)
    worksheet.write('W21', spreadsheet['Bank Card'].iloc[-1]/1000000000, origlimit_format)
    worksheet.write('V22', 'YoY', bold)
    worksheet.write('W22', (spreadsheet['Bank Card'].iloc[-1]-spreadsheet['Bank Card'].iloc[-13])/spreadsheet['Bank Card'].iloc[-13], percentformat2)
    worksheet.write('V23', 'MoM', bold)
    worksheet.write('W23', (spreadsheet['Bank Card'].iloc[-1]-spreadsheet['Bank Card'].iloc[-2])/spreadsheet['Bank Card'].iloc[-2], percentformat2)
    
    worksheet.insert_chart('G40', OrigTrendsExcelChart(writer, sheet, spreadsheet.shape, 3, 'Mortgage', 5*(10**10))) # add the Excel chart
    worksheet.insert_image('G40', 'images/mortgage.png', {'x_offset': 5, 'y_offset': 5, 'x_scale': 0.8, 'y_scale': 0.8})
    worksheet.write('V40', str(spreadsheet.index[-1])+' $ limits (billion): ', bold)
    worksheet.write('W40', spreadsheet['Mortgage'].iloc[-1]/1000000000, origlimit_format)
    worksheet.write('V41', 'YoY', bold)
    worksheet.write('W41', (spreadsheet['Mortgage'].iloc[-1]-spreadsheet['Mortgage'].iloc[-13])/spreadsheet['Mortgage'].iloc[-13], percentformat2)
    worksheet.write('V42', 'MoM', bold)
    worksheet.write('W42', (spreadsheet['Mortgage'].iloc[-1]-spreadsheet['Mortgage'].iloc[-2])/spreadsheet['Mortgage'].iloc[-2], percentformat2)
    
    worksheet.insert_chart('G59', OrigTrendsExcelChart(writer, sheet, spreadsheet.shape, 4, 'Unsecured Personal Loans', 2*(10**9))) # add the Excel chart
    worksheet.insert_image('G59', 'images/unsecuredPL.png', {'x_offset': 5, 'y_offset': 5, 'x_scale': 0.8, 'y_scale': 0.8})
    worksheet.write('V59', str(spreadsheet.index[-1])+' $ limits (billion): ', bold)
    worksheet.write('W59', spreadsheet['Unsecured PL'].iloc[-1]/1000000000, origlimit_format)
    worksheet.write('V60', 'YoY', bold)
    worksheet.write('W60', (spreadsheet['Unsecured PL'].iloc[-1]-spreadsheet['Unsecured PL'].iloc[-13])/spreadsheet['Unsecured PL'].iloc[-13], percentformat2)
    worksheet.write('V61', 'MoM', bold)
    worksheet.write('W61', (spreadsheet['Unsecured PL'].iloc[-1]-spreadsheet['Unsecured PL'].iloc[-2])/spreadsheet['Unsecured PL'].iloc[-2], percentformat2)

def AvgBalExcelChart(writer, sheet_name, tableshape, band, pdtype, y_units):
    # Create an Excel chart object.
    chart = writer.book.add_chart({'type': 'column'})
    # Set the chart series in Excel
    num_products=tableshape[1]+1
    num_scorebands = tableshape[0] + 1
    # Configure the first series.
    chart.add_series({
            'name':       [sheet_name, 0, band],
            'categories': [sheet_name, 1, 0, num_scorebands-1, 0],
            'values':     [sheet_name, 1, band, num_scorebands-1, band],
            'points': [{'fill': {'color': '#1D4F91'}}]*(num_scorebands-14) + [{'fill': {'color': '#e63888'}}] + [{'fill': {'color': '#1D4F91'}}]*11 + [{'fill': {'color': '#e63888'}}],
            'gap': 70,
    })    
    chart.set_plotarea({'fill': {'color': '#f2f2f2'}})
    # Combine the charts.
    chart.set_title({'name':pdtype,
                    'name_font': {'name':'Arial', 'color': '#1d4f91', 'size': 12, 'bold':True},
                    'layout': {
                                    'x': 0.05,
                                    'y': 0.02,
                                }
                    })
    chart.set_legend({'none': True})
    chart.set_size({'width':  12.78 * 72, 'height': 5.07 * 72})
    chart.set_x_axis({'num_font':  {'rotation': -45, 'color': '#63666a'}})
    if pdtype == 'Automotive':
        min_val = 13500
    elif pdtype == 'Mortgage':
        min_val = 165000
    else:
        min_val=0
    chart.set_y_axis({
        'name': 'Dollars', 
        'name_font':  {'color': '#63666a'},
        'major_unit': y_units,
        'min':min_val,
        'num_font':  {'color': '#63666a'},
        'major_gridlines': {'visible': True, 'line': {'width': 0.75, 'color': '#EEECE1'}}
    })
    
    return chart

def AvgBalWorksheet (pdByAvgbal, writer, sheet):
    # Pivot the data so it is in the shape required for an Excel chart
    spreadsheet = pdByAvgbal
    spreadsheet = spreadsheet.set_index('Month of Uf20 Profile Date')
    del spreadsheet.index.name
    for col in spreadsheet.columns:
        spreadsheet[col] = spreadsheet[col].astype(float)
        spreadsheet[col] = spreadsheet[col]
    spreadsheet.columns=['Auto', 'Bank Card', 'Mortgage', 'Unsecured PL']
    with pd.option_context('display.float_format', '{:.2f}'.format):
        print(spreadsheet.tail())

    # First write the data to Excel
    spreadsheet.to_excel(writer, sheet_name=sheet) 
    # Set the column width and format.
    worksheet=writer.sheets[sheet]
    worksheet.set_column('B:E', 16, decimalformat) # format the numbers and set the column width
    worksheet.set_column('V:V', 25)
    worksheet.insert_chart('G2', AvgBalExcelChart(writer, sheet, spreadsheet.shape, 1, 'Automotive', 500)) # add the Excel chart
    worksheet.insert_image('G2', 'images/automotive.png', {'x_offset': 5, 'y_offset': 5, 'x_scale': 0.8, 'y_scale': 0.8})
    worksheet.write('V2', str(spreadsheet.index[-1])+' : ', bold)
    worksheet.write('W2', spreadsheet['Auto'].iloc[-1], origlimit_format)
    worksheet.write('V3', 'YoY', bold)
    worksheet.write('W3', (spreadsheet['Auto'].iloc[-1]-spreadsheet['Auto'].iloc[-13])/spreadsheet['Auto'].iloc[-13], percentformat2)
    worksheet.write('V4', 'MoM', bold)
    worksheet.write('W4', (spreadsheet['Auto'].iloc[-1]-spreadsheet['Auto'].iloc[-2])/spreadsheet['Auto'].iloc[-2], percentformat2)
    
    worksheet.insert_chart('G21', AvgBalExcelChart(writer, sheet, spreadsheet.shape, 2, 'Bank Card', 500)) # add the Excel chart
    worksheet.insert_image('G21', 'images/bankcard.png', {'x_offset': 5, 'y_offset': 5, 'x_scale': 0.8, 'y_scale': 0.8})
    worksheet.write('V21', str(spreadsheet.index[-1])+' : ', bold)
    worksheet.write('W21', spreadsheet['Bank Card'].iloc[-1], origlimit_format)
    worksheet.write('V22', 'YoY', bold)
    worksheet.write('W22', (spreadsheet['Bank Card'].iloc[-1]-spreadsheet['Bank Card'].iloc[-13])/spreadsheet['Bank Card'].iloc[-13], percentformat2)
    worksheet.write('V23', 'MoM', bold)
    worksheet.write('W23', (spreadsheet['Bank Card'].iloc[-1]-spreadsheet['Bank Card'].iloc[-2])/spreadsheet['Bank Card'].iloc[-2], percentformat2)
    
    worksheet.insert_chart('G40', AvgBalExcelChart(writer, sheet, spreadsheet.shape, 3, 'Mortgage', 5000)) # add the Excel chart
    worksheet.insert_image('G40', 'images/mortgage.png', {'x_offset': 5, 'y_offset': 5, 'x_scale': 0.8, 'y_scale': 0.8})
    worksheet.write('V40', str(spreadsheet.index[-1])+' : ', bold)
    worksheet.write('W40', spreadsheet['Mortgage'].iloc[-1], origlimit_format)
    worksheet.write('V41', 'YoY', bold)
    worksheet.write('W41', (spreadsheet['Mortgage'].iloc[-1]-spreadsheet['Mortgage'].iloc[-13])/spreadsheet['Mortgage'].iloc[-13], percentformat2)
    worksheet.write('V42', 'MoM', bold)
    worksheet.write('W42', (spreadsheet['Mortgage'].iloc[-1]-spreadsheet['Mortgage'].iloc[-2])/spreadsheet['Mortgage'].iloc[-2], percentformat2)
    
    worksheet.insert_chart('G59', AvgBalExcelChart(writer, sheet, spreadsheet.shape, 4, 'Unsecured Personal Loans', 1000)) # add the Excel chart
    worksheet.insert_image('G59', 'images/unsecuredPL.png', {'x_offset': 5, 'y_offset': 5, 'x_scale': 0.8, 'y_scale': 0.8})
    worksheet.write('V59', str(spreadsheet.index[-1])+' : ', bold)
    worksheet.write('W59', spreadsheet['Unsecured PL'].iloc[-1], origlimit_format)
    worksheet.write('V60', 'YoY', bold)
    worksheet.write('W60', (spreadsheet['Unsecured PL'].iloc[-1]-spreadsheet['Unsecured PL'].iloc[-13])/spreadsheet['Unsecured PL'].iloc[-13], percentformat2)
    worksheet.write('V61', 'MoM', bold)
    worksheet.write('W61', (spreadsheet['Unsecured PL'].iloc[-1]-spreadsheet['Unsecured PL'].iloc[-2])/spreadsheet['Unsecured PL'].iloc[-2], percentformat2)

def convert_to_numeric (input_df):
    for col in input_df.columns:
        if col not in ['portfolio_type','year','quarter']:
            input_df[col] = input_df[col].astype(float)
    return input_df  

def main_function():
    master_table = partitioned_master_table
    findspark.init()
    s = SparkSession.builder.appName("credit_trends").config("spark.yarn.tags", tag)\
        .config("spark.sql.broadcastTimeout", "7200")\
        .config("spark.driver.memory","64g" )\
        .config('spark.executor.memory', '64g')\
        .config("spark.driver.memoryOverhead", "6g")\
        .config("hive.exec.dynamic.partition.mode", "nonstrict")\
        .enableHiveSupport().getOrCreate()
        
    _=s.sql('''create or replace temporary function  P13_JAVAFilters as 'com.exp.P13_JAVA.Experian.UDTF_P13_JAVAFilters' using jar 's3://858664478982-data-us-east-1/hiveudf/an/Hive_Experian_P13_JAVA.jar' ''')
    master_start_date = s.sql(f'select min (uf20_profile_date) as startingdate from {master_table}').toPandas().iloc[0].startingdate.strftime('%Y-%m')

    pdpartitions = s.sql(f'show partitions {schema}.trade').orderBy(desc("partition")).toPandas()
    pdpartitions=pdpartitions[pdpartitions['partition'].str[-10:] > master_start_date]
    pdpartitions['profile_date']=pd.to_datetime(pdpartitions['partition'].str[-10:]).dt.date

    pdmaster_dates = s.sql(f'select distinct (uf20_profile_date) from {master_table}').toPandas()

    dates_to_run = [d.strftime ('%Y-%m-%d') for d in sorted(set (pdpartitions['profile_date']) - set (pdmaster_dates['uf20_profile_date'])) ]
    if (len (dates_to_run) > 0):
        print (f"""
            {master_table} needs the credit trends for {len(dates_to_run)} period(s): 
                {dates_to_run}
            """)
    else:
        return (f"{master_table} is already up to date.\n")
        
    datadate = dates_to_run [0]

    df_aggregate2=s.sql(aggregationquery)
    df_aggregate2.createOrReplaceTempView("aggregate_view")

    if master_table == partitioned_master_table:
        sqlstatement = f"insert overwrite table {master_table} partition (uf20_profile_date='{datadate}') select company_id, state, gen_id, Lender_type, Portfolio_type, Account_type, account_status, Vs4band, raw_cnt, vscr4_cnt, CNT_Wgt_Joint, CNT_Raw_300DPD, CNT_Raw_60DPD, CNT_Raw_90_180DPD, CNT_Wgt_30DPD, CNT_Wgt_60DPD, CNT_Wgt_90_180DPD, f25bal_sum, f25amt_sum, BAL_30DPD, BAL_60DPD, BAL_90_180DPD, vscr4_sum, btl_sum, btl2_sum, archive_ts from aggregate_view where uf20_profile_date='{datadate}'"
    else:
        sqlstatement = f"insert into {master_table} select * from aggregate_view"
        
    _=s.sql(sqlstatement)

    #Generate Monthly Excel Reports

    select_table_statement = f"select * from {master_table}"
    master_df = s.sql(select_table_statement)
                            
    orig_lim_df = master_df.filter("account_type in ('New[0-3]','New[4-6]') and account_status IN ('Deferred','Forbearance','Open') and portfolio_type IS NOT NULL") \
                            .withColumn("f25amt_sum_tr", 10*(master_df["f25amt_sum"])/6) \
                            .withColumn("Month of Uf20 Profile Date", f.date_format('uf20_profile_date',"MMM-yyyy")) \
                            .withColumn("Uf20 Profile Date_yyyy_MM", f.date_format('uf20_profile_date',"yyyy MM")) \
                            .withColumn("quarter" , f.quarter('uf20_profile_date')) \
                            .withColumn("year", f.year('uf20_profile_date'))
                            
    avg_bal_df = master_df.filter("account_type == 'Existing' AND account_status IN ('Deferred', 'Forbearance', 'Open') AND portfolio_type IS NOT NULL") \
                                .withColumn("Month of Uf20 Profile Date", f.date_format('uf20_profile_date',"MMM-yyyy")) \
                                .withColumn("Uf20 Profile Date_yyyy_MM", f.date_format('uf20_profile_date',"yyyy-MM")) \
                                .withColumn("quarter" , f.quarter('uf20_profile_date')) \
                                .withColumn("year", f.year('uf20_profile_date'))

    vs4_band_df = s.sql(DelinquencySQL)


    avg_vs_df = master_df.filter("account_status IN ('Deferred', 'Forbearance', 'Open') AND portfolio_type IS NOT NULL") \
                            .withColumn("Uf20 Profile Date_yyyy_MM", f.date_format('uf20_profile_date',"yyyy MM"))

    delinquency_df = master_df.filter("account_type in ('New[0-3]', 'New[4-6]', 'Existing') AND account_status IN ('Deferred', 'Forbearance', 'Open') AND portfolio_type IS NOT NULL") \
                            .withColumn("Month of Uf20 Profile Date", f.date_format('uf20_profile_date',"yyyy-MMM")) \
                            .withColumn("Uf20 Profile Date_yyyy_MM", f.date_format('uf20_profile_date',"yyyy-MM"))

    ## Origination Limits 
    out1 = orig_lim_df.groupBy(['portfolio_type','uf20_profile_date']) \
        .agg(f.sum("f25amt_sum_tr").alias("origlmts")) \
        .orderBy(f.col("portfolio_type").asc(),f.col("uf20_profile_date").desc())

    ## Origination Limits pivotted by Month
    out2 = orig_lim_df.groupBy(['uf20_profile_date','Month of Uf20 Profile Date']) \
        .pivot('portfolio_type') \
        .agg(f.sum("f25amt_sum_tr").alias("origlmts")) \
        .orderBy(f.col("uf20_profile_date").asc())

    ## Origination Limits pivotted by Quarter
    out3 = orig_lim_df.groupBy('year','quarter') \
        .pivot('portfolio_type') \
        .agg(f.sum("f25amt_sum_tr").alias("origlmts")) \
        .orderBy(f.col("year").desc(),f.col("quarter").asc())

    ## Average balances pivotted by Month
    out4 = avg_bal_df.groupBy(['portfolio_type']) \
        .pivot('Uf20 Profile Date_yyyy_MM') \
        .agg((f.sum("f25bal_sum")/f.sum("CNT_Wgt_Joint")).alias("avg_bal")) \
        .orderBy(f.col("portfolio_type").asc())

    ## % by VS
    out5 = vs4_band_df.toPandas()
    out5_df = out5.pivot( 'vs4band', 'portfolio_type','bandpercent').sort_index(ascending=False)
    out5_df.index = ['Super Prime', 'Prime', 'Near Prime', 'Subprime', 'Deep Subprime']
    out5_df.columns=['Auto', 'Bank Card', 'Mortgage', 'Unsecured PL']
    out5_df.drop ('Deep Subprime', inplace=True) # do not keep deep subprimes
    out5_df = out5_df.reindex(columns=['Mortgage', 'Auto', 'Unsecured PL', 'Bank Card'])

    ## Average VantageScore by Product Pivotted by Month
    out6 = avg_vs_df.groupBy(['portfolio_type']) \
        .pivot('Uf20 Profile Date_yyyy_MM') \
        .agg(f.sum("cnt_wgt_joint").alias("measures_values")) \
        .orderBy(f.col("portfolio_type").asc())

    ## Delinquency by product pivotted by month
    out7 = delinquency_df.groupBy(['portfolio_type','Month of Uf20 Profile Date','uf20_profile_date']) \
        .agg((f.sum("bal_30dpd")/f.sum("f25bal_sum")).alias("dpd30pct"), (f.sum("bal_60dpd")/f.sum("f25bal_sum")).alias("dpd60pct"), (f.sum("bal_90_180dpd")/f.sum("f25bal_sum")).alias("dpd90pct")) \
        .orderBy(f.col("portfolio_type").asc(),f.col("uf20_profile_date").asc())
    paste_df3_dpd30 = out7.select('portfolio_type', \
                                   'Month of Uf20 Profile Date', \
                                   'uf20_profile_date', \
                                   f.col('dpd30pct').alias('Measure Values')
                                  ).toPandas()
    paste_df3_dpd30['Measure Names'] = '%bals30'
    paste_df3_dpd60 = out7.select('portfolio_type', \
                                   'Month of Uf20 Profile Date', \
                                   'uf20_profile_date', \
                                   f.col('dpd60pct').alias('Measure Values')
                                  ).toPandas()
    paste_df3_dpd60['Measure Names'] = '%bals60'
    paste_df3_dpd90 = out7.select('portfolio_type', \
                                   'Month of Uf20 Profile Date', \
                                   'uf20_profile_date', \
                                   f.col('dpd90pct').alias('Measure Values')
                                  ).toPandas()
    paste_df3_dpd90['Measure Names'] = '%bals90'
    paste_df3 = pd.concat([paste_df3_dpd30, paste_df3_dpd60, paste_df3_dpd90], ignore_index=True)
    out7_df = paste_df3.pivot_table(index=['portfolio_type', 'Measure Names'], columns='uf20_profile_date',
                         values='Measure Values', aggfunc='first')

    out3_df = out3.toPandas()
    out4_df = out4.toPandas()
    out6_df = out6.toPandas()
    out3_df = convert_to_numeric(out3_df)
    out4_df = convert_to_numeric(out4_df)
    out6_df = convert_to_numeric(out6_df)    

    ## Create a Pandas Excel writer using XlsxWriter as the engine to generate Monthly Reports File
    writer = pd.ExcelWriter('Monthly_reports_'+now.strftime("%b-%d-%Y")+'.xlsx', engine='xlsxwriter', date_format='YYYY-MM')
    # Add some cell formats.
    dollarformat = writer.book.add_format({'num_format': '$#,##0'})
    decimalformat = writer.book.add_format({'num_format': '#,##0.00'})
    percentformat = writer.book.add_format({'num_format': '0.00%'})
    bold = writer.book.add_format({'bold': True})
    dateformat = writer.book.add_format({'num_format': 'yyyy-mmm'})
    # Write each dataframe to a different worksheet.
    out1_df = out1.select('portfolio_type', \
                'uf20_profile_date', \
                'origlmts').toPandas()
    out1_df['origlmts'] = out1_df['origlmts'].astype(float)
    out1_df.to_excel(writer, sheet_name = 'origlmts', index = False)
    out2_df = out2.toPandas()
    for col in out2_df.columns:
        if col not in ['uf20_profile_date','Month of Uf20 Profile Date']:
            out2_df[col] = out2_df[col].astype(float)
    out2_df.to_excel(writer, sheet_name = 'orig limit', index = False)
    out3_df.to_excel(writer, sheet_name = 'orig limit qtr', index = False)
    out4_df.to_excel(writer, sheet_name = 'avg bal', index = False)
    out5_df.to_excel(writer, sheet_name = '% by VS', index = True)
    out6_df.to_excel(writer, sheet_name = 'avg VS by product', index = False)
    out7_df.to_excel(writer, sheet_name = 'delinquency', index = True)

    worksheet=writer.sheets['origlmts']
    worksheet.set_column('A:C', 16, decimalformat)
    worksheet=writer.sheets['orig limit']
    worksheet.set_column('A:M', 17, decimalformat)
    worksheet=writer.sheets['orig limit qtr']
    worksheet.set_column('C:M', 17, decimalformat)
    worksheet=writer.sheets['avg bal']
    worksheet.set_column('A:BZ', 16, decimalformat)
    worksheet=writer.sheets['% by VS']
    worksheet.set_column('A:E', 16, percentformat)
    worksheet=writer.sheets['avg VS by product']
    worksheet.set_column('A:BZ', 16, decimalformat)
    worksheet=writer.sheets['delinquency']
    worksheet.set_column('A:BZ', 16, percentformat)
    writer.save()

    pdBalanceByVantage = vs4_band_df.toPandas()

    pdByOriglimits = orig_lim_df.filter("portfolio_type in ('MTF', 'AUT','AUL', 'UPL', 'BCA')") \
        .groupBy(['uf20_profile_date','Month of Uf20 Profile Date']) \
        .pivot('portfolio_type') \
        .agg(f.sum("f25amt_sum_tr").alias("origlmts")) \
        .orderBy(f.col("uf20_profile_date").asc()).select('Month of Uf20 Profile Date','AUT','AUL','BCA','MTF','UPL').toPandas()

    pdDqtrends = paste_df3

    pdByOrigtrends = orig_lim_df.filter("portfolio_type in ('AUT','AUL','BCA','MTF','UPL')") \
        .groupBy(['uf20_profile_date','Month of Uf20 Profile Date']) \
        .pivot('portfolio_type') \
        .agg(f.sum("f25amt_sum_tr").alias("origlmts")) \
        .orderBy(f.col("uf20_profile_date").asc()).select('Month of Uf20 Profile Date','AUT','AUL','BCA','MTF','UPL').toPandas()

    pdByAvgbal = avg_bal_df.filter("portfolio_type in ('AUT','BCA','MTF','UPL')") \
        .groupBy(['uf20_profile_date','Month of Uf20 Profile Date']) \
        .pivot('portfolio_type') \
        .agg((f.sum("f25bal_sum")/f.sum("CNT_Wgt_Joint")).alias("avg_bal")) \
        .orderBy(f.col("uf20_profile_date").asc()).select('Month of Uf20 Profile Date','AUT','BCA','MTF','UPL').toPandas()
        
    ## Creating excel file with charts
    writer=pd.ExcelWriter(f'CreditTrendsChart_{datadate}.xlsx')

    CoverPage = writer.book.add_worksheet('Cover')
    CoverPage.write (0,0, 'Credit Trends Data and Charts')
    CoverPage.write (2,0, f'Data as of {datadate}')
    dollarformat = writer.book.add_format({'num_format': '$#,##0'})
    decimalformat = writer.book.add_format({'num_format': '#,##0'})
    percentformat = writer.book.add_format({'num_format': '0.00%'})
    percentformat2 = writer.book.add_format({'num_format': '0%'})
    origlimit_format = writer.book.add_format({'num_format': '$#,##0.0', 'bold': True})
    bold = writer.book.add_format({'bold': True})
    cols30_f = writer.book.add_format({'num_format': '0%', 'bg_color': '#1d4f91', 'font_color': 'white'})
    cols60_f = writer.book.add_format({'num_format': '0%', 'bg_color': '#6d2077', 'font_color': 'white'})
    cols90_f = writer.book.add_format({'num_format': '0%', 'bg_color': '#e63888', 'font_color': 'white'})

    origlim_header = writer.book.add_format({'bg_color': '#1d4f91', 'font_color': 'white', 'bold': True})
    origlim1_num = writer.book.add_format({'num_format': '0%', 'font_color': '#1d4f91'})
    origlim1_txt = writer.book.add_format({'font_color': '#1d4f91', 'bold': True})
    origlim2_num = writer.book.add_format({'num_format': '0%', 'font_color': '#6d2077'})
    origlim2_txt = writer.book.add_format({'font_color': '#6d2077', 'bold': True})
    origlim3_num = writer.book.add_format({'num_format': '0%', 'font_color': '#0081A6'})
    origlim3_txt = writer.book.add_format({'font_color': '#0081A6', 'bold': True})
    origlim4_num = writer.book.add_format({'num_format': '0%', 'font_color': '#e63888'})
    origlim4_txt = writer.book.add_format({'font_color': '#e63888', 'bold': True})

    #CoverPage.write (3,0, f'Automatically generated on {now.day}/{now.month}/{now.year} at {now.hour}:{now.minute}')
    CoverPage.write (4,0, f'Charts automatically generated on {now.strftime("%A %b-%d-%Y at %H:%M:%S")} GMT by {getpass.getuser()}')

    VantageDistroWorksheet (pdBalanceByVantage, writer, 'VantageDist')
    OrigLimitsWorksheet (pdByOriglimits, writer, 'OrigLimits')
    DelinquencyWorksheet(pdDqtrends, writer, 'Delinquency Trends')
    OrigTrendsWorksheet (pdByOrigtrends, writer, "Origination Trends")
    AvgBalWorksheet (pdByAvgbal, writer, "Average Balance")
    writer.save() 
    s.stop()
    return("Script Ran successfully")
    
dag = DAG(
    dag_id='credit_trends',
    default_args=args,
    schedule_interval="59 23 L * *")
    
t1 = PythonOperator(
    task_id='main_function',
    python_callable= main_function,
    dag=dag,
)

t1