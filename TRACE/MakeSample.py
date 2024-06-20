##########################################
# Isolates issues for analysis           #
# Alexander Dickerson                    #
# Email: a.dickerson@warwick.ac.uk       #
# Date: June 2024                        #
# Updated:  June 2024                    #
# Version:  1.0.0                        #
##########################################

#* ************************************** */
#* Packages                               */
#* ************************************** */  
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import wrds

#* ************************************** */
#* Connect to WRDS                        */
#* ************************************** */  
db = wrds.Connection()

#* ************************************** */
#* Download Mergent File                  */
#* ************************************** */  
fisd_issuer = db.raw_sql("""SELECT issuer_id,country_domicile                 
                  FROM fisd.fisd_mergedissuer 
                  """)

fisd_issue = db.raw_sql("""SELECT complete_cusip, issue_id,
                  issuer_id, foreign_currency,
                  coupon_type,coupon,convertible,
                  asset_backed,rule_144a,
                  bond_type,private_placement,
                  interest_frequency,dated_date,
                  day_count_basis,offering_date                 
                  FROM fisd.fisd_mergedissue  
                  """)
                  
fisd = pd.merge(fisd_issue, fisd_issuer, on = ['issuer_id'], how = "left")      

#* ************************************** */
#* Ensure KPP Bonds are in the sample     */
#* ************************************** */  
# This ensures all CUSIPs from the paper,
# "Reconciling TRACE bond returns", by
# Bryan Kelly and Seth Pruitt are included.
# The paper is here: 
# https://sethpruitt.net/2022/03/29/reconciling-trace-bond-returns/
IDs_KPP = pd.read_csv('cusips.csv')
IDs_KPP.drop(['Unnamed: 0'], axis = 1, inplace = True)
IDs_KPP.columns = ['complete_cusip']

# Merge in information on rule 144a if available for the bonds in the KPP sample
IDs_KPP = pd.merge(IDs_KPP, fisd[['complete_cusip', 'rule_144a']], on = ['complete_cusip'], how = "left")

# If any KPP bonds don't have FISD information for rule 144a, assume they are not 144a bonds
IDs_KPP['rule_144a'] = IDs_KPP['rule_144a'].fillna('N')

#* ************************************** */
#* Apply BBW Bond Filters                 */
#* ************************************** */  
#1: Discard all non-US Bonds (i) in BBW
fisd = fisd[(fisd.country_domicile == 'USA')]

#2.1: US FX
fisd = fisd[(fisd.foreign_currency == 'N')]

#3: Must have a fixed coupon
fisd = fisd[(fisd.coupon_type != 'V')]

#4: Discard ALL convertible bonds
fisd = fisd[(fisd.convertible == 'N')]

#5: Discard all asset-backed bonds
fisd = fisd[(fisd.asset_backed == 'N')]

#6: Discard all bonds under Rule 144A
# fisd = fisd[(fisd.rule_144a == 'N')]

#7: Remove Agency bonds, Muni Bonds, Government Bonds, 
mask_corp = ((fisd.bond_type != 'TXMU')&  (fisd.bond_type != 'CCOV') &  (fisd.bond_type != 'CPAS')\
            &  (fisd.bond_type != 'MBS') &  (fisd.bond_type != 'FGOV')\
            &  (fisd.bond_type != 'USTC')   &  (fisd.bond_type != 'USBD')\
            &  (fisd.bond_type != 'USNT')  &  (fisd.bond_type != 'USSP')\
            &  (fisd.bond_type != 'USSI') &  (fisd.bond_type != 'FGS')\
            &  (fisd.bond_type != 'USBL') &  (fisd.bond_type != 'ABS')\
            &  (fisd.bond_type != 'O30Y')\
            &  (fisd.bond_type != 'O10Y') &  (fisd.bond_type != 'O3Y')\
            &  (fisd.bond_type != 'O5Y') &  (fisd.bond_type != 'O4W')\
            &  (fisd.bond_type != 'CCUR') &  (fisd.bond_type != 'O13W')\
            &  (fisd.bond_type != 'O52W')\
            &  (fisd.bond_type != 'O26W')\
            # Remove all Agency backed / Agency bonds #
            &  (fisd.bond_type != 'ADEB')\
            &  (fisd.bond_type != 'AMTN')\
            &  (fisd.bond_type != 'ASPZ')\
            &  (fisd.bond_type != 'EMTN')\
            &  (fisd.bond_type != 'ADNT')\
            &  (fisd.bond_type != 'ARNT')\
            # Remove preferred securities and inflation indexed securities #
            &  (fisd.bond_type != 'PSTK')\
            &  (fisd.bond_type != 'PS')\
            &  (fisd.bond_type != 'IIDX'))
fisd = fisd[(mask_corp)]

#8: No Private Placement if not 144a
fisd = fisd[~((fisd.private_placement == 'Y') & (fisd.rule_144a == 'N'))]

#9: Remove floating-rate, bi-monthly and unclassified coupons
fisd = fisd[(fisd.interest_frequency != -1) ] # Unclassified by Mergent
fisd = fisd[(fisd.interest_frequency != 13) ] # Variable Coupon (V)
fisd = fisd[(fisd.interest_frequency != 14) ] # Bi-Monthly Coupon
fisd = fisd[(fisd.interest_frequency != 16) ] # Unclassified by Mergent
fisd = fisd[(fisd.interest_frequency != 15) ] # Unclassified by Mergent

#10 Remove bonds lacking information for accrued interest (and hence returns)
fisd['offering_date']            = pd.to_datetime(fisd['offering_date'], format='%Y-%m-%d')
fisd['dated_date']               = pd.to_datetime(fisd['dated_date'],    format='%Y-%m-%d')

# 10.1 Dated date
fisd = fisd[~fisd.dated_date.isnull()]
# 10.2 Interest frequency
fisd = fisd[~fisd.interest_frequency.isnull()]
# 10.3 Day count basis
fisd = fisd[~fisd.day_count_basis.isnull()]
# 10.4 Offering date
fisd = fisd[~fisd.offering_date.isnull()]
# 10.5 Coupon type
fisd = fisd[~fisd.coupon_type.isnull()]
# 10.6 Coupon value
fisd = fisd[~fisd.coupon.isnull()]

#* ************************************** */
#* Parse out bonds for processing         */
#* ************************************** */           
IDs = fisd[['complete_cusip', 'rule_144a']]

#* ************************************** */
#* Ensure IDs unique                      */
#* ************************************** */ 
IDs = pd.concat([IDs, IDs_KPP], axis = 0)
IDs = IDs.drop_duplicates(subset='complete_cusip')

# Save in compressed GZIP format # 
IDs.to_csv('IDs.csv', index=False)   
# =============================================================================  